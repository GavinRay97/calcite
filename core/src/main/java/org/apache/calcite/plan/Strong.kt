/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.plan

import org.apache.calcite.rex.RexCall

/** Utilities for strong predicates.
 *
 *
 * A predicate is strong (or null-rejecting) if it is UNKNOWN if any of its
 * inputs is UNKNOWN.
 *
 *
 * By the way, UNKNOWN is just the boolean form of NULL.
 *
 *
 * Examples:
 *
 *  * `UNKNOWN` is strong in [] (definitely null)
 *  * `c = 1` is strong in [c] (definitely null if and only if c is
 * null)
 *  * `c IS NULL` is not strong (always returns TRUE or FALSE, never
 * null)
 *  * `p1 AND p2` is strong in [p1, p2] (definitely null if either p1
 * is null or p2 is null)
 *  * `p1 OR p2` is strong if p1 and p2 are strong
 *  * `c1 = 1 OR c2 IS NULL` is strong in [c1] (definitely null if c1
 * is null)
 *
 */
class Strong {
    /** Returns whether an expression is definitely not true.  */
    fun isNotTrue(node: RexNode): Boolean {
        return when (node.getKind()) {
            IS_NOT_NULL -> anyNull((node as RexCall).getOperands())
            else -> isNull(node)
        }
    }

    /** Returns whether an expression is definitely null.
     *
     *
     * The answer is based on calls to [.isNull] for its constituent
     * expressions, and you may override methods to test hypotheses such as
     * "if `x` is null, is `x + y` null?  */
    fun isNull(node: RexNode): Boolean {
        val policy: Policy = policy(node)
        when (policy) {
            Policy.NOT_NULL -> return false
            Policy.ANY -> return anyNull((node as RexCall).getOperands())
            else -> {}
        }
        return when (node.getKind()) {
            LITERAL -> (node as RexLiteral).isNull()
            AND, OR, COALESCE -> allNull((node as RexCall).getOperands())
            NULLIF ->       // NULLIF(null, X) where X can be NULL, returns NULL
                // NULLIF(X, Y) where X is not NULL, then this may return NULL if X = Y, otherwise X.
                allNull(ImmutableList.of((node as RexCall).getOperands().get(0)))
            INPUT_REF -> isNull(node as RexInputRef)
            CASE -> {
                val caseCall: RexCall = node as RexCall
                val caseValues: List<RexNode> = ArrayList()
                var i = 0
                while (i < caseCall.getOperands().size()) {
                    if (!RexUtil.isCasePredicate(caseCall, i)) {
                        caseValues.add(caseCall.getOperands().get(i))
                    }
                    i++
                }
                allNull(caseValues)
            }
            else -> false
        }
    }

    /** Returns whether a given input is definitely null.  */
    fun isNull(ref: RexInputRef?): Boolean {
        return false
    }

    /** Returns whether all expressions in a list are definitely null.  */
    private fun allNull(operands: List<RexNode>): Boolean {
        for (operand in operands) {
            if (!isNull(operand)) {
                return false
            }
        }
        return true
    }

    /** Returns whether any expressions in a list are definitely null.  */
    private fun anyNull(operands: List<RexNode>): Boolean {
        for (operand in operands) {
            if (isNull(operand)) {
                return true
            }
        }
        return false
    }

    /** How whether an operator's operands are null affects whether a call to
     * that operator evaluates to null.  */
    enum class Policy {
        /** This kind of expression is never null. No need to look at its arguments,
         * if it has any.  */
        NOT_NULL,

        /** This kind of expression has its own particular rules about whether it
         * is null.  */
        CUSTOM,

        /** This kind of expression is null if and only if at least one of its
         * arguments is null.  */
        ANY,

        /** This kind of expression may be null. There is no way to rewrite.  */
        AS_IS
    }

    companion object {
        private val MAP: Map<SqlKind, Policy> = createPolicyMap()

        /** Returns a checker that consults a bit set to find out whether particular
         * inputs may be null.  */
        fun of(nullColumns: ImmutableBitSet): Strong {
            return object : Strong() {
                @Override
                override fun isNull(ref: RexInputRef): Boolean {
                    return nullColumns.get(ref.getIndex())
                }
            }
        }

        /** Returns whether the analyzed expression will definitely return null if
         * all of a given set of input columns are null.  */
        fun isNull(node: RexNode?, nullColumns: ImmutableBitSet): Boolean {
            return of(nullColumns).isNull(node)
        }

        /** Returns whether the analyzed expression will definitely not return true
         * (equivalently, will definitely not return null or false) if
         * all of a given set of input columns are null.  */
        fun isNotTrue(node: RexNode, nullColumns: ImmutableBitSet): Boolean {
            return of(nullColumns).isNotTrue(node)
        }

        /**
         * Returns how to deduce whether a particular kind of expression is null,
         * given whether its arguments are null.
         *
         */
        @Deprecated // to be removed before 2.0
        @Deprecated("Use {@link Strong#policy(RexNode)} or {@link Strong#policy(SqlOperator)}")
        fun policy(kind: SqlKind?): Policy {
            return MAP.getOrDefault(kind, Policy.AS_IS)
        }

        /**
         * Returns how to deduce whether a particular [RexNode] expression is null,
         * given whether its arguments are null.
         */
        fun policy(rexNode: RexNode): Policy {
            return if (rexNode is RexCall) {
                policy((rexNode as RexCall).getOperator())
            } else MAP.getOrDefault(
                rexNode.getKind(),
                Policy.AS_IS
            )
        }

        /**
         * Returns how to deduce whether a particular [SqlOperator] expression is null,
         * given whether its arguments are null.
         */
        fun policy(operator: SqlOperator): Policy {
            return if (operator.getStrongPolicyInference() != null) {
                operator.getStrongPolicyInference().get()
            } else MAP.getOrDefault(operator.getKind(), Policy.AS_IS)
        }

        /**
         * Returns whether a given expression is strong.
         *
         *
         * Examples:
         *
         *  * Returns true for `c = 1` since it returns null if and only if
         * c is null
         *  * Returns false for `c IS NULL` since it always returns TRUE
         * or FALSE
         *
         *
         * @param e Expression
         * @return true if the expression is strong, false otherwise
         */
        fun isStrong(e: RexNode): Boolean {
            val nullColumns: ImmutableBitSet.Builder = ImmutableBitSet.builder()
            e.accept(
                object : RexVisitorImpl<Void?>(true) {
                    @Override
                    fun visitInputRef(inputRef: RexInputRef): Void {
                        nullColumns.set(inputRef.getIndex())
                        return super.visitInputRef(inputRef)
                    }
                })
            return isNull(e, nullColumns.build())
        }

        /** Returns whether all expressions in a list are strong.  */
        fun allStrong(operands: List<RexNode?>): Boolean {
            return operands.stream().allMatch { e: RexNode -> isStrong(e) }
        }

        private fun createPolicyMap(): Map<SqlKind, Policy> {
            val map: EnumMap<SqlKind, Policy> = EnumMap(SqlKind::class.java)
            map.put(SqlKind.INPUT_REF, Policy.AS_IS)
            map.put(SqlKind.LOCAL_REF, Policy.AS_IS)
            map.put(SqlKind.DYNAMIC_PARAM, Policy.AS_IS)
            map.put(SqlKind.OTHER_FUNCTION, Policy.AS_IS)

            // The following types of expressions could potentially be custom.
            map.put(SqlKind.CASE, Policy.AS_IS)
            map.put(SqlKind.DECODE, Policy.AS_IS)
            // NULLIF(1, NULL) yields 1, but NULLIF(1, 1) yields NULL
            map.put(SqlKind.NULLIF, Policy.AS_IS)
            // COALESCE(NULL, 2) yields 2
            map.put(SqlKind.COALESCE, Policy.AS_IS)
            map.put(SqlKind.NVL, Policy.AS_IS)
            // FALSE AND NULL yields FALSE
            // TRUE AND NULL yields NULL
            map.put(SqlKind.AND, Policy.AS_IS)
            // TRUE OR NULL yields TRUE
            // FALSE OR NULL yields NULL
            map.put(SqlKind.OR, Policy.AS_IS)

            // Expression types with custom handlers.
            map.put(SqlKind.LITERAL, Policy.CUSTOM)
            map.put(SqlKind.EXISTS, Policy.NOT_NULL)
            map.put(SqlKind.IS_DISTINCT_FROM, Policy.NOT_NULL)
            map.put(SqlKind.IS_NOT_DISTINCT_FROM, Policy.NOT_NULL)
            map.put(SqlKind.IS_NULL, Policy.NOT_NULL)
            map.put(SqlKind.IS_NOT_NULL, Policy.NOT_NULL)
            map.put(SqlKind.IS_TRUE, Policy.NOT_NULL)
            map.put(SqlKind.IS_NOT_TRUE, Policy.NOT_NULL)
            map.put(SqlKind.IS_FALSE, Policy.NOT_NULL)
            map.put(SqlKind.IS_NOT_FALSE, Policy.NOT_NULL)
            map.put(SqlKind.NOT, Policy.ANY)
            map.put(SqlKind.EQUALS, Policy.ANY)
            map.put(SqlKind.NOT_EQUALS, Policy.ANY)
            map.put(SqlKind.LESS_THAN, Policy.ANY)
            map.put(SqlKind.LESS_THAN_OR_EQUAL, Policy.ANY)
            map.put(SqlKind.GREATER_THAN, Policy.ANY)
            map.put(SqlKind.GREATER_THAN_OR_EQUAL, Policy.ANY)
            map.put(SqlKind.LIKE, Policy.ANY)
            map.put(SqlKind.SIMILAR, Policy.ANY)
            map.put(SqlKind.PLUS, Policy.ANY)
            map.put(SqlKind.PLUS_PREFIX, Policy.ANY)
            map.put(SqlKind.MINUS, Policy.ANY)
            map.put(SqlKind.MINUS_PREFIX, Policy.ANY)
            map.put(SqlKind.TIMES, Policy.ANY)
            map.put(SqlKind.DIVIDE, Policy.ANY)
            map.put(SqlKind.CAST, Policy.ANY)
            map.put(SqlKind.REINTERPRET, Policy.ANY)
            map.put(SqlKind.TRIM, Policy.ANY)
            map.put(SqlKind.LTRIM, Policy.ANY)
            map.put(SqlKind.RTRIM, Policy.ANY)
            map.put(SqlKind.CEIL, Policy.ANY)
            map.put(SqlKind.FLOOR, Policy.ANY)
            map.put(SqlKind.EXTRACT, Policy.ANY)
            map.put(SqlKind.GREATEST, Policy.ANY)
            map.put(SqlKind.LEAST, Policy.ANY)
            map.put(SqlKind.TIMESTAMP_ADD, Policy.ANY)
            map.put(SqlKind.TIMESTAMP_DIFF, Policy.ANY)
            map.put(SqlKind.ITEM, Policy.ANY)

            // Assume that any other expressions cannot be simplified.
            for (k in Iterables.concat(SqlKind.EXPRESSION, SqlKind.AGGREGATE)) {
                if (!map.containsKey(k)) {
                    map.put(k, Policy.AS_IS)
                }
            }
            return map
        }
    }
}
