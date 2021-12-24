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
package org.apache.calcite.rex

/** Policy for whether a simplified expression may instead return another
 * value.
 *
 *
 * In particular, it deals with converting three-valued logic (TRUE, FALSE,
 * UNKNOWN) to two-valued logic (TRUE, FALSE) for callers that treat the UNKNOWN
 * value the same as TRUE or FALSE.
 *
 *
 * Sometimes the three-valued version of the expression is simpler (has a
 * smaller expression tree) than the two-valued version. In these cases,
 * favor simplicity over reduction to two-valued logic.
 *
 * @see RexSimplify
 */
enum class RexUnknownAs {
    /** Policy that indicates that the expression is being used in a context
     * Where an UNKNOWN value is treated in the same way as FALSE. Therefore, when
     * simplifying the expression, it is acceptable for the simplified expression
     * to evaluate to FALSE in some situations where the original expression would
     * evaluate to UNKNOWN.
     *
     *
     * SQL predicates (`WHERE`, `ON`, `HAVING` and
     * `FILTER (WHERE)` clauses, a `WHEN` clause of a `CASE`
     * expression, and in `CHECK` constraints) all treat UNKNOWN as FALSE.
     *
     *
     * If the simplified expression never returns UNKNOWN, the simplifier
     * should make this clear to the caller, if possible, by marking its type as
     * `BOOLEAN NOT NULL`.  */
    FALSE,

    /** Policy that indicates that the expression is being used in a context
     * Where an UNKNOWN value is treated in the same way as TRUE. Therefore, when
     * simplifying the expression, it is acceptable for the simplified expression
     * to evaluate to TRUE in some situations where the original expression would
     * evaluate to UNKNOWN.
     *
     *
     * This does not occur commonly in SQL. However, it occurs internally
     * during simplification. For example, "`WHERE NOT expression`"
     * evaluates "`NOT expression`" in a context that treats UNKNOWN as
     * FALSE; it is useful to consider that "`expression`" is evaluated in a
     * context that treats UNKNOWN as TRUE.
     *
     *
     * If the simplified expression never returns UNKNOWN, the simplifier
     * should make this clear to the caller, if possible, by marking its type as
     * `BOOLEAN NOT NULL`.  */
    TRUE,

    /** Policy that indicates that the expression is being used in a context
     * Where an UNKNOWN value is treated as is. This occurs:
     *
     *
     *  * In any expression whose type is not `BOOLEAN`
     *  * In `BOOLEAN` expressions that are `NOT NULL`
     *  * In `BOOLEAN` expressions where `UNKNOWN` should be
     * returned as is, for example in a `SELECT` clause, or within an
     * expression such as an operand to `AND`, `OR` or
     * `NOT`
     *
     *
     *
     * If you are unsure, use UNKNOWN. It is the safest option.  */
    UNKNOWN;

    fun toBoolean(): Boolean {
        return when (this) {
            FALSE -> false
            TRUE -> true
            else -> throw IllegalArgumentException("unknown")
        }
    }

    fun negate(): RexUnknownAs {
        return when (this) {
            TRUE -> FALSE
            FALSE -> TRUE
            else -> UNKNOWN
        }
    }

    /** Combines this with another `RexUnknownAs` in the same way as the
     * three-valued logic of OR.
     *
     *
     * For example, `TRUE or FALSE` returns `TRUE`;
     * `FALSE or UNKNOWN` returns `UNKNOWN`.  */
    fun or(other: RexUnknownAs): RexUnknownAs {
        return when (this) {
            TRUE -> this
            UNKNOWN -> if (other == TRUE) other else this
            FALSE -> other
            else -> other
        }
    }

    companion object {
        /** Returns [.FALSE] if `unknownAsFalse` is true,
         * [.UNKNOWN] otherwise.  */
        fun falseIf(unknownAsFalse: Boolean): RexUnknownAs {
            return if (unknownAsFalse) FALSE else UNKNOWN
        }
    }
}
