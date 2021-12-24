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

import org.apache.calcite.config.CalciteSystemProperty
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.type.SqlTypeUtil
import org.apache.calcite.util.Pair
import com.google.common.collect.ImmutableList
import org.apiguardian.api.API
import java.util.Arrays
import java.util.List
import java.util.Objects
import java.util.Objects.requireNonNull

/**
 * Context required to normalize a row-expression.
 *
 *
 * Currently, only simple normalization is supported, such as:
 *
 *
 *  * $2 = $1  $1 = $2
 *  * $2 &gt; $1  $1 &lt; $2
 *  * 1.23 = $1  $1 = 1.23
 *  * OR(OR(udf($1), $2), $3)  OR($3, OR($2, udf($1)))
 *
 *
 *
 * In the future, this component may extend to support more normalization cases
 * for general promotion. e.g. the strategy to decide which operand is more complex
 * should be more smart.
 *
 *
 * There is no one normalization strategy that works for all cases, and no consensus about what
 * the desired strategies should be. So by default, the normalization is disabled. We do force
 * normalization when computing the digest of [RexCall]s during planner planning.
 */
object RexNormalize {
    /**
     * Normalizes the variables of a rex call.
     *
     * @param operator The operator
     * @param operands The operands
     *
     * @return normalized variables of the call or the original
     * if there is no need to normalize
     */
    @API(since = "1.24", status = API.Status.EXPERIMENTAL)
    fun normalize(
        operator: SqlOperator,
        operands: List<RexNode>
    ): Pair<SqlOperator, List<RexNode>> {
        val original: Pair<SqlOperator, List<RexNode>> = Pair.of(operator, operands)
        if (!allowsNormalize() || operands.size() !== 2) {
            return original
        }
        val operand0: RexNode = operands[0]
        val operand1: RexNode = operands[1]

        // If arguments are the same, then we normalize < vs >
        // '<' == 60, '>' == 62, so we prefer <.
        val kind: SqlKind = operator.getKind()
        val reversedKind: SqlKind = kind.reverse()
        val x: Int = reversedKind.compareTo(kind)
        if (x < 0) {
            return Pair.of(
                requireNonNull(operator.reverse()),
                ImmutableList.of(operand1, operand0)
            )
        }
        if (x > 0) {
            return original
        }
        if (!isSymmetricalCall(operator, operand0, operand1)) {
            return original
        }
        return if (reorderOperands(operand0, operand1) < 0) {
            // $0=$1 is the same as $1=$0, so we make sure the digest is the same for them.

            // When $1 > $0 is normalized, the operation needs to be flipped
            // so we sort arguments first, then flip the sign.
            Pair.of(
                requireNonNull(operator.reverse()),
                ImmutableList.of(operand1, operand0)
            )
        } else original
    }

    /**
     * Computes the hashCode of a rex call. We ignore the operands sequence when the call is
     * symmetrical.
     *
     *
     * Note that the logic to decide whether operands need reordering
     * should be strictly same with [.normalize].
     */
    fun hashCode(
        operator: SqlOperator,
        operands: List<RexNode>
    ): Int {
        if (!allowsNormalize() || operands.size() !== 2) {
            return Objects.hash(operator, operands)
        }
        // If arguments are the same, then we normalize < vs >
        // '<' == 60, '>' == 62, so we prefer <.
        val kind: SqlKind = operator.getKind()
        val reversedKind: SqlKind = kind.reverse()
        val x: Int = reversedKind.compareTo(kind)
        if (x < 0) {
            return Objects.hash(
                requireNonNull(operator.reverse()),
                Arrays.asList(operands[1], operands[0])
            )
        }
        return if (isSymmetricalCall(operator, operands[0], operands[1])) {
            Objects.hash(operator, unorderedHash(operands))
        } else Objects.hash(operator, operands)
    }

    /**
     * Compares two operands to see which one should be normalized to be in front of the other.
     *
     *
     * We can always use the #hashCode to reorder the operands, do it as a fallback to keep
     * good readability.
     *
     * @param operand0  First operand
     * @param operand1  Second operand
     *
     * @return non-negative (>=0) if `operand0` should be in the front,
     * negative if `operand1` should be in the front
     */
    private fun reorderOperands(operand0: RexNode, operand1: RexNode): Int {
        // Reorder the operands based on the SqlKind enumeration sequence,
        // smaller is in the behind, e.g. the literal is behind of input ref and AND, OR.
        val x: Int = operand0.getKind().compareTo(operand1.getKind())
        // If the operands are same kind, use the hashcode to reorder.
        // Note: the RexInputRef's hash code is its index.
        return if (x != 0) x else operand1.hashCode() - operand0.hashCode()
    }

    /** Returns whether a call is symmetrical.  */
    private fun isSymmetricalCall(
        operator: SqlOperator,
        operand0: RexNode,
        operand1: RexNode
    ): Boolean {
        return (operator.isSymmetrical()
                || SqlKind.SYMMETRICAL_SAME_ARG_TYPE.contains(operator.getKind())
                && SqlTypeUtil.equalSansNullability(operand0.getType(), operand1.getType()))
    }

    /** Compute a hash that is symmetric in its arguments - that is a hash
     * where the order of appearance of elements does not matter.
     * This is useful for hashing symmetrical rex calls, for example.
     */
    private fun unorderedHash(xs: List<*>): Int {
        var a = 0
        var b = 0
        var c = 1
        for (x in xs) {
            val h: Int = Objects.hashCode(x)
            a += h
            b = b xor h
            if (h != 0) {
                c *= h
            }
        }
        return (a * 17 + b) * 17 + c
    }

    /**
     * The digest of `RexNode` is normalized by default.
     *
     * @return true if the digest allows normalization
     */
    private fun allowsNormalize(): Boolean {
        return CalciteSystemProperty.ENABLE_REX_DIGEST_NORMALIZE.value()
    }
}
