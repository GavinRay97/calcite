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

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.SqlSyntax
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.sql.type.SqlTypeUtil
import org.apache.calcite.util.Litmus
import org.apache.calcite.util.Pair
import org.apache.calcite.util.Sarg
import com.google.common.collect.ImmutableList
import java.util.ArrayList
import java.util.List
import java.util.Objects.requireNonNull

/**
 * An expression formed by a call to an operator with zero or more expressions
 * as operands.
 *
 *
 * Operators may be binary, unary, functions, special syntactic constructs
 * like `CASE ... WHEN ... END`, or even internally generated
 * constructs like implicit type conversions. The syntax of the operator is
 * really irrelevant, because row-expressions (unlike
 * [SQL expressions][org.apache.calcite.sql.SqlNode])
 * do not directly represent a piece of source code.
 *
 *
 * It's not often necessary to sub-class this class. The smarts should be in
 * the operator, rather than the call. Any extra information about the call can
 * often be encoded as extra arguments. (These don't need to be hidden, because
 * no one is going to be generating source code from this tree.)
 */
class RexCall(
    type: RelDataType?,
    operator: SqlOperator,
    operands: List<RexNode?>
) : RexNode() {
    //~ Instance fields --------------------------------------------------------
    val op: SqlOperator
    val operands: ImmutableList<RexNode>
    val type: RelDataType
    val nodeCount: Int

    /**
     * Cache of hash code.
     */
    protected var hash = 0

    /**
     * Cache of normalized variables used for #equals and #hashCode.
     */
    @Nullable
    private var normalized: Pair<SqlOperator, List<RexNode>>? = null
        private get() {
            if (field == null) {
                field = RexNormalize.normalize(op, operands)
            }
            return field
        }

    //~ Constructors -----------------------------------------------------------
    init {
        this.type = requireNonNull(type, "type")
        op = requireNonNull(operator, "operator")
        this.operands = ImmutableList.copyOf(operands)
        nodeCount = RexUtil.nodeCount(1, this.operands)
        assert(operator.getKind() != null) { operator }
        assert(operator.validRexOperands(operands.size(), Litmus.THROW)) { this }
        assert(operator.kind !== SqlKind.IN || this is RexSubQuery)
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Appends call operands without parenthesis.
     * [RexLiteral] might omit data type depending on the context.
     * For instance, `null:BOOLEAN` vs `=(true, null)`.
     * The idea here is to omit "obvious" types for readability purposes while
     * still maintain [RelNode.getDigest] contract.
     *
     * @see RexLiteral.computeDigest
     * @param sb destination
     */
    protected fun appendOperands(sb: StringBuilder) {
        if (operands.isEmpty()) {
            return
        }
        val operandDigests: List<String> = ArrayList(operands.size())
        for (i in 0 until operands.size()) {
            val operand: RexNode = operands.get(i)
            if (operand !is RexLiteral) {
                operandDigests.add(operand.toString())
                continue
            }
            // Type information might be omitted in certain cases to improve readability
            // For instance, AND/OR arguments should be BOOLEAN, so
            // AND(true, null) is better than AND(true, null:BOOLEAN), and we keep the same info.

            // +($0, 2) is better than +($0, 2:BIGINT). Note: if $0 is BIGINT, then 2 is expected to be
            // of BIGINT type as well.
            var includeType: RexDigestIncludeType = RexDigestIncludeType.OPTIONAL
            if ((isA(SqlKind.AND) || isA(SqlKind.OR))
                && operand.getType().getSqlTypeName() === SqlTypeName.BOOLEAN
            ) {
                includeType = RexDigestIncludeType.NO_TYPE
            }
            if (SqlKind.SIMPLE_BINARY_OPS.contains(kind)) {
                val otherArg: RexNode = operands.get(1 - i)
                if ((otherArg !is RexLiteral
                            || digestSkipsType(otherArg as RexLiteral))
                    && SqlTypeUtil.equalSansNullability(operand.getType(), otherArg.getType())
                ) {
                    includeType = RexDigestIncludeType.NO_TYPE
                }
            }
            operandDigests.add(computeDigest(operand as RexLiteral, includeType))
        }
        var totalLength: Int = (operandDigests.size() - 1) * 2 // commas
        for (s in operandDigests) {
            totalLength += s.length()
        }
        sb.ensureCapacity(sb.length() + totalLength)
        for (i in 0 until operandDigests.size()) {
            val op = operandDigests[i]
            if (i != 0) {
                sb.append(", ")
            }
            sb.append(op)
        }
    }

    protected fun computeDigest(withType: Boolean): String {
        val sb = StringBuilder(op.getName())
        if (operands.size() === 0
            && op.getSyntax() === SqlSyntax.FUNCTION_ID
        ) {
            // Don't print params for empty arg list. For example, we want
            // "SYSTEM_USER", not "SYSTEM_USER()".
        } else {
            sb.append("(")
            appendOperands(sb)
            sb.append(")")
        }
        if (withType) {
            sb.append(":")

            // NOTE jvs 16-Jan-2005:  for digests, it is very important
            // to use the full type string.
            sb.append(type.getFullTypeString())
        }
        return sb.toString()
    }

    @Override
    override fun toString(): String {
        return computeDigest(digestWithType())
    }

    private fun digestWithType(): Boolean {
        return isA(SqlKind.CAST) || isA(SqlKind.NEW_SPECIFICATION)
    }

    @Override
    fun <R> accept(visitor: RexVisitor<R>): R {
        return visitor.visitCall(this)
    }

    @Override
    fun <R, P> accept(visitor: RexBiVisitor<R, P>, arg: P): R {
        return visitor.visitCall(this, arg)
    }

    @Override
    fun getType(): RelDataType {
        return type
    }

    // "c IS NOT NULL" occurs when we expand EXISTS.
    // This reduction allows us to convert it to a semi-join.
    @get:Override
    val isAlwaysTrue: Boolean
        get() =// "c IS NOT NULL" occurs when we expand EXISTS.
            // This reduction allows us to convert it to a semi-join.
            when (kind) {
                IS_NOT_NULL -> !operands.get(0).getType().isNullable()
                IS_NOT_TRUE, IS_FALSE, NOT -> operands.get(0).isAlwaysFalse()
                IS_NOT_FALSE, IS_TRUE, CAST -> operands.get(0).isAlwaysTrue()
                SEARCH -> {
                    val sarg: Sarg = (operands.get(1) as RexLiteral).getValueAs(Sarg::class.java)
                    (requireNonNull(sarg, "sarg").isAll()
                            && (sarg.nullAs === RexUnknownAs.TRUE
                            || !operands.get(0).getType().isNullable()))
                }
                else -> false
            }

    @get:Override
    val isAlwaysFalse: Boolean
        get() = when (kind) {
            IS_NULL -> !operands.get(0).getType().isNullable()
            IS_NOT_TRUE, IS_FALSE, NOT -> operands.get(0).isAlwaysTrue()
            IS_NOT_FALSE, IS_TRUE, CAST -> operands.get(0).isAlwaysFalse()
            SEARCH -> {
                val sarg: Sarg = (operands.get(1) as RexLiteral).getValueAs(Sarg::class.java)
                (requireNonNull(sarg, "sarg").isNone()
                        && (sarg.nullAs === RexUnknownAs.FALSE
                        || !operands.get(0).getType().isNullable()))
            }
            else -> false
        }

    @get:Override
    val kind: SqlKind
        get() = op.kind

    fun getOperands(): List<RexNode> {
        return operands
    }

    val operator: SqlOperator
        get() = op

    @Override
    fun nodeCount(): Int {
        return nodeCount
    }

    /**
     * Creates a new call to the same operator with different operands.
     *
     * @param type     Return type
     * @param operands Operands to call
     * @return New call
     */
    fun clone(type: RelDataType?, operands: List<RexNode?>): RexCall {
        return RexCall(type, op, operands)
    }

    @Override
    override fun equals(@Nullable o: Object?): Boolean {
        if (this === o) {
            return true
        }
        if (o == null || getClass() !== o.getClass()) {
            return false
        }
        val x: Pair<SqlOperator, List<RexNode>> = normalized
        val rexCall = o as RexCall
        val y: Pair<SqlOperator, List<RexNode>> = rexCall.normalized
        return (x.left.equals(y.left)
                && x.right.equals(y.right)
                && type.equals(rexCall.type))
    }

    @Override
    override fun hashCode(): Int {
        if (hash == 0) {
            hash = RexNormalize.hashCode(op, operands)
        }
        return hash
    }

    companion object {
        private fun digestSkipsType(literal: RexLiteral): Boolean {
            // This seems trivial, however, this method
            // workarounds https://github.com/typetools/checker-framework/issues/3631
            return literal.digestIncludesType() === RexDigestIncludeType.NO_TYPE
        }

        private fun computeDigest(literal: RexLiteral, includeType: RexDigestIncludeType): String {
            // This seems trivial, however, this method
            // workarounds https://github.com/typetools/checker-framework/issues/3631
            return literal.computeDigest(includeType)
        }
    }
}
