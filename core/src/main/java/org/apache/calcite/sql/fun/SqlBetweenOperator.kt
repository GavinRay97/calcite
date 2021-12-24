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
package org.apache.calcite.sql.`fun`

import org.apache.calcite.rel.type.RelDataType

/**
 * Defines the BETWEEN operator.
 *
 *
 * Syntax:
 *
 * <blockquote>`X [NOT] BETWEEN [ASYMMETRIC | SYMMETRIC] Y AND
 * Z`</blockquote>
 *
 *
 * If the asymmetric/symmetric keywords are left out ASYMMETRIC is default.
 *
 *
 * This operator is always expanded (into something like `Y <= X AND
 * X <= Z`) before being converted into Rex nodes.
 */
class SqlBetweenOperator
/**
 * Returns whether this is 'NOT' variant of an operator.
 *
 * @see .not
 */

//~ Constructors -----------------------------------------------------------
    (//~ Instance fields --------------------------------------------------------
    val flag: Flag,
    /**
     * If true the call represents 'NOT BETWEEN'.
     */
    val isNegated: Boolean
) : SqlInfixOperator(
    if (isNegated) NOT_BETWEEN_NAMES else BETWEEN_NAMES, SqlKind.BETWEEN, 32,
    null, InferTypes.FIRST_KNOWN, OTC_CUSTOM
) {
    //~ Enums ------------------------------------------------------------------
    /**
     * Defines the "SYMMETRIC" and "ASYMMETRIC" keywords.
     */
    enum class Flag {
        ASYMMETRIC, SYMMETRIC
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun validRexOperands(count: Int, litmus: Litmus): Boolean {
        return litmus.fail("not a rex operator")
    }

    @Override
    operator fun not(): SqlOperator {
        return of(isNegated, flag == Flag.SYMMETRIC)
    }

    @Override
    fun inferReturnType(
        opBinding: SqlOperatorBinding
    ): RelDataType {
        val newOpBinding = ExplicitOperatorBinding(
            opBinding,
            opBinding.collectOperandTypes()
        )
        val type: RelDataType = ReturnTypes.BOOLEAN_NULLABLE.inferReturnType(
            newOpBinding
        )
        return requireNonNull(type, "inferred BETWEEN element type")
    }

    @Override
    fun getSignatureTemplate(operandsCount: Int): String {
        Util.discard(operandsCount)
        return "{1} {0} {2} AND {3}"
    }

    @get:Override
    val name: String
        get() = (super.getName()
                + " "
                + flag.name())

    @Override
    fun unparse(
        writer: SqlWriter,
        call: SqlCall,
        leftPrec: Int,
        rightPrec: Int
    ) {
        val frame: SqlWriter.Frame = writer.startList(FRAME_TYPE, "", "")
        call.operand(VALUE_OPERAND).unparse(writer, getLeftPrec(), 0)
        writer.sep(super.getName())
        writer.sep(flag.name())

        // If the expression for the lower bound contains a call to an AND
        // operator, we need to wrap the expression in parentheses to prevent
        // the AND from associating with BETWEEN. For example, we should
        // unparse
        //    a BETWEEN b OR (c AND d) OR e AND f
        // as
        //    a BETWEEN (b OR c AND d) OR e) AND f
        // If it were unparsed as
        //    a BETWEEN b OR c AND d OR e AND f
        // then it would be interpreted as
        //    (a BETWEEN (b OR c) AND d) OR (e AND f)
        // which would be wrong.
        val lower: SqlNode = call.operand(LOWER_OPERAND)
        val upper: SqlNode = call.operand(UPPER_OPERAND)
        val lowerPrec = if (AndFinder().containsAnd(lower)) 100 else 0
        lower.unparse(writer, lowerPrec, lowerPrec)
        writer.sep("AND")
        upper.unparse(writer, 0, getRightPrec())
        writer.endList(frame)
    }

    @Override
    fun reduceExpr(opOrdinal: Int, list: TokenSequence): ReduceResult {
        val op: SqlOperator = list.op(opOrdinal)
        assert(op === this)

        // Break the expression up into expressions. For example, a simple
        // expression breaks down as follows:
        //
        //            opOrdinal   endExp1
        //            |           |
        //     a + b BETWEEN c + d AND e + f
        //    |_____|       |_____|   |_____|
        //     exp0          exp1      exp2
        // Create the expression between 'BETWEEN' and 'AND'.
        val exp1: SqlNode = SqlParserUtil.toTreeEx(list, opOrdinal + 1, 0, SqlKind.AND)
        if (opOrdinal + 2 >= list.size()) {
            val lastPos: SqlParserPos = list.pos(list.size() - 1)
            val line: Int = lastPos.getEndLineNum()
            val col: Int = lastPos.getEndColumnNum() + 1
            val errPos = SqlParserPos(line, col, line, col)
            throw SqlUtil.newContextException(errPos, RESOURCE.betweenWithoutAnd())
        }
        if (!list.isOp(opOrdinal + 2)
            || list.op(opOrdinal + 2).getKind() !== SqlKind.AND
        ) {
            val errPos: SqlParserPos = list.pos(opOrdinal + 2)
            throw SqlUtil.newContextException(errPos, RESOURCE.betweenWithoutAnd())
        }

        // Create the expression after 'AND', but stopping if we encounter an
        // operator of lower precedence.
        //
        // For example,
        //   a BETWEEN b AND c + d OR e
        // becomes
        //   (a BETWEEN b AND c + d) OR e
        // because OR has lower precedence than BETWEEN.
        val exp2: SqlNode = SqlParserUtil.toTreeEx(
            list,
            opOrdinal + 3,
            getRightPrec(),
            SqlKind.OTHER
        )

        // Create the call.
        val exp0: SqlNode = list.node(opOrdinal - 1)
        val newExp: SqlCall = createCall(
            list.pos(opOrdinal),
            exp0,
            exp1,
            exp2
        )

        // Replace all of the matched nodes with the single reduced node.
        return ReduceResult(opOrdinal - 1, opOrdinal + 4, newExp)
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Finds an AND operator in an expression.
     */
    private class AndFinder : SqlBasicVisitor<Void?>() {
        @Override
        fun visit(call: SqlCall): Void {
            val operator: SqlOperator = call.getOperator()
            if (operator === SqlStdOperatorTable.AND) {
                throw Util.FoundOne.NULL
            }
            return super.visit(call)
        }

        fun containsAnd(node: SqlNode): Boolean {
            return try {
                node.accept(this)
                false
            } catch (e: Util.FoundOne) {
                true
            }
        }
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        private val BETWEEN_NAMES = arrayOf("BETWEEN", "AND")
        private val NOT_BETWEEN_NAMES = arrayOf("NOT BETWEEN", "AND")

        /**
         * Ordinal of the 'value' operand.
         */
        const val VALUE_OPERAND = 0

        /**
         * Ordinal of the 'lower' operand.
         */
        const val LOWER_OPERAND = 1

        /**
         * Ordinal of the 'upper' operand.
         */
        const val UPPER_OPERAND = 2

        /**
         * Custom operand-type checking strategy.
         */
        private val OTC_CUSTOM: SqlOperandTypeChecker = ComparableOperandTypeChecker(
            3, RelDataTypeComparability.ALL,
            SqlOperandTypeChecker.Consistency.COMPARE
        )
        private val FRAME_TYPE: SqlWriter.FrameType = SqlWriter.FrameTypeEnum.create("BETWEEN")
        private fun of(negated: Boolean, symmetric: Boolean): SqlBetweenOperator {
            return if (symmetric) {
                if (negated) SqlStdOperatorTable.SYMMETRIC_BETWEEN else SqlStdOperatorTable.SYMMETRIC_NOT_BETWEEN
            } else {
                if (negated) SqlStdOperatorTable.NOT_BETWEEN else SqlStdOperatorTable.BETWEEN
            }
        }
    }
}
