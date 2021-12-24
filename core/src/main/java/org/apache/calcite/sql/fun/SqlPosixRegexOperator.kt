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
 * An operator describing the `~` operator.
 *
 *
 *  Syntax: `src-value [!] ~ [*] pattern-value`
 */
class SqlPosixRegexOperator internal constructor(
    name: String?,
    kind: SqlKind?,
    /**
     * Returns whether this operator matches the case of its operands.
     *
     * @return whether this operator matches the case of its operands
     */
    // ~ Instance fields --------------------------------------------------------
    val isCaseSensitive: Boolean,
    /**
     * Returns whether this is 'NOT' variant of an operator.
     *
     * @see .not
     */
    val isNegated: Boolean
) : SqlBinaryOperator(
    name,
    kind,
    32,
    true,
    ReturnTypes.BOOLEAN_NULLABLE,
    InferTypes.FIRST_KNOWN,
    OperandTypes.STRING_SAME_SAME_SAME
) {

    private val operatorString: String
    // ~ Constructors -----------------------------------------------------------
    /**
     * Creates a SqlPosixRegexOperator.
     *
     * @param name    Operator name
     * @param kind    Kind
     * @param negated Whether this is '!~' or '!~*'
     */
    init {
        val sb = StringBuilder(3)
        if (isNegated) {
            sb.append("!")
        }
        sb.append("~")
        if (!isCaseSensitive) {
            sb.append("*")
        }
        operatorString = sb.toString()
    }

    // ~ Methods ----------------------------------------------------------------
    @Override
    operator fun not(): SqlOperator {
        return of(!isNegated, isCaseSensitive)
    }

    @get:Override
    val operandCountRange: SqlOperandCountRange
        get() = SqlOperandCountRanges.of(2)

    @Override
    fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        val operandCount: Int = callBinding.getOperandCount()
        if (operandCount != 2) {
            throw AssertionError(
                "Unexpected number of args to " + callBinding.getCall().toString() + ": " + operandCount
            )
        }
        val op1Type: RelDataType = callBinding.getOperandType(0)
        val op2Type: RelDataType = callBinding.getOperandType(1)
        if (!SqlTypeUtil.isComparable(op1Type, op2Type)) {
            throw AssertionError(
                "Incompatible first two operand types $op1Type and $op2Type"
            )
        }
        return SqlTypeUtil.isCharTypeComparable(
            callBinding,
            callBinding.operands().subList(0, 2),
            throwOnFailure
        )
    }

    @Override
    fun unparse(
        writer: SqlWriter,
        call: SqlCall,
        leftPrec: Int,
        rightPrec: Int
    ) {
        val frame: SqlWriter.Frame = writer.startList("", "")
        call.operand(0).unparse(writer, getLeftPrec(), getRightPrec())
        writer.print(operatorString)
        writer.print(" ")
        call.operand(1).unparse(writer, getLeftPrec(), getRightPrec())
        writer.endList(frame)
    }

    companion object {
        private fun of(negated: Boolean, ignoreCase: Boolean): SqlOperator {
            return if (ignoreCase) {
                if (negated) SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_SENSITIVE else SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE
            } else {
                if (negated) SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_INSENSITIVE else SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE
            }
        }
    }
}
