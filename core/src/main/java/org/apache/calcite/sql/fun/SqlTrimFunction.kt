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

import org.apache.calcite.sql.SqlCall

/**
 * Definition of the "TRIM" builtin SQL function.
 */
class SqlTrimFunction  //~ Constructors -----------------------------------------------------------
    (
    name: String?, kind: SqlKind?,
    returnTypeInference: SqlReturnTypeInference?,
    operandTypeChecker: SqlSingleOperandTypeChecker?
) : SqlFunction(
    name, kind, returnTypeInference, null, operandTypeChecker,
    SqlFunctionCategory.STRING
) {
    //~ Enums ------------------------------------------------------------------
    /**
     * Defines the enumerated values "LEADING", "TRAILING", "BOTH".
     */
    enum class Flag(val left: Int, val right: Int) : Symbolizable {
        BOTH(1, 1), LEADING(1, 0), TRAILING(0, 1);

    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun unparse(
        writer: SqlWriter,
        call: SqlCall,
        leftPrec: Int,
        rightPrec: Int
    ) {
        val frame: SqlWriter.Frame = writer.startFunCall(getName())
        assert(call.operand(0) is SqlLiteral) { call.operand(0) }
        call.operand(0).unparse(writer, leftPrec, rightPrec)
        call.operand(1).unparse(writer, leftPrec, rightPrec)
        writer.sep("FROM")
        call.operand(2).unparse(writer, leftPrec, rightPrec)
        writer.endFunCall(frame)
    }

    @Override
    fun getSignatureTemplate(operandsCount: Int): String {
        return when (operandsCount) {
            3 -> "{0}([BOTH|LEADING|TRAILING] {1} FROM {2})"
            else -> throw AssertionError()
        }
    }

    @Override
    fun createCall(
        @Nullable functionQualifier: SqlLiteral?,
        pos: SqlParserPos?,
        @Nullable vararg operands: SqlNode?
    ): SqlCall {
        var operands: Array<out SqlNode?> = operands
        assert(functionQualifier == null)
        when (operands.size) {
            1 ->       // This variant occurs when someone writes TRIM(string)
                // as opposed to the sugared syntax TRIM(string FROM string).
                operands = arrayOf<SqlNode?>(
                    Flag.BOTH.symbol(SqlParserPos.ZERO),
                    SqlLiteral.createCharString(" ", pos),
                    operands[0]
                )
            3 -> {
                assert(
                    operands[0] is SqlLiteral
                            && (operands[0] as SqlLiteral?).getValue() is Flag
                )
                if (operands[1] == null) {
                    operands[1] = SqlLiteral.createCharString(" ", pos)
                }
            }
            else -> throw IllegalArgumentException(
                "invalid operand count " + Arrays.toString(operands)
            )
        }
        return super.createCall(functionQualifier, pos, operands)
    }

    @Override
    fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        return if (!super.checkOperandTypes(callBinding, throwOnFailure)) {
            false
        } else when (kind) {
            TRIM -> SqlTypeUtil.isCharTypeComparable(
                callBinding,
                ImmutableList.of(callBinding.operand(1), callBinding.operand(2)),
                throwOnFailure
            )
            else -> true
        }
    }

    companion object {
        val INSTANCE = SqlTrimFunction(
            "TRIM", SqlKind.TRIM,
            ReturnTypes.ARG2.andThen(SqlTypeTransforms.TO_NULLABLE)
                .andThen(SqlTypeTransforms.TO_VARYING),
            OperandTypes.and(
                OperandTypes.family(
                    SqlTypeFamily.ANY, SqlTypeFamily.STRING,
                    SqlTypeFamily.STRING
                ),  // Arguments 1 and 2 must have same type
                object : SameOperandTypeChecker(3) {
                    @Override
                    protected fun getOperandList(operandCount: Int): List<Integer> {
                        return ImmutableList.of(1, 2)
                    }
                })
        )
    }
}
