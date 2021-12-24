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

import org.apache.calcite.linq4j.Ord

/**
 * Definition of the "SUBSTRING" builtin SQL function.
 */
class SqlSubstringFunction  //~ Constructors -----------------------------------------------------------
/**
 * Creates the SqlSubstringFunction.
 */
internal constructor() : SqlFunction(
    "SUBSTRING",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.ARG0_NULLABLE_VARYING,
    null,
    null,
    SqlFunctionCategory.STRING
) {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun getSignatureTemplate(operandsCount: Int): String {
        return when (operandsCount) {
            2 -> "{0}({1} FROM {2})"
            3 -> "{0}({1} FROM {2} FOR {3})"
            else -> throw AssertionError()
        }
    }

    @Override
    fun getAllowedSignatures(opName: String?): String {
        val ret = StringBuilder()
        for (typeName in Ord.zip(SqlTypeName.STRING_TYPES)) {
            if (typeName.i > 0) {
                ret.append(NL)
            }
            ret.append(
                SqlUtil.getAliasedSignature(
                    this, opName,
                    ImmutableList.of(typeName.e, SqlTypeName.INTEGER)
                )
            )
            ret.append(NL)
            ret.append(
                SqlUtil.getAliasedSignature(
                    this, opName,
                    ImmutableList.of(
                        typeName.e, SqlTypeName.INTEGER,
                        SqlTypeName.INTEGER
                    )
                )
            )
        }
        return ret.toString()
    }

    @Override
    fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        var operands: List<SqlNode?> = callBinding.operands()
        val n: Int = operands.size()
        assert(3 == n || 2 == n)
        if (2 == n) {
            return OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC)
                .checkOperandTypes(callBinding, throwOnFailure)
        } else {
            val checker1: FamilyOperandTypeChecker = OperandTypes.STRING_STRING_STRING
            val checker2: FamilyOperandTypeChecker = OperandTypes.family(
                SqlTypeFamily.STRING,
                SqlTypeFamily.NUMERIC,
                SqlTypeFamily.NUMERIC
            )
            // Put the STRING_NUMERIC_NUMERIC checker first because almost every other type
            // can be coerced to STRING.
            if (!OperandTypes.or(checker2, checker1)
                    .checkOperandTypes(callBinding, throwOnFailure)
            ) {
                return false
            }
            // Reset the operands because they may be coerced during
            // implicit type coercion.
            operands = callBinding.getCall().getOperandList()
            val t1: RelDataType = callBinding.getOperandType(1)
            val t2: RelDataType = callBinding.getOperandType(2)
            if (SqlTypeUtil.inCharFamily(t1)) {
                if (!SqlTypeUtil.isCharTypeComparable(
                        callBinding, operands,
                        throwOnFailure
                    )
                ) {
                    return false
                }
            }
            if (!SqlTypeUtil.inSameFamily(t1, t2)) {
                if (throwOnFailure) {
                    throw callBinding.newValidationSignatureError()
                }
                return false
            }
        }
        return true
    }

    @get:Override
    val operandCountRange: SqlOperandCountRange
        get() = SqlOperandCountRanges.between(2, 3)

    @Override
    fun unparse(
        writer: SqlWriter,
        call: SqlCall,
        leftPrec: Int,
        rightPrec: Int
    ) {
        val frame: SqlWriter.Frame = writer.startFunCall(getName())
        call.operand(0).unparse(writer, leftPrec, rightPrec)
        writer.sep("FROM")
        call.operand(1).unparse(writer, leftPrec, rightPrec)
        if (3 == call.operandCount()) {
            writer.sep("FOR")
            call.operand(2).unparse(writer, leftPrec, rightPrec)
        }
        writer.endFunCall(frame)
    }

    @Override
    fun getMonotonicity(call: SqlOperatorBinding): SqlMonotonicity {
        // SUBSTRING(x FROM 0 FOR constant) has same monotonicity as x
        if (call.getOperandCount() === 3) {
            val mono0: SqlMonotonicity = call.getOperandMonotonicity(0)
            if (mono0 != null && mono0 !== SqlMonotonicity.NOT_MONOTONIC && call.getOperandMonotonicity(1) === SqlMonotonicity.CONSTANT && Objects.equals(
                    call.getOperandLiteralValue(1, BigDecimal::class.java),
                    BigDecimal.ZERO
                )
                && call.getOperandMonotonicity(2) === SqlMonotonicity.CONSTANT
            ) {
                return mono0.unstrict()
            }
        }
        return super.getMonotonicity(call)
    }
}
