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
 * The item operator `[ ... ]`, used to access a given element of an
 * array, map or struct. For example, `myArray[3]`, `"myMap['foo']"`,
 * `myStruct[2]` or `myStruct['fieldName']`.
 */
internal class SqlItemOperator : SqlSpecialOperator("ITEM", SqlKind.ITEM, 100, true, null, null, null) {
    @Override
    fun reduceExpr(
        ordinal: Int,
        list: TokenSequence
    ): ReduceResult {
        val left: SqlNode = list.node(ordinal - 1)
        val right: SqlNode = list.node(ordinal + 1)
        return ReduceResult(
            ordinal - 1,
            ordinal + 2,
            createCall(
                SqlParserPos.sum(
                    Arrays.asList(
                        requireNonNull(left, "left").getParserPosition(),
                        requireNonNull(right, "right").getParserPosition(),
                        list.pos(ordinal)
                    )
                ),
                left,
                right
            )
        )
    }

    @Override
    fun unparse(
        writer: SqlWriter, call: SqlCall, leftPrec: Int, rightPrec: Int
    ) {
        call.operand(0).unparse(writer, leftPrec, 0)
        val frame: SqlWriter.Frame = writer.startList("[", "]")
        call.operand(1).unparse(writer, 0, 0)
        writer.endList(frame)
    }

    @get:Override
    val operandCountRange: SqlOperandCountRange
        get() = SqlOperandCountRanges.of(2)

    @Override
    fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        val left: SqlNode = callBinding.operand(0)
        val right: SqlNode = callBinding.operand(1)
        if (!ARRAY_OR_MAP.checkSingleOperandType(
                callBinding, left, 0,
                throwOnFailure
            )
        ) {
            return false
        }
        val checker: SqlSingleOperandTypeChecker = getChecker(callBinding)
        return checker.checkSingleOperandType(
            callBinding, right, 0,
            throwOnFailure
        )
    }

    @Override
    fun getAllowedSignatures(name: String?): String {
        return """
            <ARRAY>[<INTEGER>]
            <MAP>[<ANY>]
            <ROW>[<CHARACTER>|<INTEGER>]
            """.trimIndent()
    }

    @Override
    fun inferReturnType(opBinding: SqlOperatorBinding): RelDataType? {
        val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
        val operandType: RelDataType = opBinding.getOperandType(0)
        return when (operandType.getSqlTypeName()) {
            ARRAY -> typeFactory.createTypeWithNullability(
                getComponentTypeOrThrow(operandType), true
            )
            MAP -> typeFactory.createTypeWithNullability(
                requireNonNull(
                    operandType.getValueType()
                ) { "operandType.getValueType() is null for $operandType" },
                true
            )
            ROW -> {
                var fieldType: RelDataType
                val indexType: RelDataType = opBinding.getOperandType(1)
                fieldType = if (SqlTypeUtil.isString(indexType)) {
                    val fieldName: String = getOperandLiteralValueOrThrow(opBinding, 1, String::class.java)
                    val field: RelDataTypeField = operandType.getField(fieldName, false, false)
                    if (field == null) {
                        throw AssertionError(
                            "Cannot infer type of field '"
                                    + fieldName + "' within ROW type: " + operandType
                        )
                    } else {
                        field.getType()
                    }
                } else if (SqlTypeUtil.isIntType(indexType)) {
                    val index: Integer = opBinding.getOperandLiteralValue(1, Integer::class.java)
                    if (index == null || index < 1 || index > operandType.getFieldCount()) {
                        throw AssertionError(
                            "Cannot infer type of field at position "
                                    + index + " within ROW type: " + operandType
                        )
                    } else {
                        operandType.getFieldList().get(index - 1).getType() // 1 indexed
                    }
                } else {
                    throw AssertionError(
                        "Unsupported field identifier type: '"
                                + indexType + "'"
                    )
                }
                if (fieldType != null && operandType.isNullable()) {
                    fieldType = typeFactory.createTypeWithNullability(fieldType, true)
                }
                fieldType
            }
            ANY, DYNAMIC_STAR -> typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.ANY), true
            )
            else -> throw AssertionError()
        }
    }

    companion object {
        private val ARRAY_OR_MAP: SqlSingleOperandTypeChecker = OperandTypes.or(
            OperandTypes.family(SqlTypeFamily.ARRAY),
            OperandTypes.family(SqlTypeFamily.MAP),
            OperandTypes.family(SqlTypeFamily.ANY)
        )

        private fun getChecker(callBinding: SqlCallBinding): SqlSingleOperandTypeChecker {
            val operandType: RelDataType = callBinding.getOperandType(0)
            return when (operandType.getSqlTypeName()) {
                ARRAY -> OperandTypes.family(SqlTypeFamily.INTEGER)
                MAP -> {
                    val keyType: RelDataType = requireNonNull(
                        operandType.getKeyType(),
                        "operandType.getKeyType()"
                    )
                    val sqlTypeName: SqlTypeName = keyType.getSqlTypeName()
                    OperandTypes.family(
                        requireNonNull(
                            sqlTypeName.getFamily()
                        ) { "keyType.getSqlTypeName().getFamily() null, type is $sqlTypeName" })
                }
                ROW, ANY, DYNAMIC_STAR -> OperandTypes.or(
                    OperandTypes.family(SqlTypeFamily.INTEGER),
                    OperandTypes.family(SqlTypeFamily.CHARACTER)
                )
                else -> throw callBinding.newValidationSignatureError()
            }
        }
    }
}
