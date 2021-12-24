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
 * The `JSON_OBJECT` function.
 */
class SqlJsonObjectFunction : SqlFunction("JSON_OBJECT", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_2000,
    { callBinding, returnType, operandTypes ->
        val typeFactory: RelDataTypeFactory = callBinding.getTypeFactory()
        for (i in 0 until operandTypes.length) {
            operandTypes.get(i) =
                if (i == 0) typeFactory.createSqlType(SqlTypeName.SYMBOL) else if (i % 2 == 1) typeFactory.createSqlType(
                    SqlTypeName.VARCHAR
                ) else typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.ANY), true
                )
        }
    }, null, SqlFunctionCategory.SYSTEM
) {
    @get:Override
    val operandCountRange: SqlOperandCountRange
        get() = SqlOperandCountRanges.from(1)

    @Override
    protected fun checkOperandCount(
        validator: SqlValidator?,
        @Nullable argType: SqlOperandTypeChecker?, call: SqlCall
    ) {
        assert(call.operandCount() % 2 === 1)
    }

    @Override
    fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        val count: Int = callBinding.getOperandCount()
        var i = 1
        while (i < count) {
            val nameType: RelDataType = callBinding.getOperandType(i)
            if (!SqlTypeUtil.inCharFamily(nameType)) {
                if (throwOnFailure) {
                    throw callBinding.newError(RESOURCE.expectedCharacter())
                }
                return false
            }
            if (nameType.isNullable()) {
                if (throwOnFailure) {
                    throw callBinding.newError(
                        RESOURCE.argumentMustNotBeNull(
                            callBinding.operand(i).toString()
                        )
                    )
                }
                return false
            }
            i += 2
        }
        return true
    }

    @Override
    fun createCall(
        @Nullable functionQualifier: SqlLiteral?,
        pos: SqlParserPos?, @Nullable vararg operands: SqlNode?
    ): SqlCall {
        if (operands[0] == null) {
            operands[0] = SqlLiteral.createSymbol(
                SqlJsonConstructorNullClause.NULL_ON_NULL, pos
            )
        }
        return super.createCall(functionQualifier, pos, operands)
    }

    @Override
    @Nullable
    fun getSignatureTemplate(operandsCount: Int): String {
        assert(operandsCount % 2 == 1)
        val sb = StringBuilder()
        sb.append("{0}(")
        for (i in 1 until operandsCount) {
            sb.append(String.format(Locale.ROOT, "{%d} ", i + 1))
        }
        sb.append("{1})")
        return sb.toString()
    }

    @Override
    fun unparse(
        writer: SqlWriter, call: SqlCall, leftPrec: Int,
        rightPrec: Int
    ) {
        assert(call.operandCount() % 2 === 1)
        val frame: SqlWriter.Frame = writer.startFunCall(getName())
        val listFrame: SqlWriter.Frame = writer.startList("", "")
        var i = 1
        while (i < call.operandCount()) {
            writer.sep(",")
            writer.keyword("KEY")
            call.operand(i).unparse(writer, leftPrec, rightPrec)
            writer.keyword("VALUE")
            call.operand(i + 1).unparse(writer, leftPrec, rightPrec)
            i += 2
        }
        writer.endList(listFrame)
        val nullClause: SqlJsonConstructorNullClause = getEnumValue(call.operand(0))
        writer.keyword(nullClause.sql)
        writer.endFunCall(frame)
    }

    companion object {
        @SuppressWarnings("unchecked")
        private fun <E : Enum<E>?> getEnumValue(operand: SqlNode): E {
            return requireNonNull((operand as SqlLiteral).getValue(), "operand.value") as E
        }
    }
}
