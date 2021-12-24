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
 * The `JSON_QUERY` function.
 */
class SqlJsonQueryFunction : SqlFunction(
    "JSON_QUERY", SqlKind.OTHER_FUNCTION,
    ReturnTypes.VARCHAR_2000.andThen(SqlTypeTransforms.FORCE_NULLABLE),
    null,
    OperandTypes.family(
        SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER,
        SqlTypeFamily.ANY, SqlTypeFamily.ANY, SqlTypeFamily.ANY
    ),
    SqlFunctionCategory.SYSTEM
) {
    @Override
    @Nullable
    fun getSignatureTemplate(operandsCount: Int): String {
        return "{0}({1} {2} {3} WRAPPER {4} ON EMPTY {5} ON ERROR)"
    }

    @Override
    fun unparse(
        writer: SqlWriter, call: SqlCall, leftPrec: Int,
        rightPrec: Int
    ) {
        val frame: SqlWriter.Frame = writer.startFunCall(getName())
        call.operand(0).unparse(writer, 0, 0)
        writer.sep(",", true)
        call.operand(1).unparse(writer, 0, 0)
        val wrapperBehavior: SqlJsonQueryWrapperBehavior = getEnumValue(call.operand(2))
        when (wrapperBehavior) {
            WITHOUT_ARRAY -> writer.keyword("WITHOUT ARRAY")
            WITH_CONDITIONAL_ARRAY -> writer.keyword("WITH CONDITIONAL ARRAY")
            WITH_UNCONDITIONAL_ARRAY -> writer.keyword("WITH UNCONDITIONAL ARRAY")
            else -> throw IllegalStateException("unreachable code")
        }
        writer.keyword("WRAPPER")
        unparseEmptyOrErrorBehavior(writer, getEnumValue(call.operand(3)))
        writer.keyword("ON EMPTY")
        unparseEmptyOrErrorBehavior(writer, getEnumValue(call.operand(4)))
        writer.keyword("ON ERROR")
        writer.endFunCall(frame)
    }

    @Override
    fun createCall(
        @Nullable functionQualifier: SqlLiteral?,
        pos: SqlParserPos?, @Nullable vararg operands: SqlNode?
    ): SqlCall {
        if (operands[2] == null) {
            operands[2] = SqlLiteral.createSymbol(SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY, pos)
        }
        if (operands[3] == null) {
            operands[3] = SqlLiteral.createSymbol(SqlJsonQueryEmptyOrErrorBehavior.NULL, pos)
        }
        if (operands[4] == null) {
            operands[4] = SqlLiteral.createSymbol(SqlJsonQueryEmptyOrErrorBehavior.NULL, pos)
        }
        return super.createCall(functionQualifier, pos, operands)
    }

    companion object {
        private fun unparseEmptyOrErrorBehavior(
            writer: SqlWriter,
            emptyBehavior: SqlJsonQueryEmptyOrErrorBehavior
        ) {
            when (emptyBehavior) {
                NULL -> writer.keyword("NULL")
                ERROR -> writer.keyword("ERROR")
                EMPTY_ARRAY -> writer.keyword("EMPTY ARRAY")
                EMPTY_OBJECT -> writer.keyword("EMPTY OBJECT")
                else -> throw IllegalStateException("unreachable code")
            }
        }

        @SuppressWarnings("unchecked")
        private fun <E : Enum<E>?> getEnumValue(operand: SqlNode): E {
            return requireNonNull((operand as SqlLiteral).getValue(), "operand.value") as E
        }
    }
}
