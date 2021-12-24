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
 * The `JSON_ARRAY` function.
 */
class SqlJsonArrayFunction : SqlFunction(
    "JSON_ARRAY", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_2000,
    InferTypes.ANY_NULLABLE, OperandTypes.VARIADIC, SqlFunctionCategory.SYSTEM
) {
    @get:Override
    val operandCountRange: SqlOperandCountRange
        get() = SqlOperandCountRanges.from(1)

    @Override
    protected fun checkOperandCount(
        validator: SqlValidator?,
        @Nullable argType: SqlOperandTypeChecker?, call: SqlCall
    ) {
        assert(call.operandCount() >= 1)
    }

    @Override
    fun createCall(
        @Nullable functionQualifier: SqlLiteral?,
        pos: SqlParserPos?, @Nullable vararg operands: SqlNode?
    ): SqlCall {
        if (operands[0] == null) {
            operands[0] = SqlLiteral.createSymbol(
                SqlJsonConstructorNullClause.ABSENT_ON_NULL,
                pos
            )
        }
        return super.createCall(functionQualifier, pos, operands)
    }

    @Override
    @Nullable
    fun getSignatureTemplate(operandsCount: Int): String {
        assert(operandsCount >= 1)
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
        assert(call.operandCount() >= 1)
        val frame: SqlWriter.Frame = writer.startFunCall(getName())
        val listFrame: SqlWriter.Frame = writer.startList("", "")
        for (i in 1 until call.operandCount()) {
            writer.sep(",")
            call.operand(i).unparse(writer, leftPrec, rightPrec)
        }
        writer.endList(listFrame)
        val nullClause: SqlJsonConstructorNullClause = getEnumValue(call.operand(0))
        when (nullClause) {
            ABSENT_ON_NULL -> writer.keyword("ABSENT ON NULL")
            NULL_ON_NULL -> writer.keyword("NULL ON NULL")
            else -> throw IllegalStateException("unreachable code")
        }
        writer.endFunCall(frame)
    }

    companion object {
        @SuppressWarnings("unchecked")
        private fun <E : Enum<E>?> getEnumValue(operand: SqlNode): E {
            return requireNonNull((operand as SqlLiteral).getValue(), "operand.value") as E
        }
    }
}
