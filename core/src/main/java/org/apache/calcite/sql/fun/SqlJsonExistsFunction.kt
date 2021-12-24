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
 * The `JSON_EXISTS` function.
 */
class SqlJsonExistsFunction : SqlFunction(
    "JSON_EXISTS", SqlKind.OTHER_FUNCTION,
    ReturnTypes.BOOLEAN.andThen(SqlTypeTransforms.FORCE_NULLABLE), null,
    OperandTypes.or(
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER),
        OperandTypes.family(
            SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER,
            SqlTypeFamily.ANY
        )
    ),
    SqlFunctionCategory.SYSTEM
) {
    @Override
    fun getAllowedSignatures(opNameToUse: String?): String {
        return "JSON_EXISTS(json_doc, path [{TRUE | FALSE| UNKNOWN | ERROR} ON ERROR])"
    }

    @Override
    fun unparse(
        writer: SqlWriter, call: SqlCall, leftPrec: Int,
        rightPrec: Int
    ) {
        val frame: SqlWriter.Frame = writer.startFunCall(getName())
        call.operand(0).unparse(writer, 0, 0)
        writer.sep(",", true)
        for (i in 1 until call.operandCount()) {
            call.operand(i).unparse(writer, leftPrec, rightPrec)
        }
        if (call.operandCount() === 3) {
            writer.keyword("ON ERROR")
        }
        writer.endFunCall(frame)
    }
}
