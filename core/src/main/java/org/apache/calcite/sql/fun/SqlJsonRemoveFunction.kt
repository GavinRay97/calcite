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

import org.apache.calcite.sql.SqlCallBinding

/**
 * The `JSON_REMOVE` function.
 */
class SqlJsonRemoveFunction : SqlFunction(
    "JSON_REMOVE", SqlKind.OTHER_FUNCTION,
    ReturnTypes.VARCHAR_2000.andThen(SqlTypeTransforms.FORCE_NULLABLE),
    null, null, SqlFunctionCategory.SYSTEM
) {
    @get:Override
    val operandCountRange: SqlOperandCountRange
        get() = SqlOperandCountRanges.from(2)

    @Override
    fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        val operandCount: Int = callBinding.getOperandCount()
        assert(operandCount >= 2)
        if (!OperandTypes.ANY.checkSingleOperandType(
                callBinding, callBinding.operand(0), 0, throwOnFailure
            )
        ) {
            return false
        }
        val families: Array<SqlTypeFamily?> = arrayOfNulls<SqlTypeFamily>(operandCount)
        families[0] = SqlTypeFamily.ANY
        for (i in 1 until operandCount) {
            families[i] = SqlTypeFamily.CHARACTER
        }
        return OperandTypes.family(families).checkOperandTypes(callBinding, throwOnFailure)
    }

    @Override
    fun getAllowedSignatures(opNameToUse: String?): String {
        return String.format(
            Locale.ROOT, "'%s(<%s>, <%s>, <%s>...)'", getName(), SqlTypeFamily.ANY,
            SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER
        )
    }
}
