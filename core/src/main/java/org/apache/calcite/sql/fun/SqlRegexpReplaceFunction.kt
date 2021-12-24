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
 * The REGEXP_REPLACE(source_string, pattern, replacement [, pos, occurrence, match_type])
 * searches for a regular expression pattern and replaces every occurrence of the pattern
 * with the specified string.
 */
class SqlRegexpReplaceFunction : SqlFunction(
    "REGEXP_REPLACE", SqlKind.OTHER_FUNCTION,
    ReturnTypes.explicit(SqlTypeName.VARCHAR)
        .andThen(SqlTypeTransforms.TO_NULLABLE),
    null, null, SqlFunctionCategory.STRING
) {
    @get:Override
    val operandCountRange: SqlOperandCountRange
        get() = SqlOperandCountRanges.between(3, 6)

    @Override
    fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        val operandCount: Int = callBinding.getOperandCount()
        assert(operandCount >= 3)
        if (operandCount == 3) {
            return OperandTypes.STRING_STRING_STRING
                .checkOperandTypes(callBinding, throwOnFailure)
        }
        val families: List<SqlTypeFamily> = ArrayList()
        families.add(SqlTypeFamily.STRING)
        families.add(SqlTypeFamily.STRING)
        families.add(SqlTypeFamily.STRING)
        for (i in 3 until operandCount) {
            if (i == 3) {
                families.add(SqlTypeFamily.INTEGER)
            }
            if (i == 4) {
                families.add(SqlTypeFamily.INTEGER)
            }
            if (i == 5) {
                families.add(SqlTypeFamily.STRING)
            }
        }
        return OperandTypes.family(families.toArray(arrayOfNulls<SqlTypeFamily>(0)))
            .checkOperandTypes(callBinding, throwOnFailure)
    }
}
