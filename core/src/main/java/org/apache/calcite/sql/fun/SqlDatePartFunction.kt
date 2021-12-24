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

import org.apache.calcite.avatica.util.TimeUnit

/**
 * SqlDatePartFunction represents the SQL:1999 standard `YEAR`,
 * `QUARTER`, `MONTH` and `DAY` functions.
 */
class SqlDatePartFunction(name: String?, timeUnit: TimeUnit) : SqlFunction(
    name,
    SqlKind.OTHER,
    ReturnTypes.BIGINT_NULLABLE,
    InferTypes.FIRST_KNOWN,
    OperandTypes.DATETIME,
    SqlFunctionCategory.TIMEDATE
) {
    //~ Constructors -----------------------------------------------------------
    private val timeUnit: TimeUnit

    init {
        this.timeUnit = timeUnit
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun rewriteCall(validator: SqlValidator?, call: SqlCall): SqlNode {
        val operands: List<SqlNode> = call.getOperandList()
        val pos: SqlParserPos = call.getParserPosition()
        return SqlStdOperatorTable.EXTRACT.createCall(
            pos,
            SqlIntervalQualifier(timeUnit, null, SqlParserPos.ZERO),
            operands[0]
        )
    }

    @get:Override
    val operandCountRange: SqlOperandCountRange
        get() = SqlOperandCountRanges.of(1)

    @Override
    fun getSignatureTemplate(operandsCount: Int): String {
        assert(1 == operandsCount)
        return "{0}({1})"
    }

    @Override
    fun checkOperandTypes(
        callBinding: SqlCallBinding?,
        throwOnFailure: Boolean
    ): Boolean {
        // Use #checkOperandTypes instead of #checkSingleOperandType to enable implicit
        // type coercion. REVIEW Danny 2019-09-10, because we declare that the operand
        // type family is DATETIME, that means it allows arguments of type DATE, TIME
        // or TIMESTAMP, so actually we can not figure out which type we want precisely.
        // For example, the YEAR(date) function, it actually allows a DATE/TIMESTAMP operand,
        // but we declare the required operand type family to be DATETIME.
        // We just need some refactoring for the SqlDatePartFunction.
        return OperandTypes.DATETIME.checkOperandTypes(callBinding, throwOnFailure)
    }
}
