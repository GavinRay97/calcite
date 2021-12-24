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

import org.apache.calcite.avatica.util.TimeUnitRange

/**
 * The SQL `EXTRACT` operator. Extracts a specified field value from
 * a DATETIME or an INTERVAL. E.g.<br></br>
 * `EXTRACT(HOUR FROM INTERVAL '364 23:59:59')` returns `
 * 23`
 */
class SqlExtractFunction  //~ Constructors -----------------------------------------------------------
// SQL2003, Part 2, Section 4.4.3 - extract returns a exact numeric
// TODO: Return type should be decimal for seconds
    : SqlFunction(
    "EXTRACT", SqlKind.EXTRACT, ReturnTypes.BIGINT_NULLABLE, null,
    OperandTypes.INTERVALINTERVAL_INTERVALDATETIME,
    SqlFunctionCategory.SYSTEM
) {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun getSignatureTemplate(operandsCount: Int): String {
        Util.discard(operandsCount)
        return "{0}({1} FROM {2})"
    }

    @Override
    fun unparse(
        writer: SqlWriter,
        call: SqlCall,
        leftPrec: Int,
        rightPrec: Int
    ) {
        val frame: SqlWriter.Frame = writer.startFunCall(getName())
        call.operand(0).unparse(writer, 0, 0)
        writer.sep("FROM")
        call.operand(1).unparse(writer, 0, 0)
        writer.endFunCall(frame)
    }

    @Override
    fun getMonotonicity(call: SqlOperatorBinding): SqlMonotonicity {
        val value: TimeUnitRange = getOperandLiteralValueOrThrow(call, 0, TimeUnitRange::class.java)
        return when (value) {
            YEAR -> call.getOperandMonotonicity(1).unstrict()
            else -> SqlMonotonicity.NOT_MONOTONIC
        }
    }
}
