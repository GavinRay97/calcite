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
 * Common base for the `CONVERT` and `TRANSLATE`
 * functions.
 */
class SqlConvertFunction  //~ Constructors -----------------------------------------------------------
    (name: String?) : SqlFunction(
    name,
    SqlKind.OTHER_FUNCTION,
    null,
    null,
    null,
    SqlFunctionCategory.STRING
) {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun unparse(
        writer: SqlWriter,
        call: SqlCall,
        leftPrec: Int,
        rightPrec: Int
    ) {
        val frame: SqlWriter.Frame = writer.startFunCall(getName())
        call.operand(0).unparse(writer, leftPrec, rightPrec)
        writer.sep("USING")
        call.operand(1).unparse(writer, leftPrec, rightPrec)
        writer.endFunCall(frame)
    }

    @Override
    fun getSignatureTemplate(operandsCount: Int): String {
        when (operandsCount) {
            2 -> return "{0}({1} USING {2})"
            else -> {}
        }
        throw IllegalStateException("operandsCount should be 2, got $operandsCount")
    }
}
