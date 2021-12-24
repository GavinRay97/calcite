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
 * The `OVERLAY` function.
 */
class SqlOverlayFunction  //~ Constructors -----------------------------------------------------------
    : SqlFunction(
    "OVERLAY",
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.DYADIC_STRING_SUM_PRECISION_NULLABLE_VARYING,
    null,
    OTC_CUSTOM,
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
        writer.sep("PLACING")
        call.operand(1).unparse(writer, leftPrec, rightPrec)
        writer.sep("FROM")
        call.operand(2).unparse(writer, leftPrec, rightPrec)
        if (4 == call.operandCount()) {
            writer.sep("FOR")
            call.operand(3).unparse(writer, leftPrec, rightPrec)
        }
        writer.endFunCall(frame)
    }

    @Override
    fun getSignatureTemplate(operandsCount: Int): String {
        return when (operandsCount) {
            3 -> "{0}({1} PLACING {2} FROM {3})"
            4 -> "{0}({1} PLACING {2} FROM {3} FOR {4})"
            else -> throw IllegalArgumentException("operandsCount shuld be 3 or 4, got $operandsCount")
        }
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        private val OTC_CUSTOM: SqlOperandTypeChecker = OperandTypes.or(
            OperandTypes.STRING_STRING_INTEGER,
            OperandTypes.STRING_STRING_INTEGER_INTEGER
        )
    }
}
