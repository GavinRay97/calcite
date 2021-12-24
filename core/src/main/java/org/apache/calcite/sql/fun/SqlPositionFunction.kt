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
 * The `POSITION` function.
 */
class SqlPositionFunction : SqlFunction(
    "POSITION",
    SqlKind.POSITION,
    ReturnTypes.INTEGER_NULLABLE,
    null,
    OTC_CUSTOM,
    SqlFunctionCategory.NUMERIC
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
        writer.sep("IN")
        call.operand(1).unparse(writer, leftPrec, rightPrec)
        if (3 == call.operandCount()) {
            writer.sep("FROM")
            call.operand(2).unparse(writer, leftPrec, rightPrec)
        }
        writer.endFunCall(frame)
    }

    @Override
    fun getSignatureTemplate(operandsCount: Int): String {
        return when (operandsCount) {
            2 -> "{0}({1} IN {2})"
            3 -> "{0}({1} IN {2} FROM {3})"
            else -> throw AssertionError()
        }
    }

    @Override
    fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        // check that the two operands are of same type.
        return when (callBinding.getOperandCount()) {
            2 -> OperandTypes.SAME_SAME.checkOperandTypes(
                callBinding, throwOnFailure
            )
                    && super.checkOperandTypes(callBinding, throwOnFailure)
            3 -> OperandTypes.SAME_SAME_INTEGER.checkOperandTypes(
                callBinding, throwOnFailure
            )
                    && super.checkOperandTypes(callBinding, throwOnFailure)
            else -> throw AssertionError()
        }
    }

    companion object {
        //~ Constructors -----------------------------------------------------------
        // FIXME jvs 25-Jan-2009:  POSITION should verify that
        // params are all same character set, like OVERLAY does implicitly
        // as part of rtiDyadicStringSumPrecision
        private val OTC_CUSTOM: SqlOperandTypeChecker = OperandTypes.or(
            OperandTypes.STRING_SAME_SAME,
            OperandTypes.STRING_SAME_SAME_INTEGER
        )
    }
}
