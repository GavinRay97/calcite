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
package org.apache.calcite.sql

import com.google.common.collect.ImmutableList

/**
 * SqlTumbleTableFunction implements an operator for tumbling.
 *
 *
 * It allows three parameters:
 *
 *
 *  1. a table
 *  1. a descriptor to provide a watermarked column name from the input table
 *  1. an interval parameter to specify the length of window size
 *
 */
class SqlTumbleTableFunction : SqlWindowTableFunction(SqlKind.TUMBLE.name(), OperandMetadataImpl()) {
    /** Operand type checker for TUMBLE.  */
    private class OperandMetadataImpl internal constructor() : AbstractOperandMetadata(
        ImmutableList.of(PARAM_DATA, PARAM_TIMECOL, PARAM_SIZE, PARAM_OFFSET),
        3
    ) {
        @Override
        fun checkOperandTypes(
            callBinding: SqlCallBinding?,
            throwOnFailure: Boolean
        ): Boolean {
            // There should only be three operands, and number of operands are checked before
            // this call.
            if (!checkTableAndDescriptorOperands(callBinding, 1)) {
                return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure)
            }
            if (!checkTimeColumnDescriptorOperand(callBinding, 1)) {
                return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure)
            }
            return if (!checkIntervalOperands(callBinding, 2)) {
                throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure)
            } else true
        }

        @Override
        fun getAllowedSignatures(op: SqlOperator?, opName: String): String {
            return (opName + "(TABLE table_name, DESCRIPTOR(timecol), datetime interval"
                    + "[, datetime interval])")
        }
    }
}
