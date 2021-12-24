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

import org.apache.calcite.rel.type.RelDataType

/**
 * SqlSessionTableFunction implements an operator for per-key sessionization. It allows
 * four parameters:
 *
 *
 *  1. table as data source
 *  1. a descriptor to provide a watermarked column name from the input table
 *  1. a descriptor to provide a column as key, on which sessionization will be applied,
 * optional
 *  1. an interval parameter to specify a inactive activity gap to break sessions
 *
 */
class SqlSessionTableFunction : SqlWindowTableFunction(SqlKind.SESSION.name(), OperandMetadataImpl()) {
    /** Operand type checker for SESSION.  */
    private class OperandMetadataImpl internal constructor() : AbstractOperandMetadata(
        ImmutableList.of(PARAM_DATA, PARAM_TIMECOL, PARAM_KEY, PARAM_SIZE),
        3
    ) {
        @Override
        fun checkOperandTypes(
            callBinding: SqlCallBinding,
            throwOnFailure: Boolean
        ): Boolean {
            if (!checkTableAndDescriptorOperands(callBinding, 1)) {
                return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure)
            }
            if (!checkTimeColumnDescriptorOperand(callBinding, 1)) {
                return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure)
            }
            val validator: SqlValidator = callBinding.getValidator()
            val operand2: SqlNode = callBinding.operand(2)
            val type2: RelDataType = validator.getValidatedNodeType(operand2)
            if (operand2.getKind() === SqlKind.DESCRIPTOR) {
                val operand0: SqlNode = callBinding.operand(0)
                val type: RelDataType = validator.getValidatedNodeType(operand0)
                validateColumnNames(
                    validator, type.getFieldNames(), (operand2 as SqlCall).getOperandList()
                )
            } else if (!SqlTypeUtil.isInterval(type2)) {
                return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure)
            }
            if (callBinding.getOperandCount() > 3) {
                val type3: RelDataType = validator.getValidatedNodeType(callBinding.operand(3))
                if (!SqlTypeUtil.isInterval(type3)) {
                    return throwValidationSignatureErrorOrReturnFalse(callBinding, throwOnFailure)
                }
            }
            return true
        }

        @Override
        fun getAllowedSignatures(op: SqlOperator?, opName: String): String {
            return (opName + "(TABLE table_name, DESCRIPTOR(timecol), "
                    + "DESCRIPTOR(key) optional, datetime interval)")
        }
    }
}
