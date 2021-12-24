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
package org.apache.calcite.sql.type

import org.apache.calcite.rel.type.RelDataType

/**
 * Parameter type-checking strategy where all operand types except last one must be the same.
 */
class SameOperandTypeExceptLastOperandChecker     //~ Constructors -----------------------------------------------------------
    (
    nOperands: Int, //~ Instance fields --------------------------------------------------------
    protected val lastOperandTypeName: String
) : SameOperandTypeChecker(nOperands) {
    //~ Methods ----------------------------------------------------------------
    @Override
    protected override fun checkOperandTypesImpl(
        operatorBinding: SqlOperatorBinding,
        throwOnFailure: Boolean,
        @Nullable callBinding: SqlCallBinding?
    ): Boolean {
        if (throwOnFailure && callBinding == null) {
            throw IllegalArgumentException(
                "callBinding must be non-null in case throwOnFailure=true"
            )
        }
        var nOperandsActual: Int = nOperands
        if (nOperandsActual == -1) {
            nOperandsActual = operatorBinding.getOperandCount()
        }
        val types: Array<RelDataType?> = arrayOfNulls<RelDataType>(nOperandsActual)
        val operandList: List<Integer> = getOperandList(operatorBinding.getOperandCount())
        for (i in operandList) {
            if (operatorBinding.isOperandNull(i, false)) {
                if (requireNonNull(callBinding, "callBinding").isTypeCoercionEnabled()) {
                    types[i] = operatorBinding.getTypeFactory()
                        .createSqlType(SqlTypeName.NULL)
                } else return if (throwOnFailure) {
                    throw callBinding.getValidator().newValidationError(
                        callBinding.operand(i), RESOURCE.nullIllegal()
                    )
                } else {
                    false
                }
            } else {
                types[i] = operatorBinding.getOperandType(i)
            }
        }
        var prev = -1
        for (i in operandList) {
            if (prev >= 0 && i != operandList[operandList.size() - 1]) {
                if (!SqlTypeUtil.isComparable(types[i], types[prev])) {
                    if (!throwOnFailure) {
                        return false
                    }
                    throw requireNonNull(callBinding, "callBinding").newValidationError(
                        RESOURCE.needSameTypeParameter()
                    )
                }
            }
            prev = i
        }
        return true
    }

    @Override
    override fun getAllowedSignatures(op: SqlOperator?, opName: String?): String {
        val typeName: String = getTypeName()
        return if (nOperands === -1) {
            SqlUtil.getAliasedSignature(
                op, opName,
                ImmutableList.of(typeName, typeName, "...")
            )
        } else {
            val types: List<String> = Collections.nCopies(nOperands - 1, typeName)
            types.add(lastOperandTypeName)
            SqlUtil.getAliasedSignature(op, opName, types)
        }
    }
}
