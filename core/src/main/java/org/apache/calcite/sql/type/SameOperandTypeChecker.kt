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
 * Parameter type-checking strategy where all operand types must be the same.
 */
class SameOperandTypeChecker     //~ Constructors -----------------------------------------------------------
    (
    //~ Instance fields --------------------------------------------------------
    protected val nOperands: Int
) : SqlSingleOperandTypeChecker {
    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val consistency: org.apache.calcite.sql.type.SqlOperandTypeChecker.Consistency?
        get() = Consistency.NONE

    @Override
    override fun isOptional(i: Int): Boolean {
        return false
    }

    @Override
    override fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        return checkOperandTypesImpl(
            callBinding,
            throwOnFailure,
            callBinding
        )
    }

    protected fun getOperandList(operandCount: Int): List<Integer> {
        return if (nOperands == -1) Util.range(0, operandCount) else Util.range(0, nOperands)
    }

    protected fun checkOperandTypesImpl(
        operatorBinding: SqlOperatorBinding,
        throwOnFailure: Boolean,
        @Nullable callBinding: SqlCallBinding?
    ): Boolean {
        if (throwOnFailure && callBinding == null) {
            throw IllegalArgumentException(
                "callBinding must be non-null in case throwOnFailure=true"
            )
        }
        var nOperandsActual = nOperands
        if (nOperandsActual == -1) {
            nOperandsActual = operatorBinding.getOperandCount()
        }
        val types: Array<RelDataType?> = arrayOfNulls<RelDataType>(nOperandsActual)
        val operandList: List<Integer> = getOperandList(operatorBinding.getOperandCount())
        for (i in operandList) {
            types[i] = operatorBinding.getOperandType(i)
        }
        var prev = -1
        for (i in operandList) {
            if (prev >= 0) {
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

    /**
     * Similar functionality to
     * [.checkOperandTypes], but not part of the
     * interface, and cannot throw an error.
     */
    override fun checkOperandTypes(
        operatorBinding: SqlOperatorBinding, callBinding: SqlCallBinding?
    ): Boolean {
        return checkOperandTypesImpl(operatorBinding, false, null)
    }

    // implement SqlOperandTypeChecker
    @get:Override
    override val operandCountRange: SqlOperandCountRange
        get() = if (nOperands == -1) {
            SqlOperandCountRanges.any()
        } else {
            SqlOperandCountRanges.of(nOperands)
        }

    @Override
    override fun getAllowedSignatures(op: SqlOperator?, opName: String?): String {
        val typeName = typeName
        return SqlUtil.getAliasedSignature(
            op, opName,
            if (nOperands == -1) ImmutableList.of(typeName, typeName, "...") else Collections.nCopies(
                nOperands,
                typeName
            )
        )
    }

    /** Override to change the behavior of
     * [.getAllowedSignatures].  */
    protected val typeName: String
        protected get() = "EQUIVALENT_TYPE"

    @Override
    override fun checkSingleOperandType(
        callBinding: SqlCallBinding?,
        operand: SqlNode?,
        iFormalOperand: Int,
        throwOnFailure: Boolean
    ): Boolean {
        throw UnsupportedOperationException() // TODO:
    }
}
