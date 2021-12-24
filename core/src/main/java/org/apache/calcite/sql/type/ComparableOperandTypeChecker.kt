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
 * Type checking strategy which verifies that types have the required attributes
 * to be used as arguments to comparison operators.
 */
class ComparableOperandTypeChecker(
    nOperands: Int,
    requiredComparability: RelDataTypeComparability, consistency: Consistency?
) : SameOperandTypeChecker(nOperands) {
    //~ Instance fields --------------------------------------------------------
    private val requiredComparability: RelDataTypeComparability
    private override val consistency: Consistency

    //~ Constructors -----------------------------------------------------------
    @Deprecated // to be removed before 2.0
    constructor(
        nOperands: Int,
        requiredComparability: RelDataTypeComparability
    ) : this(nOperands, requiredComparability, Consistency.NONE) {
    }

    init {
        this.requiredComparability = requiredComparability
        this.consistency = Objects.requireNonNull(consistency, "consistency")
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        var b = true
        for (i in 0 until nOperands) {
            val type: RelDataType = callBinding.getOperandType(i)
            if (!checkType(callBinding, throwOnFailure, type)) {
                b = false
                break
            }
        }
        if (b) {
            // Coerce type first.
            if (callBinding.isTypeCoercionEnabled()) {
                val typeCoercion: TypeCoercion = callBinding.getValidator().getTypeCoercion()
                // For comparison operators, i.e. >, <, =, >=, <=.
                typeCoercion.binaryComparisonCoercion(callBinding)
            }
            b = super.checkOperandTypes(callBinding, false)
        }
        if (!b && throwOnFailure) {
            throw callBinding.newValidationSignatureError()
        }
        return b
    }

    private fun checkType(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean,
        type: RelDataType
    ): Boolean {
        return if (type.getComparability().ordinal()
            < requiredComparability.ordinal()
        ) {
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError()
            } else {
                false
            }
        } else {
            true
        }
    }

    /**
     * Similar functionality to
     * [.checkOperandTypes], but not part of the
     * interface, and cannot throw an error.
     */
    @Override
    override fun checkOperandTypes(
        operatorBinding: SqlOperatorBinding?, callBinding: SqlCallBinding
    ): Boolean {
        var b = true
        for (i in 0 until nOperands) {
            val type: RelDataType = callBinding.getOperandType(i)
            if (type.getComparability().ordinal() < requiredComparability.ordinal()) {
                b = false
                break
            }
        }
        if (b) {
            b = super.checkOperandTypes(operatorBinding, callBinding)
        }
        return b
    }

    @get:Override
    protected override val typeName: String
        protected get() = "COMPARABLE_TYPE"

    @Override
    fun getConsistency(): Consistency {
        return consistency
    }
}
