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

import org.apache.calcite.linq4j.Ord

/**
 * Operand type-checking strategy which checks operands for inclusion in type
 * families.
 */
class FamilyOperandTypeChecker internal constructor(
    families: List<SqlTypeFamily?>?,
    optional: Predicate<Integer?>
) : SqlSingleOperandTypeChecker, ImplicitCastOperandTypeChecker {
    //~ Instance fields --------------------------------------------------------
    protected val families: ImmutableList<SqlTypeFamily>
    protected val optional: Predicate<Integer>
    //~ Constructors -----------------------------------------------------------
    /**
     * Package private. Create using [OperandTypes.family].
     */
    init {
        this.families = ImmutableList.copyOf(families)
        this.optional = optional
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun isOptional(i: Int): Boolean {
        return optional.test(i)
    }

    @Override
    override fun checkSingleOperandType(
        callBinding: SqlCallBinding,
        node: SqlNode?,
        iFormalOperand: Int,
        throwOnFailure: Boolean
    ): Boolean {
        val family: SqlTypeFamily = families.get(iFormalOperand)
        when (family) {
            ANY -> {
                val type: RelDataType = SqlTypeUtil.deriveType(callBinding, node)
                val typeName: SqlTypeName = type.getSqlTypeName()
                if (typeName === SqlTypeName.CURSOR) {
                    // We do not allow CURSOR operands, even for ANY
                    if (throwOnFailure) {
                        throw callBinding.newValidationSignatureError()
                    }
                    return false
                }
                // no need to check
                return true
            }
            IGNORE -> return true
            else -> {}
        }
        if (SqlUtil.isNullLiteral(node, false)) {
            return if (callBinding.isTypeCoercionEnabled()) {
                true
            } else if (throwOnFailure) {
                throw callBinding.getValidator().newValidationError(
                    node,
                    RESOURCE.nullIllegal()
                )
            } else {
                false
            }
        }
        val type: RelDataType = SqlTypeUtil.deriveType(callBinding, node)
        val typeName: SqlTypeName = type.getSqlTypeName()

        // Pass type checking for operators if it's of type 'ANY'.
        if (typeName.getFamily() === SqlTypeFamily.ANY) {
            return true
        }
        if (!family.getTypeNames().contains(typeName)) {
            if (throwOnFailure) {
                throw callBinding.newValidationSignatureError()
            }
            return false
        }
        return true
    }

    @Override
    override fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        if (families.size() !== callBinding.getOperandCount()) {
            // assume this is an inapplicable sub-rule of a composite rule;
            // don't throw
            return false
        }
        for (op in Ord.zip(callBinding.operands())) {
            if (!checkSingleOperandType(
                    callBinding,
                    op.e,
                    op.i,
                    false
                )
            ) {
                // try to coerce type if it is allowed.
                var coerced = false
                if (callBinding.isTypeCoercionEnabled()) {
                    val typeCoercion: TypeCoercion = callBinding.getValidator().getTypeCoercion()
                    val builder: ImmutableList.Builder<RelDataType> = ImmutableList.builder()
                    for (i in 0 until callBinding.getOperandCount()) {
                        builder.add(callBinding.getOperandType(i))
                    }
                    val dataTypes: ImmutableList<RelDataType> = builder.build()
                    coerced = typeCoercion.builtinFunctionCoercion(callBinding, dataTypes, families)
                }
                // re-validate the new nodes type.
                for (op1 in Ord.zip(callBinding.operands())) {
                    if (!checkSingleOperandType(
                            callBinding,
                            op1.e,
                            op1.i,
                            throwOnFailure
                        )
                    ) {
                        return false
                    }
                }
                return coerced
            }
        }
        return true
    }

    @Override
    override fun checkOperandTypesWithoutTypeCoercion(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        if (families.size() !== callBinding.getOperandCount()) {
            // assume this is an inapplicable sub-rule of a composite rule;
            // don't throw exception.
            return false
        }
        for (op in Ord.zip(callBinding.operands())) {
            if (!checkSingleOperandType(
                    callBinding,
                    op.e,
                    op.i,
                    throwOnFailure
                )
            ) {
                return false
            }
        }
        return true
    }

    @Override
    override fun getOperandSqlTypeFamily(iFormalOperand: Int): SqlTypeFamily {
        return families.get(iFormalOperand)
    }

    @get:Override
    override val operandCountRange: SqlOperandCountRange
        get() {
            val max: Int = families.size()
            var min = max
            while (min > 0 && optional.test(min - 1)) {
                --min
            }
            return SqlOperandCountRanges.between(min, max)
        }

    @Override
    override fun getAllowedSignatures(op: SqlOperator?, opName: String?): String {
        return SqlUtil.getAliasedSignature(op, opName, families)
    }

    @get:Override
    override val consistency: org.apache.calcite.sql.type.SqlOperandTypeChecker.Consistency?
        get() = Consistency.NONE
}
