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
 * Parameter type-checking strategy for a set operator (UNION, INTERSECT,
 * EXCEPT).
 *
 *
 * Both arguments must be records with the same number of fields, and the
 * fields must be union-compatible.
 */
class SetopOperandTypeChecker : SqlOperandTypeChecker {
    //~ Methods ----------------------------------------------------------------
    @Override
    override fun isOptional(i: Int): Boolean {
        return false
    }

    @Override
    override fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        assert(callBinding.getOperandCount() === 2) { "setops are binary (for now)" }
        val argTypes: Array<RelDataType?> = arrayOfNulls<RelDataType>(callBinding.getOperandCount())
        var colCount = -1
        val validator: SqlValidator = callBinding.getValidator()
        for (i in argTypes.indices) {
            argTypes[i] = callBinding.getOperandType(i)
            val argType: RelDataType? = argTypes[i]
            if (!argType.isStruct()) {
                return if (throwOnFailure) {
                    throw AssertionError("setop arg must be a struct")
                } else {
                    false
                }
            }

            // Each operand must have the same number of columns.
            val fields: List<RelDataTypeField> = argType.getFieldList()
            if (i == 0) {
                colCount = fields.size()
                continue
            }
            if (fields.size() !== colCount) {
                return if (throwOnFailure) {
                    var node: SqlNode = callBinding.operand(i)
                    if (node is SqlSelect) {
                        node = (node as SqlSelect).getSelectList()
                    }
                    throw validator.newValidationError(
                        requireNonNull(node, "node"),
                        RESOURCE.columnCountMismatchInSetop(
                            callBinding.getOperator().getName()
                        )
                    )
                } else {
                    false
                }
            }
        }

        // The columns must be pairwise union compatible. For each column
        // ordinal, form a 'slice' containing the types of the ordinal'th
        // column j.
        for (i in 0 until colCount) {
            val i2: Int = i

            // Get the ith column data types list for every record type fields pair.
            // For example,
            // for record type (f0: INT, f1: BIGINT, f2: VARCHAR)
            // and record type (f3: VARCHAR, f4: DECIMAL, f5: INT),
            // the list would be [[INT, VARCHAR], [BIGINT, DECIMAL], [VARCHAR, INT]].
            val columnIthTypes: List<RelDataType> = object : AbstractList<RelDataType?>() {
                @Override
                operator fun get(index: Int): RelDataType {
                    return argTypes[index].getFieldList().get(i2)
                        .getType()
                }

                @Override
                fun size(): Int {
                    return argTypes.size
                }
            }
            val type: RelDataType = callBinding.getTypeFactory().leastRestrictive(columnIthTypes)
            if (type == null) {
                var coerced = false
                if (callBinding.isTypeCoercionEnabled()) {
                    for (j in 0 until callBinding.getOperandCount()) {
                        val typeCoercion: TypeCoercion = validator.getTypeCoercion()
                        val widenType: RelDataType = typeCoercion.getWiderTypeFor(columnIthTypes, true)
                        if (null != widenType) {
                            coerced = (typeCoercion.rowTypeCoercion(
                                callBinding.getScope(),
                                callBinding.operand(j), i, widenType
                            )
                                    || coerced)
                        }
                    }
                }
                if (!coerced) {
                    return if (throwOnFailure) {
                        val field: SqlNode = SqlUtil.getSelectListItem(callBinding.operand(0), i)
                        throw validator.newValidationError(
                            field,
                            RESOURCE.columnTypeMismatchInSetop(
                                i + 1,  // 1-based
                                callBinding.getOperator().getName()
                            )
                        )
                    } else {
                        false
                    }
                }
            }
        }
        return true
    }

    @get:Override
    override val operandCountRange: SqlOperandCountRange
        get() = SqlOperandCountRanges.of(2)

    @Override
    fun getAllowedSignatures(op: SqlOperator?, opName: String): String {
        return "{0} $opName {1}"
    }

    @get:Override
    override val consistency: org.apache.calcite.sql.type.SqlOperandTypeChecker.Consistency?
        get() = Consistency.NONE
}
