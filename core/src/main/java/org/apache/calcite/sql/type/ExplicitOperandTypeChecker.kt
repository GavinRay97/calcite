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
 * Parameter type-checking strategy for Explicit Type.
 */
class ExplicitOperandTypeChecker(type: RelDataType?) : SqlOperandTypeChecker {
    //~ Methods ----------------------------------------------------------------
    private val type: RelDataType

    init {
        this.type = Objects.requireNonNull(type, "type")
    }

    @Override
    override fun isOptional(i: Int): Boolean {
        return false
    }

    @Override
    override fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        val families: List<SqlTypeFamily> = ArrayList()
        val fieldList: List<RelDataTypeField> = type.getFieldList()
        for (i in 0 until fieldList.size()) {
            val field: RelDataTypeField = fieldList[i]
            val sqlTypeName: SqlTypeName = field.getType().getSqlTypeName()
            if (sqlTypeName === SqlTypeName.ROW) {
                if (field.getType().equals(callBinding.getOperandType(i))) {
                    families.add(SqlTypeFamily.ANY)
                }
            } else {
                families.add(
                    requireNonNull(
                        sqlTypeName.getFamily()
                    ) { "keyType.getSqlTypeName().getFamily() null, type is $sqlTypeName" })
            }
        }
        return OperandTypes.family(families).checkOperandTypes(callBinding, throwOnFailure)
    }

    @get:Override
    override val operandCountRange: SqlOperandCountRange
        get() = SqlOperandCountRanges.of(type.getFieldCount())

    @Override
    fun getAllowedSignatures(op: SqlOperator?, opName: String): String {
        return "<TYPE> $opName <TYPE>"
    }

    @get:Override
    override val consistency: org.apache.calcite.sql.type.SqlOperandTypeChecker.Consistency?
        get() = Consistency.NONE
}
