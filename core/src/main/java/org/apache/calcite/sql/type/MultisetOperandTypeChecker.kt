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
 * Parameter type-checking strategy where types must be ([nullable] Multiset,
 * [nullable] Multiset), and the two types must have the same element type.
 *
 * @see MultisetSqlType.getComponentType
 */
class MultisetOperandTypeChecker : SqlOperandTypeChecker {
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
        val op0: SqlNode = callBinding.operand(0)
        if (!OperandTypes.MULTISET.checkSingleOperandType(
                callBinding,
                op0,
                0,
                throwOnFailure
            )
        ) {
            return false
        }
        val op1: SqlNode = callBinding.operand(1)
        if (!OperandTypes.MULTISET.checkSingleOperandType(
                callBinding,
                op1,
                0,
                throwOnFailure
            )
        ) {
            return false
        }

        // TODO: this won't work if element types are of ROW types and there is
        // a mismatch.
        val biggest: RelDataType = callBinding.getTypeFactory().leastRestrictive(
            ImmutableList.of(
                getComponentTypeOrThrow(SqlTypeUtil.deriveType(callBinding, op0)),
                getComponentTypeOrThrow(SqlTypeUtil.deriveType(callBinding, op1))
            )
        )
        if (null == biggest) {
            if (throwOnFailure) {
                throw callBinding.newError(
                    RESOURCE.typeNotComparable(
                        op0.getParserPosition().toString(),
                        op1.getParserPosition().toString()
                    )
                )
            }
            return false
        }
        return true
    }

    @get:Override
    override val operandCountRange: SqlOperandCountRange
        get() = SqlOperandCountRanges.of(2)

    @Override
    fun getAllowedSignatures(op: SqlOperator?, opName: String): String {
        return "<MULTISET> $opName <MULTISET>"
    }

    @get:Override
    override val consistency: org.apache.calcite.sql.type.SqlOperandTypeChecker.Consistency?
        get() = Consistency.NONE
}
