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

import org.apache.calcite.rel.type.RelDataType

/**
 * Multiset MEMBER OF. Checks to see if a element belongs to a multiset.<br></br>
 * Example:<br></br>
 * `'green' MEMBER OF MULTISET['red','almost green','blue']` returns
 * `false`.
 */
class SqlMultisetMemberOfOperator  //~ Constructors -----------------------------------------------------------
    : SqlBinaryOperator(
    "MEMBER OF",
    SqlKind.OTHER,
    30,
    true,
    ReturnTypes.BOOLEAN_NULLABLE,
    null,
    null
) {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        if (!OperandTypes.MULTISET.checkSingleOperandType(
                callBinding,
                callBinding.operand(1),
                0,
                throwOnFailure
            )
        ) {
            return false
        }
        val mt: MultisetSqlType = callBinding.getOperandType(1) as MultisetSqlType
        val t0: RelDataType = callBinding.getOperandType(0)
        val t1: RelDataType = mt.getComponentType()
        if (t0.getFamily() !== t1.getFamily()) {
            if (throwOnFailure) {
                throw callBinding.newValidationError(
                    RESOURCE.typeNotComparableNear(t0.toString(), t1.toString())
                )
            }
            return false
        }
        return true
    }

    @get:Override
    val operandCountRange: SqlOperandCountRange
        get() = SqlOperandCountRanges.of(2)
}
