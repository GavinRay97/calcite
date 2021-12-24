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
 * `LEAD` and `LAG` aggregate functions
 * return the value of given expression evaluated at given offset.
 */
class SqlLeadLagAggFunction(kind: SqlKind) : SqlAggFunction(
    kind.name(),
    null,
    kind,
    RETURN_TYPE,
    null,
    OPERAND_TYPES,
    SqlFunctionCategory.NUMERIC,
    false,
    true,
    Optionality.FORBIDDEN
) {
    init {
        Preconditions.checkArgument(
            kind === SqlKind.LEAD
                    || kind === SqlKind.LAG
        )
    }

    @Deprecated // to be removed before 2.0
    constructor(isLead: Boolean) : this(if (isLead) SqlKind.LEAD else SqlKind.LAG) {
    }

    @Override
    fun allowsFraming(): Boolean {
        return false
    }

    @Override
    fun allowsNullTreatment(): Boolean {
        return true
    }

    companion object {
        private val OPERAND_TYPES: SqlSingleOperandTypeChecker = OperandTypes.or(
            OperandTypes.ANY,
            OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC),
            OperandTypes.and(
                OperandTypes.family(
                    SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC,
                    SqlTypeFamily.ANY
                ),  // Arguments 1 and 3 must have same type
                object : SameOperandTypeChecker(3) {
                    @Override
                    protected fun getOperandList(operandCount: Int): List<Integer> {
                        return ImmutableList.of(0, 2)
                    }
                })
        )
        private val RETURN_TYPE: SqlReturnTypeInference =
            ReturnTypes.ARG0.andThen { binding: SqlOperatorBinding, type: RelDataType -> transformType(binding, type) }

        // Result is NOT NULL if NOT NULL default value is provided
        private fun transformType(
            binding: SqlOperatorBinding,
            type: RelDataType
        ): RelDataType {
            val transform: SqlTypeTransform = if (binding.getOperandCount() < 3 || binding.getOperandType(2)
                    .isNullable()
            ) SqlTypeTransforms.FORCE_NULLABLE else SqlTypeTransforms.TO_NOT_NULLABLE
            return transform.transformType(binding, type)
        }
    }
}
