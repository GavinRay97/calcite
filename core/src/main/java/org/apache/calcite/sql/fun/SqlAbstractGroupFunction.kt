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

import org.apache.calcite.sql.SqlAggFunction

/**
 * Base class for grouping functions `GROUP_ID`, `GROUPING_ID`,
 * `GROUPING`.
 */
class SqlAbstractGroupFunction
/**
 * Creates a SqlAbstractGroupFunction.
 *
 * @param name                 Name of builtin function
 * @param kind                 kind of operator implemented by function
 * @param returnTypeInference  strategy to use for return type inference
 * @param operandTypeInference strategy to use for parameter type inference
 * @param operandTypeChecker   strategy to use for parameter type checking
 * @param category             categorization for function
 */
    (
    name: String?,
    kind: SqlKind?,
    returnTypeInference: SqlReturnTypeInference?,
    @Nullable operandTypeInference: SqlOperandTypeInference?,
    operandTypeChecker: SqlOperandTypeChecker?,
    category: SqlFunctionCategory?
) : SqlAggFunction(
    name, null, kind, returnTypeInference, operandTypeInference,
    operandTypeChecker, category, false, false, Optionality.FORBIDDEN
) {
    @Override
    fun validateCall(
        call: SqlCall, validator: SqlValidator,
        scope: SqlValidatorScope?, operandScope: SqlValidatorScope?
    ) {
        super.validateCall(call, validator, scope, operandScope)
        val selectScope: SelectScope = SqlValidatorUtil.getEnclosingSelectScope(scope)
        assert(selectScope != null)
        val select: SqlSelect = selectScope.getNode()
        if (!validator.isAggregate(select)) {
            throw validator.newValidationError(
                call,
                Static.RESOURCE.groupingInAggregate(getName())
            )
        }
        val aggregatingSelectScope: AggregatingSelectScope = SqlValidatorUtil.getEnclosingAggregateSelectScope(scope)
            ?: // We're probably in the GROUP BY clause
            throw validator.newValidationError(
                call,
                Static.RESOURCE.groupingInWrongClause(getName())
            )
        for (operand in call.getOperandList()) {
            operand = if (scope is OrderByScope) {
                validator.expandOrderExpr(select, operand)
            } else {
                validator.expand(operand, scope)
            }
            if (!aggregatingSelectScope.resolved.get().isGroupingExpr(operand)) {
                throw validator.newValidationError(
                    operand,
                    Static.RESOURCE.groupingArgument(getName())
                )
            }
        }
    }

    @get:Override
    val isQuantifierAllowed: Boolean
        get() = false

    @Override
    fun allowsFilter(): Boolean {
        return false
    }
}
