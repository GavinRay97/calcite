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
package org.apache.calcite.sql

import org.apache.calcite.rel.type.RelDataType

/**
 * An operator that applies a sort operation before rows are included in an aggregate function.
 *
 *
 * Operands are as follows:
 *
 *
 *  * 0: a call to an aggregate function ([SqlCall])
 *  * 1: order operation list
 *
 */
class SqlWithinGroupOperator : SqlBinaryOperator(
    "WITHIN GROUP", SqlKind.WITHIN_GROUP, 100, true, ReturnTypes.ARG0,
    null, OperandTypes.ANY_IGNORE
) {
    @Override
    fun unparse(writer: SqlWriter, call: SqlCall, leftPrec: Int, rightPrec: Int) {
        assert(call.operandCount() === 2)
        call.operand(0).unparse(writer, 0, 0)
        writer.keyword("WITHIN GROUP")
        val orderFrame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.ORDER_BY_LIST, "(", ")")
        writer.keyword("ORDER BY")
        call.operand(1).unparse(writer, 0, 0)
        writer.endList(orderFrame)
    }

    @Override
    fun validateCall(
        call: SqlCall,
        validator: SqlValidator,
        scope: SqlValidatorScope?,
        operandScope: SqlValidatorScope?
    ) {
        assert(call.getOperator() === this)
        assert(call.operandCount() === 2)
        val flat: SqlValidatorUtil.FlatAggregate = SqlValidatorUtil.flatten(call)
        if (!flat.aggregateCall.getOperator().isAggregator()) {
            throw validator.newValidationError(
                call,
                RESOURCE.withinGroupNotAllowed(
                    flat.aggregateCall.getOperator().getName()
                )
            )
        }
        for (order in Objects.requireNonNull(flat.orderList)) {
            Objects.requireNonNull(validator.deriveType(scope, order))
        }
        validator.validateAggregateParams(
            flat.aggregateCall, flat.filter,
            flat.distinctList, flat.orderList, scope
        )
    }

    @Override
    fun deriveType(
        validator: SqlValidator?,
        scope: SqlValidatorScope?,
        call: SqlCall?
    ): RelDataType {
        // Validate type of the inner aggregate call
        return validateOperands(validator, scope, call)
    }
}
