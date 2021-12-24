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

import org.apache.calcite.sql.parser.SqlParserPos

/**
 * An operator that decides how to handle null input
 * (`RESPECT NULLS` and `IGNORE NULLS`).
 *
 *
 * Currently, only the windowed aggregate functions `FIRST_VALUE`,
 * `LAST_VALUE`, `LEAD` and `LAG` support it.
 *
 * @see SqlAggFunction.allowsNullTreatment
 */
class SqlNullTreatmentOperator(kind: SqlKind) :
    SqlSpecialOperator(kind.sql, kind, 20, true, ReturnTypes.ARG0, null, OperandTypes.ANY) {
    init {
        Preconditions.checkArgument(
            kind === SqlKind.RESPECT_NULLS
                    || kind === SqlKind.IGNORE_NULLS
        )
    }

    @Override
    override fun createCall(
        @Nullable functionQualifier: SqlLiteral?,
        pos: SqlParserPos?, @Nullable vararg operands: SqlNode?
    ): SqlCall {
        // As super.createCall, but don't union the positions
        return SqlBasicCall(
            this, ImmutableNullableList.copyOf(operands), pos,
            functionQualifier
        )
    }

    @Override
    override fun unparse(
        writer: SqlWriter, call: SqlCall, leftPrec: Int,
        rightPrec: Int
    ) {
        assert(call.operandCount() === 1)
        call.operand(0).unparse(writer, getLeftPrec(), getRightPrec())
        writer.keyword(getName())
    }

    @Override
    override fun validateCall(
        call: SqlCall,
        validator: SqlValidator,
        scope: SqlValidatorScope?,
        operandScope: SqlValidatorScope?
    ) {
        assert(call.getOperator() === this)
        assert(call.operandCount() === 1)
        val aggCall: SqlCall = call.operand(0)
        if (!aggCall.getOperator().isAggregator()
            || !(aggCall.getOperator() as SqlAggFunction).allowsNullTreatment()
        ) {
            throw validator.newValidationError(
                aggCall,
                RESOURCE.disallowsNullTreatment(aggCall.getOperator().getName())
            )
        }
    }
}
