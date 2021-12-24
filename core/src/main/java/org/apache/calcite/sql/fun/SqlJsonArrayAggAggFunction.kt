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
 * The `JSON_OBJECTAGG` aggregate function.
 */
class SqlJsonArrayAggAggFunction(
    kind: SqlKind,
    nullClause: SqlJsonConstructorNullClause
) : SqlAggFunction(
    kind.toString() + "_" + nullClause.name(), null, kind, ReturnTypes.VARCHAR_2000,
    InferTypes.ANY_NULLABLE, OperandTypes.family(SqlTypeFamily.ANY),
    SqlFunctionCategory.SYSTEM, false, false, Optionality.OPTIONAL
) {
    private val nullClause: SqlJsonConstructorNullClause

    init {
        this.nullClause = Objects.requireNonNull(nullClause, "nullClause")
    }

    @Override
    fun unparse(
        writer: SqlWriter, call: SqlCall, leftPrec: Int,
        rightPrec: Int
    ) {
        assert(call.operandCount() === 1)
        val frame: SqlWriter.Frame = writer.startFunCall("JSON_ARRAYAGG")
        call.operand(0).unparse(writer, leftPrec, rightPrec)
        writer.keyword(nullClause.sql)
        writer.endFunCall(frame)
    }

    @Override
    fun deriveType(
        validator: SqlValidator,
        scope: SqlValidatorScope?, call: SqlCall
    ): RelDataType {
        // To prevent operator rewriting by SqlFunction#deriveType.
        for (operand in call.getOperandList()) {
            val nodeType: RelDataType = validator.deriveType(scope, operand)
            (validator as SqlValidatorImpl).setValidatedNodeType(operand, nodeType)
        }
        return validateOperands(validator, scope, call)
    }

    @Override
    fun createCall(
        @Nullable functionQualifier: SqlLiteral,
        pos: SqlParserPos, @Nullable vararg operands: SqlNode?
    ): SqlCall {
        assert(operands.size == 1 || operands.size == 2)
        val valueExpr: SqlNode? = operands[0]
        if (operands.size == 2) {
            val orderList: SqlNode? = operands[1]
            if (orderList != null) {
                // call has an order by clause, e.g. json_arrayagg(col_1 order by col_1)
                return SqlStdOperatorTable.WITHIN_GROUP.createCall(
                    SqlParserPos.ZERO,
                    createCall_(functionQualifier, pos, valueExpr), orderList
                )
            }
        }
        return createCall_(functionQualifier, pos, valueExpr)
    }

    private fun createCall_(
        @Nullable functionQualifier: SqlLiteral, pos: SqlParserPos,
        @Nullable valueExpr: SqlNode?
    ): SqlCall {
        return super.createCall(functionQualifier, pos, valueExpr)
    }

    fun with(nullClause: SqlJsonConstructorNullClause): SqlJsonArrayAggAggFunction {
        return if (this.nullClause === nullClause) this else SqlJsonArrayAggAggFunction(getKind(), nullClause)
    }

    fun getNullClause(): SqlJsonConstructorNullClause {
        return nullClause
    }
}
