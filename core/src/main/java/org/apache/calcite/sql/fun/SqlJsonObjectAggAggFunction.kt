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
class SqlJsonObjectAggAggFunction(
    kind: SqlKind,
    nullClause: SqlJsonConstructorNullClause
) : SqlAggFunction(
    kind.toString() + "_" + nullClause.name(), null, kind, ReturnTypes.VARCHAR_2000,
    { callBinding, returnType, operandTypes ->
        val typeFactory: RelDataTypeFactory = callBinding.getTypeFactory()
        operandTypes.get(0) = typeFactory.createSqlType(SqlTypeName.VARCHAR)
        operandTypes.get(1) = typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.ANY), true
        )
    }, OperandTypes.family(
        SqlTypeFamily.CHARACTER,
        SqlTypeFamily.ANY
    ),
    SqlFunctionCategory.SYSTEM, false, false, Optionality.FORBIDDEN
) {
    private val nullClause: SqlJsonConstructorNullClause

    /** Creates a SqlJsonObjectAggAggFunction.  */
    init {
        this.nullClause = Objects.requireNonNull(nullClause, "nullClause")
    }

    @Override
    fun unparse(
        writer: SqlWriter, call: SqlCall, leftPrec: Int,
        rightPrec: Int
    ) {
        assert(call.operandCount() === 2)
        val frame: SqlWriter.Frame = writer.startFunCall("JSON_OBJECTAGG")
        writer.keyword("KEY")
        call.operand(0).unparse(writer, leftPrec, rightPrec)
        writer.keyword("VALUE")
        call.operand(1).unparse(writer, leftPrec, rightPrec)
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

    fun with(nullClause: SqlJsonConstructorNullClause): SqlJsonObjectAggAggFunction {
        return if (this.nullClause === nullClause) this else SqlJsonObjectAggAggFunction(getKind(), nullClause)
    }

    fun getNullClause(): SqlJsonConstructorNullClause {
        return nullClause
    }
}
