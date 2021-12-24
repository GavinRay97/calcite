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

import org.apache.calcite.config.CalciteSystemProperty

/**
 * Definition of the SQL `COUNT` aggregation function.
 *
 *
 * `COUNT` is an aggregator which returns the number of rows which
 * have gone into it. With one argument (or more), it returns the number of rows
 * for which that argument (or all) is not `null`.
 */
class SqlCountAggFunction(
    name: String?,
    sqlOperandTypeChecker: SqlOperandTypeChecker?
) : SqlAggFunction(
    name, null, SqlKind.COUNT, ReturnTypes.BIGINT, null,
    sqlOperandTypeChecker, SqlFunctionCategory.NUMERIC, false, false,
    Optionality.FORBIDDEN
) {
    //~ Constructors -----------------------------------------------------------
    constructor(name: String?) : this(
        name,
        if (CalciteSystemProperty.STRICT.value()) OperandTypes.ANY else OperandTypes.ONE_OR_MORE
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val syntax: SqlSyntax
        get() = SqlSyntax.FUNCTION_STAR

    @SuppressWarnings("deprecation")
    @Override
    fun getParameterTypes(typeFactory: RelDataTypeFactory): List<RelDataType> {
        return ImmutableList.of(
            typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.ANY), true
            )
        )
    }

    @SuppressWarnings("deprecation")
    @Override
    fun getReturnType(typeFactory: RelDataTypeFactory): RelDataType {
        return typeFactory.createSqlType(SqlTypeName.BIGINT)
    }

    @Override
    fun deriveType(
        validator: SqlValidator,
        scope: SqlValidatorScope?,
        call: SqlCall
    ): RelDataType {
        // Check for COUNT(*) function.  If it is we don't
        // want to try and derive the "*"
        return if (call.isCountStar()) {
            validator.getTypeFactory().createSqlType(
                SqlTypeName.BIGINT
            )
        } else super.deriveType(validator, scope, call)
    }

    @Override
    fun <T : Object?> unwrap(clazz: Class<T>): @Nullable T? {
        return if (clazz === SqlSplittableAggFunction::class.java) {
            clazz.cast(SqlSplittableAggFunction.CountSplitter.INSTANCE)
        } else super.unwrap(clazz)
    }

    @get:Override
    val rollup: SqlAggFunction
        get() = SqlStdOperatorTable.SUM0
}
