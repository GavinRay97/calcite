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
 * `FIRST_VALUE` and `LAST_VALUE` aggregate functions
 * return the first or the last value in a list of values that are input to the
 * function.
 */
class SqlFirstLastValueAggFunction(kind: SqlKind) : SqlAggFunction(
    kind.name(),
    null,
    kind,
    ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
    null,
    OperandTypes.ANY,
    SqlFunctionCategory.NUMERIC,
    false,
    true,
    Optionality.FORBIDDEN
) {
    //~ Constructors -----------------------------------------------------------
    init {
        Preconditions.checkArgument(
            kind === SqlKind.FIRST_VALUE
                    || kind === SqlKind.LAST_VALUE
        )
    }

    @Deprecated // to be removed before 2.0
    constructor(firstFlag: Boolean) : this(if (firstFlag) SqlKind.FIRST_VALUE else SqlKind.LAST_VALUE) {
    }

    //~ Methods ----------------------------------------------------------------
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
        return typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.ANY), true
        )
    }

    @Override
    fun allowsNullTreatment(): Boolean {
        return true
    }
}
