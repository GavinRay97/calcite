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
 * `Sum` is an aggregator which returns the sum of the values which
 * go into it. It has precisely one argument of numeric type (`int`,
 * `long`, `float`, `double`), and the result
 * is the same type.
 */
class SqlSumAggFunction(type: RelDataType) : SqlAggFunction(
    "SUM",
    null,
    SqlKind.SUM,
    ReturnTypes.AGG_SUM,
    null,
    OperandTypes.NUMERIC,
    SqlFunctionCategory.NUMERIC,
    false,
    false,
    Optionality.FORBIDDEN
) {
    //~ Instance fields --------------------------------------------------------
    @Deprecated // to be removed before 2.0
    private val type: RelDataType

    //~ Constructors -----------------------------------------------------------
    init {
        this.type = type
    }

    //~ Methods ----------------------------------------------------------------
    @SuppressWarnings("deprecation")
    @Override
    fun getParameterTypes(typeFactory: RelDataTypeFactory?): List<RelDataType> {
        return ImmutableList.of(type)
    }

    @Deprecated // to be removed before 2.0
    fun getType(): RelDataType {
        return type
    }

    @SuppressWarnings("deprecation")
    @Override
    fun getReturnType(typeFactory: RelDataTypeFactory?): RelDataType {
        return type
    }

    @Override
    fun <T : Object?> unwrap(clazz: Class<T>): @Nullable T? {
        return if (clazz === SqlSplittableAggFunction::class.java) {
            clazz.cast(SqlSplittableAggFunction.SumSplitter.INSTANCE)
        } else super.unwrap(clazz)
    }

    @get:Override
    val rollup: SqlAggFunction
        get() = this
}
