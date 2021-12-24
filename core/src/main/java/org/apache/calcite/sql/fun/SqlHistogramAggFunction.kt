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
 * `HISTOGRAM` is the base operator that supports the Histogram
 * MIN/MAX aggregate functions. It returns the sum of the values which go
 * into it. It has precisely one argument of numeric type (`int`,
 * `long`, `float`, `double`); results are
 * retrieved using (`HistogramMin`) and (`HistogramMax`).
 */
class SqlHistogramAggFunction(type: RelDataType) : SqlAggFunction(
    "\$HISTOGRAM",
    null,
    SqlKind.OTHER_FUNCTION,
    ReturnTypes.HISTOGRAM,
    null,
    OperandTypes.NUMERIC_OR_STRING,
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
}
