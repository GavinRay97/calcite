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
 * `Covar` is an aggregator which returns the Covariance of the
 * values which go into it. It has precisely two arguments of numeric type
 * (`int`, `long`, `float`, `
 * double`), and the result is the same type.
 */
class SqlCovarAggFunction(kind: SqlKind) : SqlAggFunction(
    kind.name(),
    null,
    kind,
    if (kind === SqlKind.REGR_COUNT) ReturnTypes.BIGINT else ReturnTypes.COVAR_REGR_FUNCTION,
    null,
    OperandTypes.NUMERIC_NUMERIC,
    SqlFunctionCategory.NUMERIC,
    false,
    false,
    Optionality.FORBIDDEN
) {
    //~ Instance fields --------------------------------------------------------
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a SqlCovarAggFunction.
     */
    init {
        Preconditions.checkArgument(
            SqlKind.COVAR_AVG_AGG_FUNCTIONS.contains(kind),
            "unsupported sql kind: $kind"
        )
    }

    @Deprecated // to be removed before 2.0
    constructor(type: RelDataType?, subtype: Subtype) : this(SqlKind.valueOf(subtype.name())) {
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Returns the specific function, e.g. COVAR_POP or COVAR_SAMP.
     *
     * @return Subtype
     */ // to be removed before 2.0
    @get:Deprecated
    val subtype: Subtype
        get() = Subtype.valueOf(kind.name())

    /**
     * Enum for defining specific types.
     */
    @Deprecated // to be removed before 2.0
    enum class Subtype {
        COVAR_POP, COVAR_SAMP, REGR_SXX, REGR_SYY
    }
}
