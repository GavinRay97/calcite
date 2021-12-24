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
 * `Avg` is an aggregator which returns the average of the values
 * which go into it. It has precisely one argument of numeric type
 * (`int`, `long`, `float`, `
 * double`), and the result is the same type.
 */
class SqlAvgAggFunction internal constructor(name: String?, kind: SqlKind?) : SqlAggFunction(
    name,
    null,
    kind,
    ReturnTypes.AVG_AGG_FUNCTION,
    null,
    OperandTypes.NUMERIC,
    SqlFunctionCategory.NUMERIC,
    false,
    false,
    Optionality.FORBIDDEN
) {
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a SqlAvgAggFunction.
     */
    constructor(kind: SqlKind) : this(kind.name(), kind) {}

    init {
        Preconditions.checkArgument(
            SqlKind.AVG_AGG_FUNCTIONS.contains(kind),
            "unsupported sql kind"
        )
    }

    @Deprecated // to be removed before 2.0
    constructor(
        type: RelDataType?,
        subtype: Subtype
    ) : this(SqlKind.valueOf(subtype.name())) {
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Returns the specific function, e.g. AVG or STDDEV_POP.
     *
     * @return Subtype
     */ // to be removed before 2.0
    @get:Deprecated
    val subtype: Subtype
        get() = Subtype.valueOf(kind.name())

    /** Sub-type of aggregate function.  */
    @Deprecated // to be removed before 2.0
    enum class Subtype {
        AVG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP
    }
}
