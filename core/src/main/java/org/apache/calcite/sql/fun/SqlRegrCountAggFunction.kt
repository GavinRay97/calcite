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

import org.apache.calcite.sql.SqlKind

/**
 * Definition of the SQL `REGR_COUNT` aggregation function.
 *
 *
 * `REGR_COUNT` is an aggregator which returns the number of rows which
 * have gone into it and both arguments are not `null`.
 */
class SqlRegrCountAggFunction(kind: SqlKind) : SqlCountAggFunction("REGR_COUNT", OperandTypes.NUMERIC_NUMERIC) {
    init {
        Preconditions.checkArgument(SqlKind.REGR_COUNT === kind, "unsupported sql kind: $kind")
    }
}
