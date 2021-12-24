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

import org.apache.calcite.sql.SqlFunctionCategory
/**
 * The `GROUP_ID()` function.
 *
 *
 * Accepts no arguments. The `GROUP_ID()` function distinguishes duplicate groups
 * resulting from a GROUP BY specification. It is useful in filtering out duplicate groupings
 * from the query result.
 *
 *
 * This function is not defined in the SQL standard; our implementation is
 * consistent with Oracle.
 *
 *
 * If n duplicates exist for a particular grouping, then `GROUP_ID()` function returns
 * numbers in the range 0 to n-1.
 *
 *
 * Some examples are in `agg.iq`.
 */
internal class SqlGroupIdFunction : SqlAbstractGroupFunction(
    "GROUP_ID", SqlKind.GROUP_ID, ReturnTypes.BIGINT, null,
    OperandTypes.NILADIC, SqlFunctionCategory.SYSTEM
)
