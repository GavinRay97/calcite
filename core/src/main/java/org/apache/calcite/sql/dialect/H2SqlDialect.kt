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
package org.apache.calcite.sql.dialect

import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.sql.SqlDialect

/**
 * A `SqlDialect` implementation for the H2 database.
 */
class H2SqlDialect
/** Creates an H2SqlDialect.  */
    (context: Context?) : SqlDialect(context) {
    @Override
    fun supportsCharSet(): Boolean {
        return false
    }

    @Override
    fun supportsWindowFunctions(): Boolean {
        return false
    }

    @Override
    fun supportsJoinType(joinType: JoinRelType): Boolean {
        return joinType !== JoinRelType.FULL
    }

    companion object {
        val DEFAULT_CONTEXT: Context = SqlDialect.EMPTY_CONTEXT
            .withDatabaseProduct(SqlDialect.DatabaseProduct.H2)
            .withIdentifierQuoteString("\"")
        val DEFAULT: SqlDialect = H2SqlDialect(DEFAULT_CONTEXT)
    }
}
