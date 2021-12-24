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
package org.apache.calcite.sql.util

import org.apache.calcite.sql.SqlDialect
import com.google.common.collect.ImmutableList
import org.checkerframework.dataflow.qual.Pure

/**
 * String that represents a kocher SQL statement, expression, or fragment.
 *
 *
 * A SqlString just contains a regular Java string, but the SqlString wrapper
 * indicates that the string has been created carefully guarding against all SQL
 * dialect and injection issues.
 *
 *
 * The easiest way to do build a SqlString is to use a [SqlBuilder].
 */
class SqlString(
    dialect: SqlDialect?, sql: String,
    @Nullable dynamicParameters: ImmutableList<Integer?>
) {
    /**
     * Returns the SQL string.
     *
     * @return SQL string
     */
    val sql: String
    private val dialect: SqlDialect?

    @Nullable
    private val dynamicParameters: ImmutableList<Integer>

    /**
     * Creates a SqlString.
     */
    constructor(dialect: SqlDialect?, sql: String) : this(dialect, sql, ImmutableList.of()) {}

    /**
     * Creates a SqlString. The SQL might contain dynamic parameters, dynamicParameters
     * designate the order of the parameters.
     *
     * @param sql text
     * @param dynamicParameters indices
     */
    init {
        this.dialect = dialect
        this.sql = sql
        this.dynamicParameters = dynamicParameters
        assert(sql != null) { "sql must be NOT null" }
        assert(dialect != null) { "dialect must be NOT null" }
    }

    @Override
    override fun hashCode(): Int {
        return sql.hashCode()
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (obj === this
                || obj is SqlString
                && sql.equals((obj as SqlString).sql))
    }

    /**
     * {@inheritDoc}
     *
     *
     * Returns the SQL string.
     *
     * @return SQL string
     * @see .getSql
     */
    @Override
    override fun toString(): String {
        return sql
    }

    /**
     * Returns indices of dynamic parameters.
     *
     * @return indices of dynamic parameters
     */
    @Pure
    @Nullable
    fun getDynamicParameters(): ImmutableList<Integer> {
        return dynamicParameters
    }

    /**
     * Returns the dialect.
     */
    fun getDialect(): SqlDialect? {
        return dialect
    }
}
