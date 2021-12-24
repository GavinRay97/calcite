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
package org.apache.calcite.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import java.util.List
import java.util.Objects

/**
 * View schema element.
 *
 *
 * Like base class [JsonTable],
 * occurs within [JsonMapSchema.tables].
 *
 * <h2>Modifiable views</h2>
 *
 *
 * A view is modifiable if contains only SELECT, FROM, WHERE (no JOIN,
 * aggregation or sub-queries) and every column:
 *
 *
 *  * is specified once in the SELECT clause; or
 *  * occurs in the WHERE clause with a column = literal predicate; or
 *  * is nullable.
 *
 *
 *
 * The second clause allows Calcite to automatically provide the correct
 * value for hidden columns. It is useful in, say, a multi-tenant environment,
 * where the `tenantId` column is hidden, mandatory (NOT NULL), and has a
 * constant value for a particular view.
 *
 *
 * Errors regarding modifiable views:
 *
 *
 *  * If a view is marked modifiable: true and is not modifiable, Calcite
 * throws an error while reading the schema.
 *  * If you submit an INSERT, UPDATE or UPSERT command to a non-modifiable
 * view, Calcite throws an error when validating the statement.
 *  * If a DML statement creates a row that would not appear in the view
 * (for example, a row in female_emps, above, with gender = 'M'), Calcite
 * throws an error when executing the statement.
 *
 *
 * @see JsonRoot Description of schema elements
 */
class JsonView @JsonCreator constructor(
    @JsonProperty(value = "name", required = true) name: String?,
    @JsonProperty("steram") stream: JsonStream?,
    @JsonProperty(value = "sql", required = true) sql: Object?,
    @JsonProperty("path") @Nullable path: List<String>,
    @JsonProperty("modifiable") @Nullable modifiable: Boolean
) : JsonTable(name, stream) {
    /** SQL query that is the definition of the view.
     *
     *
     * Must be a string or a list of strings (which are concatenated into a
     * multi-line SQL string, separated by newlines).
     */
    val sql: Object

    /** Schema name(s) to use when resolving query.
     *
     *
     * If not specified, defaults to current schema.
     */
    @Nullable
    val path: List<String>

    /** Whether this view should allow INSERT requests.
     *
     *
     * The values have the following meanings:
     *
     *  * If true, Calcite throws an error when validating the schema if the
     * view is not modifiable.
     *  * If null, Calcite deduces whether the view is modifiable.
     *  * If false, Calcite will not allow inserts.
     *
     *
     *
     * The default value is `null`.
     */
    @Nullable
    val modifiable: Boolean

    init {
        this.sql = Objects.requireNonNull(sql, "sql")
        this.path = path
        this.modifiable = modifiable
    }

    @Override
    fun accept(handler: ModelHandler) {
        handler.visit(this)
    }

    @Override
    override fun toString(): String {
        return "JsonView(name=" + name.toString() + ")"
    }

    /** Returns the SQL query as a string, concatenating a list of lines if
     * necessary.  */
    fun getSql(): String {
        return JsonLattice.toString(sql)
    }
}
