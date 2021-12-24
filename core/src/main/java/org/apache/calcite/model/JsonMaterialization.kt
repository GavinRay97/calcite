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
import java.util.Objects.requireNonNull

/**
 * Element that describes how a table is a materialization of a query.
 *
 *
 * Occurs within [JsonSchema.materializations].
 *
 * @see JsonRoot Description of schema elements
 */
class JsonMaterialization @JsonCreator constructor(
    @field:Nullable @param:JsonProperty("view") @param:Nullable val view: String?,
    @field:Nullable @param:JsonProperty("table") @param:Nullable val table: String,
    @JsonProperty(value = "sql", required = true) sql: Object?,
    @JsonProperty("viewSchemaPath") @Nullable viewSchemaPath: List<String>
) {
    /** SQL query that defines the materialization.
     *
     *
     * Must be a string or a list of strings (which are concatenated into a
     * multi-line SQL string, separated by newlines).
     */
    val sql: Object

    @Nullable
    val viewSchemaPath: List<String>

    init {
        this.sql = requireNonNull(sql, "sql")
        this.viewSchemaPath = viewSchemaPath
    }

    fun accept(handler: ModelHandler) {
        handler.visit(this)
    }

    @Override
    override fun toString(): String {
        return "JsonMaterialization(table=$table, view=$view)"
    }

    /** Returns the SQL query as a string, concatenating a list of lines if
     * necessary.  */
    fun getSql(): String {
        return JsonLattice.toString(sql)
    }
}
