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
 * JSON object representing a schema that maps to a JDBC database.
 *
 *
 * Like the base class [JsonSchema],
 * occurs within [JsonRoot.schemas].
 *
 * @see JsonRoot Description of JSON schema elements
 */
class JsonJdbcSchema @JsonCreator constructor(
    @JsonProperty(value = "name", required = true) name: String,
    @JsonProperty("path") @Nullable path: List<Object>,
    @JsonProperty("cache") @Nullable cache: Boolean?,
    @JsonProperty("autoLattice") @Nullable autoLattice: Boolean,
    /** The name of the JDBC driver class.
     *
     *
     * Optional. If not specified, uses whichever class the JDBC
     * [java.sql.DriverManager] chooses.
     */
    @field:Nullable @param:JsonProperty("jdbcDriver") @param:Nullable val jdbcDriver: String,
    /** The FQN of the [org.apache.calcite.sql.SqlDialectFactory] implementation.
     *
     *
     * Optional. If not specified, uses whichever class the JDBC
     * [java.sql.DriverManager] chooses.
     */
    @field:Nullable @param:JsonProperty("sqlDialectFactory") @param:Nullable val sqlDialectFactory: String?,
    @JsonProperty(value = "jdbcUrl", required = true) jdbcUrl: String?,
    @JsonProperty("jdbcUser") @Nullable jdbcUser: String,
    @JsonProperty("jdbcPassword") @Nullable jdbcPassword: String,
    @JsonProperty("jdbcCatalog") @Nullable jdbcCatalog: String,
    @JsonProperty("jdbcSchema") @Nullable jdbcSchema: String
) : JsonSchema(name, path, cache, autoLattice) {
    /** JDBC connect string, for example "jdbc:mysql://localhost/foodmart".
     */
    val jdbcUrl: String

    /** JDBC user name.
     *
     *
     * Optional.
     */
    @Nullable
    val jdbcUser: String

    /** JDBC connect string, for example "jdbc:mysql://localhost/foodmart".
     *
     *
     * Optional.
     */
    @Nullable
    val jdbcPassword: String

    /** Name of the initial catalog in the JDBC data source.
     *
     *
     * Optional.
     */
    @Nullable
    val jdbcCatalog: String

    /** Name of the initial schema in the JDBC data source.
     *
     *
     * Optional.
     */
    @Nullable
    val jdbcSchema: String

    init {
        this.jdbcUrl = requireNonNull(jdbcUrl, "jdbcUrl")
        this.jdbcUser = jdbcUser
        this.jdbcPassword = jdbcPassword
        this.jdbcCatalog = jdbcCatalog
        this.jdbcSchema = jdbcSchema
    }

    @Override
    fun accept(handler: ModelHandler) {
        handler.visit(this)
    }
}
