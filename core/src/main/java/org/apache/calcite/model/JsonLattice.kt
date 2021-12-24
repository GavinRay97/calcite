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
import com.google.common.collect.ImmutableList
import java.util.ArrayList
import java.util.List
import java.util.StringJoiner
import java.util.Objects.requireNonNull

/**
 * Element that describes a star schema and provides a framework for defining,
 * recognizing, and recommending materialized views at various levels of
 * aggregation.
 *
 *
 * Occurs within [JsonSchema.lattices].
 *
 * @see JsonRoot Description of schema elements
 */
class JsonLattice @JsonCreator constructor(
    @JsonProperty(value = "name", required = true) name: String?,
    @JsonProperty(value = "sql", required = true) sql: Object?,
    @JsonProperty("auto") @Nullable auto: Boolean?,
    @JsonProperty("algorithm") @Nullable algorithm: Boolean?,
    @JsonProperty("algorithmMaxMillis") @Nullable algorithmMaxMillis: Long?,
    @JsonProperty("rowCountEstimate") @Nullable rowCountEstimate: Double?,
    @JsonProperty("statisticProvider") @Nullable statisticProvider: String?,
    @JsonProperty("defaultMeasures") @Nullable defaultMeasures: List<JsonMeasure?>?
) {
    /** The name of this lattice.
     *
     *
     * Required.
     */
    val name: String

    /** SQL query that defines the lattice.
     *
     *
     * Must be a string or a list of strings (which are concatenated into a
     * multi-line SQL string, separated by newlines).
     *
     *
     * The structure of the SQL statement, and in particular the order of
     * items in the FROM clause, defines the fact table, dimension tables, and
     * join paths for this lattice.
     */
    val sql: Object

    /** Whether to materialize tiles on demand as queries are executed.
     *
     *
     * Optional; default is true.
     */
    val auto: Boolean

    /** Whether to use an optimization algorithm to suggest and populate an
     * initial set of tiles.
     *
     *
     * Optional; default is false.
     */
    val algorithm: Boolean

    /** Maximum time (in milliseconds) to run the algorithm.
     *
     *
     * Optional; default is -1, meaning no timeout.
     *
     *
     * When the timeout is reached, Calcite uses the best result that has
     * been obtained so far.
     */
    val algorithmMaxMillis: Long

    /** Estimated number of rows.
     *
     *
     * If null, Calcite will a query to find the real value.  */
    @Nullable
    val rowCountEstimate: Double?

    /** Name of a class that provides estimates of the number of distinct values
     * in each column.
     *
     *
     * The class must implement the
     * [org.apache.calcite.materialize.LatticeStatisticProvider] interface.
     *
     *
     * Or, you can use a class name plus a static field, for example
     * "org.apache.calcite.materialize.Lattices#CACHING_SQL_STATISTIC_PROVIDER".
     *
     *
     * If not set, Calcite will generate and execute a SQL query to find the
     * real value, and cache the results.  */
    @Nullable
    val statisticProvider: String?

    /** List of materialized aggregates to create up front.  */
    val tiles: List<JsonTile> = ArrayList()

    /** List of measures that a tile should have by default.
     *
     *
     * A tile can define its own measures, including measures not in this list.
     *
     *
     * Optional. The default list is just "count(*)".
     */
    val defaultMeasures: List<JsonMeasure>

    init {
        this.name = requireNonNull(name, "name")
        this.sql = requireNonNull(sql, "sql")
        this.auto = auto == null || auto
        this.algorithm = algorithm != null && algorithm
        this.algorithmMaxMillis = algorithmMaxMillis ?: -1
        this.rowCountEstimate = rowCountEstimate
        this.statisticProvider = statisticProvider
        this.defaultMeasures = defaultMeasures ?: ImmutableList.of(JsonMeasure("count", null))
    }

    fun accept(handler: ModelHandler) {
        handler.visit(this)
    }

    @Override
    override fun toString(): String {
        return "JsonLattice(name=" + name + ", sql=" + getSql() + ")"
    }

    /** Returns the SQL query as a string, concatenating a list of lines if
     * necessary.  */
    fun getSql(): String {
        return toString(sql)
    }

    fun visitChildren(modelHandler: ModelHandler) {
        for (jsonMeasure in defaultMeasures) {
            jsonMeasure.accept(modelHandler)
        }
        for (jsonTile in tiles) {
            jsonTile.accept(modelHandler)
        }
    }

    companion object {
        /** Converts a string or a list of strings to a string. The list notation
         * is a convenient way of writing long multi-line strings in JSON.  */
        fun toString(o: Object): String {
            requireNonNull(o, "argument must not be null")
            return if (o is String) o else concatenate(o as List<*>)
        }

        /** Converts a list of strings into a multi-line string.  */
        private fun concatenate(list: List<*>): String {
            val buf = StringJoiner("\n", "", "\n")
            for (o in list) {
                if (o !is String) {
                    throw RuntimeException(
                        "each element of a string list must be a string; found: $o"
                    )
                }
                buf.add(o)
            }
            return buf.toString()
        }
    }
}
