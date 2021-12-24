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
package org.apache.calcite.materialize

import org.apache.calcite.jdbc.CalciteSchema

/**
 * Actor that manages the state of materializations in the system.
 */
internal class MaterializationActor {
    // Not an actor yet -- TODO make members private and add request/response
    // queues
    val keyMap: Map<MaterializationKey?, Materialization> = HashMap()
    val keyBySql: Map<QueryKey, MaterializationKey> = HashMap()
    val keyByTile: Map<TileKey, MaterializationKey> = HashMap()

    /** Tiles grouped by dimensionality. We use a
     * [TileKey] with no measures to represent a
     * dimensionality.  */
    val tilesByDimensionality: Multimap<TileKey, TileKey> = HashMultimap.create()

    /** A query materialized in a table, so that reading from the table gives the
     * same results as executing the query.  */
    internal class Materialization(
        key: MaterializationKey,
        rootSchema: CalciteSchema,
        materializedTable: @Nullable CalciteSchema.TableEntry?,
        sql: String,
        rowType: RelDataType,
        @Nullable viewSchemaPath: List<String>
    ) {
        val key: MaterializationKey
        val rootSchema: CalciteSchema
        var materializedTable: @Nullable CalciteSchema.TableEntry?
        val sql: String
        val rowType: RelDataType

        @Nullable
        val viewSchemaPath: List<String>

        /** Creates a materialization.
         *
         * @param key  Unique identifier of this materialization
         * @param materializedTable Table that currently materializes the query.
         * That is, executing "select * from table" will
         * give the same results as executing the query.
         * May be null when the materialization is created;
         * materialization service will change the value as
         * @param sql  Query that is materialized
         * @param rowType Row type
         */
        init {
            this.key = key
            this.rootSchema = Objects.requireNonNull(rootSchema, "rootSchema")
            Preconditions.checkArgument(rootSchema.isRoot(), "must be root schema")
            this.materializedTable = materializedTable // may be null
            this.sql = sql
            this.rowType = rowType
            this.viewSchemaPath = viewSchemaPath
        }
    }

    /** A materialization can be re-used if it is the same SQL, on the same
     * schema, with the same path for resolving functions.  */
    internal class QueryKey(val sql: String, schema: CalciteSchema, @Nullable path: List<String>) {
        val schema: CalciteSchema

        @Nullable
        val path: List<String>

        init {
            this.schema = schema
            this.path = path
        }

        @Override
        override fun equals(@Nullable obj: Object): Boolean {
            return (obj === this
                    || (obj is QueryKey
                    && sql.equals((obj as QueryKey).sql)
                    && schema.equals((obj as QueryKey).schema)
                    && Objects.equals(path, (obj as QueryKey).path)))
        }

        @Override
        override fun hashCode(): Int {
            return Objects.hash(sql, schema, path)
        }
    }
}
