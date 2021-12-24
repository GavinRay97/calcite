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
package org.apache.calcite.jdbc

import org.apache.calcite.linq4j.Enumerator

/** Schema that contains metadata tables such as "TABLES" and "COLUMNS".  */
internal class MetadataSchema
/** Creates the data dictionary, also called the information schema. It is a
 * schema called "metadata" that contains tables "TABLES", "COLUMNS" etc.  */
private constructor() : AbstractSchema() {
    @get:Override
    protected val tableMap: Map<String, Any>
        protected get() = TABLE_MAP

    companion object {
        private val TABLE_MAP: Map<String, Table> = ImmutableMap.of(
            "COLUMNS",
            object : MetadataTable<MetaColumn?>(MetaColumn::class.java) {
                @Override
                fun enumerator(
                    meta: CalciteMetaImpl
                ): Enumerator<MetaColumn> {
                    val catalog: String
                    catalog = try {
                        meta.getConnection().getCatalog()
                    } catch (e: SQLException) {
                        throw RuntimeException(e)
                    }
                    return meta.tables(catalog)
                        .selectMany(meta::columns).enumerator()
                }
            },
            "TABLES",
            object : MetadataTable<MetaTable?>(MetaTable::class.java) {
                @Override
                fun enumerator(meta: CalciteMetaImpl): Enumerator<MetaTable> {
                    val catalog: String
                    catalog = try {
                        meta.getConnection().getCatalog()
                    } catch (e: SQLException) {
                        throw RuntimeException(e)
                    }
                    return meta.tables(catalog).enumerator()
                }
            })
        val INSTANCE: Schema = MetadataSchema()
    }
}
