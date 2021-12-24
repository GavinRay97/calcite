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
package org.apache.calcite.adapter.clone

import org.apache.calcite.adapter.java.JavaTypeFactory

/**
 * Schema that contains in-memory copies of tables from a JDBC schema.
 */
class CloneSchema(sourceSchema: SchemaPlus) : AbstractSchema() {
    // TODO: implement 'driver' property
    // TODO: implement 'source' property
    // TODO: test Factory
    private val sourceSchema: SchemaPlus

    /**
     * Creates a CloneSchema.
     *
     * @param sourceSchema JDBC data source
     */
    init {
        this.sourceSchema = sourceSchema
    }

    @get:Override
    protected val tableMap: Map<String, Any>
        protected get() {
            val map: Map<String, Table> = LinkedHashMap()
            for (name in sourceSchema.getTableNames()) {
                val table: Table = sourceSchema.getTable(name)
                if (table is QueryableTable) {
                    val sourceTable: QueryableTable = table as QueryableTable
                    map.put(
                        name,
                        createCloneTable(MATERIALIZATION_CONNECTION, sourceTable, name)
                    )
                }
            }
            return map
        }

    private fun createCloneTable(
        queryProvider: QueryProvider,
        sourceTable: QueryableTable, name: String
    ): Table {
        val queryable: Queryable<Object> = sourceTable.asQueryable(queryProvider, sourceSchema, name)
        val typeFactory: JavaTypeFactory = (queryProvider as CalciteConnection).getTypeFactory()
        return createCloneTable(
            typeFactory, Schemas.proto(sourceTable),
            ImmutableList.of(), null, queryable
        )
    }

    /** Schema factory that creates a
     * [org.apache.calcite.adapter.clone.CloneSchema].
     * This allows you to create a clone schema inside a model.json file.
     *
     * <blockquote><pre>
     * {
     * version: '1.0',
     * defaultSchema: 'FOODMART_CLONE',
     * schemas: [
     * {
     * name: 'FOODMART_CLONE',
     * type: 'custom',
     * factory: 'org.apache.calcite.adapter.clone.CloneSchema$Factory',
     * operand: {
     * jdbcDriver: 'com.mysql.jdbc.Driver',
     * jdbcUrl: 'jdbc:mysql://localhost/foodmart',
     * jdbcUser: 'foodmart',
     * jdbcPassword: 'foodmart'
     * }
     * }
     * ]
     * }</pre></blockquote>
     */
    class Factory : SchemaFactory {
        @Override
        fun create(
            parentSchema: SchemaPlus,
            name: String,
            operand: Map<String?, Object?>?
        ): Schema {
            val schema: SchemaPlus = parentSchema.add(
                name,
                JdbcSchema.create(parentSchema, "$name\$source", operand)
            )
            return CloneSchema(schema)
        }
    }

    companion object {
        @Deprecated // to be removed before 2.0
        fun <T> createCloneTable(
            typeFactory: JavaTypeFactory,
            protoRowType: RelProtoDataType,
            @Nullable repList: List<ColumnMetaData.Rep>?,
            source: Enumerable<T>
        ): Table {
            return createCloneTable(
                typeFactory, protoRowType, ImmutableList.of(),
                repList, source
            )
        }

        fun <T> createCloneTable(
            typeFactory: JavaTypeFactory,
            protoRowType: RelProtoDataType, collations: List<RelCollation?>,
            @Nullable repList: List<ColumnMetaData.Rep>?, source: Enumerable<T>
        ): Table {
            val elementType: Type
            if (source is QueryableTable) {
                elementType = (source as QueryableTable).getElementType()
            } else if (protoRowType.apply(typeFactory).getFieldCount() === 1) {
                elementType = if (repList != null) {
                    repList[0].clazz
                } else {
                    Object::class.java
                }
            } else {
                elementType = Array<Object>::class.java
            }
            return ArrayTable(
                elementType,
                protoRowType,
                Suppliers.memoize {
                    val loader = ColumnLoader(
                        typeFactory, source, protoRowType,
                        repList
                    )
                    val collation2: List<RelCollation> = if (collations.isEmpty()
                        && loader.sortField >= 0
                    ) RelCollations.createSingleton(loader.sortField) else collations
                    Content(
                        loader.representationValues,
                        loader.size(), collation2
                    )
                })
        }
    }
}
