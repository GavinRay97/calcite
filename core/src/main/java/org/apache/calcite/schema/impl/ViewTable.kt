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
package org.apache.calcite.schema.impl

import org.apache.calcite.adapter.java.AbstractQueryableTable

/**
 * Table whose contents are defined using an SQL statement.
 *
 *
 * It is not evaluated; it is expanded during query planning.
 */
class ViewTable(
    elementType: Type?, rowType: RelProtoDataType,
    /** Returns the view's SQL definition.  */
    val viewSql: String,
    schemaPath: List<String?>?, @Nullable viewPath: List<String?>?
) : AbstractQueryableTable(elementType), TranslatableTable {
    /** Returns the the schema path of the view.  */
    val schemaPath: List<String>
    private val protoRowType: RelProtoDataType

    /** Returns the the path of the view.  */
    @get:Nullable
    @Nullable
    val viewPath: List<String>?

    init {
        this.schemaPath = ImmutableList.copyOf(schemaPath)
        protoRowType = rowType
        this.viewPath = if (viewPath == null) null else ImmutableList.copyOf(viewPath)
    }

    @get:Override
    val jdbcTableType: Schema.TableType
        get() = Schema.TableType.VIEW

    @Override
    fun getRowType(typeFactory: RelDataTypeFactory?): RelDataType {
        return protoRowType.apply(typeFactory)
    }

    @Override
    fun <T> asQueryable(
        queryProvider: QueryProvider,
        schema: SchemaPlus?, tableName: String?
    ): Queryable<T> {
        return queryProvider.createQuery(
            getExpression(schema, tableName, Queryable::class.java), elementType
        )
    }

    @Override
    fun toRel(
        context: RelOptTable.ToRelContext,
        relOptTable: RelOptTable
    ): RelNode {
        return expandView(context, relOptTable.getRowType(), viewSql).rel
    }

    private fun expandView(
        context: RelOptTable.ToRelContext,
        rowType: RelDataType, queryString: String
    ): RelRoot {
        return try {
            val root: RelRoot = context.expandView(rowType, queryString, schemaPath, viewPath)
            val rel: RelNode = RelOptUtil.createCastRel(root.rel, rowType, true)
            // Expand any views
            val rel2: RelNode = rel.accept(
                object : RelShuttleImpl() {
                    @Override
                    fun visit(scan: TableScan): RelNode {
                        val table: RelOptTable = scan.getTable()
                        val translatableTable: TranslatableTable = table.unwrap(TranslatableTable::class.java)
                        return if (translatableTable != null) {
                            translatableTable.toRel(context, table)
                        } else super.visit(scan)
                    }
                })
            root.withRel(rel2)
        } catch (e: Exception) {
            throw RuntimeException(
                "Error while parsing view definition: "
                        + queryString, e
            )
        }
    }

    companion object {
        @Deprecated // to be removed before 2.0
        fun viewMacro(
            schema: SchemaPlus?,
            viewSql: String, schemaPath: List<String?>?
        ): ViewTableMacro {
            return viewMacro(schema, viewSql, schemaPath, null, Boolean.TRUE)
        }

        @Deprecated // to be removed before 2.0
        fun viewMacro(
            schema: SchemaPlus?, viewSql: String,
            schemaPath: List<String?>?, @Nullable modifiable: Boolean?
        ): ViewTableMacro {
            return viewMacro(schema, viewSql, schemaPath, null, modifiable)
        }

        /** Table macro that returns a view.
         *
         * @param schema Schema the view will belong to
         * @param viewSql SQL query
         * @param schemaPath Path of schema
         * @param modifiable Whether view is modifiable, or null to deduce it
         */
        fun viewMacro(
            schema: SchemaPlus?, viewSql: String,
            schemaPath: List<String?>?, @Nullable viewPath: List<String?>?,
            @Nullable modifiable: Boolean?
        ): ViewTableMacro {
            return ViewTableMacro(
                CalciteSchema.from(schema), viewSql, schemaPath,
                viewPath, modifiable
            )
        }
    }
}
