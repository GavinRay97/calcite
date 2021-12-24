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

import org.apache.calcite.adapter.java.JavaTypeFactory

/**
 * Table that is a materialized view.
 *
 *
 * It can exist in two states: materialized and not materialized. Over time,
 * a given materialized view may switch states. How it is expanded depends upon
 * its current state. State is managed by
 * [org.apache.calcite.materialize.MaterializationService].
 */
class MaterializedViewTable(
    elementType: Type?,
    relDataType: RelProtoDataType,
    viewSql: String,
    viewSchemaPath: List<String?>?,
    @Nullable viewPath: List<String?>?,
    key: MaterializationKey
) : ViewTable(elementType, relDataType, viewSql, viewSchemaPath, viewPath) {
    private val key: MaterializationKey

    init {
        this.key = key
    }

    @Override
    override fun toRel(
        context: RelOptTable.ToRelContext,
        relOptTable: RelOptTable
    ): RelNode {
        val tableEntry: CalciteSchema.TableEntry = MaterializationService.instance().checkValid(key)
        if (tableEntry != null) {
            val materializeTable: Table = tableEntry.getTable()
            if (materializeTable is TranslatableTable) {
                val table: TranslatableTable = materializeTable as TranslatableTable
                return table.toRel(context, relOptTable)
            }
        }
        return super.toRel(context, relOptTable)
    }

    /** Table function that returns the table that materializes a view.  */
    class MaterializedViewTableMacro(
        schema: CalciteSchema, viewSql: String,
        @Nullable viewSchemaPath: List<String>?, viewPath: List<String>,
        @Nullable suggestedTableName: String,
        existing: Boolean
    ) : ViewTableMacro(
        schema, viewSql,
        viewSchemaPath ?: schema.path(null), viewPath,
        Boolean.TRUE
    ) {
        private val key: MaterializationKey

        init {
            key = Objects.requireNonNull(
                MaterializationService.instance().defineMaterialization(
                    schema, null, viewSql, schemaPath, suggestedTableName, true,
                    existing
                )
            )
        }

        @Override
        fun apply(arguments: List<Object?>): TranslatableTable {
            assert(arguments.isEmpty())
            val parsed: ParseResult = Schemas.parse(
                MATERIALIZATION_CONNECTION, schema, schemaPath,
                viewSql
            )
            val schemaPath1: List<String> = if (schemaPath != null) schemaPath else schema.path(null)
            val typeFactory: JavaTypeFactory = MATERIALIZATION_CONNECTION.getTypeFactory()
            return MaterializedViewTable(
                typeFactory.getJavaClass(parsed.rowType),
                RelDataTypeImpl.proto(parsed.rowType), viewSql, schemaPath1, viewPath, key
            )
        }
    }

    companion object {
        /**
         * Internal connection, used to execute queries to materialize views.
         * To be used only by Calcite internals. And sparingly.
         */
        val MATERIALIZATION_CONNECTION: CalciteConnection? = null

        init {
            try {
                MATERIALIZATION_CONNECTION = DriverManager.getConnection("jdbc:calcite:")
                    .unwrap(CalciteConnection::class.java)
            } catch (e: SQLException) {
                throw RuntimeException(e)
            }
        }

        /** Table macro that returns a materialized view.  */
        fun create(
            schema: CalciteSchema,
            viewSql: String, @Nullable viewSchemaPath: List<String>?, viewPath: List<String>,
            @Nullable suggestedTableName: String, existing: Boolean
        ): MaterializedViewTableMacro {
            return MaterializedViewTableMacro(
                schema, viewSql, viewSchemaPath, viewPath,
                suggestedTableName, existing
            )
        }
    }
}
