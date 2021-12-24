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

import org.apache.calcite.DataContext

/**
 * [TransientTable] backed by a Java list. It will be automatically added to the
 * current schema when [.scan] method gets called.
 *
 *
 * NOTE: The current API is experimental and subject to change without notice.
 */
@Experimental
class ListTransientTable(private val name: String, rowType: RelDataType) : AbstractQueryableTable(TYPE), TransientTable,
    ModifiableTable, ScannableTable {
    private val rows: List = ArrayList()
    private val protoRowType: RelDataType

    init {
        protoRowType = rowType
    }

    @Override
    fun toModificationRel(
        cluster: RelOptCluster?,
        table: RelOptTable?,
        catalogReader: CatalogReader?,
        child: RelNode?,
        operation: TableModify.Operation?,
        @Nullable updateColumnList: List<String?>?,
        @Nullable sourceExpressionList: List<RexNode?>?,
        flattened: Boolean
    ): TableModify {
        return LogicalTableModify.create(
            table, catalogReader, child, operation,
            updateColumnList, sourceExpressionList, flattened
        )
    }

    @get:Override
    val modifiableCollection: Collection
        get() = rows

    @Override
    fun scan(root: DataContext): Enumerable<Array<Object>> {
        // add the table into the schema, so that it is accessible by any potential operator
        requireNonNull(root.getRootSchema(), "root.getRootSchema()")
            .add(name, this)
        val cancelFlag: AtomicBoolean = DataContext.Variable.CANCEL_FLAG.get(root)
        return object : AbstractEnumerable<Array<Object?>?>() {
            @Override
            fun enumerator(): Enumerator<Array<Object>> {
                return object : Enumerator<Array<Object?>?>() {
                    private val list: List = ArrayList(rows)
                    private var i = -1

                    // TODO cleaner way to handle non-array objects?
                    @Override
                    fun current(): Array<Object> {
                        val current: Object = list.get(i)
                        return if (current != null && current.getClass().isArray()) current else arrayOf<Object?>(
                            current
                        )
                    }

                    @Override
                    fun moveNext(): Boolean {
                        return if (cancelFlag != null && cancelFlag.get()) {
                            false
                        } else ++i < list.size()
                    }

                    @Override
                    fun reset() {
                        i = -1
                    }

                    @Override
                    fun close() {
                    }
                }
            }
        }
    }

    @Override
    fun getExpression(
        schema: SchemaPlus?, tableName: String?,
        clazz: Class?
    ): Expression {
        return Schemas.tableExpression(schema, elementType, tableName, clazz)
    }

    @Override
    fun <T> asQueryable(
        queryProvider: QueryProvider?,
        schema: SchemaPlus?, tableName: String?
    ): Queryable<T> {
        return object : AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
            @Override
            fun enumerator(): Enumerator<T> {
                return Linq4j.enumerator(rows) as Enumerator<T>
            }
        }
    }

    @Override
    fun getRowType(typeFactory: RelDataTypeFactory): RelDataType {
        return typeFactory.copyType(protoRowType)
    }

    @get:Override
    val elementType: Type
        get() = TYPE

    companion object {
        private val TYPE: Type = Array<Object>::class.java
    }
}
