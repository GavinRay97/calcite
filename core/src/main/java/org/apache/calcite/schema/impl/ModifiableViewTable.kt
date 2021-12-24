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

import org.apache.calcite.plan.RelOptTable

/** Extension to [ViewTable] that is modifiable.  */
class ModifiableViewTable(
    elementType: Type?, rowType: RelProtoDataType,
    viewSql: String, schemaPath: List<String?>?, @Nullable viewPath: List<String?>?,
    table: Table, tablePath: Path, constraint: RexNode,
    columnMapping: ImmutableIntList
) : ViewTable(elementType, rowType, viewSql, schemaPath, viewPath), ModifiableView, Wrapper {
    private val table: Table
    private val tablePath: Path
    private val constraint: RexNode
    private val columnMapping: ImmutableIntList
    private val initializerExpressionFactory: InitializerExpressionFactory

    /** Creates a ModifiableViewTable.  */
    init {
        this.table = table
        this.tablePath = tablePath
        this.constraint = constraint
        this.columnMapping = columnMapping
        initializerExpressionFactory = ModifiableViewTableInitializerExpressionFactory()
    }

    @Override
    fun getConstraint(
        rexBuilder: RexBuilder,
        tableRowType: RelDataType?
    ): RexNode {
        return rexBuilder.copy(constraint)
    }

    @Override
    fun getColumnMapping(): ImmutableIntList {
        return columnMapping
    }

    @Override
    fun getTable(): Table {
        return table
    }

    @Override
    fun getTablePath(): Path {
        return tablePath
    }

    @Override
    fun <C : Object?> unwrap(aClass: Class<C>): @Nullable C? {
        if (aClass.isInstance(initializerExpressionFactory)) {
            return aClass.cast(initializerExpressionFactory)
        } else if (aClass.isInstance(table)) {
            return aClass.cast(table)
        }
        return super.unwrap(aClass)
    }

    /**
     * Extends the underlying table and returns a new view with updated row-type
     * and column-mapping.
     *
     *
     * The type factory is used to perform some scratch calculations, viz the
     * type mapping, but the "real" row-type will be assigned later, when the
     * table has been bound to the statement's type factory. The is important,
     * because adding types to type factories that do not belong to a statement
     * could potentially leak memory.
     *
     * @param extendedColumns Extended fields
     * @param typeFactory Type factory
     */
    fun extend(
        extendedColumns: List<RelDataTypeField>, typeFactory: RelDataTypeFactory
    ): ModifiableViewTable {
        val underlying: ExtensibleTable = unwrap(ExtensibleTable::class.java)
        assert(underlying != null)
        val builder: RelDataTypeFactory.Builder = typeFactory.builder()
        val rowType: RelDataType = getRowType(typeFactory)
        for (column in rowType.getFieldList()) {
            builder.add(column)
        }
        for (column in extendedColumns) {
            builder.add(column)
        }

        // The characteristics of the new view.
        val newRowType: RelDataType = builder.build()
        val newColumnMapping: ImmutableIntList = getNewColumnMapping(
            underlying, getColumnMapping(), extendedColumns,
            typeFactory
        )

        // Extend the underlying table with only the fields that
        // duplicate column names in neither the view nor the base table.
        val underlyingColumns: List<RelDataTypeField> = underlying.getRowType(typeFactory).getFieldList()
        val columnsOfExtendedBaseTable: List<RelDataTypeField> =
            RelOptUtil.deduplicateColumns(underlyingColumns, extendedColumns)
        val extendColumnsOfBaseTable: List<RelDataTypeField> = columnsOfExtendedBaseTable.subList(
            underlyingColumns.size(), columnsOfExtendedBaseTable.size()
        )
        val extendedTable: Table = underlying.extend(extendColumnsOfBaseTable)
        return extend(
            extendedTable, RelDataTypeImpl.proto(newRowType),
            newColumnMapping
        )
    }

    protected fun extend(
        extendedTable: Table,
        protoRowType: RelProtoDataType, newColumnMapping: ImmutableIntList
    ): ModifiableViewTable {
        return ModifiableViewTable(
            getElementType(), protoRowType, getViewSql(),
            getSchemaPath(), getViewPath(), extendedTable, getTablePath(),
            constraint, newColumnMapping
        )
    }

    /**
     * Initializes columns based on the view constraint.
     */
    private inner class ModifiableViewTableInitializerExpressionFactory private constructor() :
        NullInitializerExpressionFactory() {
        private val projectMap: ImmutableMap<Integer, RexNode>

        init {
            val projectMap: Map<Integer, RexNode> = HashMap()
            val filters: List<RexNode> = ArrayList()
            RelOptUtil.inferViewPredicates(projectMap, filters, constraint)
            assert(filters.isEmpty())
            this.projectMap = ImmutableMap.copyOf(projectMap)
        }

        @Override
        fun generationStrategy(
            table: RelOptTable,
            iColumn: Int
        ): ColumnStrategy {
            val viewTable: ModifiableViewTable = requireNonNull(
                table.unwrap(ModifiableViewTable::class.java)
            ) { "unable to unwrap ModifiableViewTable from $table" }
            assert(iColumn < viewTable.columnMapping.size())

            // Use the view constraint to generate the default value if the column is
            // constrained.
            val mappedOrdinal: Int = viewTable.columnMapping.get(iColumn)
            val viewConstraint: RexNode = projectMap.get(mappedOrdinal)
            if (viewConstraint != null) {
                return ColumnStrategy.DEFAULT
            }

            // Otherwise use the default value of the underlying table.
            val schemaTable: Table = viewTable.getTable()
            if (schemaTable is Wrapper) {
                val initializerExpressionFactory: InitializerExpressionFactory = (schemaTable as Wrapper).unwrap(
                    InitializerExpressionFactory::class.java
                )
                if (initializerExpressionFactory != null) {
                    return initializerExpressionFactory.generationStrategy(
                        table,
                        iColumn
                    )
                }
            }
            return super.generationStrategy(table, iColumn)
        }

        @Override
        fun newColumnDefaultValue(
            table: RelOptTable,
            iColumn: Int, context: InitializerContext
        ): RexNode {
            val viewTable: ModifiableViewTable = requireNonNull(
                table.unwrap(ModifiableViewTable::class.java)
            ) { "unable to unwrap ModifiableViewTable from $table" }
            assert(iColumn < viewTable.columnMapping.size())
            val rexBuilder: RexBuilder = context.getRexBuilder()
            val typeFactory: RelDataTypeFactory = rexBuilder.getTypeFactory()
            val viewType: RelDataType = viewTable.getRowType(typeFactory)
            val iType: RelDataType = viewType.getFieldList().get(iColumn).getType()

            // Use the view constraint to generate the default value if the column is constrained.
            val mappedOrdinal: Int = viewTable.columnMapping.get(iColumn)
            val viewConstraint: RexNode = projectMap.get(mappedOrdinal)
            if (viewConstraint != null) {
                return rexBuilder.ensureType(iType, viewConstraint, true)
            }

            // Otherwise use the default value of the underlying table.
            val schemaTable: Table = viewTable.getTable()
            if (schemaTable is Wrapper) {
                val initializerExpressionFactory: InitializerExpressionFactory = (schemaTable as Wrapper).unwrap(
                    InitializerExpressionFactory::class.java
                )
                if (initializerExpressionFactory != null) {
                    val tableConstraint: RexNode = initializerExpressionFactory.newColumnDefaultValue(
                        table, iColumn,
                        context
                    )
                    return rexBuilder.ensureType(iType, tableConstraint, true)
                }
            }

            // Otherwise Sql type of NULL.
            return super.newColumnDefaultValue(table, iColumn, context)
        }

        @Override
        fun newAttributeInitializer(
            type: RelDataType?,
            constructor: SqlFunction?, iAttribute: Int, constructorArgs: List<RexNode?>?,
            context: InitializerContext?
        ): RexNode {
            throw UnsupportedOperationException("Not implemented - unknown requirements")
        }
    }

    companion object {
        /**
         * Creates a mapping from the view index to the index in the underlying table.
         */
        private fun getNewColumnMapping(
            underlying: Table?,
            oldColumnMapping: ImmutableIntList, extendedColumns: List<RelDataTypeField>,
            typeFactory: RelDataTypeFactory
        ): ImmutableIntList {
            val baseColumns: List<RelDataTypeField> = underlying.getRowType(typeFactory).getFieldList()
            val nameToIndex: Map<String, Integer> = mapNameToIndex(baseColumns)
            val newMapping: ImmutableList.Builder<Integer> = ImmutableList.builder()
            newMapping.addAll(oldColumnMapping)
            var newMappedIndex: Int = baseColumns.size()
            for (extendedColumn in extendedColumns) {
                val extendedColumnName: String = extendedColumn.getName()
                if (nameToIndex.containsKey(extendedColumnName)) {
                    // The extended column duplicates a column in the underlying table.
                    // Map to the index in the underlying table.
                    newMapping.add(nameToIndex[extendedColumnName])
                } else {
                    // The extended column is not in the underlying table.
                    newMapping.add(newMappedIndex++)
                }
            }
            return ImmutableIntList.copyOf(newMapping.build())
        }
    }
}
