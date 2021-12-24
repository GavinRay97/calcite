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
package org.apache.calcite.prepare

import org.apache.calcite.jdbc.CalciteSchema

/**
 * Implementation of [org.apache.calcite.plan.RelOptTable].
 */
class RelOptTableImpl private constructor(
    @Nullable schema: RelOptSchema?,
    rowType: RelDataType,
    names: List<String>,
    @Nullable table: Table?,
    @Nullable expressionFunction: Function<Class, Expression>,
    @Nullable rowCount: Double
) : Prepare.AbstractPreparingTable() {
    @Nullable
    private val schema: RelOptSchema?
    private val rowType: RelDataType

    @Nullable
    private val table: Table?

    @Nullable
    private val expressionFunction: Function<Class, Expression>
    private val names: ImmutableList<String>

    /** Estimate for the row count, or null.
     *
     *
     * If not null, overrides the estimate from the actual table.
     *
     *
     * Useful when a table that contains a materialized query result is being
     * used to replace a query expression that wildly underestimates the row
     * count. Now the materialized table can tell the same lie.  */
    @Nullable
    private val rowCount: Double

    init {
        this.schema = schema
        this.rowType = requireNonNull(rowType, "rowType")
        this.names = ImmutableList.copyOf(names)
        this.table = table // may be null
        this.expressionFunction = expressionFunction // may be null
        this.rowCount = rowCount // may be null
    }

    /**
     * Creates a copy of this RelOptTable. The new RelOptTable will have newRowType.
     */
    fun copy(newRowType: RelDataType): RelOptTableImpl {
        return RelOptTableImpl(
            schema, newRowType, names, table,
            expressionFunction, rowCount
        )
    }

    @Override
    override fun toString(): String {
        return ("RelOptTableImpl{"
                + "schema=" + schema
                + ", names= " + names
                + ", table=" + table
                + ", rowType=" + rowType
                + '}')
    }

    @Override
    fun <T : Object?> unwrap(clazz: Class<T>): @Nullable T? {
        if (clazz.isInstance(this)) {
            return clazz.cast(this)
        }
        if (clazz.isInstance(table)) {
            return clazz.cast(table)
        }
        if (table is Wrapper) {
            val t: T = (table as Wrapper?).unwrap(clazz)
            if (t != null) {
                return t
            }
        }
        return if (clazz === CalciteSchema::class.java && schema != null) {
            clazz.cast(
                Schemas.subSchema(
                    (schema as CalciteCatalogReader).rootSchema,
                    Util.skipLast(qualifiedName)
                )
            )
        } else null
    }

    @Override
    @Nullable
    fun getExpression(clazz: Class?): Expression? {
        return if (expressionFunction == null) {
            null
        } else expressionFunction.apply(clazz)
    }

    @Override
    protected override fun extend(extendedTable: Table): RelOptTable {
        val schema: RelOptSchema = requireNonNull(relOptSchema, "relOptSchema")
        val extendedRowType: RelDataType = extendedTable.getRowType(schema.getTypeFactory())
        return RelOptTableImpl(
            schema, extendedRowType, qualifiedName,
            extendedTable, expressionFunction, getRowCount()
        )
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (obj is RelOptTableImpl
                && rowType.equals((obj as RelOptTableImpl).getRowType())
                && table === (obj as RelOptTableImpl).table)
    }

    @Override
    override fun hashCode(): Int {
        return if (table == null) super.hashCode() else table.hashCode()
    }

    @Override
    fun getRowCount(): Double {
        if (rowCount != null) {
            return rowCount
        }
        if (table != null) {
            val rowCount: Double = table.getStatistic().getRowCount()
            if (rowCount != null) {
                return rowCount
            }
        }
        return 100.0
    }

    @get:Nullable
    @get:Override
    val relOptSchema: RelOptSchema?
        get() = schema

    @Override
    fun toRel(context: ToRelContext): RelNode {
        // Make sure rowType's list is immutable. If rowType is DynamicRecordType, creates a new
        // RelOptTable by replacing with immutable RelRecordType using the same field list.
        if (getRowType().isDynamicStruct()) {
            val staticRowType: RelDataType = RelRecordType(getRowType().getFieldList())
            val relOptTable: RelOptTable = copy(staticRowType)
            return relOptTable.toRel(context)
        }

        // If there are any virtual columns, create a copy of this table without
        // those virtual columns.
        val strategies: List<ColumnStrategy> = getColumnStrategies()
        if (strategies.contains(ColumnStrategy.VIRTUAL)) {
            val b: RelDataTypeFactory.Builder = context.getCluster().getTypeFactory().builder()
            for (field in rowType.getFieldList()) {
                if (strategies[field.getIndex()] !== ColumnStrategy.VIRTUAL) {
                    b.add(field.getName(), field.getType())
                }
            }
            val relOptTable: RelOptTable = object : RelOptTableImpl(
                schema, b.build(), names, table,
                expressionFunction, rowCount
            ) {
                @Override
                override fun <T : Object?> unwrap(clazz: Class<T>): @Nullable T? {
                    return if (clazz.isAssignableFrom(InitializerExpressionFactory::class.java)) {
                        clazz.cast(NullInitializerExpressionFactory.INSTANCE)
                    } else super.unwrap(clazz)
                }
            }
            return relOptTable.toRel(context)
        }
        return if (table is TranslatableTable) {
            (table as TranslatableTable?).toRel(context, this)
        } else LogicalTableScan.create(context.getCluster(), this, context.getTableHints())
    }

    @get:Nullable
    @get:Override
    val collationList: List<Any>
        get() = if (table != null) {
            table.getStatistic().getCollations()
        } else ImmutableList.of()

    @get:Nullable
    @get:Override
    val distribution: RelDistribution
        get() = if (table != null) {
            table.getStatistic().getDistribution()
        } else RelDistributionTraitDef.INSTANCE.getDefault()

    @Override
    fun isKey(columns: ImmutableBitSet?): Boolean {
        return if (table != null) {
            table.getStatistic().isKey(columns)
        } else false
    }

    @get:Nullable
    @get:Override
    val keys: List<Any>
        get() = if (table != null) {
            table.getStatistic().getKeys()
        } else ImmutableList.of()

    @get:Nullable
    @get:Override
    val referentialConstraints: List<Any>
        get() = if (table != null) {
            table.getStatistic().getReferentialConstraints()
        } else ImmutableList.of()

    @Override
    fun getRowType(): RelDataType {
        return rowType
    }

    @Override
    fun supportsModality(modality: SqlModality?): Boolean {
        return when (modality) {
            STREAM -> table is StreamableTable
            else -> table !is StreamableTable
        }
    }

    @get:Override
    val isTemporal: Boolean
        get() = table is TemporalTable

    @get:Override
    val qualifiedName: List<String>
        get() = names

    @Override
    fun getMonotonicity(columnName: String?): SqlMonotonicity {
        if (table == null) {
            return SqlMonotonicity.NOT_MONOTONIC
        }
        val collations: List<RelCollation> =
            table.getStatistic().getCollations() ?: return SqlMonotonicity.NOT_MONOTONIC
        for (collation in collations) {
            val fieldCollation: RelFieldCollation = collation.getFieldCollations().get(0)
            val fieldIndex: Int = fieldCollation.getFieldIndex()
            if (fieldIndex < rowType.getFieldCount()
                && rowType.getFieldNames().get(fieldIndex).equals(columnName)
            ) {
                return fieldCollation.direction.monotonicity()
            }
        }
        return SqlMonotonicity.NOT_MONOTONIC
    }

    @get:Override
    val allowedAccess: SqlAccessType
        get() = SqlAccessType.ALL

    /** Implementation of [SchemaPlus] that wraps a regular schema and knows
     * its name and parent.
     *
     *
     * It is read-only, and functionality is limited in other ways, it but
     * allows table expressions to be generated.  */
    private class MySchemaPlus internal constructor(@Nullable parent: SchemaPlus?, name: String, schema: Schema) :
        SchemaPlus {
        @Nullable
        private val parent: SchemaPlus?

        @get:Override
        val name: String
        private val schema: Schema

        init {
            this.parent = parent
            this.name = name
            this.schema = schema
        }

        @get:Nullable
        @get:Override
        val parentSchema: SchemaPlus?
            get() = parent

        @Override
        @Nullable
        fun getSubSchema(name: String): SchemaPlus? {
            val subSchema: Schema = schema.getSubSchema(name)
            return if (subSchema == null) null else MySchemaPlus(this, name, subSchema)
        }

        @Override
        fun add(name: String?, schema: Schema?): SchemaPlus {
            throw UnsupportedOperationException()
        }

        @Override
        fun add(name: String?, table: Table?) {
            throw UnsupportedOperationException()
        }

        @Override
        fun add(
            name: String?,
            function: org.apache.calcite.schema.Function?
        ) {
            throw UnsupportedOperationException()
        }

        @Override
        fun add(name: String?, type: RelProtoDataType?) {
            throw UnsupportedOperationException()
        }

        @Override
        fun add(name: String?, lattice: Lattice?) {
            throw UnsupportedOperationException()
        }

        @get:Override
        val isMutable: Boolean
            get() = schema.isMutable()

        @Override
        fun <T : Object?> unwrap(clazz: Class<T>?): @Nullable T? {
            return null
        }

        @Override
        fun setPath(path: ImmutableList<ImmutableList<String?>?>?) {
            throw UnsupportedOperationException()
        }

        @get:Override
        @set:Override
        var isCacheEnabled: Boolean
            get() = false
            set(cache) {
                throw UnsupportedOperationException()
            }

        @Override
        @Nullable
        fun getTable(name: String?): Table {
            return schema.getTable(name)
        }

        @get:Override
        val tableNames: Set<String>
            get() = schema.getTableNames()

        @Override
        @Nullable
        fun getType(name: String?): RelProtoDataType {
            return schema.getType(name)
        }

        @get:Override
        val typeNames: Set<String>
            get() = schema.getTypeNames()

        @Override
        fun getFunctions(name: String?): Collection<org.apache.calcite.schema.Function> {
            return schema.getFunctions(name)
        }

        @get:Override
        val functionNames: Set<String>
            get() = schema.getFunctionNames()

        @get:Override
        val subSchemaNames: Set<String>
            get() = schema.getSubSchemaNames()

        @Override
        fun getExpression(
            @Nullable parentSchema: SchemaPlus?,
            name: String?
        ): Expression {
            return schema.getExpression(parentSchema, name)
        }

        @Override
        fun snapshot(version: SchemaVersion?): Schema {
            throw UnsupportedOperationException()
        }

        companion object {
            fun create(path: Path): MySchemaPlus {
                val pair: Pair<String, Schema> = Util.last(path)
                val parent: SchemaPlus?
                parent = if (path.size() === 1) {
                    null
                } else {
                    create(path.parent())
                }
                return MySchemaPlus(parent, pair.left, pair.right)
            }
        }
    }

    companion object {
        fun create(
            @Nullable schema: RelOptSchema?,
            rowType: RelDataType,
            names: List<String>,
            expression: Expression?
        ): RelOptTableImpl {
            return RelOptTableImpl(schema, rowType, names, null,
                Function<Class, Expression> { c -> expression }, null
            )
        }

        fun create(
            @Nullable schema: RelOptSchema?,
            rowType: RelDataType,
            names: List<String>,
            table: Table,
            expression: Expression?
        ): RelOptTableImpl {
            return RelOptTableImpl(schema, rowType, names, table,
                Function<Class, Expression> { c -> expression }, table.getStatistic().getRowCount()
            )
        }

        fun create(
            @Nullable schema: RelOptSchema?, rowType: RelDataType,
            table: Table, path: Path
        ): RelOptTableImpl {
            val schemaPlus: SchemaPlus = MySchemaPlus.create(path)
            return RelOptTableImpl(
                schema, rowType, Pair.left(path), table,
                getClassExpressionFunction(schemaPlus, Util.last(path).left, table),
                table.getStatistic().getRowCount()
            )
        }

        fun create(
            @Nullable schema: RelOptSchema?, rowType: RelDataType,
            tableEntry: CalciteSchema.TableEntry, @Nullable rowCount: Double
        ): RelOptTableImpl {
            val table: Table = tableEntry.getTable()
            return RelOptTableImpl(
                schema, rowType, tableEntry.path(),
                table, getClassExpressionFunction(tableEntry, table), rowCount
            )
        }

        private fun getClassExpressionFunction(
            tableEntry: CalciteSchema.TableEntry, table: Table
        ): Function<Class, Expression> {
            return getClassExpressionFunction(
                tableEntry.schema.plus(), tableEntry.name,
                table
            )
        }

        private fun getClassExpressionFunction(
            schema: SchemaPlus, tableName: String, table: Table
        ): Function<Class, Expression> {
            return if (table is QueryableTable) {
                val queryableTable: QueryableTable = table as QueryableTable
                Function<Class, Expression> { clazz -> queryableTable.getExpression(schema, tableName, clazz) }
            } else if (table is ScannableTable
                || table is FilterableTable
                || table is ProjectableFilterableTable
            ) {
                Function<Class, Expression> { clazz ->
                    Schemas.tableExpression(
                        schema, Array<Object>::class.java, tableName,
                        table.getClass()
                    )
                }
            } else if (table is StreamableTable) {
                getClassExpressionFunction(
                    schema, tableName,
                    (table as StreamableTable).stream()
                )
            } else {
                Function<Class, Expression> { input -> throw UnsupportedOperationException() }
            }
        }

        fun create(
            @Nullable schema: RelOptSchema?,
            rowType: RelDataType, table: Table?, names: ImmutableList<String>
        ): RelOptTableImpl {
            assert(
                table is TranslatableTable
                        || table is ScannableTable
                        || table is ModifiableTable
            )
            return RelOptTableImpl(schema, rowType, names, table, null, null)
        }

        /** Helper for [.getColumnStrategies].  */
        fun columnStrategies(table: RelOptTable): List<ColumnStrategy> {
            val fieldCount: Int = table.getRowType().getFieldCount()
            val ief: InitializerExpressionFactory = Util.first(
                table.unwrap(InitializerExpressionFactory::class.java),
                NullInitializerExpressionFactory.INSTANCE
            )
            return object : AbstractList<ColumnStrategy?>() {
                @Override
                fun size(): Int {
                    return fieldCount
                }

                @Override
                operator fun get(index: Int): ColumnStrategy {
                    return ief.generationStrategy(table, index)
                }
            }
        }

        /** Converts the ordinal of a field into the ordinal of a stored field.
         * That is, it subtracts the number of virtual fields that come before it.  */
        fun realOrdinal(table: RelOptTable, i: Int): Int {
            val strategies: List<ColumnStrategy> = table.getColumnStrategies()
            var n = 0
            for (j in 0 until i) {
                when (strategies[j]) {
                    VIRTUAL -> ++n
                    else -> {}
                }
            }
            return i - n
        }

        /** Returns the row type of a table after any [ColumnStrategy.VIRTUAL]
         * columns have been removed. This is the type of the records that are
         * actually stored.  */
        fun realRowType(table: RelOptTable): RelDataType {
            val rowType: RelDataType = table.getRowType()
            val strategies: List<ColumnStrategy> = columnStrategies(table)
            if (!strategies.contains(ColumnStrategy.VIRTUAL)) {
                return rowType
            }
            val builder: RelDataTypeFactory.Builder = requireNonNull(
                table.getRelOptSchema()
            ) { "relOptSchema for table $table" }.getTypeFactory().builder()
            for (field in rowType.getFieldList()) {
                if (strategies[field.getIndex()] !== ColumnStrategy.VIRTUAL) {
                    builder.add(field)
                }
            }
            return builder.build()
        }
    }
}
