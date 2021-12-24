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

import org.apache.calcite.DataContext

/**
 * Abstract base for classes that implement
 * the process of preparing and executing SQL expressions.
 */
abstract class Prepare protected constructor(
    context: CalcitePrepare.Context?, catalogReader: CatalogReader,
    resultConvention: Convention
) {
    protected val context: CalcitePrepare.Context?
    protected val catalogReader: CatalogReader

    /**
     * Convention via which results should be returned by execution.
     */
    val resultConvention: Convention

    @Nullable
    protected var timingTracer: CalciteTimingTracer? = null

    @MonotonicNonNull
    protected var fieldOrigins: List<List<String>>? = null

    @MonotonicNonNull
    protected var parameterRowType: RelDataType? = null

    init {
        assert(context != null)
        this.context = context
        this.catalogReader = catalogReader
        this.resultConvention = resultConvention
    }

    protected abstract fun createPreparedExplanation(
        @Nullable resultType: RelDataType?,
        parameterRowType: RelDataType?,
        @Nullable root: RelRoot?,
        format: SqlExplainFormat?,
        detailLevel: SqlExplainLevel?
    ): PreparedResult

    /**
     * Optimizes a query plan.
     *
     * @param root Root of relational expression tree
     * @param materializations Tables known to be populated with a given query
     * @param lattices Lattices
     * @return an equivalent optimized relational expression
     */
    protected fun optimize(
        root: RelRoot,
        materializations: List<Materialization>,
        lattices: List<CalciteSchema.LatticeEntry?>
    ): RelRoot {
        val planner: RelOptPlanner = root.rel.getCluster().getPlanner()
        val dataContext: DataContext = context.getDataContext()
        planner.setExecutor(RexExecutorImpl(dataContext))
        val materializationList: List<RelOptMaterialization> = ArrayList(materializations.size())
        for (materialization in materializations) {
            val qualifiedTableName: List<String> = materialization.materializedTable.path()
            materializationList.add(
                RelOptMaterialization(
                    castNonNull(materialization.tableRel),
                    castNonNull(materialization.queryRel),
                    materialization.starRelOptTable,
                    qualifiedTableName
                )
            )
        }
        val latticeList: List<RelOptLattice> = ArrayList(lattices.size())
        for (lattice in lattices) {
            val starTable: CalciteSchema.TableEntry = lattice.getStarTable()
            val typeFactory: JavaTypeFactory = context.getTypeFactory()
            val starRelOptTable: RelOptTableImpl = RelOptTableImpl.create(
                catalogReader,
                starTable.getTable().getRowType(typeFactory), starTable, null
            )
            latticeList.add(
                RelOptLattice(lattice.getLattice(), starRelOptTable)
            )
        }
        val desiredTraits: RelTraitSet = getDesiredRootTraitSet(root)
        val program: Program = program
        val rootRel4: RelNode = program.run(
            planner, root.rel, desiredTraits, materializationList, latticeList
        )
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "Plan after physical tweaks:\n{}",
                RelOptUtil.toString(rootRel4, SqlExplainLevel.ALL_ATTRIBUTES)
            )
        }
        return root.withRel(rootRel4)
    }

    // Allow a test to override the default program.
    protected val program: Program
        protected get() {
            // Allow a test to override the default program.
            val holder: Holder<Program> = Holder.empty()
            Hook.PROGRAM.run(holder)
            @Nullable val holderValue: Program = holder.get()
            return if (holderValue != null) {
                holderValue
            } else Programs.standard()
        }

    protected fun getDesiredRootTraitSet(root: RelRoot): RelTraitSet {
        // Make sure non-CallingConvention traits, if any, are preserved
        return root.rel.getTraitSet()
            .replace(resultConvention)
            .replace(root.collation)
            .simplify()
    }

    /**
     * Implements a physical query plan.
     *
     * @param root Root of the relational expression tree
     * @return an executable plan
     */
    protected abstract fun implement(root: RelRoot?): PreparedResult
    fun prepareSql(
        sqlQuery: SqlNode,
        runtimeContextClass: Class?,
        validator: SqlValidator,
        needsValidation: Boolean
    ): PreparedResult {
        return prepareSql(
            sqlQuery,
            sqlQuery,
            runtimeContextClass,
            validator,
            needsValidation
        )
    }

    fun prepareSql(
        sqlQuery: SqlNode,
        sqlNodeOriginal: SqlNode,
        runtimeContextClass: Class?,
        validator: SqlValidator,
        needsValidation: Boolean
    ): PreparedResult {
        var sqlQuery: SqlNode = sqlQuery
        init(runtimeContextClass)
        val config: SqlToRelConverter.Config = SqlToRelConverter.config()
            .withTrimUnusedFields(true)
            .withExpand(castNonNull(THREAD_EXPAND.get()))
            .withInSubQueryThreshold(castNonNull(THREAD_INSUBQUERY_THRESHOLD.get()))
            .withExplain(sqlQuery.getKind() === SqlKind.EXPLAIN)
        val configHolder: Holder<SqlToRelConverter.Config> = Holder.of(config)
        Hook.SQL2REL_CONVERTER_CONFIG_BUILDER.run(configHolder)
        val sqlToRelConverter: SqlToRelConverter = getSqlToRelConverter(validator, catalogReader, configHolder.get())
        var sqlExplain: SqlExplain? = null
        if (sqlQuery.getKind() === SqlKind.EXPLAIN) {
            // dig out the underlying SQL statement
            sqlExplain = sqlQuery as SqlExplain
            sqlQuery = sqlExplain.getExplicandum()
            sqlToRelConverter.setDynamicParamCountInExplain(
                sqlExplain.getDynamicParamCount()
            )
        }
        var root: RelRoot = sqlToRelConverter.convertQuery(sqlQuery, needsValidation, true)
        Hook.CONVERTED.run(root.rel)
        if (timingTracer != null) {
            timingTracer.traceTime("end sql2rel")
        }
        val resultType: RelDataType = validator.getValidatedNodeType(sqlQuery)
        fieldOrigins = validator.getFieldOrigins(sqlQuery)
        assert(fieldOrigins!!.size() === resultType.getFieldCount())
        parameterRowType = validator.getParameterRowType(sqlQuery)

        // Display logical plans before view expansion, plugging in physical
        // storage and decorrelation
        if (sqlExplain != null) {
            val explainDepth: Depth = sqlExplain.getDepth()
            val format: SqlExplainFormat = sqlExplain.getFormat()
            val detailLevel: SqlExplainLevel = sqlExplain.getDetailLevel()
            when (explainDepth) {
                TYPE -> return createPreparedExplanation(
                    resultType, parameterRowType, null,
                    format, detailLevel
                )
                LOGICAL -> return createPreparedExplanation(
                    null, parameterRowType, root, format,
                    detailLevel
                )
                else -> {}
            }
        }

        // Structured type flattening, view expansion, and plugging in physical
        // storage.
        root = root.withRel(flattenTypes(root.rel, true))
        if (context.config().forceDecorrelate()) {
            // Sub-query decorrelation.
            root = root.withRel(decorrelate(sqlToRelConverter, sqlQuery, root.rel))
        }

        // Trim unused fields.
        root = trimUnusedFields(root)
        Hook.TRIMMED.run(root.rel)

        // Display physical plan after decorrelation.
        if (sqlExplain != null) {
            return when (sqlExplain.getDepth()) {
                PHYSICAL -> {
                    root = optimize(root, materializations, lattices)
                    createPreparedExplanation(
                        null, parameterRowType, root,
                        sqlExplain.getFormat(), sqlExplain.getDetailLevel()
                    )
                }
                else -> {
                    root = optimize(root, materializations, lattices)
                    createPreparedExplanation(
                        null, parameterRowType, root,
                        sqlExplain.getFormat(), sqlExplain.getDetailLevel()
                    )
                }
            }
        }
        root = optimize(root, materializations, lattices)
        if (timingTracer != null) {
            timingTracer.traceTime("end optimization")
        }

        // For transformation from DML -> DML, use result of rewrite
        // (e.g. UPDATE -> MERGE).  For anything else (e.g. CALL -> SELECT),
        // use original kind.
        if (!root.kind.belongsTo(SqlKind.DML)) {
            root = root.withKind(sqlNodeOriginal.getKind())
        }
        return implement(root)
    }

    protected fun mapTableModOp(
        isDml: Boolean, sqlKind: SqlKind?
    ): @Nullable TableModify.Operation? {
        return if (!isDml) {
            null
        } else when (sqlKind) {
            INSERT -> TableModify.Operation.INSERT
            DELETE -> TableModify.Operation.DELETE
            MERGE -> TableModify.Operation.MERGE
            UPDATE -> TableModify.Operation.UPDATE
            else -> null
        }
    }

    /**
     * Protected method to allow subclasses to override construction of
     * SqlToRelConverter.
     */
    protected abstract fun getSqlToRelConverter(
        validator: SqlValidator?,
        catalogReader: CatalogReader?,
        config: SqlToRelConverter.Config?
    ): SqlToRelConverter

    abstract fun flattenTypes(
        rootRel: RelNode?,
        restructure: Boolean
    ): RelNode?

    protected abstract fun decorrelate(
        sqlToRelConverter: SqlToRelConverter?,
        query: SqlNode?, rootRel: RelNode?
    ): RelNode?

    protected abstract val materializations: List<Materialization>
    protected abstract val lattices: List<Any?>

    /**
     * Walks over a tree of relational expressions, replacing each
     * [org.apache.calcite.rel.RelNode] with a 'slimmed down' relational
     * expression that projects
     * only the columns required by its consumer.
     *
     * @param root Root of relational expression tree
     * @return Trimmed relational expression
     */
    protected fun trimUnusedFields(root: RelRoot): RelRoot {
        val config: SqlToRelConverter.Config = SqlToRelConverter.config()
            .withTrimUnusedFields(shouldTrim(root.rel))
            .withExpand(castNonNull(THREAD_EXPAND.get()))
            .withInSubQueryThreshold(castNonNull(THREAD_INSUBQUERY_THRESHOLD.get()))
        val converter: SqlToRelConverter = getSqlToRelConverter(sqlValidator, catalogReader, config)
        val ordered: Boolean = !root.collation.getFieldCollations().isEmpty()
        val dml: Boolean = SqlKind.DML.contains(root.kind)
        return root.withRel(converter.trimUnusedFields(dml || ordered, root.rel))
    }

    protected abstract fun init(runtimeContextClass: Class?)
    protected abstract val sqlValidator: SqlValidator?

    /** Interface by which validator and planner can read table metadata.  */
    interface CatalogReader : RelOptSchema, SqlValidatorCatalogReader, SqlOperatorTable {
        @Override
        @Nullable
        fun getTableForMember(names: List<String?>?): PreparingTable?

        /** Returns a catalog reader the same as this one but with a possibly
         * different schema path.  */
        fun withSchemaPath(schemaPath: List<String?>?): CatalogReader

        @Override
        @Nullable
        fun getTable(names: List<String?>?): PreparingTable?

        companion object {
            val THREAD_LOCAL: ThreadLocal<CatalogReader> = ThreadLocal()
        }
    }

    /** Definition of a table, for the purposes of the validator and planner.  */
    interface PreparingTable : RelOptTable, SqlValidatorTable

    /** Abstract implementation of [PreparingTable] with an implementation
     * for [.columnHasDefaultValue].  */
    abstract class AbstractPreparingTable : PreparingTable {
        @SuppressWarnings("deprecation")
        @Override
        fun columnHasDefaultValue(
            rowType: RelDataType, ordinal: Int,
            initializerContext: InitializerContext?
        ): Boolean {
            // This method is no longer used
            val table: Table = this.unwrap(Table::class.java)
            if (table is Wrapper) {
                val initializerExpressionFactory: InitializerExpressionFactory = (table as Wrapper).unwrap(
                    InitializerExpressionFactory::class.java
                )
                if (initializerExpressionFactory != null) {
                    return initializerExpressionFactory
                        .newColumnDefaultValue(this, ordinal, initializerContext)
                        .getType().getSqlTypeName() !== SqlTypeName.NULL
                }
            }
            return if (ordinal >= rowType.getFieldList().size()) {
                true
            } else !rowType.getFieldList().get(ordinal).getType().isNullable()
        }

        @Override
        fun extend(extendedFields: List<RelDataTypeField?>?): RelOptTable {
            val table: Table = unwrap(Table::class.java)

            // Get the set of extended columns that do not have the same name as a column
            // in the base table.
            val baseColumns: List<RelDataTypeField> = getRowType().getFieldList()
            val dedupedFields: List<RelDataTypeField> = RelOptUtil.deduplicateColumns(baseColumns, extendedFields)
            val dedupedExtendedFields: List<RelDataTypeField> =
                dedupedFields.subList(baseColumns.size(), dedupedFields.size())
            if (table is ExtensibleTable) {
                val extendedTable: Table = (table as ExtensibleTable).extend(dedupedExtendedFields)
                return extend(extendedTable)
            } else if (table is ModifiableViewTable) {
                val modifiableViewTable: ModifiableViewTable = table as ModifiableViewTable
                val extendedView: ModifiableViewTable = modifiableViewTable.extend(dedupedExtendedFields,
                    requireNonNull(
                        getRelOptSchema()
                    ) { "relOptSchema for table " + getQualifiedName() }.getTypeFactory()
                )
                return extend(extendedView)
            }
            throw RuntimeException("Cannot extend $table")
        }

        /** Implementation-specific code to instantiate a new [RelOptTable]
         * based on a [Table] that has been extended.  */
        protected abstract fun extend(extendedTable: Table?): RelOptTable?

        @get:Override
        val columnStrategies: List<Any>
            get() = RelOptTableImpl.columnStrategies(this@AbstractPreparingTable)
    }

    /**
     * PreparedExplanation is a PreparedResult for an EXPLAIN PLAN statement.
     * It's always good to have an explanation prepared.
     */
    abstract class PreparedExplain protected constructor(
        @Nullable rowType: RelDataType?,
        parameterRowType: RelDataType,
        @Nullable root: RelRoot?,
        format: SqlExplainFormat,
        detailLevel: SqlExplainLevel
    ) : PreparedResult {
        @Nullable
        private val rowType: RelDataType?
        private val parameterRowType: RelDataType

        @Nullable
        private val root: RelRoot?
        private val format: SqlExplainFormat
        private val detailLevel: SqlExplainLevel

        init {
            this.rowType = rowType
            this.parameterRowType = parameterRowType
            this.root = root
            this.format = format
            this.detailLevel = detailLevel
        }

        @get:Override
        override val code: String?
            get() = if (root == null) {
                if (rowType == null) "rowType is null" else RelOptUtil.dumpType(rowType)
            } else {
                RelOptUtil.dumpPlan("", root.rel, format, detailLevel)
            }

        @Override
        override fun getParameterRowType(): RelDataType {
            return parameterRowType
        }

        @get:Override
        override val isDml: Boolean
            get() = false

        @get:Override
        override val tableModOp: @Nullable TableModify.Operation?
            get() = null

        @Override
        override fun getFieldOrigins(): List<List<String>> {
            return Collections.singletonList(
                Collections.nCopies(4, null)
            )
        }
    }

    /**
     * Result of a call to [Prepare.prepareSql].
     */
    interface PreparedResult {
        /**
         * Returns the code generated by preparation.
         */
        val code: String?

        /**
         * Returns whether this result is for a DML statement, in which case the
         * result set is one row with one column containing the number of rows
         * affected.
         */
        val isDml: Boolean

        /**
         * Returns the table modification operation corresponding to this
         * statement if it is a table modification statement; otherwise null.
         */
        val tableModOp: @Nullable TableModify.Operation?

        /**
         * Returns a list describing, for each result field, the origin of the
         * field as a 4-element list of (database, schema, table, column).
         */
        fun getFieldOrigins(): List<List<String?>?>

        /**
         * Returns a record type whose fields are the parameters of this statement.
         */
        fun getParameterRowType(): RelDataType

        /**
         * Executes the prepared result.
         *
         * @param cursorFactory How to map values into a cursor
         * @return producer of rows resulting from execution
         */
        fun getBindable(cursorFactory: Meta.CursorFactory?): Bindable
    }

    /**
     * Abstract implementation of [PreparedResult].
     */
    abstract class PreparedResultImpl protected constructor(
        rowType: RelDataType?,
        parameterRowType: RelDataType?,
        fieldOrigins: List<List<String?>?>?,
        collations: List<RelCollation?>?,
        rootRel: RelNode?,
        tableModOp: @Nullable TableModify.Operation?,
        isDml: Boolean
    ) : PreparedResult, Typed {
        protected val rootRel: RelNode
        protected val parameterRowType: RelDataType
        protected val rowType: RelDataType

        @get:Override
        override val isDml: Boolean
        protected override val tableModOp: @Nullable TableModify.Operation?
        protected val fieldOrigins: List<List<String?>?>
        val collations: List<RelCollation>

        init {
            this.rowType = requireNonNull(rowType, "rowType")
            this.parameterRowType = requireNonNull(parameterRowType, "parameterRowType")
            this.fieldOrigins = requireNonNull(fieldOrigins, "fieldOrigins")
            this.collations = ImmutableList.copyOf(collations)
            this.rootRel = requireNonNull(rootRel, "rootRel")
            this.tableModOp = tableModOp
            this.isDml = isDml
        }

        @Override
        override fun getTableModOp(): @Nullable TableModify.Operation? {
            return tableModOp
        }

        @Override
        override fun getFieldOrigins(): List<List<String?>?> {
            return fieldOrigins
        }

        @Override
        override fun getParameterRowType(): RelDataType {
            return parameterRowType
        }

        /**
         * Returns the physical row type of this prepared statement. May not be
         * identical to the row type returned by the validator; for example, the
         * field names may have been made unique.
         */
        val physicalRowType: RelDataType
            get() = rowType

        @get:Override
        abstract val elementType: Type?
        fun getRootRel(): RelNode {
            return rootRel
        }
    }

    /** Describes that a given SQL query is materialized by a given table.
     * The materialization is currently valid, and can be used in the planning
     * process.  */
    class Materialization(
        materializedTable: CalciteSchema.TableEntry?,
        sql: String?, viewSchemaPath: List<String>
    ) {
        /** The table that holds the materialized data.  */
        val materializedTable: CalciteSchema.TableEntry?

        /** The query that derives the data.  */
        val sql: String?

        /** The schema path for the query.  */
        val viewSchemaPath: List<String>

        /** Relational expression for the table. Usually a
         * [org.apache.calcite.rel.logical.LogicalTableScan].  */
        @Nullable
        var tableRel: RelNode? = null

        /** Relational expression for the query to populate the table.  */
        @Nullable
        var queryRel: RelNode? = null

        /** Star table identified.  */
        @Nullable
        var starRelOptTable: RelOptTable? = null

        init {
            assert(materializedTable != null)
            assert(sql != null)
            this.materializedTable = materializedTable
            this.sql = sql
            this.viewSchemaPath = viewSchemaPath
        }

        fun materialize(
            queryRel: RelNode?,
            starRelOptTable: RelOptTable
        ) {
            this.queryRel = queryRel
            this.starRelOptTable = starRelOptTable
            assert(starRelOptTable.maybeUnwrap(StarTable::class.java).isPresent())
        }
    }

    companion object {
        protected val LOGGER: Logger = CalciteTrace.getStatementTracer()

        // temporary. for testing.
        val THREAD_TRIM: TryThreadLocal<Boolean> = TryThreadLocal.of(false)

        /** Temporary, until
         * [[CALCITE-1045]
 * Decorrelate sub-queries in Project and Join](https://issues.apache.org/jira/browse/CALCITE-1045) is fixed.
         *
         *
         * The default is false, meaning do not expand queries during sql-to-rel,
         * but a few tests override and set it to true. After CALCITE-1045
         * is fixed, remove those overrides and use false everywhere.  */
        val THREAD_EXPAND: TryThreadLocal<Boolean> = TryThreadLocal.of(false)

        // temporary. for testing.
        val THREAD_INSUBQUERY_THRESHOLD: TryThreadLocal<Integer> = TryThreadLocal.of(DEFAULT_IN_SUB_QUERY_THRESHOLD)
        private fun shouldTrim(rootRel: RelNode): Boolean {
            // For now, don't trim if there are more than 3 joins. The projects
            // near the leaves created by trim migrate past joins and seem to
            // prevent join-reordering.
            return castNonNull(THREAD_TRIM.get()) || RelOptUtil.countJoins(rootRel) < 2
        }
    }
}
