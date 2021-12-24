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

import org.apache.calcite.adapter.enumerable.EnumerableCalc

/**
 * Shit just got real.
 *
 *
 * This class is public so that projects that create their own JDBC driver
 * and server can fine-tune preferences. However, this class and its methods are
 * subject to change without notice.
 */
class CalcitePrepareImpl : CalcitePrepare {
    /** Whether the bindable convention should be the root convention of any
     * plan. If not, enumerable convention is the default.  */
    val enableBindable: Boolean = Hook.ENABLE_BINDABLE.get(false)
    @Override
    fun parse(
        context: Context, sql: String
    ): ParseResult {
        return parse_(context, sql, false, false, false)
    }

    @Override
    fun convert(context: Context, sql: String): ConvertResult {
        return parse_(context, sql, true, false, false) as ConvertResult
    }

    @Override
    fun analyzeView(context: Context, sql: String, fail: Boolean): AnalyzeViewResult {
        return parse_(context, sql, true, true, fail) as AnalyzeViewResult
    }

    /** Shared implementation for [.parse], [.convert] and
     * [.analyzeView].  */
    private fun parse_(
        context: Context, sql: String, convert: Boolean,
        analyze: Boolean, fail: Boolean
    ): ParseResult {
        val typeFactory: JavaTypeFactory = context.getTypeFactory()
        val catalogReader = CalciteCatalogReader(
            context.getRootSchema(),
            context.getDefaultSchemaPath(),
            typeFactory,
            context.config()
        )
        val parser: SqlParser = createParser(sql)
        val sqlNode: SqlNode
        sqlNode = try {
            parser.parseStmt()
        } catch (e: SqlParseException) {
            throw RuntimeException("parse failed", e)
        }
        val validator: SqlValidator = createSqlValidator(context, catalogReader)
        val sqlNode1: SqlNode = validator.validate(sqlNode)
        return if (convert) {
            convert_(
                context, sql, analyze, fail, catalogReader, validator, sqlNode1
            )
        } else ParseResult(
            this, validator, sql, sqlNode1,
            validator.getValidatedNodeType(sqlNode1)
        )
    }

    private fun convert_(
        context: Context, sql: String, analyze: Boolean,
        fail: Boolean, catalogReader: CalciteCatalogReader, validator: SqlValidator,
        sqlNode1: SqlNode
    ): ParseResult {
        val typeFactory: JavaTypeFactory = context.getTypeFactory()
        val resultConvention: Convention =
            if (enableBindable) BindableConvention.INSTANCE else EnumerableConvention.INSTANCE
        // Use the Volcano because it can handle the traits.
        val planner = VolcanoPlanner()
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE)
        val config: SqlToRelConverter.Config = SqlToRelConverter.config().withTrimUnusedFields(true)
        val preparingStmt = CalcitePreparingStmt(
            this, context, catalogReader, typeFactory,
            context.getRootSchema(), null,
            createCluster(planner, RexBuilder(typeFactory)),
            resultConvention, createConvertletTable()
        )
        val converter: SqlToRelConverter = preparingStmt.getSqlToRelConverter(validator, catalogReader, config)
        val root: RelRoot = converter.convertQuery(sqlNode1, false, true)
        return if (analyze) {
            analyze_(validator, sql, sqlNode1, root, fail)
        } else ConvertResult(
            this, validator, sql, sqlNode1,
            validator.getValidatedNodeType(sqlNode1), root
        )
    }

    private fun analyze_(
        validator: SqlValidator, sql: String,
        sqlNode: SqlNode, root: RelRoot, fail: Boolean
    ): AnalyzeViewResult {
        val rexBuilder: RexBuilder = root.rel.getCluster().getRexBuilder()
        var rel: RelNode = root.rel
        val viewRel: RelNode = rel
        val project: Project?
        if (rel is Project) {
            project = rel as Project
            rel = project.getInput()
        } else {
            project = null
        }
        val filter: Filter?
        if (rel is Filter) {
            filter = rel as Filter
            rel = filter.getInput()
        } else {
            filter = null
        }
        val scan: TableScan?
        scan = if (rel is TableScan) {
            rel as TableScan
        } else {
            null
        }
        if (scan == null) {
            if (fail) {
                throw validator.newValidationError(
                    sqlNode,
                    RESOURCE.modifiableViewMustBeBasedOnSingleTable()
                )
            }
            return AnalyzeViewResult(
                this, validator, sql, sqlNode,
                validator.getValidatedNodeType(sqlNode), root, null, null, null,
                null, false
            )
        }
        val targetRelTable: RelOptTable = scan.getTable()
        val targetRowType: RelDataType = targetRelTable.getRowType()
        val table: Table = targetRelTable.unwrapOrThrow(Table::class.java)
        val tablePath: List<String> = targetRelTable.getQualifiedName()
        val columnMapping: List<Integer>
        val projectMap: Map<Integer, RexNode?> = HashMap()
        if (project == null) {
            columnMapping = ImmutableIntList.range(0, targetRowType.getFieldCount())
        } else {
            columnMapping = ArrayList()
            for (node in Ord.zip(project.getProjects())) {
                if (node.e is RexInputRef) {
                    val rexInputRef: RexInputRef = node.e as RexInputRef
                    val index: Int = rexInputRef.getIndex()
                    if (projectMap[index] != null) {
                        if (fail) {
                            throw validator.newValidationError(
                                sqlNode,
                                RESOURCE.moreThanOneMappedColumn(
                                    targetRowType.getFieldList().get(index).getName(),
                                    Util.last(tablePath)
                                )
                            )
                        }
                        return AnalyzeViewResult(
                            this, validator, sql, sqlNode,
                            validator.getValidatedNodeType(sqlNode), root, null, null, null,
                            null, false
                        )
                    }
                    projectMap.put(index, rexBuilder.makeInputRef(viewRel, node.i))
                    columnMapping.add(index)
                } else {
                    columnMapping.add(-1)
                }
            }
        }
        val constraint: RexNode
        constraint = if (filter != null) {
            filter.getCondition()
        } else {
            rexBuilder.makeLiteral(true)
        }
        val filters: List<RexNode> = ArrayList()
        // If we put a constraint in projectMap above, then filters will not be empty despite
        // being a modifiable view.
        val filters2: List<RexNode> = ArrayList()
        var retry = false
        RelOptUtil.inferViewPredicates(projectMap, filters, constraint)
        if (fail && !filters.isEmpty()) {
            val projectMap2: Map<Integer, RexNode> = HashMap()
            RelOptUtil.inferViewPredicates(projectMap2, filters2, constraint)
            if (!filters2.isEmpty()) {
                throw validator.newValidationError(
                    sqlNode,
                    RESOURCE.modifiableViewMustHaveOnlyEqualityPredicates()
                )
            }
            retry = true
        }

        // Check that all columns that are not projected have a constant value
        for (field in targetRowType.getFieldList()) {
            val x = columnMapping.indexOf(field.getIndex())
            if (x >= 0) {
                assert(
                    Util.skip(columnMapping, x + 1).indexOf(field.getIndex()) < 0
                ) { "column projected more than once; should have checked above" }
                continue  // target column is projected
            }
            if (projectMap[field.getIndex()] != null) {
                continue  // constant expression
            }
            if (field.getType().isNullable()) {
                continue  // don't need expression for nullable columns; NULL suffices
            }
            if (fail) {
                throw validator.newValidationError(
                    sqlNode,
                    RESOURCE.noValueSuppliedForViewColumn(
                        field.getName(),
                        Util.last(tablePath)
                    )
                )
            }
            return AnalyzeViewResult(
                this, validator, sql, sqlNode,
                validator.getValidatedNodeType(sqlNode), root, null, null, null,
                null, false
            )
        }
        val modifiable = filters.isEmpty() || retry && filters2.isEmpty()
        return AnalyzeViewResult(
            this, validator, sql, sqlNode,
            validator.getValidatedNodeType(sqlNode), root, if (modifiable) table else null,
            ImmutableList.copyOf(tablePath),
            constraint, ImmutableIntList.copyOf(columnMapping),
            modifiable
        )
    }

    @Override
    fun executeDdl(context: Context, node: SqlNode?) {
        val config: CalciteConnectionConfig = context.config()
        val parserFactory: SqlParserImplFactory =
            config.parserFactory(SqlParserImplFactory::class.java, SqlParserImpl.FACTORY)
        val ddlExecutor: DdlExecutor = parserFactory.getDdlExecutor()
        ddlExecutor.executeDdl(context, node)
    }

    /** Factory method for default SQL parser.  */
    protected fun createParser(sql: String?): SqlParser {
        return createParser(sql, createParserConfig())
    }

    /** Factory method for SQL parser with a given configuration.  */
    protected fun createParser(sql: String?, parserConfig: SqlParser.Config?): SqlParser {
        return SqlParser.create(sql, parserConfig)
    }

    @Deprecated // to be removed before 2.0
    protected fun createParser(
        sql: String?,
        parserConfig: SqlParser.ConfigBuilder
    ): SqlParser {
        return createParser(sql, parserConfig.build())
    }

    /** Factory method for SQL parser configuration.  */
    protected fun parserConfig(): SqlParser.Config {
        return SqlParser.config()
    }

    @Deprecated // to be removed before 2.0
    protected fun createParserConfig(): SqlParser.ConfigBuilder {
        return SqlParser.configBuilder()
    }

    /** Factory method for default convertlet table.  */
    protected fun createConvertletTable(): SqlRexConvertletTable {
        return StandardConvertletTable.INSTANCE
    }

    /** Factory method for cluster.  */
    protected fun createCluster(
        planner: RelOptPlanner?,
        rexBuilder: RexBuilder?
    ): RelOptCluster {
        return RelOptCluster.create(planner, rexBuilder)
    }

    /** Creates a collection of planner factories.
     *
     *
     * The collection must have at least one factory, and each factory must
     * create a planner. If the collection has more than one planner, Calcite will
     * try each planner in turn.
     *
     *
     * One of the things you can do with this mechanism is to try a simpler,
     * faster, planner with a smaller rule set first, then fall back to a more
     * complex planner for complex and costly queries.
     *
     *
     * The default implementation returns a factory that calls
     * [.createPlanner].
     */
    protected fun createPlannerFactories(): List<Function1<Context, RelOptPlanner>> {
        return Collections.singletonList { context -> createPlanner(context, null, null) }
    }

    /** Creates a query planner and initializes it with a default set of
     * rules.  */
    protected fun createPlanner(prepareContext: CalcitePrepare.Context): RelOptPlanner {
        return createPlanner(prepareContext, null, null)
    }

    /** Creates a query planner and initializes it with a default set of
     * rules.  */
    protected fun createPlanner(
        prepareContext: CalcitePrepare.Context,
        externalContext: @Nullable org.apache.calcite.plan.Context?,
        @Nullable costFactory: RelOptCostFactory?
    ): RelOptPlanner {
        var externalContext: org.apache.calcite.plan.Context? = externalContext
        if (externalContext == null) {
            externalContext = Contexts.of(prepareContext.config())
        }
        val planner = VolcanoPlanner(costFactory, externalContext)
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE)
        if (CalciteSystemProperty.ENABLE_COLLATION_TRAIT.value()) {
            planner.addRelTraitDef(RelCollationTraitDef.INSTANCE)
        }
        planner.setTopDownOpt(prepareContext.config().topDownOpt())
        RelOptUtil.registerDefaultRules(
            planner,
            prepareContext.config().materializationsEnabled(),
            enableBindable
        )
        val spark: CalcitePrepare.SparkHandler = prepareContext.spark()
        if (spark.enabled()) {
            spark.registerRules(
                object : RuleSetBuilder() {
                    @Override
                    fun addRule(rule: RelOptRule?) {
                        // TODO:
                    }

                    @Override
                    fun removeRule(rule: RelOptRule?) {
                        // TODO:
                    }
                })
        }
        Hook.PLANNER.run(planner) // allow test to add or remove rules
        return planner
    }

    @Override
    fun <T> prepareQueryable(
        context: Context,
        queryable: Queryable<T>
    ): CalciteSignature<T> {
        return prepare_(
            context, Query.of(queryable), queryable.getElementType(),
            -1
        )
    }

    @Override
    fun <T> prepareSql(
        context: Context,
        query: Query<T>,
        elementType: Type?,
        maxRowCount: Long
    ): CalciteSignature<T> {
        return prepare_<Any>(context, query, elementType, maxRowCount)
    }

    fun <T> prepare_(
        context: Context,
        query: Query<T>,
        elementType: Type?,
        maxRowCount: Long
    ): CalciteSignature<T> {
        if (SIMPLE_SQLS.contains(query.sql)) {
            return simplePrepare(context, castNonNull(query.sql))
        }
        val typeFactory: JavaTypeFactory = context.getTypeFactory()
        val catalogReader = CalciteCatalogReader(
            context.getRootSchema(),
            context.getDefaultSchemaPath(),
            typeFactory,
            context.config()
        )
        val plannerFactories: List<Function1<Context, RelOptPlanner>> = createPlannerFactories()
        if (plannerFactories.isEmpty()) {
            throw AssertionError("no planner factories")
        }
        var exception: RuntimeException = Util.FoundOne.NULL
        for (plannerFactory in plannerFactories) {
            val planner: RelOptPlanner =
                plannerFactory.apply(context) ?: throw AssertionError("factory returned null planner")
            exception = try {
                return prepare2_<Any>(
                    context, query, elementType, maxRowCount,
                    catalogReader, planner
                )
            } catch (e: RelOptPlanner.CannotPlanException) {
                e
            }
        }
        throw exception
    }

    fun <T> prepare2_(
        context: Context,
        query: Query<T>,
        elementType: Type,
        maxRowCount: Long,
        catalogReader: CalciteCatalogReader,
        planner: RelOptPlanner?
    ): CalciteSignature<T> {
        val typeFactory: JavaTypeFactory = context.getTypeFactory()
        val prefer: EnumerableRel.Prefer
        prefer = if (elementType === Array<Object>::class.java) {
            EnumerableRel.Prefer.ARRAY
        } else {
            EnumerableRel.Prefer.CUSTOM
        }
        val resultConvention: Convention =
            if (enableBindable) BindableConvention.INSTANCE else EnumerableConvention.INSTANCE
        val preparingStmt = CalcitePreparingStmt(
            this, context, catalogReader, typeFactory,
            context.getRootSchema(), prefer, createCluster(planner, RexBuilder(typeFactory)),
            resultConvention, createConvertletTable()
        )
        val x: RelDataType
        val preparedResult: Prepare.PreparedResult
        val statementType: Meta.StatementType
        if (query.sql != null) {
            val config: CalciteConnectionConfig = context.config()
            var parserConfig: SqlParser.Config = parserConfig()
                .withQuotedCasing(config.quotedCasing())
                .withUnquotedCasing(config.unquotedCasing())
                .withQuoting(config.quoting())
                .withConformance(config.conformance())
                .withCaseSensitive(config.caseSensitive())
            val parserFactory: SqlParserImplFactory = config.parserFactory(SqlParserImplFactory::class.java, null)
            if (parserFactory != null) {
                parserConfig = parserConfig.withParserFactory(parserFactory)
            }
            val parser: SqlParser = createParser(query.sql, parserConfig)
            val sqlNode: SqlNode
            try {
                sqlNode = parser.parseStmt()
                statementType = getStatementType(sqlNode.getKind())
            } catch (e: SqlParseException) {
                throw RuntimeException(
                    "parse failed: " + e.getMessage(), e
                )
            }
            Hook.PARSE_TREE.run(arrayOf<Object>(query.sql, sqlNode))
            if (sqlNode.getKind().belongsTo(SqlKind.DDL)) {
                executeDdl(context, sqlNode)
                return CalciteSignature(
                    query.sql,
                    ImmutableList.of(),
                    ImmutableMap.of(), null,
                    ImmutableList.of(), Meta.CursorFactory.OBJECT,
                    null, ImmutableList.of(), -1, null,
                    Meta.StatementType.OTHER_DDL
                )
            }
            val validator: SqlValidator = createSqlValidator(context, catalogReader)
            preparedResult = preparingStmt.prepareSql(
                sqlNode, Object::class.java, validator, true
            )
            x = when (sqlNode.getKind()) {
                INSERT, DELETE, UPDATE, EXPLAIN ->         // FIXME: getValidatedNodeType is wrong for DML
                    RelOptUtil.createDmlRowType(sqlNode.getKind(), typeFactory)
                else -> validator.getValidatedNodeType(sqlNode)
            }
        } else if (query.queryable != null) {
            x = context.getTypeFactory().createType(elementType)
            preparedResult = preparingStmt.prepareQueryable(query.queryable, x)
            statementType = getStatementType(preparedResult)
        } else {
            assert(query.rel != null)
            x = query.rel.getRowType()
            preparedResult = preparingStmt.prepareRel(query.rel)
            statementType = getStatementType(preparedResult)
        }
        val parameters: List<AvaticaParameter> = ArrayList()
        val parameterRowType: RelDataType = preparedResult.getParameterRowType()
        for (field in parameterRowType.getFieldList()) {
            val type: RelDataType = field.getType()
            parameters.add(
                AvaticaParameter(
                    false,
                    getPrecision(type),
                    getScale(type),
                    getTypeOrdinal(type),
                    getTypeName(type),
                    getClassName(type),
                    field.getName()
                )
            )
        }
        val jdbcType: RelDataType = makeStruct(typeFactory, x)
        val originList: List<List<String?>?> = preparedResult.getFieldOrigins()
        val columns: List<ColumnMetaData> = getColumnMetaDataList(typeFactory, x, jdbcType, originList)
        var resultClazz: Class? = null
        if (preparedResult is Typed) {
            resultClazz = (preparedResult as Typed).getElementType() as Class
        }
        val cursorFactory: Meta.CursorFactory =
            if (preparingStmt.resultConvention === BindableConvention.INSTANCE) Meta.CursorFactory.ARRAY else Meta.CursorFactory.deduce(
                columns,
                resultClazz
            )
        val bindable: Bindable<T> = preparedResult.getBindable(cursorFactory)
        return CalciteSignature(
            query.sql,
            parameters,
            preparingStmt.internalParameters,
            jdbcType,
            columns,
            cursorFactory,
            context.getRootSchema(),
            if (preparedResult is Prepare.PreparedResultImpl) preparedResult.collations else ImmutableList.of(),
            maxRowCount,
            bindable,
            statementType
        )
    }

    protected fun populateMaterializations(
        context: Context,
        cluster: RelOptCluster, materialization: Prepare.Materialization
    ) {
        // REVIEW: initialize queryRel and tableRel inside MaterializationService,
        // not here?
        try {
            val schema: CalciteSchema = materialization.materializedTable.schema
            val catalogReader = CalciteCatalogReader(
                schema.root(),
                materialization.viewSchemaPath,
                context.getTypeFactory(),
                context.config()
            )
            val materializer = CalciteMaterializer(
                this, context, catalogReader, schema,
                cluster, createConvertletTable()
            )
            materializer.populate(materialization)
        } catch (e: Exception) {
            throw RuntimeException(
                "While populating materialization "
                        + materialization.materializedTable.path(), e
            )
        }
    }

    @Deprecated // to be removed before 2.0
    fun <R> perform(
        statement: CalciteServerStatement,
        action: Frameworks.PrepareAction<R>
    ): R {
        return perform(statement, action.getConfig(), action)
    }

    /** Executes a prepare action.  */
    fun <R> perform(
        statement: CalciteServerStatement,
        config: FrameworkConfig, action: Frameworks.BasePrepareAction<R>
    ): R {
        val prepareContext: CalcitePrepare.Context = statement.createPrepareContext()
        val typeFactory: JavaTypeFactory = prepareContext.getTypeFactory()
        val defaultSchema: SchemaPlus = config.getDefaultSchema()
        val schema: CalciteSchema =
            if (defaultSchema != null) CalciteSchema.from(defaultSchema) else prepareContext.getRootSchema()
        val catalogReader = CalciteCatalogReader(
            schema.root(),
            schema.path(null),
            typeFactory,
            prepareContext.config()
        )
        val rexBuilder = RexBuilder(typeFactory)
        val planner: RelOptPlanner = createPlanner(
            prepareContext,
            config.getContext(),
            config.getCostFactory()
        )
        val cluster: RelOptCluster = createCluster(planner, rexBuilder)
        return action.apply(
            cluster, catalogReader,
            prepareContext.getRootSchema().plus(), statement
        )
    }

    /** Holds state for the process of preparing a SQL statement.  */
    internal class CalcitePreparingStmt(
        protected val prepare: CalcitePrepareImpl,
        context: Context?,
        catalogReader: CatalogReader,
        typeFactory: RelDataTypeFactory,
        schema: CalciteSchema,
        prefer: @Nullable EnumerableRel.Prefer?,
        cluster: RelOptCluster,
        resultConvention: Convention,
        convertletTable: SqlRexConvertletTable
    ) : Prepare(context, catalogReader, resultConvention), RelOptTable.ViewExpander {
        protected val planner: RelOptPlanner
        protected val rexBuilder: RexBuilder
        protected val schema: CalciteSchema
        protected val typeFactory: RelDataTypeFactory
        protected val convertletTable: SqlRexConvertletTable
        private val prefer: @Nullable EnumerableRel.Prefer?
        private val cluster: RelOptCluster
        val internalParameters: Map<String, Object> = LinkedHashMap()

        @SuppressWarnings("unused")
        private var expansionDepth = 0

        @get:Override
        @Nullable
        protected override var sqlValidator: SqlValidator? = null
            protected get() {
                if (field == null) {
                    field = createSqlValidator(catalogReader)
                }
                return field
            }
            private set

        init {
            this.schema = schema
            this.prefer = prefer
            this.cluster = cluster
            planner = cluster.getPlanner()
            rexBuilder = cluster.getRexBuilder()
            this.typeFactory = typeFactory
            this.convertletTable = convertletTable
        }

        @Override
        protected override fun init(runtimeContextClass: Class?) {
        }

        fun prepareQueryable(
            queryable: Queryable,
            resultType: RelDataType
        ): PreparedResult {
            return prepare_(Supplier<RelNode> {
                val cluster: RelOptCluster = prepare.createCluster(planner, rexBuilder)
                LixToRelTranslator(cluster, this@CalcitePreparingStmt)
                    .translate(queryable)
            }, resultType)
        }

        fun prepareRel(rel: RelNode): PreparedResult {
            return prepare_(Supplier<RelNode> { rel }, rel.getRowType())
        }

        private fun prepare_(
            fn: Supplier<RelNode>,
            resultType: RelDataType
        ): PreparedResult {
            val runtimeContextClass: Class = Object::class.java
            init(runtimeContextClass)
            val rel: RelNode = fn.get()
            val rowType: RelDataType = rel.getRowType()
            val fields: List<Pair<Integer, String>> = Pair.zip(
                ImmutableIntList.identity(rowType.getFieldCount()),
                rowType.getFieldNames()
            )
            val collation: RelCollation = if (rel is Sort) (rel as Sort).collation else RelCollations.EMPTY
            var root = RelRoot(
                rel, resultType, SqlKind.SELECT, fields,
                collation, ArrayList()
            )
            if (timingTracer != null) {
                timingTracer.traceTime("end sql2rel")
            }
            val jdbcType: RelDataType = makeStruct(rexBuilder.getTypeFactory(), resultType)
            fieldOrigins = Collections.nCopies(jdbcType.getFieldCount(), null)
            parameterRowType = rexBuilder.getTypeFactory().builder().build()

            // Structured type flattening, view expansion, and plugging in
            // physical storage.
            root = root.withRel(flattenTypes(root.rel, true))

            // Trim unused fields.
            root = trimUnusedFields(root)
            val materializations: List<Materialization> = ImmutableList.of()
            val lattices: List<CalciteSchema.LatticeEntry> = ImmutableList.of()
            root = optimize(root, materializations, lattices)
            if (timingTracer != null) {
                timingTracer.traceTime("end optimization")
            }
            return implement(root)
        }

        @Override
        override fun getSqlToRelConverter(
            validator: SqlValidator?,
            catalogReader: CatalogReader?,
            config: SqlToRelConverter.Config?
        ): SqlToRelConverter {
            return SqlToRelConverter(
                this, validator, catalogReader, cluster,
                convertletTable, config
            )
        }

        @Override
        override fun flattenTypes(
            rootRel: RelNode,
            restructure: Boolean
        ): RelNode {
            val spark: SparkHandler = context.spark()
            return if (spark.enabled()) {
                spark.flattenTypes(planner, rootRel, restructure)
            } else rootRel
        }

        @Override
        protected override fun decorrelate(
            sqlToRelConverter: SqlToRelConverter,
            query: SqlNode?, rootRel: RelNode?
        ): RelNode {
            return sqlToRelConverter.decorrelate(query, rootRel)
        }

        @Override
        fun expandView(
            rowType: RelDataType?, queryString: String?,
            schemaPath: List<String?>?, @Nullable viewPath: List<String?>?
        ): RelRoot {
            expansionDepth++
            val parser: SqlParser = prepare.createParser(queryString)
            val sqlNode: SqlNode
            sqlNode = try {
                parser.parseQuery()
            } catch (e: SqlParseException) {
                throw RuntimeException("parse failed", e)
            }
            // View may have different schema path than current connection.
            val catalogReader: CatalogReader = this.catalogReader.withSchemaPath(schemaPath)
            val validator: SqlValidator = createSqlValidator(catalogReader)
            val config: SqlToRelConverter.Config = SqlToRelConverter.config().withTrimUnusedFields(true)
            val sqlToRelConverter: SqlToRelConverter = getSqlToRelConverter(validator, catalogReader, config)
            val root: RelRoot = sqlToRelConverter.convertQuery(sqlNode, true, false)
            --expansionDepth
            return root
        }

        protected fun createSqlValidator(catalogReader: CatalogReader): SqlValidator {
            return createSqlValidator(
                context,
                catalogReader as CalciteCatalogReader
            )
        }

        @Override
        protected override fun createPreparedExplanation(
            @Nullable resultType: RelDataType?,
            parameterRowType: RelDataType,
            @Nullable root: RelRoot?,
            format: SqlExplainFormat,
            detailLevel: SqlExplainLevel
        ): PreparedResult {
            return CalcitePreparedExplain(
                resultType, parameterRowType, root,
                format, detailLevel
            )
        }

        @Override
        protected override fun implement(root: RelRoot): PreparedResult {
            Hook.PLAN_BEFORE_IMPLEMENTATION.run(root)
            val resultType: RelDataType = root.rel.getRowType()
            val isDml: Boolean = root.kind.belongsTo(SqlKind.DML)
            val bindable: Bindable
            if (resultConvention === BindableConvention.INSTANCE) {
                bindable = Interpreters.bindable(root.rel)
            } else {
                var enumerable: EnumerableRel = root.rel as EnumerableRel
                if (!root.isRefTrivial()) {
                    val projects: List<RexNode> = ArrayList()
                    val rexBuilder: RexBuilder = enumerable.getCluster().getRexBuilder()
                    for (field in Pair.left(root.fields)) {
                        projects.add(rexBuilder.makeInputRef(enumerable, field))
                    }
                    val program: RexProgram = RexProgram.create(
                        enumerable.getRowType(),
                        projects, null, root.validatedRowType, rexBuilder
                    )
                    enumerable = EnumerableCalc.create(enumerable, program)
                }
                bindable = try {
                    CatalogReader.THREAD_LOCAL.set(catalogReader)
                    val conformance: SqlConformance = context.config().conformance()
                    internalParameters.put("_conformance", conformance)
                    EnumerableInterpretable.toBindable(
                        internalParameters,
                        context.spark(), enumerable,
                        Objects.requireNonNull(prefer, "EnumerableRel.Prefer prefer")
                    )
                } finally {
                    CatalogReader.THREAD_LOCAL.remove()
                }
            }
            if (timingTracer != null) {
                timingTracer.traceTime("end codegen")
            }
            if (timingTracer != null) {
                timingTracer.traceTime("end compilation")
            }
            return object : PreparedResultImpl(
                resultType,
                Objects.requireNonNull(parameterRowType, "parameterRowType"),
                Objects.requireNonNull(fieldOrigins, "fieldOrigins"),
                if (root.collation.getFieldCollations()
                        .isEmpty()
                ) ImmutableList.of() else ImmutableList.of(root.collation),
                root.rel,
                mapTableModOp(isDml, root.kind),
                isDml
            ) {
                @get:Override
                override val code: String?
                    get() {
                        throw UnsupportedOperationException()
                    }

                @Override
                override fun getBindable(cursorFactory: Meta.CursorFactory?): Bindable {
                    return bindable
                }

                @get:Override
                override val elementType: Type
                    get() = (bindable as Typed).getElementType()
            }
        }

        @get:Override
        protected override val materializations: List<org.apache.calcite.prepare.Prepare.Materialization>
            protected get() {
                val materializations: List<Prepare.Materialization> =
                    if (context.config().materializationsEnabled()) MaterializationService.instance()
                        .query(schema) else ImmutableList.of()
                for (materialization in materializations) {
                    prepare.populateMaterializations(context, cluster, materialization)
                }
                return materializations
            }

        @get:Override
        protected override val lattices: List<Any?>
            protected get() = Schemas.getLatticeEntries(schema)
    }

    /** An `EXPLAIN` statement, prepared and ready to execute.  */
    private class CalcitePreparedExplain internal constructor(
        @Nullable resultType: RelDataType?,
        parameterRowType: RelDataType,
        @Nullable root: RelRoot?,
        format: SqlExplainFormat,
        detailLevel: SqlExplainLevel
    ) : Prepare.PreparedExplain(resultType, parameterRowType, root, format, detailLevel) {
        @Override
        override fun getBindable(cursorFactory: Meta.CursorFactory): Bindable {
            val explanation: String = getCode()
            return label@ Bindable { dataContext ->
                when (cursorFactory.style) {
                    ARRAY -> return@label Linq4j.singletonEnumerable(arrayOf(explanation))
                    OBJECT -> return@label Linq4j.singletonEnumerable(explanation)
                    else -> return@label Linq4j.singletonEnumerable(explanation)
                }
            }
        }
    }

    /** Translator from Java AST to [RexNode].  */
    internal interface ScalarTranslator {
        fun toRex(statement: BlockStatement?): RexNode?
        fun toRexList(statement: BlockStatement?): List<RexNode>
        fun toRex(expression: Expression?): RexNode?
        fun bind(
            parameterList: List<ParameterExpression?>?,
            values: List<RexNode?>?
        ): ScalarTranslator
    }

    /** Basic translator.  */
    internal class EmptyScalarTranslator(rexBuilder: RexBuilder) : ScalarTranslator {
        private val rexBuilder: RexBuilder

        init {
            this.rexBuilder = rexBuilder
        }

        @Override
        override fun toRexList(statement: BlockStatement): List<RexNode> {
            val simpleList: List<Expression> = simpleList(statement)
            val list: List<RexNode> = ArrayList()
            for (expression1 in simpleList) {
                list.add(toRex(expression1))
            }
            return list
        }

        @Override
        override fun toRex(statement: BlockStatement?): RexNode {
            return toRex(Blocks.simple(statement))
        }

        @Override
        override fun toRex(expression: Expression): RexNode {
            return when (expression.getNodeType()) {
                MemberAccess -> {
                    // Case-sensitive name match because name was previously resolved.
                    val memberExpression: MemberExpression = expression as MemberExpression
                    val field: PseudoField = memberExpression.field
                    val targetExpression: Expression = Objects.requireNonNull(
                        memberExpression.expression
                    ) {
                        ("static field access is not implemented yet."
                                + " field.name=" + field.getName()
                                + ", field.declaringClass=" + field.getDeclaringClass())
                    }
                    rexBuilder.makeFieldAccess(
                        toRex(targetExpression),
                        field.getName(),
                        true
                    )
                }
                GreaterThan -> binary(expression, SqlStdOperatorTable.GREATER_THAN)
                LessThan -> binary(expression, SqlStdOperatorTable.LESS_THAN)
                Parameter -> parameter(expression as ParameterExpression)
                Call -> {
                    val call: MethodCallExpression = expression as MethodCallExpression
                    val operator: SqlOperator = RexToLixTranslator.JAVA_TO_SQL_METHOD_MAP.get(call.method)
                    if (operator != null) {
                        return rexBuilder.makeCall(
                            type(call),
                            operator,
                            toRex(
                                Expressions.< Expression > list < Expression ? > ()
                                    .appendIfNotNull(call.targetExpression)
                                    .appendAll(call.expressions)
                            )
                        )
                    }
                    throw RuntimeException(
                        "Could translate call to method " + call.method
                    )
                }
                Constant -> {
                    val constant: ConstantExpression = expression as ConstantExpression
                    val value: Object = constant.value
                    if (value is Number) {
                        val number = value as Number
                        if (value is Double || value is Float) {
                            rexBuilder.makeApproxLiteral(
                                BigDecimal.valueOf(number.doubleValue())
                            )
                        } else if (value is BigDecimal) {
                            rexBuilder.makeExactLiteral(value as BigDecimal)
                        } else {
                            rexBuilder.makeExactLiteral(
                                BigDecimal.valueOf(number.longValue())
                            )
                        }
                    } else if (value is Boolean) {
                        rexBuilder.makeLiteral(value as Boolean)
                    } else {
                        rexBuilder.makeLiteral(constant.toString())
                    }
                }
                else -> throw UnsupportedOperationException(
                    "unknown expression type " + expression.getNodeType().toString() + " "
                            + expression
                )
            }
        }

        private fun binary(expression: Expression, op: SqlBinaryOperator): RexNode {
            val call: BinaryExpression = expression as BinaryExpression
            return rexBuilder.makeCall(
                type(call), op,
                toRex(ImmutableList.of(call.expression0, call.expression1))
            )
        }

        private override fun toRex(expressions: List<Expression>): List<RexNode> {
            val list: List<RexNode> = ArrayList()
            for (expression in expressions) {
                list.add(toRex(expression))
            }
            return list
        }

        protected fun type(expression: Expression): RelDataType {
            val type: Type = expression.getType()
            return (rexBuilder.getTypeFactory() as JavaTypeFactory).createType(type)
        }

        @Override
        override fun bind(
            parameterList: List<ParameterExpression?>, values: List<RexNode?>
        ): ScalarTranslator {
            return LambdaScalarTranslator(
                rexBuilder, parameterList, values
            )
        }

        fun parameter(param: ParameterExpression): RexNode {
            throw RuntimeException("unknown parameter $param")
        }

        companion object {
            fun empty(builder: RexBuilder): ScalarTranslator {
                return EmptyScalarTranslator(builder)
            }

            private fun simpleList(statement: BlockStatement): List<Expression> {
                val simple: Expression = Blocks.simple(statement)
                return if (simple is NewExpression) {
                    val newExpression: NewExpression = simple as NewExpression
                    newExpression.arguments
                } else {
                    Collections.singletonList(simple)
                }
            }
        }
    }

    /** Translator that looks for parameters.  */
    private class LambdaScalarTranslator internal constructor(
        rexBuilder: RexBuilder,
        parameterList: List<ParameterExpression?>,
        values: List<RexNode?>
    ) : EmptyScalarTranslator(rexBuilder) {
        private val parameterList: List<ParameterExpression?>
        private val values: List<RexNode?>

        init {
            this.parameterList = parameterList
            this.values = values
        }

        @Override
        override fun parameter(param: ParameterExpression): RexNode? {
            val i = parameterList.indexOf(param)
            if (i >= 0) {
                return values[i]
            }
            throw RuntimeException("unknown parameter $param")
        }
    }

    companion object {
        @Deprecated // to be removed before 2.0
        val ENABLE_ENUMERABLE: Boolean = CalciteSystemProperty.ENABLE_ENUMERABLE.value()

        @Deprecated // to be removed before 2.0
        val ENABLE_STREAM: Boolean = CalciteSystemProperty.ENABLE_STREAM.value()

        @Deprecated // to be removed before 2.0
        val ENUMERABLE_RULES: List<RelOptRule> = EnumerableRules.ENUMERABLE_RULES
        private val SIMPLE_SQLS: Set<String> = ImmutableSet.of(
            "SELECT 1",
            "select 1",
            "SELECT 1 FROM DUAL",
            "select 1 from dual",
            "values 1",
            "VALUES 1"
        )

        /** Quickly prepares a simple SQL statement, circumventing the usual
         * preparation process.  */
        private fun <T> simplePrepare(context: Context, sql: String): CalciteSignature<T> {
            val typeFactory: JavaTypeFactory = context.getTypeFactory()
            val x: RelDataType = typeFactory.builder()
                .add(SqlUtil.deriveAliasFromOrdinal(0), SqlTypeName.INTEGER)
                .build()
            val origin: List<String>? = null
            val origins: List<List<String>> = Collections.nCopies(x.getFieldCount(), origin)
            val columns: List<ColumnMetaData> = getColumnMetaDataList(typeFactory, x, x, origins)
            val cursorFactory: Meta.CursorFactory = Meta.CursorFactory.deduce(columns, null)
            return CalciteSignature(
                sql,
                ImmutableList.of(),
                ImmutableMap.of(),
                x,
                columns,
                cursorFactory,
                context.getRootSchema(),
                ImmutableList.of(),
                -1, { dataContext -> Linq4j.asEnumerable(ImmutableList.of(1)) },
                Meta.StatementType.SELECT
            )
        }

        /**
         * Deduces the broad type of statement.
         * Currently returns SELECT for most statement types, but this may change.
         *
         * @param kind Kind of statement
         */
        private fun getStatementType(kind: SqlKind): Meta.StatementType {
            return when (kind) {
                INSERT, DELETE, UPDATE -> Meta.StatementType.IS_DML
                else -> Meta.StatementType.SELECT
            }
        }

        /**
         * Deduces the broad type of statement for a prepare result.
         * Currently returns SELECT for most statement types, but this may change.
         *
         * @param preparedResult Prepare result
         */
        private fun getStatementType(preparedResult: Prepare.PreparedResult): Meta.StatementType {
            return if (preparedResult.isDml()) {
                Meta.StatementType.IS_DML
            } else {
                Meta.StatementType.SELECT
            }
        }

        private fun createSqlValidator(
            context: Context,
            catalogReader: CalciteCatalogReader
        ): SqlValidator {
            val opTab0: SqlOperatorTable = context.config().`fun`(
                SqlOperatorTable::class.java,
                SqlStdOperatorTable.instance()
            )
            val list: List<SqlOperatorTable> = ArrayList()
            list.add(opTab0)
            list.add(catalogReader)
            val opTab: SqlOperatorTable = SqlOperatorTables.chain(list)
            val typeFactory: JavaTypeFactory = context.getTypeFactory()
            val connectionConfig: CalciteConnectionConfig = context.config()
            val config: SqlValidator.Config = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(connectionConfig.lenientOperatorLookup())
                .withSqlConformance(connectionConfig.conformance())
                .withDefaultNullCollation(connectionConfig.defaultNullCollation())
                .withIdentifierExpansion(true)
            return CalciteSqlValidator(
                opTab, catalogReader, typeFactory,
                config
            )
        }

        private fun getColumnMetaDataList(
            typeFactory: JavaTypeFactory, x: RelDataType, jdbcType: RelDataType,
            originList: List<List<String?>?>
        ): List<ColumnMetaData> {
            val columns: List<ColumnMetaData> = ArrayList()
            for (pair in Ord.zip(jdbcType.getFieldList())) {
                val field: RelDataTypeField = pair.e
                val type: RelDataType = field.getType()
                val fieldType: RelDataType = if (x.isStruct()) x.getFieldList().get(pair.i).getType() else type
                columns.add(
                    metaData(
                        typeFactory, columns.size(), field.getName(), type,
                        fieldType, originList[pair.i]
                    )
                )
            }
            return columns
        }

        private fun metaData(
            typeFactory: JavaTypeFactory, ordinal: Int,
            fieldName: String, type: RelDataType, @Nullable fieldType: RelDataType?,
            @Nullable origins: List<String?>?
        ): ColumnMetaData {
            val avaticaType: ColumnMetaData.AvaticaType = avaticaType(typeFactory, type, fieldType)
            return ColumnMetaData(
                ordinal,
                false,
                true,
                false,
                false,
                if (type.isNullable()) DatabaseMetaData.columnNullable else DatabaseMetaData.columnNoNulls,
                true,
                type.getPrecision(),
                fieldName,
                origin(origins, 0),
                origin(origins, 2),
                getPrecision(type),
                getScale(type),
                origin(origins, 1),
                null,
                avaticaType,
                true,
                false,
                false,
                avaticaType.columnClassName()
            )
        }

        private fun avaticaType(
            typeFactory: JavaTypeFactory,
            type: RelDataType, @Nullable fieldType: RelDataType?
        ): ColumnMetaData.AvaticaType {
            val typeName = getTypeName(type)
            return if (type.getComponentType() != null) {
                val componentType: ColumnMetaData.AvaticaType = avaticaType(typeFactory, type.getComponentType(), null)
                val clazz: Type = typeFactory.getJavaClass(type.getComponentType())
                val rep: ColumnMetaData.Rep = ColumnMetaData.Rep.of(clazz)
                assert(rep != null)
                ColumnMetaData.array(componentType, typeName, rep)
            } else {
                var typeOrdinal = getTypeOrdinal(type)
                when (typeOrdinal) {
                    Types.STRUCT -> {
                        val columns: List<ColumnMetaData> = ArrayList(type.getFieldList().size())
                        for (field in type.getFieldList()) {
                            columns.add(
                                metaData(
                                    typeFactory, field.getIndex(), field.getName(),
                                    field.getType(), null, null
                                )
                            )
                        }
                        ColumnMetaData.struct(columns)
                    }
                    ExtraSqlTypes.GEOMETRY -> {
                        typeOrdinal = Types.VARCHAR
                        val clazz: Type = typeFactory.getJavaClass(Util.first(fieldType, type))
                        val rep: ColumnMetaData.Rep = ColumnMetaData.Rep.of(clazz)
                        assert(rep != null)
                        ColumnMetaData.scalar(typeOrdinal, typeName, rep)
                    }
                    else -> {
                        val clazz: Type = typeFactory.getJavaClass(Util.first(fieldType, type))
                        val rep: ColumnMetaData.Rep = ColumnMetaData.Rep.of(clazz)
                        assert(rep != null)
                        ColumnMetaData.scalar(typeOrdinal, typeName, rep)
                    }
                }
            }
        }

        @Nullable
        private fun origin(@Nullable origins: List<String?>?, offsetFromEnd: Int): String? {
            return if (origins == null || offsetFromEnd >= origins.size()) null else origins[origins.size() - 1 - offsetFromEnd]
        }

        private fun getTypeOrdinal(type: RelDataType): Int {
            return type.getSqlTypeName().getJdbcOrdinal()
        }

        private fun getClassName(@SuppressWarnings("unused") type: RelDataType): String {
            return Object::class.java.getName() // CALCITE-2613
        }

        private fun getScale(type: RelDataType): Int {
            return if (type.getScale() === RelDataType.SCALE_NOT_SPECIFIED) 0 else type.getScale()
        }

        private fun getPrecision(type: RelDataType): Int {
            return if (type.getPrecision() === RelDataType.PRECISION_NOT_SPECIFIED) 0 else type.getPrecision()
        }

        /** Returns the type name in string form. Does not include precision, scale
         * or whether nulls are allowed. Example: "DECIMAL" not "DECIMAL(7, 2)";
         * "INTEGER" not "JavaType(int)".  */
        private fun getTypeName(type: RelDataType): String {
            val sqlTypeName: SqlTypeName = type.getSqlTypeName()
            return when (sqlTypeName) {
                ARRAY, MULTISET, MAP, ROW -> type.toString() // e.g. "INTEGER ARRAY"
                INTERVAL_YEAR_MONTH -> "INTERVAL_YEAR_TO_MONTH"
                INTERVAL_DAY_HOUR -> "INTERVAL_DAY_TO_HOUR"
                INTERVAL_DAY_MINUTE -> "INTERVAL_DAY_TO_MINUTE"
                INTERVAL_DAY_SECOND -> "INTERVAL_DAY_TO_SECOND"
                INTERVAL_HOUR_MINUTE -> "INTERVAL_HOUR_TO_MINUTE"
                INTERVAL_HOUR_SECOND -> "INTERVAL_HOUR_TO_SECOND"
                INTERVAL_MINUTE_SECOND -> "INTERVAL_MINUTE_TO_SECOND"
                else -> sqlTypeName.getName() // e.g. "DECIMAL", "INTERVAL_YEAR_MONTH"
            }
        }

        private fun makeStruct(
            typeFactory: RelDataTypeFactory,
            type: RelDataType
        ): RelDataType {
            return if (type.isStruct()) {
                type
            } else typeFactory.builder().add("$0", type).build()
        }
    }
}
