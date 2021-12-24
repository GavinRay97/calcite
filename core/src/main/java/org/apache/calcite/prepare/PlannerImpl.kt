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

import org.apache.calcite.adapter.java.JavaTypeFactory

/** Implementation of [org.apache.calcite.tools.Planner].  */
class PlannerImpl @SuppressWarnings("method.invocation.invalid") constructor(config: FrameworkConfig) : Planner,
    ViewExpander {
    private val operatorTable: SqlOperatorTable
    private val programs: ImmutableList<Program>

    @Nullable
    private val costFactory: RelOptCostFactory
    private val context: Context
    private val connectionConfig: CalciteConnectionConfig
    private val typeSystem: RelDataTypeSystem

    /** Holds the trait definitions to be registered with planner. May be null.  */
    @Nullable
    private val traitDefs: ImmutableList<RelTraitDef>
    private val parserConfig: SqlParser.Config
    private val sqlValidatorConfig: SqlValidator.Config
    private val sqlToRelConverterConfig: SqlToRelConverter.Config
    private val convertletTable: SqlRexConvertletTable
    private var state: State

    // set in STATE_1_RESET
    @SuppressWarnings("unused")
    private var open = false

    // set in STATE_2_READY
    @Nullable
    private val defaultSchema: SchemaPlus

    @Nullable
    private var typeFactory: JavaTypeFactory? = null

    @Nullable
    private var planner: RelOptPlanner? = null

    @Nullable
    private val executor: RexExecutor

    // set in STATE_4_VALIDATE
    @Nullable
    private var validator: SqlValidator? = null

    @Nullable
    private var validatedSqlNode: SqlNode? = null

    /** Creates a planner. Not a public API; call
     * [org.apache.calcite.tools.Frameworks.getPlanner] instead.  */
    init {
        costFactory = config.getCostFactory()
        defaultSchema = config.getDefaultSchema()
        operatorTable = config.getOperatorTable()
        programs = config.getPrograms()
        parserConfig = config.getParserConfig()
        sqlValidatorConfig = config.getSqlValidatorConfig()
        sqlToRelConverterConfig = config.getSqlToRelConverterConfig()
        state = State.STATE_0_CLOSED
        traitDefs = config.getTraitDefs()
        convertletTable = config.getConvertletTable()
        executor = config.getExecutor()
        context = config.getContext()
        connectionConfig = connConfig(context, parserConfig)
        typeSystem = config.getTypeSystem()
        reset()
    }

    /** Makes sure that the state is at least the given state.  */
    private fun ensure(state: State) {
        if (state === this.state) {
            return
        }
        if (state.ordinal() < this.state.ordinal()) {
            throw IllegalArgumentException(
                "cannot move to " + state + " from "
                        + this.state
            )
        }
        state.from(this)
    }

    @get:Override
    val emptyTraitSet: RelTraitSet
        get() = requireNonNull(planner, "planner").emptyTraitSet()

    @Override
    fun close() {
        open = false
        typeFactory = null
        state = State.STATE_0_CLOSED
    }

    @Override
    fun reset() {
        ensure(State.STATE_0_CLOSED)
        open = true
        state = State.STATE_1_RESET
    }

    private fun ready() {
        when (state) {
            State.STATE_0_CLOSED -> reset()
            else -> {}
        }
        ensure(State.STATE_1_RESET)
        typeFactory = JavaTypeFactoryImpl(typeSystem)
        planner = VolcanoPlanner(costFactory, context)
        val planner: RelOptPlanner? = planner
        RelOptUtil.registerDefaultRules(
            planner,
            connectionConfig.materializationsEnabled(),
            Hook.ENABLE_BINDABLE.get(false)
        )
        planner.setExecutor(executor)
        state = State.STATE_2_READY

        // If user specify own traitDef, instead of default default trait,
        // register the trait def specified in traitDefs.
        if (traitDefs == null) {
            planner.addRelTraitDef(ConventionTraitDef.INSTANCE)
            if (CalciteSystemProperty.ENABLE_COLLATION_TRAIT.value()) {
                planner.addRelTraitDef(RelCollationTraitDef.INSTANCE)
            }
        } else {
            for (def in traitDefs) {
                planner.addRelTraitDef(def)
            }
        }
    }

    @Override
    @Throws(SqlParseException::class)
    fun parse(reader: Reader?): SqlNode {
        when (state) {
            State.STATE_0_CLOSED, State.STATE_1_RESET -> ready()
            else -> {}
        }
        ensure(State.STATE_2_READY)
        val parser: SqlParser = SqlParser.create(reader, parserConfig)
        val sqlNode: SqlNode = parser.parseStmt()
        state = State.STATE_3_PARSED
        return sqlNode
    }

    @EnsuresNonNull("validator")
    @Override
    @Throws(ValidationException::class)
    fun validate(sqlNode: SqlNode?): SqlNode? {
        ensure(State.STATE_3_PARSED)
        validator = createSqlValidator(createCatalogReader())
        validatedSqlNode = try {
            validator.validate(sqlNode)
        } catch (e: RuntimeException) {
            throw ValidationException(e)
        }
        state = State.STATE_4_VALIDATED
        return validatedSqlNode
    }

    @Override
    @Throws(ValidationException::class)
    fun validateAndGetType(sqlNode: SqlNode?): Pair<SqlNode, RelDataType> {
        val validatedNode: SqlNode? = validate(sqlNode)
        val type: RelDataType = validator.getValidatedNodeType(validatedNode)
        return Pair.of(validatedNode, type)
    }

    @SuppressWarnings("deprecation")
    @Override
    fun convert(sql: SqlNode?): RelNode {
        return rel(sql).rel
    }

    @Override
    fun rel(sql: SqlNode?): RelRoot {
        ensure(State.STATE_4_VALIDATED)
        val validatedSqlNode: SqlNode = requireNonNull(
            validatedSqlNode,
            "validatedSqlNode is null. Need to call #validate() first"
        )
        val rexBuilder: RexBuilder = createRexBuilder()
        val cluster: RelOptCluster = RelOptCluster.create(
            requireNonNull(planner, "planner"),
            rexBuilder
        )
        val config: SqlToRelConverter.Config = sqlToRelConverterConfig.withTrimUnusedFields(false)
        val sqlToRelConverter = SqlToRelConverter(
            this, validator,
            createCatalogReader(), cluster, convertletTable, config
        )
        var root: RelRoot = sqlToRelConverter.convertQuery(validatedSqlNode, false, true)
        root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true))
        val relBuilder: RelBuilder = config.getRelBuilderFactory().create(cluster, null)
        root = root.withRel(
            RelDecorrelator.decorrelateQuery(root.rel, relBuilder)
        )
        state = State.STATE_5_CONVERTED
        return root
    }
    // CHECKSTYLE: IGNORE 2

    @Deprecated // to be removed before 2.0
    @Deprecated(
        """Now {@link PlannerImpl} implements {@link ViewExpander}
    directly. """
    )
    inner class ViewExpanderImpl internal constructor() : ViewExpander {
        @Override
        fun expandView(
            rowType: RelDataType?, queryString: String?,
            schemaPath: List<String?>?, @Nullable viewPath: List<String?>?
        ): RelRoot {
            return this@PlannerImpl.expandView(
                rowType, queryString, schemaPath,
                viewPath
            )
        }
    }

    @Override
    fun expandView(
        rowType: RelDataType?, queryString: String?,
        schemaPath: List<String?>?, @Nullable viewPath: List<String?>?
    ): RelRoot {
        var planner: RelOptPlanner? = planner
        if (planner == null) {
            ready()
            planner = requireNonNull(this.planner, "planner")
        }
        val parser: SqlParser = SqlParser.create(queryString, parserConfig)
        val sqlNode: SqlNode
        sqlNode = try {
            parser.parseQuery()
        } catch (e: SqlParseException) {
            throw RuntimeException("parse failed", e)
        }
        val catalogReader: CalciteCatalogReader = createCatalogReader().withSchemaPath(schemaPath)
        val validator: SqlValidator = createSqlValidator(catalogReader)
        val rexBuilder: RexBuilder = createRexBuilder()
        val cluster: RelOptCluster = RelOptCluster.create(planner, rexBuilder)
        val config: SqlToRelConverter.Config = sqlToRelConverterConfig.withTrimUnusedFields(false)
        val sqlToRelConverter = SqlToRelConverter(
            this, validator,
            catalogReader, cluster, convertletTable, config
        )
        val root: RelRoot = sqlToRelConverter.convertQuery(sqlNode, true, false)
        val root2: RelRoot = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true))
        val relBuilder: RelBuilder = config.getRelBuilderFactory().create(cluster, null)
        return root2.withRel(
            RelDecorrelator.decorrelateQuery(root.rel, relBuilder)
        )
    }

    // CalciteCatalogReader is stateless; no need to store one
    private fun createCatalogReader(): CalciteCatalogReader {
        val defaultSchema: SchemaPlus = requireNonNull(defaultSchema, "defaultSchema")
        val rootSchema: SchemaPlus = rootSchema(defaultSchema)
        return CalciteCatalogReader(
            CalciteSchema.from(rootSchema),
            CalciteSchema.from(defaultSchema).path(null),
            getTypeFactory(), connectionConfig
        )
    }

    private fun createSqlValidator(catalogReader: CalciteCatalogReader): SqlValidator {
        val opTab: SqlOperatorTable = SqlOperatorTables.chain(operatorTable, catalogReader)
        return CalciteSqlValidator(
            opTab,
            catalogReader,
            getTypeFactory(),
            sqlValidatorConfig
                .withDefaultNullCollation(connectionConfig.defaultNullCollation())
                .withLenientOperatorLookup(connectionConfig.lenientOperatorLookup())
                .withSqlConformance(connectionConfig.conformance())
                .withIdentifierExpansion(true)
        )
    }

    // RexBuilder is stateless; no need to store one
    private fun createRexBuilder(): RexBuilder {
        return RexBuilder(getTypeFactory())
    }

    @Override
    fun getTypeFactory(): JavaTypeFactory {
        return requireNonNull(typeFactory, "typeFactory")
    }

    @SuppressWarnings("deprecation")
    @Override
    fun transform(
        ruleSetIndex: Int, requiredOutputTraits: RelTraitSet?,
        rel: RelNode
    ): RelNode {
        ensure(State.STATE_5_CONVERTED)
        rel.getCluster().setMetadataProvider(
            CachingRelMetadataProvider(
                requireNonNull(rel.getCluster().getMetadataProvider(), "metadataProvider"),
                rel.getCluster().getPlanner()
            )
        )
        val program: Program = programs.get(ruleSetIndex)
        return program.run(
            requireNonNull(planner, "planner"),
            rel, requiredOutputTraits, ImmutableList.of(),
            ImmutableList.of()
        )
    }

    /** Stage of a statement in the query-preparation lifecycle.  */
    private enum class State {
        STATE_0_CLOSED {
            @Override
            override fun from(planner: PlannerImpl) {
                planner.close()
            }
        },
        STATE_1_RESET {
            @Override
            override fun from(planner: PlannerImpl) {
                planner.ensure(STATE_0_CLOSED)
                planner.reset()
            }
        },
        STATE_2_READY {
            @Override
            override fun from(planner: PlannerImpl) {
                STATE_1_RESET.from(planner)
                planner.ready()
            }
        },
        STATE_3_PARSED, STATE_4_VALIDATED, STATE_5_CONVERTED;

        /** Moves planner's state to this state. This must be a higher state.  */
        open fun from(planner: PlannerImpl) {
            throw IllegalArgumentException(
                "cannot move from " + planner.state
                        + " to " + this
            )
        }
    }

    companion object {
        /** Gets a user-defined config and appends default connection values.  */
        private fun connConfig(
            context: Context,
            parserConfig: SqlParser.Config
        ): CalciteConnectionConfig {
            var config: CalciteConnectionConfigImpl = context.maybeUnwrap(CalciteConnectionConfigImpl::class.java)
                .orElse(CalciteConnectionConfig.DEFAULT)
            if (!config.isSet(CalciteConnectionProperty.CASE_SENSITIVE)) {
                config = config.set(
                    CalciteConnectionProperty.CASE_SENSITIVE,
                    String.valueOf(parserConfig.caseSensitive())
                )
            }
            if (!config.isSet(CalciteConnectionProperty.CONFORMANCE)) {
                config = config.set(
                    CalciteConnectionProperty.CONFORMANCE,
                    String.valueOf(parserConfig.conformance())
                )
            }
            return config
        }

        private fun rootSchema(schema: SchemaPlus): SchemaPlus {
            var schema: SchemaPlus = schema
            while (true) {
                val parentSchema: SchemaPlus = schema.getParentSchema() ?: return schema
                schema = parentSchema
            }
        }
    }
}
