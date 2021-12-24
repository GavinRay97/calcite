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
package org.apache.calcite.interpreter

import org.apache.calcite.DataContext

/**
 * Utilities pertaining to [BindableRel] and [BindableConvention].
 */
@Value.Enclosing
object Bindables {
    val BINDABLE_TABLE_SCAN_RULE: RelOptRule = BindableTableScanRule.Config.DEFAULT.toRule()
    val BINDABLE_FILTER_RULE: RelOptRule = BindableFilterRule.DEFAULT_CONFIG.toRule(
        BindableFilterRule::class.java
    )
    val BINDABLE_PROJECT_RULE: RelOptRule = BindableProjectRule.DEFAULT_CONFIG.toRule(
        BindableProjectRule::class.java
    )
    val BINDABLE_SORT_RULE: RelOptRule = BindableSortRule.DEFAULT_CONFIG.toRule(
        BindableSortRule::class.java
    )
    val BINDABLE_JOIN_RULE: RelOptRule = BindableJoinRule.DEFAULT_CONFIG.toRule(
        BindableJoinRule::class.java
    )
    val BINDABLE_SET_OP_RULE: RelOptRule = BindableSetOpRule.DEFAULT_CONFIG.toRule(
        BindableSetOpRule::class.java
    )
    // CHECKSTYLE: IGNORE 1

    @SuppressWarnings("MissingSummary")
    @Deprecated("Use {@link #BINDABLE_SET_OP_RULE}. ")
    val BINDABLE_SETOP_RULE: RelOptRule = BINDABLE_SET_OP_RULE
    val BINDABLE_VALUES_RULE: RelOptRule = BindableValuesRule.DEFAULT_CONFIG.toRule(
        BindableValuesRule::class.java
    )
    val BINDABLE_AGGREGATE_RULE: RelOptRule = BindableAggregateRule.DEFAULT_CONFIG.toRule(
        BindableAggregateRule::class.java
    )
    val BINDABLE_WINDOW_RULE: RelOptRule = BindableWindowRule.DEFAULT_CONFIG.toRule(
        BindableWindowRule::class.java
    )
    val BINDABLE_MATCH_RULE: RelOptRule = BindableMatchRule.DEFAULT_CONFIG.toRule(
        BindableMatchRule::class.java
    )

    /** Rule that converts a relational expression from
     * [org.apache.calcite.plan.Convention.NONE]
     * to [org.apache.calcite.interpreter.BindableConvention].  */
    val FROM_NONE_RULE: NoneToBindableConverterRule = NoneToBindableConverterRule.DEFAULT_CONFIG
        .toRule(NoneToBindableConverterRule::class.java)

    /** All rules that convert logical relational expression to bindable.  */
    val RULES: ImmutableList<RelOptRule> = ImmutableList.of(
        FROM_NONE_RULE,
        BINDABLE_TABLE_SCAN_RULE,
        BINDABLE_FILTER_RULE,
        BINDABLE_PROJECT_RULE,
        BINDABLE_SORT_RULE,
        BINDABLE_JOIN_RULE,
        BINDABLE_SET_OP_RULE,
        BINDABLE_VALUES_RULE,
        BINDABLE_AGGREGATE_RULE,
        BINDABLE_WINDOW_RULE,
        BINDABLE_MATCH_RULE
    )

    /** Helper method that converts a bindable relational expression into a
     * record iterator.
     *
     *
     * Any bindable can be compiled; if its input is also bindable, it becomes
     * part of the same compilation unit.
     */
    private fun help(
        dataContext: DataContext,
        rel: BindableRel
    ): Enumerable<Array<Object>> {
        return Interpreter(dataContext, rel)
    }

    /** Rule that converts a [org.apache.calcite.rel.core.TableScan]
     * to bindable convention.
     *
     * @see .BINDABLE_TABLE_SCAN_RULE
     */
    class BindableTableScanRule
    /** Called from Config.  */
    protected constructor(config: Config?) : RelRule<BindableTableScanRule.Config?>(config) {
        @Deprecated // to be removed before 2.0
        constructor(relBuilderFactory: RelBuilderFactory?) : this(
            Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
                .`as`(Config::class.java)
        ) {
        }

        @Override
        fun onMatch(call: RelOptRuleCall) {
            val scan: LogicalTableScan = call.rel(0)
            val table: RelOptTable = scan.getTable()
            if (BindableTableScan.canHandle(table)) {
                call.transformTo(
                    BindableTableScan.create(scan.getCluster(), table)
                )
            }
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface Config : RelRule.Config {
            @Override
            fun toRule(): BindableTableScanRule {
                return BindableTableScanRule(this)
            }

            companion object {
                val DEFAULT: Config = ImmutableBindables.Config.of()
                    .withOperandSupplier { b -> b.operand(LogicalTableScan::class.java).noInputs() }
            }
        }
    }

    /** Scan of a table that implements [ScannableTable] and therefore can
     * be converted into an [Enumerable].  */
    class BindableTableScan internal constructor(
        cluster: RelOptCluster?, traitSet: RelTraitSet?,
        table: RelOptTable, filters: ImmutableList<RexNode?>?,
        projects: ImmutableIntList?
    ) : TableScan(cluster, traitSet, ImmutableList.of(), table), BindableRel {
        val filters: ImmutableList<RexNode>
        val projects: ImmutableIntList

        /** Creates a BindableTableScan.
         *
         *
         * Use [.create] unless you know what you are doing.  */
        init {
            this.filters = Objects.requireNonNull(filters, "filters")
            this.projects = Objects.requireNonNull(projects, "projects")
            Preconditions.checkArgument(canHandle(table))
        }

        @Override
        fun deriveRowType(): RelDataType {
            val builder: RelDataTypeFactory.Builder = getCluster().getTypeFactory().builder()
            val fieldList: List<RelDataTypeField> = table.getRowType().getFieldList()
            for (project in projects) {
                builder.add(fieldList[project])
            }
            return builder.build()
        }

        @get:Override
        val elementType: Class<Array<Object>>
            get() = Array<Object>::class.java

        @Override
        fun explainTerms(pw: RelWriter?): RelWriter {
            return super.explainTerms(pw)
                .itemIf("filters", filters, !filters.isEmpty())
                .itemIf("projects", projects, !projects.equals(identity()))
        }

        @Override
        @Nullable
        fun computeSelfCost(
            planner: RelOptPlanner?,
            mq: RelMetadataQuery?
        ): RelOptCost? {
            val noPushing = (filters.isEmpty()
                    && projects.size() === table.getRowType().getFieldCount())
            val cost: RelOptCost = super.computeSelfCost(planner, mq)
            if (noPushing || cost == null) {
                return cost
            }
            // Cost factor for pushing filters
            val f = if (filters.isEmpty()) 1.0 else 0.5

            // Cost factor for pushing fields
            // The "+ 2d" on top and bottom keeps the function fairly smooth.
            val p = ((projects.size() as Double + 2.0)
                    / (table.getRowType().getFieldCount() as Double + 2.0))

            // Multiply the cost by a factor that makes a scan more attractive if
            // filters and projects are pushed to the table scan
            return cost.multiplyBy(f * p * 0.01)
        }

        @Override
        fun bind(dataContext: DataContext): Enumerable<Array<Object>> {
            return help(dataContext, this)
        }

        @Override
        fun implement(implementor: InterpreterImplementor?): Node {
            throw UnsupportedOperationException() // TODO:
        }

        companion object {
            /** Creates a BindableTableScan.  */
            fun create(
                cluster: RelOptCluster,
                relOptTable: RelOptTable
            ): BindableTableScan {
                return create(
                    cluster, relOptTable, ImmutableList.of(),
                    identity(relOptTable)
                )
            }

            /** Creates a BindableTableScan.  */
            fun create(
                cluster: RelOptCluster,
                relOptTable: RelOptTable, filters: List<RexNode?>?,
                projects: List<Integer?>?
            ): BindableTableScan {
                val table: Table = relOptTable.unwrap(Table::class.java)
                val traitSet: RelTraitSet = cluster.traitSetOf(BindableConvention.INSTANCE)
                    .replaceIfs(RelCollationTraitDef.INSTANCE) {
                        if (table != null) {
                            return@replaceIfs table.getStatistic().getCollations()
                        }
                        ImmutableList.of()
                    }
                return BindableTableScan(
                    cluster, traitSet, relOptTable,
                    ImmutableList.copyOf(filters), ImmutableIntList.copyOf(projects)
                )
            }

            fun canHandle(table: RelOptTable): Boolean {
                return (table.maybeUnwrap(ScannableTable::class.java).isPresent()
                        || table.maybeUnwrap(FilterableTable::class.java).isPresent()
                        || table.maybeUnwrap(ProjectableFilterableTable::class.java).isPresent())
            }
        }
    }

    /** Rule that converts a [Filter] to bindable convention.
     *
     * @see .BINDABLE_FILTER_RULE
     */
    class BindableFilterRule
    /** Called from the Config.  */
    protected constructor(config: Config?) : ConverterRule(config) {
        @Override
        fun convert(rel: RelNode): RelNode {
            val filter: LogicalFilter = rel as LogicalFilter
            return BindableFilter.create(
                convert(
                    filter.getInput(),
                    filter.getInput().getTraitSet()
                        .replace(BindableConvention.INSTANCE)
                ),
                filter.getCondition()
            )
        }

        companion object {
            /** Default configuration.  */
            val DEFAULT_CONFIG: Config = Config.INSTANCE
                .withConversion(
                    LogicalFilter::class.java, { f -> !f.containsOver() },
                    Convention.NONE, BindableConvention.INSTANCE,
                    "BindableFilterRule"
                )
                .withRuleFactory { config: Config? -> BindableFilterRule(config) }
        }
    }

    /** Implementation of [org.apache.calcite.rel.core.Filter]
     * in bindable convention.  */
    class BindableFilter(
        cluster: RelOptCluster?, traitSet: RelTraitSet?,
        input: RelNode?, condition: RexNode?
    ) : Filter(cluster, traitSet, input, condition), BindableRel {
        init {
            assert(getConvention() is BindableConvention)
        }

        @Override
        fun copy(
            traitSet: RelTraitSet?, input: RelNode?,
            condition: RexNode?
        ): BindableFilter {
            return BindableFilter(getCluster(), traitSet, input, condition)
        }

        @get:Override
        val elementType: Class<Array<Object>>
            get() = Array<Object>::class.java

        @Override
        fun bind(dataContext: DataContext): Enumerable<Array<Object>> {
            return help(dataContext, this)
        }

        @Override
        fun implement(implementor: InterpreterImplementor): Node {
            return FilterNode(implementor.compiler, this)
        }

        companion object {
            /** Creates a BindableFilter.  */
            fun create(
                input: RelNode,
                condition: RexNode?
            ): BindableFilter {
                val cluster: RelOptCluster = input.getCluster()
                val mq: RelMetadataQuery = cluster.getMetadataQuery()
                val traitSet: RelTraitSet = cluster.traitSetOf(BindableConvention.INSTANCE)
                    .replaceIfs(
                        RelCollationTraitDef.INSTANCE
                    ) { RelMdCollation.filter(mq, input) }
                return BindableFilter(cluster, traitSet, input, condition)
            }
        }
    }

    /**
     * Rule to convert a [org.apache.calcite.rel.logical.LogicalProject]
     * to a [BindableProject].
     *
     * @see .BINDABLE_PROJECT_RULE
     */
    class BindableProjectRule
    /** Called from the Config.  */
    protected constructor(config: Config?) : ConverterRule(config) {
        @Override
        fun convert(rel: RelNode): RelNode {
            val project: LogicalProject = rel as LogicalProject
            return BindableProject(
                rel.getCluster(),
                rel.getTraitSet().replace(BindableConvention.INSTANCE),
                convert(
                    project.getInput(),
                    project.getInput().getTraitSet()
                        .replace(BindableConvention.INSTANCE)
                ),
                project.getProjects(),
                project.getRowType()
            )
        }

        companion object {
            /** Default configuration.  */
            val DEFAULT_CONFIG: Config = Config.INSTANCE
                .withConversion(
                    LogicalProject::class.java, { p -> !p.containsOver() },
                    Convention.NONE, BindableConvention.INSTANCE,
                    "BindableProjectRule"
                )
                .withRuleFactory { config: Config? -> BindableProjectRule(config) }
        }
    }

    /** Implementation of [org.apache.calcite.rel.core.Project] in
     * bindable calling convention.  */
    class BindableProject(
        cluster: RelOptCluster?, traitSet: RelTraitSet?,
        input: RelNode?, projects: List<RexNode?>?, rowType: RelDataType?
    ) : Project(cluster, traitSet, ImmutableList.of(), input, projects, rowType), BindableRel {
        init {
            assert(getConvention() is BindableConvention)
        }

        @Override
        fun copy(
            traitSet: RelTraitSet?, input: RelNode?,
            projects: List<RexNode?>?, rowType: RelDataType?
        ): BindableProject {
            return BindableProject(
                getCluster(), traitSet, input,
                projects, rowType
            )
        }

        @get:Override
        val elementType: Class<Array<Object>>
            get() = Array<Object>::class.java

        @Override
        fun bind(dataContext: DataContext): Enumerable<Array<Object>> {
            return help(dataContext, this)
        }

        @Override
        fun implement(implementor: InterpreterImplementor): Node {
            return ProjectNode(implementor.compiler, this)
        }
    }

    /**
     * Rule to convert an [org.apache.calcite.rel.core.Sort] to a
     * [org.apache.calcite.interpreter.Bindables.BindableSort].
     *
     * @see .BINDABLE_SORT_RULE
     */
    class BindableSortRule
    /** Called from the Config.  */
    protected constructor(config: Config?) : ConverterRule(config) {
        @Override
        fun convert(rel: RelNode): RelNode {
            val sort: Sort = rel as Sort
            val traitSet: RelTraitSet = sort.getTraitSet().replace(BindableConvention.INSTANCE)
            val input: RelNode = sort.getInput()
            return BindableSort(
                rel.getCluster(), traitSet,
                convert(
                    input,
                    input.getTraitSet().replace(BindableConvention.INSTANCE)
                ),
                sort.getCollation(), sort.offset, sort.fetch
            )
        }

        companion object {
            /** Default configuration.  */
            val DEFAULT_CONFIG: Config = Config.INSTANCE
                .withConversion(
                    Sort::class.java, Convention.NONE,
                    BindableConvention.INSTANCE, "BindableSortRule"
                )
                .withRuleFactory { config: Config? -> BindableSortRule(config) }
        }
    }

    /** Implementation of [org.apache.calcite.rel.core.Sort]
     * bindable calling convention.  */
    class BindableSort(
        cluster: RelOptCluster?, traitSet: RelTraitSet?,
        input: RelNode?, collation: RelCollation?, @Nullable offset: RexNode?, @Nullable fetch: RexNode?
    ) : Sort(cluster, traitSet, input, collation, offset, fetch), BindableRel {
        init {
            assert(getConvention() is BindableConvention)
        }

        @Override
        fun copy(
            traitSet: RelTraitSet?, newInput: RelNode?,
            newCollation: RelCollation?, @Nullable offset: RexNode?, @Nullable fetch: RexNode?
        ): BindableSort {
            return BindableSort(
                getCluster(), traitSet, newInput, newCollation,
                offset, fetch
            )
        }

        @get:Override
        val elementType: Class<Array<Object>>
            get() = Array<Object>::class.java

        @Override
        fun bind(dataContext: DataContext): Enumerable<Array<Object>> {
            return help(dataContext, this)
        }

        @Override
        fun implement(implementor: InterpreterImplementor): Node {
            return SortNode(implementor.compiler, this)
        }
    }

    /**
     * Rule to convert a [org.apache.calcite.rel.logical.LogicalJoin]
     * to a [BindableJoin].
     *
     * @see .BINDABLE_JOIN_RULE
     */
    class BindableJoinRule
    /** Called from the Config.  */
    protected constructor(config: Config?) : ConverterRule(config) {
        @Override
        fun convert(rel: RelNode): RelNode {
            val join: LogicalJoin = rel as LogicalJoin
            val out: BindableConvention = BindableConvention.INSTANCE
            val traitSet: RelTraitSet = join.getTraitSet().replace(out)
            return BindableJoin(
                rel.getCluster(), traitSet,
                convert(
                    join.getLeft(),
                    join.getLeft().getTraitSet()
                        .replace(BindableConvention.INSTANCE)
                ),
                convert(
                    join.getRight(),
                    join.getRight().getTraitSet()
                        .replace(BindableConvention.INSTANCE)
                ),
                join.getCondition(), join.getVariablesSet(), join.getJoinType()
            )
        }

        companion object {
            /** Default configuration.  */
            val DEFAULT_CONFIG: Config = Config.INSTANCE
                .withConversion(
                    LogicalJoin::class.java, Convention.NONE,
                    BindableConvention.INSTANCE, "BindableJoinRule"
                )
                .withRuleFactory { config: Config? -> BindableJoinRule(config) }
        }
    }

    /** Implementation of [org.apache.calcite.rel.core.Join] in
     * bindable calling convention.  */
    class BindableJoin
    /** Creates a BindableJoin.  */
    protected constructor(
        cluster: RelOptCluster?, traitSet: RelTraitSet?,
        left: RelNode?, right: RelNode?, condition: RexNode?,
        variablesSet: Set<CorrelationId?>?, joinType: JoinRelType?
    ) : Join(
        cluster, traitSet, ImmutableList.of(), left, right,
        condition, variablesSet, joinType
    ), BindableRel {
        @Deprecated // to be removed before 2.0
        protected constructor(
            cluster: RelOptCluster?, traitSet: RelTraitSet?,
            left: RelNode?, right: RelNode?, condition: RexNode?, joinType: JoinRelType?,
            variablesStopped: Set<String?>?
        ) : this(
            cluster, traitSet, left, right, condition,
            CorrelationId.setOf(variablesStopped), joinType
        ) {
        }

        @Override
        fun copy(
            traitSet: RelTraitSet?, conditionExpr: RexNode?,
            left: RelNode?, right: RelNode?, joinType: JoinRelType?,
            semiJoinDone: Boolean
        ): BindableJoin {
            return BindableJoin(
                getCluster(), traitSet, left, right,
                conditionExpr, variablesSet, joinType
            )
        }

        @get:Override
        val elementType: Class<Array<Object>>
            get() = Array<Object>::class.java

        @Override
        fun bind(dataContext: DataContext): Enumerable<Array<Object>> {
            return help(dataContext, this)
        }

        @Override
        fun implement(implementor: InterpreterImplementor): Node {
            return JoinNode(implementor.compiler, this)
        }
    }

    /**
     * Rule to convert an [SetOp] to a [BindableUnion]
     * or [BindableIntersect] or [BindableMinus].
     *
     * @see .BINDABLE_SET_OP_RULE
     */
    class BindableSetOpRule
    /** Called from the Config.  */
    protected constructor(config: Config?) : ConverterRule(config) {
        @Override
        fun convert(rel: RelNode): RelNode {
            val setOp: SetOp = rel as SetOp
            val out: BindableConvention = BindableConvention.INSTANCE
            val traitSet: RelTraitSet = setOp.getTraitSet().replace(out)
            return if (setOp is LogicalUnion) {
                BindableUnion(
                    rel.getCluster(), traitSet,
                    convertList(setOp.getInputs(), out), setOp.all
                )
            } else if (setOp is LogicalIntersect) {
                BindableIntersect(
                    rel.getCluster(), traitSet,
                    convertList(setOp.getInputs(), out), setOp.all
                )
            } else {
                BindableMinus(
                    rel.getCluster(), traitSet,
                    convertList(setOp.getInputs(), out), setOp.all
                )
            }
        }

        companion object {
            /** Default configuration.  */
            val DEFAULT_CONFIG: Config = Config.INSTANCE
                .withConversion(
                    SetOp::class.java, Convention.NONE,
                    BindableConvention.INSTANCE, "BindableSetOpRule"
                )
                .withRuleFactory { config: Config? -> BindableSetOpRule(config) }
        }
    }

    /** Implementation of [org.apache.calcite.rel.core.Union] in
     * bindable calling convention.  */
    class BindableUnion(
        cluster: RelOptCluster?, traitSet: RelTraitSet?,
        inputs: List<RelNode?>?, all: Boolean
    ) : Union(cluster, traitSet, inputs, all), BindableRel {
        @Override
        fun copy(
            traitSet: RelTraitSet?, inputs: List<RelNode?>?,
            all: Boolean
        ): BindableUnion {
            return BindableUnion(getCluster(), traitSet, inputs, all)
        }

        @get:Override
        val elementType: Class<Array<Object>>
            get() = Array<Object>::class.java

        @Override
        fun bind(dataContext: DataContext): Enumerable<Array<Object>> {
            return help(dataContext, this)
        }

        @Override
        fun implement(implementor: InterpreterImplementor): Node {
            return SetOpNode(implementor.compiler, this)
        }
    }

    /** Implementation of [org.apache.calcite.rel.core.Intersect] in
     * bindable calling convention.  */
    class BindableIntersect(
        cluster: RelOptCluster?, traitSet: RelTraitSet?,
        inputs: List<RelNode?>?, all: Boolean
    ) : Intersect(cluster, traitSet, inputs, all), BindableRel {
        @Override
        fun copy(
            traitSet: RelTraitSet?, inputs: List<RelNode?>?,
            all: Boolean
        ): BindableIntersect {
            return BindableIntersect(getCluster(), traitSet, inputs, all)
        }

        @get:Override
        val elementType: Class<Array<Object>>
            get() = Array<Object>::class.java

        @Override
        fun bind(dataContext: DataContext): Enumerable<Array<Object>> {
            return help(dataContext, this)
        }

        @Override
        fun implement(implementor: InterpreterImplementor): Node {
            return SetOpNode(implementor.compiler, this)
        }
    }

    /** Implementation of [org.apache.calcite.rel.core.Minus] in
     * bindable calling convention.  */
    class BindableMinus(
        cluster: RelOptCluster?, traitSet: RelTraitSet?,
        inputs: List<RelNode?>?, all: Boolean
    ) : Minus(cluster, traitSet, inputs, all), BindableRel {
        @Override
        fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>?, all: Boolean): BindableMinus {
            return BindableMinus(getCluster(), traitSet, inputs, all)
        }

        @get:Override
        val elementType: Class<Array<Object>>
            get() = Array<Object>::class.java

        @Override
        fun bind(dataContext: DataContext): Enumerable<Array<Object>> {
            return help(dataContext, this)
        }

        @Override
        fun implement(implementor: InterpreterImplementor): Node {
            return SetOpNode(implementor.compiler, this)
        }
    }

    /** Implementation of [org.apache.calcite.rel.core.Values]
     * in bindable calling convention.  */
    class BindableValues internal constructor(
        cluster: RelOptCluster?, rowType: RelDataType?,
        tuples: ImmutableList<ImmutableList<RexLiteral?>?>?, traitSet: RelTraitSet?
    ) : Values(cluster, rowType, tuples, traitSet), BindableRel {
        @Override
        fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>): RelNode {
            assert(inputs.isEmpty())
            return BindableValues(getCluster(), getRowType(), tuples, traitSet)
        }

        @get:Override
        val elementType: Class<Array<Object>>
            get() = Array<Object>::class.java

        @Override
        fun bind(dataContext: DataContext): Enumerable<Array<Object>> {
            return help(dataContext, this)
        }

        @Override
        fun implement(implementor: InterpreterImplementor): Node {
            return ValuesNode(implementor.compiler, this)
        }
    }

    /** Rule that converts a [Values] to bindable convention.
     *
     * @see .BINDABLE_VALUES_RULE
     */
    class BindableValuesRule
    /** Called from the Config.  */
    protected constructor(config: Config?) : ConverterRule(config) {
        @Override
        fun convert(rel: RelNode): RelNode {
            val values: LogicalValues = rel as LogicalValues
            return BindableValues(
                values.getCluster(), values.getRowType(),
                values.getTuples(),
                values.getTraitSet().replace(BindableConvention.INSTANCE)
            )
        }

        companion object {
            /** Default configuration.  */
            val DEFAULT_CONFIG: Config = Config.INSTANCE
                .withConversion(
                    LogicalValues::class.java, Convention.NONE,
                    BindableConvention.INSTANCE, "BindableValuesRule"
                )
                .withRuleFactory { config: Config? -> BindableValuesRule(config) }
        }
    }

    /** Implementation of [org.apache.calcite.rel.core.Aggregate]
     * in bindable calling convention.  */
    class BindableAggregate(
        cluster: RelOptCluster?,
        traitSet: RelTraitSet?,
        input: RelNode?,
        groupSet: ImmutableBitSet?,
        @Nullable groupSets: List<ImmutableBitSet?>?,
        aggCalls: List<AggregateCall?>
    ) : Aggregate(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls), BindableRel {
        init {
            assert(getConvention() is BindableConvention)
            for (aggCall in aggCalls) {
                if (aggCall.isDistinct()) {
                    throw InvalidRelException(
                        "distinct aggregation not supported"
                    )
                }
                if (aggCall.distinctKeys != null) {
                    throw InvalidRelException(
                        "within-distinct aggregation not supported"
                    )
                }
                val implementor2: AggImplementor = RexImpTable.INSTANCE.get(aggCall.getAggregation(), false)
                    ?: throw InvalidRelException(
                        "aggregation " + aggCall.getAggregation().toString() + " not supported"
                    )
            }
        }

        @Deprecated // to be removed before 2.0
        constructor(
            cluster: RelOptCluster?, traitSet: RelTraitSet?,
            input: RelNode?, indicator: Boolean, groupSet: ImmutableBitSet?,
            groupSets: List<ImmutableBitSet?>?, aggCalls: List<AggregateCall?>
        ) : this(cluster, traitSet, input, groupSet, groupSets, aggCalls) {
            checkIndicator(indicator)
        }

        @Override
        fun copy(
            traitSet: RelTraitSet?, input: RelNode?,
            groupSet: ImmutableBitSet?,
            @Nullable groupSets: List<ImmutableBitSet?>?, aggCalls: List<AggregateCall?>
        ): BindableAggregate {
            return try {
                BindableAggregate(
                    getCluster(), traitSet, input,
                    groupSet, groupSets, aggCalls
                )
            } catch (e: InvalidRelException) {
                // Semantic error not possible. Must be a bug. Convert to
                // internal error.
                throw AssertionError(e)
            }
        }

        @get:Override
        val elementType: Class<Array<Object>>
            get() = Array<Object>::class.java

        @Override
        fun bind(dataContext: DataContext): Enumerable<Array<Object>> {
            return help(dataContext, this)
        }

        @Override
        fun implement(implementor: InterpreterImplementor): Node {
            return AggregateNode(implementor.compiler, this)
        }
    }

    /** Rule that converts an [Aggregate] to bindable convention.
     *
     * @see .BINDABLE_AGGREGATE_RULE
     */
    class BindableAggregateRule
    /** Called from the Config.  */
    protected constructor(config: Config?) : ConverterRule(config) {
        @Override
        @Nullable
        fun convert(rel: RelNode): RelNode? {
            val agg: LogicalAggregate = rel as LogicalAggregate
            val traitSet: RelTraitSet = agg.getTraitSet().replace(BindableConvention.INSTANCE)
            return try {
                BindableAggregate(
                    rel.getCluster(), traitSet,
                    convert(agg.getInput(), traitSet), false, agg.getGroupSet(),
                    agg.getGroupSets(), agg.getAggCallList()
                )
            } catch (e: InvalidRelException) {
                RelOptPlanner.LOGGER.debug(e.toString())
                null
            }
        }

        companion object {
            /** Default configuration.  */
            val DEFAULT_CONFIG: Config = Config.INSTANCE
                .withConversion(
                    LogicalAggregate::class.java, Convention.NONE,
                    BindableConvention.INSTANCE, "BindableAggregateRule"
                )
                .withRuleFactory { config: Config? -> BindableAggregateRule(config) }
        }
    }

    /** Implementation of [org.apache.calcite.rel.core.Window]
     * in bindable convention.  */
    class BindableWindow
    /** Creates a BindableWindow.  */
    internal constructor(
        cluster: RelOptCluster?, traitSet: RelTraitSet?, input: RelNode?,
        constants: List<RexLiteral?>?, rowType: RelDataType?, groups: List<Group?>?
    ) : Window(cluster, traitSet, input, constants, rowType, groups), BindableRel {
        @Override
        fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>?): RelNode {
            return BindableWindow(
                getCluster(), traitSet, sole(inputs),
                constants, getRowType(), groups
            )
        }

        @Override
        @Nullable
        fun computeSelfCost(
            planner: RelOptPlanner?,
            mq: RelMetadataQuery?
        ): RelOptCost? {
            val cost: RelOptCost = super.computeSelfCost(planner, mq) ?: return null
            return cost.multiplyBy(BindableConvention.COST_MULTIPLIER)
        }

        @get:Override
        val elementType: Class<Array<Object>>
            get() = Array<Object>::class.java

        @Override
        fun bind(dataContext: DataContext): Enumerable<Array<Object>> {
            return help(dataContext, this)
        }

        @Override
        fun implement(implementor: InterpreterImplementor): Node {
            return WindowNode(implementor.compiler, this)
        }
    }

    /** Rule to convert a [org.apache.calcite.rel.logical.LogicalWindow]
     * to a [BindableWindow].
     *
     * @see .BINDABLE_WINDOW_RULE
     */
    class BindableWindowRule
    /** Called from the Config.  */
    protected constructor(config: Config?) : ConverterRule(config) {
        @Override
        fun convert(rel: RelNode): RelNode {
            val winAgg: LogicalWindow = rel as LogicalWindow
            val traitSet: RelTraitSet = winAgg.getTraitSet().replace(BindableConvention.INSTANCE)
            val input: RelNode = winAgg.getInput()
            val convertedInput: RelNode = convert(
                input,
                input.getTraitSet().replace(BindableConvention.INSTANCE)
            )
            return BindableWindow(
                rel.getCluster(), traitSet, convertedInput,
                winAgg.getConstants(), winAgg.getRowType(), winAgg.groups
            )
        }

        companion object {
            /** Default configuration.  */
            val DEFAULT_CONFIG: Config = Config.INSTANCE
                .withConversion(
                    LogicalWindow::class.java, Convention.NONE,
                    BindableConvention.INSTANCE, "BindableWindowRule"
                )
                .withRuleFactory { config: Config? -> BindableWindowRule(config) }
        }
    }

    /** Implementation of [org.apache.calcite.rel.core.Match]
     * in bindable convention.  */
    class BindableMatch
    /** Singleton instance of BindableMatch.  */
    internal constructor(
        cluster: RelOptCluster?, traitSet: RelTraitSet?, input: RelNode?,
        rowType: RelDataType?, pattern: RexNode?, strictStart: Boolean,
        strictEnd: Boolean, patternDefinitions: Map<String?, RexNode?>?,
        measures: Map<String?, RexNode?>?, after: RexNode?,
        subsets: Map<String?, SortedSet<String?>?>?, allRows: Boolean,
        partitionKeys: ImmutableBitSet?, orderKeys: RelCollation?,
        @Nullable interval: RexNode?
    ) : Match(
        cluster, traitSet, input, rowType, pattern, strictStart, strictEnd,
        patternDefinitions, measures, after, subsets, allRows, partitionKeys,
        orderKeys, interval
    ), BindableRel {
        @Override
        fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>): RelNode {
            return BindableMatch(
                getCluster(), traitSet, inputs[0], getRowType(),
                pattern, strictStart, strictEnd, patternDefinitions, measures, after,
                subsets, allRows, partitionKeys, orderKeys, interval
            )
        }

        @get:Override
        val elementType: Class<Array<Object>>
            get() = Array<Object>::class.java

        @Override
        fun bind(dataContext: DataContext): Enumerable<Array<Object>> {
            return help(dataContext, this)
        }

        @Override
        fun implement(implementor: InterpreterImplementor): Node {
            return MatchNode(implementor.compiler, this)
        }
    }

    /**
     * Rule to convert a [org.apache.calcite.rel.logical.LogicalMatch]
     * to a [BindableMatch].
     *
     * @see .BINDABLE_MATCH_RULE
     */
    class BindableMatchRule
    /** Called from the Config.  */
    protected constructor(config: Config?) : ConverterRule(config) {
        @Override
        fun convert(rel: RelNode): RelNode {
            val match: LogicalMatch = rel as LogicalMatch
            val traitSet: RelTraitSet = match.getTraitSet().replace(BindableConvention.INSTANCE)
            val input: RelNode = match.getInput()
            val convertedInput: RelNode = convert(
                input,
                input.getTraitSet().replace(BindableConvention.INSTANCE)
            )
            return BindableMatch(
                rel.getCluster(), traitSet, convertedInput,
                match.getRowType(), match.getPattern(), match.isStrictStart(),
                match.isStrictEnd(), match.getPatternDefinitions(),
                match.getMeasures(), match.getAfter(), match.getSubsets(),
                match.isAllRows(), match.getPartitionKeys(), match.getOrderKeys(),
                match.getInterval()
            )
        }

        companion object {
            /** Default configuration.  */
            val DEFAULT_CONFIG: Config = Config.INSTANCE
                .withConversion(
                    LogicalMatch::class.java, Convention.NONE,
                    BindableConvention.INSTANCE, "BindableMatchRule"
                )
                .withRuleFactory { config: Config? -> BindableMatchRule(config) }
        }
    }
}
