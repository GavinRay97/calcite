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
package org.apache.calcite.rel.rules

import org.apache.calcite.plan.RelOptCluster

/**
 * Collection of planner rules that apply various simplifying transformations on
 * RexNode trees. Currently, there are two transformations:
 *
 *
 *  * Constant reduction, which evaluates constant subtrees, replacing them
 * with a corresponding RexLiteral
 *  * Removal of redundant casts, which occurs when the argument into the cast
 * is the same as the type of the resulting cast expression
 *
 *
 * @param <C> Configuration type
</C> */
abstract class ReduceExpressionsRule<C : ReduceExpressionsRule.Config?>  //~ Constructors -----------------------------------------------------------
/** Creates a ReduceExpressionsRule.  */
protected constructor(config: C) : RelRule<C>(config), SubstitutionRule {
    /**
     * Rule that reduces constants inside a [org.apache.calcite.rel.core.Filter].
     * If the condition is a constant, the filter is removed (if TRUE) or replaced with
     * an empty [org.apache.calcite.rel.core.Values] (if FALSE or NULL).
     *
     * @see CoreRules.FILTER_REDUCE_EXPRESSIONS
     */
    class FilterReduceExpressionsRule
    /** Creates a FilterReduceExpressionsRule.  */
    protected constructor(config: FilterReduceExpressionsRuleConfig) :
        ReduceExpressionsRule<FilterReduceExpressionsRule.FilterReduceExpressionsRuleConfig?>(config) {
        @Deprecated // to be removed before 2.0
        constructor(
            filterClass: Class<out Filter?>?,
            relBuilderFactory: RelBuilderFactory?
        ) : this(
            FilterReduceExpressionsRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
                .`as`(FilterReduceExpressionsRuleConfig::class.java)
                .withOperandFor(filterClass)
                .withMatchNullability(true)
                .`as`(FilterReduceExpressionsRuleConfig::class.java)
        ) {
        }

        @Deprecated // to be removed before 2.0
        constructor(
            filterClass: Class<out Filter?>?,
            matchNullability: Boolean, relBuilderFactory: RelBuilderFactory?
        ) : this(
            FilterReduceExpressionsRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
                .`as`(FilterReduceExpressionsRuleConfig::class.java)
                .withOperandFor(filterClass)
                .withMatchNullability(matchNullability)
                .`as`(FilterReduceExpressionsRuleConfig::class.java)
        ) {
        }

        @Override
        fun onMatch(call: RelOptRuleCall) {
            val filter: Filter = call.rel(0)
            val expList: List<RexNode> = Lists.newArrayList(filter.getCondition())
            var newConditionExp: RexNode
            val reduced: Boolean
            val mq: RelMetadataQuery = call.getMetadataQuery()
            val predicates: RelOptPredicateList = mq.getPulledUpPredicates(filter.getInput())
            if (reduceExpressions(
                    filter, expList, predicates, true,
                    config.matchNullability(), config.treatDynamicCallsAsConstant()
                )
            ) {
                assert(expList.size() === 1)
                newConditionExp = expList[0]
                reduced = true
            } else {
                // No reduction, but let's still test the original
                // predicate to see if it was already a constant,
                // in which case we don't need any runtime decision
                // about filtering.
                newConditionExp = filter.getCondition()
                reduced = false
            }

            // Even if no reduction, let's still test the original
            // predicate to see if it was already a constant,
            // in which case we don't need any runtime decision
            // about filtering.
            if (newConditionExp.isAlwaysTrue()) {
                call.transformTo(
                    filter.getInput()
                )
            } else if (newConditionExp is RexLiteral
                || RexUtil.isNullLiteral(newConditionExp, true)
            ) {
                call.transformTo(createEmptyRelOrEquivalent(call, filter))
            } else if (reduced) {
                call.transformTo(
                    call.builder()
                        .push(filter.getInput())
                        .filter(newConditionExp).build()
                )
            } else {
                if (newConditionExp is RexCall) {
                    val reverse = newConditionExp.getKind() === SqlKind.NOT
                    if (reverse) {
                        newConditionExp = (newConditionExp as RexCall).getOperands().get(0)
                    }
                    reduceNotNullableFilter(call, filter, newConditionExp, reverse)
                }
                return
            }

            // New plan is absolutely better than old plan.
            call.getPlanner().prune(filter)
        }

        /**
         * For static schema systems, a filter that is always false or null can be
         * replaced by a values operator that produces no rows, as the schema
         * information can just be taken from the input Rel. In dynamic schema
         * environments, the filter might have an unknown input type, in these cases
         * they must define a system specific alternative to a Values operator, such
         * as inserting a limit 0 instead of a filter on top of the original input.
         *
         *
         * The default implementation of this method is to call
         * [RelBuilder.empty], which for the static schema will be optimized
         * to an empty
         * [org.apache.calcite.rel.core.Values].
         *
         * @param input rel to replace, assumes caller has already determined
         * equivalence to Values operation for 0 records or a
         * false filter.
         * @return equivalent but less expensive replacement rel
         */
        protected fun createEmptyRelOrEquivalent(call: RelOptRuleCall, input: Filter?): RelNode {
            return call.builder().push(input).empty().build()
        }

        private fun reduceNotNullableFilter(
            call: RelOptRuleCall,
            filter: Filter,
            rexNode: RexNode,
            reverse: Boolean
        ) {
            // If the expression is a IS [NOT] NULL on a non-nullable
            // column, then we can either remove the filter or replace
            // it with an Empty.
            var alwaysTrue: Boolean
            alwaysTrue = when (rexNode.getKind()) {
                IS_NULL, IS_UNKNOWN -> false
                IS_NOT_NULL -> true
                else -> return
            }
            if (reverse) {
                alwaysTrue = !alwaysTrue
            }
            val operand: RexNode = (rexNode as RexCall).getOperands().get(0)
            if (operand is RexInputRef) {
                val inputRef: RexInputRef = operand as RexInputRef
                if (!inputRef.getType().isNullable()) {
                    if (alwaysTrue) {
                        call.transformTo(filter.getInput())
                    } else {
                        call.transformTo(createEmptyRelOrEquivalent(call, filter))
                    }
                    // New plan is absolutely better than old plan.
                    call.getPlanner().prune(filter)
                }
            }
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface FilterReduceExpressionsRuleConfig : Config {
            @Override
            override fun toRule(): FilterReduceExpressionsRule {
                return FilterReduceExpressionsRule(this)
            }

            companion object {
                val DEFAULT: FilterReduceExpressionsRuleConfig = ImmutableFilterReduceExpressionsRuleConfig.of()
                    .withMatchNullability(true)
                    .withOperandFor(LogicalFilter::class.java)
                    .withDescription("ReduceExpressionsRule(Filter)")
                    .`as`(FilterReduceExpressionsRuleConfig::class.java)
            }
        }
    }

    /** Rule that reduces constants inside a
     * [org.apache.calcite.rel.core.Project].
     *
     * @see CoreRules.PROJECT_REDUCE_EXPRESSIONS
     */
    class ProjectReduceExpressionsRule
    /** Creates a ProjectReduceExpressionsRule.  */
    protected constructor(config: ProjectReduceExpressionsRuleConfig) :
        ReduceExpressionsRule<ProjectReduceExpressionsRule.ProjectReduceExpressionsRuleConfig?>(config) {
        @Deprecated // to be removed before 2.0
        constructor(
            projectClass: Class<out Project?>?,
            relBuilderFactory: RelBuilderFactory?
        ) : this(
            ProjectReduceExpressionsRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
                .`as`(ProjectReduceExpressionsRuleConfig::class.java)
                .withOperandFor(projectClass)
                .`as`(ProjectReduceExpressionsRuleConfig::class.java)
        ) {
        }

        @Deprecated // to be removed before 2.0
        constructor(
            projectClass: Class<out Project?>?,
            matchNullability: Boolean, relBuilderFactory: RelBuilderFactory?
        ) : this(
            ProjectReduceExpressionsRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
                .`as`(ProjectReduceExpressionsRuleConfig::class.java)
                .withOperandFor(projectClass)
                .withMatchNullability(matchNullability)
                .`as`(ProjectReduceExpressionsRuleConfig::class.java)
        ) {
        }

        @Override
        fun onMatch(call: RelOptRuleCall) {
            val project: Project = call.rel(0)
            val mq: RelMetadataQuery = call.getMetadataQuery()
            val predicates: RelOptPredicateList = mq.getPulledUpPredicates(project.getInput())
            val expList: List<RexNode> = Lists.newArrayList(project.getProjects())
            if (reduceExpressions(
                    project, expList, predicates, false,
                    config.matchNullability(), config.treatDynamicCallsAsConstant()
                )
            ) {
                assert(
                    !project.getProjects().equals(expList)
                ) { "Reduced expressions should be different from original expressions" }
                call.transformTo(
                    call.builder()
                        .push(project.getInput())
                        .project(expList, project.getRowType().getFieldNames())
                        .build()
                )

                // New plan is absolutely better than old plan.
                call.getPlanner().prune(project)
            }
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface ProjectReduceExpressionsRuleConfig : Config {
            @Override
            override fun toRule(): ProjectReduceExpressionsRule {
                return ProjectReduceExpressionsRule(this)
            }

            companion object {
                val DEFAULT: ProjectReduceExpressionsRuleConfig = ImmutableProjectReduceExpressionsRuleConfig.of()
                    .withMatchNullability(true)
                    .withOperandFor(LogicalProject::class.java)
                    .withDescription("ReduceExpressionsRule(Project)")
                    .`as`(ProjectReduceExpressionsRuleConfig::class.java)
            }
        }
    }

    /** Rule that reduces constants inside a [Join].
     *
     * @see CoreRules.JOIN_REDUCE_EXPRESSIONS
     */
    class JoinReduceExpressionsRule
    /** Creates a JoinReduceExpressionsRule.  */
    protected constructor(config: JoinReduceExpressionsRuleConfig) :
        ReduceExpressionsRule<JoinReduceExpressionsRule.JoinReduceExpressionsRuleConfig?>(config) {
        @Deprecated // to be removed before 2.0
        constructor(
            joinClass: Class<out Join?>?,
            relBuilderFactory: RelBuilderFactory?
        ) : this(
            JoinReduceExpressionsRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
                .`as`(JoinReduceExpressionsRuleConfig::class.java)
                .withOperandFor(joinClass)
                .withMatchNullability(true)
                .`as`(JoinReduceExpressionsRuleConfig::class.java)
        ) {
        }

        @Deprecated // to be removed before 2.0
        constructor(
            joinClass: Class<out Join?>?,
            matchNullability: Boolean, relBuilderFactory: RelBuilderFactory?
        ) : this(
            JoinReduceExpressionsRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
                .`as`(JoinReduceExpressionsRuleConfig::class.java)
                .withOperandFor(joinClass)
                .withMatchNullability(matchNullability)
                .`as`(JoinReduceExpressionsRuleConfig::class.java)
        ) {
        }

        @Override
        fun onMatch(call: RelOptRuleCall) {
            val join: Join = call.rel(0)
            val expList: List<RexNode> = Lists.newArrayList(join.getCondition())
            val fieldCount: Int = join.getLeft().getRowType().getFieldCount()
            val mq: RelMetadataQuery = call.getMetadataQuery()
            val leftPredicates: RelOptPredicateList = mq.getPulledUpPredicates(join.getLeft())
            val rightPredicates: RelOptPredicateList = mq.getPulledUpPredicates(join.getRight())
            val rexBuilder: RexBuilder = join.getCluster().getRexBuilder()
            val predicates: RelOptPredicateList = leftPredicates.union(
                rexBuilder,
                rightPredicates.shift(rexBuilder, fieldCount)
            )
            if (!reduceExpressions(
                    join, expList, predicates, true,
                    config.matchNullability(), config.treatDynamicCallsAsConstant()
                )
            ) {
                return
            }
            call.transformTo(
                join.copy(
                    join.getTraitSet(),
                    expList[0],
                    join.getLeft(),
                    join.getRight(),
                    join.getJoinType(),
                    join.isSemiJoinDone()
                )
            )

            // New plan is absolutely better than old plan.
            call.getPlanner().prune(join)
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface JoinReduceExpressionsRuleConfig : Config {
            @Override
            override fun toRule(): JoinReduceExpressionsRule {
                return JoinReduceExpressionsRule(this)
            }

            companion object {
                val DEFAULT: JoinReduceExpressionsRuleConfig = ImmutableJoinReduceExpressionsRuleConfig.of()
                    .withMatchNullability(false)
                    .withOperandFor(Join::class.java)
                    .withDescription("ReduceExpressionsRule(Join)")
                    .`as`(JoinReduceExpressionsRuleConfig::class.java)
            }
        }
    }

    /**
     * Rule that reduces constants inside a [Calc].
     *
     * @see CoreRules.CALC_REDUCE_EXPRESSIONS
     */
    class CalcReduceExpressionsRule
    /** Creates a CalcReduceExpressionsRule.  */
    protected constructor(config: CalcReduceExpressionsRuleConfig) :
        ReduceExpressionsRule<CalcReduceExpressionsRule.CalcReduceExpressionsRuleConfig?>(config) {
        @Deprecated // to be removed before 2.0
        constructor(
            calcClass: Class<out Calc?>?,
            relBuilderFactory: RelBuilderFactory?
        ) : this(
            CalcReduceExpressionsRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
                .`as`(CalcReduceExpressionsRuleConfig::class.java)
                .withOperandFor(calcClass)
                .withMatchNullability(true)
                .`as`(CalcReduceExpressionsRuleConfig::class.java)
        ) {
        }

        @Deprecated // to be removed before 2.0
        constructor(
            calcClass: Class<out Calc?>?,
            matchNullability: Boolean, relBuilderFactory: RelBuilderFactory?
        ) : this(
            CalcReduceExpressionsRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
                .`as`(CalcReduceExpressionsRuleConfig::class.java)
                .withOperandFor(calcClass)
                .withMatchNullability(matchNullability)
                .`as`(CalcReduceExpressionsRuleConfig::class.java)
        ) {
        }

        @Override
        fun onMatch(call: RelOptRuleCall) {
            val calc: Calc = call.rel(0)
            val program: RexProgram = calc.getProgram()
            val exprList: List<RexNode> = program.getExprList()

            // Form a list of expressions with sub-expressions fully expanded.
            val expandedExprList: List<RexNode> = ArrayList()
            val shuttle: RexShuttle = object : RexShuttle() {
                @Override
                fun visitLocalRef(localRef: RexLocalRef): RexNode {
                    return expandedExprList[localRef.getIndex()]
                }
            }
            for (expr in exprList) {
                expandedExprList.add(expr.accept(shuttle))
            }
            val predicates: RelOptPredicateList = RelOptPredicateList.EMPTY
            if (reduceExpressions(
                    calc, expandedExprList, predicates, false,
                    config.matchNullability(), config.treatDynamicCallsAsConstant()
                )
            ) {
                val builder = RexProgramBuilder(
                    calc.getInput().getRowType(),
                    calc.getCluster().getRexBuilder()
                )
                val list: List<RexLocalRef> = ArrayList()
                for (expr in expandedExprList) {
                    list.add(builder.registerInput(expr))
                }
                if (program.getCondition() != null) {
                    val conditionIndex: Int = program.getCondition().getIndex()
                    val newConditionExp: RexNode = expandedExprList[conditionIndex]
                    if (newConditionExp.isAlwaysTrue()) {
                        // condition is always TRUE - drop it.
                    } else if (newConditionExp is RexLiteral
                        || RexUtil.isNullLiteral(newConditionExp, true)
                    ) {
                        // condition is always NULL or FALSE - replace calc
                        // with empty.
                        call.transformTo(createEmptyRelOrEquivalent(call, calc))
                        return
                    } else {
                        builder.addCondition(list[conditionIndex])
                    }
                }
                var k = 0
                for (projectExpr in program.getProjectList()) {
                    val index: Int = projectExpr.getIndex()
                    builder.addProject(
                        list[index].getIndex(),
                        program.getOutputRowType().getFieldNames().get(k++)
                    )
                }
                call.transformTo(
                    calc.copy(calc.getTraitSet(), calc.getInput(), builder.getProgram())
                )

                // New plan is absolutely better than old plan.
                call.getPlanner().prune(calc)
            }
        }

        /**
         * For static schema systems, a filter that is always false or null can be
         * replaced by a values operator that produces no rows, as the schema
         * information can just be taken from the input Rel. In dynamic schema
         * environments, the filter might have an unknown input type, in these cases
         * they must define a system specific alternative to a Values operator, such
         * as inserting a limit 0 instead of a filter on top of the original input.
         *
         *
         * The default implementation of this method is to call
         * [RelBuilder.empty], which for the static schema will be optimized
         * to an Immutable.Config.of()
         * [org.apache.calcite.rel.core.Values].
         *
         * @param input rel to replace, assumes caller has already determined
         * equivalence to Values operation for 0 records or a
         * false filter.
         * @return equivalent but less expensive replacement rel
         */
        protected fun createEmptyRelOrEquivalent(call: RelOptRuleCall, input: Calc?): RelNode {
            return call.builder().push(input).empty().build()
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface CalcReduceExpressionsRuleConfig : Config {
            @Override
            override fun toRule(): CalcReduceExpressionsRule {
                return CalcReduceExpressionsRule(this)
            }

            companion object {
                val DEFAULT: CalcReduceExpressionsRuleConfig = ImmutableCalcReduceExpressionsRuleConfig.of()
                    .withMatchNullability(true)
                    .withOperandFor(LogicalCalc::class.java)
                    .withDescription("ReduceExpressionsRule(Calc)")
                    .`as`(CalcReduceExpressionsRuleConfig::class.java)
            }
        }
    }

    /** Rule that reduces constants inside a [Window].
     *
     * @see CoreRules.WINDOW_REDUCE_EXPRESSIONS
     */
    class WindowReduceExpressionsRule
    /** Creates a WindowReduceExpressionsRule.  */
    protected constructor(config: WindowReduceExpressionsRuleConfig) :
        ReduceExpressionsRule<WindowReduceExpressionsRule.WindowReduceExpressionsRuleConfig?>(config) {
        @Deprecated // to be removed before 2.0
        constructor(
            windowClass: Class<out Window?>?,
            matchNullability: Boolean, relBuilderFactory: RelBuilderFactory?
        ) : this(
            WindowReduceExpressionsRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
                .`as`(WindowReduceExpressionsRuleConfig::class.java)
                .withOperandFor(windowClass)
                .withMatchNullability(matchNullability)
                .`as`(WindowReduceExpressionsRuleConfig::class.java)
        ) {
        }

        @Override
        fun onMatch(call: RelOptRuleCall) {
            val window: LogicalWindow = call.rel(0)
            val rexBuilder: RexBuilder = window.getCluster().getRexBuilder()
            val mq: RelMetadataQuery = call.getMetadataQuery()
            val predicates: RelOptPredicateList = mq
                .getPulledUpPredicates(window.getInput())
            var reduced = false
            val groups: List<Window.Group> = ArrayList()
            for (group in window.groups) {
                val aggCalls: List<Window.RexWinAggCall> = ArrayList()
                for (aggCall in group.aggCalls) {
                    val expList: List<RexNode> = ArrayList(aggCall.getOperands())
                    if (reduceExpressions(window, expList, predicates)) {
                        aggCall = RexWinAggCall(
                            aggCall.getOperator() as SqlAggFunction, aggCall.type, expList,
                            aggCall.ordinal, aggCall.distinct, aggCall.ignoreNulls
                        )
                        reduced = true
                    }
                    aggCalls.add(aggCall)
                }
                val keyBuilder: ImmutableBitSet.Builder = ImmutableBitSet.builder()
                for (key in group.keys) {
                    if (!predicates.constantMap.containsKey(
                            rexBuilder.makeInputRef(window.getInput(), key)
                        )
                    ) {
                        keyBuilder.set(key)
                    }
                }
                val keys: ImmutableBitSet = keyBuilder.build()
                reduced = reduced or (keys.cardinality() !== group.keys.cardinality())
                val collationsList: List<RelFieldCollation> = group.orderKeys
                    .getFieldCollations().stream()
                    .filter { fc ->
                        !predicates.constantMap.containsKey(
                            rexBuilder.makeInputRef(
                                window.getInput(),
                                fc.getFieldIndex()
                            )
                        )
                    }
                    .collect(Collectors.toList())
                val collationReduced = group.orderKeys.getFieldCollations().size() !== collationsList.size()
                reduced = reduced or collationReduced
                val relCollation: RelCollation =
                    if (collationReduced) RelCollations.of(collationsList) else group.orderKeys
                groups.add(
                    Group(
                        keys, group.isRows, group.lowerBound,
                        group.upperBound, relCollation, aggCalls
                    )
                )
            }
            if (reduced) {
                call.transformTo(
                    LogicalWindow
                        .create(
                            window.getTraitSet(), window.getInput(),
                            window.getConstants(), window.getRowType(), groups
                        )
                )
                call.getPlanner().prune(window)
            }
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface WindowReduceExpressionsRuleConfig : Config {
            @Override
            override fun toRule(): WindowReduceExpressionsRule {
                return WindowReduceExpressionsRule(this)
            }

            companion object {
                val DEFAULT: WindowReduceExpressionsRuleConfig = ImmutableWindowReduceExpressionsRuleConfig.of()
                    .withMatchNullability(true)
                    .withOperandFor(LogicalWindow::class.java)
                    .withDescription("ReduceExpressionsRule(Window)")
                    .`as`(WindowReduceExpressionsRuleConfig::class.java)
            }
        }
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Replaces expressions with their reductions. Note that we only have to
     * look for RexCall, since nothing else is reducible in the first place.
     */
    protected class RexReplacer internal constructor(
        simplify: RexSimplify,
        unknownAs: RexUnknownAs?,
        reducibleExps: List<RexNode>,
        reducedValues: List<RexNode>,
        addCasts: List<Boolean>
    ) : RexShuttle() {
        private val simplify: RexSimplify
        private val reducibleExps: List<RexNode>
        private val reducedValues: List<RexNode>
        private val addCasts: List<Boolean>

        init {
            this.simplify = simplify
            this.reducibleExps = reducibleExps
            this.reducedValues = reducedValues
            this.addCasts = addCasts
        }

        @Override
        fun visitInputRef(inputRef: RexInputRef): RexNode {
            return visit(inputRef) ?: return super.visitInputRef(inputRef)
        }

        @Override
        fun visitCall(call: RexCall): RexNode? {
            var node: RexNode? = visit(call)
            if (node != null) {
                return node
            }
            node = super.visitCall(call)
            return node
        }

        @Nullable
        private fun visit(call: RexNode): RexNode? {
            val i = reducibleExps.indexOf(call)
            if (i == -1) {
                return null
            }
            var replacement: RexNode = reducedValues[i]
            if (addCasts[i]
                && replacement.getType() !== call.getType()
            ) {
                // Handle change from nullable to NOT NULL by claiming
                // that the result is still nullable, even though
                // we know it isn't.
                //
                // Also, we cannot reduce CAST('abc' AS VARCHAR(4)) to 'abc'.
                // If we make 'abc' of type VARCHAR(4), we may later encounter
                // the same expression in a Project's digest where it has
                // type VARCHAR(3), and that's wrong.
                replacement = simplify.rexBuilder.makeAbstractCast(call.getType(), replacement)
            }
            return replacement
        }
    }

    /**
     * Helper class used to locate expressions that either can be reduced to
     * literals or contain redundant casts.
     */
    protected class ReducibleExprLocator internal constructor(
        typeFactory: RelDataTypeFactory?,
        constants: ImmutableMap<RexNode?, RexNode?>, constExprs: List<RexNode>,
        addCasts: List<Boolean>, treatDynamicCallsAsConstant: Boolean
    ) : RexVisitorImpl<Void?>(true) {
        /** Whether an expression is constant, and if so, whether it can be
         * reduced to a simpler constant.  */
        internal enum class Constancy {
            NON_CONSTANT, REDUCIBLE_CONSTANT, IRREDUCIBLE_CONSTANT
        }

        private val treatDynamicCallsAsConstant: Boolean
        private val stack: List<Constancy> = ArrayList()
        private val constants: ImmutableMap<RexNode, RexNode>
        private val constExprs: List<RexNode>
        private val addCasts: List<Boolean>
        private val parentCallTypeStack: Deque<SqlOperator> = ArrayDeque()

        init {
            // go deep
            this.constants = constants
            this.constExprs = constExprs
            this.addCasts = addCasts
            this.treatDynamicCallsAsConstant = treatDynamicCallsAsConstant
        }

        fun analyze(exp: RexNode) {
            assert(stack.isEmpty())
            exp.accept(this)
            assert(stack.size() === 1)
            assert(parentCallTypeStack.isEmpty())
            val rootConstancy = stack[0]
            if (rootConstancy == Constancy.REDUCIBLE_CONSTANT) {
                // The entire subtree was constant, so add it to the result.
                addResult(exp)
            }
            stack.clear()
        }

        private fun pushVariable(): Void? {
            stack.add(Constancy.NON_CONSTANT)
            return null
        }

        private fun addResult(exp: RexNode) {
            // Cast of literal can't be reduced, so skip those (otherwise we'd
            // go into an infinite loop as we add them back).
            if (exp.getKind() === SqlKind.CAST) {
                val cast: RexCall = exp as RexCall
                val operand: RexNode = cast.getOperands().get(0)
                if (operand is RexLiteral) {
                    return
                }
            }
            constExprs.add(exp)

            // In the case where the expression corresponds to a UDR argument,
            // we need to preserve casts.  Note that this only applies to
            // the topmost argument, not expressions nested within the UDR
            // call.
            //
            // REVIEW zfong 6/13/08 - Are there other expressions where we
            // also need to preserve casts?
            val op: SqlOperator = parentCallTypeStack.peek()
            if (op == null) {
                addCasts.add(false)
            } else {
                addCasts.add(isUdf(op))
            }
        }

        @Override
        fun visitInputRef(inputRef: RexInputRef?): Void? {
            val constant: RexNode = constants.get(inputRef)
            if (constant != null) {
                if (constant is RexCall || constant is RexDynamicParam) {
                    constant.accept(this)
                } else {
                    stack.add(Constancy.REDUCIBLE_CONSTANT)
                }
                return null
            }
            return pushVariable()
        }

        @Override
        fun visitLiteral(literal: RexLiteral?): Void? {
            stack.add(Constancy.IRREDUCIBLE_CONSTANT)
            return null
        }

        @Override
        fun visitOver(over: RexOver): Void? {
            // assume non-constant (running SUM(1) looks constant but isn't)
            analyzeCall(over, Constancy.NON_CONSTANT)
            return null
        }

        @Override
        fun visitCorrelVariable(variable: RexCorrelVariable?): Void? {
            return pushVariable()
        }

        @Override
        fun visitCall(call: RexCall): Void? {
            // assume REDUCIBLE_CONSTANT until proven otherwise
            analyzeCall(call, Constancy.REDUCIBLE_CONSTANT)
            return null
        }

        @Override
        fun visitSubQuery(subQuery: RexSubQuery): Void? {
            analyzeCall(subQuery, Constancy.REDUCIBLE_CONSTANT)
            return null
        }

        private fun analyzeCall(call: RexCall, callConstancy: Constancy) {
            var callConstancy = callConstancy
            parentCallTypeStack.push(call.getOperator())

            // visit operands, pushing their states onto stack
            super.visitCall(call)

            // look for NON_CONSTANT operands
            val operandCount: Int = call.getOperands().size()
            val operandStack: List<Constancy> = Util.last(stack, operandCount)
            for (operandConstancy in operandStack) {
                if (operandConstancy == Constancy.NON_CONSTANT) {
                    callConstancy = Constancy.NON_CONSTANT
                    break
                }
            }

            // Even if all operands are constant, the call itself may
            // be non-deterministic.
            if (!call.getOperator().isDeterministic()) {
                callConstancy = Constancy.NON_CONSTANT
            } else if (!treatDynamicCallsAsConstant
                && call.getOperator().isDynamicFunction()
            ) {
                // In some circumstances, we should avoid caching the plan if we have dynamic functions.
                // If desired, treat this situation the same as a non-deterministic function.
                callConstancy = Constancy.NON_CONSTANT
            }

            // Row operator itself can't be reduced to a literal, but if
            // the operands are constants, we still want to reduce those
            if (callConstancy == Constancy.REDUCIBLE_CONSTANT
                && call.getOperator() is SqlRowOperator
            ) {
                callConstancy = Constancy.NON_CONSTANT
            }
            if (callConstancy == Constancy.NON_CONSTANT) {
                // any REDUCIBLE_CONSTANT children are now known to be maximal
                // reducible subtrees, so they can be added to the result
                // list
                for (iOperand in 0 until operandCount) {
                    val constancy = operandStack[iOperand]
                    if (constancy == Constancy.REDUCIBLE_CONSTANT) {
                        addResult(call.getOperands().get(iOperand))
                    }
                }
            }

            // pop operands off of the stack
            operandStack.clear()

            // pop this parent call operator off the stack
            parentCallTypeStack.pop()

            // push constancy result for this call onto stack
            stack.add(callConstancy)
        }

        @Override
        fun visitDynamicParam(dynamicParam: RexDynamicParam?): Void? {
            return pushVariable()
        }

        @Override
        fun visitRangeRef(rangeRef: RexRangeRef?): Void? {
            return pushVariable()
        }

        @Override
        fun visitFieldAccess(fieldAccess: RexFieldAccess?): Void? {
            return pushVariable()
        }

        companion object {
            private fun isUdf(@SuppressWarnings("unused") operator: SqlOperator): Boolean {
                // return operator instanceof UserDefinedRoutine
                return false
            }
        }
    }

    /** Shuttle that pushes predicates into a CASE.  */
    protected class CaseShuttle : RexShuttle() {
        @Override
        fun visitCall(call: RexCall): RexNode {
            var call: RexCall = call
            while (true) {
                call = super.visitCall(call) as RexCall
                val old: RexCall = call
                call = pushPredicateIntoCase(call)
                if (call === old) {
                    return call
                }
            }
        }
    }

    /** Rule configuration.  */
    interface Config : RelRule.Config {
        @Override
        fun toRule(): ReduceExpressionsRule<*>?

        /** Whether to add a CAST when a nullable expression
         * reduces to a NOT NULL literal.  */
        @Value.Default
        fun matchNullability(): Boolean {
            return false
        }

        /** Sets [.matchNullability].  */
        fun withMatchNullability(matchNullability: Boolean): Config?

        /** Whether to treat
         * [dynamic functions][SqlOperator.isDynamicFunction] as constants.
         *
         *
         * When false (the default), calls to dynamic functions (e.g.
         * `USER`) are not reduced. When true, calls to dynamic functions
         * are treated as a constant, and reduced.  */
        @Value.Default
        fun treatDynamicCallsAsConstant(): Boolean {
            return false
        }

        /** Sets [.treatDynamicCallsAsConstant].  */
        fun withTreatDynamicCallsAsConstant(treatDynamicCallsAsConstant: Boolean): Config?

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(relClass: Class<out RelNode?>?): Config? {
            return withOperandSupplier { b -> b.operand(relClass).anyInputs() }
                .`as`(Config::class.java)
        }
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        /**
         * Regular expression that matches the description of all instances of this
         * rule and [ValuesReduceRule] also. Use
         * it to prevent the planner from invoking these rules.
         */
        val EXCLUSION_PATTERN: Pattern = Pattern.compile("Reduce(Expressions|Values)Rule.*")
        //~ Methods ----------------------------------------------------------------
        /**
         * Reduces a list of expressions.
         *
         * @param rel     Relational expression
         * @param expList List of expressions, modified in place
         * @param predicates Constraints known to hold on input expressions
         * @return whether reduction found something to change, and succeeded
         */
        protected fun reduceExpressions(
            rel: RelNode?, expList: List<RexNode?>,
            predicates: RelOptPredicateList
        ): Boolean {
            return reduceExpressions(rel, expList, predicates, false, true, false)
        }

        @Deprecated // to be removed before 2.0
        protected fun reduceExpressions(
            rel: RelNode?, expList: List<RexNode?>,
            predicates: RelOptPredicateList, unknownAsFalse: Boolean
        ): Boolean {
            return reduceExpressions(rel, expList, predicates, unknownAsFalse, true, false)
        }

        /**
         * Reduces a list of expressions.
         *
         *
         * The `matchNullability` flag comes into play when reducing a
         * expression whose type is nullable. Suppose we are reducing an expression
         * `CASE WHEN 'a' = 'a' THEN 1 ELSE NULL END`. Before reduction the
         * type is `INTEGER` (nullable), but after reduction the literal 1 has
         * type `INTEGER NOT NULL`.
         *
         *
         * In some situations it is more important to preserve types; in this
         * case you should use `matchNullability = true` (which used to be
         * the default behavior of this method), and it will cast the literal to
         * `INTEGER` (nullable).
         *
         *
         * In other situations, you would rather propagate the new stronger type,
         * because it may allow further optimizations later; pass
         * `matchNullability = false` and no cast will be added, but you may
         * need to adjust types elsewhere in the expression tree.
         *
         * @param rel     Relational expression
         * @param expList List of expressions, modified in place
         * @param predicates Constraints known to hold on input expressions
         * @param unknownAsFalse Whether UNKNOWN will be treated as FALSE
         * @param matchNullability Whether Calcite should add a CAST to a literal
         * resulting from simplification and expression if the
         * expression had nullable type and the literal is
         * NOT NULL
         * @param treatDynamicCallsAsConstant Whether to treat dynamic functions as
         * constants
         *
         * @return whether reduction found something to change, and succeeded
         */
        fun reduceExpressions(
            rel: RelNode?, expList: List<RexNode?>,
            predicates: RelOptPredicateList, unknownAsFalse: Boolean,
            matchNullability: Boolean, treatDynamicCallsAsConstant: Boolean
        ): Boolean {
            val cluster: RelOptCluster = rel.getCluster()
            val rexBuilder: RexBuilder = cluster.getRexBuilder()
            val originExpList: List<RexNode> = Lists.newArrayList(expList)
            val executor: RexExecutor = Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR)
            val simplify = RexSimplify(rexBuilder, predicates, executor)

            // Simplify predicates in place
            val unknownAs: RexUnknownAs = RexUnknownAs.falseIf(unknownAsFalse)
            val reduced = reduceExpressionsInternal(
                rel, simplify, unknownAs,
                expList, predicates, treatDynamicCallsAsConstant
            )
            var simplified = false
            for (i in 0 until expList.size()) {
                val expr2: RexNode = simplify.simplifyPreservingType(
                    expList[i], unknownAs,
                    matchNullability
                )
                if (!expr2.equals(expList[i])) {
                    expList.set(i, expr2)
                    simplified = true
                }
            }
            return if (reduced && simplified) {
                !originExpList.equals(expList)
            } else reduced || simplified
        }

        protected fun reduceExpressionsInternal(
            rel: RelNode?,
            simplify: RexSimplify, unknownAs: RexUnknownAs?, expList: List<RexNode?>,
            predicates: RelOptPredicateList, treatDynamicCallsAsConstant: Boolean
        ): Boolean {
            // Replace predicates on CASE to CASE on predicates.
            val changed: Boolean = CaseShuttle().mutate(expList)

            // Find reducible expressions.
            val constExps: List<RexNode> = ArrayList()
            var addCasts: List<Boolean> = ArrayList()
            findReducibleExps(
                rel.getCluster().getTypeFactory(), expList,
                predicates.constantMap, constExps, addCasts, treatDynamicCallsAsConstant
            )
            if (constExps.isEmpty()) {
                return changed
            }
            val constExps2: List<RexNode> = Lists.newArrayList(constExps)
            if (!predicates.constantMap.isEmpty()) {
                val pairs: List<Map.Entry<RexNode, RexNode>> = Lists.newArrayList(predicates.constantMap.entrySet())
                val replacer = RexReplacer(
                    simplify, unknownAs, Pair.left(pairs),
                    Pair.right(pairs), Collections.nCopies(pairs.size(), false)
                )
                replacer.mutate(constExps2)
            }

            // Compute the values they reduce to.
            val executor: RexExecutor = rel.getCluster().getPlanner().getExecutor()
                ?: // Cannot reduce expressions: caller has not set an executor in their
                // environment. Caller should execute something like the following before
                // invoking the planner:
                //
                // final RexExecutorImpl executor =
                //   new RexExecutorImpl(Schemas.createDataContext(null));
                // rootRel.getCluster().getPlanner().setExecutor(executor);
                return changed
            val reducedValues: List<RexNode> = ArrayList()
            executor.reduce(simplify.rexBuilder, constExps2, reducedValues)

            // Use RexNode.digest to judge whether each newly generated RexNode
            // is equivalent to the original one.
            if (RexUtil.strings(constExps).equals(RexUtil.strings(reducedValues))) {
                return changed
            }

            // For Project, we have to be sure to preserve the result
            // types, so always cast regardless of the expression type.
            // For other RelNodes like Filter, in general, this isn't necessary,
            // and the presence of casts could hinder other rules such as sarg
            // analysis, which require bare literals.  But there are special cases,
            // like when the expression is a UDR argument, that need to be
            // handled as special cases.
            if (rel is Project) {
                addCasts = Collections.nCopies(reducedValues.size(), true)
            }
            RexReplacer(simplify, unknownAs, constExps, reducedValues, addCasts)
                .mutate(expList)
            return true
        }

        /**
         * Locates expressions that can be reduced to literals or converted to
         * expressions with redundant casts removed.
         *
         * @param typeFactory    Type factory
         * @param exps           list of candidate expressions to be examined for
         * reduction
         * @param constants      List of expressions known to be constant
         * @param constExps      returns the list of expressions that can be constant
         * reduced
         * @param addCasts       indicator for each expression that can be constant
         * reduced, whether a cast of the resulting reduced
         * expression is potentially necessary
         * @param treatDynamicCallsAsConstant Whether to treat dynamic functions as
         * constants
         */
        protected fun findReducibleExps(
            typeFactory: RelDataTypeFactory?,
            exps: List<RexNode?>, constants: ImmutableMap<RexNode?, RexNode?>,
            constExps: List<RexNode>, addCasts: List<Boolean>, treatDynamicCallsAsConstant: Boolean
        ) {
            val gardener = ReducibleExprLocator(
                typeFactory, constants, constExps,
                addCasts, treatDynamicCallsAsConstant
            )
            for (exp in exps) {
                gardener.analyze(exp)
            }
            assert(constExps.size() === addCasts.size())
        }

        /** Creates a map containing each (e, constant) pair that occurs within
         * a predicate list.
         *
         * @param clazz Class of expression that is considered constant
         * @param rexBuilder Rex builder
         * @param predicates Predicate list
         * @param <C> what to consider a constant: [RexLiteral] to use a narrow
         * definition of constant, or [RexNode] to use
         * [RexUtil.isConstant]
         * @return Map from values to constants
         *
        </C> */
        @Deprecated // to be removed before 2.0
        @Deprecated("Use {@link RelOptPredicateList#constantMap}")
        fun <C : RexNode?> predicateConstants(
            clazz: Class<C>?, rexBuilder: RexBuilder?, predicates: RelOptPredicateList
        ): ImmutableMap<RexNode, C> {
            return RexUtil.predicateConstants(
                clazz, rexBuilder,
                predicates.pulledUpPredicates
            )
        }

        /** Pushes predicates into a CASE.
         *
         *
         * We have a loose definition of 'predicate': any boolean expression will
         * do, except CASE. For example '(CASE ...) = 5' or '(CASE ...) IS NULL'.
         */
        fun pushPredicateIntoCase(call: RexCall): RexCall {
            if (call.getType().getSqlTypeName() !== SqlTypeName.BOOLEAN) {
                return call
            }
            when (call.getKind()) {
                CASE, AND, OR -> return call // don't push CASE into CASE!
                EQUALS -> {

                    // checks that the EQUALS operands may be splitted and
                    // doesn't push EQUALS into CASE
                    val equalsOperands: List<RexNode> = call.getOperands()
                    val left: ImmutableBitSet = RelOptUtil.InputFinder.bits(equalsOperands[0])
                    val right: ImmutableBitSet = RelOptUtil.InputFinder.bits(equalsOperands[1])
                    if (!left.isEmpty() && !right.isEmpty() && left.intersect(right).isEmpty()) {
                        return call
                    }
                }
                else -> {}
            }
            var caseOrdinal = -1
            val operands: List<RexNode> = call.getOperands()
            for (i in 0 until operands.size()) {
                val operand: RexNode = operands[i]
                if (operand.getKind() === SqlKind.CASE) {
                    caseOrdinal = i
                }
            }
            if (caseOrdinal < 0) {
                return call
            }
            // Convert
            //   f(CASE WHEN p1 THEN v1 ... END, arg)
            // to
            //   CASE WHEN p1 THEN f(v1, arg) ... END
            val case_: RexCall = operands[caseOrdinal] as RexCall
            val nodes: List<RexNode> = ArrayList()
            for (i in 0 until case_.getOperands().size()) {
                var node: RexNode = case_.getOperands().get(i)
                if (!RexUtil.isCasePredicate(case_, i)) {
                    node = substitute(call, caseOrdinal, node)
                }
                nodes.add(node)
            }
            return case_.clone(call.getType(), nodes)
        }

        /** Converts op(arg0, ..., argOrdinal, ..., argN) to op(arg0,..., node, ..., argN).  */
        protected fun substitute(call: RexCall, ordinal: Int, node: RexNode?): RexNode {
            val newOperands: List<RexNode> = Lists.newArrayList(call.getOperands())
            newOperands.set(ordinal, node)
            return call.clone(call.getType(), newOperands)
        }
    }
}
