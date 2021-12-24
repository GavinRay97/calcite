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

import org.apache.calcite.plan.RelOptPredicateList

/**
 * Planner rule that folds projections and filters into an underlying
 * [org.apache.calcite.rel.logical.LogicalValues].
 *
 *
 * Returns a simplified `Values`, perhaps containing zero tuples
 * if all rows are filtered away.
 *
 *
 * For example,
 *
 * <blockquote>`select a - b from (values (1, 2), (3, 5), (7, 11)) as t (a,
 * b) where a + b > 4`</blockquote>
 *
 *
 * becomes
 *
 * <blockquote>`select x from (values (-2), (-4))`</blockquote>
 *
 *
 * Ignores an empty `Values`; this is better dealt with by
 * [PruneEmptyRules].
 *
 * @see CoreRules.FILTER_VALUES_MERGE
 *
 * @see CoreRules.PROJECT_VALUES_MERGE
 *
 * @see CoreRules.PROJECT_FILTER_VALUES_MERGE
 */
@Value.Enclosing
class ValuesReduceRule protected constructor(config: Config?) : RelRule<ValuesReduceRule.Config?>(config),
    TransformationRule {
    /** Creates a ValuesReduceRule.  */
    init {
        Util.discard(LOGGER)
    }

    @Deprecated // to be removed before 2.0
    constructor(
        operand: RelOptRuleOperand?,
        relBuilderFactory: RelBuilderFactory?, desc: String?
    ) : this(ImmutableValuesReduceRule.Config.builder().withRelBuilderFactory(relBuilderFactory)
        .withDescription(desc)
        .withOperandSupplier { b -> b.exactly(operand) }
        .withMatchHandler { u, v -> throw IllegalArgumentException("Match handler not set.") }
        .build()) {
        throw IllegalArgumentException("cannot guess matchHandler")
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall?) {
        config.matchHandler().accept(this, call)
    }

    /**
     * Does the work.
     *
     * @param call    Rule call
     * @param project Project, may be null
     * @param filter  Filter, may be null
     * @param values  Values rel to be reduced
     */
    protected fun apply(
        call: RelOptRuleCall, @Nullable project: LogicalProject?,
        @Nullable filter: LogicalFilter?, values: LogicalValues?
    ) {
        assert(values != null)
        assert(filter != null || project != null)
        val conditionExpr: RexNode? = if (filter == null) null else filter.getCondition()
        val projectExprs: List<RexNode>? = if (project == null) null else project.getProjects()
        val rexBuilder: RexBuilder = values.getCluster().getRexBuilder()

        // Find reducible expressions.
        val reducibleExps: List<RexNode> = ArrayList()
        val shuttle = MyRexShuttle()
        for (literalList in values.getTuples()) {
            shuttle.literalList = literalList
            if (conditionExpr != null) {
                val c: RexNode = conditionExpr.accept(shuttle)
                reducibleExps.add(c)
            }
            if (projectExprs != null) {
                requireNonNull(project, "project")
                var k = -1
                for (projectExpr in projectExprs) {
                    ++k
                    var e: RexNode = projectExpr.accept(shuttle)
                    if (RexLiteral.isNullLiteral(e)) {
                        e = rexBuilder.makeAbstractCast(
                            project.getRowType().getFieldList().get(k).getType(),
                            e
                        )
                    }
                    reducibleExps.add(e)
                }
            }
        }
        val fieldsPerRow: Int = ((if (conditionExpr == null) 0 else 1)
                + (projectExprs?.size() ?: 0))
        assert(fieldsPerRow > 0)
        assert(reducibleExps.size() === values.getTuples().size() * fieldsPerRow)

        // Compute the values they reduce to.
        val predicates: RelOptPredicateList = RelOptPredicateList.EMPTY
        ReduceExpressionsRule.reduceExpressions(
            values, reducibleExps, predicates,
            false, true, false
        )
        var changeCount = 0
        val tuplesBuilder: ImmutableList.Builder<ImmutableList<RexLiteral>> = ImmutableList.builder()
        for (row in 0 until values.getTuples().size()) {
            var i = 0
            if (conditionExpr != null) {
                val reducedValue: RexNode = reducibleExps[row * fieldsPerRow + i]
                ++i
                if (!reducedValue.isAlwaysTrue()) {
                    ++changeCount
                    continue
                }
            }
            val valuesList: ImmutableList<RexLiteral>
            valuesList = if (projectExprs != null) {
                ++changeCount
                val tupleBuilder: ImmutableList.Builder<RexLiteral> = ImmutableList.builder()
                while (i < fieldsPerRow) {
                    val reducedValue: RexNode = reducibleExps[row * fieldsPerRow + i]
                    if (reducedValue is RexLiteral) {
                        tupleBuilder.add(reducedValue as RexLiteral)
                    } else if (RexUtil.isNullLiteral(reducedValue, true)) {
                        tupleBuilder.add(rexBuilder.makeNullLiteral(reducedValue.getType()))
                    } else {
                        return
                    }
                    ++i
                }
                tupleBuilder.build()
            } else {
                values.getTuples().get(row)
            }
            tuplesBuilder.add(valuesList)
        }
        if (changeCount > 0) {
            val rowType: RelDataType
            rowType = if (projectExprs != null) {
                requireNonNull(project, "project").getRowType()
            } else {
                values.getRowType()
            }
            val newRel: RelNode = LogicalValues.create(
                values.getCluster(), rowType,
                tuplesBuilder.build()
            )
            call.transformTo(newRel)
        } else {
            // Filter had no effect, so we can say that Filter(Values) ==
            // Values.
            call.transformTo(values)
        }

        // New plan is absolutely better than old plan. (Moreover, if
        // changeCount == 0, we've proved that the filter was trivial, and that
        // can send the volcano planner into a loop; see dtbug 2070.)
        if (filter != null) {
            call.getPlanner().prune(filter)
        }
    }
    //~ Inner Classes ----------------------------------------------------------
    /** Shuttle that converts inputs to literals.  */
    private class MyRexShuttle : RexShuttle() {
        @Nullable
        val literalList: List<RexLiteral>? = null
        @Override
        fun visitInputRef(inputRef: RexInputRef): RexNode {
            requireNonNull(literalList, "literalList")
            return literalList!![inputRef.getIndex()]
        }
    }

    /** Rule configuration.  */
    @Value.Immutable(singleton = false)
    interface Config : RelRule.Config {
        @Override
        fun toRule(): ValuesReduceRule? {
            return ValuesReduceRule(this)
        }

        /** Forwards a call to [.onMatch].  */
        @Value.Parameter
        fun matchHandler(): MatchHandler<ValuesReduceRule?>?

        /** Sets [.matchHandler].  */
        fun withMatchHandler(matchHandler: MatchHandler<ValuesReduceRule?>?): Config?

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(relClass: Class<out RelNode?>?): Config? {
            return withOperandSupplier { b -> b.operand(relClass).anyInputs() }
                .`as`(Config::class.java)
        }

        companion object {
            val FILTER: Config = ImmutableValuesReduceRule.Config.builder()
                .withDescription("ValuesReduceRule(Filter)")
                .withOperandSupplier { b0 ->
                    b0.operand(LogicalFilter::class.java).oneInput { b1 ->
                        b1.operand(
                            LogicalValues::class.java
                        )
                            .predicate(Values::isNotEmpty).noInputs()
                    }
                }
                .withMatchHandler { rule: ValuesReduceRule, call: RelOptRuleCall -> matchFilter(rule, call) }
                .build()
            val PROJECT: Config = ImmutableValuesReduceRule.Config.builder()
                .withDescription("ValuesReduceRule(Project)")
                .withOperandSupplier { b0 ->
                    b0.operand(LogicalProject::class.java).oneInput { b1 ->
                        b1.operand(
                            LogicalValues::class.java
                        )
                            .predicate(Values::isNotEmpty).noInputs()
                    }
                }
                .withMatchHandler { rule: ValuesReduceRule, call: RelOptRuleCall -> matchProject(rule, call) }
                .build()
            val PROJECT_FILTER: Config = ImmutableValuesReduceRule.Config.builder()
                .withDescription("ValuesReduceRule(Project-Filter)")
                .withOperandSupplier { b0 ->
                    b0.operand(LogicalProject::class.java).oneInput { b1 ->
                        b1.operand(
                            LogicalFilter::class.java
                        ).oneInput { b2 ->
                            b2.operand(LogicalValues::class.java)
                                .predicate(Values::isNotEmpty).noInputs()
                        }
                    }
                }
                .withMatchHandler { rule: ValuesReduceRule, call: RelOptRuleCall -> matchProjectFilter(rule, call) }
                .build()
        }
    }

    companion object {
        private val LOGGER: Logger = CalciteTrace.getPlannerTracer()
        private fun matchProjectFilter(
            rule: ValuesReduceRule,
            call: RelOptRuleCall
        ) {
            val project: LogicalProject = call.rel(0)
            val filter: LogicalFilter = call.rel(1)
            val values: LogicalValues = call.rel(2)
            rule.apply(call, project, filter, values)
        }

        private fun matchProject(rule: ValuesReduceRule, call: RelOptRuleCall) {
            val project: LogicalProject = call.rel(0)
            val values: LogicalValues = call.rel(1)
            rule.apply(call, project, null, values)
        }

        private fun matchFilter(rule: ValuesReduceRule, call: RelOptRuleCall) {
            val filter: LogicalFilter = call.rel(0)
            val values: LogicalValues = call.rel(1)
            rule.apply(call, null, filter, values)
        }
    }
}
