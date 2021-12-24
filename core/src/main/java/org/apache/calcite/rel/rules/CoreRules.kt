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

import org.apache.calcite.rel.RelNode

/** Rules that perform logical transformations on relational expressions.
 *
 * @see MaterializedViewRules
 */
object CoreRules {
    /** Rule that recognizes an [Aggregate]
     * on top of a [Project] and if possible
     * aggregates through the Project or removes the Project.  */
    val AGGREGATE_PROJECT_MERGE: AggregateProjectMergeRule = AggregateProjectMergeRule.Config.DEFAULT.toRule()

    /** Rule that removes constant keys from an [Aggregate].  */
    val AGGREGATE_PROJECT_PULL_UP_CONSTANTS: AggregateProjectPullUpConstantsRule =
        AggregateProjectPullUpConstantsRule.Config.DEFAULT.toRule()

    /** More general form of [.AGGREGATE_PROJECT_PULL_UP_CONSTANTS]
     * that matches any relational expression.  */
    val AGGREGATE_ANY_PULL_UP_CONSTANTS: AggregateProjectPullUpConstantsRule =
        AggregateProjectPullUpConstantsRule.Config.DEFAULT
            .withOperandFor(LogicalAggregate::class.java, RelNode::class.java)
            .toRule()

    /** Rule that matches an [Aggregate] on
     * a [StarTable.StarTableScan].  */
    val AGGREGATE_STAR_TABLE: AggregateStarTableRule = AggregateStarTableRule.Config.DEFAULT.toRule()

    /** Variant of [.AGGREGATE_STAR_TABLE] that accepts a [Project]
     * between the [Aggregate] and its [StarTable.StarTableScan]
     * input.  */
    val AGGREGATE_PROJECT_STAR_TABLE: AggregateProjectStarTableRule =
        AggregateProjectStarTableRule.Config.DEFAULT.toRule()

    /** Rule that reduces aggregate functions in
     * an [Aggregate] to simpler forms.  */
    val AGGREGATE_REDUCE_FUNCTIONS: AggregateReduceFunctionsRule = AggregateReduceFunctionsRule.Config.DEFAULT.toRule()

    /** Rule that matches an [Aggregate] on an [Aggregate],
     * and merges into a single Aggregate if the top aggregate's group key is a
     * subset of the lower aggregate's group key, and the aggregates are
     * expansions of rollups.  */
    val AGGREGATE_MERGE: AggregateMergeRule = AggregateMergeRule.Config.DEFAULT.toRule()

    /** Rule that removes an [Aggregate]
     * if it computes no aggregate functions
     * (that is, it is implementing `SELECT DISTINCT`),
     * or all the aggregate functions are splittable,
     * and the underlying relational expression is already distinct.  */
    val AGGREGATE_REMOVE: AggregateRemoveRule = AggregateRemoveRule.Config.DEFAULT.toRule()

    /** Rule that expands distinct aggregates
     * (such as `COUNT(DISTINCT x)`) from a
     * [Aggregate].
     * This instance operates only on logical expressions.  */
    val AGGREGATE_EXPAND_DISTINCT_AGGREGATES: AggregateExpandDistinctAggregatesRule =
        AggregateExpandDistinctAggregatesRule.Config.DEFAULT.toRule()

    /** As [.AGGREGATE_EXPAND_DISTINCT_AGGREGATES] but generates a Join.  */
    val AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN: AggregateExpandDistinctAggregatesRule =
        AggregateExpandDistinctAggregatesRule.Config.JOIN.toRule()

    /** Rule that rewrites a [LogicalAggregate] that contains
     * `WITHIN DISTINCT` aggregate functions to one that does not.  */
    val AGGREGATE_EXPAND_WITHIN_DISTINCT: AggregateExpandWithinDistinctRule =
        AggregateExpandWithinDistinctRule.Config.DEFAULT.toRule()

    /** Rule that matches an [Aggregate]
     * on a [Filter] and transposes them,
     * pushing the aggregate below the filter.  */
    val AGGREGATE_FILTER_TRANSPOSE: AggregateFilterTransposeRule = AggregateFilterTransposeRule.Config.DEFAULT.toRule()

    /** Rule that matches an [Aggregate]
     * on a [Join] and removes the left input
     * of the join provided that the left input is also a left join if
     * possible.  */
    val AGGREGATE_JOIN_JOIN_REMOVE: AggregateJoinJoinRemoveRule = AggregateJoinJoinRemoveRule.Config.DEFAULT.toRule()

    /** Rule that matches an [Aggregate]
     * on a [Join] and removes the join
     * provided that the join is a left join or right join and it computes no
     * aggregate functions or all the aggregate calls have distinct.  */
    val AGGREGATE_JOIN_REMOVE: AggregateJoinRemoveRule = AggregateJoinRemoveRule.Config.DEFAULT.toRule()

    /** Rule that pushes an [Aggregate]
     * past a [Join].  */
    val AGGREGATE_JOIN_TRANSPOSE: AggregateJoinTransposeRule = AggregateJoinTransposeRule.Config.DEFAULT.toRule()

    /** As [.AGGREGATE_JOIN_TRANSPOSE], but extended to push down aggregate
     * functions.  */
    val AGGREGATE_JOIN_TRANSPOSE_EXTENDED: AggregateJoinTransposeRule =
        AggregateJoinTransposeRule.Config.EXTENDED.toRule()

    /** Rule that pushes an [Aggregate]
     * past a non-distinct [Union].  */
    val AGGREGATE_UNION_TRANSPOSE: AggregateUnionTransposeRule = AggregateUnionTransposeRule.Config.DEFAULT.toRule()

    /** Rule that matches an [Aggregate] whose input is a [Union]
     * one of whose inputs is an `Aggregate`.
     *
     *
     * Because it matches [RelNode] for each input of `Union`, it
     * will create O(N ^ 2) matches, which may cost too much during the popMatch
     * phase in VolcanoPlanner. If efficiency is a concern, we recommend that you
     * use [.AGGREGATE_UNION_AGGREGATE_FIRST]
     * and [.AGGREGATE_UNION_AGGREGATE_SECOND] instead.  */
    val AGGREGATE_UNION_AGGREGATE: AggregateUnionAggregateRule = AggregateUnionAggregateRule.Config.DEFAULT.toRule()

    /** As [.AGGREGATE_UNION_AGGREGATE], but matches an `Aggregate`
     * only as the left input of the `Union`.  */
    val AGGREGATE_UNION_AGGREGATE_FIRST: AggregateUnionAggregateRule =
        AggregateUnionAggregateRule.Config.AGG_FIRST.toRule()

    /** As [.AGGREGATE_UNION_AGGREGATE], but matches an `Aggregate`
     * only as the right input of the `Union`.  */
    val AGGREGATE_UNION_AGGREGATE_SECOND: AggregateUnionAggregateRule =
        AggregateUnionAggregateRule.Config.AGG_SECOND.toRule()

    /** Rule that converts CASE-style filtered aggregates into true filtered
     * aggregates.  */
    val AGGREGATE_CASE_TO_FILTER: AggregateCaseToFilterRule = AggregateCaseToFilterRule.Config.DEFAULT.toRule()

    /** Rule that merges a [Calc] onto a `Calc`.  */
    val CALC_MERGE: CalcMergeRule = CalcMergeRule.Config.DEFAULT.toRule()

    /** Rule that removes a trivial [LogicalCalc].  */
    val CALC_REMOVE: CalcRemoveRule = CalcRemoveRule.Config.DEFAULT.toRule()

    /** Rule that reduces operations on the DECIMAL type, such as casts or
     * arithmetic, into operations involving more primitive types such as BIGINT
     * and DOUBLE.  */
    val CALC_REDUCE_DECIMALS: ReduceDecimalsRule = ReduceDecimalsRule.Config.DEFAULT.toRule()

    /** Rule that reduces constants inside a [LogicalCalc].
     *
     * @see .FILTER_REDUCE_EXPRESSIONS
     */
    val CALC_REDUCE_EXPRESSIONS: ReduceExpressionsRule.CalcReduceExpressionsRule =
        ReduceExpressionsRule.CalcReduceExpressionsRule.CalcReduceExpressionsRuleConfig.DEFAULT
            .toRule()

    /** Rule that converts a [Calc] to a [Project] and
     * [Filter].  */
    val CALC_SPLIT: CalcSplitRule = CalcSplitRule.Config.DEFAULT.toRule()

    /** Rule that transforms a [Calc]
     * that contains windowed aggregates to a mixture of
     * [LogicalWindow] and `Calc`.  */
    val CALC_TO_WINDOW: ProjectToWindowRule.CalcToWindowRule =
        ProjectToWindowRule.CalcToWindowRule.CalcToWindowRuleConfig.DEFAULT.toRule()

    /** Rule that pre-casts inputs to a particular type. This can assist operator
     * implementations that impose requirements on their input types.  */
    val COERCE_INPUTS: CoerceInputsRule = CoerceInputsRule.Config.DEFAULT.toRule()

    /** Rule that removes constants inside a [LogicalExchange].  */
    val EXCHANGE_REMOVE_CONSTANT_KEYS: ExchangeRemoveConstantKeysRule =
        ExchangeRemoveConstantKeysRule.Config.DEFAULT.toRule()

    /** Rule that removes constants inside a [LogicalSortExchange].  */
    val SORT_EXCHANGE_REMOVE_CONSTANT_KEYS: ExchangeRemoveConstantKeysRule =
        ExchangeRemoveConstantKeysRule.Config.SORT.toRule()

    /** Rule that tries to push filter expressions into a join
     * condition and into the inputs of the join.  */
    val FILTER_INTO_JOIN: FilterJoinRule.FilterIntoJoinRule =
        FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig.DEFAULT.toRule()

    /** Dumber version of [.FILTER_INTO_JOIN]. Not intended for production
     * use, but keeps some tests working for which `FILTER_INTO_JOIN` is too
     * smart.  */
    val FILTER_INTO_JOIN_DUMB: FilterJoinRule.FilterIntoJoinRule = FILTER_INTO_JOIN.config
        .withSmart(false)
        .`as`(FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig::class.java)
        .toRule()

    /** Rule that combines two [LogicalFilter]s.  */
    val FILTER_MERGE: FilterMergeRule = FilterMergeRule.Config.DEFAULT.toRule()

    /** Rule that merges a [Filter] and a [LogicalCalc]. The
     * result is a [LogicalCalc] whose filter condition is the logical AND
     * of the two.
     *
     * @see .PROJECT_CALC_MERGE
     */
    val FILTER_CALC_MERGE: FilterCalcMergeRule = FilterCalcMergeRule.Config.DEFAULT.toRule()

    /** Rule that converts a [LogicalFilter] to a [LogicalCalc].
     *
     * @see .PROJECT_TO_CALC
     */
    val FILTER_TO_CALC: FilterToCalcRule = FilterToCalcRule.Config.DEFAULT.toRule()

    /** Rule that pushes a [Filter] past an [Aggregate].
     *
     * @see .AGGREGATE_FILTER_TRANSPOSE
     */
    val FILTER_AGGREGATE_TRANSPOSE: FilterAggregateTransposeRule = FilterAggregateTransposeRule.Config.DEFAULT.toRule()

    /** The default instance of
     * [org.apache.calcite.rel.rules.FilterProjectTransposeRule].
     *
     *
     * It does not allow a Filter to be pushed past the Project if
     * [there is a correlation condition][RexUtil.containsCorrelation])
     * anywhere in the Filter, since in some cases it can prevent a
     * [Correlate] from being de-correlated.
     */
    val FILTER_PROJECT_TRANSPOSE: FilterProjectTransposeRule = FilterProjectTransposeRule.Config.DEFAULT.toRule()

    /** Rule that pushes a [LogicalFilter]
     * past a [LogicalTableFunctionScan].  */
    val FILTER_TABLE_FUNCTION_TRANSPOSE: FilterTableFunctionTransposeRule =
        FilterTableFunctionTransposeRule.Config.DEFAULT.toRule()

    /** Rule that matches a [Filter] on a [TableScan].  */
    val FILTER_SCAN: FilterTableScanRule = FilterTableScanRule.Config.DEFAULT.toRule()

    /** Rule that matches a [Filter] on an
     * [org.apache.calcite.adapter.enumerable.EnumerableInterpreter] on a
     * [TableScan].  */
    val FILTER_INTERPRETER_SCAN: FilterTableScanRule = FilterTableScanRule.Config.INTERPRETER.toRule()

    /** Rule that pushes a [Filter] above a [Correlate] into the
     * inputs of the `Correlate`.  */
    val FILTER_CORRELATE: FilterCorrelateRule = FilterCorrelateRule.Config.DEFAULT.toRule()

    /** Rule that merges a [Filter] into a [MultiJoin],
     * creating a richer `MultiJoin`.
     *
     * @see .PROJECT_MULTI_JOIN_MERGE
     */
    val FILTER_MULTI_JOIN_MERGE: FilterMultiJoinMergeRule = FilterMultiJoinMergeRule.Config.DEFAULT.toRule()

    /** Rule that replaces `IS NOT DISTINCT FROM`
     * in a [Filter] with logically equivalent operations.  */
    val FILTER_EXPAND_IS_NOT_DISTINCT_FROM: FilterRemoveIsNotDistinctFromRule =
        FilterRemoveIsNotDistinctFromRule.Config.DEFAULT.toRule()

    /** Rule that pushes a [Filter] past a [SetOp].  */
    val FILTER_SET_OP_TRANSPOSE: FilterSetOpTransposeRule = FilterSetOpTransposeRule.Config.DEFAULT.toRule()

    /** Rule that reduces constants inside a [LogicalFilter].
     *
     * @see .JOIN_REDUCE_EXPRESSIONS
     *
     * @see .PROJECT_REDUCE_EXPRESSIONS
     *
     * @see .CALC_REDUCE_EXPRESSIONS
     *
     * @see .WINDOW_REDUCE_EXPRESSIONS
     */
    val FILTER_REDUCE_EXPRESSIONS: ReduceExpressionsRule.FilterReduceExpressionsRule =
        ReduceExpressionsRule.FilterReduceExpressionsRule.FilterReduceExpressionsRuleConfig.DEFAULT
            .toRule()

    /** Rule that flattens an [Intersect] on an `Intersect`
     * into a single `Intersect`.  */
    val INTERSECT_MERGE: UnionMergeRule = UnionMergeRule.Config.INTERSECT.toRule()

    /** Rule that translates a distinct
     * [Intersect] into a group of operators
     * composed of [Union], [Aggregate], etc.  */
    val INTERSECT_TO_DISTINCT: IntersectToDistinctRule = IntersectToDistinctRule.Config.DEFAULT.toRule()

    /** Rule that converts a [LogicalMatch] to the result of calling
     * [LogicalMatch.copy].  */
    val MATCH: MatchRule = MatchRule.Config.DEFAULT.toRule()

    /** Rule that flattens a [Minus] on a `Minus`
     * into a single `Minus`.  */
    val MINUS_MERGE: UnionMergeRule = UnionMergeRule.Config.MINUS.toRule()

    /** Rule that matches a [Project] on an [Aggregate],
     * projecting away aggregate calls that are not used.  */
    val PROJECT_AGGREGATE_MERGE: ProjectAggregateMergeRule = ProjectAggregateMergeRule.Config.DEFAULT.toRule()

    /** Rule that merges a [LogicalProject] and a [LogicalCalc].
     *
     * @see .FILTER_CALC_MERGE
     */
    val PROJECT_CALC_MERGE: ProjectCalcMergeRule = ProjectCalcMergeRule.Config.DEFAULT.toRule()

    /** Rule that matches a [Project] on a [Correlate] and
     * pushes the projections to the Correlate's left and right inputs.  */
    val PROJECT_CORRELATE_TRANSPOSE: ProjectCorrelateTransposeRule =
        ProjectCorrelateTransposeRule.Config.DEFAULT.toRule()

    /** Rule that pushes a [Project] past a [Filter].
     *
     * @see .PROJECT_FILTER_TRANSPOSE_WHOLE_PROJECT_EXPRESSIONS
     *
     * @see .PROJECT_FILTER_TRANSPOSE_WHOLE_EXPRESSIONS
     */
    val PROJECT_FILTER_TRANSPOSE: ProjectFilterTransposeRule = ProjectFilterTransposeRule.Config.DEFAULT.toRule()

    /** As [.PROJECT_FILTER_TRANSPOSE], but pushes down project and filter
     * expressions whole, not field references.  */
    val PROJECT_FILTER_TRANSPOSE_WHOLE_EXPRESSIONS: ProjectFilterTransposeRule =
        ProjectFilterTransposeRule.Config.PROJECT_FILTER.toRule()

    /** As [.PROJECT_FILTER_TRANSPOSE],
     * pushes down field references for filters,
     * but pushes down project expressions whole.  */
    val PROJECT_FILTER_TRANSPOSE_WHOLE_PROJECT_EXPRESSIONS: ProjectFilterTransposeRule =
        ProjectFilterTransposeRule.Config.PROJECT.toRule()

    /** Rule that reduces constants inside a [LogicalProject].
     *
     * @see .FILTER_REDUCE_EXPRESSIONS
     */
    val PROJECT_REDUCE_EXPRESSIONS: ReduceExpressionsRule.ProjectReduceExpressionsRule =
        ReduceExpressionsRule.ProjectReduceExpressionsRule.ProjectReduceExpressionsRuleConfig.DEFAULT.toRule()

    /** Rule that converts sub-queries from project expressions into
     * [Correlate] instances.
     *
     * @see .FILTER_SUB_QUERY_TO_CORRELATE
     *
     * @see .JOIN_SUB_QUERY_TO_CORRELATE
     */
    val PROJECT_SUB_QUERY_TO_CORRELATE: SubQueryRemoveRule = SubQueryRemoveRule.Config.PROJECT.toRule()

    /** Rule that converts a sub-queries from filter expressions into
     * [Correlate] instances.
     *
     * @see .PROJECT_SUB_QUERY_TO_CORRELATE
     *
     * @see .JOIN_SUB_QUERY_TO_CORRELATE
     */
    val FILTER_SUB_QUERY_TO_CORRELATE: SubQueryRemoveRule = SubQueryRemoveRule.Config.FILTER.toRule()

    /** Rule that converts sub-queries from join expressions into
     * [Correlate] instances.
     *
     * @see .PROJECT_SUB_QUERY_TO_CORRELATE
     *
     * @see .FILTER_SUB_QUERY_TO_CORRELATE
     */
    val JOIN_SUB_QUERY_TO_CORRELATE: SubQueryRemoveRule = SubQueryRemoveRule.Config.JOIN.toRule()

    /** Rule that transforms a [Project]
     * into a mixture of `LogicalProject`
     * and [LogicalWindow].  */
    val PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW: ProjectToWindowRule.ProjectToLogicalProjectAndWindowRule =
        ProjectToWindowRule.ProjectToLogicalProjectAndWindowRule.ProjectToLogicalProjectAndWindowRuleConfig.DEFAULT.toRule()

    /** Rule that creates a [semi-join][Join.isSemiJoin] from a
     * [Project] on top of a [Join] with an [Aggregate] as its
     * right input.
     *
     * @see .JOIN_TO_SEMI_JOIN
     */
    val PROJECT_TO_SEMI_JOIN: SemiJoinRule.ProjectToSemiJoinRule =
        SemiJoinRule.ProjectToSemiJoinRule.ProjectToSemiJoinRuleConfig.DEFAULT.toRule()

    /** Rule that matches an [Project] on a [Join] and removes the
     * left input of the join provided that the left input is also a left join.  */
    val PROJECT_JOIN_JOIN_REMOVE: ProjectJoinJoinRemoveRule = ProjectJoinJoinRemoveRule.Config.DEFAULT.toRule()

    /** Rule that matches an [Project] on a [Join] and removes the
     * join provided that the join is a left join or right join and the join keys
     * are unique.  */
    val PROJECT_JOIN_REMOVE: ProjectJoinRemoveRule = ProjectJoinRemoveRule.Config.DEFAULT.toRule()

    /** Rule that pushes a [LogicalProject] past a [LogicalJoin]
     * by splitting the projection into a projection on top of each child of
     * the join.  */
    val PROJECT_JOIN_TRANSPOSE: ProjectJoinTransposeRule = ProjectJoinTransposeRule.Config.DEFAULT.toRule()

    /** Rule that merges a [Project] into another [Project],
     * provided the projects are not projecting identical sets of input
     * references.  */
    val PROJECT_MERGE: ProjectMergeRule = ProjectMergeRule.Config.DEFAULT.toRule()

    /** Rule that pushes a [LogicalProject] past a [SetOp].
     *
     *
     * The children of the `SetOp` will project
     * only the [RexInputRef]s referenced in the original
     * `LogicalProject`.  */
    val PROJECT_SET_OP_TRANSPOSE: ProjectSetOpTransposeRule = ProjectSetOpTransposeRule.Config.DEFAULT.toRule()

    /** Rule that pushes a [Project] into a [MultiJoin],
     * creating a richer `MultiJoin`.
     *
     * @see .FILTER_MULTI_JOIN_MERGE
     */
    val PROJECT_MULTI_JOIN_MERGE: ProjectMultiJoinMergeRule = ProjectMultiJoinMergeRule.Config.DEFAULT.toRule()

    /** Rule that, given a [Project] node that merely returns its input,
     * converts the node into its input.  */
    val PROJECT_REMOVE: ProjectRemoveRule = ProjectRemoveRule.Config.DEFAULT.toRule()

    /** Rule that converts a [Project] on a [TableScan]
     * of a [org.apache.calcite.schema.ProjectableFilterableTable]
     * to a [org.apache.calcite.interpreter.Bindables.BindableTableScan].
     *
     * @see .PROJECT_INTERPRETER_TABLE_SCAN
     */
    val PROJECT_TABLE_SCAN: ProjectTableScanRule = ProjectTableScanRule.Config.DEFAULT.toRule()

    /** As [.PROJECT_TABLE_SCAN], but with an intervening
     * [org.apache.calcite.adapter.enumerable.EnumerableInterpreter].  */
    val PROJECT_INTERPRETER_TABLE_SCAN: ProjectTableScanRule = ProjectTableScanRule.Config.INTERPRETER.toRule()

    /** Rule that converts a [LogicalProject] to a [LogicalCalc].
     *
     * @see .FILTER_TO_CALC
     */
    val PROJECT_TO_CALC: ProjectToCalcRule = ProjectToCalcRule.Config.DEFAULT.toRule()

    /** Rule that pushes a [LogicalProject] past a [LogicalWindow].  */
    val PROJECT_WINDOW_TRANSPOSE: ProjectWindowTransposeRule = ProjectWindowTransposeRule.Config.DEFAULT.toRule()

    /** Rule that pushes predicates in a Join into the inputs to the join.  */
    val JOIN_CONDITION_PUSH: FilterJoinRule.JoinConditionPushRule =
        FilterJoinRule.JoinConditionPushRule.JoinConditionPushRuleConfig.DEFAULT.toRule()

    /** Rule to add a semi-join into a [Join].  */
    val JOIN_ADD_REDUNDANT_SEMI_JOIN: JoinAddRedundantSemiJoinRule =
        JoinAddRedundantSemiJoinRule.Config.DEFAULT.toRule()

    /** Rule that changes a join based on the associativity rule,
     * ((a JOIN b) JOIN c)  (a JOIN (b JOIN c)).  */
    val JOIN_ASSOCIATE: JoinAssociateRule = JoinAssociateRule.Config.DEFAULT.toRule()

    /** Rule that permutes the inputs to an inner [Join].  */
    val JOIN_COMMUTE: JoinCommuteRule = JoinCommuteRule.Config.DEFAULT.toRule()

    /** As [.JOIN_COMMUTE] but swaps outer joins as well as inner joins.  */
    val JOIN_COMMUTE_OUTER: JoinCommuteRule = JoinCommuteRule.Config.DEFAULT.withSwapOuter(true).toRule()

    /** Rule to convert an
     * [inner join][LogicalJoin] to a
     * [filter][LogicalFilter] on top of a
     * [cartesian inner join][LogicalJoin].  */
    val JOIN_EXTRACT_FILTER: JoinExtractFilterRule = JoinExtractFilterRule.Config.DEFAULT.toRule()

    /** Rule that matches a [LogicalJoin] whose inputs are
     * [LogicalProject]s, and pulls the project expressions up.  */
    val JOIN_PROJECT_BOTH_TRANSPOSE: JoinProjectTransposeRule = JoinProjectTransposeRule.Config.DEFAULT.toRule()

    /** As [.JOIN_PROJECT_BOTH_TRANSPOSE] but only the left input is
     * a [LogicalProject].  */
    val JOIN_PROJECT_LEFT_TRANSPOSE: JoinProjectTransposeRule = JoinProjectTransposeRule.Config.LEFT.toRule()

    /** As [.JOIN_PROJECT_BOTH_TRANSPOSE] but only the right input is
     * a [LogicalProject].  */
    val JOIN_PROJECT_RIGHT_TRANSPOSE: JoinProjectTransposeRule = JoinProjectTransposeRule.Config.RIGHT.toRule()

    /** As [.JOIN_PROJECT_BOTH_TRANSPOSE] but match outer as well as
     * inner join.  */
    val JOIN_PROJECT_BOTH_TRANSPOSE_INCLUDE_OUTER: JoinProjectTransposeRule =
        JoinProjectTransposeRule.Config.OUTER.toRule()

    /** As [.JOIN_PROJECT_LEFT_TRANSPOSE] but match outer as well as
     * inner join.  */
    val JOIN_PROJECT_LEFT_TRANSPOSE_INCLUDE_OUTER: JoinProjectTransposeRule =
        JoinProjectTransposeRule.Config.LEFT_OUTER.toRule()

    /** As [.JOIN_PROJECT_RIGHT_TRANSPOSE] but match outer as well as
     * inner join.  */
    val JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER: JoinProjectTransposeRule =
        JoinProjectTransposeRule.Config.RIGHT_OUTER.toRule()

    /** Rule that matches a [Join] and pushes down expressions on either
     * side of "equal" conditions.  */
    val JOIN_PUSH_EXPRESSIONS: JoinPushExpressionsRule = JoinPushExpressionsRule.Config.DEFAULT.toRule()

    /** Rule that infers predicates from on a [Join] and creates
     * [Filter]s if those predicates can be pushed to its inputs.  */
    val JOIN_PUSH_TRANSITIVE_PREDICATES: JoinPushTransitivePredicatesRule =
        JoinPushTransitivePredicatesRule.Config.DEFAULT.toRule()

    /** Rule that reduces constants inside a [Join].
     *
     * @see .FILTER_REDUCE_EXPRESSIONS
     *
     * @see .PROJECT_REDUCE_EXPRESSIONS
     */
    val JOIN_REDUCE_EXPRESSIONS: ReduceExpressionsRule.JoinReduceExpressionsRule =
        ReduceExpressionsRule.JoinReduceExpressionsRule.JoinReduceExpressionsRuleConfig.DEFAULT.toRule()

    /** Rule that converts a [LogicalJoin]
     * into a [LogicalCorrelate].  */
    val JOIN_TO_CORRELATE: JoinToCorrelateRule = JoinToCorrelateRule.Config.DEFAULT.toRule()

    /** Rule that flattens a tree of [LogicalJoin]s
     * into a single [MultiJoin] with N inputs.  */
    val JOIN_TO_MULTI_JOIN: JoinToMultiJoinRule = JoinToMultiJoinRule.Config.DEFAULT.toRule()

    /** Rule that creates a [semi-join][Join.isSemiJoin] from a
     * [Join] with an empty [Aggregate] as its right input.
     *
     * @see .PROJECT_TO_SEMI_JOIN
     */
    val JOIN_TO_SEMI_JOIN: SemiJoinRule.JoinToSemiJoinRule =
        SemiJoinRule.JoinToSemiJoinRule.JoinToSemiJoinRuleConfig.DEFAULT.toRule()

    /** Rule that pushes a [Join]
     * past a non-distinct [Union] as its left input.  */
    val JOIN_LEFT_UNION_TRANSPOSE: JoinUnionTransposeRule = JoinUnionTransposeRule.Config.LEFT.toRule()

    /** Rule that pushes a [Join]
     * past a non-distinct [Union] as its right input.  */
    val JOIN_RIGHT_UNION_TRANSPOSE: JoinUnionTransposeRule = JoinUnionTransposeRule.Config.RIGHT.toRule()

    /** Rule that re-orders a [Join] using a heuristic planner.
     *
     *
     * It is triggered by the pattern
     * [LogicalProject] ([MultiJoin]).
     *
     * @see .JOIN_TO_MULTI_JOIN
     *
     * @see .MULTI_JOIN_OPTIMIZE_BUSHY
     */
    val MULTI_JOIN_OPTIMIZE: LoptOptimizeJoinRule = LoptOptimizeJoinRule.Config.DEFAULT.toRule()

    /** Rule that finds an approximately optimal ordering for join operators
     * using a heuristic algorithm and can handle bushy joins.
     *
     *
     * It is triggered by the pattern
     * [LogicalProject] ([MultiJoin]).
     *
     * @see .MULTI_JOIN_OPTIMIZE
     */
    val MULTI_JOIN_OPTIMIZE_BUSHY: MultiJoinOptimizeBushyRule = MultiJoinOptimizeBushyRule.Config.DEFAULT.toRule()

    /** Rule that matches a [LogicalJoin] whose inputs are both a
     * [MultiJoin] with intervening [LogicalProject]s,
     * and pulls the Projects up above the Join.  */
    val MULTI_JOIN_BOTH_PROJECT: MultiJoinProjectTransposeRule =
        MultiJoinProjectTransposeRule.Config.BOTH_PROJECT.toRule()

    /** As [.MULTI_JOIN_BOTH_PROJECT] but only the left input has
     * an intervening Project.  */
    val MULTI_JOIN_LEFT_PROJECT: MultiJoinProjectTransposeRule =
        MultiJoinProjectTransposeRule.Config.LEFT_PROJECT.toRule()

    /** As [.MULTI_JOIN_BOTH_PROJECT] but only the right input has
     * an intervening Project.  */
    val MULTI_JOIN_RIGHT_PROJECT: MultiJoinProjectTransposeRule =
        MultiJoinProjectTransposeRule.Config.RIGHT_PROJECT.toRule()

    /** Rule that pushes a [semi-join][Join.isSemiJoin] down in a tree past
     * a [Filter].
     *
     * @see .SEMI_JOIN_PROJECT_TRANSPOSE
     *
     * @see .SEMI_JOIN_JOIN_TRANSPOSE
     */
    val SEMI_JOIN_FILTER_TRANSPOSE: SemiJoinFilterTransposeRule = SemiJoinFilterTransposeRule.Config.DEFAULT.toRule()

    /** Rule that pushes a [semi-join][Join.isSemiJoin] down in a tree past
     * a [Project].
     *
     * @see .SEMI_JOIN_FILTER_TRANSPOSE
     *
     * @see .SEMI_JOIN_JOIN_TRANSPOSE
     */
    val SEMI_JOIN_PROJECT_TRANSPOSE: SemiJoinProjectTransposeRule = SemiJoinProjectTransposeRule.Config.DEFAULT.toRule()

    /** Rule that pushes a [semi-join][Join.isSemiJoin] down in a tree past a
     * [Join].
     *
     * @see .SEMI_JOIN_FILTER_TRANSPOSE
     *
     * @see .SEMI_JOIN_PROJECT_TRANSPOSE
     */
    val SEMI_JOIN_JOIN_TRANSPOSE: SemiJoinJoinTransposeRule = SemiJoinJoinTransposeRule.Config.DEFAULT.toRule()

    /** Rule that removes a [semi-join][Join.isSemiJoin] from a join tree.  */
    val SEMI_JOIN_REMOVE: SemiJoinRemoveRule = SemiJoinRemoveRule.Config.DEFAULT.toRule()

    /** Rule that pushes a [Sort] past a [Union].
     *
     *
     * This rule instance is for a Union implementation that does not preserve
     * the ordering of its inputs. Thus, it makes no sense to match this rule
     * if the Sort does not have a limit, i.e., [Sort.fetch] is null.
     *
     * @see .SORT_UNION_TRANSPOSE_MATCH_NULL_FETCH
     */
    val SORT_UNION_TRANSPOSE: SortUnionTransposeRule = SortUnionTransposeRule.Config.DEFAULT.toRule()

    /** As [.SORT_UNION_TRANSPOSE], but for a Union implementation that
     * preserves the ordering of its inputs. It is still worth applying this rule
     * even if the Sort does not have a limit, for the merge of already sorted
     * inputs that the Union can do is usually cheap.  */
    val SORT_UNION_TRANSPOSE_MATCH_NULL_FETCH: SortUnionTransposeRule =
        SortUnionTransposeRule.Config.DEFAULT.withMatchNullFetch(true).toRule()

    /** Rule that copies a [Sort] past a [Join] without its limit and
     * offset. The original [Sort] is preserved but can potentially be
     * removed by [.SORT_REMOVE] if redundant.  */
    val SORT_JOIN_COPY: SortJoinCopyRule = SortJoinCopyRule.Config.DEFAULT.toRule()

    /** Rule that removes a [Sort] if its input is already sorted.  */
    val SORT_REMOVE: SortRemoveRule = SortRemoveRule.Config.DEFAULT.toRule()

    /** Rule that removes keys from a [Sort]
     * if those keys are known to be constant, or removes the entire Sort if all
     * keys are constant.  */
    val SORT_REMOVE_CONSTANT_KEYS: SortRemoveConstantKeysRule = SortRemoveConstantKeysRule.Config.DEFAULT.toRule()

    /** Rule that pushes a [Sort] past a [Join].  */
    val SORT_JOIN_TRANSPOSE: SortJoinTransposeRule = SortJoinTransposeRule.Config.DEFAULT.toRule()

    /** Rule that pushes a [Sort] past a [Project].  */
    val SORT_PROJECT_TRANSPOSE: SortProjectTransposeRule = SortProjectTransposeRule.Config.DEFAULT.toRule()

    /** Rule that flattens a [Union] on a `Union`
     * into a single `Union`.  */
    val UNION_MERGE: UnionMergeRule = UnionMergeRule.Config.DEFAULT.toRule()

    /** Rule that removes a [Union] if it has only one input.
     *
     * @see PruneEmptyRules.UNION_INSTANCE
     */
    val UNION_REMOVE: UnionEliminatorRule = UnionEliminatorRule.Config.DEFAULT.toRule()

    /** Rule that pulls up constants through a Union operator.  */
    val UNION_PULL_UP_CONSTANTS: UnionPullUpConstantsRule = UnionPullUpConstantsRule.Config.DEFAULT.toRule()

    /** Rule that translates a distinct [Union]
     * (`all` = `false`)
     * into an [Aggregate] on top of a non-distinct [Union]
     * (`all` = `true`).  */
    val UNION_TO_DISTINCT: UnionToDistinctRule = UnionToDistinctRule.Config.DEFAULT.toRule()

    /** Rule that applies an [Aggregate] to a [Values] (currently just
     * an empty `Values`).  */
    val AGGREGATE_VALUES: AggregateValuesRule = AggregateValuesRule.Config.DEFAULT.toRule()

    /** Rule that merges a [Filter] onto an underlying
     * [org.apache.calcite.rel.logical.LogicalValues],
     * resulting in a `Values` with potentially fewer rows.  */
    val FILTER_VALUES_MERGE: ValuesReduceRule = ValuesReduceRule.Config.FILTER.toRule()

    /** Rule that merges a [Project] onto an underlying
     * [org.apache.calcite.rel.logical.LogicalValues],
     * resulting in a `Values` with different columns.  */
    val PROJECT_VALUES_MERGE: ValuesReduceRule = ValuesReduceRule.Config.PROJECT.toRule()

    /** Rule that merges a [Project]
     * on top of a [Filter] onto an underlying
     * [org.apache.calcite.rel.logical.LogicalValues],
     * resulting in a `Values` with different columns
     * and potentially fewer rows.  */
    val PROJECT_FILTER_VALUES_MERGE: ValuesReduceRule = ValuesReduceRule.Config.PROJECT_FILTER.toRule()

    /** Rule that reduces constants inside a [LogicalWindow].
     *
     * @see .FILTER_REDUCE_EXPRESSIONS
     */
    val WINDOW_REDUCE_EXPRESSIONS: ReduceExpressionsRule.WindowReduceExpressionsRule =
        ReduceExpressionsRule.WindowReduceExpressionsRule.WindowReduceExpressionsRuleConfig.DEFAULT.toRule()
}
