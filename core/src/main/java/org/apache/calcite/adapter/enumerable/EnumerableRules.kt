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
package org.apache.calcite.adapter.enumerable

import org.apache.calcite.linq4j.function.Experimental

/**
 * Rules and relational operators for the
 * [enumerable calling convention][EnumerableConvention].
 */
object EnumerableRules {
    internal val LOGGER: Logger = CalciteTrace.getPlannerTracer()
    const val BRIDGE_METHODS = true

    /** Rule that converts a
     * [org.apache.calcite.rel.logical.LogicalJoin] to
     * [enumerable calling convention][EnumerableConvention].  */
    val ENUMERABLE_JOIN_RULE: RelOptRule = EnumerableJoinRule.DEFAULT_CONFIG.toRule(EnumerableJoinRule::class.java)

    /** Rule that converts a
     * [org.apache.calcite.rel.logical.LogicalJoin] to
     * [enumerable calling convention][EnumerableConvention].  */
    val ENUMERABLE_MERGE_JOIN_RULE: RelOptRule = EnumerableMergeJoinRule.DEFAULT_CONFIG
        .toRule(EnumerableMergeJoinRule::class.java)
    val ENUMERABLE_CORRELATE_RULE: RelOptRule = EnumerableCorrelateRule.DEFAULT_CONFIG
        .toRule(EnumerableCorrelateRule::class.java)

    /** Rule that converts a
     * [org.apache.calcite.rel.logical.LogicalJoin] into an
     * [org.apache.calcite.adapter.enumerable.EnumerableBatchNestedLoopJoin].  */
    val ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE: RelOptRule = EnumerableBatchNestedLoopJoinRule.Config.DEFAULT.toRule()

    /** Rule that converts a
     * [org.apache.calcite.rel.logical.LogicalProject] to an
     * [EnumerableProject].  */
    val ENUMERABLE_PROJECT_RULE: EnumerableProjectRule = EnumerableProjectRule.DEFAULT_CONFIG.toRule(
        EnumerableProjectRule::class.java
    )
    val ENUMERABLE_FILTER_RULE: EnumerableFilterRule = EnumerableFilterRule.DEFAULT_CONFIG.toRule(
        EnumerableFilterRule::class.java
    )
    val ENUMERABLE_CALC_RULE: EnumerableCalcRule =
        EnumerableCalcRule.DEFAULT_CONFIG.toRule(EnumerableCalcRule::class.java)
    val ENUMERABLE_AGGREGATE_RULE: EnumerableAggregateRule = EnumerableAggregateRule.DEFAULT_CONFIG
        .toRule(EnumerableAggregateRule::class.java)

    /** Rule that converts a [org.apache.calcite.rel.core.Sort] to an
     * [EnumerableSort].  */
    val ENUMERABLE_SORT_RULE: EnumerableSortRule =
        EnumerableSortRule.DEFAULT_CONFIG.toRule(EnumerableSortRule::class.java)
    val ENUMERABLE_LIMIT_SORT_RULE: EnumerableLimitSortRule = EnumerableLimitSortRule.Config.DEFAULT.toRule()
    val ENUMERABLE_LIMIT_RULE: EnumerableLimitRule = EnumerableLimitRule.Config.DEFAULT.toRule()

    /** Rule that converts a [org.apache.calcite.rel.logical.LogicalUnion]
     * to an [EnumerableUnion].  */
    val ENUMERABLE_UNION_RULE: EnumerableUnionRule =
        EnumerableUnionRule.DEFAULT_CONFIG.toRule(EnumerableUnionRule::class.java)

    /** Rule that converts a [LogicalRepeatUnion] into an
     * [EnumerableRepeatUnion].  */
    val ENUMERABLE_REPEAT_UNION_RULE: EnumerableRepeatUnionRule = EnumerableRepeatUnionRule.DEFAULT_CONFIG
        .toRule(EnumerableRepeatUnionRule::class.java)

    /** Rule to convert a [org.apache.calcite.rel.logical.LogicalSort] on top of a
     * [org.apache.calcite.rel.logical.LogicalUnion] into a [EnumerableMergeUnion].  */
    val ENUMERABLE_MERGE_UNION_RULE: EnumerableMergeUnionRule = EnumerableMergeUnionRule.Config.DEFAULT_CONFIG.toRule()

    /** Rule that converts a [LogicalTableSpool] into an
     * [EnumerableTableSpool].  */
    @Experimental
    val ENUMERABLE_TABLE_SPOOL_RULE: EnumerableTableSpoolRule = EnumerableTableSpoolRule.DEFAULT_CONFIG
        .toRule(EnumerableTableSpoolRule::class.java)

    /** Rule that converts a
     * [org.apache.calcite.rel.logical.LogicalIntersect] to an
     * [EnumerableIntersect].  */
    val ENUMERABLE_INTERSECT_RULE: EnumerableIntersectRule = EnumerableIntersectRule.DEFAULT_CONFIG
        .toRule(EnumerableIntersectRule::class.java)

    /** Rule that converts a
     * [org.apache.calcite.rel.logical.LogicalMinus] to an
     * [EnumerableMinus].  */
    val ENUMERABLE_MINUS_RULE: EnumerableMinusRule =
        EnumerableMinusRule.DEFAULT_CONFIG.toRule(EnumerableMinusRule::class.java)

    /** Rule that converts a
     * [org.apache.calcite.rel.logical.LogicalTableModify] to
     * [enumerable calling convention][EnumerableConvention].  */
    val ENUMERABLE_TABLE_MODIFICATION_RULE: EnumerableTableModifyRule = EnumerableTableModifyRule.DEFAULT_CONFIG
        .toRule(EnumerableTableModifyRule::class.java)

    /** Rule that converts a
     * [org.apache.calcite.rel.logical.LogicalValues] to
     * [enumerable calling convention][EnumerableConvention].  */
    val ENUMERABLE_VALUES_RULE: EnumerableValuesRule = EnumerableValuesRule.DEFAULT_CONFIG.toRule(
        EnumerableValuesRule::class.java
    )

    /** Rule that converts a [org.apache.calcite.rel.logical.LogicalWindow]
     * to an [org.apache.calcite.adapter.enumerable.EnumerableWindow].  */
    val ENUMERABLE_WINDOW_RULE: EnumerableWindowRule = EnumerableWindowRule.DEFAULT_CONFIG.toRule(
        EnumerableWindowRule::class.java
    )

    /** Rule that converts an [org.apache.calcite.rel.core.Collect]
     * to an [EnumerableCollect].  */
    val ENUMERABLE_COLLECT_RULE: EnumerableCollectRule = EnumerableCollectRule.DEFAULT_CONFIG.toRule(
        EnumerableCollectRule::class.java
    )

    /** Rule that converts an [org.apache.calcite.rel.core.Uncollect]
     * to an [EnumerableUncollect].  */
    val ENUMERABLE_UNCOLLECT_RULE: EnumerableUncollectRule = EnumerableUncollectRule.DEFAULT_CONFIG
        .toRule(EnumerableUncollectRule::class.java)
    val ENUMERABLE_FILTER_TO_CALC_RULE: EnumerableFilterToCalcRule = EnumerableFilterToCalcRule.Config.DEFAULT.toRule()

    /** Variant of [org.apache.calcite.rel.rules.ProjectToCalcRule] for
     * [enumerable calling convention][EnumerableConvention].  */
    val ENUMERABLE_PROJECT_TO_CALC_RULE: EnumerableProjectToCalcRule =
        EnumerableProjectToCalcRule.Config.DEFAULT.toRule()

    /** Rule that converts a
     * [org.apache.calcite.rel.logical.LogicalTableScan] to
     * [enumerable calling convention][EnumerableConvention].  */
    val ENUMERABLE_TABLE_SCAN_RULE: EnumerableTableScanRule = EnumerableTableScanRule.DEFAULT_CONFIG
        .toRule(EnumerableTableScanRule::class.java)

    /** Rule that converts a
     * [org.apache.calcite.rel.logical.LogicalTableFunctionScan] to
     * [enumerable calling convention][EnumerableConvention].  */
    val ENUMERABLE_TABLE_FUNCTION_SCAN_RULE: EnumerableTableFunctionScanRule =
        EnumerableTableFunctionScanRule.DEFAULT_CONFIG
            .toRule(EnumerableTableFunctionScanRule::class.java)

    /** Rule that converts a [LogicalMatch] to an
     * [EnumerableMatch].  */
    val ENUMERABLE_MATCH_RULE: EnumerableMatchRule =
        EnumerableMatchRule.DEFAULT_CONFIG.toRule(EnumerableMatchRule::class.java)

    /** Rule to convert a [LogicalAggregate]
     * to an [EnumerableSortedAggregate].  */
    val ENUMERABLE_SORTED_AGGREGATE_RULE: EnumerableSortedAggregateRule = EnumerableSortedAggregateRule.DEFAULT_CONFIG
        .toRule(EnumerableSortedAggregateRule::class.java)

    /** Rule that converts any enumerable relational expression to bindable.  */
    val TO_BINDABLE: EnumerableBindable.EnumerableToBindableConverterRule =
        EnumerableBindable.EnumerableToBindableConverterRule.DEFAULT_CONFIG
            .toRule(EnumerableBindable.EnumerableToBindableConverterRule::class.java)

    /**
     * Rule that converts [org.apache.calcite.interpreter.BindableRel]
     * to [org.apache.calcite.adapter.enumerable.EnumerableRel] by creating
     * an [org.apache.calcite.adapter.enumerable.EnumerableInterpreter].  */
    val TO_INTERPRETER: EnumerableInterpreterRule = EnumerableInterpreterRule.DEFAULT_CONFIG
        .toRule(EnumerableInterpreterRule::class.java)
    val ENUMERABLE_RULES: List<RelOptRule> = ImmutableList.of(
        ENUMERABLE_JOIN_RULE,
        ENUMERABLE_MERGE_JOIN_RULE,
        ENUMERABLE_CORRELATE_RULE,
        ENUMERABLE_PROJECT_RULE,
        ENUMERABLE_FILTER_RULE,
        ENUMERABLE_CALC_RULE,
        ENUMERABLE_AGGREGATE_RULE,
        ENUMERABLE_SORT_RULE,
        ENUMERABLE_LIMIT_RULE,
        ENUMERABLE_COLLECT_RULE,
        ENUMERABLE_UNCOLLECT_RULE,
        ENUMERABLE_MERGE_UNION_RULE,
        ENUMERABLE_UNION_RULE,
        ENUMERABLE_REPEAT_UNION_RULE,
        ENUMERABLE_TABLE_SPOOL_RULE,
        ENUMERABLE_INTERSECT_RULE,
        ENUMERABLE_MINUS_RULE,
        ENUMERABLE_TABLE_MODIFICATION_RULE,
        ENUMERABLE_VALUES_RULE,
        ENUMERABLE_WINDOW_RULE,
        ENUMERABLE_TABLE_SCAN_RULE,
        ENUMERABLE_TABLE_FUNCTION_SCAN_RULE,
        ENUMERABLE_MATCH_RULE
    )

    fun rules(): List<RelOptRule> {
        return ENUMERABLE_RULES
    }
}
