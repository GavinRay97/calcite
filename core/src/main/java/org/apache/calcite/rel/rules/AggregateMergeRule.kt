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

import org.apache.calcite.plan.RelOptRuleCall

/**
 * Planner rule that matches an [Aggregate] on a [Aggregate]
 * and the top aggregate's group key is a subset of the lower aggregate's
 * group key, and the aggregates are expansions of rollups, then it would
 * convert into a single aggregate.
 *
 *
 * For example, SUM of SUM becomes SUM; SUM of COUNT becomes COUNT;
 * MAX of MAX becomes MAX; MIN of MIN becomes MIN. AVG of AVG would not
 * match, nor would COUNT of COUNT.
 *
 * @see CoreRules.AGGREGATE_MERGE
 */
@Value.Enclosing
class AggregateMergeRule
/** Creates an AggregateMergeRule.  */
protected constructor(config: Config?) : RelRule<AggregateMergeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        operand: RelOptRuleOperand?,
        relBuilderFactory: RelBuilderFactory?
    ) : this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .withOperandSupplier { b -> b.exactly(operand) }
        .`as`(Config::class.java)) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val topAgg: Aggregate = call.rel(0)
        val bottomAgg: Aggregate = call.rel(1)
        if (topAgg.getGroupCount() > bottomAgg.getGroupCount()) {
            return
        }
        val bottomGroupSet: ImmutableBitSet = bottomAgg.getGroupSet()
        val map: Map<Integer, Integer> = HashMap()
        bottomGroupSet.forEach { v -> map.put(map.size(), v) }
        for (k in topAgg.getGroupSet()) {
            if (!map.containsKey(k)) {
                return
            }
        }

        // top aggregate keys must be subset of lower aggregate keys
        val topGroupSet: ImmutableBitSet = topAgg.getGroupSet().permute(map)
        if (!bottomGroupSet.contains(topGroupSet)) {
            return
        }
        val hasEmptyGroup: Boolean = topAgg.getGroupSets()
            .stream().anyMatch(ImmutableBitSet::isEmpty)
        val finalCalls: List<AggregateCall> = ArrayList()
        for (topCall in topAgg.getAggCallList()) {
            if (!isAggregateSupported(topCall)
                || topCall.getArgList().size() === 0
            ) {
                return
            }
            // Make sure top aggregate argument refers to one of the aggregate
            val bottomIndex: Int = topCall.getArgList().get(0) - bottomGroupSet.cardinality()
            if (bottomIndex >= bottomAgg.getAggCallList().size()
                || bottomIndex < 0
            ) {
                return
            }
            val bottomCall: AggregateCall = bottomAgg.getAggCallList().get(bottomIndex)
            // Should not merge if top agg with empty group keys and the lower agg
            // function is COUNT, because in case of empty input for lower agg,
            // the result is empty, if we merge them, we end up with 1 result with
            // 0, which is wrong.
            if (!isAggregateSupported(bottomCall)
                || bottomCall.getAggregation() === SqlStdOperatorTable.COUNT && topCall.getAggregation()
                    .getKind() !== SqlKind.SUM0 && hasEmptyGroup
            ) {
                return
            }
            val splitter: SqlSplittableAggFunction = bottomCall.getAggregation()
                .unwrapOrThrow(SqlSplittableAggFunction::class.java)
            val finalCall: AggregateCall = splitter.merge(topCall, bottomCall) ?: return
            // fail to merge the aggregate call, bail out
            finalCalls.add(finalCall)
        }

        // re-map grouping sets
        var newGroupingSets: ImmutableList<ImmutableBitSet?>? = null
        if (topAgg.getGroupType() !== Group.SIMPLE) {
            newGroupingSets = ImmutableBitSet.ORDERING.immutableSortedCopy(
                ImmutableBitSet.permute(topAgg.getGroupSets(), map)
            )
        }
        val finalAgg: Aggregate = topAgg.copy(
            topAgg.getTraitSet(), bottomAgg.getInput(), topGroupSet,
            newGroupingSets, finalCalls
        )
        call.transformTo(finalAgg)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): AggregateMergeRule? {
            return AggregateMergeRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableAggregateMergeRule.Config.of()
                .withOperandSupplier { b0 ->
                    b0.operand(Aggregate::class.java)
                        .oneInput { b1 ->
                            b1.operand(Aggregate::class.java)
                                .predicate(Aggregate::isSimple)
                                .anyInputs()
                        }
                }
                .`as`(Config::class.java)
        }
    }

    companion object {
        private fun isAggregateSupported(aggCall: AggregateCall): Boolean {
            return if (aggCall.isDistinct()
                || aggCall.hasFilter()
                || aggCall.isApproximate()
                || aggCall.getArgList().size() > 1
            ) {
                false
            } else aggCall.getAggregation()
                .maybeUnwrap(SqlSplittableAggFunction::class.java).isPresent()
        }
    }
}
