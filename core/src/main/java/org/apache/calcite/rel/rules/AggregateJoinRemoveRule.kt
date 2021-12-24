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
 * Planner rule that matches an [org.apache.calcite.rel.core.Aggregate]
 * on a [org.apache.calcite.rel.core.Join] and removes the join
 * provided that the join is a left join or right join and it computes no
 * aggregate functions or all the aggregate calls have distinct.
 *
 *
 * For instance,
 *
 * <blockquote>
 * <pre>select distinct s.product_id from
 * sales as s
 * left join product as p
 * on s.product_id = p.product_id</pre></blockquote>
 *
 *
 * becomes
 *
 * <blockquote>
 * <pre>select distinct s.product_id from sales as s</pre></blockquote>
 *
 * @see CoreRules.AGGREGATE_JOIN_REMOVE
 */
@Value.Enclosing
class AggregateJoinRemoveRule
/** Creates an AggregateJoinRemoveRule.  */
protected constructor(config: Config?) : RelRule<AggregateJoinRemoveRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        aggregateClass: Class<out Aggregate?>?,
        joinClass: Class<out Join?>?, relBuilderFactory: RelBuilderFactory?
    ) : this(
        Config.DEFAULT
            .withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(aggregateClass, joinClass)
    ) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val aggregate: Aggregate = call.rel(0)
        val join: Join = call.rel(1)
        val isLeftJoin = join.getJoinType() === JoinRelType.LEFT
        val lower = if (isLeftJoin) join.getLeft().getRowType().getFieldCount() else 0
        val upper: Int =
            if (isLeftJoin) join.getRowType().getFieldCount() else join.getLeft().getRowType().getFieldCount()

        // Check whether the aggregate uses columns whose index is between
        // lower(included) and upper(excluded).
        val allFields: Set<Integer> = RelOptUtil.getAllFields(aggregate)
        if (allFields.stream().anyMatch { i -> i >= lower && i < upper }) {
            return
        }
        if (aggregate.getAggCallList().stream().anyMatch { aggregateCall -> !aggregateCall.isDistinct() }) {
            return
        }
        val node: RelNode
        node = if (isLeftJoin) {
            aggregate.copy(
                aggregate.getTraitSet(), join.getLeft(),
                aggregate.getGroupSet(), aggregate.getGroupSets(),
                aggregate.getAggCallList()
            )
        } else {
            val map: Map<Integer, Integer> = HashMap()
            allFields.forEach { index -> map.put(index, index - upper) }
            val groupSet: ImmutableBitSet = aggregate.getGroupSet().permute(map)
            val aggCalls: ImmutableList.Builder<AggregateCall> = ImmutableList.builder()
            val sourceCount: Int = aggregate.getInput().getRowType().getFieldCount()
            val targetMapping: Mappings.TargetMapping = Mappings.target(map, sourceCount, sourceCount)
            aggregate.getAggCallList().forEach { aggregateCall -> aggCalls.add(aggregateCall.transform(targetMapping)) }
            val relBuilder: RelBuilder = call.builder()
            relBuilder.push(join.getRight())
                .aggregate(relBuilder.groupKey(groupSet), aggCalls.build())
                .build()
        }
        call.transformTo(node)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): AggregateJoinRemoveRule? {
            return AggregateJoinRemoveRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            aggregateClass: Class<out Aggregate?>?,
            joinClass: Class<out Join?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(aggregateClass).oneInput { b1 ->
                    b1.operand(joinClass)
                        .predicate { join ->
                            (join.getJoinType() === JoinRelType.LEFT
                                    || join.getJoinType() === JoinRelType.RIGHT)
                        }
                        .anyInputs()
                }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableAggregateJoinRemoveRule.Config.of()
                .withOperandFor(LogicalAggregate::class.java, LogicalJoin::class.java)
        }
    }
}
