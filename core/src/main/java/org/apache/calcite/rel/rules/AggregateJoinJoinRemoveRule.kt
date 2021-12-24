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
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.RelRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.core.Join
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexUtil
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.util.mapping.Mappings
import com.google.common.collect.ImmutableList
import org.immutables.value.Value
import java.util.ArrayList
import java.util.HashMap
import java.util.List
import java.util.Map
import java.util.Set

/**
 * Planner rule that matches an [org.apache.calcite.rel.core.Aggregate]
 * on a [org.apache.calcite.rel.core.Join] and removes the left input
 * of the join provided that the left input is also a left join if possible.
 *
 *
 * For instance,
 *
 * <blockquote>
 * <pre>select distinct s.product_id, pc.product_id
 * from sales as s
 * left join product as p
 * on s.product_id = p.product_id
 * left join product_class pc
 * on s.product_id = pc.product_id</pre></blockquote>
 *
 *
 * becomes
 *
 * <blockquote>
 * <pre>select distinct s.product_id, pc.product_id
 * from sales as s
 * left join product_class pc
 * on s.product_id = pc.product_id</pre></blockquote>
 *
 * @see CoreRules.AGGREGATE_JOIN_JOIN_REMOVE
 */
@Value.Enclosing
class AggregateJoinJoinRemoveRule
/** Creates an AggregateJoinJoinRemoveRule.  */
protected constructor(config: Config?) : RelRule<AggregateJoinJoinRemoveRule.Config?>(config), TransformationRule {
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
        val topJoin: Join = call.rel(1)
        val bottomJoin: Join = call.rel(2)
        val leftBottomChildSize: Int = bottomJoin.getLeft().getRowType()
            .getFieldCount()

        // Check whether the aggregate uses columns in the right input of
        // bottom join.
        val allFields: Set<Integer> = RelOptUtil.getAllFields(aggregate)
        if (allFields.stream().anyMatch { i ->
                (i >= leftBottomChildSize
                        && i < bottomJoin.getRowType().getFieldCount())
            }) {
            return
        }
        if (aggregate.getAggCallList().stream().anyMatch { aggregateCall -> !aggregateCall.isDistinct() }) {
            return
        }

        // Check whether the top join uses columns in the right input of bottom join.
        val leftKeys: List<Integer> = ArrayList()
        RelOptUtil.splitJoinCondition(
            topJoin.getLeft(), topJoin.getRight(),
            topJoin.getCondition(), leftKeys, ArrayList(),
            ArrayList()
        )
        if (leftKeys.stream().anyMatch { s -> s >= leftBottomChildSize }) {
            return
        }

        // Check whether left join keys in top join and bottom join are equal.
        val leftChildKeys: List<Integer> = ArrayList()
        RelOptUtil.splitJoinCondition(
            bottomJoin.getLeft(), bottomJoin.getRight(),
            bottomJoin.getCondition(), leftChildKeys, ArrayList(),
            ArrayList()
        )
        if (!leftKeys.equals(leftChildKeys)) {
            return
        }
        val offset: Int = bottomJoin.getRight().getRowType().getFieldCount()
        val relBuilder: RelBuilder = call.builder()
        val condition: RexNode = RexUtil.shift(
            topJoin.getCondition(),
            leftBottomChildSize, -offset
        )
        val join: RelNode = relBuilder.push(bottomJoin.getLeft())
            .push(topJoin.getRight())
            .join(topJoin.getJoinType(), condition)
            .build()
        val map: Map<Integer, Integer> = HashMap()
        allFields.forEach { index ->
            map.put(
                index,
                if (index < leftBottomChildSize) index else index - offset
            )
        }
        val groupSet: ImmutableBitSet = aggregate.getGroupSet().permute(map)
        val aggCalls: ImmutableList.Builder<AggregateCall> = ImmutableList.builder()
        val sourceCount: Int = aggregate.getInput().getRowType().getFieldCount()
        val targetMapping: Mappings.TargetMapping = Mappings.target(map, sourceCount, sourceCount)
        aggregate.getAggCallList().forEach { aggregateCall -> aggCalls.add(aggregateCall.transform(targetMapping)) }
        val newAggregate: RelNode = relBuilder.push(join)
            .aggregate(relBuilder.groupKey(groupSet), aggCalls.build())
            .build()
        call.transformTo(newAggregate)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): AggregateJoinJoinRemoveRule? {
            return AggregateJoinJoinRemoveRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            aggregateClass: Class<out Aggregate?>?,
            joinClass: Class<out Join?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(aggregateClass)
                    .oneInput { b1 ->
                        b1.operand(joinClass)
                            .predicate { join -> join.getJoinType() === JoinRelType.LEFT }
                            .inputs { b2 ->
                                b2.operand(joinClass)
                                    .predicate { join -> join.getJoinType() === JoinRelType.LEFT }
                                    .anyInputs()
                            }
                    }
            }.`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableAggregateJoinJoinRemoveRule.Config.of()
                .withOperandFor(LogicalAggregate::class.java, LogicalJoin::class.java)
        }
    }
}
