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
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.rel.core.Union
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.calcite.rel.logical.LogicalUnion
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilderFactory
import org.immutables.value.Value

/**
 * Planner rule that matches
 * [org.apache.calcite.rel.core.Aggregate]s beneath a
 * [org.apache.calcite.rel.core.Union] and pulls them up, so
 * that a single
 * [org.apache.calcite.rel.core.Aggregate] removes duplicates.
 *
 *
 * This rule only handles cases where the
 * [org.apache.calcite.rel.core.Union]s
 * still have only two inputs.
 *
 * @see CoreRules.AGGREGATE_UNION_AGGREGATE
 *
 * @see CoreRules.AGGREGATE_UNION_AGGREGATE_FIRST
 *
 * @see CoreRules.AGGREGATE_UNION_AGGREGATE_SECOND
 */
@Value.Enclosing
class AggregateUnionAggregateRule
/** Creates an AggregateUnionAggregateRule.  */
protected constructor(config: Config?) : RelRule<AggregateUnionAggregateRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        aggregateClass: Class<out Aggregate?>?,
        unionClass: Class<out Union?>?,
        firstUnionInputClass: Class<out RelNode?>?,
        secondUnionInputClass: Class<out RelNode?>?,
        relBuilderFactory: RelBuilderFactory?,
        desc: String?
    ) : this(
        Config.DEFAULT
            .withRelBuilderFactory(relBuilderFactory)
            .withDescription(desc)
            .`as`(Config::class.java)
            .withOperandFor(
                aggregateClass, unionClass, firstUnionInputClass,
                secondUnionInputClass
            )
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        aggregateClass: Class<out Aggregate?>?,
        aggregateFactory: RelFactories.AggregateFactory?,
        unionClass: Class<out Union?>?,
        setOpFactory: RelFactories.SetOpFactory?
    ) : this(
        aggregateClass, unionClass, RelNode::class.java, RelNode::class.java,
        RelBuilder.proto(aggregateFactory, setOpFactory),
        "AggregateUnionAggregateRule"
    ) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val topAggRel: Aggregate = call.rel(0)
        val union: Union = call.rel(1)

        // If distincts haven't been removed yet, defer invoking this rule
        if (!union.all) {
            return
        }
        val relBuilder: RelBuilder = call.builder()
        val bottomAggRel: Aggregate
        if (call.rel(3) is Aggregate) {
            // Aggregate is the second input
            bottomAggRel = call.rel(3)
            relBuilder.push(call.rel(2))
                .push(getInputWithSameRowType(bottomAggRel))
        } else if (call.rel(2) is Aggregate) {
            // Aggregate is the first input
            bottomAggRel = call.rel(2)
            relBuilder.push(getInputWithSameRowType(bottomAggRel))
                .push(call.rel(3))
        } else {
            return
        }

        // Only pull up aggregates if they are there just to remove distincts
        if (!topAggRel.getAggCallList().isEmpty()
            || !bottomAggRel.getAggCallList().isEmpty()
        ) {
            return
        }
        relBuilder.union(true)
        relBuilder.rename(union.getRowType().getFieldNames())
        relBuilder.aggregate(
            relBuilder.groupKey(topAggRel.getGroupSet()),
            topAggRel.getAggCallList()
        )
        call.transformTo(relBuilder.build())
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): AggregateUnionAggregateRule? {
            return AggregateUnionAggregateRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            aggregateClass: Class<out Aggregate?>?,
            unionClass: Class<out Union?>?,
            firstUnionInputClass: Class<out RelNode?>?,
            secondUnionInputClass: Class<out RelNode?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(aggregateClass)
                    .predicate(Aggregate::isSimple)
                    .oneInput { b1 ->
                        b1.operand(unionClass).inputs(
                            { b2 -> b2.operand(firstUnionInputClass).anyInputs() }
                        ) { b3 -> b3.operand(secondUnionInputClass).anyInputs() }
                    }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableAggregateUnionAggregateRule.Config.of()
                .withDescription("AggregateUnionAggregateRule")
                .withOperandFor(
                    LogicalAggregate::class.java, LogicalUnion::class.java,
                    RelNode::class.java, RelNode::class.java
                )
            val AGG_FIRST: Config = DEFAULT
                .withDescription("AggregateUnionAggregateRule:first-input-agg")
                .`as`(Config::class.java)
                .withOperandFor(
                    LogicalAggregate::class.java, LogicalUnion::class.java,
                    LogicalAggregate::class.java, RelNode::class.java
                )
            val AGG_SECOND: Config = DEFAULT
                .withDescription("AggregateUnionAggregateRule:second-input-agg")
                .`as`(Config::class.java)
                .withOperandFor(
                    LogicalAggregate::class.java, LogicalUnion::class.java,
                    RelNode::class.java, LogicalAggregate::class.java
                )
        }
    }

    companion object {
        //~ Methods ----------------------------------------------------------------
        /**
         * Returns an input with the same row type with the input Aggregate,
         * create a Project node if needed.
         */
        private fun getInputWithSameRowType(bottomAggRel: Aggregate): RelNode {
            return if (RelOptUtil.areRowTypesEqual(
                    bottomAggRel.getRowType(),
                    bottomAggRel.getInput(0).getRowType(),
                    false
                )
            ) {
                bottomAggRel.getInput(0)
            } else {
                RelOptUtil.createProject(
                    bottomAggRel.getInput(),
                    bottomAggRel.getGroupSet().asList()
                )
            }
        }
    }
}
