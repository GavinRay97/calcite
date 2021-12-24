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
import org.apache.calcite.plan.RelOptRuleOperand
import org.apache.calcite.plan.RelRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.util.Util
import org.apache.calcite.util.mapping.Mapping
import org.apache.calcite.util.mapping.MappingType
import org.apache.calcite.util.mapping.Mappings
import org.immutables.value.Value
import java.util.ArrayList
import java.util.List

/**
 * Rule to extract a [org.apache.calcite.rel.core.Project]
 * from an [org.apache.calcite.rel.core.Aggregate]
 * and push it down towards the input.
 *
 *
 * What projections can be safely pushed down depends upon which fields the
 * Aggregate uses.
 *
 *
 * To prevent cycles, this rule will not extract a `Project` if the
 * `Aggregate`s input is already a `Project`.
 */
@Value.Enclosing
class AggregateExtractProjectRule
/** Creates an AggregateExtractProjectRule.  */
protected constructor(config: Config?) : RelRule<AggregateExtractProjectRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        aggregateClass: Class<out Aggregate?>?,
        inputClass: Class<out RelNode?>?,
        relBuilderFactory: RelBuilderFactory?
    ) : this(
        Config.DEFAULT
            .withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(aggregateClass, inputClass)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        operand: RelOptRuleOperand?,
        builderFactory: RelBuilderFactory?
    ) : this(Config.DEFAULT
        .withRelBuilderFactory(builderFactory)
        .withOperandSupplier { b -> b.exactly(operand) }
        .`as`(Config::class.java)) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val aggregate: Aggregate = call.rel(0)
        val input: RelNode = call.rel(1)
        // Compute which input fields are used.
        // 1. group fields are always used
        val inputFieldsUsed: ImmutableBitSet.Builder = aggregate.getGroupSet().rebuild()
        // 2. agg functions
        for (aggCall in aggregate.getAggCallList()) {
            for (i in aggCall.getArgList()) {
                inputFieldsUsed.set(i)
            }
            if (aggCall.filterArg >= 0) {
                inputFieldsUsed.set(aggCall.filterArg)
            }
        }
        val relBuilder: RelBuilder = call.builder().push(input)
        val projects: List<RexNode> = ArrayList()
        val mapping: Mapping = Mappings.create(
            MappingType.INVERSE_SURJECTION,
            aggregate.getInput().getRowType().getFieldCount(),
            inputFieldsUsed.cardinality()
        )
        var j = 0
        for (i in inputFieldsUsed.build()) {
            projects.add(relBuilder.field(i))
            mapping.set(i, j++)
        }
        relBuilder.project(projects)
        val newGroupSet: ImmutableBitSet = Mappings.apply(mapping, aggregate.getGroupSet())
        val newGroupSets: List<ImmutableBitSet> = aggregate.getGroupSets().stream()
            .map { bitSet -> Mappings.apply(mapping, bitSet) }
            .collect(Util.toImmutableList())
        val newAggCallList: List<RelBuilder.AggCall> = aggregate.getAggCallList().stream()
            .map { aggCall -> relBuilder.aggregateCall(aggCall, mapping) }
            .collect(Util.toImmutableList())
        val groupKey: RelBuilder.GroupKey = relBuilder.groupKey(newGroupSet, newGroupSets)
        relBuilder.aggregate(groupKey, newAggCallList)
        call.transformTo(relBuilder.build())
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): AggregateExtractProjectRule {
            return AggregateExtractProjectRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            aggregateClass: Class<out Aggregate?>?,
            inputClass: Class<out RelNode?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(aggregateClass).oneInput { b1 ->
                    b1.operand(inputClass) // Predicate prevents matching against an Aggregate whose
                        // input is already a Project. Prevents this rule firing
                        // repeatedly.
                        .predicate { r -> r !is Project }.anyInputs()
                }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableAggregateExtractProjectRule.Config.of()
                .withOperandFor(Aggregate::class.java, LogicalTableScan::class.java)
        }
    }

    companion object {
        val SCAN = Config.DEFAULT.toRule()
    }
}
