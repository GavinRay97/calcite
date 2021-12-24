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
 * Planner rule that pushes a [Filter] above a [Correlate] into the
 * inputs of the Correlate.
 *
 * @see CoreRules.FILTER_CORRELATE
 */
@Value.Enclosing
class FilterCorrelateRule
/** Creates a FilterCorrelateRule.  */
protected constructor(config: Config?) : RelRule<FilterCorrelateRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        filterFactory: FilterFactory?,
        projectFactory: RelFactories.ProjectFactory?
    ) : this(RelBuilder.proto(filterFactory, projectFactory)) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val filter: Filter = call.rel(0)
        val corr: Correlate = call.rel(1)
        val aboveFilters: List<RexNode> = RelOptUtil.conjunctions(filter.getCondition())
        val leftFilters: List<RexNode> = ArrayList()
        val rightFilters: List<RexNode> = ArrayList()

        // Try to push down above filters. These are typically where clause
        // filters. They can be pushed down if they are not on the NULL
        // generating side.
        RelOptUtil.classifyFilters(
            corr,
            aboveFilters,
            false,
            true,
            corr.getJoinType().canPushRightFromAbove(),
            aboveFilters,
            leftFilters,
            rightFilters
        )
        if (leftFilters.isEmpty()
            && rightFilters.isEmpty()
        ) {
            // no filters got pushed
            return
        }

        // Create Filters on top of the children if any filters were
        // pushed to them.
        val rexBuilder: RexBuilder = corr.getCluster().getRexBuilder()
        val relBuilder: RelBuilder = call.builder()
        val leftRel: RelNode = relBuilder.push(corr.getLeft()).filter(leftFilters).build()
        val rightRel: RelNode = relBuilder.push(corr.getRight()).filter(rightFilters).build()

        // Create the new Correlate
        val newCorrRel: RelNode = corr.copy(corr.getTraitSet(), ImmutableList.of(leftRel, rightRel))
        call.getPlanner().onCopy(corr, newCorrRel)
        if (!leftFilters.isEmpty()) {
            call.getPlanner().onCopy(filter, leftRel)
        }
        if (!rightFilters.isEmpty()) {
            call.getPlanner().onCopy(filter, rightRel)
        }

        // Create a Filter on top of the join if needed
        relBuilder.push(newCorrRel)
        relBuilder.filter(
            RexUtil.fixUp(
                rexBuilder, aboveFilters,
                RelOptUtil.getFieldTypeList(relBuilder.peek().getRowType())
            )
        )
        call.transformTo(relBuilder.build())
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): FilterCorrelateRule {
            return FilterCorrelateRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            filterClass: Class<out Filter?>?,
            correlateClass: Class<out Correlate?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(filterClass).oneInput { b1 -> b1.operand(correlateClass).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableFilterCorrelateRule.Config.of()
                .withOperandFor(Filter::class.java, Correlate::class.java)
        }
    }
}
