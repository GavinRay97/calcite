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
 * Planner rule that merges a
 * [Filter] into a [MultiJoin],
 * creating a richer `MultiJoin`.
 *
 * @see org.apache.calcite.rel.rules.ProjectMultiJoinMergeRule
 *
 * @see CoreRules.FILTER_MULTI_JOIN_MERGE
 */
@Value.Enclosing
class FilterMultiJoinMergeRule
/** Creates a FilterMultiJoinMergeRule.  */
protected constructor(config: Config?) : RelRule<FilterMultiJoinMergeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        filterClass: Class<out Filter?>?,
        relBuilderFactory: RelBuilderFactory?
    ) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(filterClass, MultiJoin::class.java)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val filter: Filter = call.rel(0)
        val multiJoin: MultiJoin = call.rel(1)

        // Create a new post-join filter condition
        // Conditions are nullable, so ImmutableList can't be used here
        val filters: List<RexNode> = Arrays.asList(
            filter.getCondition(),
            multiJoin.getPostJoinFilter()
        )
        val rexBuilder: RexBuilder = multiJoin.getCluster().getRexBuilder()
        val newMultiJoin = MultiJoin(
            multiJoin.getCluster(),
            multiJoin.getInputs(),
            multiJoin.getJoinFilter(),
            multiJoin.getRowType(),
            multiJoin.isFullOuterJoin(),
            multiJoin.getOuterJoinConditions(),
            multiJoin.getJoinTypes(),
            multiJoin.getProjFields(),
            multiJoin.getJoinFieldRefCountsMap(),
            RexUtil.composeConjunction(rexBuilder, filters, true)
        )
        call.transformTo(newMultiJoin)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): FilterMultiJoinMergeRule? {
            return FilterMultiJoinMergeRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            filterClass: Class<out Filter?>?,
            multiJoinClass: Class<out MultiJoin?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(filterClass).oneInput { b1 -> b1.operand(multiJoinClass).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableFilterMultiJoinMergeRule.Config.of()
                .withOperandFor(Filter::class.java, MultiJoin::class.java)
        }
    }
}
