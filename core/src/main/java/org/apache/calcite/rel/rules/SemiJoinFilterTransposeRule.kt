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
 * Planner rule that pushes `SemiJoin`s down in a tree past
 * a [org.apache.calcite.rel.core.Filter].
 *
 *
 * The intention is to trigger other rules that will convert
 * `SemiJoin`s.
 *
 *
 * SemiJoin(LogicalFilter(X), Y)  LogicalFilter(SemiJoin(X, Y))
 *
 * @see SemiJoinProjectTransposeRule
 *
 * @see CoreRules.SEMI_JOIN_FILTER_TRANSPOSE
 */
@Value.Enclosing
class SemiJoinFilterTransposeRule
/** Creates a SemiJoinFilterTransposeRule.  */
protected constructor(config: Config?) : RelRule<SemiJoinFilterTransposeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val semiJoin: Join = call.rel(0)
        val filter: Filter = call.rel(1)
        val newSemiJoin: RelNode = LogicalJoin.create(
            filter.getInput(),
            semiJoin.getRight(),  // No need to copy the hints, the framework would try to do that.
            ImmutableList.of(),
            semiJoin.getCondition(),
            ImmutableSet.of(),
            JoinRelType.SEMI
        )
        val factory: FilterFactory = RelFactories.DEFAULT_FILTER_FACTORY
        val newFilter: RelNode = factory.createFilter(
            newSemiJoin, filter.getCondition(),
            ImmutableSet.of()
        )
        call.transformTo(newFilter)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): SemiJoinFilterTransposeRule? {
            return SemiJoinFilterTransposeRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            joinClass: Class<out Join?>?,
            filterClass: Class<out Filter?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(joinClass).predicate(Join::isSemiJoin).inputs { b1 -> b1.operand(filterClass).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableSemiJoinFilterTransposeRule.Config.of()
                .withOperandFor(LogicalJoin::class.java, LogicalFilter::class.java)
        }
    }
}
