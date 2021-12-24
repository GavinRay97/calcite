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
 * Planner rule that removes a [semi-join][Join.isSemiJoin] from a join
 * tree.
 *
 *
 * It is invoked after attempts have been made to convert a SemiJoin to an
 * indexed scan on a join factor have failed. Namely, if the join factor does
 * not reduce to a single table that can be scanned using an index.
 *
 *
 * It should only be enabled if all SemiJoins in the plan are advisory; that
 * is, they can be safely dropped without affecting the semantics of the query.
 *
 * @see CoreRules.SEMI_JOIN_REMOVE
 */
@Value.Enclosing
class SemiJoinRemoveRule
/** Creates a SemiJoinRemoveRule.  */
protected constructor(config: Config?) : RelRule<SemiJoinRemoveRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        call.transformTo(call.rel(0).getInput(0))
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): SemiJoinRemoveRule? {
            return SemiJoinRemoveRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(joinClass: Class<out Join?>?): Config? {
            return withOperandSupplier { b -> b.operand(joinClass).predicate(Join::isSemiJoin).anyInputs() }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableSemiJoinRemoveRule.Config.of()
                .withOperandFor(LogicalJoin::class.java)
        }
    }
}
