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
 * Planner rule that converts a
 * [LogicalMatch] to the result
 * of calling [LogicalMatch.copy].
 *
 * @see CoreRules.MATCH
 */
@Value.Enclosing
class MatchRule
/** Creates a MatchRule.  */
protected constructor(config: Config?) : RelRule<MatchRule.Config?>(config), TransformationRule {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val oldRel: LogicalMatch = call.rel(0)
        val match: RelNode = LogicalMatch.create(
            oldRel.getCluster(),
            oldRel.getTraitSet(), oldRel.getInput(), oldRel.getRowType(),
            oldRel.getPattern(), oldRel.isStrictStart(), oldRel.isStrictEnd(),
            oldRel.getPatternDefinitions(), oldRel.getMeasures(),
            oldRel.getAfter(), oldRel.getSubsets(), oldRel.isAllRows(),
            oldRel.getPartitionKeys(), oldRel.getOrderKeys(),
            oldRel.getInterval()
        )
        call.transformTo(match)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): MatchRule? {
            return MatchRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableMatchRule.Config.of()
                .withOperandSupplier { b -> b.operand(LogicalMatch::class.java).anyInputs() }
        }
    }
}
