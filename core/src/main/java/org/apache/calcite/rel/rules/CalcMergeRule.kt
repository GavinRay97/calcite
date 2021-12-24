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
 * [org.apache.calcite.rel.logical.LogicalCalc] onto a
 * [org.apache.calcite.rel.logical.LogicalCalc].
 *
 *
 * The resulting [org.apache.calcite.rel.logical.LogicalCalc] has the
 * same project list as the upper
 * [org.apache.calcite.rel.logical.LogicalCalc], but expressed in terms of
 * the lower [org.apache.calcite.rel.logical.LogicalCalc]'s inputs.
 *
 * @see CoreRules.CALC_MERGE
 */
@Value.Enclosing
class CalcMergeRule
/** Creates a CalcMergeRule.  */
protected constructor(config: Config?) : RelRule<CalcMergeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val topCalc: Calc = call.rel(0)
        val bottomCalc: Calc = call.rel(1)

        // Don't merge a calc which contains windowed aggregates onto a
        // calc. That would effectively be pushing a windowed aggregate down
        // through a filter.
        val topProgram: RexProgram = topCalc.getProgram()
        if (RexOver.containsOver(topProgram)) {
            return
        }

        // Merge the programs together.
        val mergedProgram: RexProgram = RexProgramBuilder.mergePrograms(
            topCalc.getProgram(),
            bottomCalc.getProgram(),
            topCalc.getCluster().getRexBuilder()
        )
        assert(
            mergedProgram.getOutputRowType()
                    === topProgram.getOutputRowType()
        )
        val newCalc: Calc = topCalc.copy(
            topCalc.getTraitSet(),
            bottomCalc.getInput(),
            mergedProgram
        )
        if (newCalc.getDigest().equals(bottomCalc.getDigest())
            && newCalc.getRowType().equals(bottomCalc.getRowType())
        ) {
            // newCalc is equivalent to bottomCalc, which means that topCalc
            // must be trivial. Take it out of the game.
            call.getPlanner().prune(topCalc)
        }
        call.transformTo(newCalc)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): CalcMergeRule {
            return CalcMergeRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableCalcMergeRule.Config.of()
                .withOperandSupplier { b0 ->
                    b0.operand(Calc::class.java).oneInput { b1 -> b1.operand(Calc::class.java).anyInputs() }
                }
                .`as`(Config::class.java)
        }
    }
}
