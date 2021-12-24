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
 * [org.apache.calcite.rel.logical.LogicalFilter] and a
 * [org.apache.calcite.rel.logical.LogicalCalc]. The
 * result is a [org.apache.calcite.rel.logical.LogicalCalc]
 * whose filter condition is the logical AND of the two.
 *
 * @see FilterMergeRule
 *
 * @see ProjectCalcMergeRule
 *
 * @see CoreRules.FILTER_CALC_MERGE
 */
@Value.Enclosing
class FilterCalcMergeRule
/** Creates a FilterCalcMergeRule.  */
protected constructor(config: Config?) : RelRule<FilterCalcMergeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT
            .withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val filter: LogicalFilter = call.rel(0)
        val calc: LogicalCalc = call.rel(1)

        // Don't merge a filter onto a calc which contains windowed aggregates.
        // That would effectively be pushing a multiset down through a filter.
        // We'll have chance to merge later, when the over is expanded.
        if (calc.containsOver()) {
            return
        }

        // Create a program containing the filter.
        val rexBuilder: RexBuilder = filter.getCluster().getRexBuilder()
        val progBuilder = RexProgramBuilder(
            calc.getRowType(),
            rexBuilder
        )
        progBuilder.addIdentity()
        progBuilder.addCondition(filter.getCondition())
        val topProgram: RexProgram = progBuilder.getProgram()
        val bottomProgram: RexProgram = calc.getProgram()

        // Merge the programs together.
        val mergedProgram: RexProgram = RexProgramBuilder.mergePrograms(
            topProgram,
            bottomProgram,
            rexBuilder
        )
        val newCalc: LogicalCalc = LogicalCalc.create(calc.getInput(), mergedProgram)
        call.transformTo(newCalc)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): FilterCalcMergeRule {
            return FilterCalcMergeRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            filterClass: Class<out Filter?>?,
            calcClass: Class<out Calc?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(filterClass).oneInput { b1 -> b1.operand(calcClass).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableFilterCalcMergeRule.Config.of()
                .withOperandFor(Filter::class.java, LogicalCalc::class.java)
        }
    }
}
