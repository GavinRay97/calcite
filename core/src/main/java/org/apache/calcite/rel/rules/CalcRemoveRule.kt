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
 * Planner rule that removes a trivial
 * [org.apache.calcite.rel.logical.LogicalCalc].
 *
 *
 * A [org.apache.calcite.rel.logical.LogicalCalc]
 * is trivial if it projects its input fields in their
 * original order, and it does not filter.
 *
 * @see ProjectRemoveRule
 */
@Value.Enclosing
class CalcRemoveRule
/** Creates a CalcRemoveRule.  */
protected constructor(config: Config?) : RelRule<CalcRemoveRule.Config?>(config), SubstitutionRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val calc: Calc = call.rel(0)
        assert(calc.getProgram().isTrivial()) { "rule predicate" }
        var input: RelNode = calc.getInput()
        input = call.getPlanner().register(input, calc)
        call.transformTo(
            convert(
                input,
                calc.getTraitSet()
            )
        )
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): CalcRemoveRule {
            return CalcRemoveRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableCalcRemoveRule.Config.of()
                .withOperandSupplier { b ->
                    b.operand(LogicalCalc::class.java)
                        .predicate { calc -> calc.getProgram().isTrivial() }
                        .anyInputs()
                }
                .`as`(Config::class.java)
        }
    }
}
