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
 * Planner rule that converts a [Calc]
 * to a [org.apache.calcite.rel.core.Project]
 * and [Filter].
 *
 *
 * Not enabled by default, as it works against the usual flow, which is to
 * convert `Project` and `Filter` to `Calc`. But useful for
 * specific tasks, such as optimizing before calling an
 * [org.apache.calcite.interpreter.Interpreter].
 *
 * @see CoreRules.CALC_SPLIT
 */
@Value.Enclosing
class CalcSplitRule
/** Creates a CalcSplitRule.  */
protected constructor(config: Config?) : RelRule<CalcSplitRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val calc: Calc = call.rel(0)
        val projectFilter: Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> = calc.getProgram().split()
        val relBuilder: RelBuilder = call.builder()
        relBuilder.push(calc.getInput())
        relBuilder.filter(projectFilter.right)
        relBuilder.project(projectFilter.left, calc.getRowType().getFieldNames())
        call.transformTo(relBuilder.build())
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): CalcSplitRule {
            return CalcSplitRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableCalcSplitRule.Config.of()
                .withOperandSupplier { b -> b.operand(Calc::class.java).anyInputs() }
        }
    }
}
