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
 * [org.apache.calcite.rel.logical.LogicalFilter] to a
 * [org.apache.calcite.rel.logical.LogicalCalc].
 *
 *
 * The rule does *NOT* fire if the child is a
 * [org.apache.calcite.rel.logical.LogicalFilter] or a
 * [org.apache.calcite.rel.logical.LogicalProject] (we assume they they
 * will be converted using [FilterToCalcRule] or
 * [ProjectToCalcRule]) or a
 * [org.apache.calcite.rel.logical.LogicalCalc]. This
 * [org.apache.calcite.rel.logical.LogicalFilter] will eventually be
 * converted by [FilterCalcMergeRule].
 *
 * @see CoreRules.FILTER_TO_CALC
 */
@Value.Enclosing
class FilterToCalcRule
/** Creates a FilterToCalcRule.  */
protected constructor(config: Config?) : RelRule<FilterToCalcRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val filter: LogicalFilter = call.rel(0)
        val rel: RelNode = filter.getInput()

        // Create a program containing a filter.
        val rexBuilder: RexBuilder = filter.getCluster().getRexBuilder()
        val inputRowType: RelDataType = rel.getRowType()
        val programBuilder = RexProgramBuilder(inputRowType, rexBuilder)
        programBuilder.addIdentity()
        programBuilder.addCondition(filter.getCondition())
        val program: RexProgram = programBuilder.getProgram()
        val calc: LogicalCalc = LogicalCalc.create(rel, program)
        call.transformTo(calc)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): FilterToCalcRule? {
            return FilterToCalcRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableFilterToCalcRule.Config.of()
                .withOperandSupplier { b -> b.operand(LogicalFilter::class.java).anyInputs() }
        }
    }
}
