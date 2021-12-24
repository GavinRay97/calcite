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
package org.apache.calcite.adapter.enumerable

import org.apache.calcite.plan.RelOptRuleCall

/** Variant of [org.apache.calcite.rel.rules.ProjectToCalcRule] for
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention].
 *
 * @see EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE
 */
@Value.Enclosing
class EnumerableProjectToCalcRule
/** Creates an EnumerableProjectToCalcRule.  */
protected constructor(config: Config?) : ProjectToCalcRule(config) {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val project: EnumerableProject = call.rel(0)
        val input: RelNode = project.getInput()
        val program: RexProgram = RexProgram.create(
            input.getRowType(),
            project.getProjects(),
            null,
            project.getRowType(),
            project.getCluster().getRexBuilder()
        )
        val calc: EnumerableCalc = EnumerableCalc.create(input, program)
        call.transformTo(calc)
    }

    /** Rule configuration.  */
    @Value.Immutable
    @SuppressWarnings("immutables")
    interface Config : ProjectToCalcRule.Config {
        @Override
        fun toRule(): EnumerableProjectToCalcRule {
            return EnumerableProjectToCalcRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableEnumerableProjectToCalcRule.Config.of()
                .withOperandSupplier { b -> b.operand(EnumerableProject::class.java).anyInputs() }
        }
    }
}
