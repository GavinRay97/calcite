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
import org.apache.calcite.plan.RelRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalCalc
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rex.RexProgram
import org.apache.calcite.tools.RelBuilderFactory
import org.immutables.value.Value

/**
 * Rule to convert a
 * [org.apache.calcite.rel.logical.LogicalProject] to a
 * [org.apache.calcite.rel.logical.LogicalCalc].
 *
 *
 * The rule does not fire if the child is a
 * [org.apache.calcite.rel.logical.LogicalProject],
 * [org.apache.calcite.rel.logical.LogicalFilter] or
 * [org.apache.calcite.rel.logical.LogicalCalc]. If it did, then the same
 * [org.apache.calcite.rel.logical.LogicalCalc] would be formed via
 * several transformation paths, which is a waste of effort.
 *
 * @see FilterToCalcRule
 *
 * @see CoreRules.PROJECT_TO_CALC
 */
@Value.Enclosing
class ProjectToCalcRule
/** Creates a ProjectToCalcRule.  */
protected constructor(config: Config?) : RelRule<ProjectToCalcRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val project: LogicalProject = call.rel(0)
        val input: RelNode = project.getInput()
        val program: RexProgram = RexProgram.create(
            input.getRowType(),
            project.getProjects(),
            null,
            project.getRowType(),
            project.getCluster().getRexBuilder()
        )
        val calc: LogicalCalc = LogicalCalc.create(input, program)
        call.transformTo(calc)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): ProjectToCalcRule? {
            return ProjectToCalcRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableProjectToCalcRule.Config.of()
                .withOperandSupplier { b -> b.operand(LogicalProject::class.java).anyInputs() }
        }
    }
}
