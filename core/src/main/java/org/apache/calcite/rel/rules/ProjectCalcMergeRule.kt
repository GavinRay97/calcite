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

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelRule
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.logical.LogicalCalc
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexOver
import org.apache.calcite.rex.RexProgram
import org.apache.calcite.rex.RexProgramBuilder
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.util.Pair
import org.immutables.value.Value

/**
 * Planner rule that merges a
 * [org.apache.calcite.rel.logical.LogicalProject] and a
 * [org.apache.calcite.rel.logical.LogicalCalc].
 *
 *
 * The resulting [org.apache.calcite.rel.logical.LogicalCalc] has the
 * same project list as the original
 * [org.apache.calcite.rel.logical.LogicalProject], but expressed in terms
 * of the original [org.apache.calcite.rel.logical.LogicalCalc]'s inputs.
 *
 * @see FilterCalcMergeRule
 *
 * @see CoreRules.PROJECT_CALC_MERGE
 */
@Value.Enclosing
class ProjectCalcMergeRule
/** Creates a ProjectCalcMergeRule.  */
protected constructor(config: Config?) : RelRule<ProjectCalcMergeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val project: Project = call.rel(0)
        val calc: Calc = call.rel(1)

        // Don't merge a project which contains windowed aggregates onto a
        // calc. That would effectively be pushing a windowed aggregate down
        // through a filter. Transform the project into an identical calc,
        // which we'll have chance to merge later, after the over is
        // expanded.
        val cluster: RelOptCluster = project.getCluster()
        val program: RexProgram = RexProgram.create(
            calc.getRowType(),
            project.getProjects(),
            null,
            project.getRowType(),
            cluster.getRexBuilder()
        )
        if (RexOver.containsOver(program)) {
            val projectAsCalc: LogicalCalc = LogicalCalc.create(calc, program)
            call.transformTo(projectAsCalc)
            return
        }

        // Create a program containing the project node's expressions.
        val rexBuilder: RexBuilder = cluster.getRexBuilder()
        val progBuilder = RexProgramBuilder(
            calc.getRowType(),
            rexBuilder
        )
        for (field in project.getNamedProjects()) {
            progBuilder.addProject(field.left, field.right)
        }
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
        fun toRule(): ProjectCalcMergeRule? {
            return ProjectCalcMergeRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            projectClass: Class<out Project?>?,
            calcClass: Class<out Calc?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(projectClass).oneInput { b1 -> b1.operand(calcClass).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableProjectCalcMergeRule.Config.of()
                .withOperandFor(LogicalProject::class.java, LogicalCalc::class.java)
        }
    }
}
