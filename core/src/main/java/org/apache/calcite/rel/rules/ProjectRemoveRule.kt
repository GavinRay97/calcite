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
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rex.RexUtil
import org.apache.calcite.tools.RelBuilderFactory
import org.immutables.value.Value

/**
 * Planner rule that,
 * given a [org.apache.calcite.rel.core.Project] node that
 * merely returns its input, converts the node into its child.
 *
 *
 * For example, `Project(ArrayReader(a), {$input0})` becomes
 * `ArrayReader(a)`.
 *
 * @see CalcRemoveRule
 *
 * @see ProjectMergeRule
 *
 * @see CoreRules.PROJECT_REMOVE
 */
@Value.Enclosing
class ProjectRemoveRule
/** Creates a ProjectRemoveRule.  */
protected constructor(config: Config?) : RelRule<ProjectRemoveRule.Config?>(config), SubstitutionRule {
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
        assert(isTrivial(project))
        var stripped: RelNode = project.getInput()
        if (stripped is Project) {
            // Rename columns of child projection if desired field names are given.
            val childProject: Project = stripped as Project
            stripped = childProject.copy(
                childProject.getTraitSet(),
                childProject.getInput(), childProject.getProjects(),
                project.getRowType()
            )
        }
        stripped = convert(stripped, project.getConvention())
        call.transformTo(stripped)
    }

    @Override
    fun autoPruneOld(): Boolean {
        return true
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): ProjectRemoveRule? {
            return ProjectRemoveRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableProjectRemoveRule.Config.of()
                .withOperandSupplier { b ->
                    b.operand(Project::class.java) // Use a predicate to detect non-matches early.
                        // This keeps the rule queue short.
                        .predicate { project: Project -> isTrivial(project) }
                        .anyInputs()
                }
        }
    }

    companion object {
        /**
         * Returns the child of a project if the project is trivial, otherwise
         * the project itself.
         */
        fun strip(project: Project): RelNode {
            return if (isTrivial(project)) project.getInput() else project
        }

        fun isTrivial(project: Project): Boolean {
            return RexUtil.isIdentity(
                project.getProjects(),
                project.getInput().getRowType()
            )
        }
    }
}
