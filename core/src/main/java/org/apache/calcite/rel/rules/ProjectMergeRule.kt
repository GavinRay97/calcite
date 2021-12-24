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
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.RelRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.core.RelFactories.ProjectFactory
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexUtil
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.util.Permutation
import org.immutables.value.Value
import java.util.List

/**
 * ProjectMergeRule merges a [org.apache.calcite.rel.core.Project] into
 * another [org.apache.calcite.rel.core.Project],
 * provided the projects aren't projecting identical sets of input references.
 *
 * @see CoreRules.PROJECT_MERGE
 */
@Value.Enclosing
class ProjectMergeRule
/** Creates a ProjectMergeRule.  */
protected constructor(config: Config?) : RelRule<ProjectMergeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        force: Boolean, bloat: Int,
        relBuilderFactory: RelBuilderFactory?
    ) : this(
        CoreRules.PROJECT_MERGE.config.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withForce(force)
            .withBloat(bloat)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(force: Boolean, relBuilderFactory: RelBuilderFactory?) : this(
        CoreRules.PROJECT_MERGE.config.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withForce(force)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(force: Boolean, projectFactory: ProjectFactory?) : this(
        CoreRules.PROJECT_MERGE.config.withRelBuilderFactory(RelBuilder.proto(projectFactory))
            .`as`(Config::class.java)
            .withForce(force)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun matches(call: RelOptRuleCall): Boolean {
        val topProject: Project = call.rel(0)
        val bottomProject: Project = call.rel(1)
        return topProject.getConvention() === bottomProject.getConvention()
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val topProject: Project = call.rel(0)
        val bottomProject: Project = call.rel(1)
        val relBuilder: RelBuilder = call.builder()

        // If one or both projects are permutations, short-circuit the complex logic
        // of building a RexProgram.
        val topPermutation: Permutation = topProject.getPermutation()
        if (topPermutation != null) {
            if (topPermutation.isIdentity()) {
                // Let ProjectRemoveRule handle this.
                return
            }
            val bottomPermutation: Permutation = bottomProject.getPermutation()
            if (bottomPermutation != null) {
                if (bottomPermutation.isIdentity()) {
                    // Let ProjectRemoveRule handle this.
                    return
                }
                val product: Permutation = topPermutation.product(bottomPermutation)
                relBuilder.push(bottomProject.getInput())
                relBuilder.project(
                    relBuilder.fields(product),
                    topProject.getRowType().getFieldNames()
                )
                call.transformTo(relBuilder.build())
                return
            }
        }

        // If we're not in force mode and the two projects reference identical
        // inputs, then return and let ProjectRemoveRule replace the projects.
        if (!config.force()) {
            if (RexUtil.isIdentity(
                    topProject.getProjects(),
                    topProject.getInput().getRowType()
                )
            ) {
                return
            }
        }
        val newProjects: List<RexNode> = RelOptUtil.pushPastProjectUnlessBloat(
            topProject.getProjects(),
            bottomProject, config.bloat()
        )
            ?: // Merged projects are significantly more complex. Do not merge.
            return
        val input: RelNode = bottomProject.getInput()
        if (RexUtil.isIdentity(newProjects, input.getRowType())) {
            if (config.force()
                || input.getRowType().getFieldNames()
                    .equals(topProject.getRowType().getFieldNames())
            ) {
                call.transformTo(input)
                return
            }
        }

        // replace the two projects with a combined projection
        relBuilder.push(bottomProject.getInput())
        relBuilder.project(newProjects, topProject.getRowType().getFieldNames())
        call.transformTo(relBuilder.build())
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): ProjectMergeRule? {
            return ProjectMergeRule(this)
        }

        /** Limit how much complexity can increase during merging.
         * Default is [.DEFAULT_BLOAT] (100).  */
        @Value.Default
        fun bloat(): Int {
            return DEFAULT_BLOAT
        }

        /** Sets [.bloat].  */
        fun withBloat(bloat: Int): Config?

        /** Whether to always merge projects, default true.  */
        @Value.Default
        fun force(): Boolean {
            return true
        }

        /** Sets [.force].  */
        fun withForce(force: Boolean): Config?

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(projectClass: Class<out Project?>?): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(projectClass).oneInput { b1 -> b1.operand(projectClass).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableProjectMergeRule.Config.of()
                .withOperandFor(Project::class.java)
        }
    }

    companion object {
        /** Default amount by which complexity is allowed to increase.
         *
         * @see Config.bloat
         */
        const val DEFAULT_BLOAT = 100
    }
}
