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
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilderFactory
import org.immutables.value.Value

/**
 * Planner rule that pushes
 * [org.apache.calcite.rel.core.Project]
 * into a [MultiJoin],
 * creating a richer `MultiJoin`.
 *
 * @see org.apache.calcite.rel.rules.FilterMultiJoinMergeRule
 *
 * @see CoreRules.PROJECT_MULTI_JOIN_MERGE
 */
@Value.Enclosing
class ProjectMultiJoinMergeRule
/** Creates a ProjectMultiJoinMergeRule.  */
protected constructor(config: Config?) : RelRule<ProjectMultiJoinMergeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        projectClass: Class<out Project?>?,
        relBuilderFactory: RelBuilderFactory?
    ) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(projectClass, MultiJoin::class.java)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val project: Project = call.rel(0)
        val multiJoin: MultiJoin = call.rel(1)

        // if all inputs have their projFields set, then projection information
        // has already been pushed into each input
        var allSet = true
        for (i in 0 until multiJoin.getInputs().size()) {
            if (multiJoin.getProjFields().get(i) == null) {
                allSet = false
                break
            }
        }
        if (allSet) {
            return
        }

        // create a new MultiJoin that reflects the columns in the projection
        // above the MultiJoin
        val relBuilder: RelBuilder = call.builder()
        val newMultiJoin: MultiJoin = RelOptUtil.projectMultiJoin(multiJoin, project)
        relBuilder.push(newMultiJoin)
            .project(project.getProjects(), project.getRowType().getFieldNames())
        call.transformTo(relBuilder.build())
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): ProjectMultiJoinMergeRule? {
            return ProjectMultiJoinMergeRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            projectClass: Class<out Project?>?,
            multiJoinClass: Class<MultiJoin?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(projectClass).oneInput { b1 -> b1.operand(multiJoinClass).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableProjectMultiJoinMergeRule.Config.of()
                .withOperandFor(LogicalProject::class.java, MultiJoin::class.java)
        }
    }
}
