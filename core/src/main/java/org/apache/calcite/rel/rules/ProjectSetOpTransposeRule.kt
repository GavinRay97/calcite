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
import org.apache.calcite.rel.core.SetOp
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexOver
import org.apache.calcite.tools.RelBuilderFactory
import org.immutables.value.Value
import java.util.ArrayList
import java.util.List

/**
 * Planner rule that pushes
 * a [org.apache.calcite.rel.logical.LogicalProject]
 * past a [org.apache.calcite.rel.core.SetOp].
 *
 *
 * The children of the `SetOp` will project
 * only the [RexInputRef]s referenced in the original
 * `LogicalProject`.
 *
 * @see CoreRules.PROJECT_SET_OP_TRANSPOSE
 */
@Value.Enclosing
class ProjectSetOpTransposeRule
/** Creates a ProjectSetOpTransposeRule.  */
protected constructor(config: Config?) : RelRule<ProjectSetOpTransposeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        preserveExprCondition: PushProjector.ExprCondition?,
        relBuilderFactory: RelBuilderFactory?
    ) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withPreserveExprCondition(preserveExprCondition)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val origProject: LogicalProject = call.rel(0)
        val setOp: SetOp = call.rel(1)

        // cannot push project past a distinct
        if (!setOp.all) {
            return
        }

        // locate all fields referenced in the projection
        val pushProjector = PushProjector(
            origProject, null, setOp,
            config.preserveExprCondition(), call.builder()
        )
        pushProjector.locateAllRefs()
        val newSetOpInputs: List<RelNode> = ArrayList()
        val adjustments: IntArray = pushProjector.getAdjustments()
        val node: RelNode
        node = if (origProject.containsOver()) {
            // should not push over past set-op but can push its operand down.
            for (input in setOp.getInputs()) {
                val p: Project = pushProjector.createProjectRefsAndExprs(input, true, false)
                // make sure that it is not a trivial project to avoid infinite loop.
                if (p.getRowType().equals(input.getRowType())) {
                    return
                }
                newSetOpInputs.add(p)
            }
            val newSetOp: SetOp = setOp.copy(setOp.getTraitSet(), newSetOpInputs)
            pushProjector.createNewProject(newSetOp, adjustments)
        } else {
            // push some expressions below the set-op; this
            // is different from pushing below a join, where we decompose
            // to try to keep expensive expressions above the join,
            // because UNION ALL does not have any filtering effect,
            // and it is the only operator this rule currently acts on
            setOp.getInputs().forEach { input ->
                newSetOpInputs.add(
                    pushProjector.createNewProject(
                        pushProjector.createProjectRefsAndExprs(
                            input, true, false
                        ), adjustments
                    )
                )
            }
            setOp.copy(setOp.getTraitSet(), newSetOpInputs)
        }
        call.transformTo(node)
    }

    /** Rule configuration.  */
    @Value.Immutable(singleton = false)
    interface Config : RelRule.Config {
        @Override
        fun toRule(): ProjectSetOpTransposeRule? {
            return ProjectSetOpTransposeRule(this)
        }

        /** Defines when an expression should not be pushed.  */
        fun preserveExprCondition(): PushProjector.ExprCondition?

        /** Sets [.preserveExprCondition].  */
        fun withPreserveExprCondition(condition: PushProjector.ExprCondition?): Config?

        companion object {
            val DEFAULT: Config = ImmutableProjectSetOpTransposeRule.Config.builder()
                .withPreserveExprCondition { expr -> expr !is RexOver }
                .build()
                .withOperandSupplier { b0 ->
                    b0.operand(LogicalProject::class.java).oneInput { b1 -> b1.operand(SetOp::class.java).anyInputs() }
                }
        }
    }
}
