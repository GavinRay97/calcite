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
import org.apache.calcite.rel.core.Join
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexOver
import org.apache.calcite.rex.RexShuttle
import org.apache.calcite.tools.RelBuilderFactory
import org.immutables.value.Value
import java.util.ArrayList
import java.util.List
import java.util.Objects.requireNonNull

/**
 * Planner rule that pushes a [org.apache.calcite.rel.core.Project]
 * past a [org.apache.calcite.rel.core.Join]
 * by splitting the projection into a projection on top of each child of
 * the join.
 *
 * @see CoreRules.PROJECT_JOIN_TRANSPOSE
 */
@Value.Enclosing
class ProjectJoinTransposeRule
/** Creates a ProjectJoinTransposeRule.  */
protected constructor(config: Config?) : RelRule<ProjectJoinTransposeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        projectClass: Class<out Project?>?,
        joinClass: Class<out Join?>?,
        preserveExprCondition: PushProjector.ExprCondition?,
        relBuilderFactory: RelBuilderFactory?
    ) : this(
        Config.DEFAULT
            .withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(projectClass, joinClass)
            .withPreserveExprCondition(preserveExprCondition)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val origProject: Project = call.rel(0)
        val join: Join = call.rel(1)

        // Normalize the join condition so we don't end up misidentified expanded
        // form of IS NOT DISTINCT FROM as PushProject also visit the filter condition
        // and push down expressions.
        val joinFilter: RexNode = join.getCondition().accept(object : RexShuttle() {
            @Override
            fun visitCall(rexCall: RexCall?): RexNode {
                val node: RexNode = super.visitCall(rexCall)
                return if (node !is RexCall) {
                    node
                } else RelOptUtil.collapseExpandedIsNotDistinctFromExpr(
                    node as RexCall,
                    call.builder().getRexBuilder()
                )
            }
        })

        // locate all fields referenced in the projection and join condition;
        // determine which inputs are referenced in the projection and
        // join condition; if all fields are being referenced and there are no
        // special expressions, no point in proceeding any further
        val pushProjector = PushProjector(
            origProject,
            joinFilter,
            join,
            config.preserveExprCondition(),
            call.builder()
        )
        if (pushProjector.locateAllRefs()) {
            return
        }

        // create left and right projections, projecting only those
        // fields referenced on each side
        val leftProject: RelNode = pushProjector.createProjectRefsAndExprs(
            join.getLeft(),
            true,
            false
        )
        val rightProject: RelNode = pushProjector.createProjectRefsAndExprs(
            join.getRight(),
            true,
            true
        )

        // convert the join condition to reference the projected columns
        var newJoinFilter: RexNode? = null
        val adjustments: IntArray = pushProjector.getAdjustments()
        if (joinFilter != null) {
            val projectJoinFieldList: List<RelDataTypeField> = ArrayList()
            projectJoinFieldList.addAll(
                join.getSystemFieldList()
            )
            projectJoinFieldList.addAll(
                leftProject.getRowType().getFieldList()
            )
            projectJoinFieldList.addAll(
                rightProject.getRowType().getFieldList()
            )
            newJoinFilter = pushProjector.convertRefsAndExprs(
                joinFilter,
                projectJoinFieldList,
                adjustments
            )
        }

        // create a new join with the projected children
        val newJoin: Join = join.copy(
            join.getTraitSet(),
            requireNonNull(newJoinFilter, "newJoinFilter must not be null"),
            leftProject,
            rightProject,
            join.getJoinType(),
            join.isSemiJoinDone()
        )

        // put the original project on top of the join, converting it to
        // reference the modified projection list
        val topProject: RelNode = pushProjector.createNewProject(newJoin, adjustments)
        call.transformTo(topProject)
    }

    /** Rule configuration.  */
    @Value.Immutable(singleton = false)
    interface Config : RelRule.Config {
        @Override
        fun toRule(): ProjectJoinTransposeRule? {
            return ProjectJoinTransposeRule(this)
        }

        /** Defines when an expression should not be pushed.  */
        fun preserveExprCondition(): PushProjector.ExprCondition?

        /** Sets [.preserveExprCondition].  */
        fun withPreserveExprCondition(condition: PushProjector.ExprCondition?): Config?

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            projectClass: Class<out Project?>?,
            joinClass: Class<out Join?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(projectClass).oneInput { b1 -> b1.operand(joinClass).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableProjectJoinTransposeRule.Config.builder()
                .withPreserveExprCondition { expr -> expr !is RexOver }
                .build()
                .withOperandFor(LogicalProject::class.java, LogicalJoin::class.java)
        }
    }
}
