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
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.RelShuttleImpl
import org.apache.calcite.rel.core.Correlate
import org.apache.calcite.rel.core.CorrelationId
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexCorrelVariable
import org.apache.calcite.rex.RexFieldAccess
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexOver
import org.apache.calcite.rex.RexShuttle
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.util.BitSets
import org.apache.calcite.util.ImmutableBitSet
import org.immutables.value.Value
import java.util.BitSet
import java.util.HashMap
import java.util.Map
import java.util.Objects.requireNonNull

/**
 * Planner rule that pushes a [Project] under [Correlate] to apply
 * on Correlate's left and right inputs.
 *
 * @see CoreRules.PROJECT_CORRELATE_TRANSPOSE
 */
@Value.Enclosing
class ProjectCorrelateTransposeRule
/** Creates a ProjectCorrelateTransposeRule.  */
protected constructor(config: Config?) : RelRule<ProjectCorrelateTransposeRule.Config?>(config), TransformationRule {
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
        val origProject: Project = call.rel(0)
        val correlate: Correlate = call.rel(1)

        // locate all fields referenced in the projection
        // determine which inputs are referenced in the projection;
        // if all fields are being referenced and there are no
        // special expressions, no point in proceeding any further
        val pushProjector = PushProjector(
            origProject, call.builder().literal(true), correlate,
            config.preserveExprCondition(), call.builder()
        )
        if (pushProjector.locateAllRefs()) {
            return
        }

        // create left and right projections, projecting only those
        // fields referenced on each side
        val leftProject: RelNode = pushProjector.createProjectRefsAndExprs(
            correlate.getLeft(),
            true,
            false
        )
        var rightProject: RelNode = pushProjector.createProjectRefsAndExprs(
            correlate.getRight(),
            true,
            true
        )
        val requiredColsMap: Map<Integer?, Integer> = HashMap()

        // adjust requiredColumns that reference the projected columns
        val adjustments: IntArray = pushProjector.getAdjustments()
        val updatedBits = BitSet()
        for (col in correlate.getRequiredColumns()) {
            val newCol: Int = col + adjustments[col]
            updatedBits.set(newCol)
            requiredColsMap.put(col, newCol)
        }
        val rexBuilder: RexBuilder = call.builder().getRexBuilder()
        val correlationId: CorrelationId = correlate.getCluster().createCorrel()
        val rexCorrel: RexCorrelVariable = rexBuilder.makeCorrel(
            leftProject.getRowType(),
            correlationId
        ) as RexCorrelVariable

        // updates RexCorrelVariable and sets actual RelDataType for RexFieldAccess
        rightProject = rightProject.accept(
            RelNodesExprsHandler(
                RexFieldAccessReplacer(
                    correlate.getCorrelationId(),
                    rexCorrel, rexBuilder, requiredColsMap
                )
            )
        )

        // create a new correlate with the projected children
        val newCorrelate: Correlate = correlate.copy(
            correlate.getTraitSet(),
            leftProject,
            rightProject,
            correlationId,
            ImmutableBitSet.of(BitSets.toIter(updatedBits)),
            correlate.getJoinType()
        )

        // put the original project on top of the correlate, converting it to
        // reference the modified projection list
        val topProject: RelNode = pushProjector.createNewProject(newCorrelate, adjustments)
        call.transformTo(topProject)
    }

    /**
     * Visitor for RexNodes which replaces [RexCorrelVariable] with specified.
     */
    class RexFieldAccessReplacer(
        rexCorrelVariableToReplace: CorrelationId,
        rexCorrelVariable: RexCorrelVariable,
        builder: RexBuilder,
        requiredColsMap: Map<Integer?, Integer>
    ) : RexShuttle() {
        private val builder: RexBuilder
        private val rexCorrelVariableToReplace: CorrelationId
        private val rexCorrelVariable: RexCorrelVariable
        private val requiredColsMap: Map<Integer?, Integer>

        init {
            this.rexCorrelVariableToReplace = rexCorrelVariableToReplace
            this.rexCorrelVariable = rexCorrelVariable
            this.builder = builder
            this.requiredColsMap = requiredColsMap
        }

        @Override
        fun visitCorrelVariable(variable: RexCorrelVariable): RexNode {
            return if (variable.id.equals(rexCorrelVariableToReplace)) {
                rexCorrelVariable
            } else variable
        }

        @Override
        fun visitFieldAccess(fieldAccess: RexFieldAccess): RexNode {
            val refExpr: RexNode = fieldAccess.getReferenceExpr().accept(this)
            // creates new RexFieldAccess instance for the case when referenceExpr was replaced.
            // Otherwise calls super method.
            if (refExpr === rexCorrelVariable) {
                val fieldIndex: Int = fieldAccess.getField().getIndex()
                return builder.makeFieldAccess(
                    refExpr,
                    requireNonNull(
                        requiredColsMap[fieldIndex]
                    ) { "no entry for field $fieldIndex in $requiredColsMap" })
            }
            return super.visitFieldAccess(fieldAccess)
        }
    }

    /**
     * Visitor for RelNodes which applies specified [RexShuttle] visitor
     * for every node in the tree.
     */
    class RelNodesExprsHandler(rexVisitor: RexShuttle) : RelShuttleImpl() {
        private val rexVisitor: RexShuttle

        init {
            this.rexVisitor = rexVisitor
        }

        @Override
        protected fun visitChild(parent: RelNode?, i: Int, child: RelNode): RelNode {
            var child: RelNode = child
            if (child is HepRelVertex) {
                child = (child as HepRelVertex).getCurrentRel()
            } else if (child is RelSubset) {
                val subset: RelSubset = child as RelSubset
                child = subset.getBestOrOriginal()
            }
            return super.visitChild(parent, i, child).accept(rexVisitor)
        }
    }

    /** Rule configuration.  */
    @Value.Immutable(singleton = false)
    interface Config : RelRule.Config {
        @Override
        fun toRule(): ProjectCorrelateTransposeRule? {
            return ProjectCorrelateTransposeRule(this)
        }

        /** Defines when an expression should not be pushed.  */
        fun preserveExprCondition(): PushProjector.ExprCondition?

        /** Sets [.preserveExprCondition].  */
        fun withPreserveExprCondition(condition: PushProjector.ExprCondition?): Config?

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            projectClass: Class<out Project?>?,
            correlateClass: Class<out Correlate?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(projectClass).oneInput { b1 -> b1.operand(correlateClass).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableProjectCorrelateTransposeRule.Config.builder()
                .withPreserveExprCondition { expr -> expr !is RexOver }
                .build()
                .withOperandFor(Project::class.java, Correlate::class.java)
        }
    }
}
