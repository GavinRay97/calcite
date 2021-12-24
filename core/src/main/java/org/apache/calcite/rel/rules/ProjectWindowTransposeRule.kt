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
import org.apache.calcite.rel.RelCollations
import org.apache.calcite.rel.RelFieldCollation
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.core.Window
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rel.logical.LogicalWindow
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexShuttle
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.util.BitSets
import org.apache.calcite.util.ImmutableBitSet
import com.google.common.collect.ImmutableList
import org.immutables.value.Value
import java.util.ArrayList
import java.util.List

/**
 * Planner rule that pushes
 * a [org.apache.calcite.rel.logical.LogicalProject]
 * past a [org.apache.calcite.rel.logical.LogicalWindow].
 *
 * @see CoreRules.PROJECT_WINDOW_TRANSPOSE
 */
@Value.Enclosing
class ProjectWindowTransposeRule
/** Creates a ProjectWindowTransposeRule.  */
protected constructor(config: Config?) : RelRule<ProjectWindowTransposeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val project: Project = call.rel(0)
        val window: Window = call.rel(1)
        val cluster: RelOptCluster = window.getCluster()
        val rowTypeWindowInput: List<RelDataTypeField> = window.getInput().getRowType().getFieldList()
        val windowInputColumn: Int = rowTypeWindowInput.size()

        // Record the window input columns which are actually referred
        // either in the LogicalProject above LogicalWindow or LogicalWindow itself
        // (Note that the constants used in LogicalWindow are not considered here)
        val beReferred: ImmutableBitSet = findReference(project, window)

        // If all the the window input columns are referred,
        // it is impossible to trim anyone of them out
        if (beReferred.cardinality() === windowInputColumn) {
            return
        }

        // Put a DrillProjectRel below LogicalWindow
        val exps: List<RexNode> = ArrayList()
        val builder: RelDataTypeFactory.Builder = cluster.getTypeFactory().builder()

        // Keep only the fields which are referred
        for (index in BitSets.toIter(beReferred)) {
            val relDataTypeField: RelDataTypeField = rowTypeWindowInput[index]
            exps.add(RexInputRef(index, relDataTypeField.getType()))
            builder.add(relDataTypeField)
        }
        val projectBelowWindow = LogicalProject(
            cluster, window.getTraitSet(), ImmutableList.of(),
            window.getInput(), exps, builder.build()
        )

        // Create a new LogicalWindow with necessary inputs only
        val groups: List<Window.Group> = ArrayList()

        // As the un-referred columns are trimmed by the LogicalProject,
        // the indices specified in LogicalWindow would need to be adjusted
        val indexAdjustment: RexShuttle = object : RexShuttle() {
            @Override
            fun visitInputRef(inputRef: RexInputRef): RexNode {
                val newIndex = getAdjustedIndex(
                    inputRef.getIndex(), beReferred,
                    windowInputColumn
                )
                return RexInputRef(newIndex, inputRef.getType())
            }

            @Override
            fun visitCall(call: RexCall): RexNode {
                return if (call is Window.RexWinAggCall) {
                    val aggCall: Window.RexWinAggCall = call as Window.RexWinAggCall
                    val update = booleanArrayOf(false)
                    val clonedOperands: List<RexNode> = visitList(call.operands, update)
                    if (update[0]) {
                        RexWinAggCall(
                            call.getOperator() as SqlAggFunction, call.getType(),
                            clonedOperands, aggCall.ordinal, aggCall.distinct,
                            aggCall.ignoreNulls
                        )
                    } else {
                        call
                    }
                } else {
                    super.visitCall(call)
                }
            }
        }
        var aggCallIndex = windowInputColumn
        val outputBuilder: RelDataTypeFactory.Builder = cluster.getTypeFactory().builder()
        outputBuilder.addAll(projectBelowWindow.getRowType().getFieldList())
        for (group in window.groups) {
            val keys: ImmutableBitSet.Builder = ImmutableBitSet.builder()
            val orderKeys: List<RelFieldCollation> = ArrayList()
            val aggCalls: List<Window.RexWinAggCall> = ArrayList()

            // Adjust keys
            for (index in group.keys) {
                keys.set(getAdjustedIndex(index, beReferred, windowInputColumn))
            }

            // Adjust orderKeys
            for (relFieldCollation in group.orderKeys.getFieldCollations()) {
                val index: Int = relFieldCollation.getFieldIndex()
                orderKeys.add(
                    relFieldCollation.withFieldIndex(
                        getAdjustedIndex(index, beReferred, windowInputColumn)
                    )
                )
            }

            // Adjust Window Functions
            for (rexWinAggCall in group.aggCalls) {
                aggCalls.add(rexWinAggCall.accept(indexAdjustment) as Window.RexWinAggCall)
                val relDataTypeField: RelDataTypeField = window.getRowType().getFieldList().get(aggCallIndex)
                outputBuilder.add(relDataTypeField)
                ++aggCallIndex
            }
            groups.add(
                Group(
                    keys.build(), group.isRows, group.lowerBound,
                    group.upperBound, RelCollations.of(orderKeys), aggCalls
                )
            )
        }
        val newLogicalWindow: LogicalWindow = LogicalWindow.create(
            window.getTraitSet(), projectBelowWindow,
            window.constants, outputBuilder.build(), groups
        )

        // Modify the top LogicalProject
        val topProjExps: List<RexNode> = indexAdjustment.visitList(project.getProjects())
        val newTopProj: Project = project.copy(
            newLogicalWindow.getTraitSet(),
            newLogicalWindow,
            topProjExps,
            project.getRowType()
        )
        if (ProjectRemoveRule.isTrivial(newTopProj)) {
            call.transformTo(newLogicalWindow)
        } else {
            call.transformTo(newTopProj)
        }
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): ProjectWindowTransposeRule? {
            return ProjectWindowTransposeRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            projectClass: Class<out Project?>?,
            windowClass: Class<out Window?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(projectClass).oneInput { b1 -> b1.operand(windowClass).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableProjectWindowTransposeRule.Config.of()
                .withOperandFor(LogicalProject::class.java, LogicalWindow::class.java)
        }
    }

    companion object {
        private fun findReference(
            project: Project,
            window: Window
        ): ImmutableBitSet {
            val windowInputColumn: Int = window.getInput().getRowType().getFieldCount()
            val beReferred: ImmutableBitSet.Builder = ImmutableBitSet.builder()
            val referenceFinder: RexShuttle = object : RexShuttle() {
                @Override
                fun visitInputRef(inputRef: RexInputRef): RexNode {
                    val index: Int = inputRef.getIndex()
                    if (index < windowInputColumn) {
                        beReferred.set(index)
                    }
                    return inputRef
                }
            }

            // Reference in LogicalProject
            referenceFinder.visitEach(project.getProjects())

            // Reference in LogicalWindow
            for (group in window.groups) {
                // Reference in Partition-By
                for (index in group.keys) {
                    if (index < windowInputColumn) {
                        beReferred.set(index)
                    }
                }

                // Reference in Order-By
                for (relFieldCollation in group.orderKeys.getFieldCollations()) {
                    if (relFieldCollation.getFieldIndex() < windowInputColumn) {
                        beReferred.set(relFieldCollation.getFieldIndex())
                    }
                }

                // Reference in Window Functions
                referenceFinder.visitEach(group.aggCalls)
            }
            return beReferred.build()
        }

        private fun getAdjustedIndex(
            initIndex: Int,
            beReferred: ImmutableBitSet, windowInputColumn: Int
        ): Int {
            return if (initIndex >= windowInputColumn) {
                beReferred.cardinality() + (initIndex - windowInputColumn)
            } else {
                beReferred.get(0, initIndex).cardinality()
            }
        }
    }
}
