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
import org.apache.calcite.plan.RelOptRuleOperand
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.RelRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.logical.LogicalFilter
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexShuttle
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.util.ImmutableBitSet
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import org.immutables.value.Value
import java.util.ArrayList
import java.util.LinkedHashSet
import java.util.List
import java.util.Set

/**
 * Planner rule that pushes a [org.apache.calcite.rel.core.Project]
 * past a [org.apache.calcite.rel.core.Filter].
 *
 * @see CoreRules.PROJECT_FILTER_TRANSPOSE
 *
 * @see CoreRules.PROJECT_FILTER_TRANSPOSE_WHOLE_EXPRESSIONS
 *
 * @see CoreRules.PROJECT_FILTER_TRANSPOSE_WHOLE_PROJECT_EXPRESSIONS
 */
@Value.Enclosing
class ProjectFilterTransposeRule
/** Creates a ProjectFilterTransposeRule.  */
protected constructor(config: Config?) : RelRule<ProjectFilterTransposeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        projectClass: Class<out Project?>?,
        filterClass: Class<out Filter?>?,
        relBuilderFactory: RelBuilderFactory?,
        preserveExprCondition: PushProjector.ExprCondition?
    ) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(projectClass, filterClass)
            .withPreserveExprCondition(preserveExprCondition)
    ) {
    }

    @Deprecated // to be removed before 2.0
    protected constructor(
        operand: RelOptRuleOperand?,
        preserveExprCondition: PushProjector.ExprCondition?, wholeProject: Boolean,
        wholeFilter: Boolean, relBuilderFactory: RelBuilderFactory?
    ) : this(Config.DEFAULT
        .withOperandSupplier { b -> b.exactly(operand) }
        .withRelBuilderFactory(relBuilderFactory)
        .`as`(Config::class.java)
        .withPreserveExprCondition(preserveExprCondition)
        .withWholeProject(wholeProject)
        .withWholeFilter(wholeFilter)) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val origProject: Project?
        val filter: Filter
        if (call.rels.length >= 2) {
            origProject = call.rel(0)
            filter = call.rel(1)
        } else {
            origProject = null
            filter = call.rel(0)
        }
        val input: RelNode = filter.getInput()
        val origFilter: RexNode = filter.getCondition()
        if (origProject != null && origProject.containsOver()) {
            // Cannot push project through filter if project contains a windowed
            // aggregate -- it will affect row counts. Abort this rule
            // invocation; pushdown will be considered after the windowed
            // aggregate has been implemented. It's OK if the filter contains a
            // windowed aggregate.
            return
        }
        if (origProject != null
            && origProject.getRowType().isStruct()
            && origProject.getRowType().getFieldList().stream()
                .anyMatch(RelDataTypeField::isDynamicStar)
        ) {
            // The PushProjector would change the plan:
            //
            //    prj(**=[$0])
            //    : - filter
            //        : - scan
            //
            // to form like:
            //
            //    prj(**=[$0])                    (1)
            //    : - filter                      (2)
            //        : - prj(**=[$0], ITEM= ...) (3)
            //            :  - scan
            // This new plan has more cost that the old one, because of the new
            // redundant project (3), if we also have FilterProjectTransposeRule in
            // the rule set, it will also trigger infinite match of the ProjectMergeRule
            // for project (1) and (3).
            return
        }
        val builder: RelBuilder = call.builder()
        val topProject: RelNode
        topProject = if (origProject != null
            && (config.isWholeProject() || config.isWholeFilter())
        ) {
            builder.push(input)
            val set: Set<RexNode> = LinkedHashSet()
            val refCollector: RelOptUtil.InputFinder = InputFinder()
            if (config.isWholeFilter()) {
                set.add(filter.getCondition())
            } else {
                filter.getCondition().accept(refCollector)
            }
            if (config.isWholeProject()) {
                set.addAll(origProject.getProjects())
            } else {
                refCollector.visitEach(origProject.getProjects())
            }

            // Build a list with inputRefs, in order, first, then other expressions.
            val list: List<RexNode> = ArrayList()
            val refs: ImmutableBitSet = refCollector.build()
            for (field in builder.fields()) {
                if (refs.get((field as RexInputRef).getIndex()) || set.contains(field)) {
                    list.add(field)
                }
            }
            set.removeAll(list)
            list.addAll(set)
            builder.project(list)
            val replacer = Replacer(list, builder)
            builder.filter(replacer.visit(filter.getCondition()))
            builder.project(
                replacer.visitList(origProject.getProjects()),
                origProject.getRowType().getFieldNames()
            )
            builder.build()
        } else {
            // The traditional mode of operation of this rule: push down field
            // references. The effect is similar to RelFieldTrimmer.
            val pushProjector = PushProjector(
                origProject, origFilter, input,
                config.preserveExprCondition(), builder
            )
            pushProjector.convertProject(null)
        }
        if (topProject != null) {
            call.transformTo(topProject)
        }
    }

    /** Replaces whole expressions, or parts of an expression, with references to
     * expressions computed by an underlying Project.  */
    private class Replacer internal constructor(exprs: Iterable<RexNode?>, relBuilder: RelBuilder) : RexShuttle() {
        val map: ImmutableMap<RexNode, Integer>
        val relBuilder: RelBuilder

        init {
            this.relBuilder = relBuilder
            val b: ImmutableMap.Builder<RexNode, Integer> = ImmutableMap.builder()
            var i = 0
            for (expr in exprs) {
                b.put(expr, i++)
            }
            map = b.build()
        }

        fun visit(e: RexNode): RexNode {
            val i: Integer = map.get(e)
            return if (i != null) {
                relBuilder.field(i)
            } else e.accept(this)
        }

        @Override
        fun visitList(
            exprs: Iterable<RexNode?>,
            out: List<RexNode?>
        ) {
            for (expr in exprs) {
                out.add(visit(expr))
            }
        }

        @Override
        fun visitList(
            exprs: List<RexNode?>,
            update: @Nullable BooleanArray?
        ): List<RexNode> {
            val clonedOperands: ImmutableList.Builder<RexNode> = ImmutableList.builder()
            for (operand in exprs) {
                val clonedOperand: RexNode = visit(operand)
                if (clonedOperand !== operand && update != null) {
                    update[0] = true
                }
                clonedOperands.add(clonedOperand)
            }
            return clonedOperands.build()
        }
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): ProjectFilterTransposeRule? {
            return ProjectFilterTransposeRule(this)
        }

        /** Expressions that should be preserved in the projection.  */
        @Value.Default
        fun preserveExprCondition(): PushProjector.ExprCondition? {
            return PushProjector.ExprCondition { expr -> false }
        }

        /** Sets [.preserveExprCondition].  */
        fun withPreserveExprCondition(condition: PushProjector.ExprCondition?): Config?

        /** Whether to push whole expressions from the project;
         * if false (the default), only pushes references.  */
        @get:Value.Default
        val isWholeProject: Boolean
            get() = false

        /** Sets [.isWholeProject].  */
        fun withWholeProject(wholeProject: Boolean): Config

        /** Whether to push whole expressions from the filter;
         * if false (the default), only pushes references.  */
        @get:Value.Default
        val isWholeFilter: Boolean
            get() = false

        /** Sets [.isWholeFilter].  */
        fun withWholeFilter(wholeFilter: Boolean): Config

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            projectClass: Class<out Project?>?,
            filterClass: Class<out Filter?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(projectClass).oneInput { b1 -> b1.operand(filterClass).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        @Override
        @Value.Default
        fun operandSupplier(): OperandTransform? {
            return OperandTransform { b0 ->
                b0.operand(LogicalProject::class.java).oneInput { b1 ->
                    b1.operand(
                        LogicalFilter::class.java
                    ).anyInputs()
                }
            }
        }

        /** Defines an operand tree for the given 3 classes.  */
        fun withOperandFor(
            projectClass: Class<out Project?>?,
            filterClass: Class<out Filter?>?,
            inputClass: Class<out RelNode?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(projectClass)
                    .oneInput { b1 -> b1.operand(filterClass).oneInput { b2 -> b2.operand(inputClass).anyInputs() } }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableProjectFilterTransposeRule.Config.of()
            val PROJECT = DEFAULT.withWholeProject(true)
            val PROJECT_FILTER = PROJECT.withWholeFilter(true)
        }
    }
}
