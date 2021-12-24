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
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.logical.LogicalCalc
import org.apache.calcite.rel.logical.LogicalWindow
import org.apache.calcite.rex.RexBiVisitorImpl
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexDynamicParam
import org.apache.calcite.rex.RexFieldAccess
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.rex.RexLocalRef
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexOver
import org.apache.calcite.rex.RexProgram
import org.apache.calcite.rex.RexWindow
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.util.ImmutableIntList
import org.apache.calcite.util.Pair
import org.apache.calcite.util.Util
import org.apache.calcite.util.graph.DefaultDirectedGraph
import org.apache.calcite.util.graph.DefaultEdge
import org.apache.calcite.util.graph.DirectedGraph
import org.apache.calcite.util.graph.TopologicalOrderIterator
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import org.immutables.value.Value
import java.util.ArrayDeque
import java.util.ArrayList
import java.util.Deque
import java.util.HashSet
import java.util.List
import java.util.Set

/**
 * Planner rule that slices a
 * [org.apache.calcite.rel.core.Project]
 * into sections which contain windowed
 * aggregate functions and sections which do not.
 *
 *
 * The sections which contain windowed agg functions become instances of
 * [org.apache.calcite.rel.logical.LogicalWindow].
 * If the [org.apache.calcite.rel.logical.LogicalCalc] does not contain
 * any windowed agg functions, does nothing.
 *
 *
 * There is also a variant that matches
 * [org.apache.calcite.rel.core.Calc] rather than `Project`.
 */
abstract class ProjectToWindowRule
/** Creates a ProjectToWindowRule.  */
protected constructor(config: Config?) : RelRule<ProjectToWindowRule.Config?>(config), TransformationRule {
    /**
     * Instance of the rule that applies to a
     * [org.apache.calcite.rel.core.Calc] that contains
     * windowed aggregates and converts it into a mixture of
     * [org.apache.calcite.rel.logical.LogicalWindow] and `Calc`.
     *
     * @see CoreRules.CALC_TO_WINDOW
     */
    class CalcToWindowRule
    /** Creates a CalcToWindowRule.  */
    protected constructor(config: CalcToWindowRuleConfig?) : ProjectToWindowRule(config) {
        @Override
        fun onMatch(call: RelOptRuleCall) {
            val calc: Calc = call.rel(0)
            assert(calc.containsOver())
            val transform: CalcRelSplitter = WindowedAggRelSplitter(calc, call.builder())
            val newRel: RelNode = transform.execute()
            call.transformTo(newRel)
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface CalcToWindowRuleConfig : Config {
            @Override
            override fun toRule(): CalcToWindowRule {
                return CalcToWindowRule(this)
            }

            companion object {
                val DEFAULT: CalcToWindowRuleConfig = ImmutableCalcToWindowRuleConfig.of()
                    .withOperandSupplier { b ->
                        b.operand(Calc::class.java)
                            .predicate(Calc::containsOver)
                            .anyInputs()
                    }
                    .withDescription("ProjectToWindowRule")
            }
        }
    }

    /**
     * Instance of the rule that can be applied to a
     * [org.apache.calcite.rel.core.Project] and that produces, in turn,
     * a mixture of `LogicalProject`
     * and [org.apache.calcite.rel.logical.LogicalWindow].
     *
     * @see CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW
     */
    class ProjectToLogicalProjectAndWindowRule
    /** Creates a ProjectToLogicalProjectAndWindowRule.  */
    protected constructor(
        config: ProjectToLogicalProjectAndWindowRuleConfig?
    ) : ProjectToWindowRule(config) {
        @Deprecated // to be removed before 2.0
        constructor(
            relBuilderFactory: RelBuilderFactory?
        ) : this(
            ProjectToLogicalProjectAndWindowRuleConfig.DEFAULT
                .withRelBuilderFactory(relBuilderFactory)
                .`as`(ProjectToLogicalProjectAndWindowRuleConfig::class.java)
        ) {
        }

        @Override
        fun onMatch(call: RelOptRuleCall) {
            val project: Project = call.rel(0)
            assert(project.containsOver())
            val input: RelNode = project.getInput()
            val program: RexProgram = RexProgram.create(
                input.getRowType(),
                project.getProjects(),
                null,
                project.getRowType(),
                project.getCluster().getRexBuilder()
            )
            // temporary LogicalCalc, never registered
            val calc: LogicalCalc = LogicalCalc.create(input, program)
            val transform: CalcRelSplitter = object : WindowedAggRelSplitter(
                calc,
                call.builder()
            ) {
                @Override
                protected fun handle(rel: RelNode): RelNode {
                    if (rel !is LogicalCalc) {
                        return rel
                    }
                    val calc: LogicalCalc = rel as LogicalCalc
                    val program: RexProgram = calc.getProgram()
                    relBuilder.push(calc.getInput())
                    if (program.getCondition() != null) {
                        relBuilder.filter(
                            program.expandLocalRef(program.getCondition())
                        )
                    }
                    if (!program.projectsOnlyIdentity()) {
                        relBuilder.project(
                            Util.(this)(program.getProjectList(), program::expandLocalRef),
                            calc.getRowType().getFieldNames()
                        )
                    }
                    return relBuilder.build()
                }
            }
            val newRel: RelNode = transform.execute()
            call.transformTo(newRel)
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface ProjectToLogicalProjectAndWindowRuleConfig : Config {
            @Override
            override fun toRule(): ProjectToLogicalProjectAndWindowRule {
                return ProjectToLogicalProjectAndWindowRule(this)
            }

            companion object {
                val DEFAULT: ProjectToLogicalProjectAndWindowRuleConfig =
                    ImmutableProjectToLogicalProjectAndWindowRuleConfig.of()
                        .withOperandSupplier { b ->
                            b.operand(Project::class.java)
                                .predicate(Project::containsOver)
                                .anyInputs()
                        }
                        .withDescription("ProjectToWindowRule:project")
            }
        }
    }

    /**
     * Splitter that distinguishes between windowed aggregation expressions
     * (calls to [RexOver]) and ordinary expressions.
     */
    internal class WindowedAggRelSplitter(calc: Calc?, relBuilder: RelBuilder?) :
        CalcRelSplitter(calc, relBuilder, REL_TYPES) {// Check the second condition

        // This RexOver cannot be added into any existing cohort
// Check the first condition// If we can found an existing cohort which satisfies the two conditions,
        // we will add this RexOver into that cohort
        // Two RexOver will be put in the same cohort
        // if the following conditions are satisfied
        // (1). They have the same RexWindow
        // (2). They are not dependent on each other
        @get:Override
        protected val cohorts: List<Set<Any>>
            protected get() {
                // Two RexOver will be put in the same cohort
                // if the following conditions are satisfied
                // (1). They have the same RexWindow
                // (2). They are not dependent on each other
                val exprs: List<RexNode> = this.program.getExprList()
                val graph: DirectedGraph<Integer, DefaultEdge> = createGraphFromExpression(exprs)
                val rank: List<Integer> = getRank(graph)
                val windowToIndices: List<Pair<RexWindow, Set<Integer>>> = ArrayList()
                for (i in 0 until exprs.size()) {
                    val expr: RexNode = exprs[i]
                    if (expr is RexOver) {
                        val over: RexOver = expr as RexOver

                        // If we can found an existing cohort which satisfies the two conditions,
                        // we will add this RexOver into that cohort
                        var isFound = false
                        for (pair in windowToIndices) {
                            // Check the first condition
                            if (pair.left.equals(over.getWindow())) {
                                // Check the second condition
                                var hasDependency = false
                                for (ordinal in pair.right) {
                                    if (isDependent(graph, rank, ordinal, i)) {
                                        hasDependency = true
                                        break
                                    }
                                }
                                if (!hasDependency) {
                                    pair.right.add(i)
                                    isFound = true
                                    break
                                }
                            }
                        }

                        // This RexOver cannot be added into any existing cohort
                        if (!isFound) {
                            val newSet: Set<Integer> = HashSet(ImmutableList.of(i))
                            windowToIndices.add(Pair.of(over.getWindow(), newSet))
                        }
                    }
                }
                val cohorts: List<Set<Integer>> = ArrayList()
                for (pair in windowToIndices) {
                    cohorts.add(pair.right)
                }
                return cohorts
            }

        companion object {
            private val REL_TYPES: Array<RelType> = arrayOf(
                object : RelType("CalcRelType") {
                    @Override
                    protected fun canImplement(field: RexFieldAccess?): Boolean {
                        return true
                    }

                    @Override
                    protected fun canImplement(param: RexDynamicParam?): Boolean {
                        return true
                    }

                    @Override
                    protected fun canImplement(literal: RexLiteral?): Boolean {
                        return true
                    }

                    @Override
                    protected fun canImplement(call: RexCall?): Boolean {
                        return call !is RexOver
                    }

                    @Override
                    protected fun makeRel(
                        cluster: RelOptCluster,
                        traitSet: RelTraitSet?, relBuilder: RelBuilder?, input: RelNode?,
                        program: RexProgram
                    ): RelNode {
                        var program: RexProgram = program
                        assert(!program.containsAggs())
                        program = program.normalize(cluster.getRexBuilder(), null)
                        return super.makeRel(
                            cluster, traitSet, relBuilder, input,
                            program
                        )
                    }
                },
                object : RelType("WinAggRelType") {
                    @Override
                    protected fun canImplement(field: RexFieldAccess?): Boolean {
                        return false
                    }

                    @Override
                    protected fun canImplement(param: RexDynamicParam?): Boolean {
                        return false
                    }

                    @Override
                    protected fun canImplement(literal: RexLiteral?): Boolean {
                        return false
                    }

                    @Override
                    protected fun canImplement(call: RexCall?): Boolean {
                        return call is RexOver
                    }

                    @Override
                    protected fun supportsCondition(): Boolean {
                        return false
                    }

                    @Override
                    protected fun makeRel(
                        cluster: RelOptCluster?, traitSet: RelTraitSet?,
                        relBuilder: RelBuilder?, input: RelNode?, program: RexProgram
                    ): RelNode {
                        Preconditions.checkArgument(
                            program.getCondition() == null,
                            "WindowedAggregateRel cannot accept a condition"
                        )
                        return LogicalWindow.create(
                            cluster, traitSet, relBuilder, input,
                            program
                        )
                    }
                }
            )

            private fun isDependent(
                graph: DirectedGraph<Integer, DefaultEdge>,
                rank: List<Integer>,
                ordinal1: Int,
                ordinal2: Int
            ): Boolean {
                if (rank[ordinal2] > rank[ordinal1]) {
                    return isDependent(graph, rank, ordinal2, ordinal1)
                }

                // Check if the expression in ordinal1
                // could depend on expression in ordinal2 by Depth-First-Search
                val dfs: Deque<Integer> = ArrayDeque()
                val visited: Set<Integer> = HashSet()
                dfs.push(ordinal2)
                while (!dfs.isEmpty()) {
                    val source: Int = dfs.pop()
                    if (visited.contains(source)) {
                        continue
                    }
                    if (source == ordinal1) {
                        return true
                    }
                    visited.add(source)
                    for (e in graph.getOutwardEdges(source)) {
                        val target = e.target as Int
                        if (rank[target] <= rank[ordinal1]) {
                            dfs.push(target)
                        }
                    }
                }
                return false
            }

            private fun getRank(graph: DirectedGraph<Integer, DefaultEdge>): List<Integer> {
                val rankArr = IntArray(graph.vertexSet().size())
                var rank = 0
                for (i in TopologicalOrderIterator.of(graph)) {
                    rankArr[i] = rank++
                }
                return ImmutableIntList.of(rankArr)
            }

            private fun createGraphFromExpression(
                exprs: List<RexNode>
            ): DirectedGraph<Integer, DefaultEdge> {
                val graph: DirectedGraph<Integer, DefaultEdge> = DefaultDirectedGraph.create()
                for (i in 0 until exprs.size()) {
                    graph.addVertex(i)
                }
                object : RexBiVisitorImpl<Void?, Integer?>(true) {
                    @Override
                    fun visitLocalRef(localRef: RexLocalRef, i: Integer?): Void? {
                        graph.addEdge(localRef.getIndex(), i)
                        return null
                    }
                }.visitEachIndexed(exprs)
                assert(graph.vertexSet().size() === exprs.size())
                return graph
            }
        }
    }

    /** Rule configuration.  */
    interface Config : RelRule.Config {
        @Override
        fun toRule(): ProjectToWindowRule?
    }
}
