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
package org.apache.calcite.rel.rules.materialize

import org.apache.calcite.plan.hep.HepPlanner
import org.apache.calcite.plan.hep.HepProgram
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexSimplify
import org.apache.calcite.rex.RexTableInputRef.RelTableRef
import org.apache.calcite.rex.RexUtil
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.Pair
import com.google.common.collect.BiMap
import com.google.common.collect.ImmutableList
import com.google.common.collect.Multimap
import java.util.ArrayList
import java.util.Collection
import java.util.HashSet
import java.util.List
import java.util.Set

/** Materialized view rewriting for join.
 *
 * @param <C> Configuration type
</C> */
abstract class MaterializedViewJoinRule<C : MaterializedViewRule.Config?>
/** Creates a MaterializedViewJoinRule.  */
internal constructor(config: C) : MaterializedViewRule<C>(config) {
    @Override
    protected override fun isValidPlan(
        @Nullable topProject: Project?, node: RelNode?,
        mq: RelMetadataQuery?
    ): Boolean {
        return isValidRelNodePlan(node, mq)
    }

    @Override
    @Nullable
    protected fun compensateViewPartial(
        relBuilder: RelBuilder,
        rexBuilder: RexBuilder,
        mq: RelMetadataQuery,
        input: RelNode?,
        @Nullable topProject: Project?,
        node: RelNode,
        queryTableRefs: Set<RelTableRef?>,
        queryEC: EquivalenceClasses?,
        @Nullable topViewProject: Project?,
        viewNode: RelNode?,
        viewTableRefs: Set<RelTableRef>
    ): ViewPartialRewriting? {
        // We only create the rewriting in the minimal subtree of plan operators.
        // Otherwise we will produce many EQUAL rewritings at different levels of
        // the plan.
        // View: (A JOIN B) JOIN C
        // Query: (((A JOIN B) JOIN D) JOIN C) JOIN E
        // We produce it at:
        // ((A JOIN B) JOIN D) JOIN C
        // But not at:
        // (((A JOIN B) JOIN D) JOIN C) JOIN E
        if (config.fastBailOut()) {
            for (joinInput in node.getInputs()) {
                val tableReferences: Set<RelTableRef> = mq.getTableReferences(joinInput)
                if (tableReferences == null || tableReferences.containsAll(viewTableRefs)) {
                    return null
                }
            }
        }

        // Extract tables that are in the query and not in the view
        val extraTableRefs: Set<RelTableRef> = HashSet()
        for (tRef in queryTableRefs) {
            if (!viewTableRefs.contains(tRef)) {
                // Add to extra tables if table is not part of the view
                extraTableRefs.add(tRef)
            }
        }

        // Rewrite the view and the view plan. We only need to add the missing
        // tables on top of the view and view plan using a cartesian product.
        // Then the rest of the rewriting algorithm can be executed in the same
        // fashion, and if there are predicates between the existing and missing
        // tables, the rewriting algorithm will enforce them.
        val nodeTypes: Multimap<Class<out RelNode?>, RelNode> = mq.getNodeTypes(node) ?: return null
        val tableScanNodes: Collection<RelNode> = nodeTypes.get(TableScan::class.java)
        val newRels: List<RelNode> = ArrayList()
        for (tRef in extraTableRefs) {
            var i = 0
            for (relNode in tableScanNodes) {
                val scan: TableScan = relNode as TableScan
                if (tRef.getQualifiedName().equals(scan.getTable().getQualifiedName())) {
                    if (tRef.getEntityNumber() === i++) {
                        newRels.add(relNode)
                        break
                    }
                }
            }
        }
        assert(extraTableRefs.size() === newRels.size())
        relBuilder.push(input)
        for (newRel in newRels) {
            // Add to the view
            relBuilder.push(newRel)
            relBuilder.join(JoinRelType.INNER, rexBuilder.makeLiteral(true))
        }
        val newView: RelNode = relBuilder.build()
        relBuilder.push(if (topViewProject != null) topViewProject else viewNode)
        for (newRel in newRels) {
            // Add to the view plan
            relBuilder.push(newRel)
            relBuilder.join(JoinRelType.INNER, rexBuilder.makeLiteral(true))
        }
        val newViewNode: RelNode = relBuilder.build()
        return ViewPartialRewriting.of(newView, null, newViewNode)
    }

    @Override
    @Nullable
    protected override fun rewriteQuery(
        relBuilder: RelBuilder,
        rexBuilder: RexBuilder?,
        simplify: RexSimplify,
        mq: RelMetadataQuery?,
        compensationColumnsEquiPred: RexNode,
        otherCompensationPred: RexNode,
        @Nullable topProject: Project?,
        node: RelNode,
        viewToQueryTableMapping: BiMap<RelTableRef?, RelTableRef?>,
        viewEC: EquivalenceClasses?, queryEC: EquivalenceClasses?
    ): RelNode? {
        // Our target node is the node below the root, which should have the maximum
        // number of available expressions in the tree in order to maximize our
        // number of rewritings.
        // We create a project on top. If the program is available, we execute
        // it to maximize rewriting opportunities. For instance, a program might
        // pull up all the expressions that are below the aggregate so we can
        // introduce compensation filters easily. This is important depending on
        // the planner strategy.
        var compensationColumnsEquiPred: RexNode = compensationColumnsEquiPred
        var otherCompensationPred: RexNode = otherCompensationPred
        var newNode: RelNode = node
        var target: RelNode = node
        val unionRewritingPullProgram: HepProgram = config.unionRewritingPullProgram()
        if (unionRewritingPullProgram != null) {
            val tmpPlanner = HepPlanner(unionRewritingPullProgram)
            tmpPlanner.setRoot(newNode)
            newNode = tmpPlanner.findBestExp()
            target = newNode.getInput(0)
        }

        // All columns required by compensating predicates must be contained
        // in the query.
        val queryExprs: List<RexNode> = extractReferences(rexBuilder, target)
        if (!compensationColumnsEquiPred.isAlwaysTrue()) {
            val newCompensationColumnsEquiPred: RexNode = rewriteExpression(
                rexBuilder, mq,
                target, target, queryExprs, viewToQueryTableMapping.inverse(), queryEC!!, false,
                compensationColumnsEquiPred
            )
                ?: // Skip it
                return null
            compensationColumnsEquiPred = newCompensationColumnsEquiPred
        }
        // For the rest, we use the query equivalence classes
        if (!otherCompensationPred.isAlwaysTrue()) {
            val newOtherCompensationPred: RexNode = rewriteExpression(
                rexBuilder, mq,
                target, target, queryExprs, viewToQueryTableMapping.inverse(), viewEC!!, true,
                otherCompensationPred
            )
                ?: // Skip it
                return null
            otherCompensationPred = newOtherCompensationPred
        }
        val queryCompensationPred: RexNode = RexUtil.not(
            RexUtil.composeConjunction(
                rexBuilder,
                ImmutableList.of(
                    compensationColumnsEquiPred,
                    otherCompensationPred
                )
            )
        )

        // Generate query rewriting.
        var rewrittenPlan: RelNode = relBuilder
            .push(target)
            .filter(simplify.simplifyUnknownAsFalse(queryCompensationPred))
            .build()
        if (unionRewritingPullProgram != null) {
            rewrittenPlan = newNode.copy(
                newNode.getTraitSet(), ImmutableList.of(rewrittenPlan)
            )
        }
        return if (topProject != null) {
            topProject.copy(topProject.getTraitSet(), ImmutableList.of(rewrittenPlan))
        } else rewrittenPlan
    }

    @Override
    @Nullable
    protected override fun createUnion(
        relBuilder: RelBuilder, rexBuilder: RexBuilder,
        @Nullable topProject: RelNode?, unionInputQuery: RelNode, unionInputView: RelNode?
    ): RelNode {
        relBuilder.push(unionInputQuery)
        relBuilder.push(unionInputView)
        relBuilder.union(true)
        val exprList: List<RexNode> = ArrayList(relBuilder.peek().getRowType().getFieldCount())
        val nameList: List<String> = ArrayList(relBuilder.peek().getRowType().getFieldCount())
        for (i in 0 until relBuilder.peek().getRowType().getFieldCount()) {
            // We can take unionInputQuery as it is query based.
            val field: RelDataTypeField = unionInputQuery.getRowType().getFieldList().get(i)
            exprList.add(
                rexBuilder.ensureType(
                    field.getType(),
                    rexBuilder.makeInputRef(relBuilder.peek(), i),
                    true
                )
            )
            nameList.add(field.getName())
        }
        relBuilder.project(exprList, nameList)
        return relBuilder.build()
    }

    @Override
    @Nullable
    protected fun rewriteView(
        relBuilder: RelBuilder,
        rexBuilder: RexBuilder?,
        simplify: RexSimplify?,
        mq: RelMetadataQuery,
        matchModality: MatchModality?,
        unionRewriting: Boolean,
        input: RelNode?,
        @Nullable topProject: Project?,
        node: RelNode,
        @Nullable topViewProject: Project?,
        viewNode: RelNode?,
        queryToViewTableMapping: BiMap<RelTableRef?, RelTableRef?>,
        queryEC: EquivalenceClasses
    ): RelNode? {
        val exprs: List<RexNode> =
            if (topProject == null) extractReferences(rexBuilder, node) else topProject.getProjects()
        val exprsLineage: List<RexNode> = ArrayList(exprs.size())
        for (expr in exprs) {
            val lineages: Set<RexNode> = mq.getExpressionLineage(node, expr)
                ?: // Bail out
                return null
            if (lineages.size() !== 1) {
                throw IllegalStateException(
                    "We only support project - filter - join, "
                            + "thus expression lineage should map to a single expression, got: '"
                            + lineages + "' for expr '" + expr + "' in node '" + node + "'"
                )
            }
            // Rewrite expr. Take first element from the corresponding equivalence class
            // (no need to swap the table references following the table mapping)
            exprsLineage.add(
                RexUtil.swapColumnReferences(
                    rexBuilder,
                    lineages.iterator().next(), queryEC.getEquivalenceClassesMap()
                )
            )
        }
        val viewExprs: List<RexNode> =
            if (topViewProject == null) extractReferences(rexBuilder, viewNode) else topViewProject.getProjects()
        val rewrittenExprs: List<RexNode> = rewriteExpressions(
            rexBuilder, mq, input, viewNode, viewExprs,
            queryToViewTableMapping.inverse(), queryEC, true, exprsLineage
        ) ?: return null
        return relBuilder
            .push(input)
            .project(rewrittenExprs)
            .convert(if (topProject != null) topProject.getRowType() else node.getRowType(), false)
            .build()
    }

    @Override
    override fun pushFilterToOriginalViewPlan(
        builder: RelBuilder?,
        @Nullable topViewProject: RelNode?, viewNode: RelNode?, cond: RexNode?
    ): Pair<RelNode, RelNode> {
        // Nothing to do
        return Pair.of(topViewProject, viewNode)
    }
}
