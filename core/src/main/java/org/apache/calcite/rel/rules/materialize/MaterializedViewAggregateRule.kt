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

import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.calcite.linq4j.Ord
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.hep.HepPlanner
import org.apache.calcite.plan.hep.HepProgram
import org.apache.calcite.plan.hep.HepProgramBuilder
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule
import org.apache.calcite.rel.rules.CoreRules
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule
import org.apache.calcite.rel.rules.FilterProjectTransposeRule
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexPermuteInputsShuttle
import org.apache.calcite.rex.RexSimplify
import org.apache.calcite.rex.RexTableInputRef
import org.apache.calcite.rex.RexTableInputRef.RelTableRef
import org.apache.calcite.rex.RexUtil
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.SqlFunction
import org.apache.calcite.sql.`fun`.SqlMinMaxAggFunction
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilder.AggCall
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.util.Pair
import org.apache.calcite.util.mapping.Mapping
import org.apache.calcite.util.mapping.MappingType
import org.apache.calcite.util.mapping.Mappings
import com.google.common.base.Preconditions
import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.BiMap
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMultimap
import com.google.common.collect.Iterables
import com.google.common.collect.Multimap
import org.immutables.value.Value
import java.util.ArrayList
import java.util.Collection
import java.util.HashSet
import java.util.LinkedHashSet
import java.util.List
import java.util.Map
import java.util.Set
import java.util.Objects.requireNonNull

/** Materialized view rewriting for aggregate.
 *
 * @param <C> Configuration type
</C> */
abstract class MaterializedViewAggregateRule<C : MaterializedViewAggregateRule.Config?>
/** Creates a MaterializedViewAggregateRule.  */
internal constructor(config: C) : MaterializedViewRule<C>(config) {
    @Override
    protected override fun isValidPlan(
        @Nullable topProject: Project?, node: RelNode,
        mq: RelMetadataQuery?
    ): Boolean {
        if (node !is Aggregate) {
            return false
        }
        val aggregate: Aggregate = node as Aggregate
        return if (aggregate.getGroupType() !== Aggregate.Group.SIMPLE) {
            // TODO: Rewriting with grouping sets not supported yet
            false
        } else isValidRelNodePlan(aggregate.getInput(), mq)
    }

    @Override
    @Nullable
    protected fun compensateViewPartial(
        relBuilder: RelBuilder,
        rexBuilder: RexBuilder,
        mq: RelMetadataQuery,
        input: RelNode?,
        @Nullable topProject: Project?,
        node: RelNode?,
        queryTableRefs: Set<RelTableRef?>,
        queryEC: EquivalenceClasses?,
        @Nullable topViewProject: Project?,
        viewNode: RelNode?,
        viewTableRefs: Set<RelTableRef?>
    ): ViewPartialRewriting? {
        // Modify view to join with missing tables and add Project on top to reorder columns.
        // In turn, modify view plan to join with missing tables before Aggregate operator,
        // change Aggregate operator to group by previous grouping columns and columns in
        // attached tables, and add a final Project on top.
        // We only need to add the missing tables on top of the view and view plan using
        // a cartesian product.
        // Then the rest of the rewriting algorithm can be executed in the same
        // fashion, and if there are predicates between the existing and missing
        // tables, the rewriting algorithm will enforce them.
        val extraTableRefs: Set<RelTableRef> = HashSet()
        for (tRef in queryTableRefs) {
            if (!viewTableRefs.contains(tRef)) {
                // Add to extra tables if table is not part of the view
                extraTableRefs.add(tRef)
            }
        }
        val nodeTypes: Multimap<Class<out RelNode?>, RelNode> = mq.getNodeTypes(node) ?: return null
        val tableScanNodes: Collection<RelNode> = nodeTypes.get(TableScan::class.java) ?: return null
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
        val aggregateViewNode: Aggregate = viewNode as Aggregate
        relBuilder.push(aggregateViewNode.getInput())
        var offset = 0
        for (newRel in newRels) {
            // Add to the view plan
            relBuilder.push(newRel)
            relBuilder.join(JoinRelType.INNER, rexBuilder.makeLiteral(true))
            offset += newRel.getRowType().getFieldCount()
        }
        // Modify aggregate: add grouping columns
        val groupSet: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        groupSet.addAll(aggregateViewNode.getGroupSet())
        groupSet.addAll(
            ImmutableBitSet.range(
                aggregateViewNode.getInput().getRowType().getFieldCount(),
                aggregateViewNode.getInput().getRowType().getFieldCount() + offset
            )
        )
        val newViewNode: Aggregate = aggregateViewNode.copy(
            aggregateViewNode.getTraitSet(), relBuilder.build(),
            groupSet.build(), null, aggregateViewNode.getAggCallList()
        )
        relBuilder.push(newViewNode)
        val nodes: List<RexNode> = ArrayList()
        val fieldNames: List<String> = ArrayList()
        if (topViewProject != null) {
            // Insert existing expressions (and shift aggregation arguments),
            // then append rest of columns
            val shiftMapping: Mappings.TargetMapping = Mappings.createShiftMapping(
                newViewNode.getRowType().getFieldCount(),
                0, 0, aggregateViewNode.getGroupCount(),
                newViewNode.getGroupCount(), aggregateViewNode.getGroupCount(),
                aggregateViewNode.getAggCallList().size()
            )
            for (i in 0 until topViewProject.getProjects().size()) {
                nodes.add(
                    topViewProject.getProjects().get(i).accept(
                        RexPermuteInputsShuttle(shiftMapping, newViewNode)
                    )
                )
                fieldNames.add(topViewProject.getRowType().getFieldNames().get(i))
            }
            for (i in aggregateViewNode.getRowType().getFieldCount() until newViewNode.getRowType().getFieldCount()) {
                val idx: Int = i - aggregateViewNode.getAggCallList().size()
                nodes.add(rexBuilder.makeInputRef(newViewNode, idx))
                fieldNames.add(newViewNode.getRowType().getFieldNames().get(idx))
            }
        } else {
            // Original grouping columns, aggregation columns, then new grouping columns
            for (i in 0 until newViewNode.getRowType().getFieldCount()) {
                var idx: Int
                idx = if (i < aggregateViewNode.getGroupCount()) {
                    i
                } else if (i < aggregateViewNode.getRowType().getFieldCount()) {
                    i + offset
                } else {
                    i - aggregateViewNode.getAggCallList().size()
                }
                nodes.add(rexBuilder.makeInputRef(newViewNode, idx))
                fieldNames.add(newViewNode.getRowType().getFieldNames().get(idx))
            }
        }
        relBuilder.project(nodes, fieldNames, true)
        val newTopViewProject: Project = relBuilder.build() as Project
        return ViewPartialRewriting.of(newView, newTopViewProject, newViewNode)
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
        queryToViewTableMapping: BiMap<RelTableRef?, RelTableRef?>,
        viewEC: EquivalenceClasses?, queryEC: EquivalenceClasses?
    ): RelNode? {
        var compensationColumnsEquiPred: RexNode = compensationColumnsEquiPred
        var otherCompensationPred: RexNode = otherCompensationPred
        val aggregate: Aggregate = node as Aggregate

        // Our target node is the node below the root, which should have the maximum
        // number of available expressions in the tree in order to maximize our
        // number of rewritings.
        // If the program is available, we execute it to maximize rewriting opportunities.
        // For instance, a program might pull up all the expressions that are below the
        // aggregate so we can introduce compensation filters easily. This is important
        // depending on the planner strategy.
        var newAggregateInput: RelNode = aggregate.getInput(0)
        var target: RelNode = aggregate.getInput(0)
        val unionRewritingPullProgram: HepProgram = config.unionRewritingPullProgram()
        if (unionRewritingPullProgram != null) {
            val tmpPlanner = HepPlanner(unionRewritingPullProgram)
            tmpPlanner.setRoot(newAggregateInput)
            newAggregateInput = tmpPlanner.findBestExp()
            target = newAggregateInput.getInput(0)
        }

        // We need to check that all columns required by compensating predicates
        // are contained in the query.
        val queryExprs: List<RexNode> = extractReferences(rexBuilder, target)
        if (!compensationColumnsEquiPred.isAlwaysTrue()) {
            val newCompensationColumnsEquiPred: RexNode = rewriteExpression(
                rexBuilder, mq,
                target, target, queryExprs, queryToViewTableMapping, queryEC!!, false,
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
                target, target, queryExprs, queryToViewTableMapping, viewEC!!, true,
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
        val rewrittenPlan: RelNode = relBuilder
            .push(target)
            .filter(simplify.simplifyUnknownAsFalse(queryCompensationPred))
            .build()
        return if (config.unionRewritingPullProgram() != null) {
            aggregate.copy(
                aggregate.getTraitSet(),
                ImmutableList.of(
                    newAggregateInput.copy(
                        newAggregateInput.getTraitSet(),
                        ImmutableList.of(rewrittenPlan)
                    )
                )
            )
        } else aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(rewrittenPlan))
    }

    @Override
    @Nullable
    protected override fun createUnion(
        relBuilder: RelBuilder, rexBuilder: RexBuilder,
        @Nullable topProject: RelNode?, unionInputQuery: RelNode, unionInputView: RelNode?
    ): RelNode? {
        // Union
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
        // Rollup aggregate
        val aggregate: Aggregate = unionInputQuery as Aggregate
        val groupSet: ImmutableBitSet = ImmutableBitSet.range(aggregate.getGroupCount())
        val aggregateCalls: List<AggCall> = ArrayList()
        for (i in 0 until aggregate.getAggCallList().size()) {
            val aggCall: AggregateCall = aggregate.getAggCallList().get(i)
            if (aggCall.isDistinct()) {
                // Cannot ROLLUP distinct
                return null
            }
            val rollupAgg: SqlAggFunction = aggCall.getAggregation().getRollup()
                ?: // Cannot rollup this aggregate, bail out
                return null
            val operand: RexInputRef = rexBuilder.makeInputRef(
                relBuilder.peek(),
                aggregate.getGroupCount() + i
            )
            aggregateCalls.add(
                relBuilder.aggregateCall(rollupAgg, operand)
                    .distinct(aggCall.isDistinct())
                    .approximate(aggCall.isApproximate())
                    .`as`(aggCall.name)
            )
        }
        val prevNode: RelNode = relBuilder.peek()
        var result: RelNode = relBuilder
            .aggregate(relBuilder.groupKey(groupSet), aggregateCalls)
            .build()
        if (prevNode === result && groupSet.cardinality() !== result.getRowType().getFieldCount()) {
            // Aggregate was not inserted but we need to prune columns
            result = relBuilder
                .push(result)
                .project(relBuilder.fields(groupSet))
                .build()
        }
        return if (topProject != null) {
            // Top project
            topProject.copy(topProject.getTraitSet(), ImmutableList.of(result))
        } else result
        // Result
    }

    @Override
    @Nullable
    protected fun rewriteView(
        relBuilder: RelBuilder,
        rexBuilder: RexBuilder,
        simplify: RexSimplify,
        mq: RelMetadataQuery,
        matchModality: MatchModality,
        unionRewriting: Boolean,
        input: RelNode?,
        @Nullable topProject: Project?,
        node: RelNode,
        @Nullable topViewProject0: Project?,
        viewNode: RelNode,
        queryToViewTableMapping: BiMap<RelTableRef?, RelTableRef?>,
        queryEC: EquivalenceClasses
    ): RelNode? {
        val queryAggregate: Aggregate = node as Aggregate
        val viewAggregate: Aggregate = viewNode as Aggregate
        // Get group by references and aggregate call input references needed
        val indexes: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        val references: ImmutableBitSet?
        if (topProject != null && !unionRewriting) {
            // We have a Project on top, gather only what is needed
            val inputFinder: RelOptUtil.InputFinder = InputFinder(LinkedHashSet())
            inputFinder.visitEach(topProject.getProjects())
            references = inputFinder.build()
            for (i in 0 until queryAggregate.getGroupCount()) {
                indexes.set(queryAggregate.getGroupSet().nth(i))
            }
            for (i in 0 until queryAggregate.getAggCallList().size()) {
                if (references.get(queryAggregate.getGroupCount() + i)) {
                    for (inputIdx in queryAggregate.getAggCallList().get(i).getArgList()) {
                        indexes.set(inputIdx)
                    }
                }
            }
        } else {
            // No project on top, all of them are needed
            for (i in 0 until queryAggregate.getGroupCount()) {
                indexes.set(queryAggregate.getGroupSet().nth(i))
            }
            for (queryAggCall in queryAggregate.getAggCallList()) {
                for (inputIdx in queryAggCall.getArgList()) {
                    indexes.set(inputIdx)
                }
            }
            references = null
        }

        // Create mapping from query columns to view columns
        val rollupNodes: List<RexNode> = ArrayList()
        val m: Multimap<Integer, Integer> = generateMapping(
            rexBuilder, simplify, mq,
            queryAggregate.getInput(), viewAggregate.getInput(), indexes.build(),
            queryToViewTableMapping, queryEC, rollupNodes
        )
            ?: // Bail out
            return null

        // We could map all expressions. Create aggregate mapping.
        @SuppressWarnings("unused") val viewAggregateAdditionalFieldCount: Int = rollupNodes.size()
        val viewInputFieldCount: Int = viewAggregate.getInput().getRowType().getFieldCount()
        val viewInputDifferenceViewFieldCount: Int = viewAggregate.getRowType().getFieldCount() - viewInputFieldCount
        val viewAggregateTotalFieldCount: Int = viewAggregate.getRowType().getFieldCount() + rollupNodes.size()
        var forceRollup = false
        val aggregateMapping: Mapping = Mappings.create(
            MappingType.FUNCTION,
            queryAggregate.getRowType().getFieldCount(), viewAggregateTotalFieldCount
        )
        for (i in 0 until queryAggregate.getGroupCount()) {
            val c: Collection<Integer> = m.get(queryAggregate.getGroupSet().nth(i))
            for (j in c) {
                if (j >= viewAggregate.getInput().getRowType().getFieldCount()) {
                    // This is one of the rollup columns
                    aggregateMapping.set(i, j + viewInputDifferenceViewFieldCount)
                    forceRollup = true
                } else {
                    val targetIdx: Int = viewAggregate.getGroupSet().indexOf(j)
                    if (targetIdx == -1) {
                        continue
                    }
                    aggregateMapping.set(i, targetIdx)
                }
                break
            }
            if (aggregateMapping.getTargetOpt(i) === -1) {
                // It is not part of group by, we bail out
                return null
            }
        }
        var containsDistinctAgg = false
        for (ord in Ord.zip(queryAggregate.getAggCallList())) {
            if (references != null
                && !references.get(queryAggregate.getGroupCount() + ord.i)
            ) {
                // Ignore
                continue
            }
            val queryAggCall: AggregateCall = ord.e
            if (queryAggCall.filterArg >= 0) {
                // Not supported currently
                return null
            }
            val queryAggCallIndexes: List<Integer> = ArrayList()
            for (aggCallIdx in queryAggCall.getArgList()) {
                queryAggCallIndexes.add(m.get(aggCallIdx).iterator().next())
            }
            for (j in 0 until viewAggregate.getAggCallList().size()) {
                val viewAggCall: AggregateCall = viewAggregate.getAggCallList().get(j)
                if (queryAggCall.getAggregation().getKind() !== viewAggCall.getAggregation()
                        .getKind() || queryAggCall.isDistinct() !== viewAggCall.isDistinct() || queryAggCall.getArgList()
                        .size() !== viewAggCall.getArgList()
                        .size() || queryAggCall.getType() !== viewAggCall.getType() || viewAggCall.filterArg >= 0
                ) {
                    // Continue
                    continue
                }
                if (!queryAggCallIndexes.equals(viewAggCall.getArgList())) {
                    // Continue
                    continue
                }
                aggregateMapping.set(
                    queryAggregate.getGroupCount() + ord.i,
                    viewAggregate.getGroupCount() + j
                )
                if (queryAggCall.isDistinct()) {
                    containsDistinctAgg = true
                }
                break
            }
        }

        // To simplify things, create an identity topViewProject if not present.
        val topViewProject: Project = if (topViewProject0 != null) topViewProject0 else relBuilder.push(viewNode)
            .project(relBuilder.fields(), ImmutableList.of(), true)
            .build() as Project

        // Generate result rewriting
        val additionalViewExprs: List<RexNode> = ArrayList()

        // Multimap is required since a column in the materialized view's project
        // could map to multiple columns in the target query
        val rewritingMapping: ImmutableMultimap<Integer, Integer>?
        relBuilder.push(input)
        // We create view expressions that will be used in a Project on top of the
        // view in case we need to rollup the expression
        val inputViewExprs: List<RexNode> = ArrayList(relBuilder.fields())
        if (forceRollup
            || queryAggregate.getGroupCount() !== viewAggregate.getGroupCount() || matchModality === MatchModality.VIEW_PARTIAL
        ) {
            if (containsDistinctAgg) {
                // Cannot rollup DISTINCT aggregate
                return null
            }
            // Target is coarser level of aggregation. Generate an aggregate.
            val rewritingMappingB: ImmutableMultimap.Builder<Integer, Integer> = ImmutableMultimap.builder()
            val groupSetB: ImmutableBitSet.Builder = ImmutableBitSet.builder()
            for (i in 0 until queryAggregate.getGroupCount()) {
                val targetIdx: Int = aggregateMapping.getTargetOpt(i)
                if (targetIdx == -1) {
                    // No matching group by column, we bail out
                    return null
                }
                if (targetIdx >= viewAggregate.getRowType().getFieldCount()) {
                    val targetNode: RexNode = rollupNodes[targetIdx - viewInputFieldCount
                            - viewInputDifferenceViewFieldCount]
                    // We need to rollup this expression
                    val exprsLineage: Multimap<RexNode, Integer> = ArrayListMultimap.create()
                    for (r in RelOptUtil.InputFinder.bits(targetNode)) {
                        val j = find(viewNode, r)
                        val k = find(topViewProject, j)
                        if (k < 0) {
                            // No matching column needed for computed expression, bail out
                            return null
                        }
                        val ref: RexInputRef = relBuilder.with(viewNode.getInput(0)) { b -> b.field(r) }
                        exprsLineage.put(ref, k)
                    }
                    // We create the new node pointing to the index
                    groupSetB.set(inputViewExprs.size())
                    rewritingMappingB.put(inputViewExprs.size(), i)
                    additionalViewExprs.add(
                        RexInputRef(targetIdx, targetNode.getType())
                    )
                    // We need to create the rollup expression
                    val rollupExpression: RexNode = requireNonNull(
                        shuttleReferences(rexBuilder, targetNode, exprsLineage)
                    ) {
                        ("shuttleReferences produced null for targetNode="
                                + targetNode + ", exprsLineage=" + exprsLineage)
                    }
                    inputViewExprs.add(rollupExpression)
                } else {
                    // This expression should be referenced directly
                    val k = find(topViewProject, targetIdx)
                    if (k < 0) {
                        // No matching group by column, we bail out
                        return null
                    }
                    groupSetB.set(k)
                    rewritingMappingB.put(k, i)
                }
            }
            val groupSet: ImmutableBitSet = groupSetB.build()
            val aggregateCalls: List<AggCall> = ArrayList()
            for (ord in Ord.zip(queryAggregate.getAggCallList())) {
                val sourceIdx: Int = queryAggregate.getGroupCount() + ord.i
                if (references != null && !references.get(sourceIdx)) {
                    // Ignore
                    continue
                }
                val targetIdx: Int = aggregateMapping.getTargetOpt(sourceIdx)
                if (targetIdx < 0) {
                    // No matching aggregation column, we bail out
                    return null
                }
                val k = find(topViewProject, targetIdx)
                if (k < 0) {
                    // No matching aggregation column, we bail out
                    return null
                }
                val queryAggCall: AggregateCall = ord.e
                val rollupAgg: SqlAggFunction = queryAggCall.getAggregation().getRollup()
                    ?: // Cannot rollup this aggregate, bail out
                    return null
                rewritingMappingB.put(
                    k,
                    queryAggregate.getGroupCount() + aggregateCalls.size()
                )
                val operand: RexInputRef = rexBuilder.makeInputRef(input, k)
                aggregateCalls.add(
                    relBuilder.aggregateCall(rollupAgg, operand)
                        .approximate(queryAggCall.isApproximate())
                        .distinct(queryAggCall.isDistinct())
                        .`as`(queryAggCall.name)
                )
            }
            // Create aggregate on top of input
            val prevNode: RelNode = relBuilder.peek()
            if (inputViewExprs.size() > prevNode.getRowType().getFieldCount()) {
                relBuilder.project(inputViewExprs)
            }
            relBuilder
                .aggregate(relBuilder.groupKey(groupSet), aggregateCalls)
            if (prevNode === relBuilder.peek()
                && groupSet.cardinality() !== relBuilder.peek().getRowType().getFieldCount()
            ) {
                // Aggregate was not inserted but we need to prune columns
                relBuilder.project(relBuilder.fields(groupSet))
            }
            // We introduce a project on top, as group by columns order is lost.
            // Multimap is required since a column in the materialized view's project
            // could map to multiple columns in the target query.
            rewritingMapping = rewritingMappingB.build()
            val inverseMapping: ImmutableMultimap<Integer, Integer> = rewritingMapping.inverse()
            val projects: List<RexNode> = ArrayList()
            val addedProjects: ImmutableBitSet.Builder = ImmutableBitSet.builder()
            for (i in 0 until queryAggregate.getGroupCount()) {
                val pos: Int = groupSet.indexOf(inverseMapping.get(i).iterator().next())
                addedProjects.set(pos)
                projects.add(relBuilder.field(pos))
            }
            val projectedCols: ImmutableBitSet = addedProjects.build()
            // We add aggregate functions that are present in result to projection list
            for (i in 0 until relBuilder.peek().getRowType().getFieldCount()) {
                if (!projectedCols.get(i)) {
                    projects.add(relBuilder.field(i))
                }
            }
            relBuilder.project(projects)
        } else {
            rewritingMapping = null
        }

        // Add query expressions on top. We first map query expressions to view
        // expressions. Once we have done that, if the expression is contained
        // and we have introduced already an operator on top of the input node,
        // we use the mapping to resolve the position of the expression in the
        // node.
        val topRowType: RelDataType
        val topExprs: List<RexNode> = ArrayList()
        topRowType = if (topProject != null && !unionRewriting) {
            topExprs.addAll(topProject.getProjects())
            topProject.getRowType()
        } else {
            // Add all
            for (pos in 0 until queryAggregate.getRowType().getFieldCount()) {
                topExprs.add(rexBuilder.makeInputRef(queryAggregate, pos))
            }
            queryAggregate.getRowType()
        }
        // Available in view.
        val viewExprs: Multimap<RexNode, Integer> = ArrayListMultimap.create()
        addAllIndexed(viewExprs, topViewProject.getProjects())
        addAllIndexed(viewExprs, additionalViewExprs)
        val rewrittenExprs: List<RexNode> = ArrayList(topExprs.size())
        for (expr in topExprs) {
            // First map through the aggregate
            val e2: RexNode = shuttleReferences(rexBuilder, expr, aggregateMapping)
                ?: // Cannot map expression
                return null
            // Next map through the last project
            val e3: RexNode = shuttleReferences(
                rexBuilder, e2, viewExprs,
                relBuilder.peek(), rewritingMapping
            )
                ?: // Cannot map expression
                return null
            rewrittenExprs.add(e3)
        }
        return relBuilder
            .project(rewrittenExprs)
            .convert(topRowType, false)
            .build()
    }

    /**
     * Mapping from node expressions to target expressions.
     *
     *
     * If any of the expressions cannot be mapped, we return null.
     */
    @Nullable
    protected fun generateMapping(
        rexBuilder: RexBuilder,
        simplify: RexSimplify,
        mq: RelMetadataQuery,
        node: RelNode?,
        target: RelNode,
        positions: ImmutableBitSet,
        tableMapping: BiMap<RelTableRef?, RelTableRef?>,
        sourceEC: EquivalenceClasses,
        additionalExprs: List<RexNode?>
    ): Multimap<Integer, Integer>? {
        Preconditions.checkArgument(additionalExprs.isEmpty())
        val m: Multimap<Integer, Integer> = ArrayListMultimap.create()
        val equivalenceClassesMap: Map<RexTableInputRef, Set<RexTableInputRef>> = sourceEC.getEquivalenceClassesMap()
        val exprsLineage: Multimap<RexNode, Integer> = ArrayListMultimap.create()
        val timestampExprs: List<RexNode> = ArrayList()
        for (i in 0 until target.getRowType().getFieldCount()) {
            val s: Set<RexNode> = mq.getExpressionLineage(target, rexBuilder.makeInputRef(target, i))
                ?: // Bail out
                continue
            // We only support project - filter - join, thus it should map to
            // a single expression
            val e: RexNode = Iterables.getOnlyElement(s)
            // Rewrite expr to be expressed on query tables
            val simplified: RexNode = simplify.simplifyUnknownAsFalse(e)
            val expr: RexNode = RexUtil.swapTableColumnReferences(
                rexBuilder,
                simplified,
                tableMapping.inverse(),
                equivalenceClassesMap
            )
            exprsLineage.put(expr, i)
            val sqlTypeName: SqlTypeName = expr.getType().getSqlTypeName()
            if (sqlTypeName === SqlTypeName.TIMESTAMP
                || sqlTypeName === SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
            ) {
                timestampExprs.add(expr)
            }
        }

        // If this is a column of TIMESTAMP (WITH LOCAL TIME ZONE)
        // type, we add the possible rollup columns too.
        // This way we will be able to match FLOOR(ts to HOUR) to
        // FLOOR(ts to DAY) via FLOOR(FLOOR(ts to HOUR) to DAY)
        for (timestampExpr in timestampExprs) {
            for (value in SUPPORTED_DATE_TIME_ROLLUP_UNITS) {
                val functions: Array<SqlFunction> = arrayOf<SqlFunction>(
                    getCeilSqlFunction(value),
                    getFloorSqlFunction(value)
                )
                for (function in functions) {
                    val call: RexNode = rexBuilder.makeCall(
                        function,
                        timestampExpr, rexBuilder.makeFlag(value)
                    )
                    // References self-row
                    val rewrittenCall: RexNode = shuttleReferences(rexBuilder, call, exprsLineage) ?: continue
                    // We add the CEIL or FLOOR expression to the additional
                    // expressions, replacing the child expression by the position that
                    // it references
                    additionalExprs.add(rewrittenCall)
                    // Then we simplify the expression and we add it to the expressions
                    // lineage so we can try to find a match.
                    val simplified: RexNode = simplify.simplifyUnknownAsFalse(call)
                    exprsLineage.put(
                        simplified,
                        target.getRowType().getFieldCount() + additionalExprs.size() - 1
                    )
                }
            }
        }
        for (i in positions) {
            val s: Set<RexNode> = mq.getExpressionLineage(node, rexBuilder.makeInputRef(node, i))
                ?: // Bail out
                return null
            // We only support project - filter - join, thus it should map to
            // a single expression
            val e: RexNode = Iterables.getOnlyElement(s)
            // Rewrite expr to be expressed on query tables
            val simplified: RexNode = simplify.simplifyUnknownAsFalse(e)
            val targetExpr: RexNode = RexUtil.swapColumnReferences(
                rexBuilder,
                simplified, equivalenceClassesMap
            )
            val c: Collection<Integer> = exprsLineage.get(targetExpr)
            if (!c.isEmpty()) {
                for (j in c) {
                    m.put(i, j)
                }
            } else {
                // If we did not find the expression, try to navigate it
                val rewrittenTargetExpr: RexNode = shuttleReferences(rexBuilder, targetExpr, exprsLineage)
                    ?: // Some expressions were not present
                    return null
                m.put(i, target.getRowType().getFieldCount() + additionalExprs.size())
                additionalExprs.add(rewrittenTargetExpr)
            }
        }
        return m
    }

    /**
     * Get ceil function datetime.
     */
    protected fun getCeilSqlFunction(flag: TimeUnitRange?): SqlFunction {
        return SqlStdOperatorTable.CEIL
    }

    /**
     * Get floor function datetime.
     */
    protected fun getFloorSqlFunction(flag: TimeUnitRange?): SqlFunction {
        return SqlStdOperatorTable.FLOOR
    }

    /**
     * Get rollup aggregation function.
     */
    @Deprecated // to be removed before 2.0
    @Nullable
    protected fun getRollup(aggregation: SqlAggFunction): SqlAggFunction? {
        return if (aggregation === SqlStdOperatorTable.SUM || aggregation === SqlStdOperatorTable.SUM0 || aggregation is SqlMinMaxAggFunction
            || aggregation === SqlStdOperatorTable.ANY_VALUE
        ) {
            aggregation
        } else if (aggregation === SqlStdOperatorTable.COUNT) {
            SqlStdOperatorTable.SUM0
        } else {
            null
        }
    }

    @Override
    override fun pushFilterToOriginalViewPlan(
        builder: RelBuilder,
        @Nullable topViewProject: RelNode?, viewNode: RelNode?, cond: RexNode?
    ): Pair<RelNode, RelNode> {
        // We add (and push) the filter to the view plan before triggering the rewriting.
        // This is useful in case some of the columns can be folded to same value after
        // filter is added.
        val pushFiltersProgram = HepProgramBuilder()
        if (topViewProject != null) {
            pushFiltersProgram.addRuleInstance(config.filterProjectTransposeRule())
        }
        pushFiltersProgram
            .addRuleInstance(config.filterAggregateTransposeRule())
            .addRuleInstance(config.aggregateProjectPullUpConstantsRule())
            .addRuleInstance(config.projectMergeRule())
        val tmpPlanner = HepPlanner(pushFiltersProgram.build())
        // Now that the planner is created, push the node
        var topNode: RelNode? = builder
            .push(if (topViewProject != null) topViewProject else viewNode)
            .filter(cond).build()
        tmpPlanner.setRoot(topNode)
        topNode = tmpPlanner.findBestExp()
        var resultTopViewProject: RelNode? = null
        var resultViewNode: RelNode? = null
        while (topNode != null) {
            if (topNode is Project) {
                if (resultTopViewProject != null) {
                    // Both projects could not be merged, we will bail out
                    return Pair.of(topViewProject, viewNode)
                }
                resultTopViewProject = topNode
                topNode = topNode.getInput(0)
            } else if (topNode is Aggregate) {
                resultViewNode = topNode
                topNode = null
            } else {
                // We move to the child
                topNode = topNode.getInput(0)
            }
        }
        return Pair.of(resultTopViewProject, requireNonNull(resultViewNode, "resultViewNode"))
    }

    /**
     * Rule configuration.
     */
    interface Config : MaterializedViewRule.Config {
        /** Instance of rule to push filter through project.  */
        @Value.Default
        fun filterProjectTransposeRule(): RelOptRule? {
            return CoreRules.FILTER_PROJECT_TRANSPOSE.config
                .withRelBuilderFactory(relBuilderFactory())
                .`as`(FilterProjectTransposeRule.Config::class.java)
                .withOperandFor(
                    Filter::class.java, { filter -> !RexUtil.containsCorrelation(filter.getCondition()) },
                    Project::class.java
                ) { project -> true }
                .withCopyFilter(true)
                .withCopyProject(true)
                .toRule()
        }

        /** Sets [.filterProjectTransposeRule].  */
        fun withFilterProjectTransposeRule(rule: RelOptRule?): Config?

        /** Instance of rule to push filter through aggregate.  */
        @Value.Default
        fun filterAggregateTransposeRule(): RelOptRule? {
            return CoreRules.FILTER_AGGREGATE_TRANSPOSE.config
                .withRelBuilderFactory(relBuilderFactory())
                .`as`(FilterAggregateTransposeRule.Config::class.java)
                .withOperandFor(Filter::class.java, Aggregate::class.java)
                .toRule()
        }

        /** Sets [.filterAggregateTransposeRule].  */
        fun withFilterAggregateTransposeRule(rule: RelOptRule?): Config?

        /** Instance of rule to pull up constants into aggregate.  */
        @Value.Default
        fun aggregateProjectPullUpConstantsRule(): RelOptRule? {
            return AggregateProjectPullUpConstantsRule.Config.DEFAULT
                .withRelBuilderFactory(relBuilderFactory())
                .withDescription("AggFilterPullUpConstants")
                .`as`(AggregateProjectPullUpConstantsRule.Config::class.java)
                .withOperandFor(Aggregate::class.java, Filter::class.java)
                .toRule()
        }

        /** Sets [.aggregateProjectPullUpConstantsRule].  */
        fun withAggregateProjectPullUpConstantsRule(rule: RelOptRule?): Config?

        /** Instance of rule to merge project operators.  */
        @Value.Default
        fun projectMergeRule(): RelOptRule? {
            return CoreRules.PROJECT_MERGE.config
                .withRelBuilderFactory(relBuilderFactory())
                .toRule()
        }

        /** Sets [.projectMergeRule].  */
        fun withProjectMergeRule(rule: RelOptRule?): Config?
    }

    companion object {
        protected val SUPPORTED_DATE_TIME_ROLLUP_UNITS: ImmutableList<TimeUnitRange> = ImmutableList.of(
            TimeUnitRange.YEAR, TimeUnitRange.QUARTER, TimeUnitRange.MONTH,
            TimeUnitRange.DAY, TimeUnitRange.HOUR, TimeUnitRange.MINUTE,
            TimeUnitRange.SECOND, TimeUnitRange.MILLISECOND, TimeUnitRange.MICROSECOND
        )

        private fun <K> addAllIndexed(
            multimap: Multimap<K, Integer>,
            list: Iterable<K>
        ) {
            for (k in list) {
                multimap.put(k, multimap.size())
            }
        }

        /** Given a relational expression with a single input (such as a Project or
         * Aggregate) and the ordinal of an input field, returns the ordinal of the
         * output field that references the input field. Or -1 if the field is not
         * propagated.
         *
         *
         * For example, if `rel` is `Project(c0, c2)` (on input with
         * columns (c0, c1, c2)), then `find(rel, 2)` returns 1 (c2);
         * `find(rel, 1)` returns -1 (because c1 is not projected).
         *
         *
         * If `rel` is `Aggregate([0, 2], sum(1))`, then
         * `find(rel, 2)` returns 1, and `find(rel, 1)` returns -1.
         *
         * @param rel Relational expression
         * @param ref Ordinal of output field
         * @return Ordinal of input field, or -1
         */
        private fun find(rel: RelNode, ref: Int): Int {
            if (rel is Project) {
                val project: Project = rel as Project
                for (p in Ord.zip(project.getProjects())) {
                    if (p.e is RexInputRef
                        && (p.e as RexInputRef).getIndex() === ref
                    ) {
                        return p.i
                    }
                }
            }
            if (rel is Aggregate) {
                val aggregate: Aggregate = rel as Aggregate
                val k: Int = aggregate.getGroupSet().indexOf(ref)
                if (k >= 0) {
                    return k
                }
            }
            return -1
        }
    }
}
