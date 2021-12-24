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
package org.apache.calcite.rel.mutable

import org.apache.calcite.plan.RelOptUtil

/** Utilities for dealing with [MutableRel]s.  */
object MutableRels {
    fun contains(
        ancestor: MutableRel,
        target: MutableRel?
    ): Boolean {
        return if (ancestor.equals(target)) {
            // Short-cut common case.
            true
        } else try {
            object : MutableRelVisitor() {
                @Override
                override fun visit(@Nullable node: MutableRel?) {
                    if (Objects.equals(node, target)) {
                        throw Util.FoundOne.NULL
                    }
                    super.visit(node)
                } // CHECKSTYLE: IGNORE 1
            }.go(ancestor)
            false
        } catch (e: Util.FoundOne) {
            true
        }
    }

    @Nullable
    fun preOrderTraverseNext(node: MutableRel): MutableRel? {
        var node: MutableRel = node
        var parent: MutableRel = node.getParent()
        var ordinal: Int = node.ordinalInParent + 1
        while (parent != null) {
            if (parent.getInputs().size() > ordinal) {
                return parent.getInputs().get(ordinal)
            }
            node = parent
            parent = node.getParent()
            ordinal = node.ordinalInParent + 1
        }
        return null
    }

    fun descendants(query: MutableRel): List<MutableRel> {
        val list: List<MutableRel> = ArrayList()
        descendantsRecurse(list, query)
        return list
    }

    private fun descendantsRecurse(
        list: List<MutableRel>,
        rel: MutableRel
    ) {
        list.add(rel)
        for (input in rel.getInputs()) {
            descendantsRecurse(list, input)
        }
    }

    /** Based on
     * [org.apache.calcite.rel.rules.ProjectRemoveRule.strip].  */
    fun strip(project: MutableProject): MutableRel {
        return if (isTrivial(project)) project.getInput()!! else project
    }

    /** Based on
     * [org.apache.calcite.rel.rules.ProjectRemoveRule.isTrivial].  */
    fun isTrivial(project: MutableProject): Boolean {
        val child: MutableRel = project.getInput()!!
        return RexUtil.isIdentity(project.projects, child.rowType)
    }

    /** Equivalent to
     * [RelOptUtil.createProject]
     * for [MutableRel].  */
    fun createProject(
        child: MutableRel,
        posList: List<Integer>
    ): MutableRel {
        val rowType: RelDataType = child.rowType
        if (Mappings.isIdentity(posList, rowType.getFieldCount())) {
            return child
        }
        val mapping: Mapping = Mappings.create(
            MappingType.INVERSE_SURJECTION,
            rowType.getFieldCount(),
            posList.size()
        )
        for (i in 0 until posList.size()) {
            mapping.set(posList[i], i)
        }
        return MutableProject.of(
            RelOptUtil.permute(child.cluster.getTypeFactory(), rowType, mapping),
            child,
            object : AbstractList<RexNode?>() {
                @Override
                fun size(): Int {
                    return posList.size()
                }

                @Override
                operator fun get(index: Int): RexNode {
                    val pos: Int = posList[index]
                    return RexInputRef.of(pos, rowType)
                }
            })
    }

    /**
     * Construct expression list of Project by the given fields of the input.
     */
    fun createProjectExprs(
        child: MutableRel,
        posList: List<Integer?>
    ): List<RexNode> {
        return posList.stream().map { pos -> RexInputRef.of(pos, child.rowType) }
            .collect(Collectors.toList())
    }

    /**
     * Construct expression list of Project by the given fields of the input.
     */
    fun createProjects(
        child: MutableRel,
        projects: List<RexNode?>
    ): List<RexNode> {
        val rexNodeList: List<RexNode> = ArrayList(projects.size())
        for (project in projects) {
            if (project is RexInputRef) {
                val rexInputRef: RexInputRef = project as RexInputRef
                rexNodeList.add(RexInputRef.of(rexInputRef.getIndex(), child.rowType))
            } else {
                rexNodeList.add(project)
            }
        }
        return rexNodeList
    }

    /** Equivalence to [org.apache.calcite.plan.RelOptUtil.createCastRel]
     * for [MutableRel].  */
    fun createCastRel(
        rel: MutableRel,
        castRowType: RelDataType, rename: Boolean
    ): MutableRel {
        val rowType: RelDataType = rel.rowType
        if (RelOptUtil.areRowTypesEqual(rowType, castRowType, rename)) {
            // nothing to do
            return rel
        }
        val castExps: List<RexNode> = RexUtil.generateCastExpressions(
            rel.cluster.getRexBuilder(),
            castRowType, rowType
        )
        val fieldNames: List<String> = if (rename) castRowType.getFieldNames() else rowType.getFieldNames()
        return MutableProject.of(rel, castExps, fieldNames)
    }

    fun fromMutable(node: MutableRel): RelNode? {
        return fromMutable(node, RelFactories.LOGICAL_BUILDER.create(node.cluster, null))
    }

    fun fromMutable(node: MutableRel, relBuilder: RelBuilder): RelNode? {
        return when (node.type) {
            TABLE_SCAN, VALUES -> (node as MutableLeafRel).rel
            PROJECT -> {
                val project: MutableProject = node as MutableProject
                relBuilder.push(fromMutable(project.input!!, relBuilder))
                relBuilder.project(project.projects, project.rowType.getFieldNames(), true)
                relBuilder.build()
            }
            FILTER -> {
                val filter: MutableFilter = node as MutableFilter
                relBuilder.push(fromMutable(filter.input!!, relBuilder))
                relBuilder.filter(filter.condition)
                relBuilder.build()
            }
            AGGREGATE -> {
                val aggregate: MutableAggregate = node as MutableAggregate
                relBuilder.push(fromMutable(aggregate.input!!, relBuilder))
                relBuilder.aggregate(
                    relBuilder.groupKey(aggregate.groupSet, aggregate.groupSets),
                    aggregate.aggCalls
                )
                relBuilder.build()
            }
            SORT -> {
                val sort: MutableSort = node as MutableSort
                LogicalSort.create(
                    fromMutable(sort.input!!, relBuilder), sort.collation,
                    sort.offset, sort.fetch
                )
            }
            CALC -> {
                val calc: MutableCalc = node as MutableCalc
                LogicalCalc.create(
                    fromMutable(calc.input!!, relBuilder),
                    calc.program
                )
            }
            EXCHANGE -> {
                val exchange: MutableExchange = node as MutableExchange
                LogicalExchange.create(
                    fromMutable(exchange.getInput()!!, relBuilder),
                    exchange.distribution
                )
            }
            COLLECT -> {
                val collect: MutableCollect = node as MutableCollect
                val child: RelNode? =
                    fromMutable(collect.getInput()!!, relBuilder)
                Collect.create(child, SqlTypeName.MULTISET, collect.fieldName)
            }
            UNCOLLECT -> {
                val uncollect: MutableUncollect = node as MutableUncollect
                val child: RelNode? =
                    fromMutable(uncollect.getInput()!!, relBuilder)
                Uncollect.create(
                    child.getTraitSet(), child, uncollect.withOrdinality,
                    Collections.emptyList()
                )
            }
            WINDOW -> {
                val window: MutableWindow = node as MutableWindow
                val child: RelNode? =
                    fromMutable(window.getInput()!!, relBuilder)
                LogicalWindow.create(
                    child.getTraitSet(),
                    child, window.constants, window.rowType, window.groups
                )
            }
            MATCH -> {
                val match: MutableMatch = node as MutableMatch
                val child: RelNode? = fromMutable(match.getInput()!!, relBuilder)
                LogicalMatch.create(
                    child, match.rowType, match.pattern,
                    match.strictStart, match.strictEnd, match.patternDefinitions,
                    match.measures, match.after, match.subsets, match.allRows,
                    match.partitionKeys, match.orderKeys, match.interval
                )
            }
            TABLE_MODIFY -> {
                val modify: MutableTableModify = node as MutableTableModify
                LogicalTableModify.create(
                    modify.table,
                    modify.catalogReader,
                    fromMutable(modify.getInput()!!, relBuilder),
                    modify.operation,
                    modify.updateColumnList,
                    modify.sourceExpressionList,
                    modify.flattened
                )
            }
            SAMPLE -> {
                val sample: MutableSample = node as MutableSample
                Sample(
                    sample.cluster,
                    fromMutable(sample.getInput()!!, relBuilder),
                    sample.params
                )
            }
            TABLE_FUNCTION_SCAN -> {
                val tableFunctionScan: MutableTableFunctionScan = node as MutableTableFunctionScan
                LogicalTableFunctionScan.create(
                    tableFunctionScan.cluster,
                    fromMutables(tableFunctionScan.getInputs(), relBuilder),
                    tableFunctionScan.rexCall,
                    tableFunctionScan.elementType,
                    tableFunctionScan.rowType,
                    tableFunctionScan.columnMappings
                )
            }
            JOIN -> {
                val join: MutableJoin = node as MutableJoin
                relBuilder.push(fromMutable(join.getLeft()!!, relBuilder))
                relBuilder.push(fromMutable(join.getRight()!!, relBuilder))
                relBuilder.join(join.joinType, join.condition, join.variablesSet)
                relBuilder.build()
            }
            CORRELATE -> {
                val correlate: MutableCorrelate = node as MutableCorrelate
                LogicalCorrelate.create(
                    fromMutable(correlate.getLeft()!!, relBuilder),
                    fromMutable(correlate.getRight()!!, relBuilder),
                    correlate.correlationId,
                    correlate.requiredColumns,
                    correlate.joinType
                )
            }
            UNION -> {
                val union: MutableUnion = node as MutableUnion
                relBuilder.pushAll(fromMutables(union.inputs, relBuilder))
                relBuilder.union(union.all, union.inputs.size())
                relBuilder.build()
            }
            MINUS -> {
                val minus: MutableMinus = node as MutableMinus
                relBuilder.pushAll(fromMutables(minus.inputs, relBuilder))
                relBuilder.minus(minus.all, minus.inputs.size())
                relBuilder.build()
            }
            INTERSECT -> {
                val intersect: MutableIntersect = node as MutableIntersect
                relBuilder.pushAll(fromMutables(intersect.inputs, relBuilder))
                relBuilder.intersect(intersect.all, intersect.inputs.size())
                relBuilder.build()
            }
            else -> throw AssertionError(node.deep())
        }
    }

    private fun fromMutables(
        nodes: List<MutableRel>,
        relBuilder: RelBuilder
    ): List<RelNode> {
        return Util.transform(
            nodes
        ) { mutableRel -> fromMutable(mutableRel, relBuilder) }
    }

    fun toMutable(rel: RelNode?): MutableRel {
        if (rel is HepRelVertex) {
            return toMutable((rel as HepRelVertex?).getCurrentRel())
        }
        if (rel is RelSubset) {
            val subset: RelSubset? = rel as RelSubset?
            var best: RelNode? = subset.getBest()
            if (best == null) {
                best = requireNonNull(
                    subset.getOriginal()
                ) { "subset.getOriginal() is null for $subset" }
            }
            return toMutable(best)
        }
        if (rel is TableScan) {
            return MutableScan.of(rel as TableScan?)
        }
        if (rel is Values) {
            return MutableValues.of(rel as Values?)
        }
        if (rel is Project) {
            val project: Project? = rel as Project?
            val input: MutableRel = toMutable(project.getInput())
            return MutableProject.of(
                input, project.getProjects(),
                project.getRowType().getFieldNames()
            )
        }
        if (rel is Filter) {
            val filter: Filter? = rel as Filter?
            val input: MutableRel = toMutable(filter.getInput())
            return MutableFilter.of(input, filter.getCondition())
        }
        if (rel is Aggregate) {
            val aggregate: Aggregate? = rel as Aggregate?
            val input: MutableRel = toMutable(aggregate.getInput())
            return MutableAggregate.of(
                input, aggregate.getGroupSet(),
                aggregate.getGroupSets(), aggregate.getAggCallList()
            )
        }
        if (rel is Sort) {
            val sort: Sort? = rel as Sort?
            val input: MutableRel = toMutable(sort.getInput())
            return MutableSort.of(input, sort.getCollation(), sort.offset, sort.fetch)
        }
        if (rel is Calc) {
            val calc: Calc? = rel as Calc?
            val input: MutableRel = toMutable(calc.getInput())
            return MutableCalc.of(input, calc.getProgram())
        }
        if (rel is Exchange) {
            val exchange: Exchange? = rel as Exchange?
            val input: MutableRel = toMutable(exchange.getInput())
            return MutableExchange.of(input, exchange.getDistribution())
        }
        if (rel is Collect) {
            val collect: Collect? = rel as Collect?
            val input: MutableRel = toMutable(collect.getInput())
            return MutableCollect.of(collect.getRowType(), input, collect.getFieldName())
        }
        if (rel is Uncollect) {
            val uncollect: Uncollect? = rel as Uncollect?
            val input: MutableRel = toMutable(uncollect.getInput())
            return MutableUncollect.of(uncollect.getRowType(), input, uncollect.withOrdinality)
        }
        if (rel is Window) {
            val window: Window? = rel as Window?
            val input: MutableRel = toMutable(window.getInput())
            return MutableWindow.of(
                window.getRowType(),
                input, window.groups, window.getConstants()
            )
        }
        if (rel is Match) {
            val match: Match? = rel as Match?
            val input: MutableRel = toMutable(match.getInput())
            return MutableMatch.of(
                match.getRowType(),
                input, match.getPattern(), match.isStrictStart(), match.isStrictEnd(),
                match.getPatternDefinitions(), match.getMeasures(), match.getAfter(),
                match.getSubsets(), match.isAllRows(), match.getPartitionKeys(),
                match.getOrderKeys(), match.getInterval()
            )
        }
        if (rel is TableModify) {
            val modify: TableModify? = rel as TableModify?
            val input: MutableRel = toMutable(modify.getInput())
            return MutableTableModify.of(
                modify.getRowType(), input, modify.getTable(),
                modify.getCatalogReader(), modify.getOperation(), modify.getUpdateColumnList(),
                modify.getSourceExpressionList(), modify.isFlattened()
            )
        }
        if (rel is Sample) {
            val sample: Sample? = rel as Sample?
            val input: MutableRel = toMutable(sample.getInput())
            return MutableSample.of(input, sample.getSamplingParameters())
        }
        if (rel is TableFunctionScan) {
            val tableFunctionScan: TableFunctionScan? = rel as TableFunctionScan?
            val inputs: List<MutableRel> = toMutables(tableFunctionScan.getInputs())
            return MutableTableFunctionScan.of(
                tableFunctionScan.getCluster(),
                tableFunctionScan.getRowType(), inputs, tableFunctionScan.getCall(),
                tableFunctionScan.getElementType(), tableFunctionScan.getColumnMappings()
            )
        }
        // It is necessary that SemiJoin is placed in front of Join here, since SemiJoin
        // is a sub-class of Join.
        if (rel is Join) {
            val join: Join? = rel as Join?
            val left: MutableRel = toMutable(join.getLeft())
            val right: MutableRel = toMutable(join.getRight())
            return MutableJoin.of(
                join.getRowType(), left, right,
                join.getCondition(), join.getJoinType(), join.getVariablesSet()
            )
        }
        if (rel is Correlate) {
            val correlate: Correlate? = rel as Correlate?
            val left: MutableRel = toMutable(correlate.getLeft())
            val right: MutableRel = toMutable(correlate.getRight())
            return MutableCorrelate.of(
                correlate.getRowType(), left, right,
                correlate.getCorrelationId(), correlate.getRequiredColumns(),
                correlate.getJoinType()
            )
        }
        if (rel is Union) {
            val union: Union? = rel as Union?
            val inputs: List<MutableRel> = toMutables(union.getInputs())
            return MutableUnion.of(union.getRowType(), inputs, union.all)
        }
        if (rel is Minus) {
            val minus: Minus? = rel as Minus?
            val inputs: List<MutableRel> = toMutables(minus.getInputs())
            return MutableMinus.of(minus.getRowType(), inputs, minus.all)
        }
        if (rel is Intersect) {
            val intersect: Intersect? = rel as Intersect?
            val inputs: List<MutableRel> = toMutables(intersect.getInputs())
            return MutableIntersect.of(intersect.getRowType(), inputs, intersect.all)
        }
        throw RuntimeException("cannot translate $rel to MutableRel")
    }

    private fun toMutables(nodes: List<RelNode>): List<MutableRel> {
        return nodes.stream().map { obj: MutableRels?, rel: RelNode? -> toMutable(rel) }
            .collect(Collectors.toList())
    }
}
