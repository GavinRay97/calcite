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
package org.apache.calcite.rel.logical

import org.apache.calcite.adapter.enumerable.EnumerableInterpreter

/**
 * Shuttle to convert any rel plan to a plan with all logical nodes.
 */
class ToLogicalConverter(relBuilder: RelBuilder) : RelShuttleImpl() {
    private val relBuilder: RelBuilder

    init {
        this.relBuilder = relBuilder
    }

    @Override
    fun visit(scan: TableScan): RelNode {
        return LogicalTableScan.create(scan.getCluster(), scan.getTable(), scan.getHints())
    }

    @Override
    fun visit(relNode: RelNode): RelNode {
        if (relNode is Aggregate) {
            val agg: Aggregate = relNode as Aggregate
            return relBuilder.push(visit(agg.getInput()))
                .aggregate(
                    relBuilder.groupKey(agg.getGroupSet(), agg.groupSets),
                    agg.getAggCallList()
                )
                .build()
        }
        if (relNode is TableScan) {
            return visit(relNode as TableScan)
        }
        if (relNode is Filter) {
            val filter: Filter = relNode as Filter
            return relBuilder.push(visit(filter.getInput()))
                .filter(filter.getCondition())
                .build()
        }
        if (relNode is Project) {
            val project: Project = relNode as Project
            return relBuilder.push(visit(project.getInput()))
                .project(project.getProjects(), project.getRowType().getFieldNames())
                .build()
        }
        if (relNode is Union) {
            val union: Union = relNode as Union
            for (rel in union.getInputs()) {
                relBuilder.push(visit(rel))
            }
            return relBuilder.union(union.all, union.getInputs().size())
                .build()
        }
        if (relNode is Intersect) {
            val intersect: Intersect = relNode as Intersect
            for (rel in intersect.getInputs()) {
                relBuilder.push(visit(rel))
            }
            return relBuilder.intersect(intersect.all, intersect.getInputs().size())
                .build()
        }
        if (relNode is Minus) {
            val minus: Minus = relNode as Minus
            for (rel in minus.getInputs()) {
                relBuilder.push(visit(rel))
            }
            return relBuilder.minus(minus.all, minus.getInputs().size())
                .build()
        }
        if (relNode is Join) {
            val join: Join = relNode as Join
            return relBuilder.push(visit(join.getLeft()))
                .push(visit(join.getRight()))
                .join(join.getJoinType(), join.getCondition())
                .build()
        }
        if (relNode is Correlate) {
            val corr: Correlate = relNode as Correlate
            return relBuilder.push(visit(corr.getLeft()))
                .push(visit(corr.getRight()))
                .join(
                    corr.getJoinType(), relBuilder.literal(true),
                    corr.getVariablesSet()
                )
                .build()
        }
        if (relNode is Values) {
            val values: Values = relNode as Values
            return relBuilder.values(values.tuples, values.getRowType())
                .build()
        }
        if (relNode is Sort) {
            val sort: Sort = relNode as Sort
            return LogicalSort.create(
                visit(sort.getInput()), sort.getCollation(),
                sort.offset, sort.fetch
            )
        }
        if (relNode is Window) {
            val window: Window = relNode as Window
            val input: RelNode = visit(window.getInput())
            return LogicalWindow.create(
                input.getTraitSet(), input, window.constants,
                window.getRowType(), window.groups
            )
        }
        if (relNode is Calc) {
            val calc: Calc = relNode as Calc
            return LogicalCalc.create(visit(calc.getInput()), calc.getProgram())
        }
        if (relNode is TableModify) {
            val tableModify: TableModify = relNode as TableModify
            val input: RelNode = visit(tableModify.getInput())
            return LogicalTableModify.create(
                tableModify.getTable(),
                tableModify.getCatalogReader(), input, tableModify.getOperation(),
                tableModify.getUpdateColumnList(), tableModify.getSourceExpressionList(),
                tableModify.isFlattened()
            )
        }
        if (relNode is EnumerableInterpreter
            || relNode is JdbcToEnumerableConverter
        ) {
            return visit((relNode as SingleRel).getInput())
        }
        if (relNode is EnumerableLimit) {
            val limit: EnumerableLimit = relNode as EnumerableLimit
            var logicalInput: RelNode = visit(limit.getInput())
            var collation: RelCollation = RelCollations.of()
            if (logicalInput is Sort) {
                collation = (logicalInput as Sort).collation
                logicalInput = (logicalInput as Sort).getInput()
            }
            return LogicalSort.create(
                logicalInput, collation, limit.offset,
                limit.fetch
            )
        }
        if (relNode is Uncollect) {
            val uncollect: Uncollect = relNode as Uncollect
            val input: RelNode = visit(uncollect.getInput())
            return Uncollect.create(
                input.getTraitSet(), input,
                uncollect.withOrdinality, Collections.emptyList()
            )
        }
        throw AssertionError(
            "Need to implement logical converter for "
                    + relNode.getClass().getName()
        )
    }
}
