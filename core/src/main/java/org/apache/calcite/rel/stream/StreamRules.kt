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
package org.apache.calcite.rel.stream

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.RelRule
import org.apache.calcite.prepare.RelOptTableImpl
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rel.core.Join
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.core.Union
import org.apache.calcite.rel.core.Values
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.calcite.rel.logical.LogicalFilter
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rel.logical.LogicalSort
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.rel.logical.LogicalUnion
import org.apache.calcite.rel.rules.TransformationRule
import org.apache.calcite.schema.StreamableTable
import org.apache.calcite.schema.Table
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.Util
import com.google.common.collect.ImmutableList
import org.immutables.value.Value
import java.util.ArrayList
import java.util.List

/**
 * Rules and relational operators for streaming relational expressions.
 */
object StreamRules {
    val RULES: ImmutableList<RelOptRule> = ImmutableList.of(
        DeltaProjectTransposeRule.DeltaProjectTransposeRuleConfig.DEFAULT.toRule(),
        DeltaFilterTransposeRule.DeltaFilterTransposeRuleConfig.DEFAULT.toRule(),
        DeltaAggregateTransposeRule.DeltaAggregateTransposeRuleConfig.DEFAULT.toRule(),
        DeltaSortTransposeRule.DeltaSortTransposeRuleConfig.DEFAULT.toRule(),
        DeltaUnionTransposeRule.DeltaUnionTransposeRuleConfig.DEFAULT.toRule(),
        DeltaJoinTransposeRule.DeltaJoinTransposeRuleConfig.DEFAULT.toRule(),
        DeltaTableScanRule.DeltaTableScanRuleConfig.DEFAULT.toRule(),
        DeltaTableScanToEmptyRule.DeltaTableScanToEmptyRuleConfig.DEFAULT.toRule()
    )

    /** Planner rule that pushes a [Delta] through a [Project].  */
    class DeltaProjectTransposeRule
    /** Creates a DeltaProjectTransposeRule.  */
    protected constructor(config: DeltaProjectTransposeRuleConfig?) :
        RelRule<DeltaProjectTransposeRule.DeltaProjectTransposeRuleConfig?>(config), TransformationRule {
        @Override
        fun onMatch(call: RelOptRuleCall) {
            val delta: Delta = call.rel(0)
            Util.discard(delta)
            val project: Project = call.rel(1)
            val newDelta: LogicalDelta = LogicalDelta.create(project.getInput())
            val newProject: LogicalProject = LogicalProject.create(
                newDelta,
                project.getHints(),
                project.getProjects(),
                project.getRowType().getFieldNames()
            )
            call.transformTo(newProject)
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface DeltaProjectTransposeRuleConfig : RelRule.Config {
            @Override
            fun toRule(): DeltaProjectTransposeRule? {
                return DeltaProjectTransposeRule(this)
            }

            /** Defines an operand tree for the given classes.  */
            fun withOperandFor(relClass: Class<out RelNode?>?): Config? {
                return withOperandSupplier { b -> b.operand(relClass).anyInputs() }
                    .`as`(Config::class.java)
            }

            companion object {
                val DEFAULT: DeltaProjectTransposeRuleConfig = ImmutableDeltaProjectTransposeRuleConfig.of()
                    .withOperandSupplier { b0 ->
                        b0.operand(Delta::class.java).oneInput { b1 -> b1.operand(Project::class.java).anyInputs() }
                    }
            }
        }
    }

    /** Planner rule that pushes a [Delta] through a [Filter].  */
    class DeltaFilterTransposeRule
    /** Creates a DeltaFilterTransposeRule.  */
    protected constructor(config: DeltaFilterTransposeRuleConfig?) :
        RelRule<DeltaFilterTransposeRule.DeltaFilterTransposeRuleConfig?>(config), TransformationRule {
        @Override
        fun onMatch(call: RelOptRuleCall) {
            val delta: Delta = call.rel(0)
            Util.discard(delta)
            val filter: Filter = call.rel(1)
            val newDelta: LogicalDelta = LogicalDelta.create(filter.getInput())
            val newFilter: LogicalFilter = LogicalFilter.create(newDelta, filter.getCondition())
            call.transformTo(newFilter)
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface DeltaFilterTransposeRuleConfig : RelRule.Config {
            @Override
            fun toRule(): DeltaFilterTransposeRule? {
                return DeltaFilterTransposeRule(this)
            }

            /** Defines an operand tree for the given classes.  */
            fun withOperandFor(relClass: Class<out RelNode?>?): Config? {
                return withOperandSupplier { b -> b.operand(relClass).anyInputs() }
                    .`as`(Config::class.java)
            }

            companion object {
                val DEFAULT: DeltaFilterTransposeRuleConfig = ImmutableDeltaFilterTransposeRuleConfig.of()
                    .withOperandSupplier { b0 ->
                        b0.operand(Delta::class.java).oneInput { b1 -> b1.operand(Filter::class.java).anyInputs() }
                    }
            }
        }
    }

    /** Planner rule that pushes a [Delta] through an [Aggregate].  */
    class DeltaAggregateTransposeRule
    /** Creates a DeltaAggregateTransposeRule.  */
    protected constructor(config: DeltaAggregateTransposeRuleConfig?) :
        RelRule<DeltaAggregateTransposeRule.DeltaAggregateTransposeRuleConfig?>(config), TransformationRule {
        @Override
        fun onMatch(call: RelOptRuleCall) {
            val delta: Delta = call.rel(0)
            Util.discard(delta)
            val aggregate: Aggregate = call.rel(1)
            val newDelta: LogicalDelta = LogicalDelta.create(aggregate.getInput())
            val newAggregate: LogicalAggregate = LogicalAggregate.create(
                newDelta, aggregate.getHints(), aggregate.getGroupSet(),
                aggregate.groupSets, aggregate.getAggCallList()
            )
            call.transformTo(newAggregate)
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface DeltaAggregateTransposeRuleConfig : RelRule.Config {
            @Override
            fun toRule(): DeltaAggregateTransposeRule? {
                return DeltaAggregateTransposeRule(this)
            }

            /** Defines an operand tree for the given classes.  */
            fun withOperandFor(relClass: Class<out RelNode?>?): Config? {
                return withOperandSupplier { b -> b.operand(relClass).anyInputs() }
                    .`as`(Config::class.java)
            }

            companion object {
                val DEFAULT: DeltaAggregateTransposeRuleConfig = ImmutableDeltaAggregateTransposeRuleConfig.of()
                    .withOperandSupplier { b0 ->
                        b0.operand(Delta::class.java).oneInput { b1 ->
                            b1.operand(Aggregate::class.java)
                                .predicate(Aggregate::isSimple).anyInputs()
                        }
                    }
            }
        }
    }

    /** Planner rule that pushes a [Delta] through an [Sort].  */
    class DeltaSortTransposeRule
    /** Creates a DeltaSortTransposeRule.  */
    protected constructor(config: DeltaSortTransposeRuleConfig?) :
        RelRule<DeltaSortTransposeRule.DeltaSortTransposeRuleConfig?>(config), TransformationRule {
        @Override
        fun onMatch(call: RelOptRuleCall) {
            val delta: Delta = call.rel(0)
            Util.discard(delta)
            val sort: Sort = call.rel(1)
            val newDelta: LogicalDelta = LogicalDelta.create(sort.getInput())
            val newSort: LogicalSort = LogicalSort.create(newDelta, sort.collation, sort.offset, sort.fetch)
            call.transformTo(newSort)
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface DeltaSortTransposeRuleConfig : RelRule.Config {
            @Override
            fun toRule(): DeltaSortTransposeRule? {
                return DeltaSortTransposeRule(this)
            }

            companion object {
                val DEFAULT: DeltaSortTransposeRuleConfig = ImmutableDeltaSortTransposeRuleConfig.of()
                    .withOperandSupplier { b0 ->
                        b0.operand(Delta::class.java).oneInput { b1 -> b1.operand(Sort::class.java).anyInputs() }
                    }
            }
        }
    }

    /** Planner rule that pushes a [Delta] through an [Union].  */
    class DeltaUnionTransposeRule
    /** Creates a DeltaUnionTransposeRule.  */
    protected constructor(config: DeltaUnionTransposeRuleConfig?) :
        RelRule<DeltaUnionTransposeRule.DeltaUnionTransposeRuleConfig?>(config), TransformationRule {
        @Override
        fun onMatch(call: RelOptRuleCall) {
            val delta: Delta = call.rel(0)
            Util.discard(delta)
            val union: Union = call.rel(1)
            val newInputs: List<RelNode> = ArrayList()
            for (input in union.getInputs()) {
                val newDelta: LogicalDelta = LogicalDelta.create(input)
                newInputs.add(newDelta)
            }
            val newUnion: LogicalUnion = LogicalUnion.create(newInputs, union.all)
            call.transformTo(newUnion)
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface DeltaUnionTransposeRuleConfig : RelRule.Config {
            @Override
            fun toRule(): DeltaUnionTransposeRule? {
                return DeltaUnionTransposeRule(this)
            }

            companion object {
                val DEFAULT: DeltaUnionTransposeRuleConfig = ImmutableDeltaUnionTransposeRuleConfig.of()
                    .withOperandSupplier { b0 ->
                        b0.operand(Delta::class.java).oneInput { b1 -> b1.operand(Union::class.java).anyInputs() }
                    }
            }
        }
    }

    /** Planner rule that pushes a [Delta] into a [TableScan] of a
     * [org.apache.calcite.schema.StreamableTable].
     *
     *
     * Very likely, the stream was only represented as a table for uniformity
     * with the other relations in the system. The Delta disappears and the stream
     * can be implemented directly.  */
    class DeltaTableScanRule
    /** Creates a DeltaTableScanRule.  */
    protected constructor(config: DeltaTableScanRuleConfig?) :
        RelRule<DeltaTableScanRule.DeltaTableScanRuleConfig?>(config), TransformationRule {
        @Override
        fun onMatch(call: RelOptRuleCall) {
            val delta: Delta = call.rel(0)
            val scan: TableScan = call.rel(1)
            val cluster: RelOptCluster = delta.getCluster()
            val relOptTable: RelOptTable = scan.getTable()
            val streamableTable: StreamableTable = relOptTable.unwrap(StreamableTable::class.java)
            if (streamableTable != null) {
                val table1: Table = streamableTable.stream()
                val relOptTable2: RelOptTable = RelOptTableImpl.create(
                    relOptTable.getRelOptSchema(),
                    relOptTable.getRowType(), table1,
                    ImmutableList.< String > builder < String ? > ()
                        .addAll(relOptTable.getQualifiedName())
                        .add("(STREAM)").build()
                )
                val newScan: LogicalTableScan = LogicalTableScan.create(cluster, relOptTable2, scan.getHints())
                call.transformTo(newScan)
            }
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface DeltaTableScanRuleConfig : RelRule.Config {
            @Override
            fun toRule(): DeltaTableScanRule? {
                return DeltaTableScanRule(this)
            }

            companion object {
                val DEFAULT: DeltaTableScanRuleConfig = ImmutableDeltaTableScanRuleConfig.of()
                    .withOperandSupplier { b0 ->
                        b0.operand(Delta::class.java).oneInput { b1 -> b1.operand(TableScan::class.java).anyInputs() }
                    }
            }
        }
    }

    /**
     * Planner rule that converts [Delta] over a [TableScan] of
     * a table other than [org.apache.calcite.schema.StreamableTable] to
     * an empty [Values].
     */
    class DeltaTableScanToEmptyRule
    /** Creates a DeltaTableScanToEmptyRule.  */
    protected constructor(config: DeltaTableScanToEmptyRuleConfig?) :
        RelRule<DeltaTableScanToEmptyRule.DeltaTableScanToEmptyRuleConfig?>(config), TransformationRule {
        @Override
        fun onMatch(call: RelOptRuleCall) {
            val delta: Delta = call.rel(0)
            val scan: TableScan = call.rel(1)
            val relOptTable: RelOptTable = scan.getTable()
            val streamableTable: StreamableTable = relOptTable.unwrap(StreamableTable::class.java)
            val builder: RelBuilder = call.builder()
            if (streamableTable == null) {
                call.transformTo(builder.values(delta.getRowType()).build())
            }
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface DeltaTableScanToEmptyRuleConfig : RelRule.Config {
            @Override
            fun toRule(): DeltaTableScanToEmptyRule? {
                return DeltaTableScanToEmptyRule(this)
            }

            companion object {
                val DEFAULT: ImmutableDeltaTableScanToEmptyRuleConfig = ImmutableDeltaTableScanToEmptyRuleConfig.of()
                    .withOperandSupplier { b0 ->
                        b0.operand(Delta::class.java).oneInput { b1 -> b1.operand(TableScan::class.java).anyInputs() }
                    }
            }
        }
    }

    /**
     * Planner rule that pushes a [Delta] through a [Join].
     *
     *
     * We apply something analogous to the
     * [product rule of
 * differential calculus](https://en.wikipedia.org/wiki/Product_rule) to implement the transpose:
     *
     * <blockquote>`stream(x join y)
     * x join stream(y) union all stream(x) join y`</blockquote>
     */
    class DeltaJoinTransposeRule
    /** Creates a DeltaJoinTransposeRule.  */
    protected constructor(config: DeltaJoinTransposeRuleConfig?) :
        RelRule<DeltaJoinTransposeRule.DeltaJoinTransposeRuleConfig?>(config), TransformationRule {
        @Deprecated // to be removed before 2.0
        constructor() : this(DeltaJoinTransposeRuleConfig.DEFAULT.toRule().config) {
        }

        @Override
        fun onMatch(call: RelOptRuleCall) {
            val delta: Delta = call.rel(0)
            Util.discard(delta)
            val join: Join = call.rel(1)
            val left: RelNode = join.getLeft()
            val right: RelNode = join.getRight()
            val rightWithDelta: LogicalDelta = LogicalDelta.create(right)
            val joinL: LogicalJoin = LogicalJoin.create(
                left,
                rightWithDelta,
                join.getHints(),
                join.getCondition(),
                join.getVariablesSet(),
                join.getJoinType(),
                join.isSemiJoinDone(),
                ImmutableList.copyOf(join.getSystemFieldList())
            )
            val leftWithDelta: LogicalDelta = LogicalDelta.create(left)
            val joinR: LogicalJoin = LogicalJoin.create(
                leftWithDelta,
                right,
                join.getHints(),
                join.getCondition(),
                join.getVariablesSet(),
                join.getJoinType(),
                join.isSemiJoinDone(),
                ImmutableList.copyOf(join.getSystemFieldList())
            )
            val inputsToUnion: List<RelNode> = ArrayList()
            inputsToUnion.add(joinL)
            inputsToUnion.add(joinR)
            val newNode: LogicalUnion = LogicalUnion.create(inputsToUnion, true)
            call.transformTo(newNode)
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface DeltaJoinTransposeRuleConfig : RelRule.Config {
            @Override
            fun toRule(): DeltaJoinTransposeRule {
                return DeltaJoinTransposeRule(this)
            }

            companion object {
                val DEFAULT: DeltaJoinTransposeRuleConfig = ImmutableDeltaJoinTransposeRuleConfig.of()
                    .withOperandSupplier { b0 ->
                        b0.operand(Delta::class.java).oneInput { b1 -> b1.operand(Join::class.java).anyInputs() }
                    }
            }
        }
    }
}
