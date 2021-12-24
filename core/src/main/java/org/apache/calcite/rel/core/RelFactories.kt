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
package org.apache.calcite.rel.core

import org.apache.calcite.linq4j.function.Experimental

/**
 * Contains factory interface and default implementation for creating various
 * rel nodes.
 */
object RelFactories {
    val DEFAULT_PROJECT_FACTORY: ProjectFactory = ProjectFactoryImpl()
    val DEFAULT_FILTER_FACTORY: FilterFactory = FilterFactoryImpl()
    val DEFAULT_JOIN_FACTORY: JoinFactory = JoinFactoryImpl()
    val DEFAULT_CORRELATE_FACTORY: CorrelateFactory = CorrelateFactoryImpl()
    val DEFAULT_SORT_FACTORY: SortFactory = SortFactoryImpl()
    val DEFAULT_EXCHANGE_FACTORY: ExchangeFactory = ExchangeFactoryImpl()
    val DEFAULT_SORT_EXCHANGE_FACTORY: SortExchangeFactory = SortExchangeFactoryImpl()
    val DEFAULT_AGGREGATE_FACTORY: AggregateFactory = AggregateFactoryImpl()
    val DEFAULT_MATCH_FACTORY: MatchFactory = MatchFactoryImpl()
    val DEFAULT_SET_OP_FACTORY: SetOpFactory = SetOpFactoryImpl()
    val DEFAULT_VALUES_FACTORY: ValuesFactory = ValuesFactoryImpl()
    val DEFAULT_TABLE_SCAN_FACTORY: TableScanFactory = TableScanFactoryImpl()
    val DEFAULT_TABLE_FUNCTION_SCAN_FACTORY: TableFunctionScanFactory = TableFunctionScanFactoryImpl()
    val DEFAULT_SNAPSHOT_FACTORY: SnapshotFactory = SnapshotFactoryImpl()
    val DEFAULT_SPOOL_FACTORY: SpoolFactory = SpoolFactoryImpl()
    val DEFAULT_REPEAT_UNION_FACTORY: RepeatUnionFactory = RepeatUnionFactoryImpl()
    val DEFAULT_STRUCT = Struct(
        DEFAULT_FILTER_FACTORY,
        DEFAULT_PROJECT_FACTORY,
        DEFAULT_AGGREGATE_FACTORY,
        DEFAULT_SORT_FACTORY,
        DEFAULT_EXCHANGE_FACTORY,
        DEFAULT_SORT_EXCHANGE_FACTORY,
        DEFAULT_SET_OP_FACTORY,
        DEFAULT_JOIN_FACTORY,
        DEFAULT_CORRELATE_FACTORY,
        DEFAULT_VALUES_FACTORY,
        DEFAULT_TABLE_SCAN_FACTORY,
        DEFAULT_TABLE_FUNCTION_SCAN_FACTORY,
        DEFAULT_SNAPSHOT_FACTORY,
        DEFAULT_MATCH_FACTORY,
        DEFAULT_SPOOL_FACTORY,
        DEFAULT_REPEAT_UNION_FACTORY
    )

    /** A [RelBuilderFactory] that creates a [RelBuilder] that will
     * create logical relational expressions for everything.  */
    val LOGICAL_BUILDER: RelBuilderFactory = RelBuilder.proto(Contexts.of(DEFAULT_STRUCT))

    /**
     * Can create a
     * [org.apache.calcite.rel.logical.LogicalProject] of the
     * appropriate type for this rule's calling convention.
     */
    interface ProjectFactory {
        /**
         * Creates a project.
         *
         * @param input The input
         * @param hints The hints
         * @param childExprs The projection expressions
         * @param fieldNames The projection field names
         * @return a project
         */
        fun createProject(
            input: RelNode?, hints: List<RelHint?>?,
            childExprs: List<RexNode?>?, @Nullable fieldNames: List<String?>?
        ): RelNode?
    }

    /**
     * Implementation of [ProjectFactory] that returns a vanilla
     * [org.apache.calcite.rel.logical.LogicalProject].
     */
    private class ProjectFactoryImpl : ProjectFactory {
        @Override
        override fun createProject(
            input: RelNode?, hints: List<RelHint?>?,
            childExprs: List<RexNode?>?, @Nullable fieldNames: List<String?>?
        ): RelNode {
            return LogicalProject.create(input, hints, childExprs, fieldNames)
        }
    }

    /**
     * Can create a [Sort] of the appropriate type
     * for this rule's calling convention.
     */
    interface SortFactory {
        /** Creates a sort.  */
        fun createSort(
            input: RelNode?, collation: RelCollation?, @Nullable offset: RexNode?,
            @Nullable fetch: RexNode?
        ): RelNode?

        @Deprecated // to be removed before 2.0
        fun createSort(
            traitSet: RelTraitSet?, input: RelNode?,
            collation: RelCollation?, @Nullable offset: RexNode?, @Nullable fetch: RexNode?
        ): RelNode? {
            return createSort(input, collation, offset, fetch)
        }
    }

    /**
     * Implementation of [RelFactories.SortFactory] that
     * returns a vanilla [Sort].
     */
    private class SortFactoryImpl : SortFactory {
        @Override
        override fun createSort(
            input: RelNode?, collation: RelCollation?,
            @Nullable offset: RexNode?, @Nullable fetch: RexNode?
        ): RelNode {
            return LogicalSort.create(input, collation, offset, fetch)
        }
    }

    /**
     * Can create a [org.apache.calcite.rel.core.Exchange]
     * of the appropriate type for a rule's calling convention.
     */
    interface ExchangeFactory {
        /** Creates an Exchange.  */
        fun createExchange(input: RelNode?, distribution: RelDistribution?): RelNode?
    }

    /**
     * Implementation of
     * [RelFactories.ExchangeFactory]
     * that returns a [Exchange].
     */
    private class ExchangeFactoryImpl : ExchangeFactory {
        @Override
        override fun createExchange(
            input: RelNode?, distribution: RelDistribution?
        ): RelNode {
            return LogicalExchange.create(input, distribution)
        }
    }

    /**
     * Can create a [SortExchange]
     * of the appropriate type for a rule's calling convention.
     */
    interface SortExchangeFactory {
        /**
         * Creates a [SortExchange].
         */
        fun createSortExchange(
            input: RelNode?,
            distribution: RelDistribution?,
            collation: RelCollation?
        ): RelNode?
    }

    /**
     * Implementation of
     * [RelFactories.SortExchangeFactory]
     * that returns a [SortExchange].
     */
    private class SortExchangeFactoryImpl : SortExchangeFactory {
        @Override
        override fun createSortExchange(
            input: RelNode?,
            distribution: RelDistribution?,
            collation: RelCollation?
        ): RelNode {
            return LogicalSortExchange.create(input, distribution, collation)
        }
    }

    /**
     * Can create a [SetOp] for a particular kind of
     * set operation (UNION, EXCEPT, INTERSECT) and of the appropriate type
     * for this rule's calling convention.
     */
    interface SetOpFactory {
        /** Creates a set operation.  */
        fun createSetOp(kind: SqlKind?, inputs: List<RelNode?>?, all: Boolean): RelNode?
    }

    /**
     * Implementation of [RelFactories.SetOpFactory] that
     * returns a vanilla [SetOp] for the particular kind of set
     * operation (UNION, EXCEPT, INTERSECT).
     */
    private class SetOpFactoryImpl : SetOpFactory {
        @Override
        override fun createSetOp(
            kind: SqlKind, inputs: List<RelNode?>?,
            all: Boolean
        ): RelNode {
            return when (kind) {
                UNION -> LogicalUnion.create(inputs, all)
                EXCEPT -> LogicalMinus.create(inputs, all)
                INTERSECT -> LogicalIntersect.create(inputs, all)
                else -> throw AssertionError("not a set op: $kind")
            }
        }
    }

    /**
     * Can create a [LogicalAggregate] of the appropriate type
     * for this rule's calling convention.
     */
    interface AggregateFactory {
        /** Creates an aggregate.  */
        fun createAggregate(
            input: RelNode?, hints: List<RelHint?>?, groupSet: ImmutableBitSet?,
            groupSets: ImmutableList<ImmutableBitSet?>?, aggCalls: List<AggregateCall?>?
        ): RelNode?
    }

    /**
     * Implementation of [RelFactories.AggregateFactory]
     * that returns a vanilla [LogicalAggregate].
     */
    private class AggregateFactoryImpl : AggregateFactory {
        @Override
        override fun createAggregate(
            input: RelNode?, hints: List<RelHint?>?,
            groupSet: ImmutableBitSet?, groupSets: ImmutableList<ImmutableBitSet?>?,
            aggCalls: List<AggregateCall?>?
        ): RelNode {
            return LogicalAggregate.create(input, hints, groupSet, groupSets, aggCalls)
        }
    }

    /**
     * Can create a [Filter] of the appropriate type
     * for this rule's calling convention.
     */
    interface FilterFactory {
        /** Creates a filter.
         *
         *
         * Some implementations of `Filter` do not support correlation
         * variables, and for these, this method will throw if `variablesSet`
         * is not empty.
         *
         * @param input Input relational expression
         * @param condition Filter condition; only rows for which this condition
         * evaluates to TRUE will be emitted
         * @param variablesSet Correlating variables that are set when reading
         * a row from the input, and which may be referenced from inside the
         * condition
         */
        fun createFilter(
            input: RelNode?, condition: RexNode?,
            variablesSet: Set<CorrelationId?>?
        ): RelNode?

        @Deprecated // to be removed before 2.0
        fun createFilter(input: RelNode?, condition: RexNode?): RelNode? {
            return createFilter(input, condition, ImmutableSet.of())
        }
    }

    /**
     * Implementation of [RelFactories.FilterFactory] that
     * returns a vanilla [LogicalFilter].
     */
    private class FilterFactoryImpl : FilterFactory {
        @Override
        override fun createFilter(
            input: RelNode?, condition: RexNode?,
            variablesSet: Set<CorrelationId?>?
        ): RelNode {
            return LogicalFilter.create(
                input, condition,
                ImmutableSet.copyOf(variablesSet)
            )
        }
    }

    /**
     * Can create a join of the appropriate type for a rule's calling convention.
     *
     *
     * The result is typically a [Join].
     */
    interface JoinFactory {
        /**
         * Creates a join.
         *
         * @param left             Left input
         * @param right            Right input
         * @param hints            Hints
         * @param condition        Join condition
         * @param variablesSet     Set of variables that are set by the
         * LHS and used by the RHS and are not available to
         * nodes above this LogicalJoin in the tree
         * @param joinType         Join type
         * @param semiJoinDone     Whether this join has been translated to a
         * semi-join
         */
        fun createJoin(
            left: RelNode?, right: RelNode?, hints: List<RelHint?>?,
            condition: RexNode?, variablesSet: Set<CorrelationId?>?, joinType: JoinRelType?,
            semiJoinDone: Boolean
        ): RelNode?
    }

    /**
     * Implementation of [JoinFactory] that returns a vanilla
     * [org.apache.calcite.rel.logical.LogicalJoin].
     */
    private class JoinFactoryImpl : JoinFactory {
        @Override
        override fun createJoin(
            left: RelNode?, right: RelNode?, hints: List<RelHint?>?,
            condition: RexNode?, variablesSet: Set<CorrelationId?>?,
            joinType: JoinRelType?, semiJoinDone: Boolean
        ): RelNode {
            return LogicalJoin.create(
                left, right, hints, condition, variablesSet, joinType,
                semiJoinDone, ImmutableList.of()
            )
        }
    }

    /**
     * Can create a correlate of the appropriate type for a rule's calling
     * convention.
     *
     *
     * The result is typically a [Correlate].
     */
    interface CorrelateFactory {
        /**
         * Creates a correlate.
         *
         * @param left             Left input
         * @param right            Right input
         * @param correlationId    Variable name for the row of left input
         * @param requiredColumns  Required columns
         * @param joinType         Join type
         */
        fun createCorrelate(
            left: RelNode?, right: RelNode?,
            correlationId: CorrelationId?, requiredColumns: ImmutableBitSet?,
            joinType: JoinRelType?
        ): RelNode?
    }

    /**
     * Implementation of [CorrelateFactory] that returns a vanilla
     * [org.apache.calcite.rel.logical.LogicalCorrelate].
     */
    private class CorrelateFactoryImpl : CorrelateFactory {
        @Override
        override fun createCorrelate(
            left: RelNode?, right: RelNode?,
            correlationId: CorrelationId?, requiredColumns: ImmutableBitSet?,
            joinType: JoinRelType?
        ): RelNode {
            return LogicalCorrelate.create(
                left, right, correlationId,
                requiredColumns, joinType
            )
        }
    }

    /**
     * Can create a semi-join of the appropriate type for a rule's calling
     * convention.
     *
     */
    @Deprecated // to be removed before 2.0
    @Deprecated("Use {@link JoinFactory} instead.")
    interface SemiJoinFactory {
        /**
         * Creates a semi-join.
         *
         * @param left             Left input
         * @param right            Right input
         * @param condition        Join condition
         */
        fun createSemiJoin(left: RelNode?, right: RelNode?, condition: RexNode?): RelNode?
    }

    /**
     * Can create a [Values] of the appropriate type for a rule's calling
     * convention.
     */
    interface ValuesFactory {
        /**
         * Creates a Values.
         */
        fun createValues(
            cluster: RelOptCluster?, rowType: RelDataType?,
            tuples: List<ImmutableList<RexLiteral?>?>?
        ): RelNode?
    }

    /**
     * Implementation of [ValuesFactory] that returns a
     * [LogicalValues].
     */
    private class ValuesFactoryImpl : ValuesFactory {
        @Override
        override fun createValues(
            cluster: RelOptCluster?, rowType: RelDataType?,
            tuples: List<ImmutableList<RexLiteral?>?>?
        ): RelNode {
            return LogicalValues.create(
                cluster, rowType,
                ImmutableList.copyOf(tuples)
            )
        }
    }

    /**
     * Can create a [TableScan] of the appropriate type for a rule's calling
     * convention.
     */
    interface TableScanFactory {
        /**
         * Creates a [TableScan].
         */
        fun createScan(toRelContext: RelOptTable.ToRelContext?, table: RelOptTable?): RelNode?
    }

    /**
     * Implementation of [TableScanFactory] that returns a
     * [LogicalTableScan].
     */
    private class TableScanFactoryImpl : TableScanFactory {
        @Override
        override fun createScan(toRelContext: RelOptTable.ToRelContext?, table: RelOptTable): RelNode {
            return table.toRel(toRelContext)
        }
    }

    /**
     * Can create a [TableFunctionScan]
     * of the appropriate type for a rule's calling convention.
     */
    interface TableFunctionScanFactory {
        /** Creates a [TableFunctionScan].  */
        fun createTableFunctionScan(
            cluster: RelOptCluster?,
            inputs: List<RelNode?>?, call: RexCall?, @Nullable elementType: Type?,
            @Nullable columnMappings: Set<RelColumnMapping?>?
        ): RelNode?
    }

    /**
     * Implementation of
     * [TableFunctionScanFactory]
     * that returns a [TableFunctionScan].
     */
    private class TableFunctionScanFactoryImpl : TableFunctionScanFactory {
        @Override
        override fun createTableFunctionScan(
            cluster: RelOptCluster,
            inputs: List<RelNode?>?, call: RexCall, @Nullable elementType: Type?,
            @Nullable columnMappings: Set<RelColumnMapping?>?
        ): RelNode {
            val rowType: RelDataType
            // To deduce the return type:
            // 1. if the operator implements SqlTableFunction,
            // use the SqlTableFunction's return type inference;
            // 2. else use the call's type, e.g. the operator may has
            // its custom way for return type inference.
            rowType = if (call.getOperator() is SqlTableFunction) {
                val callBinding: SqlOperatorBinding = RexCallBinding(
                    cluster.getTypeFactory(), call.getOperator(),
                    call.operands, ImmutableList.of()
                )
                val operator: SqlTableFunction = call.getOperator() as SqlTableFunction
                val rowTypeInference: SqlReturnTypeInference = operator.getRowTypeInference()
                rowTypeInference.inferReturnType(callBinding)
            } else {
                call.getType()
            }
            return LogicalTableFunctionScan.create(
                cluster, inputs, call,
                elementType, requireNonNull(rowType, "rowType"), columnMappings
            )
        }
    }

    /**
     * Can create a [Snapshot] of
     * the appropriate type for a rule's calling convention.
     */
    interface SnapshotFactory {
        /**
         * Creates a [Snapshot].
         */
        fun createSnapshot(input: RelNode?, period: RexNode?): RelNode?
    }

    /**
     * Implementation of [RelFactories.SnapshotFactory] that
     * returns a vanilla [LogicalSnapshot].
     */
    class SnapshotFactoryImpl : SnapshotFactory {
        @Override
        override fun createSnapshot(input: RelNode?, period: RexNode?): RelNode {
            return LogicalSnapshot.create(input, period)
        }
    }

    /**
     * Can create a [Match] of
     * the appropriate type for a rule's calling convention.
     */
    interface MatchFactory {
        /** Creates a [Match].  */
        fun createMatch(
            input: RelNode?, pattern: RexNode?,
            rowType: RelDataType?, strictStart: Boolean, strictEnd: Boolean,
            patternDefinitions: Map<String?, RexNode?>?, measures: Map<String?, RexNode?>?,
            after: RexNode?, subsets: Map<String?, SortedSet<String?>?>?,
            allRows: Boolean, partitionKeys: ImmutableBitSet?, orderKeys: RelCollation?,
            @Nullable interval: RexNode?
        ): RelNode?
    }

    /**
     * Implementation of [MatchFactory]
     * that returns a [LogicalMatch].
     */
    private class MatchFactoryImpl : MatchFactory {
        @Override
        override fun createMatch(
            input: RelNode?, pattern: RexNode?,
            rowType: RelDataType?, strictStart: Boolean, strictEnd: Boolean,
            patternDefinitions: Map<String?, RexNode?>?, measures: Map<String?, RexNode?>?,
            after: RexNode?, subsets: Map<String?, SortedSet<String?>?>?,
            allRows: Boolean, partitionKeys: ImmutableBitSet?, orderKeys: RelCollation?,
            @Nullable interval: RexNode?
        ): RelNode {
            return LogicalMatch.create(
                input, rowType, pattern, strictStart,
                strictEnd, patternDefinitions, measures, after, subsets, allRows,
                partitionKeys, orderKeys, interval
            )
        }
    }

    /**
     * Can create a [Spool] of
     * the appropriate type for a rule's calling convention.
     */
    @Experimental
    interface SpoolFactory {
        /** Creates a [TableSpool].  */
        fun createTableSpool(
            input: RelNode?, readType: Spool.Type?,
            writeType: Spool.Type?, table: RelOptTable?
        ): RelNode?
    }

    /**
     * Implementation of [SpoolFactory]
     * that returns Logical Spools.
     */
    private class SpoolFactoryImpl : SpoolFactory {
        @Override
        override fun createTableSpool(
            input: RelNode?, readType: Spool.Type?,
            writeType: Spool.Type?, table: RelOptTable?
        ): RelNode {
            return LogicalTableSpool.create(input, readType, writeType, table)
        }
    }

    /**
     * Can create a [RepeatUnion] of
     * the appropriate type for a rule's calling convention.
     */
    @Experimental
    interface RepeatUnionFactory {
        /** Creates a [RepeatUnion].  */
        fun createRepeatUnion(
            seed: RelNode?, iterative: RelNode?, all: Boolean,
            iterationLimit: Int
        ): RelNode?
    }

    /**
     * Implementation of [RepeatUnion]
     * that returns a [LogicalRepeatUnion].
     */
    private class RepeatUnionFactoryImpl : RepeatUnionFactory {
        @Override
        override fun createRepeatUnion(
            seed: RelNode?, iterative: RelNode?,
            all: Boolean, iterationLimit: Int
        ): RelNode {
            return LogicalRepeatUnion.create(seed, iterative, all, iterationLimit)
        }
    }

    /** Immutable record that contains an instance of each factory.  */
    class Struct(
        filterFactory: FilterFactory,
        projectFactory: ProjectFactory,
        aggregateFactory: AggregateFactory,
        sortFactory: SortFactory,
        exchangeFactory: ExchangeFactory,
        sortExchangeFactory: SortExchangeFactory,
        setOpFactory: SetOpFactory,
        joinFactory: JoinFactory,
        correlateFactory: CorrelateFactory,
        valuesFactory: ValuesFactory,
        scanFactory: TableScanFactory,
        tableFunctionScanFactory: TableFunctionScanFactory,
        snapshotFactory: SnapshotFactory,
        matchFactory: MatchFactory,
        spoolFactory: SpoolFactory,
        repeatUnionFactory: RepeatUnionFactory
    ) {
        val filterFactory: FilterFactory
        val projectFactory: ProjectFactory
        val aggregateFactory: AggregateFactory
        val sortFactory: SortFactory
        val exchangeFactory: ExchangeFactory
        val sortExchangeFactory: SortExchangeFactory
        val setOpFactory: SetOpFactory
        val joinFactory: JoinFactory
        val correlateFactory: CorrelateFactory
        val valuesFactory: ValuesFactory
        val scanFactory: TableScanFactory
        val tableFunctionScanFactory: TableFunctionScanFactory
        val snapshotFactory: SnapshotFactory
        val matchFactory: MatchFactory
        val spoolFactory: SpoolFactory
        val repeatUnionFactory: RepeatUnionFactory

        init {
            this.filterFactory = requireNonNull(filterFactory, "filterFactory")
            this.projectFactory = requireNonNull(projectFactory, "projectFactory")
            this.aggregateFactory = requireNonNull(aggregateFactory, "aggregateFactory")
            this.sortFactory = requireNonNull(sortFactory, "sortFactory")
            this.exchangeFactory = requireNonNull(exchangeFactory, "exchangeFactory")
            this.sortExchangeFactory = requireNonNull(sortExchangeFactory, "sortExchangeFactory")
            this.setOpFactory = requireNonNull(setOpFactory, "setOpFactory")
            this.joinFactory = requireNonNull(joinFactory, "joinFactory")
            this.correlateFactory = requireNonNull(correlateFactory, "correlateFactory")
            this.valuesFactory = requireNonNull(valuesFactory, "valuesFactory")
            this.scanFactory = requireNonNull(scanFactory, "scanFactory")
            this.tableFunctionScanFactory = requireNonNull(tableFunctionScanFactory, "tableFunctionScanFactory")
            this.snapshotFactory = requireNonNull(snapshotFactory, "snapshotFactory")
            this.matchFactory = requireNonNull(matchFactory, "matchFactory")
            this.spoolFactory = requireNonNull(spoolFactory, "spoolFactory")
            this.repeatUnionFactory = requireNonNull(repeatUnionFactory, "repeatUnionFactory")
        }

        companion object {
            fun fromContext(context: Context): Struct {
                val struct: Struct = context.unwrap(Struct::class.java)
                return struct
                    ?: Struct(
                        context.maybeUnwrap(FilterFactory::class.java)
                            .orElse(DEFAULT_FILTER_FACTORY),
                        context.maybeUnwrap(ProjectFactory::class.java)
                            .orElse(DEFAULT_PROJECT_FACTORY),
                        context.maybeUnwrap(AggregateFactory::class.java)
                            .orElse(DEFAULT_AGGREGATE_FACTORY),
                        context.maybeUnwrap(SortFactory::class.java)
                            .orElse(DEFAULT_SORT_FACTORY),
                        context.maybeUnwrap(ExchangeFactory::class.java)
                            .orElse(DEFAULT_EXCHANGE_FACTORY),
                        context.maybeUnwrap(SortExchangeFactory::class.java)
                            .orElse(DEFAULT_SORT_EXCHANGE_FACTORY),
                        context.maybeUnwrap(SetOpFactory::class.java)
                            .orElse(DEFAULT_SET_OP_FACTORY),
                        context.maybeUnwrap(JoinFactory::class.java)
                            .orElse(DEFAULT_JOIN_FACTORY),
                        context.maybeUnwrap(CorrelateFactory::class.java)
                            .orElse(DEFAULT_CORRELATE_FACTORY),
                        context.maybeUnwrap(ValuesFactory::class.java)
                            .orElse(DEFAULT_VALUES_FACTORY),
                        context.maybeUnwrap(TableScanFactory::class.java)
                            .orElse(DEFAULT_TABLE_SCAN_FACTORY),
                        context.maybeUnwrap(TableFunctionScanFactory::class.java)
                            .orElse(DEFAULT_TABLE_FUNCTION_SCAN_FACTORY),
                        context.maybeUnwrap(SnapshotFactory::class.java)
                            .orElse(DEFAULT_SNAPSHOT_FACTORY),
                        context.maybeUnwrap(MatchFactory::class.java)
                            .orElse(DEFAULT_MATCH_FACTORY),
                        context.maybeUnwrap(SpoolFactory::class.java)
                            .orElse(DEFAULT_SPOOL_FACTORY),
                        context.maybeUnwrap(RepeatUnionFactory::class.java)
                            .orElse(DEFAULT_REPEAT_UNION_FACTORY)
                    )
            }
        }
    }
}
