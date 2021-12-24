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

import org.apache.calcite.linq4j.Ord

/**
 * Planner rule that pushes an
 * [org.apache.calcite.rel.core.Aggregate]
 * past a [org.apache.calcite.rel.core.Join].
 *
 * @see CoreRules.AGGREGATE_JOIN_TRANSPOSE
 *
 * @see CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED
 */
@Value.Enclosing
class AggregateJoinTransposeRule
/** Creates an AggregateJoinTransposeRule.  */
protected constructor(config: Config?) : RelRule<AggregateJoinTransposeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        aggregateClass: Class<out Aggregate?>?,
        joinClass: Class<out Join?>?, relBuilderFactory: RelBuilderFactory?,
        allowFunctions: Boolean
    ) : this(
        Config.DEFAULT
            .withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(aggregateClass, joinClass, allowFunctions)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        aggregateClass: Class<out Aggregate?>?,
        aggregateFactory: RelFactories.AggregateFactory?,
        joinClass: Class<out Join?>?,
        joinFactory: RelFactories.JoinFactory?
    ) : this(
        aggregateClass, joinClass,
        RelBuilder.proto(aggregateFactory, joinFactory), false
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        aggregateClass: Class<out Aggregate?>?,
        aggregateFactory: RelFactories.AggregateFactory?,
        joinClass: Class<out Join?>?,
        joinFactory: RelFactories.JoinFactory?,
        allowFunctions: Boolean
    ) : this(
        aggregateClass, joinClass,
        RelBuilder.proto(aggregateFactory, joinFactory), allowFunctions
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        aggregateClass: Class<out Aggregate?>?,
        aggregateFactory: RelFactories.AggregateFactory?,
        joinClass: Class<out Join?>?,
        joinFactory: RelFactories.JoinFactory?,
        projectFactory: RelFactories.ProjectFactory?
    ) : this(
        aggregateClass, joinClass,
        RelBuilder.proto(aggregateFactory, joinFactory, projectFactory), false
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        aggregateClass: Class<out Aggregate?>?,
        aggregateFactory: RelFactories.AggregateFactory?,
        joinClass: Class<out Join?>?,
        joinFactory: RelFactories.JoinFactory?,
        projectFactory: RelFactories.ProjectFactory?,
        allowFunctions: Boolean
    ) : this(
        aggregateClass, joinClass,
        RelBuilder.proto(aggregateFactory, joinFactory, projectFactory),
        allowFunctions
    ) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val aggregate: Aggregate = call.rel(0)
        val join: Join = call.rel(1)
        val rexBuilder: RexBuilder = aggregate.getCluster().getRexBuilder()
        val relBuilder: RelBuilder = call.builder()
        if (!isJoinSupported(join, aggregate)) {
            return
        }

        // Do the columns used by the join appear in the output of the aggregate?
        val aggregateColumns: ImmutableBitSet = aggregate.getGroupSet()
        val mq: RelMetadataQuery = call.getMetadataQuery()
        val keyColumns: ImmutableBitSet = keyColumns(
            aggregateColumns,
            mq.getPulledUpPredicates(join).pulledUpPredicates
        )
        val joinColumns: ImmutableBitSet = RelOptUtil.InputFinder.bits(join.getCondition())
        val allColumnsInAggregate: Boolean = keyColumns.contains(joinColumns)
        val belowAggregateColumns: ImmutableBitSet = aggregateColumns.union(joinColumns)

        // Split join condition
        val leftKeys: List<Integer> = ArrayList()
        val rightKeys: List<Integer> = ArrayList()
        val filterNulls: List<Boolean> = ArrayList()
        val nonEquiConj: RexNode = RelOptUtil.splitJoinCondition(
            join.getLeft(), join.getRight(),
            join.getCondition(), leftKeys, rightKeys, filterNulls
        )
        // If it contains non-equi join conditions, we bail out
        if (!nonEquiConj.isAlwaysTrue()) {
            return
        }

        // Push each aggregate function down to each side that contains all of its
        // arguments. Note that COUNT(*), because it has no arguments, can go to
        // both sides.
        val map: Map<Integer, Integer> = HashMap()
        val sides: List<Side> = ArrayList()
        var uniqueCount = 0
        var offset = 0
        var belowOffset = 0
        for (s in 0..1) {
            val side = Side()
            val joinInput: RelNode = join.getInput(s)
            val fieldCount: Int = joinInput.getRowType().getFieldCount()
            val fieldSet: ImmutableBitSet = ImmutableBitSet.range(offset, offset + fieldCount)
            val belowAggregateKeyNotShifted: ImmutableBitSet = belowAggregateColumns.intersect(fieldSet)
            for (c in Ord.zip(belowAggregateKeyNotShifted)) {
                map.put(c.e, belowOffset + c.i)
            }
            val mapping: Mappings.TargetMapping =
                if (s == 0) Mappings.createIdentity(fieldCount) else Mappings.createShiftMapping(
                    fieldCount + offset, 0, offset,
                    fieldCount
                )
            val belowAggregateKey: ImmutableBitSet = belowAggregateKeyNotShifted.shift(-offset)
            val unique: Boolean
            unique = if (!config.isAllowFunctions()) {
                assert(aggregate.getAggCallList().isEmpty())
                // If there are no functions, it doesn't matter as much whether we
                // aggregate the inputs before the join, because there will not be
                // any functions experiencing a cartesian product effect.
                //
                // But finding out whether the input is already unique requires a call
                // to areColumnsUnique that currently (until [CALCITE-1048] "Make
                // metadata more robust" is fixed) places a heavy load on
                // the metadata system.
                //
                // So we choose to imagine the the input is already unique, which is
                // untrue but harmless.
                //
                Util.discard(Bug.CALCITE_1048_FIXED)
                true
            } else {
                val unique0: Boolean = mq.areColumnsUnique(joinInput, belowAggregateKey)
                unique0 != null && unique0
            }
            if (unique) {
                ++uniqueCount
                side.aggregate = false
                relBuilder.push(joinInput)
                val projects: List<RexNode> = ArrayList()
                for (i in belowAggregateKey) {
                    projects.add(relBuilder.field(i))
                }
                for (aggCall in Ord.zip(aggregate.getAggCallList())) {
                    val aggregation: SqlAggFunction = aggCall.e.getAggregation()
                    val splitter: SqlSplittableAggFunction =
                        aggregation.unwrapOrThrow(SqlSplittableAggFunction::class.java)
                    if (!aggCall.e.getArgList().isEmpty()
                        && fieldSet.contains(ImmutableBitSet.of(aggCall.e.getArgList()))
                    ) {
                        val singleton: RexNode = splitter.singleton(
                            rexBuilder,
                            joinInput.getRowType(), aggCall.e.transform(mapping)
                        )
                        if (singleton is RexInputRef) {
                            val index: Int = (singleton as RexInputRef).getIndex()
                            if (!belowAggregateKey.get(index)) {
                                projects.add(singleton)
                                side.split.put(aggCall.i, projects.size() - 1)
                            } else {
                                side.split.put(aggCall.i, index)
                            }
                        } else {
                            projects.add(singleton)
                            side.split.put(aggCall.i, projects.size() - 1)
                        }
                    }
                }
                relBuilder.project(projects)
                side.newInput = relBuilder.build()
            } else {
                side.aggregate = true
                val belowAggCalls: List<AggregateCall> = ArrayList()
                val belowAggCallRegistry: Registry<AggregateCall> = registry<Any>(belowAggCalls)
                val oldGroupKeyCount: Int = aggregate.getGroupCount()
                val newGroupKeyCount: Int = belowAggregateKey.cardinality()
                for (aggCall in Ord.zip(aggregate.getAggCallList())) {
                    val aggregation: SqlAggFunction = aggCall.e.getAggregation()
                    val splitter: SqlSplittableAggFunction =
                        aggregation.unwrapOrThrow(SqlSplittableAggFunction::class.java)
                    val call1: AggregateCall
                    call1 = if (fieldSet.contains(ImmutableBitSet.of(aggCall.e.getArgList()))) {
                        val splitCall: AggregateCall = splitter.split(aggCall.e, mapping)
                        splitCall.adaptTo(
                            joinInput, splitCall.getArgList(),
                            splitCall.filterArg, oldGroupKeyCount, newGroupKeyCount
                        )
                    } else {
                        splitter.other(rexBuilder.getTypeFactory(), aggCall.e)
                    }
                    if (call1 != null) {
                        side.split.put(
                            aggCall.i, belowAggregateKey.cardinality()
                                    + belowAggCallRegistry.register(call1)
                        )
                    }
                }
                side.newInput = relBuilder.push(joinInput)
                    .aggregate(relBuilder.groupKey(belowAggregateKey), belowAggCalls)
                    .build()
            }
            offset += fieldCount
            belowOffset += side.newInput.getRowType().getFieldCount()
            sides.add(side)
        }
        if (uniqueCount == 2) {
            // Both inputs to the join are unique. There is nothing to be gained by
            // this rule. In fact, this aggregate+join may be the result of a previous
            // invocation of this rule; if we continue we might loop forever.
            return
        }

        // Update condition
        val mapping: Mapping = Mappings.target(
            map::get,
            join.getRowType().getFieldCount(),
            belowOffset
        ) as Mapping
        val newCondition: RexNode = RexUtil.apply(mapping, join.getCondition())

        // Create new join
        val side0: RelNode = requireNonNull(sides[0].newInput, "sides.get(0).newInput")
        relBuilder.push(side0)
            .push(requireNonNull(sides[1].newInput, "sides.get(1).newInput"))
            .join(join.getJoinType(), newCondition)

        // Aggregate above to sum up the sub-totals
        val newAggCalls: List<AggregateCall> = ArrayList()
        val groupCount: Int = aggregate.getGroupCount()
        val newLeftWidth: Int = side0.getRowType().getFieldCount()
        val projects: List<RexNode> = ArrayList(
            rexBuilder.identityProjects(relBuilder.peek().getRowType())
        )
        for (aggCall in Ord.zip(aggregate.getAggCallList())) {
            val aggregation: SqlAggFunction = aggCall.e.getAggregation()
            val splitter: SqlSplittableAggFunction = aggregation.unwrapOrThrow(SqlSplittableAggFunction::class.java)
            val leftSubTotal: Integer? = sides[0].split[aggCall.i]
            val rightSubTotal: Integer? = sides[1].split[aggCall.i]
            newAggCalls.add(
                splitter.topSplit(
                    rexBuilder, registry<Any>(projects),
                    groupCount, relBuilder.peek().getRowType(), aggCall.e,
                    if (leftSubTotal == null) -1 else leftSubTotal,
                    if (rightSubTotal == null) -1 else rightSubTotal + newLeftWidth
                )
            )
        }
        relBuilder.project(projects)
        var aggConvertedToProjects = false
        if (allColumnsInAggregate && join.getJoinType() !== JoinRelType.FULL) {
            // let's see if we can convert aggregate into projects
            // This shouldn't be done for FULL OUTER JOIN, aggregate on top is always required
            val projects2: List<RexNode> = ArrayList()
            for (key in Mappings.apply(mapping, aggregate.getGroupSet())) {
                projects2.add(relBuilder.field(key))
            }
            for (newAggCall in newAggCalls) {
                newAggCall.getAggregation().maybeUnwrap(SqlSplittableAggFunction::class.java)
                    .ifPresent { splitter ->
                        val rowType: RelDataType = relBuilder.peek().getRowType()
                        projects2.add(splitter.singleton(rexBuilder, rowType, newAggCall))
                    }
            }
            if (projects2.size()
                === aggregate.getGroupSet().cardinality() + newAggCalls.size()
            ) {
                // We successfully converted agg calls into projects.
                relBuilder.project(projects2)
                aggConvertedToProjects = true
            }
        }
        if (!aggConvertedToProjects) {
            relBuilder.aggregate(
                relBuilder.groupKey(
                    Mappings.apply(mapping, aggregate.getGroupSet()),
                    Mappings.apply2(mapping, aggregate.getGroupSets())
                ),
                newAggCalls
            )
        }
        call.transformTo(relBuilder.build())
    }

    /** Work space for an input to a join.  */
    private class Side {
        val split: Map<Integer, Integer> = HashMap()

        @Nullable
        var newInput: RelNode? = null
        var aggregate = false
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): AggregateJoinTransposeRule? {
            return AggregateJoinTransposeRule(this)
        }

        /** Whether to push down aggregate functions, default false.  */
        @get:Value.Default
        val isAllowFunctions: Boolean
            get() = false

        /** Sets [.isAllowFunctions].  */
        fun withAllowFunctions(allowFunctions: Boolean): Config

        /** Defines an operand tree for the given classes, and also sets
         * [.isAllowFunctions].  */
        fun withOperandFor(
            aggregateClass: Class<out Aggregate?>?,
            joinClass: Class<out Join?>?, allowFunctions: Boolean
        ): Config? {
            return withAllowFunctions(allowFunctions)
                .withOperandSupplier { b0 ->
                    b0.operand(aggregateClass)
                        .predicate { agg -> isAggregateSupported(agg, allowFunctions) }
                        .oneInput { b1 -> b1.operand(joinClass).anyInputs() }
                }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableAggregateJoinTransposeRule.Config.of()
                .withOperandFor(LogicalAggregate::class.java, LogicalJoin::class.java, false)

            /** Extended instance that can push down aggregate functions.  */
            val EXTENDED: Config = ImmutableAggregateJoinTransposeRule.Config.of()
                .withOperandFor(LogicalAggregate::class.java, LogicalJoin::class.java, true)
        }
    }

    companion object {
        private fun isAggregateSupported(
            aggregate: Aggregate,
            allowFunctions: Boolean
        ): Boolean {
            if (!allowFunctions && !aggregate.getAggCallList().isEmpty()) {
                return false
            }
            if (aggregate.getGroupType() !== Aggregate.Group.SIMPLE) {
                return false
            }
            // If any aggregate functions do not support splitting, bail out
            // If any aggregate call has a filter or is distinct, bail out
            for (aggregateCall in aggregate.getAggCallList()) {
                if (aggregateCall.getAggregation().unwrap(SqlSplittableAggFunction::class.java)
                    == null
                ) {
                    return false
                }
                if (aggregateCall.filterArg >= 0 || aggregateCall.isDistinct()) {
                    return false
                }
            }
            return true
        }

        // OUTER joins are supported for group by without aggregate functions
        // FULL OUTER JOIN is not supported since it could produce wrong result
        // due to bug (CALCITE-3012)
        private fun isJoinSupported(join: Join, aggregate: Aggregate): Boolean {
            return join.getJoinType() === JoinRelType.INNER || aggregate.getAggCallList().isEmpty()
        }

        /** Computes the closure of a set of columns according to a given list of
         * constraints. Each 'x = y' constraint causes bit y to be set if bit x is
         * set, and vice versa.  */
        private fun keyColumns(
            aggregateColumns: ImmutableBitSet,
            predicates: ImmutableList<RexNode>
        ): ImmutableBitSet {
            val equivalence: NavigableMap<Integer, BitSet> = TreeMap()
            for (predicate in predicates) {
                populateEquivalences(equivalence, predicate)
            }
            var keyColumns: ImmutableBitSet = aggregateColumns
            for (aggregateColumn in aggregateColumns) {
                val bitSet: BitSet = equivalence.get(aggregateColumn)
                if (bitSet != null) {
                    keyColumns = keyColumns.union(bitSet)
                }
            }
            return keyColumns
        }

        private fun populateEquivalences(
            equivalence: Map<Integer, BitSet>,
            predicate: RexNode
        ) {
            when (predicate.getKind()) {
                EQUALS -> {
                    val call: RexCall = predicate as RexCall
                    val operands: List<RexNode> = call.getOperands()
                    if (operands[0] is RexInputRef) {
                        val ref0: RexInputRef = operands[0] as RexInputRef
                        if (operands[1] is RexInputRef) {
                            val ref1: RexInputRef = operands[1] as RexInputRef
                            populateEquivalence(equivalence, ref0.getIndex(), ref1.getIndex())
                            populateEquivalence(equivalence, ref1.getIndex(), ref0.getIndex())
                        }
                    }
                }
                else -> {}
            }
        }

        private fun populateEquivalence(
            equivalence: Map<Integer, BitSet>,
            i0: Int, i1: Int
        ) {
            var bitSet: BitSet? = equivalence[i0]
            if (bitSet == null) {
                bitSet = BitSet()
                equivalence.put(i0, bitSet)
            }
            bitSet.set(i1)
        }

        /** Creates a [org.apache.calcite.sql.SqlSplittableAggFunction.Registry]
         * that is a view of a list.  */
        private fun <E> registry(
            list: List<E>
        ): Registry<E> {
            return Registry<E> { e ->
                var i = list.indexOf(e)
                if (i < 0) {
                    i = list.size()
                    list.add(e)
                }
                i
            }
        }
    }
}
