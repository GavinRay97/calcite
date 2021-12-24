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

import org.apache.calcite.plan.Convention

/**
 * `LogicalAggregate` is a relational operator which eliminates
 * duplicates and computes totals.
 *
 *
 * Rules:
 *
 *
 *  * [org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule]
 *  * [org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule]
 *  * [org.apache.calcite.rel.rules.AggregateReduceFunctionsRule].
 *
 */
class LogicalAggregate : Aggregate {
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a LogicalAggregate.
     *
     *
     * Use [.create] unless you know what you're doing.
     *
     * @param cluster    Cluster that this relational expression belongs to
     * @param traitSet   Traits
     * @param hints      Hints for this relational expression
     * @param input      Input relational expression
     * @param groupSet Bit set of grouping fields
     * @param groupSets Grouping sets, or null to use just `groupSet`
     * @param aggCalls Array of aggregates to compute, not null
     */
    constructor(
        cluster: RelOptCluster?,
        traitSet: RelTraitSet?,
        hints: List<RelHint?>?,
        input: RelNode?,
        groupSet: ImmutableBitSet?,
        @Nullable groupSets: List<ImmutableBitSet?>?,
        aggCalls: List<AggregateCall?>?
    ) : super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster?,
        traitSet: RelTraitSet?,
        input: RelNode?,
        groupSet: ImmutableBitSet?,
        groupSets: List<ImmutableBitSet?>?,
        aggCalls: List<AggregateCall?>?
    ) : this(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster?, traitSet: RelTraitSet?,
        input: RelNode?, indicator: Boolean, groupSet: ImmutableBitSet?,
        groupSets: List<ImmutableBitSet?>?, aggCalls: List<AggregateCall?>?
    ) : super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls) {
        checkIndicator(indicator)
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster,
        input: RelNode?, indicator: Boolean, groupSet: ImmutableBitSet?,
        groupSets: List<ImmutableBitSet?>?, aggCalls: List<AggregateCall?>?
    ) : super(
        cluster, cluster.traitSetOf(Convention.NONE), ImmutableList.of(), input, groupSet,
        groupSets, aggCalls
    ) {
        checkIndicator(indicator)
    }

    /**
     * Creates a LogicalAggregate by parsing serialized output.
     */
    constructor(input: RelInput?) : super(input) {}

    //~ Methods ----------------------------------------------------------------
    @Override
    fun copy(
        traitSet: RelTraitSet, input: RelNode?,
        groupSet: ImmutableBitSet?,
        @Nullable groupSets: List<ImmutableBitSet?>?, aggCalls: List<AggregateCall?>?
    ): LogicalAggregate {
        assert(traitSet.containsIfApplicable(Convention.NONE))
        return LogicalAggregate(
            getCluster(), traitSet, hints, input,
            groupSet, groupSets, aggCalls
        )
    }

    @Override
    fun accept(shuttle: RelShuttle): RelNode {
        return shuttle.visit(this)
    }

    @Override
    fun withHints(hintList: List<RelHint?>?): RelNode {
        return LogicalAggregate(
            getCluster(), traitSet, hintList, input,
            groupSet, groupSets, aggCalls
        )
    }

    companion object {
        /** Creates a LogicalAggregate.  */
        fun create(
            input: RelNode,
            hints: List<RelHint>,
            groupSet: ImmutableBitSet,
            @Nullable groupSets: List<ImmutableBitSet>,
            aggCalls: List<AggregateCall>
        ): LogicalAggregate {
            return create_(input, hints, groupSet, groupSets, aggCalls)
        }

        @Deprecated // to be removed before 2.0
        fun create(
            input: RelNode,
            groupSet: ImmutableBitSet,
            groupSets: List<ImmutableBitSet>,
            aggCalls: List<AggregateCall>
        ): LogicalAggregate {
            return create_(input, ImmutableList.of(), groupSet, groupSets, aggCalls)
        }

        @Deprecated // to be removed before 2.0
        fun create(
            input: RelNode,
            indicator: Boolean,
            groupSet: ImmutableBitSet,
            groupSets: List<ImmutableBitSet>,
            aggCalls: List<AggregateCall>
        ): LogicalAggregate {
            checkIndicator(indicator)
            return create_(input, ImmutableList.of(), groupSet, groupSets, aggCalls)
        }

        private fun create_(
            input: RelNode,
            hints: List<RelHint>,
            groupSet: ImmutableBitSet,
            @Nullable groupSets: List<ImmutableBitSet>,
            aggCalls: List<AggregateCall>
        ): LogicalAggregate {
            val cluster: RelOptCluster = input.getCluster()
            val traitSet: RelTraitSet = cluster.traitSetOf(Convention.NONE)
            return LogicalAggregate(
                cluster, traitSet, hints, input, groupSet,
                groupSets, aggCalls
            )
        }
    }
}
