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

import org.apache.calcite.rel.core.Aggregate

/** Mutable equivalent of [org.apache.calcite.rel.core.Aggregate].  */
class MutableAggregate private constructor(
    input: MutableRel, rowType: RelDataType,
    groupSet: ImmutableBitSet,
    @Nullable groupSets: List<ImmutableBitSet?>?, aggCalls: List<AggregateCall>
) : MutableSingleRel(MutableRelType.AGGREGATE, rowType, input) {
    val groupSet: ImmutableBitSet
    val groupSets: ImmutableList<ImmutableBitSet?>
    val aggCalls: List<AggregateCall>

    init {
        this.groupSet = groupSet
        this.groupSets = if (groupSets == null) ImmutableList.of(groupSet) else ImmutableList.copyOf(groupSets)
        assert(
            ImmutableBitSet.ORDERING.isStrictlyOrdered(
                this.groupSets
            )
        ) { this.groupSets }
        for (set in this.groupSets) {
            assert(groupSet.contains(set))
        }
        this.aggCalls = aggCalls
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (obj === this
                || (obj is MutableAggregate
                && groupSet.equals((obj as MutableAggregate).groupSet)
                && groupSets.equals((obj as MutableAggregate).groupSets)
                && aggCalls.equals((obj as MutableAggregate).aggCalls)
                && input!!.equals((obj as MutableAggregate).input)))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(input, groupSet, groupSets, aggCalls)
    }

    @Override
    override fun digest(buf: StringBuilder): StringBuilder {
        return buf.append("Aggregate(groupSet: ").append(groupSet)
            .append(", groupSets: ").append(groupSets)
            .append(", calls: ").append(aggCalls).append(")")
    }

    val groupType: Aggregate.Group
        get() = Aggregate.Group.induce(groupSet, groupSets)

    @Override
    override fun clone(): MutableRel {
        return of(input!!.clone(), groupSet, groupSets, aggCalls)
    }

    companion object {
        /**
         * Creates a MutableAggregate.
         *
         * @param input     Input relational expression
         * @param groupSet  Bit set of grouping fields
         * @param groupSets List of all grouping sets; null for just `groupSet`
         * @param aggCalls  Collection of calls to aggregate functions
         */
        fun of(
            input: MutableRel, groupSet: ImmutableBitSet,
            @Nullable groupSets: ImmutableList<ImmutableBitSet?>?, aggCalls: List<AggregateCall>
        ): MutableAggregate {
            val rowType: RelDataType = Aggregate.deriveRowType(
                input.cluster.getTypeFactory(),
                input.rowType, false, groupSet, groupSets, aggCalls
            )
            return MutableAggregate(
                input, rowType, groupSet,
                groupSets, aggCalls
            )
        }
    }
}
