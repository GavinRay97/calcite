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
package org.apache.calcite.adapter.enumerable

import org.apache.calcite.plan.Convention

/**
 * Rule to convert a [LogicalAggregate] to an [EnumerableSortedAggregate].
 * You may provide a custom config to convert other nodes that extend [Aggregate].
 *
 * @see EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE
 */
class EnumerableSortedAggregateRule
/** Called from the Config.  */
protected constructor(config: Config?) : ConverterRule(config) {
    @Override
    @Nullable
    fun convert(rel: RelNode): RelNode? {
        val agg: Aggregate = rel as Aggregate
        if (!Aggregate.isSimple(agg)) {
            return null
        }
        val inputTraits: RelTraitSet = rel.getCluster()
            .traitSet().replace(EnumerableConvention.INSTANCE)
            .replace(
                RelCollations.of(
                    ImmutableIntList.copyOf(
                        agg.getGroupSet().asList()
                    )
                )
            )
        val selfTraits: RelTraitSet = inputTraits.replace(
            RelCollations.of(
                ImmutableIntList.identity(agg.getGroupSet().cardinality())
            )
        )
        return EnumerableSortedAggregate(
            rel.getCluster(),
            selfTraits,
            convert(agg.getInput(), inputTraits),
            agg.getGroupSet(),
            agg.getGroupSets(),
            agg.getAggCallList()
        )
    }

    companion object {
        /** Default configuration.  */
        val DEFAULT_CONFIG: Config = Config.INSTANCE
            .withConversion(
                LogicalAggregate::class.java, Convention.NONE,
                EnumerableConvention.INSTANCE, "EnumerableSortedAggregateRule"
            )
            .withRuleFactory { config: Config? -> EnumerableSortedAggregateRule(config) }
    }
}
