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
 * Rule to convert a [LogicalAggregate] to an [EnumerableAggregate].
 * You may provide a custom config to convert other nodes that extend [Aggregate].
 *
 * @see EnumerableRules.ENUMERABLE_AGGREGATE_RULE
 */
class EnumerableAggregateRule
/** Called from the Config.  */
protected constructor(config: Config?) : ConverterRule(config) {
    @Override
    @Nullable
    fun convert(rel: RelNode): RelNode? {
        val agg: Aggregate = rel as Aggregate
        val traitSet: RelTraitSet = rel.getCluster()
            .traitSet().replace(EnumerableConvention.INSTANCE)
        return try {
            EnumerableAggregate(
                rel.getCluster(),
                traitSet,
                convert(agg.getInput(), traitSet),
                agg.getGroupSet(),
                agg.getGroupSets(),
                agg.getAggCallList()
            )
        } catch (e: InvalidRelException) {
            EnumerableRules.LOGGER.debug(e.toString())
            null
        }
    }

    companion object {
        /** Default configuration.  */
        val DEFAULT_CONFIG: Config = Config.INSTANCE
            .withConversion(
                LogicalAggregate::class.java, Convention.NONE,
                EnumerableConvention.INSTANCE, "EnumerableAggregateRule"
            )
            .withRuleFactory { config: Config? -> EnumerableAggregateRule(config) }
    }
}
