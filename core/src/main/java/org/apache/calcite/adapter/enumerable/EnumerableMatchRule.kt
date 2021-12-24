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
 * Rule to convert a [LogicalMatch] to an [EnumerableMatch].
 * You may provide a custom config to convert other nodes that extend [Match].
 *
 * @see EnumerableRules.ENUMERABLE_MATCH_RULE
 */
class EnumerableMatchRule
/** Creates an EnumerableMatchRule.  */
protected constructor(config: Config?) : ConverterRule(config) {
    @Override
    fun convert(rel: RelNode): RelNode {
        val match: Match = rel as Match
        return EnumerableMatch.create(
            convert(
                match.getInput(),
                match.getInput().getTraitSet()
                    .replace(EnumerableConvention.INSTANCE)
            ),
            match.getRowType(),
            match.getPattern(), match.isStrictStart(), match.isStrictEnd(),
            match.getPatternDefinitions(), match.getMeasures(), match.getAfter(),
            match.getSubsets(), match.isAllRows(), match.getPartitionKeys(),
            match.getOrderKeys(), match.getInterval()
        )
    }

    companion object {
        /** Default configuration.  */
        val DEFAULT_CONFIG: Config = Config.INSTANCE
            .withConversion(
                LogicalMatch::class.java, Convention.NONE,
                EnumerableConvention.INSTANCE, "EnumerableMatchRule"
            )
            .withRuleFactory { config: Config? -> EnumerableMatchRule(config) }
    }
}
