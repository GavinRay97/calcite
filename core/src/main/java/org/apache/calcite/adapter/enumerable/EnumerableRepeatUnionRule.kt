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
 * Rule to convert a [LogicalRepeatUnion] into an [EnumerableRepeatUnion].
 * You may provide a custom config to convert other nodes that extend [RepeatUnion].
 *
 * @see EnumerableRules.ENUMERABLE_REPEAT_UNION_RULE
 */
class EnumerableRepeatUnionRule
/** Called from the Config.  */
protected constructor(config: Config?) : ConverterRule(config) {
    @Override
    fun convert(rel: RelNode): RelNode {
        val union: RepeatUnion = rel as RepeatUnion
        val out: EnumerableConvention = EnumerableConvention.INSTANCE
        val traitSet: RelTraitSet = union.getTraitSet().replace(out)
        val seedRel: RelNode = union.getSeedRel()
        val iterativeRel: RelNode = union.getIterativeRel()
        return EnumerableRepeatUnion(
            rel.getCluster(),
            traitSet,
            convert(seedRel, seedRel.getTraitSet().replace(out)),
            convert(iterativeRel, iterativeRel.getTraitSet().replace(out)),
            union.all,
            union.iterationLimit
        )
    }

    companion object {
        /** Default configuration.  */
        val DEFAULT_CONFIG: Config = Config.INSTANCE
            .withConversion(
                LogicalRepeatUnion::class.java, Convention.NONE,
                EnumerableConvention.INSTANCE, "EnumerableRepeatUnionRule"
            )
            .withRuleFactory { config: Config? -> EnumerableRepeatUnionRule(config) }
    }
}
