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
 * Rule to convert an [org.apache.calcite.rel.core.Uncollect] to an
 * [EnumerableUncollect].
 *
 * @see EnumerableRules.ENUMERABLE_UNCOLLECT_RULE
 */
internal class EnumerableUncollectRule
/** Called from the Config.  */
protected constructor(config: Config?) : ConverterRule(config) {
    @Override
    fun convert(rel: RelNode): RelNode {
        val uncollect: Uncollect = rel as Uncollect
        val traitSet: RelTraitSet = uncollect.getTraitSet().replace(EnumerableConvention.INSTANCE)
        val input: RelNode = uncollect.getInput()
        val newInput: RelNode = convert(
            input,
            input.getTraitSet().replace(EnumerableConvention.INSTANCE)
        )
        return EnumerableUncollect.create(
            traitSet, newInput,
            uncollect.withOrdinality
        )
    }

    companion object {
        /** Default configuration.  */
        val DEFAULT_CONFIG: Config = Config.INSTANCE
            .withConversion(
                Uncollect::class.java, Convention.NONE,
                EnumerableConvention.INSTANCE, "EnumerableUncollectRule"
            )
            .withRuleFactory { config: Config? -> EnumerableUncollectRule(config) }
    }
}
