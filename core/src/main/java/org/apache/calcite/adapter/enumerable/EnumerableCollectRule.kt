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
 * Rule to convert an [org.apache.calcite.rel.core.Collect] to an
 * [EnumerableCollect].
 *
 * @see EnumerableRules.ENUMERABLE_COLLECT_RULE
 */
class EnumerableCollectRule
/** Called from the Config.  */
protected constructor(config: Config?) : ConverterRule(config) {
    @Override
    fun convert(rel: RelNode): RelNode {
        val collect: Collect = rel as Collect
        val input: RelNode = collect.getInput()
        return EnumerableCollect.create(
            convert(
                input,
                input.getTraitSet().replace(EnumerableConvention.INSTANCE)
            ),
            collect.getRowType()
        )
    }

    companion object {
        /** Default configuration.  */
        val DEFAULT_CONFIG: Config = Config.INSTANCE
            .withConversion(
                Collect::class.java, Convention.NONE,
                EnumerableConvention.INSTANCE, "EnumerableCollectRule"
            )
            .withRuleFactory { config: Config? -> EnumerableCollectRule(config) }
    }
}
