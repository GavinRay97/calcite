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
 * Rule to convert a [LogicalIntersect] to an [EnumerableIntersect].
 * You may provide a custom config to convert other nodes that extend [Intersect].
 *
 * @see EnumerableRules.ENUMERABLE_INTERSECT_RULE
 */
class EnumerableIntersectRule
/** Called from the Config.  */
protected constructor(config: Config?) : ConverterRule(config) {
    @Override
    fun convert(rel: RelNode): RelNode {
        val intersect: Intersect = rel as Intersect
        val out: EnumerableConvention = EnumerableConvention.INSTANCE
        val traitSet: RelTraitSet = intersect.getTraitSet().replace(out)
        return EnumerableIntersect(
            rel.getCluster(), traitSet,
            convertList(intersect.getInputs(), out), intersect.all
        )
    }

    companion object {
        /** Default configuration.  */
        val DEFAULT_CONFIG: Config = Config.INSTANCE
            .withConversion(
                LogicalIntersect::class.java, Convention.NONE,
                EnumerableConvention.INSTANCE, "EnumerableIntersectRule"
            )
            .withRuleFactory { config: Config? -> EnumerableIntersectRule(config) }
    }
}
