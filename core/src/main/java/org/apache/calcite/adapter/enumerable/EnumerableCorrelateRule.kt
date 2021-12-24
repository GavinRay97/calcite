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
 * Implementation of nested loops over enumerable inputs.
 *
 * @see EnumerableRules.ENUMERABLE_CORRELATE_RULE
 */
@Value.Enclosing
class EnumerableCorrelateRule
/** Creates an EnumerableCorrelateRule.  */
protected constructor(config: Config?) : ConverterRule(config) {
    @Override
    fun convert(rel: RelNode): RelNode {
        val c: Correlate = rel as Correlate
        return EnumerableCorrelate.create(
            convert(
                c.getLeft(), c.getLeft().getTraitSet()
                    .replace(EnumerableConvention.INSTANCE)
            ),
            convert(
                c.getRight(), c.getRight().getTraitSet()
                    .replace(EnumerableConvention.INSTANCE)
            ),
            c.getCorrelationId(),
            c.getRequiredColumns(),
            c.getJoinType()
        )
    }

    companion object {
        /** Default configuration.  */
        val DEFAULT_CONFIG: Config = Config.INSTANCE
            .withConversion(
                LogicalCorrelate::class.java, { r -> true }, Convention.NONE,
                EnumerableConvention.INSTANCE, "EnumerableCorrelateRule"
            )
            .withRuleFactory { config: Config? -> EnumerableCorrelateRule(config) }
    }
}
