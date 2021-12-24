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
package org.apache.calcite.adapter.jdbc

import org.apache.calcite.adapter.enumerable.EnumerableConvention

/**
 * Rule to convert a relational expression from
 * [JdbcConvention] to
 * [EnumerableConvention].
 */
class JdbcToEnumerableConverterRule
/** Called from the Config.  */
protected constructor(config: Config?) : ConverterRule(config) {
    @Override
    @Nullable
    fun convert(rel: RelNode): RelNode {
        val newTraitSet: RelTraitSet = rel.getTraitSet().replace(getOutTrait())
        return JdbcToEnumerableConverter(rel.getCluster(), newTraitSet, rel)
    }

    companion object {
        /** Creates a JdbcToEnumerableConverterRule.  */
        fun create(out: JdbcConvention?): JdbcToEnumerableConverterRule {
            return Config.INSTANCE
                .withConversion(
                    RelNode::class.java, out, EnumerableConvention.INSTANCE,
                    "JdbcToEnumerableConverterRule"
                )
                .withRuleFactory { config: Config? -> JdbcToEnumerableConverterRule(config) }
                .toRule(JdbcToEnumerableConverterRule::class.java)
        }
    }
}
