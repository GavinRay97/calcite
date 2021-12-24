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
package org.apache.calcite.rel.rules

import org.apache.calcite.rel.core.Join

/**
 * Rule to convert an
 * [inner join][org.apache.calcite.rel.logical.LogicalJoin] to a
 * [filter][org.apache.calcite.rel.logical.LogicalFilter] on top of a
 * [cartesian inner join][org.apache.calcite.rel.logical.LogicalJoin].
 *
 *
 * One benefit of this transformation is that after it, the join condition
 * can be combined with conditions and expressions above the join. It also makes
 * the `FennelCartesianJoinRule` applicable.
 *
 *
 * Can be configured to match any sub-class of
 * [org.apache.calcite.rel.core.Join], not just
 * [org.apache.calcite.rel.logical.LogicalJoin].
 *
 * @see CoreRules.JOIN_EXTRACT_FILTER
 */
@Value.Enclosing
class JoinExtractFilterRule
/** Creates a JoinExtractFilterRule.  */
internal constructor(config: Config?) : AbstractJoinExtractFilterRule(config) {
    @Deprecated // to be removed before 2.0
    constructor(
        clazz: Class<out Join?>?,
        relBuilderFactory: RelBuilderFactory?
    ) : this(Config.DEFAULT
        .withRelBuilderFactory(relBuilderFactory)
        .withOperandSupplier { b -> b.operand(clazz).anyInputs() }
        .`as`(Config::class.java)) {
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : AbstractJoinExtractFilterRule.Config {
        @Override
        fun toRule(): JoinExtractFilterRule? {
            return JoinExtractFilterRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableJoinExtractFilterRule.Config.of()
                .withOperandSupplier { b -> b.operand(LogicalJoin::class.java).anyInputs() }
        }
    }
}
