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

import org.apache.calcite.plan.RelOptRuleCall

/**
 * Rule to convert an [org.apache.calcite.rel.core.Sort] that has
 * `offset` or `fetch` set to an
 * [EnumerableLimit]
 * on top of a "pure" `Sort` that has no offset or fetch.
 *
 * @see EnumerableRules.ENUMERABLE_LIMIT_RULE
 */
@Value.Enclosing
class EnumerableLimitRule
/** Creates an EnumerableLimitRule.  */
protected constructor(config: Config?) : RelRule<EnumerableLimitRule.Config?>(config) {
    @Deprecated
    internal constructor() : this(Config.DEFAULT) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val sort: Sort = call.rel(0)
        if (sort.offset == null && sort.fetch == null) {
            return
        }
        var input: RelNode = sort.getInput()
        if (!sort.getCollation().getFieldCollations().isEmpty()) {
            // Create a sort with the same sort key, but no offset or fetch.
            input = sort.copy(
                sort.getTraitSet(),
                input,
                sort.getCollation(),
                null,
                null
            )
        }
        call.transformTo(
            EnumerableLimit.create(
                convert(input, input.getTraitSet().replace(EnumerableConvention.INSTANCE)),
                sort.offset,
                sort.fetch
            )
        )
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): EnumerableLimitRule {
            return EnumerableLimitRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableEnumerableLimitRule.Config.of()
                .withOperandSupplier { b -> b.operand(Sort::class.java).anyInputs() }
        }
    }
}
