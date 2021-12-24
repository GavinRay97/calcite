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
 * Rule to convert an [EnumerableLimit] of on
 * [EnumerableSort] into an [EnumerableLimitSort].
 */
@Value.Enclosing
class EnumerableLimitSortRule
/**
 * Creates a EnumerableLimitSortRule.
 */
    (config: Config?) : RelRule<EnumerableLimitSortRule.Config?>(config) {
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val sort: Sort = call.rel(0)
        val input: RelNode = sort.getInput()
        val o: Sort = EnumerableLimitSort.create(
            convert(input, input.getTraitSet().replace(EnumerableConvention.INSTANCE)),
            sort.getCollation(),
            sort.offset, sort.fetch
        )
        call.transformTo(o)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): EnumerableLimitSortRule {
            return EnumerableLimitSortRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableEnumerableLimitSortRule.Config.of().withOperandSupplier { b0 ->
                b0.operand(LogicalSort::class.java).predicate { sort -> sort.fetch != null }
                    .anyInputs()
            }
        }
    }
}
