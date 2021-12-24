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

import org.apache.calcite.plan.Contexts

/**
 * Planner rule that combines two
 * [org.apache.calcite.rel.logical.LogicalFilter]s.
 */
@Value.Enclosing
class FilterMergeRule
/** Creates a FilterMergeRule.  */
protected constructor(config: Config?) : RelRule<FilterMergeRule.Config?>(config), SubstitutionRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT
            .withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(filterFactory: FilterFactory?) : this(RelBuilder.proto(Contexts.of(filterFactory))) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val topFilter: Filter = call.rel(0)
        val bottomFilter: Filter = call.rel(1)
        val relBuilder: RelBuilder = call.builder()
        relBuilder.push(bottomFilter.getInput())
            .filter(bottomFilter.getCondition(), topFilter.getCondition())
        call.transformTo(relBuilder.build())
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): FilterMergeRule {
            return FilterMergeRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(filterClass: Class<out Filter?>?): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(filterClass).oneInput { b1 -> b1.operand(filterClass).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableFilterMergeRule.Config.of()
                .withOperandFor(Filter::class.java)
        }
    }
}
