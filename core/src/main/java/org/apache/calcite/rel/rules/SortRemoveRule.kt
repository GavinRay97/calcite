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

import org.apache.calcite.plan.ConventionTraitDef

/**
 * Planner rule that removes
 * a [org.apache.calcite.rel.core.Sort] if its input is already sorted.
 *
 *
 * Requires [RelCollationTraitDef].
 *
 * @see CoreRules.SORT_REMOVE
 */
@Value.Enclosing
class SortRemoveRule
/** Creates a SortRemoveRule.  */
protected constructor(config: Config?) : RelRule<SortRemoveRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        if (!call.getPlanner().getRelTraitDefs()
                .contains(RelCollationTraitDef.INSTANCE)
        ) {
            // Collation is not an active trait.
            return
        }
        val sort: Sort = call.rel(0)
        if (sort.offset != null || sort.fetch != null) {
            // Don't remove sort if would also remove OFFSET or LIMIT.
            return
        }
        // Express the "sortedness" requirement in terms of a collation trait and
        // we can get rid of the sort. This allows us to use rels that just happen
        // to be sorted but get the same effect.
        val collation: RelCollation = sort.getCollation()
        assert(
            collation === sort.getTraitSet()
                .getTrait(RelCollationTraitDef.INSTANCE)
        )
        val traits: RelTraitSet = sort.getInput().getTraitSet()
            .replace(collation).replaceIf(ConventionTraitDef.INSTANCE, sort::getConvention)
        call.transformTo(convert(sort.getInput(), traits))
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): SortRemoveRule? {
            return SortRemoveRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableSortRemoveRule.Config.of()
                .withOperandSupplier { b -> b.operand(Sort::class.java).anyInputs() }
        }
    }
}
