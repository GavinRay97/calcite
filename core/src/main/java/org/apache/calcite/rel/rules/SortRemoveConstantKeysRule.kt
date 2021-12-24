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

import org.apache.calcite.plan.RelOptPredicateList

/**
 * Planner rule that removes keys from a
 * a [org.apache.calcite.rel.core.Sort] if those keys are known to be
 * constant, or removes the entire Sort if all keys are constant.
 *
 *
 * Requires [RelCollationTraitDef].
 */
@Value.Enclosing
class SortRemoveConstantKeysRule
/** Creates a SortRemoveConstantKeysRule.  */
protected constructor(config: Config?) : RelRule<SortRemoveConstantKeysRule.Config?>(config), SubstitutionRule {
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val sort: Sort = call.rel(0)
        val mq: RelMetadataQuery = call.getMetadataQuery()
        val input: RelNode = sort.getInput()
        val predicates: RelOptPredicateList = mq.getPulledUpPredicates(input)
        if (RelOptPredicateList.isEmpty(predicates)) {
            return
        }
        val rexBuilder: RexBuilder = sort.getCluster().getRexBuilder()
        val collationsList: List<RelFieldCollation> = sort.getCollation().getFieldCollations().stream()
            .filter { fc ->
                !predicates.constantMap.containsKey(
                    rexBuilder.makeInputRef(input, fc.getFieldIndex())
                )
            }
            .collect(Collectors.toList())
        if (collationsList.size() === sort.collation.getFieldCollations().size()) {
            return
        }

        // No active collations. Remove the sort completely
        if (collationsList.isEmpty() && sort.offset == null && sort.fetch == null) {
            call.transformTo(input)
            call.getPlanner().prune(sort)
            return
        }
        val result: Sort = sort.copy(sort.getTraitSet(), input, RelCollations.of(collationsList))
        call.transformTo(result)
        call.getPlanner().prune(sort)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): SortRemoveConstantKeysRule? {
            return SortRemoveConstantKeysRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableSortRemoveConstantKeysRule.Config.of()
                .withOperandSupplier { b -> b.operand(Sort::class.java).anyInputs() }
        }
    }
}
