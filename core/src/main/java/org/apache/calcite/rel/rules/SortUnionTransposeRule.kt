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

import org.apache.calcite.plan.RelOptRuleCall

/**
 * Planner rule that pushes a [org.apache.calcite.rel.core.Sort] past a
 * [org.apache.calcite.rel.core.Union].
 *
 * @see CoreRules.SORT_UNION_TRANSPOSE
 *
 * @see CoreRules.SORT_UNION_TRANSPOSE_MATCH_NULL_FETCH
 */
@Value.Enclosing
class SortUnionTransposeRule
/** Creates a SortUnionTransposeRule.  */
protected constructor(config: Config?) : RelRule<SortUnionTransposeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        sortClass: Class<out Sort?>?,
        unionClass: Class<out Union?>?,
        matchNullFetch: Boolean,
        relBuilderFactory: RelBuilderFactory?,
        description: String?
    ) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .withDescription(description)
            .`as`(Config::class.java)
            .withOperandFor(sortClass, unionClass)
            .withMatchNullFetch(matchNullFetch)
    ) {
    }

    // ~ Methods ----------------------------------------------------------------
    @Override
    fun matches(call: RelOptRuleCall): Boolean {
        val sort: Sort = call.rel(0)
        val union: Union = call.rel(1)
        // We only apply this rule if Union.all is true and Sort.offset is null.
        // There is a flag indicating if this rule should be applied when
        // Sort.fetch is null.
        return (union.all
                && sort.offset == null && (config.matchNullFetch() || sort.fetch != null))
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val sort: Sort = call.rel(0)
        val union: Union = call.rel(1)
        val inputs: List<RelNode> = ArrayList()
        // Thus we use 'ret' as a flag to identify if we have finished pushing the
        // sort past a union.
        var ret = true
        val mq: RelMetadataQuery = call.getMetadataQuery()
        for (input in union.getInputs()) {
            if (!RelMdUtil.checkInputForCollationAndLimit(
                    mq, input,
                    sort.getCollation(), sort.offset, sort.fetch
                )
            ) {
                ret = false
                val branchSort: Sort = sort.copy(
                    sort.getTraitSet(), input,
                    sort.getCollation(), sort.offset, sort.fetch
                )
                inputs.add(branchSort)
            } else {
                inputs.add(input)
            }
        }
        // there is nothing to change
        if (ret) {
            return
        }
        // create new union and sort
        val unionCopy: Union = union
            .copy(union.getTraitSet(), inputs, union.all) as Union
        val result: Sort = sort.copy(
            sort.getTraitSet(), unionCopy, sort.getCollation(),
            sort.offset, sort.fetch
        )
        call.transformTo(result)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): SortUnionTransposeRule? {
            return SortUnionTransposeRule(this)
        }

        /** Whether to match a Sort whose [Sort.fetch] is null. Generally
         * this only makes sense if the Union preserves order (and merges).  */
        @Value.Default
        fun matchNullFetch(): Boolean {
            return false
        }

        /** Sets [.matchNullFetch].  */
        fun withMatchNullFetch(matchNullFetch: Boolean): Config?

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            sortClass: Class<out Sort?>?,
            unionClass: Class<out Union?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(sortClass).oneInput { b1 -> b1.operand(unionClass).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableSortUnionTransposeRule.Config.of()
                .withOperandFor(Sort::class.java, Union::class.java)
                .withMatchNullFetch(false)
        }
    }
}
