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
 * Rule to convert a [org.apache.calcite.rel.logical.LogicalSort] on top of a
 * [org.apache.calcite.rel.logical.LogicalUnion] into a [EnumerableMergeUnion].
 *
 * @see EnumerableRules.ENUMERABLE_MERGE_UNION_RULE
 */
@Value.Enclosing
class EnumerableMergeUnionRule(config: Config?) : RelRule<EnumerableMergeUnionRule.Config?>(config) {
    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): EnumerableMergeUnionRule {
            return EnumerableMergeUnionRule(this)
        }

        companion object {
            val DEFAULT_CONFIG: Config = ImmutableEnumerableMergeUnionRule.Config.of()
                .withDescription("EnumerableMergeUnionRule").withOperandSupplier { b0 ->
                    b0.operand(LogicalSort::class.java)
                        .oneInput { b1 -> b1.operand(LogicalUnion::class.java).anyInputs() }
                }
        }
    }

    @Override
    fun matches(call: RelOptRuleCall): Boolean {
        val sort: Sort = call.rel(0)
        val collation: RelCollation = sort.getCollation()
        if (collation == null || collation.getFieldCollations().isEmpty()) {
            return false
        }
        val union: Union = call.rel(1)
        return if (union.getInputs().size() < 2) {
            false
        } else true
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val sort: Sort = call.rel(0)
        val collation: RelCollation = sort.getCollation()
        val union: Union = call.rel(1)
        val unionInputsSize: Int = union.getInputs().size()

        // Push down sort limit, if possible.
        var inputFetch: RexNode? = null
        if (sort.fetch != null) {
            if (sort.offset == null) {
                inputFetch = sort.fetch
            } else if (sort.fetch is RexLiteral && sort.offset is RexLiteral) {
                inputFetch = call.builder().literal(
                    RexLiteral.intValue(sort.fetch) + RexLiteral.intValue(sort.offset)
                )
            }
        }
        val inputs: List<RelNode> = ArrayList(unionInputsSize)
        for (input in union.getInputs()) {
            val newInput: RelNode = sort.copy(sort.getTraitSet(), input, collation, null, inputFetch)
            inputs.add(
                convert(newInput, newInput.getTraitSet().replace(EnumerableConvention.INSTANCE))
            )
        }
        var result: RelNode = EnumerableMergeUnion.create(sort.getCollation(), inputs, union.all)

        // If Sort contained a LIMIT / OFFSET, then put it back as an EnumerableLimit.
        // The output of the MergeUnion is already sorted, so we do not need a sort anymore.
        if (sort.offset != null || sort.fetch != null) {
            result = EnumerableLimit.create(result, sort.offset, sort.fetch)
        }
        call.transformTo(result)
    }
}
