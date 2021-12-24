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
 * Planner rule that pulls up constants through a Union operator.
 *
 * @see CoreRules.UNION_PULL_UP_CONSTANTS
 */
@Value.Enclosing
class UnionPullUpConstantsRule
/** Creates a UnionPullUpConstantsRule.  */
protected constructor(config: Config?) : RelRule<UnionPullUpConstantsRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        unionClass: Class<out Union?>?,
        relBuilderFactory: RelBuilderFactory?
    ) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(unionClass)
    ) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val union: Union = call.rel(0)
        val rexBuilder: RexBuilder = union.getCluster().getRexBuilder()
        val mq: RelMetadataQuery = call.getMetadataQuery()
        val predicates: RelOptPredicateList = mq.getPulledUpPredicates(union)
        if (RelOptPredicateList.isEmpty(predicates)) {
            return
        }
        val constants: Map<Integer, RexNode> = HashMap()
        for (e in predicates.constantMap.entrySet()) {
            if (e.getKey() is RexInputRef) {
                constants.put((e.getKey() as RexInputRef).getIndex(), e.getValue())
            }
        }

        // None of the expressions are constant. Nothing to do.
        if (constants.isEmpty()) {
            return
        }

        // Create expressions for Project operators before and after the Union
        val fields: List<RelDataTypeField> = union.getRowType().getFieldList()
        var topChildExprs: List<RexNode?> = ArrayList()
        val topChildExprsFields: List<String> = ArrayList()
        val refs: List<RexNode> = ArrayList()
        val refsIndexBuilder: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        for (field in fields) {
            val constant: RexNode? = constants[field.getIndex()]
            if (constant != null) {
                topChildExprs.add(constant)
                topChildExprsFields.add(field.getName())
            } else {
                val expr: RexNode = rexBuilder.makeInputRef(union, field.getIndex())
                topChildExprs.add(expr)
                topChildExprsFields.add(field.getName())
                refs.add(expr)
                refsIndexBuilder.set(field.getIndex())
            }
        }
        val refsIndex: ImmutableBitSet = refsIndexBuilder.build()

        // Update top Project positions
        val mapping: Mappings.TargetMapping = RelOptUtil.permutation(refs, union.getInput(0).getRowType()).inverse()
        topChildExprs = RexUtil.apply(mapping, topChildExprs)

        // Create new Project-Union-Project sequences
        val relBuilder: RelBuilder = call.builder()
        for (input in union.getInputs()) {
            val newChildExprs: List<Pair<RexNode, String>> = ArrayList()
            for (j in refsIndex) {
                newChildExprs.add(
                    Pair.of(
                        rexBuilder.makeInputRef(input, j),
                        input.getRowType().getFieldList().get(j).getName()
                    )
                )
            }
            if (newChildExprs.isEmpty()) {
                // At least a single item in project is required.
                newChildExprs.add(
                    Pair.of(topChildExprs[0], topChildExprsFields[0])
                )
            }
            // Add the input with project on top
            relBuilder.push(input)
            relBuilder.project(Pair.left(newChildExprs), Pair.right(newChildExprs))
        }
        relBuilder.union(union.all, union.getInputs().size())
        // Create top Project fixing nullability of fields
        relBuilder.project(topChildExprs, topChildExprsFields)
        relBuilder.convert(union.getRowType(), false)
        call.transformTo(relBuilder.build())
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): UnionPullUpConstantsRule? {
            return UnionPullUpConstantsRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(unionClass: Class<out Union?>?): Config? {
            return withOperandSupplier { b ->
                b.operand(unionClass) // If field count is 1, then there's no room for
                    // optimization since we cannot create an empty Project
                    // operator. If we created a Project with one column,
                    // this rule would cycle.
                    .predicate { union -> union.getRowType().getFieldCount() > 1 }
                    .anyInputs()
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableUnionPullUpConstantsRule.Config.of()
                .withOperandFor(Union::class.java)
        }
    }
}
