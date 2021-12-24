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
 * Planner rule that pushes a
 * [org.apache.calcite.rel.core.Join]
 * past a non-distinct [org.apache.calcite.rel.core.Union].
 *
 * @see CoreRules.JOIN_LEFT_UNION_TRANSPOSE
 *
 * @see CoreRules.JOIN_RIGHT_UNION_TRANSPOSE
 */
@Value.Enclosing
class JoinUnionTransposeRule
/** Creates a JoinUnionTransposeRule.  */
protected constructor(config: Config?) : RelRule<JoinUnionTransposeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        operand: RelOptRuleOperand?,
        relBuilderFactory: RelBuilderFactory?, description: String?
    ) : this(Config.LEFT.withRelBuilderFactory(relBuilderFactory)
        .withDescription(description)
        .withOperandSupplier { b -> b.exactly(operand) }
        .`as`(Config::class.java)) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val join: Join = call.rel(0)
        val unionRel: Union
        val otherInput: RelNode
        val unionOnLeft: Boolean
        if (call.rel(1) is Union) {
            unionRel = call.rel(1)
            otherInput = call.rel(2)
            unionOnLeft = true
        } else {
            otherInput = call.rel(1)
            unionRel = call.rel(2)
            unionOnLeft = false
        }
        if (!unionRel.all) {
            return
        }
        if (!join.getVariablesSet().isEmpty()) {
            return
        }
        // The UNION ALL cannot be on the null generating side
        // of an outer join (otherwise we might generate incorrect
        // rows for the other side for join keys which lack a match
        // in one or both branches of the union)
        if (unionOnLeft) {
            if (join.getJoinType().generatesNullsOnLeft()) {
                return
            }
        } else {
            if (join.getJoinType().generatesNullsOnRight()
                || !join.getJoinType().projectsRight()
            ) {
                return
            }
        }
        val newUnionInputs: List<RelNode> = ArrayList()
        for (input in unionRel.getInputs()) {
            var joinLeft: RelNode
            var joinRight: RelNode
            if (unionOnLeft) {
                joinLeft = input
                joinRight = otherInput
            } else {
                joinLeft = otherInput
                joinRight = input
            }
            newUnionInputs.add(
                join.copy(
                    join.getTraitSet(),
                    join.getCondition(),
                    joinLeft,
                    joinRight,
                    join.getJoinType(),
                    join.isSemiJoinDone()
                )
            )
        }
        val newUnionRel: SetOp = unionRel.copy(unionRel.getTraitSet(), newUnionInputs, true)
        call.transformTo(newUnionRel)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): JoinUnionTransposeRule? {
            return JoinUnionTransposeRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            joinClass: Class<out Join?>?,
            unionClass: Class<out Union?>, left: Boolean
        ): Config? {
            val leftClass: Class<out RelNode?> = if (left) unionClass else RelNode::class.java
            val rightClass: Class<out RelNode?> = if (left) RelNode::class.java else unionClass
            return withOperandSupplier { b0 ->
                b0.operand(joinClass).inputs(
                    { b1 -> b1.operand(leftClass).anyInputs() }
                ) { b2 -> b2.operand(rightClass).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val LEFT: Config = ImmutableJoinUnionTransposeRule.Config.of()
                .withDescription("JoinUnionTransposeRule(Union-Other)")
                .withOperandFor(Join::class.java, Union::class.java, true)
            val RIGHT: Config = ImmutableJoinUnionTransposeRule.Config.of()
                .withDescription("JoinUnionTransposeRule(Other-Union)")
                .withOperandFor(Join::class.java, Union::class.java, false)
        }
    }
}
