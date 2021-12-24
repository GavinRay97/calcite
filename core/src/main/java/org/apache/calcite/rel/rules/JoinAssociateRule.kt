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

import org.apache.calcite.plan.RelOptCluster

/**
 * Planner rule that changes a join based on the associativity rule.
 *
 *
 * ((a JOIN b) JOIN c)  (a JOIN (b JOIN c))
 *
 *
 * We do not need a rule to convert (a JOIN (b JOIN c))
 * ((a JOIN b) JOIN c) because we have
 * [JoinCommuteRule].
 *
 * @see JoinCommuteRule
 *
 * @see CoreRules.JOIN_ASSOCIATE
 */
@Value.Enclosing
class JoinAssociateRule
/** Creates a JoinAssociateRule.  */
protected constructor(config: Config?) : RelRule<JoinAssociateRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val topJoin: Join = call.rel(0)
        val bottomJoin: Join = call.rel(1)
        val relA: RelNode = bottomJoin.getLeft()
        val relB: RelNode = bottomJoin.getRight()
        val relC: RelNode = call.rel(2)
        val cluster: RelOptCluster = topJoin.getCluster()
        val rexBuilder: RexBuilder = cluster.getRexBuilder()
        if (relC.getConvention() !== relA.getConvention()) {
            // relC could have any trait-set. But if we're matching say
            // EnumerableConvention, we're only interested in enumerable subsets.
            return
        }

        //        topJoin
        //        /     \
        //   bottomJoin  C
        //    /    \
        //   A      B
        val aCount: Int = relA.getRowType().getFieldCount()
        val bCount: Int = relB.getRowType().getFieldCount()
        val cCount: Int = relC.getRowType().getFieldCount()
        val aBitSet: ImmutableBitSet = ImmutableBitSet.range(0, aCount)
        @SuppressWarnings("unused") val bBitSet: ImmutableBitSet = ImmutableBitSet.range(aCount, aCount + bCount)
        if (!topJoin.getSystemFieldList().isEmpty()) {
            // FIXME Enable this rule for joins with system fields
            return
        }

        // If either join is not inner, we cannot proceed.
        // (Is this too strict?)
        if (topJoin.getJoinType() !== JoinRelType.INNER
            || bottomJoin.getJoinType() !== JoinRelType.INNER
        ) {
            return
        }

        // Goal is to transform to
        //
        //       newTopJoin
        //        /     \
        //       A   newBottomJoin
        //               /    \
        //              B      C

        // Split the condition of topJoin and bottomJoin into a conjunctions. A
        // condition can be pushed down if it does not use columns from A.
        val top: List<RexNode> = ArrayList()
        val bottom: List<RexNode> = ArrayList()
        JoinPushThroughJoinRule.split(topJoin.getCondition(), aBitSet, top, bottom)
        JoinPushThroughJoinRule.split(
            bottomJoin.getCondition(), aBitSet, top,
            bottom
        )
        val allowAlwaysTrueCondition: Boolean = config.isAllowAlwaysTrueCondition()
        if (!allowAlwaysTrueCondition && (top.isEmpty() || bottom.isEmpty())) {
            return
        }

        // Mapping for moving conditions from topJoin or bottomJoin to
        // newBottomJoin.
        // target: | B | C      |
        // source: | A       | B | C      |
        val bottomMapping: Mappings.TargetMapping = Mappings.createShiftMapping(
            aCount + bCount + cCount,
            0, aCount, bCount,
            bCount, aCount + bCount, cCount
        )
        val newBottomList: List<RexNode> = RexPermuteInputsShuttle(bottomMapping, relB, relC)
            .visitList(bottom)
        val newBottomCondition: RexNode = RexUtil.composeConjunction(rexBuilder, newBottomList)
        if (!allowAlwaysTrueCondition && newBottomCondition.isAlwaysTrue()) {
            return
        }

        // Condition for newTopJoin consists of pieces from bottomJoin and topJoin.
        // Field ordinals do not need to be changed.
        val newTopCondition: RexNode = RexUtil.composeConjunction(rexBuilder, top)
        if (!allowAlwaysTrueCondition && newTopCondition.isAlwaysTrue()) {
            return
        }
        val newBottomJoin: Join = bottomJoin.copy(
            bottomJoin.getTraitSet(), newBottomCondition, relB,
            relC, JoinRelType.INNER, false
        )
        @SuppressWarnings("SuspiciousNameCombination") val newTopJoin: Join = topJoin.copy(
            topJoin.getTraitSet(), newTopCondition, relA,
            newBottomJoin, JoinRelType.INNER, false
        )
        call.transformTo(newTopJoin)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): JoinAssociateRule? {
            return JoinAssociateRule(this)
        }

        /**
         * Whether to emit the new join tree if the new top or bottom join has a condition which
         * is always `TRUE`.
         */
        @get:Value.Default
        val isAllowAlwaysTrueCondition: Boolean
            get() = true

        /** Sets [.isAllowAlwaysTrueCondition].  */
        fun withAllowAlwaysTrueCondition(allowAlwaysTrueCondition: Boolean): Config?

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(joinClass: Class<out Join?>?): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(joinClass).inputs(
                    { b1 -> b1.operand(joinClass).anyInputs() }
                ) { b2 -> b2.operand(RelNode::class.java).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableJoinAssociateRule.Config.of()
                .withOperandFor(Join::class.java)
        }
    }
}
