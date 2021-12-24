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
 * Rule that pushes the right input of a join into through the left input of
 * the join, provided that the left input is also a join.
 *
 *
 * Thus, `(A join B) join C` becomes `(A join C) join B`. The
 * advantage of applying this rule is that it may be possible to apply
 * conditions earlier. For instance,
 *
 * <blockquote>
 * <pre>(sales as s join product_class as pc on true)
 * join product as p
 * on s.product_id = p.product_id
 * and p.product_class_id = pc.product_class_id</pre></blockquote>
 *
 *
 * becomes
 *
 * <blockquote>
 * <pre>(sales as s join product as p on s.product_id = p.product_id)
 * join product_class as pc
 * on p.product_class_id = pc.product_class_id</pre></blockquote>
 *
 *
 * Before the rule, one join has two conditions and the other has none
 * (`ON TRUE`). After the rule, each join has one condition.
 */
@Value.Enclosing
class JoinPushThroughJoinRule
/** Creates a JoinPushThroughJoinRule.  */
protected constructor(config: Config?) : RelRule<JoinPushThroughJoinRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        description: String?, right: Boolean,
        joinClass: Class<out Join?>?, relBuilderFactory: RelBuilderFactory?
    ) : this(
        Config.LEFT.withDescription(description)
            .withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(joinClass)
            .withRight(right)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        description: String?, right: Boolean,
        joinClass: Class<out Join?>?, projectFactory: ProjectFactory?
    ) : this(
        Config.LEFT.withDescription(description)
            .withRelBuilderFactory(RelBuilder.proto(projectFactory))
            .`as`(Config::class.java)
            .withOperandFor(joinClass)
            .withRight(right)
    ) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        if (config.isRight()) {
            onMatchRight(call)
        } else {
            onMatchLeft(call)
        }
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): JoinPushThroughJoinRule {
            return JoinPushThroughJoinRule(this)
        }

        /** Whether to push on the right. If false, push to the left.  */
        @get:Value.Default
        val isRight: Boolean
            get() = false

        /** Sets [.isRight].  */
        fun withRight(right: Boolean): Config?

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(joinClass: Class<out Join?>?): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(joinClass).inputs(
                    { b1 -> b1.operand(joinClass).anyInputs() }
                ) { b2 ->
                    b2.operand(RelNode::class.java)
                        .predicate { n -> !n.isEnforcer() }.anyInputs()
                }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val RIGHT: Config = ImmutableJoinPushThroughJoinRule.Config.of()
                .withDescription("JoinPushThroughJoinRule:right")
                .withOperandFor(LogicalJoin::class.java)
                .withRight(true)
            val LEFT: Config = ImmutableJoinPushThroughJoinRule.Config.of()
                .withDescription("JoinPushThroughJoinRule:left")
                .withOperandFor(LogicalJoin::class.java)
                .withRight(false)
        }
    }

    companion object {
        /** Instance of the rule that works on logical joins only, and pushes to the
         * right.  */
        val RIGHT = Config.RIGHT.toRule()

        /** Instance of the rule that works on logical joins only, and pushes to the
         * left.  */
        val LEFT = Config.LEFT.toRule()
        private fun onMatchRight(call: RelOptRuleCall) {
            val topJoin: Join = call.rel(0)
            val bottomJoin: Join = call.rel(1)
            val relC: RelNode = call.rel(2)
            val relA: RelNode = bottomJoin.getLeft()
            val relB: RelNode = bottomJoin.getRight()
            val cluster: RelOptCluster = topJoin.getCluster()

            //        topJoin
            //        /     \
            //   bottomJoin  C
            //    /    \
            //   A      B
            val aCount: Int = relA.getRowType().getFieldCount()
            val bCount: Int = relB.getRowType().getFieldCount()
            val cCount: Int = relC.getRowType().getFieldCount()
            val bBitSet: ImmutableBitSet = ImmutableBitSet.range(aCount, aCount + bCount)

            // becomes
            //
            //        newTopJoin
            //        /        \
            //   newBottomJoin  B
            //    /    \
            //   A      C

            // If either join is not inner, we cannot proceed.
            // (Is this too strict?)
            if (topJoin.getJoinType() !== JoinRelType.INNER
                || bottomJoin.getJoinType() !== JoinRelType.INNER
            ) {
                return
            }

            // Split the condition of topJoin into a conjunction. Each of the
            // parts that does not use columns from B can be pushed down.
            val intersecting: List<RexNode> = ArrayList()
            val nonIntersecting: List<RexNode> = ArrayList()
            split(topJoin.getCondition(), bBitSet, intersecting, nonIntersecting)

            // If there's nothing to push down, it's not worth proceeding.
            if (nonIntersecting.isEmpty()) {
                return
            }

            // Split the condition of bottomJoin into a conjunction. Each of the
            // parts that use columns from B will need to be pulled up.
            val bottomIntersecting: List<RexNode> = ArrayList()
            val bottomNonIntersecting: List<RexNode> = ArrayList()
            split(
                bottomJoin.getCondition(), bBitSet, bottomIntersecting,
                bottomNonIntersecting
            )

            // target: | A       | C      |
            // source: | A       | B | C      |
            val bottomMapping: Mappings.TargetMapping = Mappings.createShiftMapping(
                aCount + bCount + cCount,
                0, 0, aCount,
                aCount, aCount + bCount, cCount
            )
            val newBottomList: List<RexNode> = ArrayList()
            RexPermuteInputsShuttle(bottomMapping, relA, relC)
                .visitList(nonIntersecting, newBottomList)
            RexPermuteInputsShuttle(bottomMapping, relA, relC)
                .visitList(bottomNonIntersecting, newBottomList)
            val rexBuilder: RexBuilder = cluster.getRexBuilder()
            val newBottomCondition: RexNode = RexUtil.composeConjunction(rexBuilder, newBottomList)
            val newBottomJoin: Join = bottomJoin.copy(
                bottomJoin.getTraitSet(), newBottomCondition, relA,
                relC, bottomJoin.getJoinType(), bottomJoin.isSemiJoinDone()
            )

            // target: | A       | C      | B |
            // source: | A       | B | C      |
            val topMapping: Mappings.TargetMapping = Mappings.createShiftMapping(
                aCount + bCount + cCount,
                0, 0, aCount,
                aCount + cCount, aCount, bCount,
                aCount, aCount + bCount, cCount
            )
            val newTopList: List<RexNode> = ArrayList()
            RexPermuteInputsShuttle(topMapping, newBottomJoin, relB)
                .visitList(intersecting, newTopList)
            RexPermuteInputsShuttle(topMapping, newBottomJoin, relB)
                .visitList(bottomIntersecting, newTopList)
            val newTopCondition: RexNode = RexUtil.composeConjunction(rexBuilder, newTopList)
            @SuppressWarnings("SuspiciousNameCombination") val newTopJoin: Join = topJoin.copy(
                topJoin.getTraitSet(), newTopCondition, newBottomJoin,
                relB, topJoin.getJoinType(), topJoin.isSemiJoinDone()
            )
            assert(!Mappings.isIdentity(topMapping))
            val relBuilder: RelBuilder = call.builder()
            relBuilder.push(newTopJoin)
            relBuilder.project(relBuilder.fields(topMapping))
            call.transformTo(relBuilder.build())
        }

        /**
         * Similar to [.onMatch], but swaps the upper sibling with the left
         * of the two lower siblings, rather than the right.
         */
        private fun onMatchLeft(call: RelOptRuleCall) {
            val topJoin: Join = call.rel(0)
            val bottomJoin: Join = call.rel(1)
            val relC: RelNode = call.rel(2)
            val relA: RelNode = bottomJoin.getLeft()
            val relB: RelNode = bottomJoin.getRight()
            val cluster: RelOptCluster = topJoin.getCluster()

            //        topJoin
            //        /     \
            //   bottomJoin  C
            //    /    \
            //   A      B
            val aCount: Int = relA.getRowType().getFieldCount()
            val bCount: Int = relB.getRowType().getFieldCount()
            val cCount: Int = relC.getRowType().getFieldCount()
            val aBitSet: ImmutableBitSet = ImmutableBitSet.range(aCount)

            // becomes
            //
            //        newTopJoin
            //        /        \
            //   newBottomJoin  A
            //    /    \
            //   C      B

            // If either join is not inner, we cannot proceed.
            // (Is this too strict?)
            if (topJoin.getJoinType() !== JoinRelType.INNER
                || bottomJoin.getJoinType() !== JoinRelType.INNER
            ) {
                return
            }

            // Split the condition of topJoin into a conjunction. Each of the
            // parts that does not use columns from A can be pushed down.
            val intersecting: List<RexNode> = ArrayList()
            val nonIntersecting: List<RexNode> = ArrayList()
            split(topJoin.getCondition(), aBitSet, intersecting, nonIntersecting)

            // If there's nothing to push down, it's not worth proceeding.
            if (nonIntersecting.isEmpty()) {
                return
            }

            // Split the condition of bottomJoin into a conjunction. Each of the
            // parts that use columns from A will need to be pulled up.
            val bottomIntersecting: List<RexNode> = ArrayList()
            val bottomNonIntersecting: List<RexNode> = ArrayList()
            split(
                bottomJoin.getCondition(), aBitSet, bottomIntersecting,
                bottomNonIntersecting
            )

            // target: | C      | B |
            // source: | A       | B | C      |
            val bottomMapping: Mappings.TargetMapping = Mappings.createShiftMapping(
                aCount + bCount + cCount,
                cCount, aCount, bCount,
                0, aCount + bCount, cCount
            )
            val newBottomList: List<RexNode> = ArrayList()
            RexPermuteInputsShuttle(bottomMapping, relC, relB)
                .visitList(nonIntersecting, newBottomList)
            RexPermuteInputsShuttle(bottomMapping, relC, relB)
                .visitList(bottomNonIntersecting, newBottomList)
            val rexBuilder: RexBuilder = cluster.getRexBuilder()
            val newBottomCondition: RexNode = RexUtil.composeConjunction(rexBuilder, newBottomList)
            val newBottomJoin: Join = bottomJoin.copy(
                bottomJoin.getTraitSet(), newBottomCondition, relC,
                relB, bottomJoin.getJoinType(), bottomJoin.isSemiJoinDone()
            )

            // target: | C      | B | A       |
            // source: | A       | B | C      |
            val topMapping: Mappings.TargetMapping = Mappings.createShiftMapping(
                aCount + bCount + cCount,
                cCount + bCount, 0, aCount,
                cCount, aCount, bCount,
                0, aCount + bCount, cCount
            )
            val newTopList: List<RexNode> = ArrayList()
            RexPermuteInputsShuttle(topMapping, newBottomJoin, relA)
                .visitList(intersecting, newTopList)
            RexPermuteInputsShuttle(topMapping, newBottomJoin, relA)
                .visitList(bottomIntersecting, newTopList)
            val newTopCondition: RexNode = RexUtil.composeConjunction(rexBuilder, newTopList)
            @SuppressWarnings("SuspiciousNameCombination") val newTopJoin: Join = topJoin.copy(
                topJoin.getTraitSet(), newTopCondition, newBottomJoin,
                relA, topJoin.getJoinType(), topJoin.isSemiJoinDone()
            )
            val relBuilder: RelBuilder = call.builder()
            relBuilder.push(newTopJoin)
            relBuilder.project(relBuilder.fields(topMapping))
            call.transformTo(relBuilder.build())
        }

        /**
         * Splits a condition into conjunctions that do or do not intersect with
         * a given bit set.
         */
        fun split(
            condition: RexNode?,
            bitSet: ImmutableBitSet,
            intersecting: List<RexNode?>,
            nonIntersecting: List<RexNode?>
        ) {
            for (node in RelOptUtil.conjunctions(condition)) {
                val inputBitSet: ImmutableBitSet = RelOptUtil.InputFinder.bits(node)
                if (bitSet.intersects(inputBitSet)) {
                    intersecting.add(node)
                } else {
                    nonIntersecting.add(node)
                }
            }
        }
    }
}
