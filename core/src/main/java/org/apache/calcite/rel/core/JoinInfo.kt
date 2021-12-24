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
package org.apache.calcite.rel.core

import org.apache.calcite.plan.RelOptUtil

/** An analyzed join condition.
 *
 *
 * It is useful for the many algorithms that care whether a join has an
 * equi-join condition.
 *
 *
 * You can create one using [.of], or call
 * [Join.analyzeCondition]; many kinds of join cache their
 * join info, especially those that are equi-joins.
 *
 * @see Join.analyzeCondition
 */
class JoinInfo protected constructor(
    leftKeys: ImmutableIntList, rightKeys: ImmutableIntList,
    nonEquiConditions: ImmutableList<RexNode?>?
) {
    val leftKeys: ImmutableIntList
    val rightKeys: ImmutableIntList
    val nonEquiConditions: ImmutableList<RexNode>

    /** Creates a JoinInfo.  */
    init {
        this.leftKeys = Objects.requireNonNull(leftKeys, "leftKeys")
        this.rightKeys = Objects.requireNonNull(rightKeys, "rightKeys")
        this.nonEquiConditions = Objects.requireNonNull(nonEquiConditions, "nonEquiConditions")
        assert(leftKeys.size() === rightKeys.size())
    }

    /** Returns whether this is an equi-join.  */
    val isEqui: Boolean
        get() = nonEquiConditions.isEmpty()

    /** Returns a list of (left, right) key ordinals.  */
    fun pairs(): List<IntPair> {
        return IntPair.zip(leftKeys, rightKeys)
    }

    fun leftSet(): ImmutableBitSet {
        return ImmutableBitSet.of(leftKeys)
    }

    fun rightSet(): ImmutableBitSet {
        return ImmutableBitSet.of(rightKeys)
    }

    @Deprecated // to be removed before 2.0
    fun getRemaining(rexBuilder: RexBuilder?): RexNode {
        return RexUtil.composeConjunction(rexBuilder, nonEquiConditions)
    }

    fun getEquiCondition(
        left: RelNode?, right: RelNode?,
        rexBuilder: RexBuilder?
    ): RexNode {
        return RelOptUtil.createEquiJoinCondition(
            left, leftKeys, right, rightKeys,
            rexBuilder
        )
    }

    fun keys(): List<ImmutableIntList> {
        return FlatLists.of(leftKeys, rightKeys)
    }

    companion object {
        /** Creates a `JoinInfo` by analyzing a condition.  */
        fun of(left: RelNode?, right: RelNode?, condition: RexNode?): JoinInfo {
            val leftKeys: List<Integer> = ArrayList()
            val rightKeys: List<Integer> = ArrayList()
            val filterNulls: List<Boolean> = ArrayList()
            val nonEquiList: List<RexNode> = ArrayList()
            RelOptUtil.splitJoinCondition(
                left, right, condition, leftKeys, rightKeys,
                filterNulls, nonEquiList
            )
            return JoinInfo(
                ImmutableIntList.copyOf(leftKeys),
                ImmutableIntList.copyOf(rightKeys), ImmutableList.copyOf(nonEquiList)
            )
        }

        /** Creates an equi-join.  */
        fun of(
            leftKeys: ImmutableIntList,
            rightKeys: ImmutableIntList
        ): JoinInfo {
            return JoinInfo(leftKeys, rightKeys, ImmutableList.of())
        }
    }
}
