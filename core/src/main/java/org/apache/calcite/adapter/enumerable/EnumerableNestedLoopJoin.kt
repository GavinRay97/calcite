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

import org.apache.calcite.linq4j.tree.BlockBuilder

/** Implementation of [org.apache.calcite.rel.core.Join] in
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention]
 * that allows conditions that are not just `=` (equals).  */
class EnumerableNestedLoopJoin
/** Creates an EnumerableNestedLoopJoin.  */
protected constructor(
    cluster: RelOptCluster?, traits: RelTraitSet?,
    left: RelNode?, right: RelNode?, condition: RexNode?,
    variablesSet: Set<CorrelationId?>?, joinType: JoinRelType?
) : Join(cluster, traits, ImmutableList.of(), left, right, condition, variablesSet, joinType), EnumerableRel {
    @Deprecated // to be removed before 2.0
    protected constructor(
        cluster: RelOptCluster?, traits: RelTraitSet?,
        left: RelNode?, right: RelNode?, condition: RexNode?, joinType: JoinRelType?,
        variablesStopped: Set<String?>?
    ) : this(
        cluster, traits, left, right, condition,
        CorrelationId.setOf(variablesStopped), joinType
    ) {
    }

    @Override
    fun copy(
        traitSet: RelTraitSet?,
        condition: RexNode?, left: RelNode?, right: RelNode?, joinType: JoinRelType?,
        semiJoinDone: Boolean
    ): EnumerableNestedLoopJoin {
        return EnumerableNestedLoopJoin(
            getCluster(), traitSet, left, right,
            condition, variablesSet, joinType
        )
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner,
        mq: RelMetadataQuery
    ): RelOptCost {
        var rowCount: Double = mq.getRowCount(this)
        when (joinType) {
            SEMI, ANTI -> {}
            RIGHT -> rowCount = RelMdUtil.addEpsilon(rowCount)
            else -> if (RelNodes.COMPARATOR.compare(left, right) > 0) {
                rowCount = RelMdUtil.addEpsilon(rowCount)
            }
        }
        val rightRowCount: Double = right.estimateRowCount(mq)
        val leftRowCount: Double = left.estimateRowCount(mq)
        if (Double.isInfinite(leftRowCount)) {
            rowCount = leftRowCount
        }
        if (Double.isInfinite(rightRowCount)) {
            rowCount = rightRowCount
        }
        var cost: RelOptCost = planner.getCostFactory().makeCost(rowCount, 0, 0)
        // Give it some penalty
        cost = cost.multiplyBy(10)
        return cost
    }

    @Override
    @Nullable
    override fun passThroughTraits(
        required: RelTraitSet?
    ): Pair<RelTraitSet, List<RelTraitSet>> {
        // EnumerableNestedLoopJoin traits passdown shall only pass through collation to
        // left input. It is because for EnumerableNestedLoopJoin always
        // uses left input as the outer loop, thus only left input can preserve ordering.
        // Push sort both to left and right inputs does not help right outer join. It's because in
        // implementation, EnumerableNestedLoopJoin produces (null, right_unmatched) all together,
        // which does not preserve ordering from right side.
        return EnumerableTraitsUtils.passThroughTraitsForJoin(
            required, joinType, getLeft().getRowType().getFieldCount(), traitSet
        )
    }

    @Override
    @Nullable
    override fun deriveTraits(
        childTraits: RelTraitSet?, childId: Int
    ): Pair<RelTraitSet, List<RelTraitSet>> {
        return EnumerableTraitsUtils.deriveTraitsForJoin(
            childTraits, childId, joinType, traitSet, right.getTraitSet()
        )
    }

    @get:Override
    override val deriveMode: DeriveMode
        get() = if (joinType === JoinRelType.FULL || joinType === JoinRelType.RIGHT) {
            DeriveMode.PROHIBITED
        } else DeriveMode.LEFT_FIRST

    @Override
    fun implement(implementor: EnumerableRelImplementor, pref: Prefer): Result {
        val builder = BlockBuilder()
        val leftResult: Result = implementor.visitChild(this, 0, left as EnumerableRel, pref)
        val leftExpression: Expression = builder.append("left", leftResult.block)
        val rightResult: Result = implementor.visitChild(this, 1, right as EnumerableRel, pref)
        val rightExpression: Expression = builder.append("right", rightResult.block)
        val physType: PhysType = PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref.preferArray()
        )
        val predicate: Expression = EnumUtils.generatePredicate(
            implementor, getCluster().getRexBuilder(), left, right,
            leftResult.physType, rightResult.physType, condition
        )
        return implementor.result(
            physType,
            builder.append(
                Expressions.call(
                    BuiltInMethod.NESTED_LOOP_JOIN.method,
                    leftExpression,
                    rightExpression,
                    predicate,
                    EnumUtils.joinSelector(
                        joinType,
                        physType,
                        ImmutableList.of(
                            leftResult.physType,
                            rightResult.physType
                        )
                    ),
                    Expressions.constant(EnumUtils.toLinq4jJoinType(joinType))
                )
            )
                .toBlock()
        )
    }

    companion object {
        /** Creates an EnumerableNestedLoopJoin.  */
        fun create(
            left: RelNode,
            right: RelNode?,
            condition: RexNode?,
            variablesSet: Set<CorrelationId?>?,
            joinType: JoinRelType?
        ): EnumerableNestedLoopJoin {
            val cluster: RelOptCluster = left.getCluster()
            val mq: RelMetadataQuery = cluster.getMetadataQuery()
            val traitSet: RelTraitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE)
                .replaceIfs(
                    RelCollationTraitDef.INSTANCE
                ) { RelMdCollation.enumerableNestedLoopJoin(mq, left, right, joinType) }
            return EnumerableNestedLoopJoin(
                cluster, traitSet, left, right, condition,
                variablesSet, joinType
            )
        }
    }
}
