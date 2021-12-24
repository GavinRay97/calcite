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
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention].  */
class EnumerableHashJoin
/** Creates an EnumerableHashJoin.
 *
 *
 * Use [.create] unless you know what you're doing.  */
protected constructor(
    cluster: RelOptCluster?,
    traits: RelTraitSet?,
    left: RelNode?,
    right: RelNode?,
    condition: RexNode?,
    variablesSet: Set<CorrelationId?>?,
    joinType: JoinRelType?
) : Join(
    cluster,
    traits,
    ImmutableList.of(),
    left,
    right,
    condition,
    variablesSet,
    joinType
), EnumerableRel {
    @Deprecated // to be removed before 2.0
    protected constructor(
        cluster: RelOptCluster?, traits: RelTraitSet?,
        left: RelNode?, right: RelNode?, condition: RexNode?, leftKeys: ImmutableIntList?,
        rightKeys: ImmutableIntList?, joinType: JoinRelType?,
        variablesStopped: Set<String?>?
    ) : this(cluster, traits, left, right, condition, CorrelationId.setOf(variablesStopped), joinType) {
    }

    @Override
    fun copy(
        traitSet: RelTraitSet?, condition: RexNode?,
        left: RelNode?, right: RelNode?, joinType: JoinRelType?,
        semiJoinDone: Boolean
    ): EnumerableHashJoin {
        return EnumerableHashJoin(
            getCluster(), traitSet, left, right,
            condition, variablesSet, joinType
        )
    }

    @Override
    @Nullable
    override fun passThroughTraits(
        required: RelTraitSet?
    ): Pair<RelTraitSet, List<RelTraitSet>> {
        return EnumerableTraitsUtils.passThroughTraitsForJoin(
            required, joinType, left.getRowType().getFieldCount(), getTraitSet()
        )
    }

    @Override
    @Nullable
    override fun deriveTraits(
        childTraits: RelTraitSet?, childId: Int
    ): Pair<RelTraitSet, List<RelTraitSet>> {
        // should only derive traits (limited to collation for now) from left join input.
        return EnumerableTraitsUtils.deriveTraitsForJoin(
            childTraits, childId, joinType, getTraitSet(), right.getTraitSet()
        )
    }

    @get:Override
    override val deriveMode: DeriveMode
        get() = if (joinType === JoinRelType.FULL || joinType === JoinRelType.RIGHT) {
            DeriveMode.PROHIBITED
        } else DeriveMode.LEFT_FIRST

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

        // Cheaper if the smaller number of rows is coming from the LHS.
        // Model this by adding L log L to the cost.
        val rightRowCount: Double = right.estimateRowCount(mq)
        val leftRowCount: Double = left.estimateRowCount(mq)
        if (Double.isInfinite(leftRowCount)) {
            rowCount = leftRowCount
        } else {
            rowCount += Util.nLogN(leftRowCount)
        }
        if (Double.isInfinite(rightRowCount)) {
            rowCount = rightRowCount
        } else {
            rowCount += rightRowCount
        }
        return if (isSemiJoin()) {
            planner.getCostFactory().makeCost(rowCount, 0, 0).multiplyBy(.01)
        } else {
            planner.getCostFactory().makeCost(rowCount, 0, 0)
        }
    }

    @Override
    fun implement(implementor: EnumerableRelImplementor, pref: Prefer): Result {
        return when (joinType) {
            SEMI, ANTI -> implementHashSemiJoin(implementor, pref)
            else -> implementHashJoin(implementor, pref)
        }
    }

    private fun implementHashSemiJoin(implementor: EnumerableRelImplementor, pref: Prefer): Result {
        assert(joinType === JoinRelType.SEMI || joinType === JoinRelType.ANTI)
        val method: Method =
            if (joinType === JoinRelType.SEMI) BuiltInMethod.SEMI_JOIN.method else BuiltInMethod.ANTI_JOIN.method
        val builder = BlockBuilder()
        val leftResult: Result = implementor.visitChild(this, 0, left as EnumerableRel, pref)
        val leftExpression: Expression = builder.append(
            "left", leftResult.block
        )
        val rightResult: Result = implementor.visitChild(this, 1, right as EnumerableRel, pref)
        val rightExpression: Expression = builder.append(
            "right", rightResult.block
        )
        val physType: PhysType = leftResult.physType
        val keyPhysType: PhysType = leftResult.physType.project(
            joinInfo.leftKeys, JavaRowFormat.LIST
        )
        var predicate: Expression = Expressions.constant(null)
        if (!joinInfo.nonEquiConditions.isEmpty()) {
            val nonEquiCondition: RexNode = RexUtil.composeConjunction(
                getCluster().getRexBuilder(), joinInfo.nonEquiConditions, true
            )
            if (nonEquiCondition != null) {
                predicate = EnumUtils.generatePredicate(
                    implementor, getCluster().getRexBuilder(),
                    left, right, leftResult.physType, rightResult.physType, nonEquiCondition
                )
            }
        }
        return implementor.result(
            physType,
            builder.append(
                Expressions.call(
                    method,
                    Expressions.list(
                        leftExpression,
                        rightExpression,
                        leftResult.physType.generateAccessor(joinInfo.leftKeys),
                        rightResult.physType.generateAccessor(joinInfo.rightKeys),
                        Util.first(
                            keyPhysType.comparer(),
                            Expressions.constant(null)
                        ),
                        predicate
                    )
                )
            )
                .toBlock()
        )
    }

    private fun implementHashJoin(implementor: EnumerableRelImplementor, pref: Prefer): Result {
        val builder = BlockBuilder()
        val leftResult: Result = implementor.visitChild(this, 0, left as EnumerableRel, pref)
        val leftExpression: Expression = builder.append(
            "left", leftResult.block
        )
        val rightResult: Result = implementor.visitChild(this, 1, right as EnumerableRel, pref)
        val rightExpression: Expression = builder.append(
            "right", rightResult.block
        )
        val physType: PhysType = PhysTypeImpl.of(
            implementor.getTypeFactory(), getRowType(), pref.preferArray()
        )
        val keyPhysType: PhysType = leftResult.physType.project(
            joinInfo.leftKeys, JavaRowFormat.LIST
        )
        var predicate: Expression = Expressions.constant(null)
        if (!joinInfo.nonEquiConditions.isEmpty()) {
            val nonEquiCondition: RexNode = RexUtil.composeConjunction(
                getCluster().getRexBuilder(), joinInfo.nonEquiConditions, true
            )
            if (nonEquiCondition != null) {
                predicate = EnumUtils.generatePredicate(
                    implementor, getCluster().getRexBuilder(),
                    left, right, leftResult.physType, rightResult.physType, nonEquiCondition
                )
            }
        }
        return implementor.result(
            physType,
            builder.append(
                Expressions.call(
                    leftExpression,
                    BuiltInMethod.HASH_JOIN.method,
                    Expressions.list(
                        rightExpression,
                        leftResult.physType.generateAccessor(joinInfo.leftKeys),
                        rightResult.physType.generateAccessor(joinInfo.rightKeys),
                        EnumUtils.joinSelector(
                            joinType,
                            physType,
                            ImmutableList.of(
                                leftResult.physType, rightResult.physType
                            )
                        )
                    )
                        .append(
                            Util.first(
                                keyPhysType.comparer(),
                                Expressions.constant(null)
                            )
                        )
                        .append(
                            Expressions.constant(joinType.generatesNullsOnLeft())
                        )
                        .append(
                            Expressions.constant(
                                joinType.generatesNullsOnRight()
                            )
                        )
                        .append(predicate)
                )
            )
                .toBlock()
        )
    }

    companion object {
        /** Creates an EnumerableHashJoin.  */
        fun create(
            left: RelNode,
            right: RelNode?,
            condition: RexNode?,
            variablesSet: Set<CorrelationId?>?,
            joinType: JoinRelType?
        ): EnumerableHashJoin {
            val cluster: RelOptCluster = left.getCluster()
            val mq: RelMetadataQuery = cluster.getMetadataQuery()
            val traitSet: RelTraitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE)
                .replaceIfs(
                    RelCollationTraitDef.INSTANCE
                ) { RelMdCollation.enumerableHashJoin(mq, left, right, joinType) }
            return EnumerableHashJoin(
                cluster, traitSet, left, right, condition,
                variablesSet, joinType
            )
        }
    }
}
