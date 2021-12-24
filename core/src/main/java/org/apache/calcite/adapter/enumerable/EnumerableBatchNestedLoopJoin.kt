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

/** Implementation of batch nested loop join in
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention].  */
class EnumerableBatchNestedLoopJoin protected constructor(
    cluster: RelOptCluster?,
    traits: RelTraitSet?,
    left: RelNode?,
    right: RelNode?,
    condition: RexNode?,
    variablesSet: Set<CorrelationId?>?,
    requiredColumns: ImmutableBitSet,
    joinType: JoinRelType?
) : Join(cluster, traits, ImmutableList.of(), left, right, condition, variablesSet, joinType), EnumerableRel {
    private val requiredColumns: ImmutableBitSet

    init {
        this.requiredColumns = requiredColumns
    }

    @Override
    @Nullable
    override fun passThroughTraits(
        required: RelTraitSet?
    ): Pair<RelTraitSet, List<RelTraitSet>> {
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
    fun copy(
        traitSet: RelTraitSet?,
        condition: RexNode?, left: RelNode?, right: RelNode?, joinType: JoinRelType?,
        semiJoinDone: Boolean
    ): EnumerableBatchNestedLoopJoin {
        return EnumerableBatchNestedLoopJoin(
            getCluster(), traitSet,
            left, right, condition, variablesSet, requiredColumns, joinType
        )
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner,
        mq: RelMetadataQuery
    ): RelOptCost? {
        val rowCount: Double = mq.getRowCount(this)
        val rightRowCount: Double = right.estimateRowCount(mq)
        val leftRowCount: Double = left.estimateRowCount(mq)
        if (Double.isInfinite(leftRowCount) || Double.isInfinite(rightRowCount)) {
            return planner.getCostFactory().makeInfiniteCost()
        }
        val restartCount: Double = mq.getRowCount(getLeft()) / variablesSet.size()
        val rightCost: RelOptCost = planner.getCost(getRight(), mq) ?: return null
        val rescanCost: RelOptCost = rightCost.multiplyBy(Math.max(1.0, restartCount - 1))

        // TODO Add cost of last loop (the one that looks for the match)
        return planner.getCostFactory().makeCost(
            rowCount + leftRowCount, 0, 0
        ).plus(rescanCost)
    }

    @Override
    fun explainTerms(pw: RelWriter): RelWriter {
        super.explainTerms(pw)
        return pw.item("batchSize", variablesSet.size())
    }

    @Override
    fun implement(implementor: EnumerableRelImplementor, pref: Prefer): Result {
        val builder = BlockBuilder()
        val leftResult: Result = implementor.visitChild(this, 0, left as EnumerableRel, pref)
        val leftExpression: Expression = builder.append(
            "left", leftResult.block
        )
        val corrVar: List<String> = ArrayList()
        for (c in variablesSet) {
            corrVar.add(c.getName())
        }
        val corrBlock = BlockBuilder()
        val corrVarType: Type = leftResult.physType.getJavaRowType()
        var corrArg: ParameterExpression
        val corrArgList: ParameterExpression = Expressions.parameter(
            Modifier.FINAL,
            List::class.java, "corrList" + Integer.toUnsignedString(this.getId())
        )

        // Declare batchSize correlation variables
        if (!Primitive.`is`(corrVarType)) {
            for (c in 0 until corrVar.size()) {
                corrArg = Expressions.parameter(
                    Modifier.FINAL,
                    corrVarType, corrVar[c]
                )
                val decl: DeclarationStatement = Expressions.declare(
                    Modifier.FINAL,
                    corrArg,
                    Expressions.convert_(
                        Expressions.call(
                            corrArgList,
                            BuiltInMethod.LIST_GET.method,
                            Expressions.constant(c)
                        ),
                        corrVarType
                    )
                )
                corrBlock.add(decl)
                implementor.registerCorrelVariable(
                    corrVar[c], corrArg,
                    corrBlock, leftResult.physType
                )
            }
        } else {
            for (c in 0 until corrVar.size()) {
                corrArg = Expressions.parameter(
                    Modifier.FINAL,
                    Primitive.box(corrVarType), "\$box" + corrVar[c]
                )
                val decl: DeclarationStatement = Expressions.declare(
                    Modifier.FINAL,
                    corrArg,
                    Expressions.call(
                        corrArgList,
                        BuiltInMethod.LIST_GET.method,
                        Expressions.constant(c)
                    )
                )
                corrBlock.add(decl)
                val corrRef: ParameterExpression = corrBlock.append(
                    corrVar[c],
                    Expressions.unbox(corrArg)
                ) as ParameterExpression
                implementor.registerCorrelVariable(
                    corrVar[c], corrRef,
                    corrBlock, leftResult.physType
                )
            }
        }
        val rightResult: Result = implementor.visitChild(this, 1, right as EnumerableRel, pref)
        corrBlock.add(rightResult.block)
        for (c in corrVar) {
            implementor.clearCorrelVariable(c)
        }
        val physType: PhysType = PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref.prefer(JavaRowFormat.CUSTOM)
        )
        val selector: Expression = EnumUtils.joinSelector(
            joinType, physType,
            ImmutableList.of(leftResult.physType, rightResult.physType)
        )
        val predicate: Expression = EnumUtils.generatePredicate(
            implementor, getCluster().getRexBuilder(), left, right,
            leftResult.physType, rightResult.physType, condition
        )
        builder.append(
            Expressions.call(
                BuiltInMethod.CORRELATE_BATCH_JOIN.method,
                Expressions.constant(EnumUtils.toLinq4jJoinType(joinType)),
                leftExpression,
                Expressions.lambda(corrBlock.toBlock(), corrArgList),
                selector,
                predicate,
                Expressions.constant(variablesSet.size())
            )
        )
        return implementor.result(physType, builder.toBlock())
    }

    companion object {
        fun create(
            left: RelNode,
            right: RelNode?,
            condition: RexNode?,
            requiredColumns: ImmutableBitSet,
            variablesSet: Set<CorrelationId?>?,
            joinType: JoinRelType?
        ): EnumerableBatchNestedLoopJoin {
            val cluster: RelOptCluster = left.getCluster()
            val mq: RelMetadataQuery = cluster.getMetadataQuery()
            val traitSet: RelTraitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE)
                .replaceIfs(
                    RelCollationTraitDef.INSTANCE
                ) { RelMdCollation.enumerableBatchNestedLoopJoin(mq, left, right, joinType) }
            return EnumerableBatchNestedLoopJoin(
                cluster,
                traitSet,
                left,
                right,
                condition,
                variablesSet,
                requiredColumns,
                joinType
            )
        }
    }
}
