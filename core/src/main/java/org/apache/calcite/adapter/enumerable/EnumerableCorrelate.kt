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

/** Implementation of [org.apache.calcite.rel.core.Correlate] in
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention].  */
class EnumerableCorrelate(
    cluster: RelOptCluster?, traits: RelTraitSet?,
    left: RelNode?, right: RelNode?,
    correlationId: CorrelationId?,
    requiredColumns: ImmutableBitSet?, joinType: JoinRelType?
) : Correlate(
    cluster, traits, left, right, correlationId, requiredColumns,
    joinType
), EnumerableRel {
    @Override
    fun copy(
        traitSet: RelTraitSet?,
        left: RelNode?, right: RelNode?, correlationId: CorrelationId?,
        requiredColumns: ImmutableBitSet?, joinType: JoinRelType?
    ): EnumerableCorrelate {
        return EnumerableCorrelate(
            getCluster(),
            traitSet, left, right, correlationId, requiredColumns, joinType
        )
    }

    @Override
    @Nullable
    override fun passThroughTraits(
        required: RelTraitSet?
    ): Pair<RelTraitSet, List<RelTraitSet>> {
        // EnumerableCorrelate traits passdown shall only pass through collation to left input.
        // This is because for EnumerableCorrelate always uses left input as the outer loop,
        // thus only left input can preserve ordering.
        return EnumerableTraitsUtils.passThroughTraitsForJoin(
            required, joinType, left.getRowType().getFieldCount(), getTraitSet()
        )
    }

    @Override
    @Nullable
    override fun deriveTraits(
        childTraits: RelTraitSet?, childId: Int
    ): Pair<RelTraitSet, List<RelTraitSet>> {
        // should only derive traits (limited to collation for now) from left input.
        return EnumerableTraitsUtils.deriveTraitsForJoin(
            childTraits, childId, joinType, traitSet, right.getTraitSet()
        )
    }

    @get:Override
    override val deriveMode: DeriveMode
        get() = DeriveMode.LEFT_FIRST

    @Override
    fun implement(
        implementor: EnumerableRelImplementor,
        pref: Prefer
    ): Result {
        val builder = BlockBuilder()
        val leftResult: Result = implementor.visitChild(this, 0, left as EnumerableRel, pref)
        val leftExpression: Expression = builder.append(
            "left", leftResult.block
        )
        val corrBlock = BlockBuilder()
        val corrVarType: Type = leftResult.physType.getJavaRowType()
        val corrRef: ParameterExpression // correlate to be used in inner loop
        val corrArg: ParameterExpression // argument to correlate lambda (must be boxed)
        if (!Primitive.`is`(corrVarType)) {
            corrArg = Expressions.parameter(
                Modifier.FINAL,
                corrVarType, getCorrelVariable()
            )
            corrRef = corrArg
        } else {
            corrArg = Expressions.parameter(
                Modifier.FINAL,
                Primitive.box(corrVarType), "\$box" + getCorrelVariable()
            )
            corrRef = corrBlock.append(
                getCorrelVariable(),
                Expressions.unbox(corrArg)
            ) as ParameterExpression
        }
        implementor.registerCorrelVariable(
            getCorrelVariable(), corrRef,
            corrBlock, leftResult.physType
        )
        val rightResult: Result = implementor.visitChild(this, 1, right as EnumerableRel, pref)
        implementor.clearCorrelVariable(getCorrelVariable())
        corrBlock.add(rightResult.block)
        val physType: PhysType = PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref.prefer(JavaRowFormat.CUSTOM)
        )
        val selector: Expression = EnumUtils.joinSelector(
            joinType, physType,
            ImmutableList.of(leftResult.physType, rightResult.physType)
        )
        builder.append(
            Expressions.call(
                leftExpression, BuiltInMethod.CORRELATE_JOIN.method,
                Expressions.constant(EnumUtils.toLinq4jJoinType(joinType)),
                Expressions.lambda(corrBlock.toBlock(), corrArg),
                selector
            )
        )
        return implementor.result(physType, builder.toBlock())
    }

    companion object {
        /** Creates an EnumerableCorrelate.  */
        fun create(
            left: RelNode,
            right: RelNode?,
            correlationId: CorrelationId?,
            requiredColumns: ImmutableBitSet?,
            joinType: JoinRelType?
        ): EnumerableCorrelate {
            val cluster: RelOptCluster = left.getCluster()
            val mq: RelMetadataQuery = cluster.getMetadataQuery()
            val traitSet: RelTraitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE)
                .replaceIfs(
                    RelCollationTraitDef.INSTANCE
                ) { RelMdCollation.enumerableCorrelate(mq, left, right, joinType) }
            return EnumerableCorrelate(
                cluster,
                traitSet,
                left,
                right,
                correlationId,
                requiredColumns,
                joinType
            )
        }
    }
}
