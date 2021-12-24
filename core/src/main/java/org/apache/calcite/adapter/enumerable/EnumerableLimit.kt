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

import org.apache.calcite.DataContext

/** Relational expression that applies a limit and/or offset to its input.  */
class EnumerableLimit(
    cluster: RelOptCluster?,
    traitSet: RelTraitSet?,
    input: RelNode?,
    @Nullable offset: RexNode?,
    @Nullable fetch: RexNode?
) : SingleRel(cluster, traitSet, input), EnumerableRel {
    @Nullable
    val offset: RexNode?

    @Nullable
    val fetch: RexNode?

    /** Creates an EnumerableLimit.
     *
     *
     * Use [.create] unless you know what you're doing.  */
    init {
        this.offset = offset
        this.fetch = fetch
        assert(getConvention() is EnumerableConvention)
        assert(getConvention() === input.getConvention())
    }

    @Override
    fun copy(
        traitSet: RelTraitSet?,
        newInputs: List<RelNode?>?
    ): EnumerableLimit {
        return EnumerableLimit(
            getCluster(),
            traitSet,
            sole(newInputs),
            offset,
            fetch
        )
    }

    @Override
    fun explainTerms(pw: RelWriter?): RelWriter {
        return super.explainTerms(pw)
            .itemIf("offset", offset, offset != null)
            .itemIf("fetch", fetch, fetch != null)
    }

    @Override
    fun implement(implementor: EnumerableRelImplementor, pref: Prefer?): Result {
        val builder = BlockBuilder()
        val child: EnumerableRel = getInput() as EnumerableRel
        val result: Result = implementor.visitChild(this, 0, child, pref)
        val physType: PhysType = PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            result.format
        )
        var v: Expression = builder.append("child", result.block)
        if (offset != null) {
            v = builder.append(
                "offset",
                Expressions.call(
                    v,
                    BuiltInMethod.SKIP.method,
                    getExpression(offset)
                )
            )
        }
        if (fetch != null) {
            v = builder.append(
                "fetch",
                Expressions.call(
                    v,
                    BuiltInMethod.TAKE.method,
                    getExpression(fetch)
                )
            )
        }
        builder.add(
            Expressions.return_(
                null,
                v
            )
        )
        return implementor.result(physType, builder.toBlock())
    }

    companion object {
        /** Creates an EnumerableLimit.  */
        fun create(
            input: RelNode, @Nullable offset: RexNode?,
            @Nullable fetch: RexNode?
        ): EnumerableLimit {
            val cluster: RelOptCluster = input.getCluster()
            val mq: RelMetadataQuery = cluster.getMetadataQuery()
            val traitSet: RelTraitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE)
                .replaceIfs(
                    RelCollationTraitDef.INSTANCE
                ) { RelMdCollation.limit(mq, input) }
                .replaceIf(
                    RelDistributionTraitDef.INSTANCE
                ) { RelMdDistribution.limit(mq, input) }
            return EnumerableLimit(cluster, traitSet, input, offset, fetch)
        }

        fun getExpression(rexNode: RexNode): Expression {
            return if (rexNode is RexDynamicParam) {
                val param: RexDynamicParam = rexNode as RexDynamicParam
                Expressions.convert_(
                    Expressions.call(
                        DataContext.ROOT,
                        BuiltInMethod.DATA_CONTEXT_GET.method,
                        Expressions.constant("?" + param.getIndex())
                    ),
                    Integer::class.java
                )
            } else {
                Expressions.constant(RexLiteral.intValue(rexNode))
            }
        }
    }
}
