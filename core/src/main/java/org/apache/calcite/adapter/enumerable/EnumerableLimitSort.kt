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

/**
 * Implementation of [org.apache.calcite.rel.core.Sort] in
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention].
 * It optimizes sorts that have a limit and an optional offset.
 */
class EnumerableLimitSort(
    cluster: RelOptCluster?,
    traitSet: RelTraitSet?,
    input: RelNode?,
    collation: RelCollation?,
    @Nullable offset: RexNode?,
    @Nullable fetch: RexNode?
) : Sort(cluster, traitSet, input, collation, offset, fetch), EnumerableRel {
    /**
     * Creates an EnumerableLimitSort.
     *
     *
     * Use [.create] unless you know what you're doing.
     */
    init {
        assert(this.getConvention() is EnumerableConvention)
        assert(this.getConvention() === input.getConvention())
    }

    @Override
    fun copy(
        traitSet: RelTraitSet?,
        newInput: RelNode?,
        newCollation: RelCollation?,
        @Nullable offset: RexNode?,
        @Nullable fetch: RexNode?
    ): EnumerableLimitSort {
        return EnumerableLimitSort(
            this.getCluster(),
            traitSet,
            newInput,
            newCollation,
            offset,
            fetch
        )
    }

    @Override
    fun implement(implementor: EnumerableRelImplementor, pref: Prefer?): Result {
        val builder = BlockBuilder()
        val child: EnumerableRel = this.getInput() as EnumerableRel
        val result: Result = implementor.visitChild(this, 0, child, pref)
        val physType: PhysType = PhysTypeImpl.of(
            implementor.getTypeFactory(),
            this.getRowType(),
            result.format
        )
        val childExp: Expression = builder.append("child", result.block)
        val inputPhysType: PhysType = result.physType
        val pair: Pair<Expression, Expression> = inputPhysType.generateCollationKey(this.collation.getFieldCollations())
        val fetchVal: Expression
        if (this.fetch == null) {
            fetchVal = Expressions.constant(Integer.valueOf(Integer.MAX_VALUE))
        } else {
            fetchVal = getExpression(this.fetch)
        }
        val offsetVal: Expression =
            if (this.offset == null) Expressions.constant(Integer.valueOf(0)) else getExpression(this.offset)
        builder.add(
            Expressions.return_(
                null, Expressions.call(
                    BuiltInMethod.ORDER_BY_WITH_FETCH_AND_OFFSET.method, Expressions.list(
                        childExp,
                        builder.append("keySelector", pair.left)
                    )
                        .appendIfNotNull(builder.appendIfNotNull("comparator", pair.right))
                        .appendIfNotNull(
                            builder.appendIfNotNull(
                                "offset",
                                Expressions.constant(offsetVal)
                            )
                        )
                        .appendIfNotNull(
                            builder.appendIfNotNull(
                                "fetch",
                                Expressions.constant(fetchVal)
                            )
                        )
                )
            )
        )
        return implementor.result(physType, builder.toBlock())
    }

    companion object {
        /** Creates an EnumerableLimitSort.  */
        fun create(
            input: RelNode,
            collation: RelCollation?,
            @Nullable offset: RexNode?,
            @Nullable fetch: RexNode?
        ): EnumerableLimitSort {
            val cluster: RelOptCluster = input.getCluster()
            val traitSet: RelTraitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE).replace(
                collation
            )
            return EnumerableLimitSort(cluster, traitSet, input, collation, offset, fetch)
        }
    }
}
