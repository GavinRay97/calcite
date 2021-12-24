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

/** Implementation of [org.apache.calcite.rel.core.Sort] in
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention].  */
class EnumerableSort(
    cluster: RelOptCluster?, traitSet: RelTraitSet?,
    input: RelNode?, collation: RelCollation?, @Nullable offset: RexNode?, @Nullable fetch: RexNode?
) : Sort(cluster, traitSet, input, collation, offset, fetch), EnumerableRel {
    /**
     * Creates an EnumerableSort.
     *
     *
     * Use [.create] unless you know what you're doing.
     */
    init {
        assert(getConvention() is EnumerableConvention)
        assert(getConvention() === input.getConvention())
        assert(fetch == null) { "fetch must be null" }
        assert(offset == null) { "offset must be null" }
    }

    @Override
    fun copy(
        traitSet: RelTraitSet?,
        newInput: RelNode?,
        newCollation: RelCollation?,
        @Nullable offset: RexNode?,
        @Nullable fetch: RexNode?
    ): EnumerableSort {
        return EnumerableSort(
            getCluster(), traitSet, newInput, newCollation,
            offset, fetch
        )
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
        val childExp: Expression = builder.append("child", result.block)
        val inputPhysType: PhysType = result.physType
        val pair: Pair<Expression, Expression> = inputPhysType.generateCollationKey(
            collation.getFieldCollations()
        )
        builder.add(
            Expressions.return_(
                null,
                Expressions.call(
                    childExp,
                    BuiltInMethod.ORDER_BY.method,
                    Expressions.list(
                        builder.append("keySelector", pair.left)
                    )
                        .appendIfNotNull(
                            builder.appendIfNotNull("comparator", pair.right)
                        )
                )
            )
        )
        return implementor.result(physType, builder.toBlock())
    }

    companion object {
        /** Creates an EnumerableSort.  */
        fun create(
            child: RelNode?, collation: RelCollation?,
            @Nullable offset: RexNode?, @Nullable fetch: RexNode?
        ): EnumerableSort {
            val cluster: RelOptCluster = child.getCluster()
            val traitSet: RelTraitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE)
                .replace(collation)
            return EnumerableSort(
                cluster, traitSet, child, collation, offset,
                fetch
            )
        }
    }
}
