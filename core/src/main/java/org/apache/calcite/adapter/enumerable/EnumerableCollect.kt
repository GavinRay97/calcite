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

/** Implementation of [org.apache.calcite.rel.core.Collect] in
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention].  */
class EnumerableCollect(
    cluster: RelOptCluster?, traitSet: RelTraitSet?,
    input: RelNode?, rowType: RelDataType?
) : Collect(cluster, traitSet, input, rowType), EnumerableRel {
    /**
     * Creates an EnumerableCollect.
     *
     *
     * Use [.create] unless you know what you're doing.
     *
     * @param cluster   Cluster
     * @param traitSet  Trait set
     * @param input     Input relational expression
     * @param rowType   Row type
     */
    init {
        assert(getConvention() is EnumerableConvention)
        assert(getConvention() === input.getConvention())
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster, traitSet: RelTraitSet?,
        input: RelNode, fieldName: String?
    ) : this(
        cluster, traitSet, input,
        deriveRowType(
            cluster.getTypeFactory(), SqlTypeName.MULTISET, fieldName,
            input.getRowType()
        )
    ) {
    }

    @Override
    fun copy(
        traitSet: RelTraitSet?,
        newInput: RelNode?
    ): EnumerableCollect {
        return EnumerableCollect(getCluster(), traitSet, newInput, rowType())
    }

    @Override
    fun implement(implementor: EnumerableRelImplementor, pref: Prefer?): Result {
        val builder = BlockBuilder()
        val child: EnumerableRel = getInput() as EnumerableRel
        // REVIEW zabetak January 7, 2019: Even if we ask the implementor to provide a result
        // where records are represented as arrays (Prefer.ARRAY) this may not be respected.
        val result: Result = implementor.visitChild(this, 0, child, Prefer.ARRAY)
        val physType: PhysType = PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            JavaRowFormat.LIST
        )

        // final Enumerable child = <<child adapter>>;
        // final Enumerable<Object[]> converted = child.select(<<conversion code>>);
        // final List<Object[]> list = converted.toList();
        val child_: Expression = builder.append(
            "child", result.block
        )
        // In the internal representation of multisets , every element must be a record. In case the
        // result above is a scalar type we have to wrap it around a physical type capable of
        // representing records. For this reason the following conversion is necessary.
        // REVIEW zabetak January 7, 2019: If we can ensure that the input to this operator
        // has the correct physical type (e.g., respecting the Prefer.ARRAY above) then this conversion
        // can be removed.
        val conv_: Expression = builder.append(
            "converted", result.physType.convertTo(child_, JavaRowFormat.ARRAY)
        )
        val list_: Expression = builder.append(
            "list",
            Expressions.call(
                conv_,
                BuiltInMethod.ENUMERABLE_TO_LIST.method
            )
        )
        builder.add(
            Expressions.return_(
                null,
                Expressions.call(
                    BuiltInMethod.SINGLETON_ENUMERABLE.method, list_
                )
            )
        )
        return implementor.result(physType, builder.toBlock())
    }

    companion object {
        /**
         * Creates an EnumerableCollect.
         *
         * @param input          Input relational expression
         * @param rowType        Row type
         */
        fun create(input: RelNode, rowType: RelDataType?): Collect {
            val cluster: RelOptCluster = input.getCluster()
            val traitSet: RelTraitSet = cluster.traitSet().replace(EnumerableConvention.INSTANCE)
            return EnumerableCollect(cluster, traitSet, input, rowType)
        }
    }
}
