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

/** Implementation of [org.apache.calcite.rel.core.Uncollect] in
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention].  */
class EnumerableUncollect(
    cluster: RelOptCluster?, traitSet: RelTraitSet?,
    child: RelNode?, withOrdinality: Boolean
) : Uncollect(cluster, traitSet, child, withOrdinality, Collections.emptyList()), EnumerableRel {
    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster?, traitSet: RelTraitSet?,
        child: RelNode?
    ) : this(cluster, traitSet, child, false) {
    }

    /** Creates an EnumerableUncollect.
     *
     *
     * Use [.create] unless you know what you're doing.  */
    init {
        assert(getConvention() is EnumerableConvention)
        assert(getConvention() === child.getConvention())
    }

    @Override
    fun copy(
        traitSet: RelTraitSet?,
        newInput: RelNode?
    ): EnumerableUncollect {
        return EnumerableUncollect(
            getCluster(), traitSet, newInput,
            withOrdinality
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
            JavaRowFormat.LIST
        )

        // final Enumerable<List<Employee>> child = <<child adapter>>;
        // return child.selectMany(FLAT_PRODUCT);
        val child_: Expression = builder.append(
            "child", result.block
        )
        val fieldCounts: List<Integer> = ArrayList()
        val inputTypes: List<FlatProductInputType> = ArrayList()
        var lambdaForStructWithSingleItem: Expression? = null
        for (field in child.getRowType().getFieldList()) {
            val type: RelDataType = field.getType()
            if (type is MapSqlType) {
                fieldCounts.add(2)
                inputTypes.add(FlatProductInputType.MAP)
            } else {
                val elementType: RelDataType = getComponentTypeOrThrow(type)
                if (elementType.isStruct()) {
                    if (elementType.getFieldCount() === 1 && child.getRowType().getFieldList()
                            .size() === 1 && !withOrdinality
                    ) {
                        // Solves CALCITE-4063: if we are processing a single field, which is a struct with a
                        // single item inside, and no ordinality; the result must be a scalar, hence use a
                        // special lambda that does not return lists, but the (single) items within those lists
                        lambdaForStructWithSingleItem = Expressions.call(BuiltInMethod.FLAT_LIST.method)
                    } else {
                        fieldCounts.add(elementType.getFieldCount())
                        inputTypes.add(FlatProductInputType.LIST)
                    }
                } else {
                    fieldCounts.add(-1)
                    inputTypes.add(FlatProductInputType.SCALAR)
                }
            }
        }
        val lambda: Expression =
            if (lambdaForStructWithSingleItem != null) lambdaForStructWithSingleItem else Expressions.call(
                BuiltInMethod.FLAT_PRODUCT.method,
                Expressions.constant(Ints.toArray(fieldCounts)),
                Expressions.constant(withOrdinality),
                Expressions.constant(
                    inputTypes.toArray(arrayOfNulls<FlatProductInputType>(0))
                )
            )
        builder.add(
            Expressions.return_(
                null,
                Expressions.call(
                    child_,
                    BuiltInMethod.SELECT_MANY.method,
                    lambda
                )
            )
        )
        return implementor.result(physType, builder.toBlock())
    }

    companion object {
        /**
         * Creates an EnumerableUncollect.
         *
         *
         * Each field of the input relational expression must be an array or
         * multiset.
         *
         * @param traitSet Trait set
         * @param input    Input relational expression
         * @param withOrdinality Whether output should contain an ORDINALITY column
         */
        fun create(
            traitSet: RelTraitSet?, input: RelNode,
            withOrdinality: Boolean
        ): EnumerableUncollect {
            val cluster: RelOptCluster = input.getCluster()
            return EnumerableUncollect(cluster, traitSet, input, withOrdinality)
        }
    }
}
