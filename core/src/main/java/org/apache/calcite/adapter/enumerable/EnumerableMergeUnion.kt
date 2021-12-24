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

import org.apache.calcite.linq4j.Ord

/** Implementation of [org.apache.calcite.rel.core.Union] in
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention].
 * Performs a union (or union all) of all its inputs (which must be already sorted),
 * respecting the order.  */
class EnumerableMergeUnion protected constructor(
    cluster: RelOptCluster?, traitSet: RelTraitSet, inputs: List<RelNode?>,
    all: Boolean
) : EnumerableUnion(cluster, traitSet, inputs, all) {
    init {
        val collation: RelCollation = traitSet.getCollation()
        if (collation == null || collation.getFieldCollations().isEmpty()) {
            throw IllegalArgumentException("EnumerableMergeUnion with no collation")
        }
        for (input in inputs) {
            val inputCollation: RelCollation = input.getTraitSet().getCollation()
            if (inputCollation == null || !inputCollation.satisfies(collation)) {
                throw IllegalArgumentException(
                    "EnumerableMergeUnion input does not satisfy collation. "
                            + "EnumerableMergeUnion collation: " + collation + ". Input collation: "
                            + inputCollation + ". Input: " + input
                )
            }
        }
    }

    @Override
    fun copy(
        traitSet: RelTraitSet, inputs: List<RelNode?>,
        all: Boolean
    ): EnumerableMergeUnion {
        return EnumerableMergeUnion(getCluster(), traitSet, inputs, all)
    }

    @Override
    fun implement(implementor: EnumerableRelImplementor, pref: Prefer): Result {
        val builder = BlockBuilder()
        val inputListExp: ParameterExpression = Expressions.parameter(
            List::class.java,
            builder.newName("mergeUnionInputs" + Integer.toUnsignedString(this.getId()))
        )
        builder.add(Expressions.declare(0, inputListExp, Expressions.new_(ArrayList::class.java)))
        for (ord in Ord.zip(inputs)) {
            val result: Result = implementor.visitChild(this, ord.i, ord.e, pref)
            val childExp: Expression = builder.append("child" + ord.i, result.block)
            builder.add(
                Expressions.statement(
                    Expressions.call(inputListExp, BuiltInMethod.COLLECTION_ADD.method, childExp)
                )
            )
        }
        val physType: PhysType = PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref.prefer(JavaRowFormat.CUSTOM)
        )
        val collation: RelCollation = getTraitSet().getCollation()
        if (collation == null || collation.getFieldCollations().isEmpty()) {
            // should not happen
            throw IllegalStateException("EnumerableMergeUnion with no collation")
        }
        val pair: Pair<Expression, Expression> = physType.generateCollationKey(collation.getFieldCollations())
        val sortKeySelector: Expression = pair.left
        val sortComparator: Expression = pair.right
        val equalityComparator: Expression = Util.first(
            physType.comparer(),
            Expressions.call(BuiltInMethod.IDENTITY_COMPARER.method)
        )
        val unionExp: Expression = Expressions.call(
            BuiltInMethod.MERGE_UNION.method,
            inputListExp,
            sortKeySelector,
            sortComparator,
            Expressions.constant(all, Boolean::class.javaPrimitiveType),
            equalityComparator
        )
        builder.add(unionExp)
        return implementor.result(physType, builder.toBlock())
    }

    companion object {
        fun create(
            collation: RelCollation?, inputs: List<RelNode>,
            all: Boolean
        ): EnumerableMergeUnion {
            val cluster: RelOptCluster = inputs[0].getCluster()
            val traitSet: RelTraitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE).replace(
                collation
            )
            return EnumerableMergeUnion(cluster, traitSet, inputs, all)
        }
    }
}
