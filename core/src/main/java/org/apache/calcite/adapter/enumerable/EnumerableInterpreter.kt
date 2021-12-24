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

import org.apache.calcite.adapter.java.JavaTypeFactory

/** Relational expression that executes its children using an interpreter.
 *
 *
 * Although quite a few kinds of [org.apache.calcite.rel.RelNode] can
 * be interpreted, this is only created by default for
 * [org.apache.calcite.schema.FilterableTable] and
 * [org.apache.calcite.schema.ProjectableFilterableTable].
 */
class EnumerableInterpreter(
    cluster: RelOptCluster?, traitSet: RelTraitSet?,
    input: RelNode?, factor: Double
) : SingleRel(cluster, traitSet, input), EnumerableRel {
    private val factor: Double

    /**
     * Creates an EnumerableInterpreter.
     *
     *
     * Use [.create] unless you know what you're doing.
     *
     * @param cluster Cluster
     * @param traitSet Traits
     * @param input Input relation
     * @param factor Cost multiply factor
     */
    init {
        assert(getConvention() is EnumerableConvention)
        this.factor = factor
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner?,
        mq: RelMetadataQuery?
    ): RelOptCost? {
        val cost: RelOptCost = super.computeSelfCost(planner, mq) ?: return null
        return cost.multiplyBy(factor)
    }

    @Override
    fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>?): RelNode {
        return EnumerableInterpreter(
            getCluster(), traitSet, sole(inputs),
            factor
        )
    }

    @Override
    fun implement(implementor: EnumerableRelImplementor, pref: Prefer?): Result {
        val typeFactory: JavaTypeFactory = implementor.getTypeFactory()
        val builder = BlockBuilder()
        val physType: PhysType = PhysTypeImpl.of(typeFactory, getRowType(), JavaRowFormat.ARRAY)
        val interpreter_: Expression = builder.append(
            "interpreter",
            Expressions.new_(
                Interpreter::class.java,
                implementor.getRootExpression(),
                implementor.stash(getInput(), RelNode::class.java)
            )
        )
        val sliced_: Expression = if (getRowType().getFieldCount() === 1) Expressions.call(
            BuiltInMethod.SLICE0.method,
            interpreter_
        ) else interpreter_
        builder.add(sliced_)
        return implementor.result(physType, builder.toBlock())
    }

    companion object {
        /**
         * Creates an EnumerableInterpreter.
         *
         * @param input Input relation
         * @param factor Cost multiply factor
         */
        fun create(input: RelNode, factor: Double): EnumerableInterpreter {
            val traitSet: RelTraitSet = input.getTraitSet()
                .replace(EnumerableConvention.INSTANCE)
            return EnumerableInterpreter(
                input.getCluster(), traitSet, input,
                factor
            )
        }
    }
}
