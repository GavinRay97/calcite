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

import org.apache.calcite.linq4j.function.Experimental

/**
 * Implementation of [RepeatUnion] in
 * [enumerable calling convention][EnumerableConvention].
 *
 *
 * NOTE: The current API is experimental and subject to change without
 * notice.
 */
@Experimental
class EnumerableRepeatUnion
/**
 * Creates an EnumerableRepeatUnion.
 */
internal constructor(
    cluster: RelOptCluster?, traitSet: RelTraitSet?,
    seed: RelNode?, iterative: RelNode?, all: Boolean, iterationLimit: Int
) : RepeatUnion(cluster, traitSet, seed, iterative, all, iterationLimit), EnumerableRel {
    @Override
    fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>): EnumerableRepeatUnion {
        assert(inputs.size() === 2)
        return EnumerableRepeatUnion(
            getCluster(), traitSet,
            inputs[0], inputs[1], all, iterationLimit
        )
    }

    @Override
    fun implement(implementor: EnumerableRelImplementor, pref: Prefer): Result {

        // return repeatUnion(<seedExp>, <iterativeExp>, iterationLimit, all, <comparer>);
        val builder = BlockBuilder()
        val seed: RelNode = getSeedRel()
        val iteration: RelNode = getIterativeRel()
        val seedResult: Result = implementor.visitChild(this, 0, seed as EnumerableRel, pref)
        val iterationResult: Result = implementor.visitChild(this, 1, iteration as EnumerableRel, pref)
        val seedExp: Expression = builder.append("seed", seedResult.block)
        val iterativeExp: Expression = builder.append("iteration", iterationResult.block)
        val physType: PhysType = PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref.prefer(seedResult.format)
        )
        val unionExp: Expression = Expressions.call(
            BuiltInMethod.REPEAT_UNION.method,
            seedExp,
            iterativeExp,
            Expressions.constant(iterationLimit, Int::class.javaPrimitiveType),
            Expressions.constant(all, Boolean::class.javaPrimitiveType),
            Util.first(physType.comparer(), Expressions.call(BuiltInMethod.IDENTITY_COMPARER.method))
        )
        builder.add(unionExp)
        return implementor.result(physType, builder.toBlock())
    }
}
