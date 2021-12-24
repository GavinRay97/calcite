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

/** Implementation of [org.apache.calcite.rel.core.Minus] in
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention].  */
class EnumerableMinus(
    cluster: RelOptCluster?, traitSet: RelTraitSet?,
    inputs: List<RelNode?>?, all: Boolean
) : Minus(cluster, traitSet, inputs, all), EnumerableRel {
    @Override
    fun copy(
        traitSet: RelTraitSet?, inputs: List<RelNode?>?,
        all: Boolean
    ): EnumerableMinus {
        return EnumerableMinus(getCluster(), traitSet, inputs, all)
    }

    @Override
    fun implement(implementor: EnumerableRelImplementor, pref: Prefer): Result {
        var pref: Prefer = pref
        val builder = BlockBuilder()
        var minusExp: Expression? = null
        for (ord in Ord.zip(inputs)) {
            val result: Result = implementor.visitChild(this, ord.i, ord.e, pref)
            val childExp: Expression = builder.append(
                "child" + ord.i,
                result.block
            )
            assert(childExp != null) { "childExp must not be null" }
            minusExp = if (minusExp == null) {
                childExp
            } else {
                Expressions.call(
                    minusExp,
                    BuiltInMethod.EXCEPT.method,
                    Expressions.list(childExp)
                        .appendIfNotNull(result.physType.comparer())
                        .append(Expressions.constant(all))
                )
            }

            // Once the first input has chosen its format, ask for the same for
            // other inputs.
            pref = pref.of(result.format)
        }
        builder.add(
            requireNonNull(minusExp) { "minusExp is null, inputs=" + inputs.toString() + ", rel=" + this })
        val physType: PhysType = PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref.prefer(JavaRowFormat.CUSTOM)
        )
        return implementor.result(physType, builder.toBlock())
    }
}
