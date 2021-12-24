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

/**
 * Relational expression that converts an enumerable input to interpretable
 * calling convention.
 *
 * @see org.apache.calcite.adapter.enumerable.EnumerableConvention
 *
 * @see org.apache.calcite.interpreter.BindableConvention
 */
class EnumerableBindable protected constructor(cluster: RelOptCluster, input: RelNode?) : ConverterImpl(
    cluster, ConventionTraitDef.INSTANCE,
    cluster.traitSetOf(BindableConvention.INSTANCE), input
), BindableRel {
    @Override
    fun copy(
        traitSet: RelTraitSet?,
        inputs: List<RelNode?>?
    ): EnumerableBindable {
        return EnumerableBindable(getCluster(), sole(inputs))
    }

    @get:Override
    val elementType: Class<Array<Object>>
        get() = Array<Object>::class.java

    @Override
    fun bind(dataContext: DataContext?): Enumerable<Array<Object>> {
        val map: ImmutableMap<String?, Object?> = ImmutableMap.of()
        val bindable: Bindable = EnumerableInterpretable.toBindable(
            map, null,
            getInput() as EnumerableRel, EnumerableRel.Prefer.ARRAY
        )
        val arrayBindable: ArrayBindable = EnumerableInterpretable.box(bindable)
        return arrayBindable.bind(dataContext)
    }

    @Override
    fun implement(implementor: InterpreterImplementor): Node {
        return Node {
            val sink: Sink = requireNonNull(
                implementor.relSinks.get(this@EnumerableBindable)
            ) { "relSinks.get is null for " + this@EnumerableBindable }.get(0)
            val enumerable: Enumerable<Array<Object>> = bind(implementor.dataContext)
            val enumerator: Enumerator<Array<Object>> = enumerable.enumerator()
            while (enumerator.moveNext()) {
                sink.send(Row.asCopy(enumerator.current()))
            }
        }
    }

    /**
     * Rule that converts any enumerable relational expression to bindable.
     *
     * @see EnumerableRules.TO_BINDABLE
     */
    class EnumerableToBindableConverterRule protected constructor(config: Config?) : ConverterRule(config) {
        @Override
        fun convert(rel: RelNode): RelNode {
            return EnumerableBindable(rel.getCluster(), rel)
        }

        companion object {
            /** Default configuration.  */
            val DEFAULT_CONFIG: Config = Config.INSTANCE
                .withConversion(
                    EnumerableRel::class.java,
                    EnumerableConvention.INSTANCE, BindableConvention.INSTANCE,
                    "EnumerableToBindableConverterRule"
                )
                .withRuleFactory { config: Config? -> EnumerableToBindableConverterRule(config) }
        }
    }
}
