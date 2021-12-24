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

import org.apache.calcite.linq4j.function.Function0
import org.apache.calcite.linq4j.function.Function1
import org.apache.calcite.linq4j.function.Function2
import java.util.List

/**
 * Implementation of [AggregateLambdaFactory] that applies a sequence of
 * accumulator adders to input source.
 *
 * @param <TSource> Type of the enumerable input source
 * @param <TAccumulate> Type of the accumulator
 * @param <TResult> Type of the enumerable output result
 * @param <TKey> Type of the group-by key
</TKey></TResult></TAccumulate></TSource> */
class BasicAggregateLambdaFactory<TSource, TAccumulate, TResult, TKey>(
    private val accumulatorInitializer: Function0<TAccumulate>,
    accumulatorAdders: List<Function2<TAccumulate, TSource, TAccumulate>?>?
) : AggregateLambdaFactory<TSource, TAccumulate, TAccumulate, TResult, TKey> {
    private val accumulatorAdderDecorator: Function2<TAccumulate, TSource, TAccumulate>

    init {
        accumulatorAdderDecorator = AccumulatorAdderSeq(accumulatorAdders)
    }

    @Override
    override fun accumulatorInitializer(): Function0<TAccumulate> {
        return accumulatorInitializer
    }

    @Override
    override fun accumulatorAdder(): Function2<TAccumulate, TSource, TAccumulate> {
        return accumulatorAdderDecorator
    }

    @Override
    fun singleGroupResultSelector(
        resultSelector: Function1<TAccumulate, TResult>
    ): Function1<TAccumulate, TResult> {
        return resultSelector
    }

    @Override
    fun resultSelector(
        resultSelector: Function2<TKey, TAccumulate, TResult>
    ): Function2<TKey, TAccumulate, TResult> {
        return resultSelector
    }

    /**
     * Decorator class of a sequence of accumulator adder functions.
     */
    private inner class AccumulatorAdderSeq internal constructor(
        private val accumulatorAdders: List<Function2<TAccumulate, TSource, TAccumulate>>
    ) : Function2<TAccumulate, TSource, TAccumulate> {
        @Override
        fun apply(accumulator: TAccumulate, source: TSource): TAccumulate {
            var result = accumulator
            for (accumulatorAdder in accumulatorAdders) {
                result = accumulatorAdder.apply(accumulator, source)
            }
            return result
        }
    }
}
