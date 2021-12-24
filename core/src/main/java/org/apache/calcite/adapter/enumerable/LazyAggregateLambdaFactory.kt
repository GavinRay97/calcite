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

/**
 * Generate aggregate lambdas that preserve the input source before calling each
 * aggregate adder, this implementation is generally used when we need to sort the input
 * before performing aggregation.
 *
 * @param <TSource> Type of the enumerable input source
 * @param <TKey> Type of the group-by key
 * @param <TOrigAccumulate> Type of the original accumulator
 * @param <TResult> Type of the enumerable output result
</TResult></TOrigAccumulate></TKey></TSource> */
class LazyAggregateLambdaFactory<TSource, TKey, TOrigAccumulate, TResult>(
    private val accumulatorInitializer: Function0<TOrigAccumulate>,
    private val accumulators: List<LazyAccumulator<TOrigAccumulate, TSource>>
) : AggregateLambdaFactory<TSource, TOrigAccumulate, LazyAggregateLambdaFactory.LazySource<TSource>?, TResult, TKey> {
    @Override
    fun accumulatorInitializer(): Function0<LazySource<TSource>> {
        return Function0<LazySource<TSource>> { LazySource<Any?>() }
    }

    @Override
    fun accumulatorAdder(): Function2<LazySource<TSource>, TSource, LazySource<TSource>> {
        return Function2<LazySource<TSource>, TSource, LazySource<TSource>> { lazySource, source ->
            lazySource.add(source)
            lazySource
        }
    }

    @Override
    fun singleGroupResultSelector(
        resultSelector: Function1<TOrigAccumulate, TResult>
    ): Function1<LazySource<TSource>, TResult> {
        return Function1<LazySource<TSource>, TResult> { lazySource ->
            val accumulator: TOrigAccumulate = accumulatorInitializer.apply()
            for (acc in accumulators) {
                acc.accumulate(lazySource, accumulator)
            }
            resultSelector.apply(accumulator)
        }
    }

    @Override
    fun resultSelector(
        resultSelector: Function2<TKey, TOrigAccumulate, TResult>
    ): Function2<TKey, LazySource<TSource>, TResult> {
        return Function2<TKey, LazySource<TSource>, TResult> { groupByKey, lazySource ->
            val accumulator: TOrigAccumulate = accumulatorInitializer.apply()
            for (acc in accumulators) {
                acc.accumulate(lazySource, accumulator)
            }
            resultSelector.apply(groupByKey, accumulator)
        }
    }

    /**
     * Cache the input sources. (Will be aggregated in result selector.)
     *
     * @param <TSource> Type of the enumerable input source.
    </TSource> */
    class LazySource<TSource> : Iterable<TSource> {
        private val list: List<TSource> = ArrayList()
        private fun add(source: TSource) {
            list.add(source)
        }

        @Override
        override fun iterator(): Iterator<TSource> {
            return list.iterator()
        }
    }

    /**
     * Accumulate on the cached input sources.
     *
     * @param <TOrigAccumulate> Type of the original accumulator
     * @param <TSource> Type of the enumerable input source.
    </TSource></TOrigAccumulate> */
    interface LazyAccumulator<TOrigAccumulate, TSource> {
        fun accumulate(sourceIterable: Iterable<TSource>?, accumulator: TOrigAccumulate)
    }
}
