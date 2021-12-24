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
package org.apache.calcite.util

import java.util.AbstractMap

/**
 * An implementation of `java.util.Map` that ignores any `put`
 * operation.
 *
 *
 * The implementation does not fully conform to `java.util.Map` API, as
 * any write operation would succeed, but any read operation would not return
 * any value.
 *
 * @param <K> the type of the keys for the map
 * @param <V> the type of the values for the map
</V></K> */
internal class BlackholeMap<K, V> private constructor() : AbstractMap<K, V>() {
    /**
     * Blackhole implementation of `Iterator`. Always empty.
     *
     * @param <E> type of the entries for the iterator
    </E> */
    private class BHIterator<E> private constructor() : Iterator<E> {
        @Override
        override fun hasNext(): Boolean {
            return false
        }

        @Override
        override fun next(): E {
            throw NoSuchElementException()
        }

        companion object {
            @SuppressWarnings("rawtypes")
            private val INSTANCE: Iterator = BHIterator<Any>()
            @SuppressWarnings("unchecked")
            fun <T> of(): Iterator<T> {
                return INSTANCE
            }
        }
    }

    /**
     * Blackhole implementation of `Set`. Always ignores add.
     *
     * @param <E> type of the entries for the set
    </E> */
    private class BHSet<E> : AbstractSet<E>() {
        @Override
        fun add(e: E): Boolean {
            return true
        }

        @Override
        operator fun iterator(): Iterator<E> {
            return BHIterator.of<Any>()
        }

        @Override
        fun size(): Int {
            return 0
        }

        companion object {
            @SuppressWarnings("rawtypes")
            private val INSTANCE: Set = BHSet<Any>()
            @SuppressWarnings("unchecked")
            fun <T> of(): Set<T> {
                return INSTANCE
            }
        }
    }

    @SuppressWarnings("contracts.postcondition.not.satisfied")
    @Override
    @Nullable
    fun put(key: K, value: V): V? {
        return null
    }

    @SuppressWarnings("override.return.invalid")
    @Override
    fun entrySet(): Set<Entry<K, V>> {
        return BHSet.of<Any>()
    }

    companion object {
        @SuppressWarnings("rawtypes")
        private val INSTANCE: Map = BlackholeMap<Any, Any>()

        /**
         * Gets an instance of `BlackholeMap`.
         *
         * @param <K> type of the keys for the map
         * @param <V> type of the values for the map
         * @return a blackhole map
        </V></K> */
        @SuppressWarnings("unchecked")
        fun <K, V> of(): Map<K, V> {
            return INSTANCE
        }
    }
}
