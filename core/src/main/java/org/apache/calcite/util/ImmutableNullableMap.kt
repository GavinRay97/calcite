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

import com.google.common.collect.ImmutableMap

/**
 * An immutable map that may contain null values.
 *
 *
 * If the map cannot contain null values, use [ImmutableMap].
 *
 * @param <K> Key type
 * @param <V> Value type
</V></K> */
object ImmutableNullableMap<K, V> : AbstractMap<K, V>() {
    private val SINGLETON_MAP: Map<Integer, Integer> = Collections.singletonMap(0, 0)

    /**
     * Returns an immutable map containing the given elements.
     *
     *
     * Behavior is as [ImmutableMap.copyOf]
     * except that this map allows nulls.
     */
    @SuppressWarnings(["JdkObsolete", "unchecked", "rawtypes"])
    fun <K, V> copyOf(map: Map<out K, V>): Map<K, V> {
        if (map is ImmutableNullableMap<*, *>
            || map is ImmutableMap
            || map === Collections.emptyMap() || map === Collections.emptyNavigableMap() || map.getClass() === org.apache.calcite.util.ImmutableNullableMap.SINGLETON_MAP.getClass()
        ) {
            return map as Map<K, V>
        }
        return if (map is SortedMap) {
            val sortedMap: SortedMap<K, V> = map as SortedMap
            try {
                val comparator: Comparator<in K> = sortedMap.comparator()
                if (comparator == null) {
                    ImmutableSortedMap.copyOf(sortedMap)
                } else {
                    ImmutableSortedMap.copyOf(sortedMap, comparator)
                }
            } catch (e: NullPointerException) {
                // Make an effectively immutable map by creating a mutable copy
                // and wrapping it to prevent modification. Unfortunately, if we see
                // it again we will not recognize that it is immutable and we will make
                // another copy.
                Collections.unmodifiableNavigableMap(TreeMap(sortedMap))
            }
        } else {
            try {
                ImmutableMap.copyOf(map)
            } catch (e: NullPointerException) {
                // Make an effectively immutable map by creating a mutable copy
                // and wrapping it to prevent modification. Unfortunately, if we see
                // it again we will not recognize that it is immutable and we will make
                // another copy.
                Collections.unmodifiableMap(HashMap(map))
            }
        }
    }

    /**
     * Returns an immutable navigable map containing the given entries.
     *
     *
     * Behavior is as [ImmutableSortedMap.copyOf]
     * except that this map allows nulls.
     */
    @SuppressWarnings(["JdkObsolete", "unchecked", "rawtypes"])
    fun <K, V> copyOf(
        map: SortedMap<out K, out V>
    ): Map<K, V> {
        if (map is ImmutableNullableMap<*, *>
            || map is ImmutableMap
            || map === Collections.emptyMap() || map === Collections.emptyNavigableMap()
        ) {
            return map
        }
        val sortedMap: SortedMap<K, V> = map as SortedMap
        return try {
            val comparator: Comparator<in K> = sortedMap.comparator()
            if (comparator == null) {
                ImmutableSortedMap.copyOf(sortedMap)
            } else {
                ImmutableSortedMap.copyOf(sortedMap, comparator)
            }
        } catch (e: NullPointerException) {
            // Make an effectively immutable map by creating a mutable copy
            // and wrapping it to prevent modification. Unfortunately, if we see
            // it again we will not recognize that it is immutable and we will make
            // another copy.
            Collections.unmodifiableNavigableMap(TreeMap(sortedMap))
        }
    }
}
