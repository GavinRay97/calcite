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

import com.google.common.collect.ImmutableList

/** Unmodifiable view onto multiple backing maps. An element occurs in the map
 * if it occurs in any of the backing maps; the value is the value that occurs
 * in the first map that contains the key.
 *
 * @param <K> Key type
 * @param <V> Value type
</V></K> */
class CompositeMap<K, V>(maps: ImmutableList<Map<K, V>?>) : Map<K, V> {
    private val maps: ImmutableList<Map<K, V>>

    init {
        this.maps = maps
    }

    @Override
    fun size(): Int {
        return keySet().size()
    }

    @Override
    override fun isEmpty(): Boolean {
        // Empty iff all maps are empty.
        for (map in maps) {
            if (!map.isEmpty()) {
                return false
            }
        }
        return true
    }

    @SuppressWarnings("contracts.conditional.postcondition.not.satisfied")
    @Override
    override fun containsKey(@Nullable key: Object): Boolean {
        for (map in maps) {
            if (map.containsKey(key)) {
                return true
            }
        }
        return false
    }

    @Override
    override fun containsValue(@Nullable value: Object): Boolean {
        for (map in maps) {
            if (map.containsValue(value)) {
                return true
            }
        }
        return false
    }

    @Override
    @Nullable
    override fun get(@Nullable key: Object): V? {
        for (map in maps) {
            if (map.containsKey(key)) {
                return map[key]
            }
        }
        return null
    }

    @Override
    fun put(key: K, value: V): V {
        // we are an unmodifiable view on the maps
        throw UnsupportedOperationException()
    }

    @Override
    fun remove(@Nullable key: Object?): V {
        // we are an unmodifiable view on the maps
        throw UnsupportedOperationException()
    }

    @Override
    fun putAll(m: Map<out K, V>?) {
        // we are an unmodifiable view on the maps
        throw UnsupportedOperationException()
    }

    @Override
    fun clear() {
        // we are an unmodifiable view on the maps
        throw UnsupportedOperationException()
    }

    @SuppressWarnings("return.type.incompatible")
    @Override
    fun keySet(): Set<K> {
        val keys: Set<K> = LinkedHashSet()
        for (map in maps) {
            keys.addAll(map.keySet())
        }
        return keys
    }

    private fun combinedMap(): Map<K, V> {
        val builder: ImmutableMap.Builder<K, V> = ImmutableMap.builder()
        val keys: Set<K> = LinkedHashSet()
        for (map in maps) {
            for (entry in map.entrySet()) {
                if (keys.add(entry.getKey())) {
                    builder.put(entry)
                }
            }
        }
        return builder.build()
    }

    @Override
    fun values(): Collection<V> {
        return combinedMap().values()
    }

    @SuppressWarnings("return.type.incompatible")
    @Override
    fun entrySet(): Set<Entry<K, V>> {
        return combinedMap().entrySet()
    }

    companion object {
        /** Creates a CompositeMap.  */ // Would like to use '@SafeVarargs' but JDK 1.6 doesn't support it.
        @SafeVarargs
        fun <K, V> of(
            map0: Map<K, V>,
            vararg maps: Map<K, V>
        ): CompositeMap<K, V> {
            return CompositeMap<Any, Any>(list(map0, maps))
        }

        private fun <E> list(e: E, es: Array<E>): ImmutableList<E> {
            val builder: ImmutableList.Builder<E> = ImmutableList.builder()
            builder.add(e)
            for (map in es) {
                builder.add(map)
            }
            return builder.build()
        }
    }
}
