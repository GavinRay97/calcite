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

import org.apache.calcite.linq4j.function.Experimental

/** Multimap whose keys are names and can be accessed with and without case
 * sensitivity.
 *
 * @param <V> Value type
</V> */
class NameMultimap<V> private constructor(map: NameMap<List<V>>) {
    private val map: NameMap<List<V>>

    /** Creates a NameMultimap based on an existing map.  */
    init {
        this.map = map
        assert(map.map().comparator() === COMPARATOR)
    }

    /** Creates a NameMultimap, initially empty.  */
    constructor() : this(NameMap()) {}

    @Override
    override fun toString(): String {
        return map.toString()
    }

    @Override
    override fun hashCode(): Int {
        return map.hashCode()
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (this === obj
                || obj is NameMultimap<*>
                && map.equals((obj as NameMultimap<*>).map))
    }

    /** Adds an entry to this multimap.  */
    fun put(name: String?, v: V) {
        val list: List<V> = map().computeIfAbsent(name) { k -> ArrayList() }
        list.add(v)
    }

    /** Removes all entries that have the given case-sensitive key.
     *
     * @return Whether a value was removed
     */
    @Experimental
    fun remove(key: String?, value: V): Boolean {
        val list: List<V> = map().get(key) ?: return false
        return list.remove(value)
    }

    /** Returns a map containing all the entries in this multimap that match the
     * given name.  */
    fun range(
        name: String?,
        caseSensitive: Boolean
    ): Collection<Map.Entry<String, V>> {
        val range: NavigableMap<String, List<V>> = map.range(name!!, caseSensitive)
        val result: List<Pair<String, V>> = range.entrySet().stream()
            .flatMap { e -> e.getValue().stream().map { v -> Pair.of(e.getKey(), v) } }
            .collect(Collectors.toList())
        return Collections.unmodifiableList(result)
    }

    /** Returns whether this map contains a given key, with a given
     * case-sensitivity.  */
    fun containsKey(name: String?, caseSensitive: Boolean): Boolean {
        return map.containsKey(name!!, caseSensitive)
    }

    /** Returns the underlying map.
     * Its size is the number of keys, not the number of values.  */
    fun map(): NavigableMap<String, List<V>> {
        return map.map()
    }
}
