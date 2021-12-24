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

/** Map whose keys are names and can be accessed with and without case
 * sensitivity.
 *
 * @param <V> Value type
</V> */
class NameMap<V> private constructor(map: NavigableMap<String, V>) {
    private val map: NavigableMap<String, V>

    /** Creates a NameSet based on an existing set.  */
    init {
        this.map = map
        assert(this.map.comparator() === COMPARATOR)
    }

    /** Creates a NameMap, initially empty.  */
    constructor() : this(TreeMap<String, V>(COMPARATOR)) {}

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
                || obj is NameMap<*>
                && map.equals((obj as NameMap<*>).map))
    }

    fun put(name: String?, v: V) {
        map.put(name, v)
    }

    /** Returns a map containing all the entries in the map that match the given
     * name. If case-sensitive, that map will have 0 or 1 elements; if
     * case-insensitive, it may have 0 or more.  */
    fun range(name: String, caseSensitive: Boolean): NavigableMap<String, V> {
        val floorKey: Object
        val ceilingKey: Object
        if (caseSensitive) {
            floorKey = name
            ceilingKey = name
        } else {
            floorKey = COMPARATOR.floorKey(name)
            ceilingKey = COMPARATOR.ceilingKey(name)
        }
        val subMap: NavigableMap = (map as NavigableMap).subMap(floorKey, true, ceilingKey, true)
        return Collections.unmodifiableNavigableMap(subMap as NavigableMap<String?, V>)
    }

    /** Returns whether this map contains a given key, with a given
     * case-sensitivity.  */
    fun containsKey(name: String, caseSensitive: Boolean): Boolean {
        return !range(name, caseSensitive).isEmpty()
    }

    /** Returns the underlying map.  */
    fun map(): NavigableMap<String, V> {
        return map
    }

    @Experimental
    @Nullable
    fun remove(key: String?): V {
        return map.remove(key)
    }

    companion object {
        /** Creates a NameMap that is an immutable copy of a given map.  */
        fun <V> immutableCopyOf(names: Map<String?, V>?): NameMap<*> {
            return NameMap<V>(ImmutableSortedMap.copyOf(names, COMPARATOR))
        }
    }
}
