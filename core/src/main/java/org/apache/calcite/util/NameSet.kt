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

import com.google.common.collect.Maps

/** Set of names that can be accessed with and without case sensitivity.  */
class NameSet private constructor(names: NameMap<Object>) {
    private val names: NameMap<Object>

    /** Creates a NameSet based on an existing set.  */
    init {
        this.names = names
    }

    /** Creates a NameSet, initially empty.  */
    constructor() : this(NameMap()) {}

    @Override
    override fun toString(): String {
        return names.map().keySet().toString()
    }

    @Override
    override fun hashCode(): Int {
        return names.hashCode()
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (this === obj
                || obj is NameSet
                && names.equals((obj as NameSet).names))
    }

    fun add(name: String?) {
        names.put(name, DUMMY)
    }

    /** Returns an iterable over all the entries in the set that match the given
     * name. If case-sensitive, that iterable will have 0 or 1 elements; if
     * case-insensitive, it may have 0 or more.  */
    fun range(name: String?, caseSensitive: Boolean): Collection<String> {
        // This produces checkerframework false-positive
        // type of expression: Set<@KeyFor("this.names.range(name, caseSensitive)") String>
        // method return type: Collection<String>
        return names.range(name!!, caseSensitive).keySet()
    }

    /** Returns whether this set contains the given name, with a given
     * case-sensitivity.  */
    fun contains(name: String?, caseSensitive: Boolean): Boolean {
        return names.containsKey(name!!, caseSensitive)
    }

    /** Returns the contents as an iterable.  */
    fun iterable(): Iterable<String> {
        return Collections.unmodifiableSet(names.map().keySet())
    }

    companion object {
        val COMPARATOR: Comparator<String> = CaseInsensitiveComparator.COMPARATOR
        private val DUMMY: Object = Object()

        /** Creates a NameSet that is an immutable copy of a given collection.  */
        fun immutableCopyOf(names: Set<String?>?): NameSet {
            return NameSet(NameMap.immutableCopyOf(Maps.asMap(names) { k -> DUMMY }))
        }
    }
}
