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
package org.apache.calcite.sql.validate

import org.apache.calcite.rel.type.RelDataType

/**
 * Checks whether two names are the same according to a case-sensitivity policy.
 *
 * @see SqlNameMatchers
 */
interface SqlNameMatcher {
    /** Returns whether name matching is case-sensitive.  */
    val isCaseSensitive: Boolean

    /** Returns a name matches another.
     *
     * @param string Name written in code
     * @param name Name of object we are trying to match
     * @return Whether matches
     */
    fun matches(string: String?, name: String?): Boolean

    /** Looks up an item in a map.  */
    operator fun <K : List<String?>?, V> get(
        map: Map<K, V>?, prefixNames: List<String?>?,
        names: List<String?>?
    ): @Nullable V?

    /** Returns the most recent match.
     *
     *
     * In the default implementation,
     * throws [UnsupportedOperationException].  */
    fun bestString(): String?

    /** Finds a field with a given name, using the current case-sensitivity,
     * returning null if not found.
     *
     * @param rowType    Row type
     * @param fieldName Field name
     * @return Field, or null if not found
     */
    @Nullable
    fun field(rowType: RelDataType?, fieldName: String?): RelDataTypeField?

    /** Returns how many times a string occurs in a collection.
     *
     *
     * Similar to [java.util.Collections.frequency].  */
    fun frequency(names: Iterable<String?>?, name: String?): Int

    /** Returns the index of the first element of a collection that matches.  */
    fun indexOf(names: Iterable<String?>?, name: String?): Int {
        return Iterables.indexOf(names) { n -> matches(n, name) }
    }

    /** Creates a set that has the same case-sensitivity as this matcher.  */
    fun createSet(): Set<String>
}
