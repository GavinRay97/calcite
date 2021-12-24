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
package org.apache.calcite.schema

import org.apache.calcite.rel.RelCollation

/**
 * Statistics about a [Table].
 *
 *
 * Each of the methods may return `null` meaning "not known".
 *
 * @see Statistics
 */
interface Statistic {
    /** Returns the approximate number of rows in the table.  */
    @get:Nullable
    val rowCount: Double?
        get() = null

    /** Returns whether the given set of columns is a unique key, or a superset
     * of a unique key, of the table.
     */
    fun isKey(columns: ImmutableBitSet?): Boolean {
        return false
    }

    /** Returns a list of unique keys, or null if no key exist.  */
    @get:Nullable
    val keys: List<Any?>?
        get() = null

    /** Returns the collection of referential constraints (foreign-keys)
     * for this table.  */
    @get:Nullable
    val referentialConstraints: List<Any?>?
        get() = null

    /** Returns the collections of columns on which this table is sorted.  */
    @get:Nullable
    val collations: List<Any?>?
        get() = null

    /** Returns the distribution of the data in this table.  */
    @get:Nullable
    val distribution: RelDistribution?
        get() = null
}
