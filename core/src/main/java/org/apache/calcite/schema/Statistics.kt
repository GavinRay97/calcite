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
 * Utility functions regarding [Statistic].
 */
object Statistics {
    /** Returns a [Statistic] that knows nothing about a table.  */
    val UNKNOWN: Statistic = object : Statistic() {}

    /** Returns a statistic with a given set of referential constraints.  */
    fun of(@Nullable referentialConstraints: List<RelReferentialConstraint?>?): Statistic {
        return of(
            null, null,
            referentialConstraints, null
        )
    }

    /** Returns a statistic with a given row count and set of unique keys.  */
    fun of(
        rowCount: Double,
        @Nullable keys: List<ImmutableBitSet?>?
    ): Statistic {
        return of(
            rowCount, keys, null,
            null
        )
    }

    /** Returns a statistic with a given row count, set of unique keys,
     * and collations.  */
    fun of(
        rowCount: Double,
        @Nullable keys: List<ImmutableBitSet?>?,
        @Nullable collations: List<RelCollation?>?
    ): Statistic {
        return of(rowCount, keys, null, collations)
    }

    /** Returns a statistic with a given row count, set of unique keys,
     * referential constraints, and collations.  */
    fun of(
        @Nullable rowCount: Double?,
        @Nullable keys: List<ImmutableBitSet?>?,
        @Nullable referentialConstraints: List<RelReferentialConstraint?>?,
        @Nullable collations: List<RelCollation?>?
    ): Statistic {
        val keysCopy: List<ImmutableBitSet> = if (keys == null) ImmutableList.of() else ImmutableList.copyOf(keys)
        val referentialConstraintsCopy: List<RelReferentialConstraint>? =
            if (referentialConstraints == null) null else ImmutableList.copyOf(referentialConstraints)
        val collationsCopy: List<RelCollation>? = if (collations == null) null else ImmutableList.copyOf(collations)
        return object : Statistic() {
            @get:Nullable
            @get:Override
            override val rowCount: Double?
                get() = rowCount

            @Override
            override fun isKey(columns: ImmutableBitSet): Boolean {
                for (key in keysCopy) {
                    if (columns.contains(key)) {
                        return true
                    }
                }
                return false
            }

            @get:Nullable
            @get:Override
            override val keys: List<Any?>?
                get() = keysCopy

            @get:Nullable
            @get:Override
            override val referentialConstraints: List<Any?>?
                get() = referentialConstraintsCopy

            @get:Nullable
            @get:Override
            override val collations: List<Any?>?
                get() = collationsCopy
        }
    }
}
