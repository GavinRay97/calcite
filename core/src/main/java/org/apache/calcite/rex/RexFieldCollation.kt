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
package org.apache.calcite.rex

import org.apache.calcite.rel.RelFieldCollation
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.util.Pair
import com.google.common.collect.ImmutableMap
import com.google.common.collect.ImmutableSet
import com.google.common.collect.Sets
import java.util.Set

/**
 * Expression combined with sort flags (DESCENDING, NULLS LAST).
 */
class RexFieldCollation(left: RexNode?, right: Set<SqlKind?>?) :
    Pair<RexNode?, ImmutableSet<SqlKind?>?>(left, KINDS.get(right)) {
    @Override
    override fun toString(): String {
        val s: String = left.toString()
        if (right.isEmpty()) {
            return s
        }
        val b = StringBuilder(s)
        for (operator in right) {
            when (operator) {
                DESCENDING -> b.append(" DESC")
                NULLS_FIRST -> b.append(" NULLS FIRST")
                NULLS_LAST -> b.append(" NULLS LAST")
                else -> throw AssertionError(operator)
            }
        }
        return b.toString()
    }

    val direction: RelFieldCollation.Direction
        get() = if (right.contains(SqlKind.DESCENDING)) RelFieldCollation.Direction.DESCENDING else RelFieldCollation.Direction.ASCENDING
    val nullDirection: RelFieldCollation.NullDirection
        get() = if (right.contains(SqlKind.NULLS_LAST)) RelFieldCollation.NullDirection.LAST else if (right.contains(
                SqlKind.NULLS_FIRST
            )
        ) RelFieldCollation.NullDirection.FIRST else direction.defaultNullDirection()

    /** Helper, used during initialization, that builds a canonizing map from
     * sets of `SqlKind` to immutable sets of `SqlKind`.  */
    private class Initializer {
        val builder: ImmutableMap.Builder<Set<SqlKind>, ImmutableSet<SqlKind>> = ImmutableMap.builder()
        fun add(): Initializer {
            return add(ImmutableSet.of())
        }

        fun add(kind: SqlKind?, vararg kinds: SqlKind?): Initializer {
            return add(Sets.immutableEnumSet(kind, kinds))
        }

        private fun add(set: ImmutableSet<SqlKind>): Initializer {
            builder.put(set, set)
            return this
        }

        fun build(): ImmutableMap<Set<SqlKind>, ImmutableSet<SqlKind>> {
            return builder.build()
        }
    }

    companion object {
        /** Canonical map of all combinations of [SqlKind] values that can ever
         * occur. We use a canonical map to save a bit of memory. Because the sets
         * are EnumSets they have predictable order for toString().  */
        private val KINDS: ImmutableMap<Set<SqlKind>, ImmutableSet<SqlKind>> = Initializer()
            .add()
            .add(SqlKind.NULLS_FIRST)
            .add(SqlKind.NULLS_LAST)
            .add(SqlKind.DESCENDING)
            .add(SqlKind.DESCENDING, SqlKind.NULLS_FIRST)
            .add(SqlKind.DESCENDING, SqlKind.NULLS_LAST)
            .build()
    }
}
