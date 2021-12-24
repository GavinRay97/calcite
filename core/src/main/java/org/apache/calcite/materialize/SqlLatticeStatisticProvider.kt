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
package org.apache.calcite.materialize

import org.apache.calcite.DataContexts

/**
 * Implementation of [LatticeStatisticProvider] that gets statistics by
 * executing "SELECT COUNT(DISTINCT ...) ..." SQL queries.
 */
internal class SqlLatticeStatisticProvider private constructor(lattice: Lattice) : LatticeStatisticProvider {
    private val lattice: Lattice

    /** Creates a SqlLatticeStatisticProvider.  */
    init {
        this.lattice = requireNonNull(lattice, "lattice")
    }

    @Override
    fun cardinality(columns: List<Lattice.Column?>): Double {
        val counts: List<Double> = ArrayList()
        for (column in columns) {
            counts.add(cardinality(lattice, column))
        }
        return (Lattice.getRowCount(lattice.getFactRowCount(), counts) as Int).toDouble()
    }

    companion object {
        val FACTORY: Factory = Factory { lattice: Lattice -> SqlLatticeStatisticProvider(lattice) }
        val CACHED_FACTORY: Factory = Factory { lattice ->
            val provider: LatticeStatisticProvider = FACTORY.apply(lattice)
            CachingLatticeStatisticProvider(lattice, provider)
        }

        private fun cardinality(lattice: Lattice, column: Lattice.Column): Double {
            val sql: String = lattice.countSql(ImmutableBitSet.of(column.ordinal))
            val table: Table = DefaultTableFactory()
                .createTable(lattice.rootSchema, sql, ImmutableList.of())
            @Nullable val values: Array<Object> = Iterables.getOnlyElement(
                (table as ScannableTable).scan(
                    DataContexts.of(
                        MaterializedViewTable.MATERIALIZATION_CONNECTION,
                        lattice.rootSchema.plus()
                    )
                )
            )
            val value = values[0] as Number
            requireNonNull(value) { "count(*) produced null in $sql" }
            return value.doubleValue()
        }
    }
}
