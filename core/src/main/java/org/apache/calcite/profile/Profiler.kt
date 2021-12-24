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
package org.apache.calcite.profile

import org.apache.calcite.materialize.Lattice
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.util.JsonBuilder
import org.apache.calcite.util.Util
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.ImmutableSortedSet
import java.math.BigDecimal
import java.math.MathContext
import java.math.RoundingMode
import java.util.ArrayList
import java.util.Collection
import java.util.List
import java.util.Map
import java.util.NavigableSet
import java.util.SortedSet
import java.util.Objects.requireNonNull

/**
 * Analyzes data sets.
 */
interface Profiler {
    /** Creates a profile of a data set.
     *
     * @param rows List of rows. Can be iterated over more than once (maybe not
     * cheaply)
     * @param columns Column definitions
     *
     * @param initialGroups List of combinations of columns that should be
     * profiled early, because they may be interesting
     *
     * @return A profile describing relationships within the data set
     */
    fun profile(
        rows: Iterable<List<Comparable?>?>?, columns: List<Column?>?,
        initialGroups: Collection<ImmutableBitSet?>?
    ): Profile?

    /** Column.  */
    class Column
    /** Creates a Column.
     *
     * @param ordinal Unique and contiguous within a particular data set
     * @param name Name of the column
     */(val ordinal: Int, val name: String) : Comparable<Column?> {
        @Override
        override fun hashCode(): Int {
            return ordinal
        }

        @Override
        override fun equals(@Nullable o: Object): Boolean {
            return (this === o
                    || o is Column
                    && ordinal == (o as Column).ordinal)
        }

        @Override
        operator fun compareTo(column: Column): Int {
            return Integer.compare(ordinal, column.ordinal)
        }

        @Override
        override fun toString(): String {
            return name
        }

        companion object {
            fun toOrdinals(columns: Iterable<Column>): ImmutableBitSet {
                val builder: ImmutableBitSet.Builder = ImmutableBitSet.builder()
                for (column in columns) {
                    builder.set(column.ordinal)
                }
                return builder.build()
            }
        }
    }

    /** Statistic produced by the profiler.  */
    interface Statistic {
        fun toMap(jsonBuilder: JsonBuilder?): Object?
    }

    /** Whole data set.  */
    class RowCount(val rowCount: Int) : Statistic {
        @Override
        override fun toMap(jsonBuilder: JsonBuilder): Object {
            val map: Map<String, Object> = jsonBuilder.map()
            map.put("type", "rowCount")
            map.put("rowCount", rowCount)
            return map
        }
    }

    /** Unique key.  */
    class Unique(columns: SortedSet<Column?>?) : Statistic {
        val columns: NavigableSet<Column>

        init {
            this.columns = ImmutableSortedSet.copyOf(columns)
        }

        @Override
        override fun toMap(jsonBuilder: JsonBuilder): Object {
            val map: Map<String, Object> = jsonBuilder.map()
            map.put("type", "unique")
            map.put("columns", FunctionalDependency.getObjects(jsonBuilder, columns))
            return map
        }
    }

    /** Functional dependency.  */
    class FunctionalDependency internal constructor(columns: SortedSet<Column?>?, dependentColumn: Column) : Statistic {
        val columns: NavigableSet<Column>
        val dependentColumn: Column

        init {
            this.columns = ImmutableSortedSet.copyOf(columns)
            this.dependentColumn = dependentColumn
        }

        @Override
        override fun toMap(jsonBuilder: JsonBuilder): Object {
            val map: Map<String, Object> = jsonBuilder.map()
            map.put("type", "fd")
            map.put("columns", getObjects(jsonBuilder, columns))
            map.put("dependentColumn", dependentColumn.name)
            return map
        }

        companion object {
            fun getObjects(
                jsonBuilder: JsonBuilder,
                columns: NavigableSet<Column>
            ): List<Object> {
                val list: List<Object> = jsonBuilder.list()
                for (column in columns) {
                    list.add(column.name)
                }
                return list
            }
        }
    }

    /** Value distribution, including cardinality and optionally values, of a
     * column or set of columns. If the set of columns is empty, it describes
     * the number of rows in the entire data set.  */
    class Distribution(
        columns: SortedSet<Column?>?, @Nullable values: SortedSet<Comparable?>?,
        cardinality: Double, nullCount: Int, expectedCardinality: Double,
        minimal: Boolean
    ) : Statistic {
        val columns: NavigableSet<Column>

        @Nullable
        val values: NavigableSet<Comparable>?
        val cardinality: Double
        val nullCount: Int
        val expectedCardinality: Double
        val minimal: Boolean

        /** Creates a Distribution.
         *
         * @param columns Column or columns being described
         * @param values Values of columns, or null if there are too many
         * @param cardinality Number of distinct values
         * @param nullCount Number of rows where this column had a null value;
         * @param expectedCardinality Expected cardinality
         * @param minimal Whether the distribution is not implied by a unique
         * or functional dependency
         */
        init {
            this.columns = ImmutableSortedSet.copyOf(columns)
            this.values = if (values == null) null else ImmutableSortedSet.copyOf(values)
            this.cardinality = cardinality
            this.nullCount = nullCount
            this.expectedCardinality = expectedCardinality
            this.minimal = minimal
        }

        @Override
        override fun toMap(jsonBuilder: JsonBuilder): Object {
            val map: Map<String, Object> = jsonBuilder.map()
            map.put("type", "distribution")
            map.put("columns", FunctionalDependency.getObjects(jsonBuilder, columns))
            if (values != null) {
                val list: List<Object> = jsonBuilder.list()
                for (value in values) {
                    if (value is java.sql.Date) {
                        value = value.toString()
                    }
                    list.add(value)
                }
                map.put("values", list)
            }
            map.put("cardinality", BigDecimal(cardinality, ROUND5))
            if (nullCount > 0) {
                map.put("nullCount", nullCount)
            }
            map.put(
                "expectedCardinality",
                BigDecimal(expectedCardinality, ROUND5)
            )
            map.put("surprise", BigDecimal(surprise(), ROUND3))
            return map
        }

        fun columnOrdinals(): ImmutableBitSet {
            return Column.toOrdinals(columns)
        }

        fun surprise(): Double {
            return SimpleProfiler.surprise(expectedCardinality, cardinality)
        }

        companion object {
            val ROUND5: MathContext = MathContext(5, RoundingMode.HALF_EVEN)
            val ROUND3: MathContext = MathContext(3, RoundingMode.HALF_EVEN)
        }
    }

    /** The result of profiling, contains various statistics about the
     * data in a table.  */
    class Profile internal constructor(
        columns: List<Column?>, val rowCount: RowCount,
        functionalDependencyList: Iterable<FunctionalDependency?>?,
        distributionList: Iterable<Distribution>, uniqueList: Iterable<Unique?>?
    ) {
        val functionalDependencyList: List<FunctionalDependency>
        val distributionList: List<Distribution>
        val uniqueList: List<Unique>
        private val distributionMap: Map<ImmutableBitSet, Distribution>
        private val singletonDistributionList: List<Distribution>

        init {
            this.functionalDependencyList = ImmutableList.copyOf(functionalDependencyList)
            this.distributionList = ImmutableList.copyOf(distributionList)
            this.uniqueList = ImmutableList.copyOf(uniqueList)
            val m: ImmutableMap.Builder<ImmutableBitSet, Distribution> = ImmutableMap.builder()
            for (distribution in distributionList) {
                m.put(distribution.columnOrdinals(), distribution)
            }
            distributionMap = m.build()
            val b: ImmutableList.Builder<Distribution> = ImmutableList.builder()
            for (i in 0 until columns.size()) {
                val key: Int = i
                b.add(
                    requireNonNull(
                        distributionMap[ImmutableBitSet.of(i)]
                    ) { "distributionMap.get(ImmutableBitSet.of(i)) for $key" })
            }
            singletonDistributionList = b.build()
        }

        fun statistics(): List<Statistic> {
            return ImmutableList.< Statistic > builder < org . apache . calcite . profile . Profiler . Statistic ? > ()
                .add(rowCount)
                .addAll(functionalDependencyList)
                .addAll(distributionList)
                .addAll(uniqueList)
                .build()
        }

        fun cardinality(columnOrdinals: ImmutableBitSet): Double {
            var columnOrdinals: ImmutableBitSet = columnOrdinals
            val originalOrdinals: ImmutableBitSet = columnOrdinals
            while (true) {
                val distribution = distributionMap[columnOrdinals]
                if (distribution != null) {
                    return if (columnOrdinals === originalOrdinals) {
                        distribution.cardinality
                    } else {
                        val cardinalityList: List<Double> = ArrayList()
                        cardinalityList.add(distribution.cardinality)
                        for (ordinal in originalOrdinals.except(columnOrdinals)) {
                            val d = singletonDistributionList[ordinal]
                            cardinalityList.add(d.cardinality)
                        }
                        Lattice.getRowCount(rowCount.rowCount, cardinalityList)
                    }
                }
                // Clear the last bit and iterate.
                // Better would be to combine all of our nearest ancestors.
                val list: List<Integer> = columnOrdinals.asList()
                columnOrdinals = columnOrdinals.clear(Util.last(list))
            }
        }
    }
}
