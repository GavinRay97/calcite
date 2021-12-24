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

import org.apache.calcite.linq4j.Ord
import org.apache.calcite.materialize.Lattice
import org.apache.calcite.rel.metadata.NullSentinel
import org.apache.calcite.runtime.FlatLists
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.util.PartiallyOrderedSet
import org.apache.calcite.util.Util
import com.google.common.collect.ImmutableSortedSet
import com.google.common.collect.Iterables
import org.checkerframework.checker.initialization.qual.UnknownInitialization
import org.checkerframework.checker.nullness.qual.RequiresNonNull
import java.util.ArrayList
import java.util.BitSet
import java.util.Collection
import java.util.Collections
import java.util.HashMap
import java.util.HashSet
import java.util.List
import java.util.Map
import java.util.NavigableSet
import java.util.Set
import java.util.SortedSet
import java.util.TreeSet
import java.util.Objects.requireNonNull

/**
 * Basic implementation of [Profiler].
 */
class SimpleProfiler : Profiler {
    @Override
    fun profile(
        rows: Iterable<List<Comparable?>?>,
        columns: List<Column?>, initialGroups: Collection<ImmutableBitSet?>?
    ): Profile {
        Util.discard(initialGroups) // this profiler ignores initial groups
        return Run(columns).profile(rows)
    }

    /** A run of the profiler.  */
    internal class Run(columns: List<Column?>) {
        private val columns: List<Column?>
        val spaces: List<Space> = ArrayList()
        val singletonSpaces: List<Space>
        val statistics: List<Statistic> = ArrayList()
        val ordering: PartiallyOrderedSet.Ordering<Space> =
            PartiallyOrderedSet.Ordering<Space> { e1, e2 -> e2.columnOrdinals.contains(e1.columnOrdinals) }
        val results: PartiallyOrderedSet<Space> = PartiallyOrderedSet(ordering)
        val keyResults: PartiallyOrderedSet<Space> = PartiallyOrderedSet(ordering)
        private val keyOrdinalLists: List<ImmutableBitSet> = ArrayList()

        init {
            for (column in Ord.zip(columns)) {
                if (column.e.ordinal !== column.i) {
                    throw IllegalArgumentException()
                }
            }
            this.columns = columns
            singletonSpaces = ArrayList(Collections.nCopies(columns.size(), null))
            for (ordinals in ImmutableBitSet.range(columns.size()).powerSet()) {
                val space = Space(ordinals, toColumns(ordinals))
                spaces.add(space)
                if (ordinals.cardinality() === 1) {
                    singletonSpaces.set(ordinals.nth(0), space)
                }
            }
        }

        fun profile(rows: Iterable<List<Comparable?>?>): Profile {
            val values: List<Comparable> = ArrayList()
            var rowCount = 0
            for (row in rows) {
                ++rowCount
                joint@ for (space in spaces) {
                    values.clear()
                    for (column in space.columns) {
                        val value: Comparable = row[column.ordinal]
                        values.add(value)
                        if (value === NullSentinel.INSTANCE) {
                            space.nullCount++
                            continue@joint
                        }
                    }
                    space.values.add(FlatLists.ofComparable(values))
                }
            }

            // Populate unique keys
            // If [x, y] is a key,
            // then [x, y, z] is a key but not intersecting,
            // and [x, y] => [a] is a functional dependency but not interesting,
            // and [x, y, z] is not an interesting distribution.
            val distributions: Map<ImmutableBitSet, Distribution> = HashMap()
            for (space in spaces) {
                if (space.values.size() === rowCount
                    && !containsKey(space.columnOrdinals, false)
                ) {
                    // We have discovered a new key.
                    // It is not an existing key or a super-set of a key.
                    statistics.add(Unique(space.columns))
                    space.unique = true
                    keyOrdinalLists.add(space.columnOrdinals)
                }
                var nonMinimal = 0
                dependents@ for (s in results.getDescendants(space)) {
                    if (s.cardinality() == space.cardinality()) {
                        // We have discovered a sub-set that has the same cardinality.
                        // The column(s) that are not in common are functionally
                        // dependent.
                        val dependents: ImmutableBitSet = space.columnOrdinals.except(s.columnOrdinals)
                        for (i in s.columnOrdinals) {
                            val s1 = singletonSpaces[i]
                            val rest: ImmutableBitSet = s.columnOrdinals.clear(i)
                            for (dependent in requireNonNull(s1, "s1").dependents) {
                                if (rest.contains(dependent)) {
                                    // The "key" of this functional dependency is not minimal.
                                    // For instance, if we know that
                                    //   (a) -> x
                                    // then
                                    //   (a, b, x) -> y
                                    // is not minimal; we could say the same with a smaller key:
                                    //   (a, b) -> y
                                    ++nonMinimal
                                    continue@dependents
                                }
                            }
                        }
                        for (dependent in dependents) {
                            val s1 = singletonSpaces[dependent]
                            for (d in requireNonNull(s1, "s1").dependents) {
                                if (s.columnOrdinals.contains(d)) {
                                    ++nonMinimal
                                    continue@dependents
                                }
                            }
                        }
                        space.dependencies.or(dependents.toBitSet())
                        for (d in dependents) {
                            val spaceD: Space = requireNonNull(
                                singletonSpaces[d]
                            ) { "singletonSpaces.get(d) is null for $d" }
                            spaceD.dependents.add(s.columnOrdinals)
                        }
                    }
                }
                var nullCount: Int
                val valueSet: SortedSet<Comparable>?
                if (space.columns.size() === 1) {
                    nullCount = space.nullCount
                    valueSet = ImmutableSortedSet.copyOf(
                        Util.transform(space.values, Iterables::getOnlyElement)
                    )
                } else {
                    nullCount = -1
                    valueSet = null
                }
                var expectedCardinality: Double
                val cardinality = space.cardinality()
                when (space.columns.size()) {
                    0 -> expectedCardinality = 1.0
                    1 -> expectedCardinality = rowCount.toDouble()
                    else -> {
                        expectedCardinality = rowCount.toDouble()
                        for (column in space.columns) {
                            val d1: Distribution? = distributions[ImmutableBitSet.of(column.ordinal)]
                            val d2: Distribution? = distributions[space.columnOrdinals.clear(column.ordinal)]
                            val d: Double = Lattice.getRowCount(
                                rowCount,
                                requireNonNull(d1, "d1").cardinality,
                                requireNonNull(d2, "d2").cardinality
                            )
                            expectedCardinality = Math.min(expectedCardinality, d)
                        }
                    }
                }
                val minimal = (nonMinimal == 0 && !space.unique
                        && !containsKey(space.columnOrdinals, true))
                val distribution = Distribution(
                    space.columns, valueSet, cardinality, nullCount,
                    expectedCardinality, minimal
                )
                statistics.add(distribution)
                distributions.put(space.columnOrdinals, distribution)
                if (distribution.minimal) {
                    results.add(space)
                }
            }
            for (s in singletonSpaces) {
                for (dependent in requireNonNull(s, "s").dependents) {
                    if (!containsKey(dependent, false)
                        && !hasNull(dependent)
                    ) {
                        statistics.add(
                            FunctionalDependency(
                                toColumns(dependent),
                                Iterables.getOnlyElement(s.columns)
                            )
                        )
                    }
                }
            }
            return Profile(
                columns, RowCount(rowCount),
                Iterables.filter(statistics, FunctionalDependency::class.java),
                Iterables.filter(statistics, Distribution::class.java),
                Iterables.filter(statistics, Unique::class.java)
            )
        }

        /** Returns whether a set of column ordinals
         * matches or contains a unique key.
         * If `strict`, it must contain a unique key.  */
        private fun containsKey(ordinals: ImmutableBitSet, strict: Boolean): Boolean {
            for (keyOrdinals in keyOrdinalLists) {
                if (ordinals.contains(keyOrdinals)) {
                    return !(strict && keyOrdinals.equals(ordinals))
                }
            }
            return false
        }

        private fun hasNull(columnOrdinals: ImmutableBitSet): Boolean {
            for (columnOrdinal in columnOrdinals) {
                val space: Space = requireNonNull(
                    singletonSpaces[columnOrdinal]
                ) { "singletonSpaces.get(columnOrdinal) is null for $columnOrdinal" }
                if (space.nullCount > 0) {
                    return true
                }
            }
            return false
        }

        @RequiresNonNull("columns")
        private fun toColumns(
            ordinals: Iterable<Integer>
        ): ImmutableSortedSet<Column> {
            return ImmutableSortedSet.copyOf(
                Util.transform(ordinals) { idx -> columns[idx] })
        }
    }

    /** Work space for a particular combination of columns.  */
    internal class Space(columnOrdinals: ImmutableBitSet, columns: Iterable<Column?>?) : Comparable<Space?> {
        val columnOrdinals: ImmutableBitSet
        val columns: ImmutableSortedSet<Column>
        var nullCount = 0
        val values: NavigableSet<FlatLists.ComparableList<Comparable>> = TreeSet()
        var unique = false
        val dependencies: BitSet = BitSet()
        val dependents: Set<ImmutableBitSet> = HashSet()

        init {
            this.columnOrdinals = columnOrdinals
            this.columns = ImmutableSortedSet.copyOf(columns)
        }

        @Override
        override fun hashCode(): Int {
            return columnOrdinals.hashCode()
        }

        @Override
        override fun equals(@Nullable o: Object): Boolean {
            return (o === this
                    || o is Space
                    && columnOrdinals.equals((o as Space).columnOrdinals))
        }

        @Override
        operator fun compareTo(o: Space): Int {
            return if (columnOrdinals.equals(o.columnOrdinals)) 0 else if (columnOrdinals.contains(o.columnOrdinals)) 1 else -1
        }

        /** Number of distinct values. Null is counted as a value, if present.  */
        fun cardinality(): Double {
            return values.size() + if (nullCount > 0) 1 else 0
        }
    }

    companion object {
        /** Returns a measure of how much an actual value differs from expected.
         * The formula is `abs(expected - actual) / (expected + actual)`.
         *
         *
         * Examples:
         *  * surprise(e, a) is always between 0 and 1;
         *  * surprise(e, a) is 0 if e = a;
         *  * surprise(e, 0) is 1 if e &gt; 0;
         *  * surprise(0, a) is 1 if a &gt; 0;
         *  * surprise(5, 0) is 100%;
         *  * surprise(5, 3) is 25%;
         *  * surprise(5, 4) is 11%;
         *  * surprise(5, 5) is 0%;
         *  * surprise(5, 6) is 9%;
         *  * surprise(5, 16) is 52%;
         *  * surprise(5, 100) is 90%;
         *
         *
         * @param expected Expected value
         * @param actual Actual value
         * @return Measure of how much expected deviates from actual
         */
        fun surprise(expected: Double, actual: Double): Double {
            if (expected == actual) {
                return 0.0
            }
            val sum = expected + actual
            return if (sum <= 0.0) {
                1.0
            } else Math.abs(expected - actual) / sum
        }
    }
}
