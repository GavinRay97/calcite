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

import org.apache.calcite.config.CalciteSystemProperty
import org.apache.calcite.linq4j.Ord
import org.apache.calcite.linq4j.tree.Primitive
import org.apache.calcite.materialize.Lattice
import org.apache.calcite.rel.metadata.NullSentinel
import org.apache.calcite.runtime.FlatLists
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.util.Pair
import org.apache.calcite.util.PartiallyOrderedSet
import org.apache.calcite.util.Util
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSortedSet
import com.google.common.collect.Iterables
import com.google.common.collect.Ordering
import com.yahoo.sketches.hll.HllSketch
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.ArrayDeque
import java.util.ArrayList
import java.util.Arrays
import java.util.BitSet
import java.util.Collection
import java.util.Collections
import java.util.Deque
import java.util.HashMap
import java.util.HashSet
import java.util.List
import java.util.Map
import java.util.NavigableSet
import java.util.PriorityQueue
import java.util.Queue
import java.util.Set
import java.util.SortedSet
import java.util.TreeSet
import java.util.function.Predicate
import org.apache.calcite.linq4j.Nullness.castNonNull
import org.apache.calcite.profile.ProfilerImpl.CompositeCollector.OF
import java.util.Objects.requireNonNull

/**
 * Implementation of [Profiler] that only investigates "interesting"
 * combinations of columns.
 */
class ProfilerImpl internal constructor(
    combinationsPerPass: Int,
    interestingCount: Int, predicate: Predicate<Pair<Space?, Column?>?>
) : Profiler {
    /** The number of combinations to consider per pass.
     * The number is determined by memory, but a value of 1,000 is typical.
     * You need 2KB memory per sketch, and one sketch for each combination.  */
    private val combinationsPerPass: Int

    /** The minimum number of combinations considered "interesting". After that,
     * a combination is only considered "interesting" if its surprise is greater
     * than the median surprise.  */
    private val interestingCount: Int

    /** Whether a successor is considered interesting enough to analyze.  */
    private val predicate: Predicate<Pair<Space, Column>>

    /**
     * Creates a `ProfilerImpl`.
     *
     * @param combinationsPerPass Maximum number of columns (or combinations of
     * columns) to compute each pass
     * @param interestingCount Minimum number of combinations considered
     * interesting
     * @param predicate Whether a successor is considered interesting enough to
     * analyze
     */
    init {
        Preconditions.checkArgument(combinationsPerPass > 2)
        Preconditions.checkArgument(interestingCount > 2)
        this.combinationsPerPass = combinationsPerPass
        this.interestingCount = interestingCount
        this.predicate = predicate
    }

    @Override
    override fun profile(
        rows: Iterable<List<Comparable?>?>?,
        columns: List<Column?>?, initialGroups: Collection<ImmutableBitSet?>?
    ): Profile {
        return Run(columns, initialGroups).profile(rows)
    }

    /** A run of the profiler.  */
    internal inner class Run(columns: List<Column?>, initialGroups: Collection<ImmutableBitSet?>) {
        private val columns: List<Column>
        val keyPoset: PartiallyOrderedSet<ImmutableBitSet> = PartiallyOrderedSet(
            PartiallyOrderedSet.BIT_SET_INCLUSION_ORDERING
        )
        val distributions: Map<ImmutableBitSet, Distribution> = HashMap()

        /** List of spaces that have one column.  */
        val singletonSpaces: List<Space>

        /** Combinations of columns that we have computed but whose successors have
         * not yet been computed. We may add some of those successors to
         * [.spaceQueue].  */
        val doneQueue: Queue<Space> = PriorityQueue(100) { s0, s1 ->
            // The space with 0 columns is more interesting than
            // any space with 1 column, and so forth.
            // For spaces with 2 or more columns we compare "surprise":
            // how many fewer values did it have than expected?
            var c: Int = Integer.compare(s0.columns.size(), s1.columns.size())
            if (c == 0) {
                c = Double.compare(s0.surprise(), s1.surprise())
            }
            c
        }
        val surprises: SurpriseQueue

        /** Combinations of columns that we will compute next pass.  */
        val spaceQueue: Deque<ImmutableBitSet> = ArrayDeque()
        val uniques: List<Unique> = ArrayList()
        val functionalDependencies: List<FunctionalDependency> = ArrayList()

        /** Column ordinals that have ever been placed on [.spaceQueue].
         * Ensures that we do not calculate the same combination more than once,
         * even though we generate a column set from multiple parents.  */
        val resultSet: Set<ImmutableBitSet> = HashSet()
        val results: PartiallyOrderedSet<Space> =
            PartiallyOrderedSet { e1, e2 -> e2.columnOrdinals.contains(e1.columnOrdinals) }
        private val keyOrdinalLists: List<ImmutableBitSet> = ArrayList()
        private var rowCount = 0

        /**
         * Creates a Run.
         *
         * @param columns List of columns
         *
         * @param initialGroups List of combinations of columns that should be
         * profiled early, because they may be interesting
         */
        init {
            this.columns = ImmutableList.copyOf(columns)
            for (column in Ord.zip(columns)) {
                if (column.e.ordinal !== column.i) {
                    throw IllegalArgumentException()
                }
            }
            singletonSpaces = ArrayList(Collections.nCopies(columns.size(), null as Space?))
            if (combinationsPerPass > Math.pow(2.0, columns.size())) {
                // There are not many columns. We can compute all combinations in the
                // first pass.
                for (ordinals in ImmutableBitSet.range(columns.size()).powerSet()) {
                    spaceQueue.add(ordinals)
                }
            } else {
                // We will need to take multiple passes.
                // Pass 0, just put the empty combination on the queue.
                // Next pass, we will do its successors, the singleton combinations.
                spaceQueue.add(ImmutableBitSet.of())
                spaceQueue.addAll(initialGroups)
                if (columns.size() < combinationsPerPass) {
                    // There are not very many columns. Compute the singleton
                    // groups in pass 0.
                    for (column in columns) {
                        spaceQueue.add(ImmutableBitSet.of(column.ordinal))
                    }
                }
            }
            // The surprise queue must have enough room for all singleton groups
            // plus all initial groups.
            surprises = SurpriseQueue(
                1 + columns.size() + initialGroups.size(),
                interestingCount
            )
        }

        fun profile(rows: Iterable<List<Comparable?>?>): Profile {
            var pass = 0
            while (true) {
                val spaces = nextBatch(pass)
                if (spaces.isEmpty()) {
                    break
                }
                pass(pass++, spaces, rows)
            }
            for (s in singletonSpaces) {
                for (dependent in requireNonNull(s, "s").dependents) {
                    functionalDependencies.add(
                        FunctionalDependency(
                            toColumns(dependent),
                            Iterables.getOnlyElement(s.columns)
                        )
                    )
                }
            }
            return Profile(
                columns, RowCount(rowCount),
                functionalDependencies, distributions.values(), uniques
            )
        }

        /** Populates `spaces` with the next batch.
         * Returns an empty list if done.  */
        fun nextBatch(pass: Int): List<Space> {
            val spaces: List<Space> = ArrayList()
            loop@ while (true) {
                if (spaces.size() >= combinationsPerPass) {
                    // We have enough for the next pass.
                    return spaces
                }
                // First, see if there is a space we did have room for last pass.
                val ordinals: ImmutableBitSet = spaceQueue.poll()
                if (ordinals != null) {
                    val space = Space(this, ordinals, toColumns(ordinals))
                    spaces.add(space)
                    if (ordinals.cardinality() === 1) {
                        singletonSpaces.set(ordinals.nth(0), space)
                    }
                } else {
                    // Next, take a space that was done last time, generate its
                    // successors, and add the interesting ones to the space queue.
                    while (true) {
                        val doneSpace: Space = doneQueue.poll()
                            ?: // There are no more done spaces. We're done.
                            return spaces
                        if (doneSpace.columnOrdinals.cardinality() > 4) {
                            // Do not generate successors for groups with lots of columns,
                            // probably initial groups
                            continue
                        }
                        for (column in columns) {
                            if (!doneSpace.columnOrdinals.get(column.ordinal)) {
                                if (pass == 0 || doneSpace.columnOrdinals.cardinality() === 0 || (!containsKey(
                                        doneSpace.columnOrdinals.set(column.ordinal)
                                    )
                                            && predicate.test(Pair.of(doneSpace, column)))
                                ) {
                                    val nextOrdinals: ImmutableBitSet = doneSpace.columnOrdinals.set(column.ordinal)
                                    if (resultSet.add(nextOrdinals)) {
                                        spaceQueue.add(nextOrdinals)
                                    }
                                }
                            }
                        }
                        // We've converted at a space into at least one interesting
                        // successor.
                        if (!spaceQueue.isEmpty()) {
                            continue@loop
                        }
                    }
                }
            }
        }

        private fun containsKey(ordinals: ImmutableBitSet): Boolean {
            for (keyOrdinals in keyOrdinalLists) {
                if (ordinals.contains(keyOrdinals)) {
                    return true
                }
            }
            return false
        }

        fun pass(pass: Int, spaces: List<Space>, rows: Iterable<List<Comparable?>?>) {
            if (CalciteSystemProperty.DEBUG.value()) {
                System.out.println(
                    "pass: " + pass
                            + ", spaces.size: " + spaces.size()
                            + ", distributions.size: " + distributions.size()
                )
            }
            for (space in spaces) {
                space.collector = Collector.create(space, 1000)
            }
            var rowCount = 0
            for (row in rows) {
                ++rowCount
                for (space in spaces) {
                    castNonNull(space.collector).add(row)
                }
            }

            // Populate unique keys.
            // If [x, y] is a key,
            // then [x, y, z] is a non-minimal key (therefore not interesting),
            // and [x, y] => [a] is a functional dependency but not interesting,
            // and [x, y, z] is not an interesting distribution.
            for (space in spaces) {
                val collector = space.collector
                collector?.finish()
                space.collector = null
                //        results.add(space);
                var nonMinimal = 0
                dependents@ for (s in results.getDescendants(space)) {
                    if (s.cardinality == space.cardinality) {
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
                                singletonSpaces[d], "singletonSpaces.get(d)"
                            )
                            spaceD.dependents.add(s.columnOrdinals)
                        }
                    }
                }
                if (nonMinimal > 0) {
                    continue
                }
                val s: String = space.columns.toString() // for debug
                Util.discard(s)
                val expectedCardinality = expectedCardinality(rowCount.toDouble(), space.columnOrdinals)
                val minimal = (nonMinimal == 0 && !space.unique
                        && !containsKey(space.columnOrdinals))
                space.expectedCardinality = expectedCardinality
                if (minimal) {
                    val distribution = Distribution(
                        space.columns, space.valueSet, space.cardinality,
                        space.nullCount, expectedCardinality, minimal
                    )
                    val surprise: Double = distribution.surprise()
                    if (CalciteSystemProperty.DEBUG.value() && surprise > 0.1) {
                        System.out.println(
                            distribution.columnOrdinals()
                                    + " " + distribution.columns
                                    + ", cardinality: " + distribution.cardinality
                                    + ", expected: " + distribution.expectedCardinality
                                    + ", surprise: " + distribution.surprise()
                        )
                    }
                    if (surprises.offer(surprise)) {
                        distributions.put(space.columnOrdinals, distribution)
                        keyPoset.add(space.columnOrdinals)
                        doneQueue.add(space)
                    }
                }
                if (space.cardinality == rowCount) {
                    // We have discovered a new key. It is not a super-set of a key.
                    uniques.add(Unique(space.columns))
                    keyOrdinalLists.add(space.columnOrdinals)
                    space.unique = true
                }
            }
            if (pass == 0) {
                this.rowCount = rowCount
            }
        }

        /** Estimates the cardinality of a collection of columns represented by
         * `columnOrdinals`, drawing on existing distributions.  */
        private fun cardinality(rowCount: Double, columns: ImmutableBitSet): Double {
            val distribution: Distribution? = distributions[columns]
            return if (distribution != null) {
                distribution.cardinality
            } else {
                expectedCardinality(rowCount, columns)
            }
        }

        /** Estimates the cardinality of a collection of columns represented by
         * `columnOrdinals`, drawing on existing distributions. Does not
         * look in the distribution map for this column set.  */
        private fun expectedCardinality(
            rowCount: Double,
            columns: ImmutableBitSet
        ): Double {
            return when (columns.cardinality()) {
                0 -> 1.0
                1 -> rowCount
                else -> {
                    var c = rowCount
                    val parents: List<ImmutableBitSet> = requireNonNull(
                        keyPoset.getParents(columns, true)
                    ) { "keyPoset.getParents(columns, true) is null for $columns" }
                    for (bitSet in parents) {
                        if (bitSet.isEmpty()) {
                            // If the parent is the empty group (i.e. "GROUP BY ()", the grand
                            // total) we cannot improve on the estimate.
                            continue
                        }
                        val d1: Distribution? = distributions[bitSet]
                        val c2 = cardinality(rowCount, columns.except(bitSet))
                        val d: Double = Lattice.getRowCount(rowCount, requireNonNull(d1, "d1").cardinality, c2)
                        c = Math.min(c, d)
                    }
                    val children: List<ImmutableBitSet> = requireNonNull(
                        keyPoset.getChildren(columns, true)
                    ) { "keyPoset.getChildren(columns, true) is null for $columns" }
                    for (bitSet in children) {
                        val d1: Distribution? = distributions[bitSet]
                        c = Math.min(c, requireNonNull(d1, "d1").cardinality)
                    }
                    c
                }
            }
        }

        private fun toColumns(ordinals: Iterable<Integer>): ImmutableSortedSet<Column> {
            return ImmutableSortedSet.copyOf(
                Util.transform(ordinals) { idx -> columns[idx] })
        }
    }

    /** Work space for a particular combination of columns.  */
    class Space(private val run: Run, columnOrdinals: ImmutableBitSet, columns: Iterable<Column?>?) {
        val columnOrdinals: ImmutableBitSet
        val columns: ImmutableSortedSet<Column>
        var unique = false
        val dependencies: BitSet = BitSet()
        val dependents: Set<ImmutableBitSet> = HashSet()
        var expectedCardinality = 0.0

        @Nullable
        var collector: Collector? = null

        /** Assigned by [Collector.finish].  */
        var nullCount = 0

        /** Number of distinct values. Null is counted as a value, if present.
         * Assigned by [Collector.finish].  */
        var cardinality = 0

        /** Assigned by [Collector.finish].  */
        @Nullable
        var valueSet: SortedSet<Comparable>? = null

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

        /** Returns the distribution created from this space, or null if no
         * distribution has been registered yet.  */
        @Nullable
        fun distribution(): Distribution? {
            return run.distributions[columnOrdinals]
        }

        fun surprise(): Double {
            return SimpleProfiler.surprise(expectedCardinality, cardinality.toDouble())
        }
    }

    /** Builds a [org.apache.calcite.profile.ProfilerImpl].  */
    class Builder {
        var combinationsPerPass = 100
        var predicate: Predicate<Pair<Space?, Column?>?> = Predicate<Pair<Space, Column>> { p -> true }
        fun build(): ProfilerImpl {
            return ProfilerImpl(combinationsPerPass, 200, predicate)
        }

        fun withPassSize(passSize: Int): Builder {
            combinationsPerPass = passSize
            return this
        }

        fun withMinimumSurprise(v: Double): Builder {
            predicate = Predicate<Pair<Space, Column>> { spaceColumnPair ->
                @SuppressWarnings("unused") val space: Space = spaceColumnPair.left
                false
            }
            return this
        }
    }

    /** Collects values of a column or columns.  */
    internal abstract class Collector(protected val space: Space) {
        abstract fun add(row: List<Comparable?>?)
        abstract fun finish()

        companion object {
            /** Creates an initial collector of the appropriate kind.  */
            fun create(space: Space, sketchThreshold: Int): Collector {
                val columnOrdinalList: List<Integer> = space.columnOrdinals.asList()
                return if (columnOrdinalList.size() === 1) {
                    SingletonCollector(
                        space, columnOrdinalList[0],
                        sketchThreshold
                    )
                } else {
                    CompositeCollector(
                        space,
                        Primitive.INT.toArray(columnOrdinalList) as IntArray, sketchThreshold
                    )
                }
            }
        }
    }

    /** Collector that collects values of a single column.  */
    internal class SingletonCollector(space: Space, val columnOrdinal: Int, val sketchThreshold: Int) :
        Collector(space) {
        val values: NavigableSet<Comparable> = TreeSet()
        var nullCount = 0
        @Override
        override fun add(row: List<Comparable>) {
            val v: Comparable = row[columnOrdinal]
            if (v === NullSentinel.INSTANCE) {
                nullCount++
            } else {
                if (values.add(v) && values.size() === sketchThreshold) {
                    // Too many values. Switch to a sketch collector.
                    val collector = HllSingletonCollector(
                        space, columnOrdinal
                    )
                    for (value in values) {
                        collector.add(value)
                    }
                    space.collector = collector
                }
            }
        }

        @Override
        override fun finish() {
            space.nullCount = nullCount
            space.cardinality = values.size() + if (nullCount > 0) 1 else 0
            space.valueSet = if (values.size() < 20) values else null
        }
    }

    /** Collector that collects two or more column values in a tree set.  */
    internal class CompositeCollector(space: Space, val columnOrdinals: IntArray, sketchThreshold: Int) :
        Collector(space) {
        val values: Set<FlatLists.ComparableList> = HashSet()
        val columnValues: Array<Comparable?>
        var nullCount = 0
        private val sketchThreshold: Int

        init {
            columnValues = arrayOfNulls<Comparable>(columnOrdinals.size)
            this.sketchThreshold = sketchThreshold
        }

        @Override
        override fun add(row: List<Comparable>) {
            if (space.columnOrdinals.equals(OF)) {
                Util.discard(0)
            }
            var nullCountThisRow = 0
            var i = 0
            val length = columnOrdinals.size
            while (i < length) {
                val value: Comparable = row[columnOrdinals[i]]
                if (value === NullSentinel.INSTANCE) {
                    if (nullCountThisRow++ == 0) {
                        nullCount++
                    }
                }
                columnValues[i] = value
                i++
            }
            if (values.add(FlatLists.copyOf(columnValues))
                && values.size() === sketchThreshold
            ) {
                // Too many values. Switch to a sketch collector.
                val collector = HllCompositeCollector(
                    space, columnOrdinals
                )
                val list: List<Comparable> = ArrayList(
                    Collections.nCopies(
                        columnOrdinals[columnOrdinals.size - 1]
                                + 1,
                        null
                    )
                )
                for (value in values) {
                    for (i in 0 until value.size()) {
                        list.set(columnOrdinals[i], value.get(i))
                    }
                    collector.add(list)
                }
                space.collector = collector
            }
        }

        @Override
        override fun finish() {
            // number of input rows (not distinct values)
            // that were null or partially null
            space.nullCount = nullCount
            space.cardinality = values.size() + if (nullCount > 0) 1 else 0
            space.valueSet = null
        }

        companion object {
            protected val OF: ImmutableBitSet = ImmutableBitSet.of(2, 13)
        }
    }

    /** Collector that collects two or more column values into a HyperLogLog
     * sketch.  */
    internal abstract class HllCollector(space: Space) : Collector(space) {
        val sketch: HllSketch
        var nullCount = 0

        init {
            sketch = HllSketch.builder().build()
        }

        override fun add(value: Comparable) {
            if (value === NullSentinel.INSTANCE) {
                sketch.update(NULL_BITS)
            } else if (value is String) {
                sketch.update(value as String)
            } else if (value is Double) {
                sketch.update(value as Double)
            } else if (value is Float) {
                sketch.update(value as Float)
            } else if (value is Long) {
                sketch.update(value as Long)
            } else if (value is Number) {
                sketch.update((value as Number).longValue())
            } else {
                sketch.update(value.toString())
            }
        }

        @Override
        override fun finish() {
            space.nullCount = nullCount
            space.cardinality = sketch.getEstimate()
            space.valueSet = null
        }

        companion object {
            val NULL_BITS = longArrayOf(-0x60882a816ce985eaL)
        }
    }

    /** Collector that collects one column value into a HyperLogLog sketch.  */
    internal class HllSingletonCollector(space: Space, val columnOrdinal: Int) : HllCollector(space) {
        @Override
        override fun add(row: List<Comparable>) {
            val value: Comparable = row[columnOrdinal]
            if (value === NullSentinel.INSTANCE) {
                nullCount++
                sketch.update(NULL_BITS)
            } else {
                add(value)
            }
        }
    }

    /** Collector that collects two or more column values into a HyperLogLog
     * sketch.  */
    internal class HllCompositeCollector(space: Space, private val columnOrdinals: IntArray) : HllCollector(space) {
        private val buf: ByteBuffer = ByteBuffer.allocate(1024)
        @Override
        override fun add(row: List<Comparable>) {
            if (space.columnOrdinals.equals(OF)) {
                Util.discard(0)
            }
            var nullCountThisRow = 0
            buf.clear()
            for (columnOrdinal in columnOrdinals) {
                val value: Comparable = row[columnOrdinal]
                if (value === NullSentinel.INSTANCE) {
                    if (nullCountThisRow++ == 0) {
                        nullCount++
                    }
                    buf.put(0.toByte())
                } else if (value is String) {
                    buf.put(1.toByte())
                        .put((value as String).getBytes(StandardCharsets.UTF_8))
                } else if (value is Double) {
                    buf.put(2.toByte()).putDouble(value as Double)
                } else if (value is Float) {
                    buf.put(3.toByte()).putFloat(value as Float)
                } else if (value is Long) {
                    buf.put(4.toByte()).putLong(value as Long)
                } else if (value is Integer) {
                    buf.put(5.toByte()).putInt(value as Integer)
                } else if (value is Boolean) {
                    buf.put(if (value) 6.toByte() else 7.toByte())
                } else {
                    buf.put(8.toByte())
                        .put(value.toString().getBytes(StandardCharsets.UTF_8))
                }
            }
            sketch.update(Arrays.copyOf(buf.array(), buf.position()))
        }
    }

    /** A priority queue of the last N surprise values. Accepts a new value if
     * the queue is not yet full, or if its value is greater than the median value
     * over the last N.  */
    internal class SurpriseQueue(private val warmUpCount: Int, private val size: Int) {
        var count = 0
        val deque: Deque<Double> = ArrayDeque()
        val priorityQueue: PriorityQueue<Double> = PriorityQueue(11, Ordering.natural())

        init {
            Preconditions.checkArgument(warmUpCount > 3)
            Preconditions.checkArgument(size > 0)
        }

        @Override
        override fun toString(): String {
            return "min: " + priorityQueue.peek()
                .toString() + ", contents: " + deque.toString()
        }

        val isValid: Boolean
            get() {
                if (CalciteSystemProperty.DEBUG.value()) {
                    System.out.println(toString())
                }
                assert(deque.size() === priorityQueue.size())
                if (count > size) {
                    assert(deque.size() === size)
                }
                return true
            }

        fun offer(d: Double): Boolean {
            val b: Boolean
            b = if (count++ < warmUpCount || d > castNonNull(priorityQueue.peek())) {
                if (priorityQueue.size() >= size) {
                    priorityQueue.remove(deque.pop())
                }
                priorityQueue.add(d)
                deque.add(d)
                true
            } else {
                false
            }
            if (CalciteSystemProperty.DEBUG.value()) {
                System.out.println(
                    "offer " + d
                            + " min " + priorityQueue.peek()
                            + " accepted " + b
                )
            }
            return b
        }
    }

    companion object {
        fun builder(): Builder {
            return Builder()
        }
    }
}
