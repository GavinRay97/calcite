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

import com.google.common.collect.ImmutableSortedMap

/**
 * Utility functions for [BitSet].
 */
class BitSets private constructor() {
    init {
        throw AssertionError("no instances!")
    }

    /**
     * Setup equivalence Sets for each position. If i and j are equivalent then
     * they will have the same equivalence Set. The algorithm computes the
     * closure relation at each position for the position wrt to positions
     * greater than it. Once a closure is computed for a position, the closure
     * Set is set on all its descendants. So the closure computation bubbles up
     * from lower positions and the final equivalence Set is propagated down
     * from the lowest element in the Set.
     */
    private class Closure @SuppressWarnings(["JdkObsolete", "method.invocation.invalid"]) internal constructor(
        equivalence: SortedMap<Integer?, BitSet?>
    ) {
        private val equivalence: SortedMap<Integer, BitSet>
        val closure: NavigableMap<Integer, BitSet> = TreeMap()

        init {
            this.equivalence = equivalence
            val keys: ImmutableIntList = ImmutableIntList.copyOf(equivalence.keySet())
            for (pos in keys) {
                computeClosure(pos)
            }
        }

        @SuppressWarnings("JdkObsolete")
        private fun computeClosure(pos: Int): BitSet? {
            var o: BitSet = closure.get(pos)
            if (o != null) {
                return o
            }
            val b: BitSet = requireNonNull(
                equivalence.get(pos)
            ) { "equivalence.get(pos) for $pos" }
            o = b.clone() as BitSet
            var i: Int = b.nextSetBit(pos + 1)
            while (i >= 0) {
                o.or(computeClosure(i))
                i = b.nextSetBit(i + 1)
            }
            closure.put(pos, o)
            i = o.nextSetBit(pos + 1)
            while (i >= 0) {
                closure.put(i, o)
                i = b.nextSetBit(i + 1)
            }
            return o
        }
    }

    companion object {
        /**
         * Returns true if all bits set in the second parameter are also set in the
         * first. In other words, whether x is a super-set of y.
         *
         * @param set0 Containing bitmap
         * @param set1 Bitmap to be checked
         *
         * @return Whether all bits in set1 are set in set0
         */
        fun contains(set0: BitSet, set1: BitSet): Boolean {
            var i: Int = set1.nextSetBit(0)
            while (i >= 0) {
                if (!set0.get(i)) {
                    return false
                }
                i = set1.nextSetBit(i + 1)
            }
            return true
        }

        /**
         * Returns true if all bits set in the second parameter are also set in the
         * first. In other words, whether x is a super-set of y.
         *
         * @param set0 Containing bitmap
         * @param set1 Bitmap to be checked
         *
         * @return Whether all bits in set1 are set in set0
         */
        fun contains(set0: BitSet, set1: ImmutableBitSet): Boolean {
            var i: Int = set1.nextSetBit(0)
            while (i >= 0) {
                if (!set0.get(i)) {
                    return false
                }
                i = set1.nextSetBit(i + 1)
            }
            return true
        }

        /**
         * Returns an iterable over the bits in a bitmap that are set to '1'.
         *
         *
         * This allows you to iterate over a bit set using a 'foreach' construct.
         * For instance:
         *
         * <blockquote>`
         * BitSet bitSet;<br></br>
         * for (int i : Util.toIter(bitSet)) {<br></br>
         * &nbsp;&nbsp;print(i);<br></br>
         * }`</blockquote>
         *
         * @param bitSet Bit set
         * @return Iterable
         */
        fun toIter(bitSet: BitSet): Iterable<Integer> {
            return label@ Iterable<Integer> {
                object : Iterator<Integer?>() {
                    var i: Int = bitSet.nextSetBit(0)

                    @Override
                    override fun hasNext(): Boolean {
                        return@label i >= 0
                    }

                    @Override
                    override fun next(): Integer {
                        val prev = i
                        i = bitSet.nextSetBit(i + 1)
                        return@label prev
                    }

                    @Override
                    fun remove() {
                        throw UnsupportedOperationException()
                    }
                }
            }
        }

        fun toIter(bitSet: ImmutableBitSet): Iterable<Integer> {
            return bitSet
        }

        /**
         * Converts a bitset to a list.
         *
         *
         * The list is mutable, and future changes to the list do not affect the
         * contents of the bit set.
         *
         * @param bitSet Bit set
         * @return List of set bits
         */
        fun toList(bitSet: BitSet): List<Integer> {
            val list: List<Integer> = ArrayList()
            var i: Int = bitSet.nextSetBit(0)
            while (i >= 0) {
                list.add(i)
                i = bitSet.nextSetBit(i + 1)
            }
            return list
        }

        /**
         * Converts a BitSet to an array.
         *
         * @param bitSet Bit set
         * @return Array of set bits
         */
        fun toArray(bitSet: BitSet): IntArray {
            val integers = IntArray(bitSet.cardinality())
            var j = 0
            var i: Int = bitSet.nextSetBit(0)
            while (i >= 0) {
                integers[j++] = i
                i = bitSet.nextSetBit(i + 1)
            }
            return integers
        }

        /**
         * Creates a bitset with given bits set.
         *
         *
         * For example, `of(0, 3)` returns a bit set with bits {0, 3}
         * set.
         *
         * @param bits Array of bits to set
         * @return Bit set
         */
        fun of(vararg bits: Int): BitSet {
            val bitSet = BitSet()
            for (bit in bits) {
                bitSet.set(bit)
            }
            return bitSet
        }

        /**
         * Creates a BitSet with given bits set.
         *
         *
         * For example, `of(new Integer[] {0, 3})` returns a bit set
         * with bits {0, 3} set.
         *
         * @param bits Array of bits to set
         * @return Bit set
         */
        fun of(bits: Array<Integer?>): BitSet {
            val bitSet = BitSet()
            for (bit in bits) {
                bitSet.set(bit)
            }
            return bitSet
        }

        /**
         * Creates a BitSet with given bits set.
         *
         *
         * For example, `of(Arrays.asList(0, 3)) ` returns a bit set
         * with bits {0, 3} set.
         *
         * @param bits Collection of bits to set
         * @return Bit set
         */
        fun of(bits: Iterable<Number?>): BitSet {
            val bitSet = BitSet()
            for (bit in bits) {
                bitSet.set(bit.intValue())
            }
            return bitSet
        }

        /**
         * Creates a BitSet with given bits set.
         *
         *
         * For example, `of(ImmutableIntList.of(0, 3))` returns a bit set
         * with bits {0, 3} set.
         *
         * @param bits Collection of bits to set
         * @return Bit set
         */
        fun of(bits: ImmutableIntList): BitSet {
            val bitSet = BitSet()
            var i = 0
            val n: Int = bits.size()
            while (i < n) {
                bitSet.set(bits.getInt(i))
                i++
            }
            return bitSet
        }

        /**
         * Creates a bitset with bits from `fromIndex` (inclusive) to
         * specified `toIndex` (exclusive) set to `true`.
         *
         *
         * For example, `range(0, 3)` returns a bit set with bits
         * {0, 1, 2} set.
         *
         * @param fromIndex Index of the first bit to be set.
         * @param toIndex   Index after the last bit to be set.
         * @return Bit set
         */
        fun range(fromIndex: Int, toIndex: Int): BitSet {
            val bitSet = BitSet()
            if (toIndex > fromIndex) {
                // Avoid http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6222207
                // "BitSet internal invariants may be violated"
                bitSet.set(fromIndex, toIndex)
            }
            return bitSet
        }

        /** Creates a BitSet with bits between 0 and `toIndex` set.  */
        fun range(toIndex: Int): BitSet {
            return range(0, toIndex)
        }

        /** Sets all bits in a given BitSet corresponding to integers from a list.  */
        fun setAll(bitSet: BitSet, list: Iterable<Number?>) {
            for (number in list) {
                bitSet.set(number.intValue())
            }
        }

        /** Returns a BitSet that is the union of the given BitSets. Does not modify
         * any of the inputs.  */
        fun union(set0: BitSet, vararg sets: BitSet?): BitSet {
            val s: BitSet = set0.clone() as BitSet
            for (set in sets) {
                s.or(set)
            }
            return s
        }

        /** Returns the previous clear bit.
         *
         *
         * Has same behavior as [BitSet.previousClearBit], but that method
         * does not exist before 1.7.  */
        fun previousClearBit(bitSet: BitSet, fromIndex: Int): Int {
            var fromIndex = fromIndex
            if (fromIndex < -1) {
                throw IndexOutOfBoundsException()
            }
            while (fromIndex >= 0) {
                if (!bitSet.get(fromIndex)) {
                    return fromIndex
                }
                --fromIndex
            }
            return -1
        }

        /** Computes the closure of a map from integers to bits.
         *
         *
         * The input must have an entry for each position.
         *
         *
         * Does not modify the input map or its bit sets.  */
        @SuppressWarnings("JdkObsolete")
        fun closure(
            equivalence: SortedMap<Integer?, BitSet?>
        ): SortedMap<Integer, BitSet> {
            var equivalence: SortedMap<Integer?, BitSet?> = equivalence
            if (equivalence.isEmpty()) {
                return ImmutableSortedMap.of()
            }
            var length: Int = equivalence.lastKey()
            for (bitSet in equivalence.values()) {
                length = Math.max(length, bitSet.length())
            }
            if (equivalence.size() < length
                || equivalence.firstKey() !== 0
            ) {
                val old: SortedMap<Integer?, BitSet?> = equivalence
                equivalence = TreeMap()
                for (i in 0 until length) {
                    val bitSet: BitSet = old.get(i)
                    equivalence.put(i, if (bitSet == null) BitSet() else bitSet)
                }
            }
            val closure = Closure(equivalence)
            return closure.closure
        }

        /** Populates a [BitSet] from an iterable, such as a list of integer.  */
        fun populate(bitSet: BitSet, list: Iterable<Number?>) {
            for (number in list) {
                bitSet.set(number.intValue())
            }
        }

        /** Populates a [BitSet] from an
         * [ImmutableIntList].  */
        fun populate(bitSet: BitSet, list: ImmutableIntList) {
            for (i in 0 until list.size()) {
                bitSet.set(list.getInt(i))
            }
        }
    }
}
