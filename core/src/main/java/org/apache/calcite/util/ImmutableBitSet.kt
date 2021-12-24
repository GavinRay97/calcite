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

import org.apache.calcite.linq4j.Linq4j

/**
 * An immutable list of bits.
 */
class ImmutableBitSet private constructor(private val words: LongArray?) : Iterable<Integer?>, Serializable,
    Comparable<ImmutableBitSet?> {
    /** Private constructor. Does not copy the array.  */
    init {
        assert(if (words!!.size == 0) words == EMPTY_LONGS else words[words.size - 1] != 0L)
    }

    /** Computes the power set (set of all sets) of this bit set.  */
    fun powerSet(): Iterable<ImmutableBitSet> {
        val singletons: List<List<ImmutableBitSet>> = ArrayList()
        for (bit in this) {
            singletons.add(
                ImmutableList.of(of(), of(bit))
            )
        }
        return Util.transform(
            Linq4j.product(singletons),
            ImmutableBitSet::union
        )
    }

    /**
     * Returns the value of the bit with the specified index. The value
     * is `true` if the bit with the index `bitIndex`
     * is currently set in this `ImmutableBitSet`; otherwise, the result
     * is `false`.
     *
     * @param  bitIndex   the bit index
     * @return the value of the bit with the specified index
     * @throws IndexOutOfBoundsException if the specified index is negative
     */
    operator fun get(bitIndex: Int): Boolean {
        if (bitIndex < 0) {
            throw IndexOutOfBoundsException("bitIndex < 0: $bitIndex")
        }
        val wordIndex = wordIndex(bitIndex)
        return (wordIndex < words!!.size
                && words[wordIndex] and (1L shl bitIndex) != 0L)
    }

    /**
     * Returns a new `ImmutableBitSet`
     * composed of bits from this `ImmutableBitSet`
     * from `fromIndex` (inclusive) to `toIndex` (exclusive).
     *
     * @param  fromIndex index of the first bit to include
     * @param  toIndex index after the last bit to include
     * @return a new `ImmutableBitSet` from a range of
     * this `ImmutableBitSet`
     * @throws IndexOutOfBoundsException if `fromIndex` is negative,
     * or `toIndex` is negative, or `fromIndex` is
     * larger than `toIndex`
     */
    operator fun get(fromIndex: Int, toIndex: Int): ImmutableBitSet {
        checkRange(fromIndex, toIndex)
        val builder = builder()
        var i = nextSetBit(fromIndex)
        while (i >= 0 && i < toIndex) {
            builder.set(i)
            i = nextSetBit(i + 1)
        }
        return builder.build()
    }

    /**
     * Returns a string representation of this bit set. For every index
     * for which this `BitSet` contains a bit in the set
     * state, the decimal representation of that index is included in
     * the result. Such indices are listed in order from lowest to
     * highest, separated by ",&nbsp;" (a comma and a space) and
     * surrounded by braces, resulting in the usual mathematical
     * notation for a set of integers.
     *
     *
     * Example:
     * <pre>
     * BitSet drPepper = new BitSet();</pre>
     * Now `drPepper.toString()` returns "`{}`".
     * <pre>
     * drPepper.set(2);</pre>
     * Now `drPepper.toString()` returns "`{2}`".
     * <pre>
     * drPepper.set(4);
     * drPepper.set(10);</pre>
     * Now `drPepper.toString()` returns "`{2, 4, 10}`".
     *
     * @return a string representation of this bit set
     */
    @Override
    override fun toString(): String {
        val numBits = words!!.size * BITS_PER_WORD
        val b = StringBuilder(6 * numBits + 2)
        b.append('{')
        var i = nextSetBit(0)
        if (i != -1) {
            b.append(i)
            i = nextSetBit(i + 1)
            while (i >= 0) {
                val endOfRun = nextClearBit(i)
                do {
                    b.append(", ").append(i)
                } while (++i < endOfRun)
                i = nextSetBit(i + 1)
            }
        }
        b.append('}')
        return b.toString()
    }

    /**
     * Returns true if the specified `ImmutableBitSet` has any bits set to
     * `true` that are also set to `true` in this
     * `ImmutableBitSet`.
     *
     * @param  set `ImmutableBitSet` to intersect with
     * @return boolean indicating whether this `ImmutableBitSet` intersects
     * the specified `ImmutableBitSet`
     */
    fun intersects(set: ImmutableBitSet): Boolean {
        for (i in Math.min(words!!.size, set.words!!.size) - 1 downTo 0) {
            if (words[i] and set.words[i] != 0L) {
                return true
            }
        }
        return false
    }

    /** Returns the number of bits set to `true` in this
     * `ImmutableBitSet`.
     *
     * @see .size
     */
    fun cardinality(): Int {
        return countBits(words)
    }

    /**
     * Returns the hash code value for this bit set. The hash code
     * depends only on which bits are set within this `ImmutableBitSet`.
     *
     *
     * The hash code is defined using the same calculation as
     * [java.util.BitSet.hashCode].
     *
     * @return the hash code value for this bit set
     */
    @Override
    override fun hashCode(): Int {
        var h: Long = 1234
        var i = words!!.size
        while (--i >= 0) {
            h = h xor words[i] * (i + 1)
        }
        return (h shr 32 xor h).toInt()
    }

    /**
     * Returns the number of bits of space actually in use by this
     * `ImmutableBitSet` to represent bit values.
     * The maximum element in the set is the size - 1st element.
     *
     * @return the number of bits currently in this bit set
     *
     * @see .cardinality
     */
    fun size(): Int {
        return words!!.size * BITS_PER_WORD
    }

    /**
     * Compares this object against the specified object.
     * The result is `true` if and only if the argument is
     * not `null` and is a `ImmutableBitSet` object that has
     * exactly the same set of bits set to `true` as this bit
     * set.
     *
     * @param  obj the object to compare with
     * @return `true` if the objects are the same;
     * `false` otherwise
     * @see .size
     */
    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        if (this === obj) {
            return true
        }
        if (obj !is ImmutableBitSet) {
            return false
        }
        val set = obj as ImmutableBitSet
        return Arrays.equals(words, set.words)
    }

    /** Compares this ImmutableBitSet with another, using a lexicographic
     * ordering.
     *
     *
     * Bit sets `(), (0), (0, 1), (0, 1, 3), (1), (2, 3)` are in sorted
     * order.
     */
    @Override
    operator fun compareTo(o: ImmutableBitSet): Int {
        var i = 0
        while (true) {
            val n0 = nextSetBit(i)
            val n1 = o.nextSetBit(i)
            val c: Int = Utilities.compare(n0, n1)
            if (c != 0 || n0 < 0) {
                return c
            }
            i = n0 + 1
        }
    }

    /**
     * Returns the index of the first bit that is set to `true`
     * that occurs on or after the specified starting index. If no such
     * bit exists then `-1` is returned.
     *
     *
     * Based upon [BitSet.nextSetBit].
     *
     * @param  fromIndex the index to start checking from (inclusive)
     * @return the index of the next set bit, or `-1` if there
     * is no such bit
     * @throws IndexOutOfBoundsException if the specified index is negative
     */
    fun nextSetBit(fromIndex: Int): Int {
        if (fromIndex < 0) {
            throw IndexOutOfBoundsException("fromIndex < 0: $fromIndex")
        }
        var u = wordIndex(fromIndex)
        if (u >= words!!.size) {
            return -1
        }
        var word = words[u] and (WORD_MASK shl fromIndex)
        while (true) {
            if (word != 0L) {
                return u * BITS_PER_WORD + Long.numberOfTrailingZeros(word)
            }
            if (++u == words.size) {
                return -1
            }
            word = words[u]
        }
    }

    /**
     * Returns the index of the first bit that is set to `false`
     * that occurs on or after the specified starting index.
     *
     * @param  fromIndex the index to start checking from (inclusive)
     * @return the index of the next clear bit
     * @throws IndexOutOfBoundsException if the specified index is negative
     */
    fun nextClearBit(fromIndex: Int): Int {
        if (fromIndex < 0) {
            throw IndexOutOfBoundsException("fromIndex < 0: $fromIndex")
        }
        var u = wordIndex(fromIndex)
        if (u >= words!!.size) {
            return fromIndex
        }
        var word = words[u].inv() and (WORD_MASK shl fromIndex)
        while (true) {
            if (word != 0L) {
                return u * BITS_PER_WORD + Long.numberOfTrailingZeros(word)
            }
            if (++u == words.size) {
                return words.size * BITS_PER_WORD
            }
            word = words[u].inv()
        }
    }

    /**
     * Returns the index of the nearest bit that is set to `false`
     * that occurs on or before the specified starting index.
     * If no such bit exists, or if `-1` is given as the
     * starting index, then `-1` is returned.
     *
     * @param  fromIndex the index to start checking from (inclusive)
     * @return the index of the previous clear bit, or `-1` if there
     * is no such bit
     * @throws IndexOutOfBoundsException if the specified index is less
     * than `-1`
     */
    fun previousClearBit(fromIndex: Int): Int {
        if (fromIndex < 0) {
            if (fromIndex == -1) {
                return -1
            }
            throw IndexOutOfBoundsException("fromIndex < -1: $fromIndex")
        }
        var u = wordIndex(fromIndex)
        if (u >= words!!.size) {
            return fromIndex
        }
        var word = words[u].inv() and (WORD_MASK ushr -(fromIndex + 1))
        while (true) {
            if (word != 0L) {
                return (u + 1) * BITS_PER_WORD - 1 - Long.numberOfLeadingZeros(word)
            }
            if (u-- == 0) {
                return -1
            }
            word = words[u].inv()
        }
    }

    @Override
    override fun iterator(): Iterator<Integer> {
        return object : Iterator<Integer?>() {
            var i = nextSetBit(0)

            @Override
            override fun hasNext(): Boolean {
                return i >= 0
            }

            @Override
            override fun next(): Integer {
                val prev = i
                i = nextSetBit(i + 1)
                return prev
            }

            @Override
            fun remove() {
                throw UnsupportedOperationException()
            }
        }
    }

    /** Converts this bit set to a list.  */
    fun toList(): List<Integer> {
        val list: List<Integer> = ArrayList()
        var i = nextSetBit(0)
        while (i >= 0) {
            list.add(i)
            i = nextSetBit(i + 1)
        }
        return list
    }

    /** Creates a view onto this bit set as a list of integers.
     *
     *
     * The `cardinality` and `get` methods are both O(n), but
     * the iterator is efficient. The list is memory efficient, and the CPU cost
     * breaks even (versus [.toList]) if you intend to scan it only once.  */
    fun asList(): List<Integer> {
        return object : AbstractList<Integer?>() {
            @Override
            operator fun get(index: Int): Integer {
                return nth(index)
            }

            @Override
            fun size(): Int {
                return cardinality()
            }

            @Override
            operator fun iterator(): Iterator<Integer> {
                return this@ImmutableBitSet.iterator()
            }
        }
    }

    /** Creates a view onto this bit set as a set of integers.
     *
     *
     * The `size` and `contains` methods are both O(n), but the
     * iterator is efficient.  */
    fun asSet(): Set<Integer> {
        return object : AbstractSet<Integer?>() {
            @Override
            operator fun iterator(): Iterator<Integer> {
                return this@ImmutableBitSet.iterator()
            }

            @Override
            fun size(): Int {
                return cardinality()
            }

            @Override
            operator fun contains(@Nullable o: Object?): Boolean {
                return this@ImmutableBitSet[requireNonNull(o, "o") as Integer]
            }
        }
    }

    /**
     * Converts this bit set to an array.
     *
     *
     * Each entry of the array is the ordinal of a set bit. The array is
     * sorted.
     *
     * @return Array of set bits
     */
    fun toArray(): IntArray {
        val integers = IntArray(cardinality())
        var j = 0
        var i = nextSetBit(0)
        while (i >= 0) {
            integers[j++] = i
            i = nextSetBit(i + 1)
        }
        return integers
    }

    /**
     * Converts this bit set to an array of little-endian words.
     */
    fun toLongArray(): LongArray? {
        return if (words!!.size == 0) words else words.clone()
    }

    /** Returns the union of this immutable bit set with a [BitSet].  */
    fun union(other: BitSet?): ImmutableBitSet {
        return rebuild() // remember "this" and try to re-use later
            .addAll(BitSets.toIter(other))
            .build()
    }

    /** Returns the union of this bit set with another.  */
    fun union(other: ImmutableBitSet): ImmutableBitSet {
        return rebuild() // remember "this" and try to re-use later
            .addAll(other)
            .build(other) // try to re-use "other"
    }

    /** Returns a bit set with all the bits in this set that are not in
     * another.
     *
     * @see BitSet.andNot
     */
    fun except(that: ImmutableBitSet): ImmutableBitSet {
        val builder = rebuild()
        builder.removeAll(that)
        return builder.build()
    }

    /** Returns a bit set with all the bits set in both this set and in
     * another.
     *
     * @see BitSet.and
     */
    fun intersect(that: ImmutableBitSet): ImmutableBitSet {
        val builder = rebuild()
        builder.intersect(that)
        return builder.build()
    }

    /**
     * Returns true if all bits set in the second parameter are also set in the
     * first. In other words, whether x is a super-set of y.
     *
     * @param set1 Bitmap to be checked
     *
     * @return Whether all bits in set1 are set in set0
     */
    operator fun contains(set1: ImmutableBitSet): Boolean {
        var i = set1.nextSetBit(0)
        while (i >= 0) {
            if (!get(i)) {
                return false
            }
            i = set1.nextSetBit(i + 1)
        }
        return true
    }

    /**
     * The ordinal of a given bit, or -1 if it is not set.
     */
    fun indexOf(bit: Int): Int {
        var i = nextSetBit(0)
        var k = 0
        while (true) {
            if (i < 0) {
                return -1
            }
            if (i == bit) {
                return k
            }
            i = nextSetBit(i + 1)
            ++k
        }
    }

    /**
     * Returns the "logical size" of this `ImmutableBitSet`: the index of
     * the highest set bit in the `ImmutableBitSet` plus one. Returns zero
     * if the `ImmutableBitSet` contains no set bits.
     *
     * @return the logical size of this `ImmutableBitSet`
     */
    fun length(): Int {
        return if (words!!.size == 0) {
            0
        } else BITS_PER_WORD * (words.size - 1)
        +(BITS_PER_WORD - Long.numberOfLeadingZeros(
            words!![words.size - 1]
        ))
    }

    /**
     * Returns true if this `ImmutableBitSet` contains no bits that are set
     * to `true`.
     */
    val isEmpty: Boolean
        get() = words!!.size == 0

    /** Creates a Builder whose initial contents are the same as this
     * ImmutableBitSet.  */
    fun rebuild(): Builder {
        return Rebuilder(this)
    }

    /** Returns the `n`th set bit.
     *
     * @throws java.lang.IndexOutOfBoundsException if n is less than 0 or greater
     * than the number of bits set
     */
    fun nth(n: Int): Int {
        var n = n
        var start = 0
        for (word in words!!) {
            val bitCount: Int = Long.bitCount(word)
            if (n < bitCount) {
                while (word != 0L) {
                    if (word and 1 == 1L) {
                        if (n == 0) {
                            return start
                        }
                        --n
                    }
                    word = word shr 1
                    ++start
                }
            }
            start += 64
            n -= bitCount
        }
        throw IndexOutOfBoundsException("index out of range: $n")
    }

    /** Returns a bit set the same as this but with a given bit set.  */
    fun set(i: Int): ImmutableBitSet {
        return union(of(i))
    }

    /** Returns a bit set the same as this but with a given bit set (if b is
     * true) or unset (if b is false).  */
    operator fun set(i: Int, b: Boolean): ImmutableBitSet {
        if (get(i) == b) {
            return this
        }
        return if (b) set(i) else clear(i)
    }

    /** Returns a bit set the same as this but with a given bit set if condition
     * is true.  */
    fun setIf(bit: Int, condition: Boolean): ImmutableBitSet {
        return if (condition) set(bit) else this
    }

    /** Returns a bit set the same as this but with a given bit cleared.  */
    fun clear(i: Int): ImmutableBitSet {
        return except(of(i))
    }

    /** Returns a bit set the same as this but with a given bit cleared if
     * condition is true.  */
    fun clearIf(i: Int, condition: Boolean): ImmutableBitSet {
        return if (condition) except(of(i)) else this
    }

    /** Returns a [BitSet] that has the same contents as this
     * `ImmutableBitSet`.  */
    fun toBitSet(): BitSet {
        return BitSets.of(this)
    }

    /** Permutes a bit set according to a given mapping.  */
    fun permute(map: Map<Integer?, Integer?>): ImmutableBitSet {
        val builder = builder()
        var i = nextSetBit(0)
        while (i >= 0) {
            val value: Integer = map[i] ?: throw NullPointerException("Index $i is not mapped in $map")
            builder.set(value)
            i = nextSetBit(i + 1)
        }
        return builder.build()
    }

    /** Permutes a bit set according to a given mapping.  */
    fun permute(mapping: Mappings.TargetMapping): ImmutableBitSet {
        val builder = builder()
        var i = nextSetBit(0)
        while (i >= 0) {
            builder.set(mapping.getTarget(i))
            i = nextSetBit(i + 1)
        }
        return builder.build()
    }

    /** Returns a bit set with every bit moved up `offset` positions.
     * Offset may be negative, but throws if any bit ends up negative.  */
    fun shift(offset: Int): ImmutableBitSet {
        if (offset == 0) {
            return this
        }
        val builder = builder()
        var i = nextSetBit(0)
        while (i >= 0) {
            builder.set(i + offset)
            i = nextSetBit(i + 1)
        }
        return builder.build()
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
    @SuppressWarnings("JdkObsolete")
    private class Closure internal constructor(equivalence: SortedMap<Integer?, ImmutableBitSet?>) {
        private val equivalence: SortedMap<Integer, ImmutableBitSet>
        val closure: SortedMap<Integer, ImmutableBitSet> = TreeMap()

        init {
            this.equivalence = equivalence
            val keys: ImmutableIntList = ImmutableIntList.copyOf(equivalence.keySet())
            for (pos in keys) {
                computeClosure(pos)
            }
        }

        @RequiresNonNull("equivalence")
        private fun computeClosure(
            pos: Int
        ): ImmutableBitSet? {
            var o: ImmutableBitSet? = closure.get(pos)
            if (o != null) {
                return o
            }
            val b: ImmutableBitSet = castNonNull(equivalence.get(pos))
            o = b
            var i = b.nextSetBit(pos + 1)
            while (i >= 0) {
                o = o.union(computeClosure(i))
                i = b.nextSetBit(i + 1)
            }
            closure.put(pos, o)
            i = o!!.nextSetBit(pos + 1)
            while (i >= 0) {
                closure.put(i, o)
                i = b.nextSetBit(i + 1)
            }
            return o
        }
    }

    /** Builder.  */
    class Builder(words: LongArray) {
        private var words: @Nullable LongArray?

        init {
            this.words = words
        }

        /** Builds an ImmutableBitSet from the contents of this Builder.
         *
         *
         * After calling this method, the Builder cannot be used again.  */
        fun build(): ImmutableBitSet {
            if (words == null) {
                throw IllegalArgumentException("can only use builder once")
            }
            if (words!!.size == 0) {
                return EMPTY
            }
            val words = words
            this.words = null // prevent re-use of builder
            return ImmutableBitSet(words)
        }

        /** Builds an ImmutableBitSet from the contents of this Builder.
         *
         *
         * After calling this method, the Builder may be used again.  */
        fun buildAndReset(): ImmutableBitSet {
            if (words == null) {
                throw IllegalArgumentException("can only use builder once")
            }
            if (words!!.size == 0) {
                return EMPTY
            }
            val words = words
            this.words = EMPTY_LONGS // reset for next use
            return ImmutableBitSet(words)
        }

        /** Builds an ImmutableBitSet from the contents of this Builder, using
         * an existing ImmutableBitSet if it happens to have the same contents.
         *
         *
         * Supplying the existing bit set if useful for set operations,
         * where there is a significant chance that the original bit set is
         * unchanged. We save memory because we use the same copy. For example:
         *
         * <blockquote><pre>
         * ImmutableBitSet primeNumbers;
         * ImmutableBitSet hundreds = ImmutableBitSet.of(100, 200, 300);
         * return primeNumbers.except(hundreds);</pre></blockquote>
         *
         *
         * After calling this method, the Builder cannot be used again.  */
        fun build(bitSet: ImmutableBitSet): ImmutableBitSet {
            return if (wouldEqual(bitSet)) {
                bitSet
            } else build()
        }

        fun set(bit: Int): Builder {
            if (words == null) {
                throw IllegalArgumentException("can only use builder once")
            }
            val wordIndex = wordIndex(bit)
            if (wordIndex >= words!!.size) {
                words = Arrays.copyOf(words, wordIndex + 1)
            }
            words!![wordIndex] = words!![wordIndex] or (1L shl bit)
            return this
        }

        operator fun get(bitIndex: Int): Boolean {
            if (words == null) {
                throw IllegalArgumentException("can only use builder once")
            }
            if (bitIndex < 0) {
                throw IndexOutOfBoundsException("bitIndex < 0: $bitIndex")
            }
            val wordIndex = wordIndex(bitIndex)
            return (wordIndex < words!!.size
                    && words!![wordIndex] and (1L shl bitIndex) != 0L)
        }

        private fun trim(wordCount: Int) {
            var wordCount = wordCount
            if (words == null) {
                throw IllegalArgumentException("can only use builder once")
            }
            while (wordCount > 0 && words!![wordCount - 1] == 0L) {
                --wordCount
            }
            if (wordCount == words!!.size) {
                return
            }
            words = if (wordCount == 0) {
                EMPTY_LONGS
            } else {
                Arrays.copyOfRange(words, 0, wordCount)
            }
        }

        fun clear(bit: Int): Builder {
            if (words == null) {
                throw IllegalArgumentException("can only use builder once")
            }
            val wordIndex = wordIndex(bit)
            if (wordIndex < words!!.size) {
                words!![wordIndex] = words!![wordIndex] and (1L shl bit).inv()
                trim(words!!.size)
            }
            return this
        }

        /** Returns whether the bit set that would be created by this Builder would
         * equal a given bit set.  */
        fun wouldEqual(bitSet: ImmutableBitSet): Boolean {
            if (words == null) {
                throw IllegalArgumentException("can only use builder once")
            }
            return Arrays.equals(words, bitSet.words)
        }

        /** Returns the number of set bits.  */
        fun cardinality(): Int {
            if (words == null) {
                throw IllegalArgumentException("can only use builder once")
            }
            return countBits(words)
        }

        /** Merges another builder. Does not modify the other builder.  */
        fun combine(builder: Builder): Builder {
            if (words == null) {
                throw IllegalArgumentException("can only use builder once")
            }
            val otherWords = builder.words ?: throw IllegalArgumentException("Given builder is empty")
            if (words!!.size < otherWords.size) {
                // Right has more bits. Copy the right and OR in the words of the
                // previous left.
                val newWords: LongArray = otherWords.clone()
                for (i in words.indices) {
                    newWords[i] = newWords[i] or words!![i]
                }
                words = newWords
            } else {
                // Left has same or more bits. OR in the words of the right.
                for (i in otherWords.indices) {
                    words!![i] = words!![i] or otherWords[i]
                }
            }
            return this
        }

        /** Sets all bits in a given bit set.  */
        fun addAll(bitSet: ImmutableBitSet): Builder {
            for (bit in bitSet) {
                set(bit)
            }
            return this
        }

        /** Sets all bits in a given list of bits.  */
        fun addAll(integers: Iterable<Integer?>): Builder {
            for (integer in integers) {
                set(integer)
            }
            return this
        }

        /** Sets all bits in a given list of `int`s.  */
        fun addAll(integers: ImmutableIntList): Builder {
            for (i in 0 until integers.size()) {
                set(integers.get(i))
            }
            return this
        }

        /** Clears all bits in a given bit set.  */
        fun removeAll(bitSet: ImmutableBitSet): Builder {
            for (bit in bitSet) {
                clear(bit)
            }
            return this
        }

        /** Sets a range of bits, from `from` to `to` - 1.  */
        operator fun set(fromIndex: Int, toIndex: Int): Builder {
            if (fromIndex > toIndex) {
                throw IllegalArgumentException(
                    "fromIndex(" + fromIndex + ")"
                            + " > toIndex(" + toIndex + ")"
                )
            }
            if (toIndex < 0) {
                throw IllegalArgumentException("toIndex($toIndex) < 0")
            }
            if (words == null) {
                throw IllegalArgumentException("can only use builder once")
            }
            if (fromIndex < toIndex) {
                // Increase capacity if necessary
                val startWordIndex = wordIndex(fromIndex)
                val endWordIndex = wordIndex(toIndex - 1)
                if (endWordIndex >= words!!.size) {
                    words = Arrays.copyOf(words, endWordIndex + 1)
                }
                val firstWordMask = WORD_MASK shl fromIndex
                val lastWordMask = WORD_MASK ushr -toIndex
                if (startWordIndex == endWordIndex) {
                    // One word
                    words!![startWordIndex] = words!![startWordIndex] or (firstWordMask and lastWordMask)
                } else {
                    // First word, middle words, last word
                    words!![startWordIndex] = words!![startWordIndex] or firstWordMask
                    for (i in startWordIndex + 1 until endWordIndex) {
                        words!![i] = WORD_MASK
                    }
                    words!![endWordIndex] = words!![endWordIndex] or lastWordMask
                }
            }
            return this
        }

        val isEmpty: Boolean
            get() {
                if (words == null) {
                    throw IllegalArgumentException("can only use builder once")
                }
                return words!!.size == 0
            }

        fun intersect(that: ImmutableBitSet) {
            if (words == null) {
                throw IllegalArgumentException("can only use builder once")
            }
            val x: Int = Math.min(words!!.size, that.words!!.size)
            for (i in 0 until x) {
                words!![i] = words!![i] and that.words[i]
            }
            trim(x)
        }
    }

    /** Refinement of [Builder] that remembers its original
     * [org.apache.calcite.util.ImmutableBitSet] and tries to use it
     * when [.build] is called.  */
    private class Rebuilder(private val originalBitSet: ImmutableBitSet) : Builder(originalBitSet.words.clone()) {
        @Override
        override fun build(): ImmutableBitSet {
            return if (wouldEqual(originalBitSet)) {
                originalBitSet
            } else super.build()
        }

        @Override
        override fun build(bitSet: ImmutableBitSet): ImmutableBitSet {
            // We try to re-use both originalBitSet and bitSet.
            return if (wouldEqual(originalBitSet)) {
                originalBitSet
            } else super.build(bitSet)
        }
    }

    companion object {
        /** Compares bit sets topologically, so that enclosing bit sets come first,
         * using natural ordering to break ties.  */
        val COMPARATOR: Comparator<ImmutableBitSet> = label@ Comparator<ImmutableBitSet> { o1, o2 ->
            if (o1.equals(o2)) {
                return@label 0
            }
            if (o1.contains(o2)) {
                return@label -1
            }
            if (o2.contains(o1)) {
                return@label 1
            }
            o1.compareTo(o2)
        }
        val ORDERING: Ordering<ImmutableBitSet> = Ordering.from(COMPARATOR)

        // BitSets are packed into arrays of "words."  Currently a word is
        // a long, which consists of 64 bits, requiring 6 address bits.
        // The choice of word size is determined purely by performance concerns.
        private const val ADDRESS_BITS_PER_WORD = 6
        private const val BITS_PER_WORD = 1 shl ADDRESS_BITS_PER_WORD

        /* Used to shift left or right for a partial word mask */
        private const val WORD_MASK = -0x1L
        private val EMPTY_LONGS = LongArray(0)
        private val EMPTY = ImmutableBitSet(EMPTY_LONGS)

        @SuppressWarnings("Guava")
        @Deprecated // to be removed before 2.0
        val FROM_BIT_SET: com.google.common.base.Function<in BitSet?, ImmutableBitSet> =
            com.google.common.base.Function<in BitSet?, ImmutableBitSet> { input: BitSet? -> fromBitSet(input) }

        /** Creates an ImmutableBitSet with no bits.  */
        fun of(): ImmutableBitSet {
            return EMPTY
        }

        fun of(vararg bits: Int): ImmutableBitSet {
            var max = -1
            for (bit in bits) {
                max = Math.max(bit, max)
            }
            if (max == -1) {
                return EMPTY
            }
            val words = LongArray(wordIndex(max) + 1)
            for (bit in bits) {
                val wordIndex = wordIndex(bit)
                words[wordIndex] = words[wordIndex] or (1L shl bit)
            }
            return ImmutableBitSet(words)
        }

        fun of(bits: Iterable<Integer?>): ImmutableBitSet {
            if (bits is ImmutableBitSet) {
                return bits
            }
            var max = -1
            for (bit in bits) {
                max = Math.max(bit, max)
            }
            if (max == -1) {
                return EMPTY
            }
            val words = LongArray(wordIndex(max) + 1)
            for (bit in bits) {
                val wordIndex = wordIndex(bit)
                words[wordIndex] = words[wordIndex] or (1L shl bit)
            }
            return ImmutableBitSet(words)
        }

        /**
         * Creates an ImmutableBitSet with given bits set.
         *
         *
         * For example, `of(ImmutableIntList.of(0, 3))` returns a bit
         * set with bits {0, 3} set.
         *
         * @param bits Collection of bits to set
         * @return Bit set
         */
        fun of(bits: ImmutableIntList): ImmutableBitSet {
            return builder().addAll(bits).build()
        }

        /**
         * Returns a new immutable bit set containing all the bits in the given long
         * array.
         *
         *
         * More precisely,
         *
         * <blockquote>`ImmutableBitSet.valueOf(longs).get(n)
         * == ((longs[n/64] & (1L<<(n%64))) != 0)`</blockquote>
         *
         *
         * for all `n < 64 * longs.length`.
         *
         *
         * This method is equivalent to
         * `ImmutableBitSet.valueOf(LongBuffer.wrap(longs))`.
         *
         * @param longs a long array containing a little-endian representation
         * of a sequence of bits to be used as the initial bits of the
         * new bit set
         * @return a `ImmutableBitSet` containing all the bits in the long
         * array
         */
        fun valueOf(vararg longs: Long): ImmutableBitSet {
            var n = longs.size
            while (n > 0 && longs[n - 1] == 0L) {
                --n
            }
            return if (n == 0) {
                EMPTY
            } else ImmutableBitSet(Arrays.copyOf(longs, n))
        }

        /**
         * Returns a new immutable bit set containing all the bits in the given long
         * buffer.
         */
        fun valueOf(longs: LongBuffer): ImmutableBitSet {
            var longs: LongBuffer = longs
            longs = longs.slice()
            var n: Int = longs.remaining()
            while (n > 0 && longs.get(n - 1) === 0) {
                --n
            }
            if (n == 0) {
                return EMPTY
            }
            val words = LongArray(n)
            longs.get(words)
            return ImmutableBitSet(words)
        }

        /**
         * Returns a new immutable bit set containing all the bits in the given
         * [BitSet].
         */
        fun fromBitSet(input: BitSet?): ImmutableBitSet {
            return of(BitSets.toIter(input))
        }

        /**
         * Creates an ImmutableBitSet with bits from `fromIndex` (inclusive) to
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
        fun range(fromIndex: Int, toIndex: Int): ImmutableBitSet {
            if (fromIndex > toIndex) {
                throw IllegalArgumentException()
            }
            if (toIndex < 0) {
                throw IllegalArgumentException()
            }
            if (fromIndex == toIndex) {
                return EMPTY
            }
            val startWordIndex = wordIndex(fromIndex)
            val endWordIndex = wordIndex(toIndex - 1)
            val words = LongArray(endWordIndex + 1)
            val firstWordMask = WORD_MASK shl fromIndex
            val lastWordMask = WORD_MASK ushr -toIndex
            if (startWordIndex == endWordIndex) {
                // One word
                words[startWordIndex] = words[startWordIndex] or (firstWordMask and lastWordMask)
            } else {
                // First word, middle words, last word
                words[startWordIndex] = words[startWordIndex] or firstWordMask
                for (i in startWordIndex + 1 until endWordIndex) {
                    words[i] = WORD_MASK
                }
                words[endWordIndex] = words[endWordIndex] or lastWordMask
            }
            return ImmutableBitSet(words)
        }

        /** Creates an ImmutableBitSet with bits between 0 and `toIndex` set.  */
        fun range(toIndex: Int): ImmutableBitSet {
            return range(0, toIndex)
        }

        /**
         * Given a bit index, return word index containing it.
         */
        @Pure
        private fun wordIndex(bitIndex: Int): Int {
            return bitIndex shr ADDRESS_BITS_PER_WORD
        }

        /** Creates a Collector.  */
        fun toImmutableBitSet(): Collector<Integer, Builder, ImmutableBitSet> {
            return Collector.of(
                ImmutableBitSet::builder,
                Builder::set,
                { obj: Builder, builder: Builder -> obj.combine(builder) },
                Builder::build
            )
        }

        /**
         * Checks that fromIndex ... toIndex is a valid range of bit indices.
         */
        private fun checkRange(fromIndex: Int, toIndex: Int) {
            if (fromIndex < 0) {
                throw IndexOutOfBoundsException("fromIndex < 0: $fromIndex")
            }
            if (toIndex < 0) {
                throw IndexOutOfBoundsException("toIndex < 0: $toIndex")
            }
            if (fromIndex > toIndex) {
                throw IndexOutOfBoundsException(
                    "fromIndex: " + fromIndex
                            + " > toIndex: " + toIndex
                )
            }
        }

        private fun countBits(words: LongArray?): Int {
            var sum = 0
            for (word in words!!) {
                sum += Long.bitCount(word)
            }
            return sum
        }

        /** Returns the union of a number of bit sets.  */
        fun union(
            sets: Iterable<ImmutableBitSet>
        ): ImmutableBitSet {
            val builder = builder()
            for (set in sets) {
                builder.addAll(set)
            }
            return builder.build()
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
            equivalence: SortedMap<Integer?, ImmutableBitSet?>
        ): SortedMap<Integer, ImmutableBitSet> {
            var equivalence: SortedMap<Integer?, ImmutableBitSet?> = equivalence
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
                val old: SortedMap<Integer?, ImmutableBitSet?> = equivalence
                equivalence = TreeMap()
                for (i in 0 until length) {
                    val bitSet: ImmutableBitSet = old.get(i)
                    equivalence.put(i, bitSet ?: of())
                }
            }
            val closure = Closure(equivalence)
            return closure.closure
        }

        /** Creates an empty Builder.  */
        fun builder(): Builder {
            return Builder(EMPTY_LONGS)
        }

        @Deprecated // to be removed before 2.0
        fun builder(bitSet: ImmutableBitSet): Builder {
            return bitSet.rebuild()
        }

        /** Permutes a collection of bit sets according to a given mapping.  */
        fun permute(
            bitSets: Iterable<ImmutableBitSet?>?,
            map: Map<Integer?, Integer?>?
        ): Iterable<ImmutableBitSet> {
            return Util.transform(bitSets) { bitSet -> bitSet.permute(map) }
        }

        /**
         * Checks if all bit sets contain a particular bit.
         */
        fun allContain(bitSets: Collection<ImmutableBitSet>, bit: Int): Boolean {
            for (bitSet in bitSets) {
                if (!bitSet[bit]) {
                    return false
                }
            }
            return true
        }
    }
}
