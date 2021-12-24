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

import java.io.Serializable

/**
 * Pair of objects.
 *
 *
 * Because a pair implements [.equals], [.hashCode] and
 * [.compareTo], it can be used in any kind of
 * [java.util.Collection].
 *
 * @param <T1> Left-hand type
 * @param <T2> Right-hand type
</T2></T1> */
@SuppressWarnings("type.argument.type.incompatible")
class Pair<T1 : Object?, T2 : Object?>(left: T1, right: T2) : Comparable<Pair<T1, T2>?>, Map.Entry<T1, T2>,
    Serializable {
    //~ Instance fields --------------------------------------------------------
    @get:Override
    override val key: T1?

    @get:Override
    override val value: T2?
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a Pair.
     *
     * @param left  left value
     * @param right right value
     */
    init {
        key = left
        value = right
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (this === obj
                || (obj is Pair<*, *>
                && Objects.equals(key, (obj as Pair<*, *>).key)
                && Objects.equals(value, (obj as Pair<*, *>).value)))
    }

    /** {@inheritDoc}
     *
     *
     * Computes hash code consistent with
     * [java.util.Map.Entry.hashCode].  */
    @Override
    override fun hashCode(): Int {
        val keyHash = if (key == null) 0 else key!!.hashCode()
        val valueHash = if (value == null) 0 else value!!.hashCode()
        return keyHash xor valueHash
    }

    @Override
    operator fun compareTo(that: Pair<T1, T2>): Int {
        var c: Int = NULLS_FIRST_COMPARATOR.compare(key, that.key)
        if (c == 0) {
            c = NULLS_FIRST_COMPARATOR.compare(value, that.value)
        }
        return c
    }

    @Override
    override fun toString(): String {
        return "<" + key + ", " + value + ">"
    }

    @Override
    fun setValue(value: T2): T2 {
        throw UnsupportedOperationException()
    }

    /** Iterator that returns the first element of a collection paired with every
     * other element.
     *
     * @param <E> Element type
    </E> */
    private class FirstAndIterator<E> internal constructor(iterator: Iterator<E>?, first: E) : Iterator<Pair<E, E>?> {
        private val iterator: Iterator<E>
        private val first: E

        init {
            this.iterator = Objects.requireNonNull(iterator, "iterator")
            this.first = first
        }

        @Override
        override fun hasNext(): Boolean {
            return iterator.hasNext()
        }

        @Override
        override fun next(): Pair<E, E> {
            return of(first, iterator.next())
        }

        @Override
        fun remove() {
            throw UnsupportedOperationException("remove")
        }
    }

    /** Iterator that pairs elements from two iterators.
     *
     * @param <L> Left-hand type
     * @param <R> Right-hand type
    </R></L> */
    private class ZipIterator<L, R> internal constructor(
        leftIterator: Iterator<L>?,
        rightIterator: Iterator<R>?
    ) : Iterator<Pair<L, R>?> {
        private val leftIterator: Iterator<L>
        private val rightIterator: Iterator<R>

        init {
            this.leftIterator = Objects.requireNonNull(leftIterator, "leftIterator")
            this.rightIterator = Objects.requireNonNull(rightIterator, "rightIterator")
        }

        @Override
        override fun hasNext(): Boolean {
            return leftIterator.hasNext() && rightIterator.hasNext()
        }

        @Override
        override fun next(): Pair<L, R> {
            return of(leftIterator.next(), rightIterator.next())
        }

        @Override
        fun remove() {
            leftIterator.remove()
            rightIterator.remove()
        }
    }

    /** Iterator that returns consecutive pairs of elements from an underlying
     * iterator.
     *
     * @param <E> Element type
    </E> */
    private class AdjacentIterator<E> internal constructor(iterator: Iterator<E>) : Iterator<Pair<E, E>?> {
        private val first: E
        private val iterator: Iterator<E>
        var previous: E

        init {
            this.iterator = Objects.requireNonNull(iterator, "iterator")
            first = iterator.next()
            previous = first
        }

        @Override
        override fun hasNext(): Boolean {
            return iterator.hasNext()
        }

        @Override
        override fun next(): Pair<E, E> {
            val current = iterator.next()
            val pair: Pair<E, E> = of<E, E>(previous, current)
            previous = current
            return pair
        }

        @Override
        fun remove() {
            throw UnsupportedOperationException("remove")
        }
    }

    /** Unmodifiable list of pairs, backed by a pair of lists.
     *
     *
     * Though it is unmodifiable, it is mutable: if the contents of one
     * of the backing lists changes, the contents of this list will appear to
     * change. The length, however, is fixed on creation.
     *
     * @param <K> Left-hand type
     * @param <V> Right-hand type
     *
     * @see MutableZipList
    </V></K> */
    private class ZipList<K, V> internal constructor(
        private val ks: List<K>,
        private val vs: List<V>,
        private val size: Int
    ) : AbstractList<Pair<K, V>?>() {
        @Override
        operator fun get(index: Int): Pair<K, V> {
            return of(ks[index], vs[index])
        }

        @Override
        fun size(): Int {
            return size
        }
    }

    /** A mutable list of pairs backed by a pair of mutable lists.
     *
     *
     * Modifications to this list are reflected in the backing lists, and vice
     * versa.
     *
     * @param <K> Key (left) value type
     * @param <V> Value (right) value type
    </V></K> */
    private class MutableZipList<K, V> internal constructor(ks: List<K>?, vs: List<V>?) : AbstractList<Pair<K, V>?>() {
        private val ks: List<K>
        private val vs: List<V>

        init {
            this.ks = Objects.requireNonNull(ks, "ks")
            this.vs = Objects.requireNonNull(vs, "vs")
        }

        @Override
        operator fun get(index: Int): Pair<K, V> {
            return of(ks[index], vs[index])
        }

        @Override
        fun size(): Int {
            return Math.min(ks.size(), vs.size())
        }

        @Override
        fun add(index: Int, pair: Pair<K, V>) {
            ks.add(index, pair.key)
            vs.add(index, pair.value)
        }

        @Override
        fun remove(index: Int): Pair<K, V> {
            val bufferedRow: K = ks.remove(index)
            val stateSet: V = vs.remove(index)
            return of<K, V>(bufferedRow, stateSet)
        }

        @Override
        operator fun set(index: Int, pair: Pair<K, V>): Pair<K, V> {
            val previous = get(index)
            ks.set(index, pair.key)
            vs.set(index, pair.value)
            return previous
        }
    }

    companion object {
        @SuppressWarnings(["rawtypes", "unchecked"])
        private val NULLS_FIRST_COMPARATOR: Comparator = Comparator.nullsFirst(Comparator.naturalOrder() as Comparator)

        /**
         * Creates a Pair of appropriate type.
         *
         *
         * This is a shorthand that allows you to omit implicit types. For
         * example, you can write:
         * <blockquote>return Pair.of(s, n);</blockquote>
         * instead of
         * <blockquote>return new Pair&lt;String, Integer&gt;(s, n);</blockquote>
         *
         * @param left  left value
         * @param right right value
         * @return A Pair
         */
        fun <T1, T2> of(left: T1?, right: T2?): Pair<T1?, T2?> {
            return Pair(left, right)
        }

        /** Creates a `Pair` from a [java.util.Map.Entry].  */
        fun <K, V> of(entry: Map.Entry<K, V>): Pair<K, V> {
            return of(entry.getKey(), entry.getValue())
        }

        /**
         * Converts a collection of Pairs into a Map.
         *
         *
         * This is an obvious thing to do because Pair is similar in structure to
         * [java.util.Map.Entry].
         *
         *
         * The map contains a copy of the collection of Pairs; if you change the
         * collection, the map does not change.
         *
         * @param pairs Collection of Pair objects
         * @return map with the same contents as the collection
         */
        fun <K, V> toMap(pairs: Iterable<Pair<out K, out V>>): Map<K, V> {
            val map: Map<K, V> = HashMap()
            for ((key, value) in pairs) {
                map.put(key, value)
            }
            return map
        }

        /**
         * Converts two lists into a list of [Pair]s,
         * whose length is the lesser of the lengths of the
         * source lists.
         *
         * @param ks Left list
         * @param vs Right list
         * @return List of pairs
         * @see org.apache.calcite.linq4j.Ord.zip
         */
        fun <K, V> zip(ks: List<K>, vs: List<V>): List<Pair<K, V>> {
            return zip<Any, Any>(ks, vs, false)
        }

        /**
         * Converts two lists into a list of [Pair]s.
         *
         *
         * The length of the combined list is the lesser of the lengths of the
         * source lists. But typically the source lists will be the same length.
         *
         * @param ks     Left list
         * @param vs     Right list
         * @param strict Whether to fail if lists have different size
         * @return List of pairs
         * @see org.apache.calcite.linq4j.Ord.zip
         */
        fun <K, V> zip(
            ks: List<K>,
            vs: List<V>,
            strict: Boolean
        ): List<Pair<K, V>> {
            val size: Int
            if (strict) {
                if (ks.size() !== vs.size()) {
                    throw AssertionError()
                }
                size = ks.size()
            } else {
                size = Math.min(ks.size(), vs.size())
            }
            return ZipList<Any, Any>(ks, vs, size)
        }

        /**
         * Converts two iterables into an iterable of [Pair]s.
         *
         *
         * The resulting iterator ends whenever the first of the input iterators
         * ends. But typically the source iterators will be the same length.
         *
         * @param ks Left iterable
         * @param vs Right iterable
         * @return Iterable over pairs
         */
        fun <K, V> zip(
            ks: Iterable<K>,
            vs: Iterable<V>
        ): Iterable<Pair<K, V>> {
            return Iterable<Pair<K, V>> {
                val kIterator = ks.iterator()
                val vIterator = vs.iterator()
                ZipIterator<Any, Any>(kIterator, vIterator)
            }
        }

        /**
         * Converts two arrays into a list of [Pair]s.
         *
         *
         * The length of the combined list is the lesser of the lengths of the
         * source arrays. But typically the source arrays will be the same
         * length.
         *
         * @param ks Left array
         * @param vs Right array
         * @return List of pairs
         */
        fun <K, V> zip(
            ks: Array<K>,
            vs: Array<V>
        ): List<Pair<K, V>> {
            return object : AbstractList<Pair<K, V>?>() {
                @Override
                operator fun get(index: Int): Pair<K, V> {
                    return of<K, V>(ks[index], vs[index])
                }

                @Override
                fun size(): Int {
                    return Math.min(ks.size, vs.size)
                }
            }
        }

        /** Returns a mutable list of pairs backed by a pair of mutable lists.
         *
         *
         * Modifications to this list are reflected in the backing lists, and vice
         * versa.
         *
         * @param <K> Key (left) value type
         * @param <V> Value (right) value type
        </V></K> */
        fun <K, V> zipMutable(
            ks: List<K>?,
            vs: List<V>?
        ): List<Pair<K, V>> {
            return MutableZipList<Any, Any>(ks, vs)
        }

        /** Applies an action to every element of a pair of iterables.
         *
         *
         * Calls to the action stop whenever the first of the input iterators
         * ends. But typically the source iterators will be the same length.
         *
         * @see Map.forEach
         * @see org.apache.calcite.linq4j.Ord.forEach
         * @param ks Left iterable
         * @param vs Right iterable
         * @param consumer The action to be performed for each element
         *
         * @param <K> Left type
         * @param <V> Right type
        </V></K> */
        fun <K, V> forEach(
            ks: Iterable<K?>,
            vs: Iterable<V?>,
            consumer: BiConsumer<in K?, in V?>
        ) {
            val leftIterator = ks.iterator()
            val rightIterator = vs.iterator()
            while (leftIterator.hasNext() && rightIterator.hasNext()) {
                consumer.accept(leftIterator.next(), rightIterator.next())
            }
        }

        /** Applies an action to every element of an iterable of pairs.
         *
         * @see Map.forEach
         * @param entries Pairs
         * @param consumer The action to be performed for each element
         *
         * @param <K> Left type
         * @param <V> Right type
        </V></K> */
        fun <K, V> forEach(
            entries: Iterable<Map.Entry<K?, V?>?>,
            consumer: BiConsumer<in K?, in V?>
        ) {
            for (entry in entries) {
                consumer.accept(entry.getKey(), entry.getValue())
            }
        }

        /**
         * Returns an iterable over the left slice of an iterable.
         *
         * @param iterable Iterable over pairs
         * @param <L>      Left type
         * @param <R>      Right type
         * @return Iterable over the left elements
        </R></L> */
        fun <L, R> left(
            iterable: Iterable<Map.Entry<L, R?>?>?
        ): Iterable<L> {
            return Util.transform(iterable, Map.Entry::getKey)
        }

        /**
         * Returns an iterable over the right slice of an iterable.
         *
         * @param iterable Iterable over pairs
         * @param <L>      right type
         * @param <R>      Right type
         * @return Iterable over the right elements
        </R></L> */
        fun <L, R> right(
            iterable: Iterable<Map.Entry<L?, R>?>?
        ): Iterable<R> {
            return Util.transform(iterable, Map.Entry::getValue)
        }

        fun <K, V> left(
            pairs: List<Map.Entry<K, V?>?>?
        ): List<K> {
            return Util.transform(pairs, Map.Entry::getKey)
        }

        fun <K, V> right(
            pairs: List<Map.Entry<K?, V>?>?
        ): List<V> {
            return Util.transform(pairs, Map.Entry::getValue)
        }

        /**
         * Returns an iterator that iterates over (i, i + 1) pairs in an iterable.
         *
         *
         * For example, `adjacents([3, 5, 7])` returns [(3, 5), (5, 7)].
         *
         * @param iterable Source collection
         * @param <T> Element type
         * @return Iterable over adjacent element pairs
        </T> */
        fun <T> adjacents(iterable: Iterable<T>): Iterable<Pair<T, T>> {
            return label@ Iterable<Pair<T, T>> {
                val iterator = iterable.iterator()
                if (!iterator.hasNext()) {
                    return@label Collections.emptyIterator()
                }
                AdjacentIterator<Any>(iterator)
            }
        }

        /**
         * Returns an iterator that iterates over (0, i) pairs in an iterable for
         * i &gt; 0.
         *
         *
         * For example, `firstAnd([3, 5, 7])` returns [(3, 5), (3, 7)].
         *
         * @param iterable Source collection
         * @param <T> Element type
         * @return Iterable over pairs of the first element and all other elements
        </T> */
        fun <T> firstAnd(iterable: Iterable<T>): Iterable<Pair<T, T>> {
            return label@ Iterable<Pair<T, T>> {
                val iterator = iterable.iterator()
                if (!iterator.hasNext()) {
                    return@label Collections.emptyIterator()
                }
                val first = iterator.next()
                FirstAndIterator<T>(iterator, first)
            }
        }
    }
}
