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
package org.apache.calcite.runtime

import org.apache.calcite.util.ImmutableNullableList
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import org.checkerframework.checker.nullness.qual.PolyNull
import java.util.AbstractList
import java.util.ArrayList
import java.util.Arrays
import java.util.Collections
import java.util.Iterator
import java.util.List
import java.util.Map
import java.util.Objects
import java.util.RandomAccess
import org.apache.calcite.linq4j.Nullness.castNonNull

/**
 * Space-efficient, comparable, immutable lists.
 */
object FlatLists {
    val COMPARABLE_EMPTY_LIST: ComparableEmptyList<*> = ComparableEmptyList<Any?>()

    /** Creates a flat list with 0 elements.  */
    fun <T> of(): ComparableList<T> {
        return COMPARABLE_EMPTY_LIST
    }

    /** Creates a flat list with 1 element.  */
    fun <T> of(t0: T): List<T> {
        return Flat1List(t0)
    }

    /** Creates a flat list with 2 elements.  */
    fun <T> of(t0: T, t1: T): List<T> {
        return Flat2List(t0, t1)
    }

    /** Creates a flat list with 3 elements.  */
    fun <T> of(t0: T, t1: T, t2: T): List<T> {
        return Flat3List(t0, t1, t2)
    }

    /** Creates a flat list with 4 elements.  */
    fun <T> of(t0: T, t1: T, t2: T, t3: T): List<T> {
        return Flat4List(t0, t1, t2, t3)
    }

    /** Creates a flat list with 6 elements.  */
    fun <T> of(t0: T, t1: T, t2: T, t3: T, t4: T): List<T> {
        return Flat5List(t0, t1, t2, t3, t4)
    }

    /** Creates a flat list with 6 elements.  */
    fun <T> of(t0: T, t1: T, t2: T, t3: T, t4: T, t5: T): List<T> {
        return Flat6List(t0, t1, t2, t3, t4, t5)
    }

    /**
     * Creates a memory-, CPU- and cache-efficient immutable list.
     *
     * @param t Array of members of list
     * @param <T> Element type
     * @return List containing the given members
    </T> */
    fun <T : Comparable?> of(vararg t: T): List<T> {
        return flatList_(t, false)
    }

    /**
     * Creates a memory-, CPU- and cache-efficient immutable list,
     * always copying the contents.
     *
     * @param t Array of members of list
     * @param <T> Element type
     * @return List containing the given members
    </T> */
    @Deprecated // to be removed before 2.0
    fun <T> copy(vararg t: T): List<T> {
        return flatListNotComparable<T>(t)
    }

    /**
     * Creates a memory-, CPU- and cache-efficient comparable immutable list,
     * always copying the contents.
     *
     *
     * The elements are comparable, and so is the returned list.
     * Elements may be null.
     *
     * @param t Array of members of list
     * @param <T> Element type
     * @return List containing the given members
    </T> */
    fun <T : Comparable?> copyOf(vararg t: T): List<T> {
        return flatList_(t, true)
    }

    /**
     * Creates a memory-, CPU- and cache-efficient immutable list,
     * always copying the contents.
     *
     *
     * The elements need not be comparable,
     * and the returned list may not implement [Comparable].
     * Elements may be null.
     *
     * @param t Array of members of list
     * @param <T> Element type
     * @return List containing the given members
    </T> */
    fun <T> copyOf(vararg t: T): List<T> {
        return flatListNotComparable<T>(t)
    }

    /**
     * Creates a memory-, CPU- and cache-efficient comparable immutable list,
     * optionally copying the list.
     *
     * @param copy Whether to always copy the list
     * @param t Array of members of list
     * @return List containing the given members
     */
    private fun <T : Comparable?> flatList_(
        t: Array<T>, copy: Boolean
    ): ComparableList<T> {
        return when (t.size) {
            0 -> COMPARABLE_EMPTY_LIST
            1 -> Flat1List(t[0])
            2 -> Flat2List(t[0], t[1])
            3 -> Flat3List(t[0], t[1], t[2])
            4 -> Flat4List(t[0], t[1], t[2], t[3])
            5 -> Flat5List(t[0], t[1], t[2], t[3], t[4])
            6 -> Flat6List(t[0], t[1], t[2], t[3], t[4], t[5])
            else ->       // REVIEW: AbstractList contains a modCount field; we could
                //   write our own implementation and reduce creation overhead a
                //   bit.
                if (copy) {
                    ComparableListImpl(Arrays.asList(t.clone()))
                } else {
                    ComparableListImpl(Arrays.asList(t))
                }
        }
    }

    /**
     * Creates a memory-, CPU- and cache-efficient immutable list,
     * always copying the list.
     *
     * @param t Array of members of list
     * @return List containing the given members
     */
    private fun <T> flatListNotComparable(t: Array<T>): List<T> {
        return when (t.size) {
            0 -> COMPARABLE_EMPTY_LIST
            1 -> Flat1List(t[0])
            2 -> Flat2List(t[0], t[1])
            3 -> Flat3List(t[0], t[1], t[2])
            4 -> Flat4List(t[0], t[1], t[2], t[3])
            5 -> Flat5List(t[0], t[1], t[2], t[3], t[4])
            6 -> Flat6List(t[0], t[1], t[2], t[3], t[4], t[5])
            else -> ImmutableNullableList.copyOf(t)
        }
    }

    /**
     * Creates a memory-, CPU- and cache-efficient immutable list from an
     * existing list. The list is always copied.
     *
     * @param t Array of members of list
     * @param <T> Element type
     * @return List containing the given members
    </T> */
    fun <T> of(t: List<T>): List<T> {
        return of_<Any>(t)
    }

    fun <T : Comparable?> ofComparable(
        t: List<T>
    ): ComparableList<T> {
        return of_(t)
    }

    private fun <T> of_(t: List<T>): ComparableList<T?> {
        return when (t.size()) {
            0 -> COMPARABLE_EMPTY_LIST
            1 -> Flat1List(t[0])
            2 -> Flat2List(t[0], t[1])
            3 -> Flat3List(t[0], t[1], t[2])
            4 -> Flat4List(t[0], t[1], t[2], t[3])
            5 -> Flat5List(t[0], t[1], t[2], t[3], t[4])
            6 -> Flat6List(
                t[0], t[1], t[2], t[3], t[4],
                t[5]
            )
            else ->       // REVIEW: AbstractList contains a modCount field; we could
                //   write our own implementation and reduce creation overhead a
                //   bit.
                ComparableListImpl<Any?>(Arrays.asList(t.toArray()))
        }
    }

    /** Returns a list that consists of a given list plus an element.  */
    fun <E : Object?> append(list: List<E>?, e: E): List<E> {
        if (list is AbstractFlatList<*>) {
            return (list as AbstractFlatList<*>).append(e)
        }
        val newList: List<E> = ArrayList(list)
        newList.add(e)
        return of(newList)
    }

    /** Returns a list that consists of a given list plus an element, guaranteed
     * to be an [ImmutableList].  */
    fun <E : Object?> append(list: ImmutableList<E>?, e: E): ImmutableList<E> {
        return ImmutableList.< E > builder < E ? > ().addAll(list).add(e).build()
    }

    /** Returns a map that consists of a given map plus an (key, value),
     * guaranteed to be an [ImmutableMap].  */
    fun <K : Object?, V : Object?> append(
        map: Map<K, V>, k: K, v: V
    ): ImmutableMap<K, V> {
        val builder: ImmutableMap.Builder<K, V> = ImmutableMap.builder()
        builder.put(k, v)
        map.forEach { k2, v2 ->
            if (!k!!.equals(k2)) {
                builder.put(k2, v2)
            }
        }
        return builder.build()
    }

    /** Base class for flat lists.
     *
     * @param <T> element type
    </T> */
    abstract class AbstractFlatList<T> : AbstractImmutableList<T>(), RandomAccess {
        @Override
        protected fun toList(): List<T> {
            return Arrays.asList(toArray() as Array<T>?)
        }

        /** Returns a list that consists of a this list's elements plus a given
         * element.  */
        abstract fun append(e: T): List<T>
    }

    /**
     * List that stores its one elements in the one members of the class.
     * Unlike [java.util.ArrayList] or
     * [java.util.Arrays.asList] there is
     * no array, only one piece of memory allocated, therefore is very compact
     * and cache and CPU efficient.
     *
     *
     * The list is read-only and cannot be modified or re-sized.
     * The element may be null.
     *
     *
     * The list is created via [FlatLists.of].
     *
     * @param <T> Element type
    </T> */
    protected class Flat1List<T> internal constructor(t0: T) : AbstractFlatList<T>(), ComparableList<T> {
        private val t0: T?

        init {
            this.t0 = t0
        }

        @Override
        override fun toString(): String {
            return "[$t0]"
        }

        @Override
        override fun get(index: Int): T? {
            return when (index) {
                0 -> t0
                else -> throw IndexOutOfBoundsException("index $index")
            }
        }

        @Override
        fun size(): Int {
            return 1
        }

        @Override
        override fun iterator(): Iterator<T> {
            return Collections.singletonList(t0).iterator()
        }

        @Override
        override fun equals(@Nullable o: Object): Boolean {
            if (this === o) {
                return true
            }
            if (o is Flat1List<*>) {
                val that = o as Flat1List<*>
                return Objects.equals(t0, that.t0)
            }
            return (o is List
                    && (o as List).size() === 1 && Objects.equals(t0, o[0]))
        }

        @Override
        override fun hashCode(): Int {
            var h = 1
            h = h * 31 + Utilities.hash(t0)
            return h
        }

        @Override
        override fun indexOf(@Nullable o: Object?): Int {
            if (o == null) {
                if (t0 == null) {
                    return 0
                }
            } else {
                if (o.equals(t0)) {
                    return 0
                }
            }
            return -1
        }

        @Override
        override fun lastIndexOf(@Nullable o: Object?): Int {
            if (o == null) {
                if (t0 == null) {
                    return 0
                }
            } else {
                if (o.equals(t0)) {
                    return 0
                }
            }
            return -1
        }

        @SuppressWarnings(["unchecked"])
        @Override
        fun <T2> toArray(a: @Nullable Array<T2?>?): Array<T2?>? {
            if (castNonNull(a).length < 1) {
                // Make a new array of a's runtime type, but my contents:
                return Arrays.copyOf(toArray(), 1, a.getClass())
            }
            a!![0] = t0 as T2?
            return a
        }

        @Override
        @PolyNull
        fun toArray(): Array<Object> {
            return arrayOf<Object>(castNonNull(t0))
        }

        @Override
        override fun compareTo(o: List?): Int {
            return ComparableListImpl.Companion.compare<Comparable<T>>(this as List, o)
        }

        @Override
        override fun append(e: T): List<T?> {
            return Flat2List(t0, e)
        }
    }

    /**
     * List that stores its two elements in the two members of the class.
     * Unlike [java.util.ArrayList] or
     * [java.util.Arrays.asList] there is
     * no array, only one piece of memory allocated, therefore is very compact
     * and cache and CPU efficient.
     *
     *
     * The list is read-only and cannot be modified or re-sized.
     * The elements may be null.
     *
     *
     * The list is created via [FlatLists.of].
     *
     * @param <T> Element type
    </T> */
    protected class Flat2List<T> internal constructor(t0: T, t1: T) : AbstractFlatList<T>(), ComparableList<T> {
        private val t0: T?
        private val t1: T?

        init {
            this.t0 = t0
            this.t1 = t1
        }

        @Override
        override fun toString(): String {
            return "[$t0, $t1]"
        }

        @Override
        override fun get(index: Int): T? {
            return when (index) {
                0 -> t0
                1 -> t1
                else -> throw IndexOutOfBoundsException("index $index")
            }
        }

        @Override
        fun size(): Int {
            return 2
        }

        @Override
        override fun iterator(): Iterator<T> {
            return Arrays.asList(t0, t1).iterator()
        }

        @Override
        override fun equals(@Nullable o: Object): Boolean {
            if (this === o) {
                return true
            }
            if (o is Flat2List<*>) {
                val that = o as Flat2List<*>
                return (Objects.equals(t0, that.t0)
                        && Objects.equals(t1, that.t1))
            }
            if (o is List) {
                return o.size() === 2 && o.equals(this)
            }
            return false
        }

        @Override
        override fun hashCode(): Int {
            var h = 1
            h = h * 31 + Utilities.hash(t0)
            h = h * 31 + Utilities.hash(t1)
            return h
        }

        @Override
        override fun indexOf(@Nullable o: Object?): Int {
            if (o == null) {
                if (t0 == null) {
                    return 0
                }
                if (t1 == null) {
                    return 1
                }
            } else {
                if (o.equals(t0)) {
                    return 0
                }
                if (o.equals(t1)) {
                    return 1
                }
            }
            return -1
        }

        @Override
        override fun lastIndexOf(@Nullable o: Object?): Int {
            if (o == null) {
                if (t1 == null) {
                    return 1
                }
                if (t0 == null) {
                    return 0
                }
            } else {
                if (o.equals(t1)) {
                    return 1
                }
                if (o.equals(t0)) {
                    return 0
                }
            }
            return -1
        }

        @SuppressWarnings(["unchecked"])
        @Override
        fun <T2> toArray(a: @Nullable Array<T2?>?): Array<T2?>? {
            if (castNonNull(a).length < 2) {
                // Make a new array of a's runtime type, but my contents:
                return Arrays.copyOf(toArray(), 2, a.getClass())
            }
            a!![0] = t0 as T2?
            a[1] = t1 as T2?
            return a
        }

        @Override
        @PolyNull
        fun toArray(): Array<Object> {
            return arrayOf<Object>(castNonNull(t0), castNonNull(t1))
        }

        @Override
        override fun compareTo(o: List?): Int {
            return ComparableListImpl.Companion.compare<Comparable<T>>(this as List, o)
        }

        @Override
        override fun append(e: T): List<T?> {
            return Flat3List(t0, t1, e)
        }
    }

    /**
     * List that stores its three elements in the three members of the class.
     * Unlike [java.util.ArrayList] or
     * [java.util.Arrays.asList] there is
     * no array, only one piece of memory allocated, therefore is very compact
     * and cache and CPU efficient.
     *
     *
     * The list is read-only, cannot be modified or re-sized.
     * The elements may be null.
     *
     *
     * The list is created via [FlatLists.of].
     *
     * @param <T> Element type
    </T> */
    protected class Flat3List<T> internal constructor(t0: T, t1: T, t2: T) : AbstractFlatList<T>(), ComparableList<T> {
        private val t0: T?
        private val t1: T?
        private val t2: T?

        init {
            this.t0 = t0
            this.t1 = t1
            this.t2 = t2
        }

        @Override
        override fun toString(): String {
            return "[$t0, $t1, $t2]"
        }

        @Override
        override fun get(index: Int): T? {
            return when (index) {
                0 -> t0
                1 -> t1
                2 -> t2
                else -> throw IndexOutOfBoundsException("index $index")
            }
        }

        @Override
        fun size(): Int {
            return 3
        }

        @Override
        override fun iterator(): Iterator<T> {
            return Arrays.asList(t0, t1, t2).iterator()
        }

        @Override
        override fun equals(@Nullable o: Object): Boolean {
            if (this === o) {
                return true
            }
            if (o is Flat3List<*>) {
                val that = o as Flat3List<*>
                return (Objects.equals(t0, that.t0)
                        && Objects.equals(t1, that.t1)
                        && Objects.equals(t2, that.t2))
            }
            return (o is List
                    && (o as List).size() === 3 && Arrays.asList(t0, t1, t2).equals(o))
        }

        @Override
        override fun hashCode(): Int {
            var h = 1
            h = h * 31 + Utilities.hash(t0)
            h = h * 31 + Utilities.hash(t1)
            h = h * 31 + Utilities.hash(t2)
            return h
        }

        @Override
        override fun indexOf(@Nullable o: Object?): Int {
            if (o == null) {
                if (t0 == null) {
                    return 0
                }
                if (t1 == null) {
                    return 1
                }
                if (t2 == null) {
                    return 2
                }
            } else {
                if (o.equals(t0)) {
                    return 0
                }
                if (o.equals(t1)) {
                    return 1
                }
                if (o.equals(t2)) {
                    return 2
                }
            }
            return -1
        }

        @Override
        override fun lastIndexOf(@Nullable o: Object?): Int {
            if (o == null) {
                if (t2 == null) {
                    return 2
                }
                if (t1 == null) {
                    return 1
                }
                if (t0 == null) {
                    return 0
                }
            } else {
                if (o.equals(t2)) {
                    return 2
                }
                if (o.equals(t1)) {
                    return 1
                }
                if (o.equals(t0)) {
                    return 0
                }
            }
            return -1
        }

        @SuppressWarnings(["unchecked"])
        @Override
        fun <T2> toArray(a: @Nullable Array<T2?>?): Array<T2?>? {
            if (castNonNull(a).length < 3) {
                // Make a new array of a's runtime type, but my contents:
                return Arrays.copyOf(toArray(), 3, a.getClass())
            }
            a!![0] = t0 as T2?
            a[1] = t1 as T2?
            a[2] = t2 as T2?
            return a
        }

        @Override
        @PolyNull
        fun toArray(): Array<Object> {
            return arrayOf<Object>(castNonNull(t0), castNonNull(t1), castNonNull(t2))
        }

        @Override
        override fun compareTo(o: List?): Int {
            return ComparableListImpl.Companion.compare<Comparable<T>>(this as List, o)
        }

        @Override
        override fun append(e: T): List<T?> {
            return Flat4List(t0, t1, t2, e)
        }
    }

    /**
     * List that stores its four elements in the four members of the class.
     * Unlike [java.util.ArrayList] or
     * [java.util.Arrays.asList] there is
     * no array, only one piece of memory allocated, therefore is very compact
     * and cache and CPU efficient.
     *
     *
     * The list is read-only, cannot be modified or re-sized.
     * The elements may be null.
     *
     *
     * The list is created via [FlatLists.of].
     *
     * @param <T> Element type
    </T> */
    protected class Flat4List<T> internal constructor(t0: T, t1: T, t2: T, t3: T) : AbstractFlatList<T>(),
        ComparableList<T> {
        private val t0: T?
        private val t1: T?
        private val t2: T?
        private val t3: T?

        init {
            this.t0 = t0
            this.t1 = t1
            this.t2 = t2
            this.t3 = t3
        }

        @Override
        override fun toString(): String {
            return "[$t0, $t1, $t2, $t3]"
        }

        @Override
        override fun get(index: Int): T? {
            return when (index) {
                0 -> t0
                1 -> t1
                2 -> t2
                3 -> t3
                else -> throw IndexOutOfBoundsException("index $index")
            }
        }

        @Override
        fun size(): Int {
            return 4
        }

        @Override
        override fun iterator(): Iterator<T> {
            return Arrays.asList(t0, t1, t2, t3).iterator()
        }

        @Override
        override fun equals(@Nullable o: Object): Boolean {
            if (this === o) {
                return true
            }
            if (o is Flat4List<*>) {
                val that = o as Flat4List<*>
                return (Objects.equals(t0, that.t0)
                        && Objects.equals(t1, that.t1)
                        && Objects.equals(t2, that.t2)
                        && Objects.equals(t3, that.t3))
            }
            return (o is List
                    && (o as List).size() === 4 && Arrays.asList(t0, t1, t2, t3).equals(o))
        }

        @Override
        override fun hashCode(): Int {
            var h = 1
            h = h * 31 + Utilities.hash(t0)
            h = h * 31 + Utilities.hash(t1)
            h = h * 31 + Utilities.hash(t2)
            h = h * 31 + Utilities.hash(t3)
            return h
        }

        @Override
        override fun indexOf(@Nullable o: Object?): Int {
            if (o == null) {
                if (t0 == null) {
                    return 0
                }
                if (t1 == null) {
                    return 1
                }
                if (t2 == null) {
                    return 2
                }
                if (t3 == null) {
                    return 3
                }
            } else {
                if (o.equals(t0)) {
                    return 0
                }
                if (o.equals(t1)) {
                    return 1
                }
                if (o.equals(t2)) {
                    return 2
                }
                if (o.equals(t3)) {
                    return 3
                }
            }
            return -1
        }

        @Override
        override fun lastIndexOf(@Nullable o: Object?): Int {
            if (o == null) {
                if (t3 == null) {
                    return 3
                }
                if (t2 == null) {
                    return 2
                }
                if (t1 == null) {
                    return 1
                }
                if (t0 == null) {
                    return 0
                }
            } else {
                if (o.equals(t3)) {
                    return 3
                }
                if (o.equals(t2)) {
                    return 2
                }
                if (o.equals(t1)) {
                    return 1
                }
                if (o.equals(t0)) {
                    return 0
                }
            }
            return -1
        }

        @SuppressWarnings(["unchecked"])
        @Override
        fun <T2> toArray(a: @Nullable Array<T2?>?): Array<T2?>? {
            if (castNonNull(a).length < 4) {
                // Make a new array of a's runtime type, but my contents:
                return Arrays.copyOf(toArray(), 4, a.getClass())
            }
            a!![0] = t0 as T2?
            a[1] = t1 as T2?
            a[2] = t2 as T2?
            a[3] = t3 as T2?
            return a
        }

        @Override
        @PolyNull
        fun toArray(): Array<Object> {
            return arrayOf<Object>(
                castNonNull(t0), castNonNull(t1), castNonNull(t2),
                castNonNull(t3)
            )
        }

        @Override
        override fun compareTo(o: List?): Int {
            return ComparableListImpl.Companion.compare<Comparable<T>>(this as List, o)
        }

        @Override
        override fun append(e: T): List<T?> {
            return Flat5List(t0, t1, t2, t3, e)
        }
    }

    /**
     * List that stores its five elements in the five members of the class.
     * Unlike [java.util.ArrayList] or
     * [java.util.Arrays.asList] there is
     * no array, only one piece of memory allocated, therefore is very compact
     * and cache and CPU efficient.
     *
     *
     * The list is read-only, cannot be modified or re-sized.
     * The elements may be null.
     *
     *
     * The list is created via [FlatLists.of].
     *
     * @param <T> Element type
    </T> */
    protected class Flat5List<T> internal constructor(t0: T, t1: T, t2: T, t3: T, t4: T) : AbstractFlatList<T>(),
        ComparableList<T> {
        private val t0: T?
        private val t1: T?
        private val t2: T?
        private val t3: T?
        private val t4: T?

        init {
            this.t0 = t0
            this.t1 = t1
            this.t2 = t2
            this.t3 = t3
            this.t4 = t4
        }

        @Override
        override fun toString(): String {
            return "[$t0, $t1, $t2, $t3, $t4]"
        }

        @Override
        override fun get(index: Int): T? {
            return when (index) {
                0 -> t0
                1 -> t1
                2 -> t2
                3 -> t3
                4 -> t4
                else -> throw IndexOutOfBoundsException("index $index")
            }
        }

        @Override
        fun size(): Int {
            return 5
        }

        @Override
        override fun iterator(): Iterator<T> {
            return Arrays.asList(t0, t1, t2, t3, t4).iterator()
        }

        @Override
        override fun equals(@Nullable o: Object): Boolean {
            if (this === o) {
                return true
            }
            if (o is Flat5List<*>) {
                val that = o as Flat5List<*>
                return (Objects.equals(t0, that.t0)
                        && Objects.equals(t1, that.t1)
                        && Objects.equals(t2, that.t2)
                        && Objects.equals(t3, that.t3)
                        && Objects.equals(t4, that.t4))
            }
            return (o is List
                    && (o as List).size() === 5 && Arrays.asList(t0, t1, t2, t3, t4).equals(o))
        }

        @Override
        override fun hashCode(): Int {
            var h = 1
            h = h * 31 + Utilities.hash(t0)
            h = h * 31 + Utilities.hash(t1)
            h = h * 31 + Utilities.hash(t2)
            h = h * 31 + Utilities.hash(t3)
            h = h * 31 + Utilities.hash(t4)
            return h
        }

        @Override
        override fun indexOf(@Nullable o: Object?): Int {
            if (o == null) {
                if (t0 == null) {
                    return 0
                }
                if (t1 == null) {
                    return 1
                }
                if (t2 == null) {
                    return 2
                }
                if (t3 == null) {
                    return 3
                }
                if (t4 == null) {
                    return 4
                }
            } else {
                if (o.equals(t0)) {
                    return 0
                }
                if (o.equals(t1)) {
                    return 1
                }
                if (o.equals(t2)) {
                    return 2
                }
                if (o.equals(t3)) {
                    return 3
                }
                if (o.equals(t4)) {
                    return 4
                }
            }
            return -1
        }

        @Override
        override fun lastIndexOf(@Nullable o: Object?): Int {
            if (o == null) {
                if (t4 == null) {
                    return 4
                }
                if (t3 == null) {
                    return 3
                }
                if (t2 == null) {
                    return 2
                }
                if (t1 == null) {
                    return 1
                }
                if (t0 == null) {
                    return 0
                }
            } else {
                if (o.equals(t4)) {
                    return 4
                }
                if (o.equals(t3)) {
                    return 3
                }
                if (o.equals(t2)) {
                    return 2
                }
                if (o.equals(t1)) {
                    return 1
                }
                if (o.equals(t0)) {
                    return 0
                }
            }
            return -1
        }

        @SuppressWarnings(["unchecked"])
        @Override
        fun <T2> toArray(a: @Nullable Array<T2?>?): Array<T2?>? {
            if (castNonNull(a).length < 5) {
                // Make a new array of a's runtime type, but my contents:
                return Arrays.copyOf(toArray(), 5, a.getClass())
            }
            a!![0] = t0 as T2?
            a[1] = t1 as T2?
            a[2] = t2 as T2?
            a[3] = t3 as T2?
            a[4] = t4 as T2?
            return a
        }

        @Override
        @PolyNull
        fun toArray(): Array<Object> {
            return arrayOf<Object>(
                castNonNull(t0), castNonNull(t1), castNonNull(t2),
                castNonNull(t3), castNonNull(t4)
            )
        }

        @Override
        override fun compareTo(o: List?): Int {
            return ComparableListImpl.Companion.compare<Comparable<T>>(this as List, o)
        }

        @Override
        override fun append(e: T): List<T?> {
            return Flat6List(t0, t1, t2, t3, t4, e)
        }
    }

    /**
     * List that stores its six elements in the six members of the class.
     * Unlike [java.util.ArrayList] or
     * [java.util.Arrays.asList] there is
     * no array, only one piece of memory allocated, therefore is very compact
     * and cache and CPU efficient.
     *
     *
     * The list is read-only, cannot be modified or re-sized.
     * The elements may be null.
     *
     *
     * The list is created via [FlatLists.of].
     *
     * @param <T> Element type
    </T> */
    protected class Flat6List<T> internal constructor(t0: T, t1: T, t2: T, t3: T, t4: T, t5: T) : AbstractFlatList<T>(),
        ComparableList<T> {
        private val t0: T?
        private val t1: T?
        private val t2: T?
        private val t3: T?
        private val t4: T?
        private val t5: T?

        init {
            this.t0 = t0
            this.t1 = t1
            this.t2 = t2
            this.t3 = t3
            this.t4 = t4
            this.t5 = t5
        }

        @Override
        override fun toString(): String {
            return ("[" + t0 + ", " + t1 + ", " + t2 + ", " + t3 + ", " + t4
                    + ", " + t5 + "]")
        }

        @Override
        override fun get(index: Int): T? {
            return when (index) {
                0 -> t0
                1 -> t1
                2 -> t2
                3 -> t3
                4 -> t4
                5 -> t5
                else -> throw IndexOutOfBoundsException("index $index")
            }
        }

        @Override
        fun size(): Int {
            return 6
        }

        @Override
        override fun iterator(): Iterator<T> {
            return Arrays.asList(t0, t1, t2, t3, t4, t5).iterator()
        }

        @Override
        override fun equals(@Nullable o: Object): Boolean {
            if (this === o) {
                return true
            }
            if (o is Flat6List<*>) {
                val that = o as Flat6List<*>
                return (Objects.equals(t0, that.t0)
                        && Objects.equals(t1, that.t1)
                        && Objects.equals(t2, that.t2)
                        && Objects.equals(t3, that.t3)
                        && Objects.equals(t4, that.t4)
                        && Objects.equals(t5, that.t5))
            }
            return (o is List
                    && (o as List).size() === 6 && Arrays.asList(t0, t1, t2, t3, t4, t5).equals(o))
        }

        @Override
        override fun hashCode(): Int {
            var h = 1
            h = h * 31 + Utilities.hash(t0)
            h = h * 31 + Utilities.hash(t1)
            h = h * 31 + Utilities.hash(t2)
            h = h * 31 + Utilities.hash(t3)
            h = h * 31 + Utilities.hash(t4)
            h = h * 31 + Utilities.hash(t5)
            return h
        }

        @Override
        override fun indexOf(@Nullable o: Object?): Int {
            if (o == null) {
                if (t0 == null) {
                    return 0
                }
                if (t1 == null) {
                    return 1
                }
                if (t2 == null) {
                    return 2
                }
                if (t3 == null) {
                    return 3
                }
                if (t4 == null) {
                    return 4
                }
                if (t5 == null) {
                    return 5
                }
            } else {
                if (o.equals(t0)) {
                    return 0
                }
                if (o.equals(t1)) {
                    return 1
                }
                if (o.equals(t2)) {
                    return 2
                }
                if (o.equals(t3)) {
                    return 3
                }
                if (o.equals(t4)) {
                    return 4
                }
                if (o.equals(t5)) {
                    return 5
                }
            }
            return -1
        }

        @Override
        override fun lastIndexOf(@Nullable o: Object?): Int {
            if (o == null) {
                if (t5 == null) {
                    return 5
                }
                if (t4 == null) {
                    return 4
                }
                if (t3 == null) {
                    return 3
                }
                if (t2 == null) {
                    return 2
                }
                if (t1 == null) {
                    return 1
                }
                if (t0 == null) {
                    return 0
                }
            } else {
                if (o.equals(t5)) {
                    return 5
                }
                if (o.equals(t4)) {
                    return 4
                }
                if (o.equals(t3)) {
                    return 3
                }
                if (o.equals(t2)) {
                    return 2
                }
                if (o.equals(t1)) {
                    return 1
                }
                if (o.equals(t0)) {
                    return 0
                }
            }
            return -1
        }

        @SuppressWarnings(["unchecked"])
        @Override
        fun <T2> toArray(a: @Nullable Array<T2?>?): Array<T2?>? {
            if (castNonNull(a).length < 6) {
                // Make a new array of a's runtime type, but my contents:
                return Arrays.copyOf(toArray(), 6, a.getClass())
            }
            a!![0] = t0 as T2?
            a[1] = t1 as T2?
            a[2] = t2 as T2?
            a[3] = t3 as T2?
            a[4] = t4 as T2?
            a[5] = t5 as T2?
            return a
        }

        @Override
        @PolyNull
        fun toArray(): Array<Object> {
            return arrayOf<Object>(
                castNonNull(t0), castNonNull(t1), castNonNull(t2),
                castNonNull(t3), castNonNull(t4), castNonNull(t5)
            )
        }

        @Override
        override fun compareTo(o: List?): Int {
            return ComparableListImpl.Companion.compare<Comparable<T>>(this as List, o)
        }

        @Override
        override fun append(e: T): List<T> {
            return ImmutableNullableList.of(t0, t1, t2, t3, t5, e)
        }
    }

    /** Empty list that implements the [Comparable] interface.
     *
     * @param <T> element type
    </T> */
    class ComparableEmptyList<T> : AbstractList<T>(), ComparableList<T> {
        @Override
        override fun get(index: Int): T {
            throw IndexOutOfBoundsException()
        }

        @Override
        override fun hashCode(): Int {
            return 1 // same as Collections.emptyList()
        }

        @Override
        override fun equals(@Nullable o: Object): Boolean {
            return (o === this
                    || o is List && (o as List).isEmpty())
        }

        @Override
        fun size(): Int {
            return 0
        }

        @Override
        override fun compareTo(o: List?): Int {
            return ComparableListImpl.Companion.compare<Comparable<T>>(this as List, o)
        }
    }

    /** List that is also comparable.
     *
     *
     * You can create an instance whose type
     * parameter `T` does not extend [Comparable], but you will get a
     * [ClassCastException] at runtime when you call
     * [.compareTo] if the elements of the list do not implement
     * `Comparable`.
     *
     * @param <T> element type
    </T> */
    @SuppressWarnings("ComparableType")
    interface ComparableList<T> : List<T>, Comparable<List?>

    /** Wrapper around a list that makes it implement the [Comparable]
     * interface using lexical ordering. The elements must be comparable.
     *
     * @param <T> element type
    </T> */
    internal class ComparableListImpl<T : Comparable<T>?>(private val list: List<T>) : AbstractList<T>(),
        ComparableList<T> {
        @Override
        override fun get(index: Int): T {
            return list[index]
        }

        @Override
        fun size(): Int {
            return list.size()
        }

        @Override
        override fun compareTo(o: List): Int {
            return compare<Comparable<T>>(list, o)
        }

        companion object {
            fun <T : Comparable<T>?> compare(list0: List<T>, list1: List<T>): Int {
                val size0: Int = list0.size()
                val size1: Int = list1.size()
                if (size1 == size0) {
                    return compare<Comparable<T>>(list0, list1, size0)
                }
                val c = compare(list0, list1, Math.min(size0, size1))
                return if (c != 0) {
                    c
                } else size0 - size1
            }

            fun <T : Comparable<T>?> compare(
                list0: List<T>, list1: List<T>,
                size: Int
            ): Int {
                for (i in 0 until size) {
                    val o0: Comparable = list0[i]
                    val o1: Comparable = list1[i]
                    val c = compare<Comparable<T>>(o0, o1)
                    if (c != 0) {
                        return c
                    }
                }
                return 0
            }

            fun <T : Comparable<T>?> compare(a: T, b: T?): Int {
                if (a === b) {
                    return 0
                }
                if (a == null) {
                    return -1
                }
                return if (b == null) {
                    1
                } else a.compareTo(b)
            }
        }
    }
}
