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

import org.apache.calcite.linq4j.function.Functions

/**
 * An immutable list of [Integer] values backed by an array of
 * `int`s.
 */
class ImmutableIntList  // Does not copy array. Must remain private.
private constructor(private vararg val ints: Int) : FlatLists.AbstractFlatList<Integer?>() {
    @Override
    override fun hashCode(): Int {
        return Arrays.hashCode(ints)
    }

    @SuppressWarnings("contracts.conditional.postcondition.not.satisfied")
    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return if (this === obj
            || obj is ImmutableIntList
        ) Arrays.equals(ints, (obj as ImmutableIntList).ints) else obj is List
                && obj.equals(this)
    }

    @Override
    override fun toString(): String {
        return Arrays.toString(ints)
    }

    @get:Override
    val isEmpty: Boolean
        get() = ints.size == 0

    @Override
    fun size(): Int {
        return ints.size
    }

    @Override
    fun toArray(): Array<Object?> {
        val objects: Array<Object?> = arrayOfNulls<Object>(ints.size)
        for (i in objects.indices) {
            objects[i] = ints[i]
        }
        return objects
    }

    @Override
    fun <T> toArray(a: @Nullable Array<T?>?): Array<T?>? {
        var a = a
        val size = ints.size
        if (castNonNull(a).length < size) {
            // Make a new array of a's runtime type, but my contents:
            a =
                if (a.getClass() === kotlin.Array<Object>::class.java) arrayOfNulls<Object>(size) as Array<T?>? else Array.newInstance(
                    requireNonNull(a.getClass().getComponentType()), size
                ) as Array<T>?
        }
        if (a.getClass() as Class === Array<Integer>::class.java) {
            for (i in 0 until size) {
                a!![i] = ints[i]
            }
        } else {
            System.arraycopy(toArray(), 0, a, 0, size)
        }
        if (a!!.size > size) {
            a[size] = castNonNull(null)
        }
        return a
    }

    /** Returns an array of `int`s with the same contents as this list.  */
    fun toIntArray(): IntArray {
        return ints.clone()
    }

    /** Returns an List of `Integer`.  */
    fun toIntegerList(): List<Integer> {
        val arrayList: ArrayList<Integer> = ArrayList(size())
        for (i in ints) {
            arrayList.add(i)
        }
        return arrayList
    }

    @Override
    operator fun get(index: Int): Integer {
        return ints[index]
    }

    fun getInt(index: Int): Int {
        return ints[index]
    }

    @Override
    operator fun iterator(): Iterator<Integer> {
        return listIterator()
    }

    @Override
    fun listIterator(): ListIterator<Integer> {
        return listIterator(0)
    }

    @Override
    fun listIterator(index: Int): ListIterator<Integer> {
        return object : AbstractIndexedListIterator<Integer?>(size(), index) {
            @Override
            override operator fun get(index: Int): Integer {
                return this@ImmutableIntList[index]
            }
        }
    }

    @Override
    fun indexOf(@Nullable o: Object): Int {
        return if (o is Integer) {
            indexOf(o as Integer as Int)
        } else -1
    }

    fun indexOf(seek: Int): Int {
        for (i in ints.indices) {
            if (ints[i] == seek) {
                return i
            }
        }
        return -1
    }

    @Override
    fun lastIndexOf(@Nullable o: Object): Int {
        return if (o is Integer) {
            lastIndexOf(o as Integer as Int)
        } else -1
    }

    fun lastIndexOf(seek: Int): Int {
        for (i in ints.indices.reversed()) {
            if (ints[i] == seek) {
                return i
            }
        }
        return -1
    }

    @Override
    fun append(e: Integer): ImmutableIntList {
        return append(e as Int)
    }

    /** Returns a copy of this list with one element added.  */
    fun append(element: Int): ImmutableIntList {
        if (ints.size == 0) {
            return of(element)
        }
        val newInts: IntArray = Arrays.copyOf(ints, ints.size + 1)
        newInts[ints.size] = element
        return ImmutableIntList(*newInts)
    }

    /** Returns a copy of this list with all of the given integers added.  */
    fun appendAll(list: Iterable<Integer?>?): ImmutableIntList {
        return if (list is Collection && list.isEmpty()) {
            this
        } else copyOf(Iterables.concat(this, list))
    }

    /**
     * Increments `offset` to each element of the list and
     * returns a new int list.
     */
    fun incr(offset: Int): ImmutableIntList {
        val integers = IntArray(ints.size)
        for (i in ints.indices) {
            integers[i] = ints[i] + offset
        }
        return ImmutableIntList(*integers)
    }

    /** Special sub-class of [ImmutableIntList] that is always
     * empty and has only one instance.  */
    private class EmptyImmutableIntList : ImmutableIntList() {
        @Override
        override fun toArray(): Array<Object?> {
            return EMPTY_ARRAY
        }

        @Override
        override fun <T> toArray(a: @Nullable Array<T>?): Array<T>? {
            if (castNonNull(a).length > 0) {
                a!![0] = castNonNull(null)
            }
            return a
        }

        @Override
        override fun iterator(): Iterator<Integer> {
            return Collections.< Integer > emptyList < Integer ? > ().iterator()
        }

        @Override
        override fun listIterator(): ListIterator<Integer> {
            return Collections.< Integer > emptyList < Integer ? > ().listIterator()
        }
    }

    /** Extension to [com.google.common.collect.UnmodifiableListIterator]
     * that operates by index.
     *
     * @param <E> element type
    </E> */
    private abstract class AbstractIndexedListIterator<E> protected constructor(size: Int, position: Int) :
        UnmodifiableListIterator<E>() {
        private val size: Int
        private var position: Int
        protected abstract operator fun get(index: Int): E

        init {
            Preconditions.checkPositionIndex(position, size)
            this.size = size
            this.position = position
        }

        @Override
        operator fun hasNext(): Boolean {
            return position < size
        }

        @Override
        operator fun next(): E {
            if (!hasNext()) {
                throw NoSuchElementException()
            }
            return get(position++)
        }

        @Override
        fun nextIndex(): Int {
            return position
        }

        @Override
        fun hasPrevious(): Boolean {
            return position > 0
        }

        @Override
        fun previous(): E {
            if (!hasPrevious()) {
                throw NoSuchElementException()
            }
            return get(--position)
        }

        @Override
        fun previousIndex(): Int {
            return position - 1
        }
    }

    companion object {
        private val EMPTY_ARRAY: Array<Object?> = arrayOfNulls<Object>(0)
        private val EMPTY: ImmutableIntList = EmptyImmutableIntList()

        /**
         * Returns an empty ImmutableIntList.
         */
        fun of(): ImmutableIntList {
            return EMPTY
        }

        /**
         * Creates an ImmutableIntList from an array of `int`.
         */
        fun of(vararg ints: Int): ImmutableIntList {
            return ImmutableIntList(*ints.clone())
        }

        /**
         * Creates an ImmutableIntList from an array of `Number`.
         */
        fun copyOf(vararg numbers: Number): ImmutableIntList {
            val ints = IntArray(numbers.size)
            for (i in ints.indices) {
                ints[i] = numbers[i].intValue()
            }
            return ImmutableIntList(*ints)
        }

        /**
         * Creates an ImmutableIntList from an iterable of [Number].
         */
        fun copyOf(list: Iterable<Number?>?): ImmutableIntList {
            if (list is ImmutableIntList) {
                return list as ImmutableIntList
            }
            @SuppressWarnings("unchecked") val collection = if (list is Collection) list else Lists.newArrayList(list)
            return copyFromCollection(collection)
        }

        /**
         * Creates an ImmutableIntList from an iterator of [Number].
         */
        fun copyOf(list: Iterator<Number?>?): ImmutableIntList {
            return copyFromCollection(Lists.newArrayList(list))
        }

        private fun copyFromCollection(
            list: Collection<Number?>
        ): ImmutableIntList {
            val ints = IntArray(list.size())
            var i = 0
            for (number in list) {
                ints[i++] = number.intValue()
            }
            return ImmutableIntList(*ints)
        }

        /** Returns a list that contains the values lower to upper - 1.
         *
         *
         * For example, `range(1, 3)` contains [1, 2].  */
        fun range(lower: Int, upper: Int): List<Integer> {
            return Functions.generate(upper - lower) { index -> lower + index }
        }

        /** Returns the identity list [0, ..., count - 1].
         *
         * @see Mappings.isIdentity
         */
        fun identity(count: Int): ImmutableIntList {
            val integers = IntArray(count)
            for (i in integers.indices) {
                integers[i] = i
            }
            return ImmutableIntList(*integers)
        }
    }
}
