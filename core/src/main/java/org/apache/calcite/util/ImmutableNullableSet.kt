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

import org.apache.calcite.rel.metadata.NullSentinel

/**
 * An immutable set that may contain null values.
 *
 *
 * If the set cannot contain null values, use [ImmutableSet].
 *
 *
 * We do not yet support sorted sets.
 *
 * @param <E> Element type
</E> */
class ImmutableNullableSet<E> private constructor(elements: ImmutableSet<Object>) : AbstractSet<E>() {
    private val elements: ImmutableSet<Object>

    init {
        this.elements = Objects.requireNonNull(elements, "elements")
    }

    @Override
    operator fun iterator(): Iterator<E> {
        return Util.transform(elements.iterator()) { e -> if (e === NullSentinel.INSTANCE) castNonNull(null) else e }
    }

    @Override
    fun size(): Int {
        return elements.size()
    }

    @Override
    operator fun contains(@Nullable o: Object?): Boolean {
        return elements.contains(if (o == null) NullSentinel.INSTANCE else o)
    }

    @Override
    fun remove(@Nullable o: Object?): Boolean {
        throw UnsupportedOperationException()
    }

    @Override
    fun removeAll(c: Collection<*>?): Boolean {
        throw UnsupportedOperationException()
    }

    /**
     * A builder for creating immutable nullable set instances.
     *
     * @param <E> element type
    </E> */
    class Builder<E>
    /**
     * Creates a new builder. The returned builder is equivalent to the builder
     * generated by
     * [ImmutableNullableSet.builder].
     */
    {
        private val contents: List<E> = ArrayList()

        /**
         * Adds `element` to the `ImmutableNullableSet`.
         *
         * @param element the element to add
         * @return this `Builder` object
         */
        fun add(element: E): Builder<E> {
            contents.add(element)
            return this
        }

        /**
         * Adds each element of `elements` to the
         * `ImmutableNullableSet`.
         *
         * @param elements the `Iterable` to add to the
         * `ImmutableNullableSet`
         * @return this `Builder` object
         * @throws NullPointerException if `elements` is null
         */
        fun addAll(elements: Iterable<E>?): Builder<E> {
            Iterables.addAll(contents, elements)
            return this
        }

        /**
         * Adds each element of `elements` to the
         * `ImmutableNullableSet`.
         *
         * @param elements the elements to add to the `ImmutableNullableSet`
         * @return this `Builder` object
         * @throws NullPointerException if `elements` is null
         */
        fun add(vararg elements: E): Builder<E> {
            for (element in elements) {
                add(element)
            }
            return this
        }

        /**
         * Adds each element of `elements` to the
         * `ImmutableNullableSet`.
         *
         * @param elements the elements to add to the `ImmutableNullableSet`
         * @return this `Builder` object
         * @throws NullPointerException if `elements` is null
         */
        fun addAll(elements: Iterator<E>?): Builder<E> {
            Iterators.addAll(contents, elements)
            return this
        }

        /**
         * Returns a newly-created `ImmutableNullableSet` based on the
         * contents of the `Builder`.
         */
        fun build(): Set<E> {
            return copyOf<Any>(contents)
        }
    }

    companion object {
        @SuppressWarnings("rawtypes")
        private val SINGLETON_NULL: Set = ImmutableNullableSet<Any?>(ImmutableSet.of(NullSentinel.INSTANCE))
        private val SINGLETON: Set<Integer> = Collections.singleton(0)

        /**
         * Returns an immutable set containing the given elements.
         *
         *
         * Behavior is as [ImmutableSet.copyOf]
         * except that this set allows nulls.
         */
        @SuppressWarnings(["unchecked", "StaticPseudoFunctionalStyleMethod"])
        fun <E> copyOf(elements: Iterable<E>): Set<E> {
            if (elements is ImmutableNullableSet<*>
                || elements is ImmutableSet
                || elements === Collections.emptySet() || elements === Collections.emptySortedSet() || elements === SINGLETON_NULL || elements.getClass() === org.apache.calcite.util.ImmutableNullableSet.Companion.SINGLETON.getClass()
            ) {
                return elements as Set<E>
            }
            val set: ImmutableSet<Object>
            set = if (elements is Collection) {
                val collection = elements
                when (collection.size()) {
                    0 -> return ImmutableSet.of()
                    1 -> {
                        val element: E = Iterables.getOnlyElement(collection)
                        return if (element == null) SINGLETON_NULL else ImmutableSet.of(
                            element
                        )
                    }
                    else -> ImmutableSet.copyOf(
                        Collections2.transform(collection) { e -> if (e == null) NullSentinel.INSTANCE else e })
                }
            } else {
                ImmutableSet.copyOf(
                    Util.transform(elements) { e -> if (e == null) NullSentinel.INSTANCE else e })
            }
            return if (set.contains(NullSentinel.INSTANCE)) {
                ImmutableNullableSet<Any>(set)
            } else {
                set
            }
        }

        /**
         * Returns an immutable set containing the given elements.
         *
         *
         * Behavior as
         * [ImmutableSet.copyOf]
         * except that this set allows nulls.
         */
        fun <E> copyOf(elements: Array<E>): Set<E> {
            return copyOf(elements, true)
        }

        private fun <E> copyOf(elements: Array<E>, needCopy: Boolean): Set<E> {
            // If there are no nulls, ImmutableSet is better.
            if (!containsNull(elements)) {
                return ImmutableSet.copyOf(elements)
            }
            @Nullable val objects: Array<Object?> =
                if (needCopy) Arrays.copyOf(elements, elements.size, Array<Object>::class.java) else elements
            for (i in objects.indices) {
                if (objects[i] == null) {
                    objects[i] = NullSentinel.INSTANCE
                }
            }
            @SuppressWarnings(["nullness", "NullableProblems"]) @NonNull val nonNullObjects: Array<Object?> = objects
            return ImmutableNullableSet<E>(ImmutableSet.copyOf(nonNullObjects))
        }

        private fun <E> containsNull(elements: Array<E>): Boolean {
            for (element in elements) {
                if (element == null) {
                    return true
                }
            }
            return false
        }

        /** Creates an immutable set of 1 element.  */
        fun <E> of(e1: E?): Set<E> {
            return if (e1 == null) SINGLETON_NULL else ImmutableSet.of(e1)
        }

        /** Creates an immutable set of 2 elements.  */
        @SuppressWarnings("unchecked")
        fun <E> of(e1: E, e2: E): Set<E> {
            return copyOf(arrayOf(e1, e2), false)
        }

        /** Creates an immutable set of 3 elements.  */
        @SuppressWarnings("unchecked")
        fun <E> of(e1: E, e2: E, e3: E): Set<E> {
            return copyOf(arrayOf(e1, e2, e3), false)
        }

        /** Creates an immutable set of 4 elements.  */
        @SuppressWarnings("unchecked")
        fun <E> of(e1: E, e2: E, e3: E, e4: E): Set<E> {
            return copyOf(arrayOf(e1, e2, e3, e4), false)
        }

        /** Creates an immutable set of 5 or more elements.  */
        @SuppressWarnings("unchecked")
        fun <E> of(e1: E, e2: E, e3: E, e4: E, e5: E, vararg others: E): Set<E?> {
            val elements = arrayOfNulls<Object>(5 + others.size) as Array<E?>
            elements[0] = e1
            elements[1] = e2
            elements[2] = e3
            elements[3] = e4
            elements[4] = e5
            System.arraycopy(others, 0, elements, 5, others.size)
            return copyOf(elements, false)
        }

        /**
         * Returns a new builder. The generated builder is equivalent to the builder
         * created by the [Builder] constructor.
         */
        fun <E> builder(): Builder<E> {
            return Builder()
        }
    }
}
