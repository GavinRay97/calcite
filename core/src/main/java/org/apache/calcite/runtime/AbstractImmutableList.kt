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

import java.util.Collection
import java.util.Iterator
import java.util.List
import java.util.ListIterator
import org.apache.calcite.linq4j.Nullness.castNonNull

/**
 * Base class for lists whose contents are constant after creation.
 *
 * @param <E> Element type
</E> */
internal abstract class AbstractImmutableList<E> : List<E> {
    protected abstract fun toList(): List<E>

    @Override
    override fun iterator(): Iterator<E> {
        return toList().iterator()
    }

    @Override
    override fun listIterator(): ListIterator<E> {
        return toList().listIterator()
    }

    @Override
    override fun isEmpty(): Boolean {
        return false
    }

    @Override
    fun add(t: E): Boolean {
        throw UnsupportedOperationException()
    }

    @Override
    fun addAll(c: Collection<E>?): Boolean {
        throw UnsupportedOperationException()
    }

    @Override
    fun addAll(index: Int, c: Collection<E>?): Boolean {
        throw UnsupportedOperationException()
    }

    @Override
    fun removeAll(c: Collection<*>?): Boolean {
        throw UnsupportedOperationException()
    }

    @Override
    fun retainAll(c: Collection<*>?): Boolean {
        throw UnsupportedOperationException()
    }

    @Override
    fun clear() {
        throw UnsupportedOperationException()
    }

    @Override
    operator fun set(index: Int, element: E): E {
        throw UnsupportedOperationException()
    }

    @Override
    fun add(index: Int, element: E) {
        throw UnsupportedOperationException()
    }

    @Override
    fun remove(index: Int): E {
        throw UnsupportedOperationException()
    }

    @Override
    override fun listIterator(index: Int): ListIterator<E> {
        return toList().listIterator(index)
    }

    @Override
    override fun subList(fromIndex: Int, toIndex: Int): List<E> {
        return toList().subList(fromIndex, toIndex)
    }

    @Override
    override fun contains(@Nullable o: Object?): Boolean {
        return indexOf(castNonNull(o)) >= 0
    }

    @Override
    fun containsAll(c: Collection<*>): Boolean {
        for (o in c) {
            if (!contains(o)) {
                return false
            }
        }
        return true
    }

    @Override
    fun remove(@Nullable o: Object?): Boolean {
        throw UnsupportedOperationException()
    }
}
