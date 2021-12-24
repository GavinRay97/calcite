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

import com.google.common.collect.ImmutableList
import org.checkerframework.checker.nullness.qual.PolyNull
import java.util.ArrayList
import java.util.Arrays
import java.util.Iterator
import java.util.List
import java.util.ListIterator
import org.apache.calcite.linq4j.Nullness.castNonNull

/**
 * List that consists of a head element and an immutable non-empty list.
 *
 * @param <E> Element type
</E> */
class ConsList<E> private constructor(private val first: E, private val rest: List<E>) : AbstractImmutableList<E>() {
    @Override
    operator fun get(index: Int): E {
        var index = index
        var c = this
        while (true) {
            if (index == 0) {
                return c.first
            }
            --index
            if (c.rest !is ConsList<*>) {
                return c.rest[index]
            }
            c = c.rest as ConsList<E>
        }
    }

    @Override
    fun size(): Int {
        var s = 1
        var c: ConsList<*> = this
        while (true) {
            if (c.rest !is ConsList<*>) {
                return s + c.rest.size()
            }
            c = c.rest as ConsList<*>
            ++s
        }
    }

    @Override
    override fun hashCode(): Int {
        return toList().hashCode()
    }

    @Override
    override fun equals(@Nullable o: Object): Boolean {
        return (o === this
                || o is List
                && toList().equals(o))
    }

    @Override
    override fun toString(): String {
        return toList().toString()
    }

    @Override
    protected fun toList(): List<E> {
        val list: List<E> = ArrayList()
        var c = this
        while (true) {
            list.add(c.first)
            if (c.rest !is ConsList<*>) {
                list.addAll(c.rest)
                return list
            }
            c = c.rest as ConsList<E>
        }
    }

    @Override
    fun listIterator(): ListIterator<E> {
        return toList().listIterator()
    }

    @Override
    operator fun iterator(): Iterator<E> {
        return toList().iterator()
    }

    @Override
    fun listIterator(index: Int): ListIterator<E> {
        return toList().listIterator(index)
    }

    @Override
    @PolyNull
    fun toArray(): Array<Object> {
        return toList().toArray()
    }

    @Override
    fun <T> toArray(a: @Nullable Array<T>?): Array<T>? {
        var a = a
        val s = size()
        if (s > castNonNull(a).length) {
            a = Arrays.copyOf(a, s, a.getClass())
        } else if (s < a!!.size) {
            a[s] = castNonNull(null)
        }
        var i = 0
        var c: ConsList<*> = this
        while (true) {
            a!![i++] = c.first as T
            if (c.rest !is ConsList<*>) {
                val a2: Array<Object> = c.rest.toArray()
                System.arraycopy(a2, 0, a, i, a2.size)
                return a
            }
            c = c.rest as ConsList<*>
        }
    }

    @Override
    fun indexOf(@Nullable o: Object): Int {
        return toList().indexOf(o)
    }

    @Override
    fun lastIndexOf(@Nullable o: Object): Int {
        return toList().lastIndexOf(o)
    }

    companion object {
        /** Creates a ConsList.
         * It consists of an element pre-pended to another list.
         * If the other list is mutable, creates an immutable copy.  */
        fun <E> of(first: E, rest: List<E>): List<E> {
            return if (rest is ConsList<*>
                || rest is ImmutableList
                && !rest.isEmpty()
            ) {
                ConsList(first, rest)
            } else {
                ImmutableList.< E > builder < E ? > ().add(first).addAll(rest).build()
            }
        }
    }
}
