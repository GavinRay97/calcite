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

import com.google.common.collect.ImmutableList

/**
 * Read-only list that is the concatenation of sub-lists.
 *
 *
 * The list is read-only; attempts to call methods such as
 * [.add] or [.set] will throw.
 *
 *
 * Changes to the backing lists, including changes in length, will be
 * reflected in this list.
 *
 *
 * This class is not thread-safe. Changes to backing lists will cause
 * unspecified behavior.
 *
 * @param <T> Element type
</T> */
class CompositeList<T> private constructor(lists: ImmutableList<List<T>>) : AbstractList<T>() {
    private val lists: ImmutableList<List<T>>

    /**
     * Creates a CompositeList.
     *
     * @param lists Constituent lists
     */
    init {
        this.lists = lists
    }

    @Override
    operator fun get(index: Int): T {
        var index = index
        for (list in lists) {
            val nextIndex: Int = index - list.size()
            if (nextIndex < 0) {
                return list[index]
            }
            index = nextIndex
        }
        throw IndexOutOfBoundsException()
    }

    @Override
    fun size(): Int {
        var n = 0
        for (list in lists) {
            n += list.size()
        }
        return n
    }

    companion object {
        /**
         * Creates a CompositeList.
         *
         * @param lists Constituent lists
         * @param <T>   Element type
         * @return List consisting of all lists
        </T> */
        @SafeVarargs
        fun <T> of(vararg lists: List<T>?): CompositeList<T> {
            return CompositeList(ImmutableList.copyOf(lists) as ImmutableList)
        }

        /**
         * Creates a CompositeList.
         *
         * @param lists Constituent lists
         * @param <T>   Element type
         * @return List consisting of all lists
        </T> */
        fun <T> ofCopy(lists: Iterable<List<T>?>?): CompositeList<T> {
            val list: ImmutableList<List<T>> = ImmutableList.copyOf(lists)
            return CompositeList<Any>(list)
        }

        /**
         * Creates a CompositeList of zero lists.
         *
         * @param <T>   Element type
         * @return List consisting of all lists
        </T> */
        fun <T> of(): List<T> {
            return ImmutableList.of()
        }

        /**
         * Creates a CompositeList of one list.
         *
         * @param list0 List
         * @param <T>   Element type
         * @return List consisting of all lists
        </T> */
        fun <T> of(list0: List<T>): List<T> {
            return list0
        }

        /**
         * Creates a CompositeList of two lists.
         *
         * @param list0 First list
         * @param list1 Second list
         * @param <T>   Element type
         * @return List consisting of all lists
        </T> */
        fun <T> of(
            list0: List<T>?,
            list1: List<T>?
        ): CompositeList<T> {
            return CompositeList(ImmutableList.of(list0, list1) as ImmutableList)
        }

        /**
         * Creates a CompositeList of three lists.
         *
         * @param list0 First list
         * @param list1 Second list
         * @param list2 Third list
         * @param <T>   Element type
         * @return List consisting of all lists
        </T> */
        fun <T> of(
            list0: List<T>?,
            list1: List<T>?,
            list2: List<T>?
        ): CompositeList<T> {
            return CompositeList(ImmutableList.of(list0, list1, list2) as ImmutableList)
        }
    }
}
