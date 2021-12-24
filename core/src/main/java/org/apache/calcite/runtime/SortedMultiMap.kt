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

import java.util.ArrayList

/**
 * Map that allows you to partition values into lists according to a common
 * key, and then convert those lists into an iterator of sorted arrays.
 *
 * @param <K> Key type
 * @param <V> Value type
</V></K> */
class SortedMultiMap<K, V> : HashMap<K, List<V>?>() {
    fun putMulti(key: K, value: V) {
        var list: List<V>? = put(key, Collections.singletonList(value)) ?: return
        if (list.size() === 1) {
            list = ArrayList(list)
        }
        list.add(value)
        put(key, list)
    }

    fun arrays(comparator: Comparator<V>?): Iterator<Array<V>> {
        val iterator: Iterator<List<V>> = values().iterator()
        return object : Iterator<Array<V>?>() {
            @Override
            override fun hasNext(): Boolean {
                return iterator.hasNext()
            }

            @Override
            override fun next(): Array<V> {
                val list = iterator.next()
                @SuppressWarnings("unchecked") val vs = list.toArray() as Array<V>
                Arrays.sort(vs, comparator)
                return vs
            }

            @Override
            fun remove() {
                throw UnsupportedOperationException()
            }
        }
    }

    companion object {
        /** Shortcut method if the partition key is empty. We know that we would end
         * up with a map with just one entry, so save ourselves the trouble of all
         * that hashing.  */
        fun <V> singletonArrayIterator(
            comparator: Comparator<V>, list: List<V>?
        ): Iterator<Array<V>> {
            val multiMap: SortedMultiMap<Object, V> = SortedMultiMap<Any, Any>()
            multiMap.put("x", list)
            return multiMap.arrays(comparator)
        }
    }
}
