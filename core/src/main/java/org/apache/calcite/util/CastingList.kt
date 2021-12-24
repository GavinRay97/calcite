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

import java.util.AbstractList

/**
 * Converts a list whose members are automatically down-cast to a given type.
 *
 *
 * If a member of the backing list is not an instanceof `E`, the
 * accessing method (such as [List.get]) will throw a
 * [ClassCastException].
 *
 *
 * All modifications are automatically written to the backing list. Not
 * synchronized.
 *
 * @param <E> Element type
</E> */
class CastingList<E> protected constructor(list: List<in E>, clazz: Class<E>) : AbstractList<E>(), List<E> {
    //~ Instance fields --------------------------------------------------------
    private val list: List<in E>
    private val clazz: Class<E>

    //~ Constructors -----------------------------------------------------------
    init {
        this.list = list
        this.clazz = clazz
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun get(index: Int): E {
        val o: Object = list[index]
        return clazz.cast(castNonNull(o))
    }

    @Override
    fun size(): Int {
        return list.size()
    }

    @Override
    operator fun set(index: Int, element: E): E {
        val o: Object = list.set(index, element)
        return clazz.cast(castNonNull(o))
    }

    @Override
    fun remove(index: Int): E {
        val o: Object = list.remove(index)
        return clazz.cast(castNonNull(o))
    }

    @Override
    fun add(pos: Int, o: E) {
        list.add(pos, o)
    }
}
