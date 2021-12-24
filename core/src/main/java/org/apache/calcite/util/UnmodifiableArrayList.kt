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
 * A view onto an array that cannot be modified by the client.
 *
 *
 * Since the array is not copied, modifications to the array will be
 * reflected in the list.
 *
 *
 * Null elements are allowed.
 *
 *
 * Quick and low-memory, like [java.util.Arrays.asList], but
 * unmodifiable.
 *
 * @param <E> Element type
</E> */
class UnmodifiableArrayList<E> private constructor(elements: Array<E>) : AbstractList<E>(), RandomAccess {
    private val elements: Array<E>

    init {
        this.elements = Objects.requireNonNull(elements, "elements")
    }

    @Override
    operator fun get(index: Int): E {
        return elements[index]
    }

    @Override
    fun size(): Int {
        return elements.size
    }

    companion object {
        fun <E> of(vararg elements: E): UnmodifiableArrayList<E> {
            return UnmodifiableArrayList<E>(elements)
        }
    }
}
