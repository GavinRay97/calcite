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

import java.util.Iterator

/**
 * Filtered iterator class: an iterator that includes only elements that are
 * instanceof a specified class.
 *
 *
 * Apologies for the dorky name.
 *
 * @see Util.cast
 * @see Util.cast
 * @param <E> Element type
</E> */
class Filterator<E : Object?>(var iterator: Iterator<*>, includeFilter: Class<E>) : Iterator<E> {
    //~ Instance fields --------------------------------------------------------
    var includeFilter: Class<E>

    @Nullable
    var lookAhead: E? = null
    var ready = false

    //~ Constructors -----------------------------------------------------------
    init {
        this.includeFilter = includeFilter
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun hasNext(): Boolean {
        return if (ready) {
            // Allow hasNext() to be called repeatedly.
            true
        } else try {
            lookAhead = next()
            ready = true
            true
        } catch (e: NoSuchElementException) {
            ready = false
            false
        }

        // look ahead to see if there are any additional elements
    }

    @Override
    override fun next(): E {
        if (ready) {
            val o = lookAhead
            ready = false
            return castNonNull(o)
        }
        while (iterator.hasNext()) {
            val o: Object = iterator.next()
            if (includeFilter.isInstance(o)) {
                return includeFilter.cast(o)
            }
        }
        throw NoSuchElementException()
    }

    @Override
    fun remove() {
        iterator.remove()
    }
}
