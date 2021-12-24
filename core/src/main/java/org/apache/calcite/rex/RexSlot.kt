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
package org.apache.calcite.rex

import org.apache.calcite.rel.type.RelDataType
import java.util.AbstractList
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Abstract base class for [RexInputRef] and [RexLocalRef].
 */
abstract class RexSlot protected constructor(
    name: String?,
    index: Int,
    type: RelDataType?
) : RexVariable(name, type) {
    //~ Methods ----------------------------------------------------------------
    //~ Instance fields --------------------------------------------------------
    val index: Int
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a slot.
     *
     * @param index Index of the field in the underlying rowtype
     * @param type  Type of the column
     */
    init {
        assert(index >= 0)
        this.index = index
    }

    /**
     * Thread-safe list that populates itself if you make a reference beyond
     * the end of the list. Useful if you are using the same entries repeatedly.
     * Once populated, accesses are very efficient.
     */
    protected class SelfPopulatingList internal constructor(private val prefix: String, initialSize: Int) :
        CopyOnWriteArrayList<String?>(
            fromTo(
                prefix, 0, initialSize
            )
        ) {
        @Override
        operator fun get(index: Int): String {
            while (true) {
                try {
                    return super.get(index)
                } catch (e: IndexOutOfBoundsException) {
                    if (index < 0) {
                        throw IllegalArgumentException()
                    }
                    // Double-checked locking, but safe because CopyOnWriteArrayList.array
                    // is marked volatile, and size() uses array.length.
                    synchronized(this) {
                        val size: Int = size()
                        if (index >= size) {
                            addAll(fromTo(prefix, size, Math.max(index + 1, size * 2)))
                        }
                    }
                }
            }
        }

        companion object {
            private fun fromTo(
                prefix: String,
                start: Int,
                end: Int
            ): AbstractList<String> {
                return object : AbstractList<String?>() {
                    @Override
                    operator fun get(index: Int): String {
                        return prefix + (index + start)
                    }

                    @Override
                    fun size(): Int {
                        return end - start
                    }
                }
            }
        }
    }
}
