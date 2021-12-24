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

import com.google.common.primitives.Ints

/**
 * Extension to [ArrayList] to help build an array of `int`
 * values.
 */
@Deprecated // to be removed before 2.0
class IntList : ArrayList<Integer?>() {
    //~ Methods ----------------------------------------------------------------
    fun toIntArray(): IntArray {
        return Ints.toArray(this)
    }

    fun asImmutable(): ImmutableIntList {
        return ImmutableIntList.copyOf(this)
    }

    companion object {
        /**
         * Converts a list of [Integer] objects to an array of primitive
         * `int`s.
         *
         * @param integers List of Integer objects
         * @return Array of primitive `int`s
         *
         */
        @Deprecated // to be removed before 2.0
        @Deprecated("Use {@link Ints#toArray(java.util.Collection)}")
        fun toArray(integers: List<Integer?>?): IntArray {
            return Ints.toArray(integers)
        }

        /**
         * Returns a list backed by an array of primitive `int` values.
         *
         *
         * The behavior is analogous to [Arrays.asList]. Changes
         * to the list are reflected in the array. The list cannot be extended.
         *
         * @param args Array of primitive `int` values
         * @return List backed by array
         */
        @Deprecated // to be removed before 2.0
        fun asList(args: IntArray?): List<Integer> {
            return Ints.asList(args)
        }
    }
}
