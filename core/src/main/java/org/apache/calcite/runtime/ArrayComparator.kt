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

import com.google.common.collect.Ordering
import java.util.Collections
import java.util.Comparator

/**
 * Compares arrays.
 */
class ArrayComparator : Comparator<Array<Object?>?> {
    private val comparators: Array<Comparator?>

    constructor(vararg comparators: Comparator?) {
        this.comparators = comparators
    }

    constructor(vararg descendings: Boolean) {
        comparators = comparators(descendings)
    }

    @Override
    fun compare(o1: Array<Object?>, o2: Array<Object?>): Int {
        for (i in comparators.indices) {
            val comparator: Comparator? = comparators[i]
            val c: Int = comparator.compare(o1[i], o2[i])
            if (c != 0) {
                return c
            }
        }
        return 0
    }

    companion object {
        private fun comparators(descendings: BooleanArray): Array<Comparator?> {
            val comparators: Array<Comparator?> = arrayOfNulls<Comparator>(descendings.size)
            for (i in descendings.indices) {
                val descending = descendings[i]
                comparators[i] = if (descending) Collections.reverseOrder() else Ordering.natural()
            }
            return comparators
        }
    }
}
