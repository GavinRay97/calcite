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

import org.apache.calcite.linq4j.Enumerator

/**
 * A set of non-negative integers defined by a sequence of points, intervals,
 * and exclusions.
 */
class IntegerIntervalSet private constructor(private val s: String) : AbstractSet<Integer?>() {
    @SuppressWarnings("NullableProblems")
    @Override
    operator fun iterator(): Iterator<Integer> {
        return Linq4j.enumeratorIterator(enumerator())
    }

    @Override
    fun size(): Int {
        var n = 0
        val e: Enumerator<Integer> = enumerator()
        while (e.moveNext()) {
            ++n
        }
        return n
    }

    private fun enumerator(): Enumerator<Integer> {
        val bounds = intArrayOf(Integer.MAX_VALUE, Integer.MIN_VALUE)
        visit(
            s, Handler { start: Int, end: Int, exclude: Boolean ->
                if (!exclude) {
                    bounds[0] = Math.min(bounds[0], start)
                    bounds[1] = Math.max(bounds[1], end)
                }
            })
        return object : Enumerator<Integer?>() {
            var i = bounds[0] - 1
            @Override
            fun current(): Integer {
                return i
            }

            @Override
            fun moveNext(): Boolean {
                while (true) {
                    if (++i > bounds[1]) {
                        return false
                    }
                    if (contains(i)) {
                        return true
                    }
                }
            }

            @Override
            fun reset() {
                i = bounds[0] - 1
            }

            @Override
            fun close() {
                // no resources
            }
        }
    }

    @Override
    operator fun contains(@Nullable o: Object): Boolean {
        return (o is Number
                && contains((o as Number).intValue()))
    }

    operator fun contains(n: Int): Boolean {
        val bs = booleanArrayOf(false)
        visit(
            s, Handler { start: Int, end: Int, exclude: Boolean ->
                if (start <= n && n <= end) {
                    bs[0] = !exclude
                }
            })
        return bs[0]
    }

    /** A callback.  */
    private interface Handler {
        fun range(start: Int, end: Int, exclude: Boolean)
    }

    companion object {
        private fun visit(s: String, handler: Handler) {
            val split: Array<String> = s.split(",")
            for (s1 in split) {
                if (s1.isEmpty()) {
                    continue
                }
                var exclude = false
                if (s1.startsWith("-")) {
                    s1 = s1.substring(1)
                    exclude = true
                }
                val split1: Array<String> = s1.split("-")
                if (split1.size == 1) {
                    val n: Int = Integer.parseInt(split1[0])
                    handler.range(n, n, exclude)
                } else {
                    val n0: Int = Integer.parseInt(split1[0])
                    val n1: Int = Integer.parseInt(split1[1])
                    handler.range(n0, n1, exclude)
                }
            }
        }

        /**
         * Parses a range of integers expressed as a string. The string can contain
         * non-negative integers separated by commas, ranges (represented by a
         * hyphen between two integers), and exclusions (represented by a preceding
         * hyphen). For example, "1,2,3-20,-7,-10-15,12".
         *
         *
         * Inclusions and exclusions are performed in the order that they are
         * seen. For example, "1-10,-2-9,3-7,-4-6" does contain 3, because it is
         * included by "1-10", excluded by "-2-9" and last included by "3-7". But it
         * does not include 4.
         *
         * @param s Range set
         */
        fun of(s: String): Set<Integer> {
            return IntegerIntervalSet(s)
        }
    }
}
