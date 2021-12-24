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
package org.apache.calcite.util.mapping

import org.apache.calcite.runtime.Utilities

/**
 * An immutable pair of integers.
 *
 * @see Mapping.iterator
 */
class IntPair     //~ Constructors -----------------------------------------------------------
    (//~ Instance fields --------------------------------------------------------
    val source: Int, val target: Int
) {
    @Override
    override fun toString(): String {
        return "$source-$target"
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        if (obj is IntPair) {
            val that = obj as IntPair
            return source == that.source && target == that.target
        }
        return false
    }

    @Override
    override fun hashCode(): Int {
        return Utilities.hash(source, target)
    }

    companion object {
        /** Function that swaps source and target fields of an [IntPair].  */
        @Deprecated
        val SWAP: Function<IntPair, IntPair> = object : Function<IntPair?, IntPair?>() {
            @Override
            fun apply(pair: IntPair): IntPair {
                return of(pair.target, pair.source)
            }
        }

        /** Ordering that compares pairs lexicographically: first by their source,
         * then by their target.  */
        val ORDERING: Ordering<IntPair> = Ordering.from(
            object : Comparator<IntPair?>() {
                @Override
                fun compare(o1: IntPair, o2: IntPair): Int {
                    var c: Int = Integer.compare(o1.source, o2.source)
                    if (c == 0) {
                        c = Integer.compare(o1.target, o2.target)
                    }
                    return c
                }
            })

        /** Function that returns the left (source) side of a pair.  */
        @Deprecated
        val LEFT: Function<IntPair, Integer> = object : Function<IntPair?, Integer?>() {
            @Override
            fun apply(pair: IntPair): Integer {
                return pair.source
            }
        }

        /** Function that returns the right (target) side of a pair.  */
        @Deprecated
        val RIGHT: Function<IntPair, Integer> = object : Function<IntPair?, Integer?>() {
            @Override
            fun apply(pair: IntPair): Integer {
                return pair.target
            }
        }

        //~ Methods ----------------------------------------------------------------
        fun of(left: Int, right: Int): IntPair {
            return IntPair(left, right)
        }

        /**
         * Converts two lists into a list of [IntPair]s,
         * whose length is the lesser of the lengths of the
         * source lists.
         *
         * @param lefts Left list
         * @param rights Right list
         * @return List of pairs
         */
        fun zip(
            lefts: List<Number?>,
            rights: List<Number?>
        ): List<IntPair> {
            return zip(lefts, rights, false)
        }

        /**
         * Converts two lists into a list of [IntPair]s.
         *
         *
         * The length of the combined list is the lesser of the lengths of the
         * source lists. But typically the source lists will be the same length.
         *
         * @param lefts Left list
         * @param rights Right list
         * @param strict Whether to fail if lists have different size
         * @return List of pairs
         */
        fun zip(
            lefts: List<Number?>,
            rights: List<Number?>,
            strict: Boolean
        ): List<IntPair> {
            val size: Int
            if (strict) {
                if (lefts.size() !== rights.size()) {
                    throw AssertionError()
                }
                size = lefts.size()
            } else {
                size = Math.min(lefts.size(), rights.size())
            }
            return object : AbstractList<IntPair?>() {
                @Override
                operator fun get(index: Int): IntPair {
                    return of(
                        lefts[index].intValue(),
                        rights[index].intValue()
                    )
                }

                @Override
                fun size(): Int {
                    return size
                }
            }
        }

        /** Returns the left side of a list of pairs.  */
        fun left(pairs: List<IntPair?>?): List<Integer> {
            return Util.transform(pairs) { x -> x.source }
        }

        /** Returns the right side of a list of pairs.  */
        fun right(pairs: List<IntPair?>?): List<Integer> {
            return Util.transform(pairs) { x -> x.target }
        }
    }
}
