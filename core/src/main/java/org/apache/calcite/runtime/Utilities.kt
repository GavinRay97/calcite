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

import java.text.Collator

/**
 * Utility methods called by generated code.
 */
object Utilities {
    // CHECKSTYLE: IGNORE 1

    @Deprecated // to be removed before 2.0
    @Deprecated("Use {@link java.util.Objects#equals}. ")
    fun equal(@Nullable o0: Object?, @Nullable o1: Object?): Boolean {
        // Same as java.lang.Objects.equals (JDK 1.7 and later)
        // and com.google.common.base.Objects.equal
        return Objects.equals(o0, o1)
    }

    fun hash(@Nullable v: Object?): Int {
        return if (v == null) 0 else v.hashCode()
    }

    /** Computes the hash code of a `double` value. Equivalent to
     * [Double]`.hashCode(double)`, but that method was only
     * introduced in JDK 1.8.
     *
     * @param v Value
     * @return Hash code
     */
    @Deprecated // to be removed before 2.0
    @Deprecated("Use {@link Double#hashCode(double)}")
    fun hashCode(v: Double): Int {
        return Double.hashCode(v)
    }

    /** Computes the hash code of a `float` value. Equivalent to
     * [Float]`.hashCode(float)`, but that method was only
     * introduced in JDK 1.8.
     *
     * @param v Value
     * @return Hash code
     */
    @Deprecated // to be removed before 2.0
    @Deprecated("Use {@link Float#hashCode(float)}")
    fun hashCode(v: Float): Int {
        return Float.hashCode(v)
    }

    /** Computes the hash code of a `long` value. Equivalent to
     * [Long]`.hashCode(long)`, but that method was only
     * introduced in JDK 1.8.
     *
     * @param v Value
     * @return Hash code
     */
    @Deprecated // to be removed before 2.0
    @Deprecated("Use {@link Long#hashCode(long)}")
    fun hashCode(v: Long): Int {
        return Long.hashCode(v)
    }

    /** Computes the hash code of a `boolean` value. Equivalent to
     * [Boolean]`.hashCode(boolean)`, but that method was only
     * introduced in JDK 1.8.
     *
     * @param v Value
     * @return Hash code
     */
    @Deprecated // to be removed before 2.0
    @Deprecated("Use {@link Boolean#hashCode(boolean)}")
    fun hashCode(v: Boolean): Int {
        return Boolean.hashCode(v)
    }

    fun hash(h: Int, v: Boolean): Int {
        return h * 31 + hashCode(v)
    }

    fun hash(h: Int, v: Byte): Int {
        return h * 31 + v
    }

    fun hash(h: Int, v: Char): Int {
        return h * 31 + v.code
    }

    fun hash(h: Int, v: Short): Int {
        return h * 31 + v
    }

    fun hash(h: Int, v: Int): Int {
        return h * 31 + v
    }

    fun hash(h: Int, v: Long): Int {
        return h * 31 + Long.hashCode(v)
    }

    fun hash(h: Int, v: Float): Int {
        return hash(h, Float.hashCode(v))
    }

    fun hash(h: Int, v: Double): Int {
        return hash(h, Double.hashCode(v))
    }

    fun hash(h: Int, @Nullable v: Object?): Int {
        return h * 31 + if (v == null) 1 else v.hashCode()
    }

    fun compare(v0: Boolean, v1: Boolean): Int {
        return Boolean.compare(v0, v1)
    }

    fun compare(v0: Byte, v1: Byte): Int {
        return Byte.compare(v0, v1)
    }

    fun compare(v0: Char, v1: Char): Int {
        return Character.compare(v0, v1)
    }

    fun compare(v0: Short, v1: Short): Int {
        return Short.compare(v0, v1)
    }

    fun compare(v0: Int, v1: Int): Int {
        return Integer.compare(v0, v1)
    }

    fun compare(v0: Long, v1: Long): Int {
        return Long.compare(v0, v1)
    }

    fun compare(v0: Float, v1: Float): Int {
        return Float.compare(v0, v1)
    }

    fun compare(v0: Double, v1: Double): Int {
        return Double.compare(v0, v1)
    }

    fun compare(v0: List, v1: List): Int {
        val iterator0: Iterator = v0.iterator()
        val iterator1: Iterator = v1.iterator()
        while (true) {
            if (!iterator0.hasNext()) {
                return if (!iterator1.hasNext()) 0 else -1
            }
            if (!iterator1.hasNext()) {
                return 1
            }
            val o0: Object = iterator0.next()
            val o1: Object = iterator1.next()
            val c = compare_(o0, o1)
            if (c != 0) {
                return c
            }
        }
    }

    private fun compare_(o0: Object, o1: Object): Int {
        return if (o0 is Comparable) {
            compare(o0 as Comparable, o1 as Comparable)
        } else compare(o0 as List, o1 as List)
    }

    fun compare(v0: Comparable, v1: Comparable?): Int {
        return v0.compareTo(v1)
    }

    fun compareNullsFirst(@Nullable v0: Comparable?, @Nullable v1: Comparable?): Int {
        return if (v0 === v1) 0 else if (v0 == null) -1 else if (v1 == null) 1 else v0.compareTo(v1)
    }

    fun compareNullsLast(@Nullable v0: Comparable?, @Nullable v1: Comparable?): Int {
        return if (v0 === v1) 0 else if (v0 == null) 1 else if (v1 == null) -1 else v0.compareTo(v1)
    }

    fun compare(
        @Nullable v0: Comparable?, @Nullable v1: Comparable?,
        comparator: Comparator
    ): Int {
        return comparator.compare(v0, v1)
    }

    fun compareNullsFirst(
        @Nullable v0: Comparable?, @Nullable v1: Comparable?,
        comparator: Comparator
    ): Int {
        return if (v0 === v1) 0 else if (v0 == null) -1 else if (v1 == null) 1 else comparator.compare(v0, v1)
    }

    fun compareNullsLast(
        @Nullable v0: Comparable?, @Nullable v1: Comparable?,
        comparator: Comparator
    ): Int {
        return if (v0 === v1) 0 else if (v0 == null) 1 else if (v1 == null) -1 else comparator.compare(v0, v1)
    }

    fun compareNullsLast(v0: List?, v1: List?): Int {
        return if (v0 === v1) 0 else if (v0 == null) 1 else if (v1 == null) -1 else FlatLists.ComparableListImpl.compare(
            v0,
            v1
        )
    }

    /** Creates a pattern builder.  */
    fun patternBuilder(): Pattern.PatternBuilder {
        return Pattern.builder()
    }

    fun generateCollator(locale: Locale?, strength: Int): Collator {
        val collator: Collator = Collator.getInstance(locale)
        collator.setStrength(strength)
        return collator
    }
}
