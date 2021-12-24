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
import java.util.HashMap
import java.util.List

/**
 * Utilities for space-filling curves.
 *
 *
 * Includes code from
 * [LocationTech SFCurve](https://github.com/locationtech/sfcurve),
 * Copyright (c) 2015 Azavea.
 */
interface SpaceFillingCurve2D {
    fun toIndex(x: Double, y: Double): Long
    fun toPoint(i: Long): Point?
    fun toRanges(
        xMin: Double, yMin: Double, xMax: Double,
        yMax: Double, hints: RangeComputeHints?
    ): List<IndexRange?>?

    /** Hints for the [SpaceFillingCurve2D.toRanges] method.  */
    class RangeComputeHints : HashMap<String?, Object?>()

    /** Range.  */
    interface IndexRange {
        fun lower(): Long
        fun upper(): Long
        fun contained(): Boolean
        fun tuple(): IndexRangeTuple?
    }

    /** Data representing a range.  */
    class IndexRangeTuple internal constructor(val lower: Long, val upper: Long, val contained: Boolean)

    /** Base class for Range implementations.  */
    abstract class AbstractRange protected constructor(val lower: Long, val upper: Long) : IndexRange {
        @Override
        override fun lower(): Long {
            return lower
        }

        @Override
        override fun upper(): Long {
            return upper
        }

        @Override
        override fun tuple(): IndexRangeTuple {
            return IndexRangeTuple(lower, upper, contained())
        }
    }

    /** Range that is covered.  */
    class CoveredRange internal constructor(lower: Long, upper: Long) : AbstractRange(lower, upper) {
        @Override
        override fun contained(): Boolean {
            return true
        }

        @Override
        override fun toString(): String {
            return "covered($lower, $upper)"
        }
    }

    /** Range that is not contained.  */
    class OverlappingRange internal constructor(lower: Long, upper: Long) : AbstractRange(lower, upper) {
        @Override
        override fun contained(): Boolean {
            return false
        }

        @Override
        override fun toString(): String {
            return "overlap($lower, $upper)"
        }
    }

    /** Lexicographic ordering for [IndexRange].  */
    class IndexRangeOrdering : Ordering<IndexRange?>() {
        @SuppressWarnings("override.param.invalid")
        @Override
        fun compare(x: IndexRange, y: IndexRange): Int {
            val c1: Int = Long.compare(x.lower(), y.lower())
            return if (c1 != 0) {
                c1
            } else Long.compare(x.upper(), y.upper())
        }
    }

    /** Utilities for [IndexRange].  */
    object IndexRanges {
        fun create(l: Long, u: Long, contained: Boolean): IndexRange {
            return if (contained) CoveredRange(l, u) else OverlappingRange(l, u)
        }
    }

    /** A 2-dimensional point.  */
    class Point internal constructor(val x: Double, val y: Double)
}
