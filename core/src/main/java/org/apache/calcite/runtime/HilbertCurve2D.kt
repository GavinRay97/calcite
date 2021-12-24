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

import com.google.common.collect.ImmutableList
import com.google.uzaygezen.core.BacktrackingQueryBuilder
import com.google.uzaygezen.core.BitVector
import com.google.uzaygezen.core.BitVectorFactories
import com.google.uzaygezen.core.CompactHilbertCurve
import com.google.uzaygezen.core.FilteredIndexRange
import com.google.uzaygezen.core.LongContent
import com.google.uzaygezen.core.PlainFilterCombiner
import com.google.uzaygezen.core.Query
import com.google.uzaygezen.core.SimpleRegionInspector
import com.google.uzaygezen.core.ZoomingSpaceVisitorAdapter
import com.google.uzaygezen.core.ranges.LongRange
import com.google.uzaygezen.core.ranges.LongRangeHome
import java.util.ArrayList
import java.util.List

/**
 * 2-dimensional Hilbert space-filling curve.
 *
 *
 * Includes code from
 * [LocationTech SFCurve](https://github.com/locationtech/sfcurve),
 * Copyright (c) 2015 Azavea.
 */
class HilbertCurve2D(private val resolution: Int) : SpaceFillingCurve2D {
    val precision: Long
    val chc: CompactHilbertCurve

    init {
        precision = Math.pow(2, resolution)
        chc = CompactHilbertCurve(intArrayOf(resolution, resolution))
    }

    fun getNormalizedLongitude(x: Double): Long {
        return ((x + 180) * (precision - 1) / 360.0).toLong()
    }

    fun getNormalizedLatitude(y: Double): Long {
        return ((y + 90) * (precision - 1) / 180.0).toLong()
    }

    fun setNormalizedLatitude(latNormal: Long): Long {
        if (!(latNormal >= 0 && latNormal <= precision)) {
            throw NumberFormatException(
                "Normalized latitude must be greater than 0 and less than the maximum precision"
            )
        }
        return (latNormal * 180.0 / (precision - 1)).toLong()
    }

    fun setNormalizedLongitude(lonNormal: Long): Long {
        if (!(lonNormal >= 0 && lonNormal <= precision)) {
            throw NumberFormatException(
                "Normalized longitude must be greater than 0 and less than the maximum precision"
            )
        }
        return (lonNormal * 360.0 / (precision - 1)).toLong()
    }

    @Override
    fun toIndex(x: Double, y: Double): Long {
        val normX = getNormalizedLongitude(x)
        val normY = getNormalizedLatitude(y)
        val p: Array<BitVector> = arrayOf<BitVector>(
            BitVectorFactories.OPTIMAL.apply(resolution),
            BitVectorFactories.OPTIMAL.apply(resolution)
        )
        p[0].copyFrom(normX)
        p[1].copyFrom(normY)
        val hilbert: BitVector = BitVectorFactories.OPTIMAL.apply(resolution * 2)
        chc.index(p, 0, hilbert)
        return hilbert.toLong()
    }

    @Override
    fun toPoint(i: Long): Point {
        val h: BitVector = BitVectorFactories.OPTIMAL.apply(resolution * 2)
        h.copyFrom(i)
        val p: Array<BitVector> = arrayOf<BitVector>(
            BitVectorFactories.OPTIMAL.apply(resolution),
            BitVectorFactories.OPTIMAL.apply(resolution)
        )
        chc.indexInverse(h, p)
        val x = setNormalizedLongitude(p[0].toLong()) - 180
        val y = setNormalizedLatitude(p[1].toLong()) - 90
        return Point(x.toDouble(), y.toDouble())
    }

    @Override
    fun toRanges(
        xMin: Double, yMin: Double, xMax: Double,
        yMax: Double, hints: RangeComputeHints?
    ): List<IndexRange> {
        val chc = CompactHilbertCurve(intArrayOf(resolution, resolution))
        val region: List<LongRange> = ArrayList()
        val minNormalizedLongitude = getNormalizedLongitude(xMin)
        val minNormalizedLatitude = getNormalizedLatitude(yMin)
        val maxNormalizedLongitude = getNormalizedLongitude(xMax)
        val maxNormalizedLatitude = getNormalizedLatitude(yMax)
        region.add(LongRange.of(minNormalizedLongitude, maxNormalizedLongitude))
        region.add(LongRange.of(minNormalizedLatitude, maxNormalizedLatitude))
        val zero = LongContent(0L)
        val inspector: SimpleRegionInspector<LongRange, Long, LongContent, LongRange> = SimpleRegionInspector.create(
            ImmutableList.of(region),
            LongContent(1L), { range -> range }, LongRangeHome.INSTANCE,
            zero
        )
        val combiner: PlainFilterCombiner<LongRange, Long, LongContent, LongRange> =
            PlainFilterCombiner(LongRange.of(0, 1))
        val queryBuilder: BacktrackingQueryBuilder<LongRange, Long, LongContent, LongRange> =
            BacktrackingQueryBuilder.create(
                inspector, combiner, Integer.MAX_VALUE,
                true, LongRangeHome.INSTANCE, zero
            )
        chc.accept(ZoomingSpaceVisitorAdapter(chc, queryBuilder))
        val query: Query<LongRange, LongRange> = queryBuilder.get()
        val ranges: List<FilteredIndexRange<LongRange, LongRange>> = query.getFilteredIndexRanges()

        // result
        val result: List<IndexRange> = ArrayList()
        for (l in ranges) {
            val range: LongRange = l.getIndexRange()
            val start: Long = range.getStart()
            val end: Long = range.getEnd()
            val contained: Boolean = l.isPotentialOverSelectivity()
            result.add(0, IndexRanges.create(start, end, contained))
        }
        return result
    }
}
