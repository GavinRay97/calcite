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

import com.google.common.collect.BoundType

/** Utilities for Guava [com.google.common.collect.RangeSet].  */
@SuppressWarnings(["BetaApi", "UnstableApiUsage"])
object RangeSets {
    @SuppressWarnings(["BetaApi", "rawtypes"])
    private val ALL: ImmutableRangeSet = ImmutableRangeSet.of().complement()

    /** Subtracts a range from a range set.  */
    fun <C : Comparable<C>?> minus(
        rangeSet: RangeSet<C>,
        range: Range<C>?
    ): RangeSet<C> {
        val mutableRangeSet: TreeRangeSet<C> = TreeRangeSet.create(rangeSet)
        mutableRangeSet.remove(range)
        return if (mutableRangeSet.equals(rangeSet)) rangeSet else ImmutableRangeSet.copyOf(mutableRangeSet)
    }

    /** Returns the unrestricted range set.  */
    @SuppressWarnings(["rawtypes", "unchecked"])
    fun <C : Comparable<C>?> rangeSetAll(): RangeSet<C> {
        return ALL as RangeSet
    }

    /** Compares two range sets.  */
    fun <C : Comparable<C>?> compare(
        s0: RangeSet<C>,
        s1: RangeSet<C>
    ): Int {
        val i0: Iterator<Range<C>> = s0.asRanges().iterator()
        val i1: Iterator<Range<C>> = s1.asRanges().iterator()
        while (true) {
            val h0 = i0.hasNext()
            val h1 = i1.hasNext()
            if (!h0 || !h1) {
                return Boolean.compare(h0, h1)
            }
            val r0: Range<C> = i0.next()
            val r1: Range<C> = i1.next()
            val c: Int = compare<Comparable<C>>(r0, r1)
            if (c != 0) {
                return c
            }
        }
    }

    /** Compares two ranges.  */
    fun <C : Comparable<C>?> compare(
        r0: Range<C>,
        r1: Range<C>
    ): Int {
        var c: Int = Boolean.compare(r0.hasLowerBound(), r1.hasLowerBound())
        if (c != 0) {
            return c
        }
        if (r0.hasLowerBound()) {
            c = r0.lowerEndpoint().compareTo(r1.lowerEndpoint())
            if (c != 0) {
                return c
            }
            c = r0.lowerBoundType().compareTo(r1.lowerBoundType())
            if (c != 0) {
                return c
            }
        }
        c = Boolean.compare(r0.hasUpperBound(), r1.hasUpperBound())
        if (c != 0) {
            return -c
        }
        if (r0.hasUpperBound()) {
            c = r0.upperEndpoint().compareTo(r1.upperEndpoint())
            if (c != 0) {
                return c
            }
            c = r0.upperBoundType().compareTo(r1.upperBoundType())
            if (c != 0) {
                return c
            }
        }
        return 0
    }

    /** Computes a hash code for a range set.
     *
     *
     * This method does not compute the same result as
     * [RangeSet.hashCode]. That is a poor hash code because it is based
     * upon [java.util.Set.hashCode]).
     *
     *
     * The algorithm is based on [java.util.List.hashCode],
     * which is well-defined because [RangeSet.asRanges] is sorted.  */
    fun <C : Comparable<C>?> hashCode(rangeSet: RangeSet<C>): Int {
        var h = 1
        for (r in rangeSet.asRanges()) {
            h = 31 * h + r.hashCode()
        }
        return h
    }

    /** Returns whether a range is a point.  */
    fun <C : Comparable<C>?> isPoint(range: Range<C>): Boolean {
        return (range.hasLowerBound()
                && range.hasUpperBound()
                && range.lowerEndpoint().equals(range.upperEndpoint())
                && !range.isEmpty())
    }

    /** Returns whether a range set is a single open interval.  */
    fun <C : Comparable<C>?> isOpenInterval(rangeSet: RangeSet<C>): Boolean {
        if (rangeSet.isEmpty()) {
            return false
        }
        val ranges: Set<Range<C>> = rangeSet.asRanges()
        val range: Range<C> = ranges.iterator().next()
        return (ranges.size() === 1
                && (!range.hasLowerBound() || !range.hasUpperBound()))
    }

    /** Returns the number of ranges in a range set that are points.
     *
     *
     * If every range in a range set is a point then it can be converted to a
     * SQL IN list.  */
    fun <C : Comparable<C>?> countPoints(rangeSet: RangeSet<C>): Int {
        var n = 0
        for (range in rangeSet.asRanges()) {
            if (isPoint<Comparable<C>>(range)) {
                ++n
            }
        }
        return n
    }

    /** Calls the appropriate handler method for each range in a range set,
     * creating a new range set from the results.  */
    fun <C : Comparable<C>?, C2 : Comparable<C2>?> map(
        rangeSet: RangeSet<C>,
        handler: Handler<C, Range<C2>?>?
    ): RangeSet<C2> {
        val builder: ImmutableRangeSet.Builder<C2> = ImmutableRangeSet.builder()
        rangeSet.asRanges().forEach { range -> builder.add(map(range, handler)) }
        return builder.build()
    }

    /** Calls the appropriate handler method for the type of range.  */
    fun <C : Comparable<C>?, R> map(
        range: Range<C>,
        handler: Handler<C, R>
    ): R {
        return if (range.hasLowerBound() && range.hasUpperBound()) {
            val lower: C = range.lowerEndpoint()
            val upper: C = range.upperEndpoint()
            if (range.lowerBoundType() === BoundType.OPEN) {
                if (range.upperBoundType() === BoundType.OPEN) {
                    handler.open(lower, upper)
                } else {
                    handler.openClosed(lower, upper)
                }
            } else {
                if (range.upperBoundType() === BoundType.OPEN) {
                    handler.closedOpen(lower, upper)
                } else {
                    if (lower!!.equals(upper)) {
                        handler.singleton(lower)
                    } else {
                        handler.closed(lower, upper)
                    }
                }
            }
        } else if (range.hasLowerBound()) {
            val lower: C = range.lowerEndpoint()
            if (range.lowerBoundType() === BoundType.OPEN) {
                handler.greaterThan(lower)
            } else {
                handler.atLeast(lower)
            }
        } else if (range.hasUpperBound()) {
            val upper: C = range.upperEndpoint()
            if (range.upperBoundType() === BoundType.OPEN) {
                handler.lessThan(upper)
            } else {
                handler.atMost(upper)
            }
        } else {
            handler.all()
        }
    }

    /** Copies a range set.  */
    fun <C : Comparable<C>?, C2 : Comparable<C2>?> copy(rangeSet: RangeSet<C>?, map: Function<C, C2>): RangeSet<C2> {
        return map(rangeSet, object : CopyingHandler<C, C2>() {
            @Override
            override fun convert(c: C): C2 {
                return map.apply(c)
            }
        })
    }

    /** Copies a range.  */
    fun <C : Comparable<C>?, C2 : Comparable<C2>?> copy(range: Range<C>?, map: Function<C, C2>): Range<C2> {
        return map(range, object : CopyingHandler<C, C2>() {
            @Override
            override fun convert(c: C): C2 {
                return map.apply(c)
            }
        })
    }

    fun <C : Comparable<C>?> forEach(
        rangeSet: RangeSet<C>,
        consumer: Consumer<C>?
    ) {
        rangeSet.asRanges().forEach { range -> forEach(range, consumer) }
    }

    fun <C : Comparable<C>?> forEach(
        range: Range<C>,
        consumer: Consumer<C>
    ) {
        if (range.hasLowerBound() && range.hasUpperBound()) {
            val lower: C = range.lowerEndpoint()
            val upper: C = range.upperEndpoint()
            if (range.lowerBoundType() === BoundType.OPEN) {
                if (range.upperBoundType() === BoundType.OPEN) {
                    consumer.open(lower, upper)
                } else {
                    consumer.openClosed(lower, upper)
                }
            } else {
                if (range.upperBoundType() === BoundType.OPEN) {
                    consumer.closedOpen(lower, upper)
                } else {
                    if (lower!!.equals(upper)) {
                        consumer.singleton(lower)
                    } else {
                        consumer.closed(lower, upper)
                    }
                }
            }
        } else if (range.hasLowerBound()) {
            val lower: C = range.lowerEndpoint()
            if (range.lowerBoundType() === BoundType.OPEN) {
                consumer.greaterThan(lower)
            } else {
                consumer.atLeast(lower)
            }
        } else if (range.hasUpperBound()) {
            val upper: C = range.upperEndpoint()
            if (range.upperBoundType() === BoundType.OPEN) {
                consumer.lessThan(upper)
            } else {
                consumer.atMost(upper)
            }
        } else {
            consumer.all()
        }
    }

    /** Creates a consumer that prints values to a [StringBuilder].  */
    fun <C : Comparable<C>?> printer(
        sb: StringBuilder,
        valuePrinter: BiConsumer<StringBuilder?, C>
    ): Consumer<C> {
        return Printer(sb, valuePrinter)
    }

    /** Deconstructor for [Range] values.
     *
     * @param <C> Value type
     * @param <R> Return type
     *
     * @see Consumer
    </R></C> */
    interface Handler<C : Comparable<C>?, R> {
        fun all(): R
        fun atLeast(lower: C): R
        fun atMost(upper: C): R
        fun greaterThan(lower: C): R
        fun lessThan(upper: C): R
        fun singleton(value: C): R
        fun closed(lower: C, upper: C): R
        fun closedOpen(lower: C, upper: C): R
        fun openClosed(lower: C, upper: C): R
        fun open(lower: C, upper: C): R
    }

    /** Consumer of [Range] values.
     *
     * @param <C> Value type
     *
     * @see Handler
    </C> */
    interface Consumer<C : Comparable<C>?> {
        fun all()
        fun atLeast(lower: C)
        fun atMost(upper: C)
        fun greaterThan(lower: C)
        fun lessThan(upper: C)
        fun singleton(value: C)
        fun closed(lower: C, upper: C)
        fun closedOpen(lower: C, upper: C)
        fun openClosed(lower: C, upper: C)
        fun open(lower: C, upper: C)
    }

    /** Handler that converts a Range into another Range of the same type,
     * applying a mapping function to the range's bound(s).
     *
     * @param <C> Value type
     * @param <C2> Output value type
    </C2></C> */
    private abstract class CopyingHandler<C : Comparable<C>?, C2 : Comparable<C2>?> : Handler<C, Range<C2>?> {
        abstract fun convert(c: C): C2
        @Override
        override fun all(): Range<C2> {
            return Range.all()
        }

        @Override
        override fun atLeast(lower: C): Range<C2> {
            return Range.atLeast(convert(lower))
        }

        @Override
        override fun atMost(upper: C): Range<C2> {
            return Range.atMost(convert(upper))
        }

        @Override
        override fun greaterThan(lower: C): Range<C2> {
            return Range.greaterThan(convert(lower))
        }

        @Override
        override fun lessThan(upper: C): Range<C2> {
            return Range.lessThan(convert(upper))
        }

        @Override
        override fun singleton(value: C): Range<C2> {
            return Range.singleton(convert(value))
        }

        @Override
        override fun closed(lower: C, upper: C): Range<C2> {
            return Range.closed(convert(lower), convert(upper))
        }

        @Override
        override fun closedOpen(lower: C, upper: C): Range<C2> {
            return Range.closedOpen(convert(lower), convert(upper))
        }

        @Override
        override fun openClosed(lower: C, upper: C): Range<C2> {
            return Range.openClosed(convert(lower), convert(upper))
        }

        @Override
        override fun open(lower: C, upper: C): Range<C2> {
            return Range.open(convert(lower), convert(upper))
        }
    }

    /** Converts any type of range to a string, using a given value printer.
     *
     * @param <C> Value type
    </C> */
    internal class Printer<C : Comparable<C>?>(sb: StringBuilder, valuePrinter: BiConsumer<StringBuilder?, C>) :
        Consumer<C> {
        private val sb: StringBuilder
        private val valuePrinter: BiConsumer<StringBuilder, C>

        init {
            this.sb = sb
            this.valuePrinter = valuePrinter
        }

        @Override
        override fun all() {
            sb.append("(-\u221e..+\u221e)")
        }

        @Override
        override fun atLeast(lower: C) {
            sb.append('[')
            valuePrinter.accept(sb, lower)
            sb.append("..+\u221e)")
        }

        @Override
        override fun atMost(upper: C) {
            sb.append("(-\u221e..")
            valuePrinter.accept(sb, upper)
            sb.append("]")
        }

        @Override
        override fun greaterThan(lower: C) {
            sb.append('(')
            valuePrinter.accept(sb, lower)
            sb.append("..+\u221e)")
        }

        @Override
        override fun lessThan(upper: C) {
            sb.append("(-\u221e..")
            valuePrinter.accept(sb, upper)
            sb.append(")")
        }

        @Override
        override fun singleton(value: C) {
            valuePrinter.accept(sb, value)
        }

        @Override
        override fun closed(lower: C, upper: C) {
            sb.append('[')
            valuePrinter.accept(sb, lower)
            sb.append("..")
            valuePrinter.accept(sb, upper)
            sb.append(']')
        }

        @Override
        override fun closedOpen(lower: C, upper: C) {
            sb.append('[')
            valuePrinter.accept(sb, lower)
            sb.append("..")
            valuePrinter.accept(sb, upper)
            sb.append(')')
        }

        @Override
        override fun openClosed(lower: C, upper: C) {
            sb.append('(')
            valuePrinter.accept(sb, lower)
            sb.append("..")
            valuePrinter.accept(sb, upper)
            sb.append(']')
        }

        @Override
        override fun open(lower: C, upper: C) {
            sb.append('(')
            valuePrinter.accept(sb, lower)
            sb.append("..")
            valuePrinter.accept(sb, upper)
            sb.append(')')
        }
    }
}
