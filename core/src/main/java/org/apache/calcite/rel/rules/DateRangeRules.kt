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
package org.apache.calcite.rel.rules

import org.apache.calcite.avatica.util.DateTimeUtils

/**
 * Collection of planner rules that convert
 * `EXTRACT(timeUnit FROM dateTime) = constant`,
 * `FLOOR(dateTime to timeUnit` = constant} and
 * `CEIL(dateTime to timeUnit` = constant} to
 * `dateTime BETWEEN lower AND upper`.
 *
 *
 * The rules allow conversion of queries on time dimension tables, such as
 *
 * <blockquote>SELECT ... FROM sales JOIN time_by_day USING (time_id)
 * WHERE time_by_day.the_year = 1997
 * AND time_by_day.the_month IN (4, 5, 6)</blockquote>
 *
 *
 * into
 *
 * <blockquote>SELECT ... FROM sales JOIN time_by_day USING (time_id)
 * WHERE the_date BETWEEN DATE '2016-04-01' AND DATE '2016-06-30'</blockquote>
 *
 *
 * and is especially useful for Druid, which has a single timestamp column.
 */
object DateRangeRules {
    /** Rule that matches a [Filter] and converts calls to `EXTRACT`,
     * `FLOOR` and `CEIL` functions to date ranges (typically using
     * the `BETWEEN` operator).  */
    val FILTER_INSTANCE: RelOptRule = FilterDateRangeRule.FilterDateRangeRuleConfig.DEFAULT
        .`as`(FilterDateRangeRule.FilterDateRangeRuleConfig::class.java)
        .toRule()
    private val TIME_UNIT_CODES: Map<TimeUnitRange, Integer> =
        ImmutableMap.< TimeUnitRange, Integer>builder<TimeUnitRange?, Integer?>()
    .put(TimeUnitRange.YEAR, Calendar.YEAR)
    .put(TimeUnitRange.MONTH, Calendar.MONTH)
    .put(TimeUnitRange.DAY, Calendar.DAY_OF_MONTH)
    .put(TimeUnitRange.HOUR, Calendar.HOUR)
    .put(TimeUnitRange.MINUTE, Calendar.MINUTE)
    .put(TimeUnitRange.SECOND, Calendar.SECOND)
    .put(TimeUnitRange.MILLISECOND, Calendar.MILLISECOND)
    .build()
    private val TIME_UNIT_PARENTS: Map<TimeUnitRange, TimeUnitRange> =
        ImmutableMap.< TimeUnitRange, TimeUnitRange>builder<TimeUnitRange?, TimeUnitRange?>()
    .put(TimeUnitRange.MONTH, TimeUnitRange.YEAR)
    .put(TimeUnitRange.DAY, TimeUnitRange.MONTH)
    .put(TimeUnitRange.HOUR, TimeUnitRange.DAY)
    .put(TimeUnitRange.MINUTE, TimeUnitRange.HOUR)
    .put(TimeUnitRange.SECOND, TimeUnitRange.MINUTE)
    .put(TimeUnitRange.MILLISECOND, TimeUnitRange.SECOND)
    .put(TimeUnitRange.MICROSECOND, TimeUnitRange.SECOND)
    .build()
    private fun calendarUnitFor(timeUnitRange: TimeUnitRange): Int {
        return requireNonNull(
            TIME_UNIT_CODES[timeUnitRange]
        ) {
            ("unexpected timeUnitRange: " + timeUnitRange
                    + ", the following are supported: " + TIME_UNIT_CODES)
        }
    }

    /** Tests whether an expression contains one or more calls to the
     * `EXTRACT` function, and if so, returns the time units used.
     *
     *
     * The result is an immutable set in natural order. This is important,
     * because code relies on the collection being sorted (so YEAR comes before
     * MONTH before HOUR) and unique. A predicate on MONTH is not useful if there
     * is no predicate on YEAR. Then when we apply the predicate on DAY it doesn't
     * generate hundreds of ranges we'll later throw away.  */
    fun extractTimeUnits(e: RexNode): ImmutableSortedSet<TimeUnitRange> {
        ExtractFinder.THREAD_INSTANCES.get().use { finder ->
            assert(
                requireNonNull(
                    finder,
                    "finder"
                ).timeUnits.isEmpty() && finder.opKinds.isEmpty()
            ) { "previous user did not clean up" }
            e.accept(finder)
            return ImmutableSortedSet.copyOf(finder.timeUnits)
        }
    }

    /** Replaces calls to EXTRACT, FLOOR and CEIL in an expression.  */
    @VisibleForTesting
    @SuppressWarnings("BetaApi")
    fun replaceTimeUnits(
        rexBuilder: RexBuilder?, e: RexNode,
        timeZone: String?
    ): RexNode {
        var e: RexNode = e
        var timeUnits: ImmutableSortedSet<TimeUnitRange?> = extractTimeUnits(e)
        if (!timeUnits.contains(TimeUnitRange.YEAR)) {
            // Case when we have FLOOR or CEIL but no extract on YEAR.
            // Add YEAR as TimeUnit so that FLOOR gets replaced in first iteration
            // with timeUnit YEAR.
            timeUnits = ImmutableSortedSet.< TimeUnitRange > naturalOrder < TimeUnitRange ? > ()
                .addAll(timeUnits).add(TimeUnitRange.YEAR).build()
        }
        val operandRanges: Map<RexNode?, RangeSet<Calendar?>> = HashMap()
        for (timeUnit in timeUnits) {
            e = e.accept(
                ExtractShuttle(
                    rexBuilder, timeUnit, operandRanges, timeUnits,
                    timeZone
                )
            )
        }
        return e
    }

    /** Rule that converts EXTRACT, FLOOR and CEIL in a [Filter] into a date
     * range.
     *
     * @see DateRangeRules.FILTER_INSTANCE
     */
    @SuppressWarnings("WeakerAccess")
    class FilterDateRangeRule
    /** Creates a FilterDateRangeRule.  */
    protected constructor(config: FilterDateRangeRuleConfig?) :
        RelRule<FilterDateRangeRule.FilterDateRangeRuleConfig?>(config), TransformationRule {
        @Deprecated // to be removed before 2.0
        constructor(relBuilderFactory: RelBuilderFactory?) : this(
            FilterDateRangeRuleConfig.DEFAULT.withRelBuilderFactory(relBuilderFactory)
                .`as`(FilterDateRangeRuleConfig::class.java)
        ) {
        }

        @Override
        fun onMatch(call: RelOptRuleCall) {
            val filter: Filter = call.rel(0)
            val rexBuilder: RexBuilder = filter.getCluster().getRexBuilder()
            val timeZone: String = filter.getCluster().getPlanner().getContext()
                .unwrapOrThrow(CalciteConnectionConfig::class.java).timeZone()
            val condition: RexNode = replaceTimeUnits(rexBuilder, filter.getCondition(), timeZone)
            if (condition.equals(filter.getCondition())) {
                return
            }
            val relBuilder: RelBuilder = relBuilderFactory.create(filter.getCluster(), null)
            relBuilder.push(filter.getInput())
                .filter(condition)
            call.transformTo(relBuilder.build())
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface FilterDateRangeRuleConfig : RelRule.Config {
            @Override
            fun toRule(): FilterDateRangeRule? {
                return FilterDateRangeRule(this)
            }

            companion object {
                val DEFAULT: FilterDateRangeRuleConfig = ImmutableFilterDateRangeRuleConfig.of()
                    .withOperandSupplier { b ->
                        b.operand(Filter::class.java)
                            .predicate { filter: Filter -> containsRoundingExpression(filter) }
                            .anyInputs()
                    }
            }
        }

        companion object {
            /** Whether this an EXTRACT of YEAR, or a call to FLOOR or CEIL.
             * If none of these, we cannot apply the rule.  */
            private fun containsRoundingExpression(filter: Filter): Boolean {
                ExtractFinder.THREAD_INSTANCES.get().use { finder ->
                    assert(
                        requireNonNull(
                            finder,
                            "finder"
                        ).timeUnits.isEmpty() && finder.opKinds.isEmpty()
                    ) { "previous user did not clean up" }
                    filter.getCondition().accept(finder)
                    return (finder.timeUnits.contains(TimeUnitRange.YEAR)
                            || finder.opKinds.contains(SqlKind.FLOOR)
                            || finder.opKinds.contains(SqlKind.CEIL))
                }
            }
        }
    }

    /** Visitor that searches for calls to `EXTRACT`, `FLOOR` or
     * `CEIL`, building a list of distinct time units.  */
    private class ExtractFinder private constructor() : RexVisitorImpl<Void?>(true), AutoCloseable {
        private val timeUnits: Set<TimeUnitRange> = EnumSet.noneOf(TimeUnitRange::class.java)
        private val opKinds: Set<SqlKind> = EnumSet.noneOf(SqlKind::class.java)
        @Override
        fun visitCall(call: RexCall): Void {
            when (call.getKind()) {
                EXTRACT -> {
                    val operand: RexLiteral = call.getOperands().get(0) as RexLiteral
                    timeUnits.add(
                        requireNonNull(
                            operand.getValue()
                        ) { "timeUnitRange is null for $call" } as TimeUnitRange?)
                }
                FLOOR, CEIL ->         // Check that the call to FLOOR/CEIL is on date-time
                    if (call.getOperands().size() === 2) {
                        opKinds.add(call.getKind())
                    }
                else -> {}
            }
            return super.visitCall(call)
        }

        @Override
        fun close() {
            timeUnits.clear()
            opKinds.clear()
        }

        companion object {
            val THREAD_INSTANCES: ThreadLocal<ExtractFinder> = ThreadLocal.withInitial { ExtractFinder() }
        }
    }

    /** Walks over an expression, replacing calls to
     * `EXTRACT`, `FLOOR` and `CEIL` with date ranges.  */
    @VisibleForTesting
    @SuppressWarnings("BetaApi")
    internal class ExtractShuttle @VisibleForTesting constructor(
        rexBuilder: RexBuilder?, timeUnit: TimeUnitRange?,
        operandRanges: Map<RexNode?, RangeSet<Calendar?>?>?,
        timeUnitRanges: ImmutableSortedSet<TimeUnitRange?>?, timeZone: String?
    ) : RexShuttle() {
        private val rexBuilder: RexBuilder
        private val timeUnit: TimeUnitRange
        private val operandRanges: Map<RexNode?, RangeSet<Calendar?>>
        private val calls: Deque<RexCall> = ArrayDeque()
        private val timeUnitRanges: ImmutableSortedSet<TimeUnitRange?>
        private val timeZone: String?

        init {
            this.rexBuilder = requireNonNull(rexBuilder, "rexBuilder")
            this.timeUnit = requireNonNull(timeUnit, "timeUnit")
            this.operandRanges = requireNonNull(operandRanges, "operandRanges")
            this.timeUnitRanges = requireNonNull(timeUnitRanges, "timeUnitRanges")
            this.timeZone = timeZone
        }

        @Override
        fun visitCall(call: RexCall): RexNode {
            return when (call.getKind()) {
                EQUALS, GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL, GREATER_THAN, LESS_THAN -> {
                    val op0: RexNode = call.operands.get(0)
                    val op1: RexNode = call.operands.get(1)
                    when (op0.getKind()) {
                        LITERAL -> {
                            assert(op0 is RexLiteral)
                            if (isExtractCall(op1)) {
                                assert(op1 is RexCall)
                                val subCall: RexCall = op1 as RexCall
                                val operand: RexNode = subCall.getOperands().get(1)
                                if (canRewriteExtract(operand)) {
                                    return compareExtract(
                                        call.getKind().reverse(), operand,
                                        op0 as RexLiteral
                                    )
                                }
                            }
                            if (isFloorCeilCall(op1)) {
                                assert(op1 is RexCall)
                                val subCall: RexCall = op1 as RexCall
                                val flag: RexLiteral = subCall.operands.get(1) as RexLiteral
                                val timeUnit: TimeUnitRange = requireNonNull(
                                    flag.getValue()
                                ) { "timeUnit is null for $subCall" } as TimeUnitRange
                                return compareFloorCeil(
                                    call.getKind().reverse(),
                                    subCall.getOperands().get(0), op0 as RexLiteral,
                                    timeUnit, op1.getKind() === SqlKind.FLOOR
                                )
                            }
                        }
                        else -> {}
                    }
                    when (op1.getKind()) {
                        LITERAL -> {
                            assert(op1 is RexLiteral)
                            if (isExtractCall(op0)) {
                                assert(op0 is RexCall)
                                val subCall: RexCall = op0 as RexCall
                                val operand: RexNode = subCall.operands.get(1)
                                if (canRewriteExtract(operand)) {
                                    return compareExtract(
                                        call.getKind(),
                                        subCall.operands.get(1), op1 as RexLiteral
                                    )
                                }
                            }
                            if (isFloorCeilCall(op0)) {
                                val subCall: RexCall = op0 as RexCall
                                val flag: RexLiteral = subCall.operands.get(1) as RexLiteral
                                val timeUnit: TimeUnitRange = requireNonNull(
                                    flag.getValue()
                                ) { "timeUnit is null for $subCall" } as TimeUnitRange
                                return compareFloorCeil(
                                    call.getKind(),
                                    subCall.getOperands().get(0), op1 as RexLiteral,
                                    timeUnit, op0.getKind() === SqlKind.FLOOR
                                )
                            }
                        }
                        else -> {}
                    }
                    calls.push(call)
                    try {
                        super.visitCall(call)
                    } finally {
                        calls.pop()
                    }
                }
                else -> {
                    calls.push(call)
                    try {
                        super.visitCall(call)
                    } finally {
                        calls.pop()
                    }
                }
            }
        }

        private fun canRewriteExtract(operand: RexNode): Boolean {
            // We rely on timeUnits being sorted (so YEAR comes before MONTH
            // before HOUR) and unique. If we have seen a predicate on YEAR,
            // operandRanges will not be empty. This checks whether we can rewrite
            // the "extract" condition. For example, in the condition
            //
            //   extract(MONTH from time) = someValue
            //   OR extract(YEAR from time) = someValue
            //
            // we cannot rewrite extract on MONTH.
            if (timeUnit === TimeUnitRange.YEAR) {
                return true
            }
            val calendarRangeSet: RangeSet<Calendar?>? = operandRanges[operand]
            if (calendarRangeSet == null || calendarRangeSet.isEmpty()) {
                return false
            }
            for (range in calendarRangeSet.asRanges()) {
                // Cannot reWrite if range does not have an upper or lower bound
                if (!range.hasUpperBound() || !range.hasLowerBound()) {
                    return false
                }
            }
            return true
        }

        @Override
        protected fun visitList(
            exprs: List<RexNode?>,
            update: @Nullable BooleanArray?
        ): List<RexNode?> {
            return if (exprs.isEmpty()) {
                ImmutableList.of() // a bit more efficient
            } else when (calls.getFirst().getKind()) {
                AND -> super.visitList(exprs, update)
                else -> {
                    if (timeUnit !== TimeUnitRange.YEAR) {
                        // Already visited for lower TimeUnit ranges in the loop below.
                        // Early bail out.
                        return exprs
                    }
                    val save: Map<RexNode, RangeSet<Calendar>> = ImmutableMap.copyOf(operandRanges)
                    val clonedOperands: ImmutableList.Builder<RexNode> = ImmutableList.builder()
                    for (operand in exprs) {
                        var clonedOperand: RexNode = operand
                        for (timeUnit in timeUnitRanges) {
                            clonedOperand = clonedOperand.accept(
                                ExtractShuttle(
                                    rexBuilder, timeUnit, operandRanges,
                                    timeUnitRanges, timeZone
                                )
                            )
                        }
                        if (clonedOperand !== operand && update != null) {
                            update[0] = true
                        }
                        clonedOperands.add(clonedOperand)

                        // Restore the state. For an operator such as "OR", an argument
                        // cannot inherit the previous argument's state.
                        operandRanges.clear()
                        operandRanges.putAll(save)
                    }
                    clonedOperands.build()
                }
            }
        }

        fun isExtractCall(e: RexNode): Boolean {
            return when (e.getKind()) {
                EXTRACT -> {
                    val call: RexCall = e as RexCall
                    val flag: RexLiteral = call.operands.get(0) as RexLiteral
                    val timeUnit: TimeUnitRange = flag.getValue() as TimeUnitRange
                    timeUnit === this.timeUnit
                }
                else -> false
            }
        }

        fun compareExtract(
            comparison: SqlKind, operand: RexNode,
            literal: RexLiteral?
        ): RexNode {
            var rangeSet: RangeSet<Calendar?>? = operandRanges[operand]
            if (rangeSet == null) {
                rangeSet = ImmutableRangeSet.< Calendar > of < Calendar ? > ().complement()
            }
            val s2: RangeSet<Calendar> = TreeRangeSet.create()
            // Calendar.MONTH is 0-based
            val v: Int = (RexLiteral.intValue(literal)
                    - if (timeUnit === TimeUnitRange.MONTH) 1 else 0)
            if (!isValid(v, timeUnit)) {
                // Comparison with an invalid value for timeUnit, always false.
                return rexBuilder.makeLiteral(false)
            }
            for (r in rangeSet.asRanges()) {
                val c: Calendar
                when (timeUnit) {
                    YEAR -> {
                        c = Util.calendar()
                        c.clear()
                        c.set(v, Calendar.JANUARY, 1)
                        s2.add(extractRange(timeUnit, comparison, c))
                    }
                    MONTH, DAY, HOUR, MINUTE, SECOND -> if (r.hasLowerBound() && r.hasUpperBound()) {
                        c = r.lowerEndpoint().clone() as Calendar
                        var i = 0
                        while (next(c, timeUnit, v, r, i++ > 0)) {
                            s2.add(extractRange(timeUnit, comparison, c))
                        }
                    }
                    else -> {}
                }
            }
            // Intersect old range set with new.
            s2.removeAll(rangeSet.complement())
            operandRanges.put(operand, ImmutableRangeSet.copyOf(s2))
            val nodes: List<RexNode> = ArrayList()
            for (r in s2.asRanges()) {
                nodes.add(toRex(operand, r))
            }
            return RexUtil.composeDisjunction(rexBuilder, nodes)
        }

        private fun toRex(operand: RexNode, r: Range<Calendar>): RexNode {
            val nodes: List<RexNode> = ArrayList()
            if (r.hasLowerBound()) {
                val op: SqlBinaryOperator =
                    if (r.lowerBoundType() === BoundType.CLOSED) SqlStdOperatorTable.GREATER_THAN_OR_EQUAL else SqlStdOperatorTable.GREATER_THAN
                nodes.add(
                    rexBuilder.makeCall(
                        op, operand,
                        dateTimeLiteral(rexBuilder, r.lowerEndpoint(), operand)
                    )
                )
            }
            if (r.hasUpperBound()) {
                val op: SqlBinaryOperator =
                    if (r.upperBoundType() === BoundType.CLOSED) SqlStdOperatorTable.LESS_THAN_OR_EQUAL else SqlStdOperatorTable.LESS_THAN
                nodes.add(
                    rexBuilder.makeCall(
                        op, operand,
                        dateTimeLiteral(rexBuilder, r.upperEndpoint(), operand)
                    )
                )
            }
            return RexUtil.composeConjunction(rexBuilder, nodes)
        }

        private fun dateTimeLiteral(
            rexBuilder: RexBuilder, calendar: Calendar,
            operand: RexNode
        ): RexLiteral {
            val ts: TimestampString
            val p: Int
            return when (operand.getType().getSqlTypeName()) {
                TIMESTAMP -> {
                    ts = TimestampString.fromCalendarFields(calendar)
                    p = operand.getType().getPrecision()
                    rexBuilder.makeTimestampLiteral(ts, p)
                }
                TIMESTAMP_WITH_LOCAL_TIME_ZONE -> {
                    ts = TimestampString.fromCalendarFields(calendar)
                    val tz: TimeZone = TimeZone.getTimeZone(timeZone)
                    val localTs: TimestampString = TimestampWithTimeZoneString(ts, tz)
                        .withTimeZone(DateTimeUtils.UTC_ZONE)
                        .getLocalTimestampString()
                    p = operand.getType().getPrecision()
                    rexBuilder.makeTimestampWithLocalTimeZoneLiteral(localTs, p)
                }
                DATE -> {
                    val d: DateString = DateString.fromCalendarFields(calendar)
                    rexBuilder.makeDateLiteral(d)
                }
                else -> throw Util.unexpected(operand.getType().getSqlTypeName())
            }
        }

        private fun compareFloorCeil(
            comparison: SqlKind, operand: RexNode,
            timeLiteral: RexLiteral, timeUnit: TimeUnitRange, floor: Boolean
        ): RexNode {
            var rangeSet: RangeSet<Calendar?>? = operandRanges[operand]
            if (rangeSet == null) {
                rangeSet = ImmutableRangeSet.< Calendar > of < Calendar ? > ().complement()
            }
            val s2: RangeSet<Calendar> = TreeRangeSet.create()
            val c: Calendar = timestampValue(timeLiteral)
            val range: Range<Calendar> =
                if (floor) floorRange(timeUnit, comparison, c) else ceilRange(timeUnit, comparison, c)
            s2.add(range)
            // Intersect old range set with new.
            s2.removeAll(rangeSet.complement())
            operandRanges.put(operand, ImmutableRangeSet.copyOf(s2))
            return if (range.isEmpty()) {
                rexBuilder.makeLiteral(false)
            } else toRex(operand, range)
        }

        private fun timestampValue(timeLiteral: RexLiteral): Calendar {
            return when (timeLiteral.getTypeName()) {
                TIMESTAMP_WITH_LOCAL_TIME_ZONE -> {
                    val tz: TimeZone = TimeZone.getTimeZone(timeZone)
                    Util.calendar(
                        SqlFunctions.timestampWithLocalTimeZoneToTimestamp(
                            requireNonNull(
                                timeLiteral.getValueAs(Long::class.java),
                                "timeLiteral.getValueAs(Long.class)"
                            ), tz
                        )
                    )
                }
                TIMESTAMP -> Util.calendar(
                    requireNonNull(
                        timeLiteral.getValueAs(Long::class.java),
                        "timeLiteral.getValueAs(Long.class)"
                    )
                )
                DATE -> {
                    // Cast date to timestamp with local time zone
                    val d: DateString = requireNonNull(
                        timeLiteral.getValueAs(DateString::class.java),
                        "timeLiteral.getValueAs(DateString.class)"
                    )
                    Util.calendar(d.getMillisSinceEpoch())
                }
                else -> throw Util.unexpected(timeLiteral.getTypeName())
            }
        }

        fun isFloorCeilCall(e: RexNode): Boolean {
            return when (e.getKind()) {
                FLOOR, CEIL -> {
                    val call: RexCall = e as RexCall
                    call.getOperands().size() === 2
                }
                else -> false
            }
        }

        companion object {
            // Assumes v is a valid value for given timeunit
            private fun next(
                c: Calendar, timeUnit: TimeUnitRange, v: Int,
                r: Range<Calendar>, strict: Boolean
            ): Boolean {
                val original: Calendar = c.clone() as Calendar
                val code = calendarUnitFor(timeUnit)
                while (true) {
                    c.set(code, v)
                    val v2: Int = c.get(code)
                    if (v2 < v) {
                        // E.g. when we set DAY=30 on 2014-02-01, we get 2014-02-30 because
                        // February has 28 days.
                        continue
                    }
                    if (strict && original.compareTo(c) === 0) {
                        c.add(
                            calendarUnitFor(
                                requireNonNull(
                                    TIME_UNIT_PARENTS[timeUnit]
                                ) { "TIME_UNIT_PARENTS.get(timeUnit) is null for $timeUnit" }), 1
                        )
                        continue
                    }
                    return if (!r.contains(c)) {
                        false
                    } else true
                }
            }

            private fun isValid(v: Int, timeUnit: TimeUnitRange): Boolean {
                return when (timeUnit) {
                    YEAR -> v > 0
                    MONTH -> v >= Calendar.JANUARY && v <= Calendar.DECEMBER
                    DAY -> v > 0 && v <= 31
                    HOUR -> v >= 0 && v <= 24
                    MINUTE, SECOND -> v >= 0 && v <= 60
                    else -> false
                }
            }

            private fun extractRange(
                timeUnit: TimeUnitRange, comparison: SqlKind,
                c: Calendar
            ): Range<Calendar> {
                return when (comparison) {
                    EQUALS -> Range.closedOpen(
                        round(c, timeUnit, true),
                        round(c, timeUnit, false)
                    )
                    LESS_THAN -> Range.lessThan(
                        round(
                            c,
                            timeUnit,
                            true
                        )
                    )
                    LESS_THAN_OR_EQUAL -> Range.lessThan(
                        round(
                            c,
                            timeUnit,
                            false
                        )
                    )
                    GREATER_THAN -> Range.atLeast(
                        round(
                            c,
                            timeUnit,
                            false
                        )
                    )
                    GREATER_THAN_OR_EQUAL -> Range.atLeast(
                        round(
                            c,
                            timeUnit,
                            true
                        )
                    )
                    else -> throw AssertionError(comparison)
                }
            }

            /** Returns a copy of a calendar, optionally rounded up to the next time
             * unit.  */
            private fun round(c: Calendar, timeUnit: TimeUnitRange, down: Boolean): Calendar {
                var c: Calendar = c
                c = c.clone() as Calendar
                if (!down) {
                    val code: Integer = calendarUnitFor(timeUnit)
                    val v: Int = c.get(code)
                    c.set(code, v + 1)
                }
                return c
            }

            private fun floorRange(
                timeUnit: TimeUnitRange, comparison: SqlKind,
                c: Calendar
            ): Range<Calendar> {
                val floor: Calendar = floor(c, timeUnit)
                val boundary: Boolean = floor.equals(c)
                return when (comparison) {
                    EQUALS -> Range.closedOpen(
                        floor,
                        if (boundary) increment(
                            floor,
                            timeUnit
                        ) else floor
                    )
                    LESS_THAN -> if (boundary) Range.lessThan(floor) else Range.lessThan(
                        increment(
                            floor,
                            timeUnit
                        )
                    )
                    LESS_THAN_OR_EQUAL -> Range.lessThan(
                        increment(
                            floor,
                            timeUnit
                        )
                    )
                    GREATER_THAN -> Range.atLeast(
                        increment(
                            floor,
                            timeUnit
                        )
                    )
                    GREATER_THAN_OR_EQUAL -> if (boundary) Range.atLeast(floor) else Range.atLeast(
                        increment(
                            floor,
                            timeUnit
                        )
                    )
                    else -> throw Util.unexpected(comparison)
                }
            }

            private fun ceilRange(
                timeUnit: TimeUnitRange, comparison: SqlKind,
                c: Calendar
            ): Range<Calendar> {
                val ceil: Calendar = ceil(c, timeUnit)
                val boundary: Boolean = ceil.equals(c)
                return when (comparison) {
                    EQUALS -> Range.openClosed(
                        if (boundary) decrement(
                            ceil,
                            timeUnit
                        ) else ceil, ceil
                    )
                    LESS_THAN -> Range.atMost(
                        decrement(
                            ceil,
                            timeUnit
                        )
                    )
                    LESS_THAN_OR_EQUAL -> if (boundary) Range.atMost(ceil) else Range.atMost(
                        decrement(
                            ceil,
                            timeUnit
                        )
                    )
                    GREATER_THAN -> if (boundary) Range.greaterThan(ceil) else Range.greaterThan(
                        decrement(
                            ceil,
                            timeUnit
                        )
                    )
                    GREATER_THAN_OR_EQUAL -> Range.greaterThan(
                        decrement(
                            ceil,
                            timeUnit
                        )
                    )
                    else -> throw Util.unexpected(comparison)
                }
            }

            private fun increment(c: Calendar, timeUnit: TimeUnitRange): Calendar {
                var c: Calendar = c
                c = c.clone() as Calendar
                c.add(calendarUnitFor(timeUnit), 1)
                return c
            }

            private fun decrement(c: Calendar, timeUnit: TimeUnitRange): Calendar {
                var c: Calendar = c
                c = c.clone() as Calendar
                c.add(calendarUnitFor(timeUnit), -1)
                return c
            }

            private fun ceil(c: Calendar, timeUnit: TimeUnitRange): Calendar {
                val floor: Calendar = floor(c, timeUnit)
                return if (floor.equals(c)) floor else increment(floor, timeUnit)
            }

            /**
             * Computes floor of a calendar to a given time unit.
             *
             * @return returns a copy of calendar, floored to the given time unit
             */
            private fun floor(c: Calendar, timeUnit: TimeUnitRange): Calendar {
                var c: Calendar = c
                c = c.clone() as Calendar
                when (timeUnit) {
                    YEAR -> {
                        c.set(calendarUnitFor(TimeUnitRange.MONTH), Calendar.JANUARY)
                        c.set(calendarUnitFor(TimeUnitRange.DAY), 1)
                        c.set(calendarUnitFor(TimeUnitRange.HOUR), 0)
                        c.set(calendarUnitFor(TimeUnitRange.MINUTE), 0)
                        c.set(calendarUnitFor(TimeUnitRange.SECOND), 0)
                        c.set(calendarUnitFor(TimeUnitRange.MILLISECOND), 0)
                    }
                    MONTH -> {
                        c.set(calendarUnitFor(TimeUnitRange.DAY), 1)
                        c.set(calendarUnitFor(TimeUnitRange.HOUR), 0)
                        c.set(calendarUnitFor(TimeUnitRange.MINUTE), 0)
                        c.set(calendarUnitFor(TimeUnitRange.SECOND), 0)
                        c.set(calendarUnitFor(TimeUnitRange.MILLISECOND), 0)
                    }
                    DAY -> {
                        c.set(calendarUnitFor(TimeUnitRange.HOUR), 0)
                        c.set(calendarUnitFor(TimeUnitRange.MINUTE), 0)
                        c.set(calendarUnitFor(TimeUnitRange.SECOND), 0)
                        c.set(calendarUnitFor(TimeUnitRange.MILLISECOND), 0)
                    }
                    HOUR -> {
                        c.set(calendarUnitFor(TimeUnitRange.MINUTE), 0)
                        c.set(calendarUnitFor(TimeUnitRange.SECOND), 0)
                        c.set(calendarUnitFor(TimeUnitRange.MILLISECOND), 0)
                    }
                    MINUTE -> {
                        c.set(calendarUnitFor(TimeUnitRange.SECOND), 0)
                        c.set(calendarUnitFor(TimeUnitRange.MILLISECOND), 0)
                    }
                    SECOND -> c.set(calendarUnitFor(TimeUnitRange.MILLISECOND), 0)
                    else -> {}
                }
                return c
            }
        }
    }
}
