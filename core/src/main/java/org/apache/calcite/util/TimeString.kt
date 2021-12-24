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

import org.apache.calcite.avatica.util.DateTimeUtils
import com.google.common.base.Preconditions
import com.google.common.base.Strings
import java.util.Calendar
import java.util.regex.Pattern

/**
 * Time literal.
 *
 *
 * Immutable, internally represented as a string (in ISO format),
 * and can support unlimited precision (milliseconds, nanoseconds).
 */
class TimeString
/** Internal constructor, no validation.  */ private constructor(
    val v: String,
    @SuppressWarnings("unused") ignore: Boolean
) : Comparable<TimeString?> {
    /** Creates a TimeString.  */
    @SuppressWarnings("method.invocation.invalid")
    constructor(v: String) : this(v, false) {
        Preconditions.checkArgument(
            PATTERN.matcher(v).matches(),
            "Invalid time format:", v
        )
        Preconditions.checkArgument(
            hour >= 0 && hour < 24,
            "Hour out of range:", hour
        )
        Preconditions.checkArgument(
            minute >= 0 && minute < 60,
            "Minute out of range:", minute
        )
        Preconditions.checkArgument(
            second >= 0 && second < 60,
            "Second out of range:", second
        )
    }

    /** Creates a TimeString for hour, minute, second and millisecond values.  */
    constructor(h: Int, m: Int, s: Int) : this(hms(h, m, s), false) {}

    /** Sets the fraction field of a `TimeString` to a given number
     * of milliseconds. Nukes the value set via [.withNanos].
     *
     *
     * For example,
     * `new TimeString(1970, 1, 1, 2, 3, 4).withMillis(56)`
     * yields `TIME '1970-01-01 02:03:04.056'`.  */
    fun withMillis(millis: Int): TimeString {
        Preconditions.checkArgument(millis >= 0 && millis < 1000)
        return withFraction(DateTimeStringUtils.pad(3, millis))
    }

    /** Sets the fraction field of a `TimeString` to a given number
     * of nanoseconds. Nukes the value set via [.withMillis].
     *
     *
     * For example,
     * `new TimeString(1970, 1, 1, 2, 3, 4).withNanos(56789)`
     * yields `TIME '1970-01-01 02:03:04.000056789'`.  */
    fun withNanos(nanos: Int): TimeString {
        Preconditions.checkArgument(nanos >= 0 && nanos < 1000000000)
        return withFraction(DateTimeStringUtils.pad(9, nanos))
    }

    /** Sets the fraction field of a `TimeString`.
     * The precision is determined by the number of leading zeros.
     * Trailing zeros are stripped.
     *
     *
     * For example,
     * `new TimeString(1970, 1, 1, 2, 3, 4).withFraction("00506000")`
     * yields `TIME '1970-01-01 02:03:04.00506'`.  */
    fun withFraction(fraction: String): TimeString {
        var fraction = fraction
        var v = v
        val i: Int = v.indexOf('.')
        if (i >= 0) {
            v = v.substring(0, i)
        }
        while (fraction.endsWith("0")) {
            fraction = fraction.substring(0, fraction.length() - 1)
        }
        if (fraction.length() > 0) {
            v = "$v.$fraction"
        }
        return TimeString(v)
    }

    @Override
    override fun toString(): String {
        return v
    }

    @Override
    override fun equals(@Nullable o: Object): Boolean {
        // The value is in canonical form (no trailing zeros).
        return (o === this
                || o is TimeString
                && (o as TimeString).v.equals(v))
    }

    @Override
    override fun hashCode(): Int {
        return v.hashCode()
    }

    @Override
    operator fun compareTo(o: TimeString): Int {
        return v.compareTo(o.v)
    }

    fun round(precision: Int): TimeString {
        Preconditions.checkArgument(precision >= 0)
        val targetLength = 9 + precision
        if (v.length() <= targetLength) {
            return this
        }
        var v: String = v.substring(0, targetLength)
        while (v.length() >= 9 && (v.endsWith("0") || v.endsWith("."))) {
            v = v.substring(0, v.length() - 1)
        }
        return TimeString(v)
    }

    val millisOfDay: Int
        get() {
            val h: Int = Integer.valueOf(v.substring(0, 2))
            val m: Int = Integer.valueOf(v.substring(3, 5))
            val s: Int = Integer.valueOf(v.substring(6, 8))
            val ms = millisInSecond
            return (h * DateTimeUtils.MILLIS_PER_HOUR + m * DateTimeUtils.MILLIS_PER_MINUTE + s * DateTimeUtils.MILLIS_PER_SECOND + ms)
        }
    private val millisInSecond: Int
        private get() = when (v.length()) {
            8 -> 0
            10 -> Integer.valueOf(v.substring(9)) * 100
            11 -> Integer.valueOf(v.substring(9)) * 10
            12 -> Integer.valueOf(v.substring(9, 12))
            else -> Integer.valueOf(v.substring(9, 12))
        }
    private val hour: Int
        private get() = Integer.parseInt(v.substring(0, 2))
    private val minute: Int
        private get() = Integer.parseInt(v.substring(3, 5))
    private val second: Int
        private get() = Integer.parseInt(v.substring(6, 8))

    fun toCalendar(): Calendar {
        return Util.calendar(millisOfDay)
    }

    /** Converts this TimestampString to a string, truncated or padded with
     * zeros to a given precision.  */
    fun toString(precision: Int): String {
        Preconditions.checkArgument(precision >= 0)
        val p = precision()
        if (precision < p) {
            return round(precision).toString(precision)
        }
        if (precision > p) {
            var s = v
            if (p == 0) {
                s += "."
            }
            return s + Strings.repeat("0", precision - p)
        }
        return v
    }

    private fun precision(): Int {
        return if (v.length() < 9) 0 else v.length() - 9
    }

    companion object {
        private val PATTERN: Pattern = Pattern.compile("[0-9][0-9]:[0-9][0-9]:[0-9][0-9](\\.[0-9]*[1-9])?")

        /** Validates an hour-minute-second value and converts to a string.  */
        private fun hms(h: Int, m: Int, s: Int): String {
            Preconditions.checkArgument(h >= 0 && h < 24, "Hour out of range:", h)
            Preconditions.checkArgument(m >= 0 && m < 60, "Minute out of range:", m)
            Preconditions.checkArgument(s >= 0 && s < 60, "Second out of range:", s)
            val b = StringBuilder()
            DateTimeStringUtils.hms(b, h, m, s)
            return b.toString()
        }

        /** Creates a TimeString from a Calendar.  */
        fun fromCalendarFields(calendar: Calendar): TimeString {
            return TimeString(
                calendar.get(Calendar.HOUR_OF_DAY),
                calendar.get(Calendar.MINUTE),
                calendar.get(Calendar.SECOND)
            )
                .withMillis(calendar.get(Calendar.MILLISECOND))
        }

        fun fromMillisOfDay(i: Int): TimeString {
            return TimeString(DateTimeUtils.unixTimeToString(i))
                .withMillis(DateTimeUtils.floorMod(i, 1000) as Int)
        }
    }
}
