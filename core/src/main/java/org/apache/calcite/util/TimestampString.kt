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
 * Timestamp literal.
 *
 *
 * Immutable, internally represented as a string (in ISO format),
 * and can support unlimited precision (milliseconds, nanoseconds).
 */
class TimestampString(val v: String) : Comparable<TimestampString?> {
    /** Creates a TimeString.  */
    init {
        Preconditions.checkArgument(PATTERN.matcher(v).matches(), v)
    }

    /** Creates a TimestampString for year, month, day, hour, minute, second,
     * millisecond values.  */
    constructor(
        year: Int,
        month: Int,
        day: Int,
        h: Int,
        m: Int,
        s: Int
    ) : this(DateTimeStringUtils.ymdhms(StringBuilder(), year, month, day, h, m, s).toString()) {
    }

    /** Sets the fraction field of a `TimestampString` to a given number
     * of milliseconds. Nukes the value set via [.withNanos].
     *
     *
     * For example,
     * `new TimestampString(1970, 1, 1, 2, 3, 4).withMillis(56)`
     * yields `TIMESTAMP '1970-01-01 02:03:04.056'`.  */
    fun withMillis(millis: Int): TimestampString {
        Preconditions.checkArgument(millis >= 0 && millis < 1000)
        return withFraction(DateTimeStringUtils.pad(3, millis))
    }

    /** Sets the fraction field of a `TimestampString` to a given number
     * of nanoseconds. Nukes the value set via [.withMillis].
     *
     *
     * For example,
     * `new TimestampString(1970, 1, 1, 2, 3, 4).withNanos(56789)`
     * yields `TIMESTAMP '1970-01-01 02:03:04.000056789'`.  */
    fun withNanos(nanos: Int): TimestampString {
        Preconditions.checkArgument(nanos >= 0 && nanos < 1000000000)
        return withFraction(DateTimeStringUtils.pad(9, nanos))
    }

    /** Sets the fraction field of a `TimestampString`.
     * The precision is determined by the number of leading zeros.
     * Trailing zeros are stripped.
     *
     *
     * For example,
     * `new TimestampString(1970, 1, 1, 2, 3, 4).withFraction("00506000")`
     * yields `TIMESTAMP '1970-01-01 02:03:04.00506'`.  */
    fun withFraction(fraction: String): TimestampString {
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
        return TimestampString(v)
    }

    @Override
    override fun toString(): String {
        return v
    }

    @Override
    override fun equals(@Nullable o: Object): Boolean {
        // The value is in canonical form (no trailing zeros).
        return (o === this
                || o is TimestampString
                && (o as TimestampString).v.equals(v))
    }

    @Override
    override fun hashCode(): Int {
        return v.hashCode()
    }

    @Override
    operator fun compareTo(o: TimestampString): Int {
        return v.compareTo(o.v)
    }

    fun round(precision: Int): TimestampString {
        Preconditions.checkArgument(precision >= 0)
        val targetLength = 20 + precision
        if (v.length() <= targetLength) {
            return this
        }
        var v: String = v.substring(0, targetLength)
        while (v.length() >= 20 && (v.endsWith("0") || v.endsWith("."))) {
            v = v.substring(0, v.length() - 1)
        }
        return TimestampString(v)
    }

    /** Returns the number of milliseconds since the epoch.  */
    val millisSinceEpoch: Long
        get() {
            val year: Int = Integer.valueOf(v.substring(0, 4))
            val month: Int = Integer.valueOf(v.substring(5, 7))
            val day: Int = Integer.valueOf(v.substring(8, 10))
            val h: Int = Integer.valueOf(v.substring(11, 13))
            val m: Int = Integer.valueOf(v.substring(14, 16))
            val s: Int = Integer.valueOf(v.substring(17, 19))
            val ms = millisInSecond
            val d: Int = DateTimeUtils.ymdToUnixDate(year, month, day)
            return d * DateTimeUtils.MILLIS_PER_DAY + h * DateTimeUtils.MILLIS_PER_HOUR + m * DateTimeUtils.MILLIS_PER_MINUTE + s * DateTimeUtils.MILLIS_PER_SECOND + ms
        }
    private val millisInSecond: Int
        private get() = when (v.length()) {
            19 -> 0
            21 -> Integer.valueOf(v.substring(20)) * 100
            22 -> Integer.valueOf(v.substring(20)) * 10
            23 -> Integer.valueOf(v.substring(20, 23))
            else -> Integer.valueOf(v.substring(20, 23))
        }

    fun toCalendar(): Calendar {
        return Util.calendar(millisSinceEpoch)
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
        return if (v.length() < 20) 0 else v.length() - 20
    }

    companion object {
        private val PATTERN: Pattern = Pattern.compile(
            "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]"
                    + " "
                    + "[0-9][0-9]:[0-9][0-9]:[0-9][0-9](\\.[0-9]*[1-9])?"
        )

        /** Creates a TimestampString from a Calendar.  */
        fun fromCalendarFields(calendar: Calendar): TimestampString {
            return TimestampString(
                calendar.get(Calendar.YEAR),
                calendar.get(Calendar.MONTH) + 1,
                calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.HOUR_OF_DAY),
                calendar.get(Calendar.MINUTE),
                calendar.get(Calendar.SECOND)
            )
                .withMillis(calendar.get(Calendar.MILLISECOND))
        }

        /** Creates a TimestampString that is a given number of milliseconds since
         * the epoch.  */
        fun fromMillisSinceEpoch(millis: Long): TimestampString {
            return TimestampString(DateTimeUtils.unixTimestampToString(millis))
                .withMillis(DateTimeUtils.floorMod(millis, 1000) as Int)
        }
    }
}
