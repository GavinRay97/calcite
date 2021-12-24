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
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Locale
import java.util.TimeZone

/**
 * Timestamp with time-zone literal.
 *
 *
 * Immutable, internally represented as a string (in ISO format),
 * and can support unlimited precision (milliseconds, nanoseconds).
 */
class TimestampWithTimeZoneString : Comparable<TimestampWithTimeZoneString?> {
    val localDateTime: TimestampString
    val timeZone: TimeZone
    val v: String

    /** Creates a TimestampWithTimeZoneString.  */
    constructor(localDateTime: TimestampString, timeZone: TimeZone) {
        this.localDateTime = localDateTime
        this.timeZone = timeZone
        v = localDateTime.toString() + " " + timeZone.getID()
    }

    /** Creates a TimestampWithTimeZoneString.  */
    constructor(v: String) {
        localDateTime = TimestampString(v.substring(0, v.indexOf(' ', 11)))
        val timeZoneString: String = v.substring(v.indexOf(' ', 11) + 1)
        Preconditions.checkArgument(DateTimeStringUtils.isValidTimeZone(timeZoneString))
        timeZone = TimeZone.getTimeZone(timeZoneString)
        this.v = v
    }

    /** Creates a TimestampWithTimeZoneString for year, month, day, hour, minute, second,
     * millisecond values in the given time-zone.  */
    constructor(
        year: Int, month: Int, day: Int, h: Int, m: Int, s: Int,
        timeZone: String
    ) : this(
        DateTimeStringUtils.ymdhms(StringBuilder(), year, month, day, h, m, s).toString()
                + " " + timeZone
    ) {
    }

    /** Sets the fraction field of a `TimestampWithTimeZoneString` to a given number
     * of milliseconds. Nukes the value set via [.withNanos].
     *
     *
     * For example,
     * `new TimestampWithTimeZoneString(1970, 1, 1, 2, 3, 4, "GMT").withMillis(56)`
     * yields `TIMESTAMP WITH LOCAL TIME ZONE '1970-01-01 02:03:04.056 GMT'`.  */
    fun withMillis(millis: Int): TimestampWithTimeZoneString {
        Preconditions.checkArgument(millis >= 0 && millis < 1000)
        return withFraction(DateTimeStringUtils.pad(3, millis))
    }

    /** Sets the fraction field of a `TimestampWithTimeZoneString` to a given number
     * of nanoseconds. Nukes the value set via [.withMillis].
     *
     *
     * For example,
     * `new TimestampWithTimeZoneString(1970, 1, 1, 2, 3, 4, "GMT").withNanos(56789)`
     * yields `TIMESTAMP WITH LOCAL TIME ZONE '1970-01-01 02:03:04.000056789 GMT'`.  */
    fun withNanos(nanos: Int): TimestampWithTimeZoneString {
        Preconditions.checkArgument(nanos >= 0 && nanos < 1000000000)
        return withFraction(DateTimeStringUtils.pad(9, nanos))
    }

    /** Sets the fraction field of a `TimestampString`.
     * The precision is determined by the number of leading zeros.
     * Trailing zeros are stripped.
     *
     *
     * For example, `new TimestampWithTimeZoneString(1970, 1, 1, 2, 3, 4, "GMT").withFraction("00506000")`
     * yields `TIMESTAMP WITH LOCAL TIME ZONE '1970-01-01 02:03:04.00506 GMT'`.  */
    fun withFraction(fraction: String): TimestampWithTimeZoneString {
        return TimestampWithTimeZoneString(
            localDateTime.withFraction(fraction), timeZone
        )
    }

    fun withTimeZone(timeZone: TimeZone): TimestampWithTimeZoneString {
        if (this.timeZone.equals(timeZone)) {
            return this
        }
        val localDateTimeString: String = localDateTime.toString()
        val v: String
        val fraction: String?
        val i: Int = localDateTimeString.indexOf('.')
        if (i >= 0) {
            v = localDateTimeString.substring(0, i)
            fraction = localDateTimeString.substring(i + 1)
        } else {
            v = localDateTimeString
            fraction = null
        }
        val pt: DateTimeUtils.PrecisionTime = DateTimeUtils.parsePrecisionDateTimeLiteral(
            v,
            SimpleDateFormat(DateTimeUtils.TIMESTAMP_FORMAT_STRING, Locale.ROOT),
            this.timeZone, -1
        )
        pt.getCalendar().setTimeZone(timeZone)
        return if (fraction != null) {
            TimestampWithTimeZoneString(
                pt.getCalendar().get(Calendar.YEAR),
                pt.getCalendar().get(Calendar.MONTH) + 1,
                pt.getCalendar().get(Calendar.DAY_OF_MONTH),
                pt.getCalendar().get(Calendar.HOUR_OF_DAY),
                pt.getCalendar().get(Calendar.MINUTE),
                pt.getCalendar().get(Calendar.SECOND),
                timeZone.getID()
            )
                .withFraction(fraction)
        } else TimestampWithTimeZoneString(
            pt.getCalendar().get(Calendar.YEAR),
            pt.getCalendar().get(Calendar.MONTH) + 1,
            pt.getCalendar().get(Calendar.DAY_OF_MONTH),
            pt.getCalendar().get(Calendar.HOUR_OF_DAY),
            pt.getCalendar().get(Calendar.MINUTE),
            pt.getCalendar().get(Calendar.SECOND),
            timeZone.getID()
        )
    }

    @Override
    override fun toString(): String {
        return v
    }

    @Override
    override fun equals(@Nullable o: Object): Boolean {
        // The value is in canonical form (no trailing zeros).
        return (o === this
                || o is TimestampWithTimeZoneString
                && (o as TimestampWithTimeZoneString).v.equals(v))
    }

    @Override
    override fun hashCode(): Int {
        return v.hashCode()
    }

    @Override
    operator fun compareTo(o: TimestampWithTimeZoneString): Int {
        return v.compareTo(o.v)
    }

    fun round(precision: Int): TimestampWithTimeZoneString {
        Preconditions.checkArgument(precision >= 0)
        return TimestampWithTimeZoneString(
            localDateTime.round(precision), timeZone
        )
    }

    /** Converts this TimestampWithTimeZoneString to a string, truncated or padded with
     * zeros to a given precision.  */
    fun toString(precision: Int): String {
        Preconditions.checkArgument(precision >= 0)
        return localDateTime.toString(precision) + " " + timeZone.getID()
    }

    val localDateString: DateString
        get() = DateString(localDateTime.toString().substring(0, 10))
    val localTimeString: org.apache.calcite.util.TimeString
        get() = TimeString(localDateTime.toString().substring(11))
    val localTimestampString: org.apache.calcite.util.TimestampString
        get() = localDateTime

    companion object {
        /** Creates a TimestampWithTimeZoneString that is a given number of milliseconds since
         * the epoch UTC.  */
        fun fromMillisSinceEpoch(millis: Long): TimestampWithTimeZoneString {
            return TimestampWithTimeZoneString(
                DateTimeUtils.unixTimestampToString(millis) + " " + DateTimeUtils.UTC_ZONE.getID()
            )
                .withMillis(DateTimeUtils.floorMod(millis, 1000) as Int)
        }
    }
}
