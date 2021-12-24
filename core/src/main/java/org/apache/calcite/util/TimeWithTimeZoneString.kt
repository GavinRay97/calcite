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
 * Time with time-zone literal.
 *
 *
 * Immutable, internally represented as a string (in ISO format),
 * and can support unlimited precision (milliseconds, nanoseconds).
 */
class TimeWithTimeZoneString : Comparable<TimeWithTimeZoneString?> {
    val localTime: TimeString
    val timeZone: TimeZone
    val v: String

    /** Creates a TimeWithTimeZoneString.  */
    constructor(localTime: TimeString, timeZone: TimeZone) {
        this.localTime = localTime
        this.timeZone = timeZone
        v = localTime.toString() + " " + timeZone.getID()
    }

    /** Creates a TimeWithTimeZoneString.  */
    constructor(v: String) {
        localTime = TimeString(v.substring(0, 8))
        val timeZoneString: String = v.substring(9)
        Preconditions.checkArgument(DateTimeStringUtils.isValidTimeZone(timeZoneString))
        timeZone = TimeZone.getTimeZone(timeZoneString)
        this.v = v
    }

    /** Creates a TimeWithTimeZoneString for hour, minute, second and millisecond values
     * in the given time-zone.  */
    constructor(h: Int, m: Int, s: Int, timeZone: String) : this(
        DateTimeStringUtils.hms(StringBuilder(), h, m, s).toString() + " " + timeZone
    ) {
    }

    /** Sets the fraction field of a `TimeWithTimeZoneString` to a given number
     * of milliseconds. Nukes the value set via [.withNanos].
     *
     *
     * For example,
     * `new TimeWithTimeZoneString(1970, 1, 1, 2, 3, 4, "UTC").withMillis(56)`
     * yields `TIME WITH LOCAL TIME ZONE '1970-01-01 02:03:04.056 UTC'`.  */
    fun withMillis(millis: Int): TimeWithTimeZoneString {
        Preconditions.checkArgument(millis >= 0 && millis < 1000)
        return withFraction(DateTimeStringUtils.pad(3, millis))
    }

    /** Sets the fraction field of a `TimeString` to a given number
     * of nanoseconds. Nukes the value set via [.withMillis].
     *
     *
     * For example,
     * `new TimeWithTimeZoneString(1970, 1, 1, 2, 3, 4, "UTC").withNanos(56789)`
     * yields `TIME WITH LOCAL TIME ZONE '1970-01-01 02:03:04.000056789 UTC'`.  */
    fun withNanos(nanos: Int): TimeWithTimeZoneString {
        Preconditions.checkArgument(nanos >= 0 && nanos < 1000000000)
        return withFraction(DateTimeStringUtils.pad(9, nanos))
    }

    /** Sets the fraction field of a `TimeWithTimeZoneString`.
     * The precision is determined by the number of leading zeros.
     * Trailing zeros are stripped.
     *
     *
     * For example,
     * `new TimeWithTimeZoneString(1970, 1, 1, 2, 3, 4, "UTC").withFraction("00506000")`
     * yields `TIME WITH LOCAL TIME ZONE '1970-01-01 02:03:04.00506 UTC'`.  */
    fun withFraction(fraction: String): TimeWithTimeZoneString {
        var fraction = fraction
        var v = v
        val i: Int = v.indexOf('.')
        if (i >= 0) {
            v = v.substring(0, i)
        } else {
            v = v.substring(0, 8)
        }
        while (fraction.endsWith("0")) {
            fraction = fraction.substring(0, fraction.length() - 1)
        }
        if (fraction.length() > 0) {
            v = "$v.$fraction"
        }
        v = v + this.v.substring(8) // time-zone
        return TimeWithTimeZoneString(v)
    }

    fun withTimeZone(timeZone: TimeZone): TimeWithTimeZoneString {
        if (this.timeZone.equals(timeZone)) {
            return this
        }
        val localTimeString: String = localTime.toString()
        val v: String
        val fraction: String?
        val i: Int = localTimeString.indexOf('.')
        if (i >= 0) {
            v = localTimeString.substring(0, i)
            fraction = localTimeString.substring(i + 1)
        } else {
            v = localTimeString
            fraction = null
        }
        val pt: DateTimeUtils.PrecisionTime = DateTimeUtils.parsePrecisionDateTimeLiteral(
            v,
            SimpleDateFormat(DateTimeUtils.TIME_FORMAT_STRING, Locale.ROOT),
            this.timeZone, -1
        )
        pt.getCalendar().setTimeZone(timeZone)
        return if (fraction != null) {
            TimeWithTimeZoneString(
                pt.getCalendar().get(Calendar.HOUR_OF_DAY),
                pt.getCalendar().get(Calendar.MINUTE),
                pt.getCalendar().get(Calendar.SECOND),
                timeZone.getID()
            )
                .withFraction(fraction)
        } else TimeWithTimeZoneString(
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
                || o is TimeWithTimeZoneString
                && (o as TimeWithTimeZoneString).v.equals(v))
    }

    @Override
    override fun hashCode(): Int {
        return v.hashCode()
    }

    @Override
    operator fun compareTo(o: TimeWithTimeZoneString): Int {
        return v.compareTo(o.v)
    }

    fun round(precision: Int): TimeWithTimeZoneString {
        Preconditions.checkArgument(precision >= 0)
        return TimeWithTimeZoneString(
            localTime.round(precision), timeZone
        )
    }

    /** Converts this TimeWithTimeZoneString to a string, truncated or padded with
     * zeros to a given precision.  */
    fun toString(precision: Int): String {
        Preconditions.checkArgument(precision >= 0)
        return localTime.toString(precision) + " " + timeZone.getID()
    }

    val localTimeString: org.apache.calcite.util.TimeString
        get() = localTime

    companion object {
        fun fromMillisOfDay(i: Int): TimeWithTimeZoneString {
            return TimeWithTimeZoneString(
                DateTimeUtils.unixTimeToString(i) + " " + DateTimeUtils.UTC_ZONE.getID()
            )
                .withMillis(DateTimeUtils.floorMod(i, 1000) as Int)
        }
    }
}
