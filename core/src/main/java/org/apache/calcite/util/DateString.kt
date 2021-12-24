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

/**
 * Date literal.
 *
 *
 * Immutable, internally represented as a string (in ISO format).
 */
class DateString
/** Internal constructor, no validation.  */ private constructor(
    val v: String,
    @SuppressWarnings("unused") ignore: Boolean
) : Comparable<DateString?> {
    /** Creates a DateString.  */
    @SuppressWarnings("method.invocation.invalid")
    constructor(v: String) : this(v, false) {
        Preconditions.checkArgument(
            PATTERN.matcher(v).matches(),
            "Invalid date format:", v
        )
        Preconditions.checkArgument(
            year >= 1 && year <= 9999,
            "Year out of range:", year
        )
        Preconditions.checkArgument(
            month >= 1 && month <= 12,
            "Month out of range:", month
        )
        Preconditions.checkArgument(
            day >= 1 && day <= 31,
            "Day out of range:", day
        )
    }

    /** Creates a DateString for year, month, day values.  */
    constructor(year: Int, month: Int, day: Int) : this(ymd(year, month, day), true) {}

    @Override
    override fun toString(): String {
        return v
    }

    @Override
    override fun equals(@Nullable o: Object): Boolean {
        // The value is in canonical form.
        return (o === this
                || o is DateString
                && (o as DateString).v.equals(v))
    }

    @Override
    override fun hashCode(): Int {
        return v.hashCode()
    }

    @Override
    operator fun compareTo(o: DateString): Int {
        return v.compareTo(o.v)
    }

    /** Returns the number of days since the epoch.  */
    val daysSinceEpoch: Int
        get() {
            val year = year
            val month = month
            val day = day
            return DateTimeUtils.ymdToUnixDate(year, month, day)
        }
    private val year: Int
        private get() = Integer.parseInt(v.substring(0, 4))
    private val month: Int
        private get() = Integer.parseInt(v.substring(5, 7))
    private val day: Int
        private get() = Integer.parseInt(v.substring(8, 10))

    /** Returns the number of milliseconds since the epoch. Always a multiple of
     * 86,400,000 (the number of milliseconds in a day).  */
    val millisSinceEpoch: Long
        get() = daysSinceEpoch * DateTimeUtils.MILLIS_PER_DAY

    fun toCalendar(): Calendar {
        return Util.calendar(millisSinceEpoch)
    }

    companion object {
        private val PATTERN: Pattern = Pattern.compile("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]")

        /** Validates a year-month-date and converts to a string.  */
        private fun ymd(year: Int, month: Int, day: Int): String {
            Preconditions.checkArgument(
                year >= 1 && year <= 9999,
                "Year out of range:", year
            )
            Preconditions.checkArgument(
                month >= 1 && month <= 12,
                "Month out of range:", month
            )
            Preconditions.checkArgument(
                day >= 1 && day <= 31,
                "Day out of range:", day
            )
            val b = StringBuilder()
            DateTimeStringUtils.ymd(b, year, month, day)
            return b.toString()
        }

        /** Creates a DateString from a Calendar.  */
        fun fromCalendarFields(calendar: Calendar): DateString {
            return DateString(
                calendar.get(Calendar.YEAR),
                calendar.get(Calendar.MONTH) + 1,
                calendar.get(Calendar.DAY_OF_MONTH)
            )
        }

        /** Creates a DateString that is a given number of days since the epoch.  */
        fun fromDaysSinceEpoch(days: Int): DateString {
            return DateString(DateTimeUtils.unixDateToString(days))
        }
    }
}
