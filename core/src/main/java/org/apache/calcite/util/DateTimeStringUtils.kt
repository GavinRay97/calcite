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
 * Utility methods to manipulate String representation of DateTime values.
 */
object DateTimeStringUtils {
    /** The SimpleDateFormat string for ISO timestamps,
     * "yyyy-MM-dd'T'HH:mm:ss'Z'".  */
    const val ISO_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"

    /** The SimpleDateFormat string for ISO timestamps with precisions, "yyyy-MM-dd'T'HH:mm:ss
     * .SSS'Z'" */
    const val ISO_DATETIME_FRACTIONAL_SECOND_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
    fun pad(length: Int, v: Long): String {
        val s = StringBuilder(toString(v))
        while (s.length() < length) {
            s.insert(0, "0")
        }
        return s.toString()
    }

    /** Appends hour:minute:second to a buffer; assumes they are valid.  */
    fun hms(b: StringBuilder, h: Int, m: Int, s: Int): StringBuilder {
        int2(b, h)
        b.append(':')
        int2(b, m)
        b.append(':')
        int2(b, s)
        return b
    }

    /** Appends year-month-day and hour:minute:second to a buffer; assumes they
     * are valid.  */
    fun ymdhms(
        b: StringBuilder, year: Int, month: Int, day: Int,
        h: Int, m: Int, s: Int
    ): StringBuilder {
        ymd(b, year, month, day)
        b.append(' ')
        hms(b, h, m, s)
        return b
    }

    /** Appends year-month-day to a buffer; assumes they are valid.  */
    fun ymd(b: StringBuilder, year: Int, month: Int, day: Int): StringBuilder {
        int4(b, year)
        b.append('-')
        int2(b, month)
        b.append('-')
        int2(b, day)
        return b
    }

    private fun int4(buf: StringBuilder, i: Int) {
        buf.append(('0'.code + i / 1000 % 10).toChar())
        buf.append(('0'.code + i / 100 % 10).toChar())
        buf.append(('0'.code + i / 10 % 10).toChar())
        buf.append(('0'.code + i % 10).toChar())
    }

    private fun int2(buf: StringBuilder, i: Int) {
        buf.append(('0'.code + i / 10 % 10).toChar())
        buf.append(('0'.code + i % 10).toChar())
    }

    fun isValidTimeZone(timeZone: String): Boolean {
        if (timeZone.equals("GMT")) {
            return true
        } else {
            val id: String = TimeZone.getTimeZone(timeZone).getID()
            if (!id.equals("GMT")) {
                return true
            }
        }
        return false
    }

    /**
     * Create a SimpleDateFormat with format string with default time zone UTC.
     */
    fun getDateFormatter(format: String?): SimpleDateFormat {
        return getDateFormatter(format, DateTimeUtils.UTC_ZONE)
    }

    /**
     * Create a SimpleDateFormat with format string and time zone.
     */
    fun getDateFormatter(format: String?, timeZone: TimeZone?): SimpleDateFormat {
        val dateFormatter = SimpleDateFormat(
            format, Locale.ROOT
        )
        dateFormatter.setTimeZone(timeZone)
        return dateFormatter
    }
}
