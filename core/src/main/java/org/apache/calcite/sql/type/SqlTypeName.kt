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
package org.apache.calcite.sql.type

import org.apache.calcite.avatica.util.TimeUnit

/**
 * Enumeration of the type names which can be used to construct a SQL type.
 * Rationale for this class's existence (instead of just using the standard
 * java.sql.Type ordinals):
 *
 *
 *  * [java.sql.Types] does not include all SQL2003 data-types;
 *  * SqlTypeName provides a type-safe enumeration;
 *  * SqlTypeName provides a place to hang extra information such as whether
 * the type carries precision and scale.
 *
 */
enum class SqlTypeName(
    /**
     * Bitwise-or of flags indicating allowable precision/scale combinations.
     */
    private val signatures: Int,
    /**
     * Returns true if not of a "pure" standard sql type. "Inpure" types are
     * [.ANY], [.NULL] and [.SYMBOL]
     */
    val isSpecial: Boolean,
    /** Returns the ordinal from [java.sql.Types] corresponding to this
     * SqlTypeName.  */
    val jdbcOrdinal: Int,
    @Nullable family: SqlTypeFamily?
) {
    BOOLEAN(PrecScale.NO_NO, false, Types.BOOLEAN, SqlTypeFamily.BOOLEAN), TINYINT(
        PrecScale.NO_NO,
        false,
        Types.TINYINT,
        SqlTypeFamily.NUMERIC
    ),
    SMALLINT(
        PrecScale.NO_NO, false, Types.SMALLINT, SqlTypeFamily.NUMERIC
    ),
    INTEGER(PrecScale.NO_NO, false, Types.INTEGER, SqlTypeFamily.NUMERIC), BIGINT(
        PrecScale.NO_NO, false, Types.BIGINT, SqlTypeFamily.NUMERIC
    ),
    DECIMAL(
        PrecScale.NO_NO or PrecScale.YES_NO or PrecScale.YES_YES, false,
        Types.DECIMAL, SqlTypeFamily.NUMERIC
    ),
    FLOAT(PrecScale.NO_NO, false, Types.FLOAT, SqlTypeFamily.NUMERIC), REAL(
        PrecScale.NO_NO, false, Types.REAL, SqlTypeFamily.NUMERIC
    ),
    DOUBLE(PrecScale.NO_NO, false, Types.DOUBLE, SqlTypeFamily.NUMERIC), DATE(
        PrecScale.NO_NO, false, Types.DATE, SqlTypeFamily.DATE
    ),
    TIME(
        PrecScale.NO_NO or PrecScale.YES_NO, false, Types.TIME,
        SqlTypeFamily.TIME
    ),
    TIME_WITH_LOCAL_TIME_ZONE(
        PrecScale.NO_NO or PrecScale.YES_NO, false, Types.OTHER,
        SqlTypeFamily.TIME
    ),
    TIMESTAMP(
        PrecScale.NO_NO or PrecScale.YES_NO, false, Types.TIMESTAMP,
        SqlTypeFamily.TIMESTAMP
    ),
    TIMESTAMP_WITH_LOCAL_TIME_ZONE(
        PrecScale.NO_NO or PrecScale.YES_NO, false, Types.OTHER,
        SqlTypeFamily.TIMESTAMP
    ),
    INTERVAL_YEAR(
        PrecScale.NO_NO, false, Types.OTHER,
        SqlTypeFamily.INTERVAL_YEAR_MONTH
    ),
    INTERVAL_YEAR_MONTH(
        PrecScale.NO_NO, false, Types.OTHER,
        SqlTypeFamily.INTERVAL_YEAR_MONTH
    ),
    INTERVAL_MONTH(
        PrecScale.NO_NO, false, Types.OTHER,
        SqlTypeFamily.INTERVAL_YEAR_MONTH
    ),
    INTERVAL_DAY(
        PrecScale.NO_NO or PrecScale.YES_NO or PrecScale.YES_YES,
        false, Types.OTHER, SqlTypeFamily.INTERVAL_DAY_TIME
    ),
    INTERVAL_DAY_HOUR(
        PrecScale.NO_NO or PrecScale.YES_NO or PrecScale.YES_YES,
        false, Types.OTHER, SqlTypeFamily.INTERVAL_DAY_TIME
    ),
    INTERVAL_DAY_MINUTE(
        PrecScale.NO_NO or PrecScale.YES_NO or PrecScale.YES_YES,
        false, Types.OTHER, SqlTypeFamily.INTERVAL_DAY_TIME
    ),
    INTERVAL_DAY_SECOND(
        PrecScale.NO_NO or PrecScale.YES_NO or PrecScale.YES_YES,
        false, Types.OTHER, SqlTypeFamily.INTERVAL_DAY_TIME
    ),
    INTERVAL_HOUR(
        PrecScale.NO_NO or PrecScale.YES_NO or PrecScale.YES_YES,
        false, Types.OTHER, SqlTypeFamily.INTERVAL_DAY_TIME
    ),
    INTERVAL_HOUR_MINUTE(
        PrecScale.NO_NO or PrecScale.YES_NO or PrecScale.YES_YES,
        false, Types.OTHER, SqlTypeFamily.INTERVAL_DAY_TIME
    ),
    INTERVAL_HOUR_SECOND(
        PrecScale.NO_NO or PrecScale.YES_NO or PrecScale.YES_YES,
        false, Types.OTHER, SqlTypeFamily.INTERVAL_DAY_TIME
    ),
    INTERVAL_MINUTE(
        PrecScale.NO_NO or PrecScale.YES_NO or PrecScale.YES_YES,
        false, Types.OTHER, SqlTypeFamily.INTERVAL_DAY_TIME
    ),
    INTERVAL_MINUTE_SECOND(
        PrecScale.NO_NO or PrecScale.YES_NO or PrecScale.YES_YES,
        false, Types.OTHER, SqlTypeFamily.INTERVAL_DAY_TIME
    ),
    INTERVAL_SECOND(
        PrecScale.NO_NO or PrecScale.YES_NO or PrecScale.YES_YES,
        false, Types.OTHER, SqlTypeFamily.INTERVAL_DAY_TIME
    ),
    CHAR(
        PrecScale.NO_NO or PrecScale.YES_NO, false, Types.CHAR,
        SqlTypeFamily.CHARACTER
    ),
    VARCHAR(
        PrecScale.NO_NO or PrecScale.YES_NO, false, Types.VARCHAR,
        SqlTypeFamily.CHARACTER
    ),
    BINARY(
        PrecScale.NO_NO or PrecScale.YES_NO, false, Types.BINARY,
        SqlTypeFamily.BINARY
    ),
    VARBINARY(
        PrecScale.NO_NO or PrecScale.YES_NO, false, Types.VARBINARY,
        SqlTypeFamily.BINARY
    ),
    NULL(PrecScale.NO_NO, true, Types.NULL, SqlTypeFamily.NULL), ANY(
        PrecScale.NO_NO or PrecScale.YES_NO or PrecScale.YES_YES, true,
        Types.JAVA_OBJECT, SqlTypeFamily.ANY
    ),
    SYMBOL(PrecScale.NO_NO, true, Types.OTHER, null), MULTISET(
        PrecScale.NO_NO,
        false,
        Types.ARRAY,
        SqlTypeFamily.MULTISET
    ),
    ARRAY(
        PrecScale.NO_NO, false, Types.ARRAY, SqlTypeFamily.ARRAY
    ),
    MAP(PrecScale.NO_NO, false, Types.OTHER, SqlTypeFamily.MAP), DISTINCT(
        PrecScale.NO_NO, false, Types.DISTINCT, null
    ),
    STRUCTURED(PrecScale.NO_NO, false, Types.STRUCT, null), ROW(
        PrecScale.NO_NO, false, Types.STRUCT, null
    ),
    OTHER(PrecScale.NO_NO, false, Types.OTHER, null), CURSOR(
        PrecScale.NO_NO, false, ExtraSqlTypes.REF_CURSOR,
        SqlTypeFamily.CURSOR
    ),
    COLUMN_LIST(
        PrecScale.NO_NO, false, Types.OTHER + 2,
        SqlTypeFamily.COLUMN_LIST
    ),
    DYNAMIC_STAR(
        PrecScale.NO_NO or PrecScale.YES_NO or PrecScale.YES_YES, true,
        Types.JAVA_OBJECT, SqlTypeFamily.ANY
    ),

    /** Spatial type. Though not standard, it is common to several DBs, so we
     * do not flag it 'special' (internal).  */
    GEOMETRY(PrecScale.NO_NO, false, ExtraSqlTypes.GEOMETRY, SqlTypeFamily.GEO), SARG(
        PrecScale.NO_NO,
        true,
        Types.OTHER,
        SqlTypeFamily.ANY
    );

    @Nullable
    private val family: SqlTypeFamily?

    init {
        this.family = family
    }

    fun allowsNoPrecNoScale(): Boolean {
        return signatures and PrecScale.NO_NO != 0
    }

    fun allowsPrecNoScale(): Boolean {
        return signatures and PrecScale.YES_NO != 0
    }

    fun allowsPrec(): Boolean {
        return (allowsPrecScale(true, true)
                || allowsPrecScale(true, false))
    }

    fun allowsScale(): Boolean {
        return allowsPrecScale(true, true)
    }

    /**
     * Returns whether this type can be specified with a given combination of
     * precision and scale. For example,
     *
     *
     *  * `Varchar.allowsPrecScale(true, false)` returns `
     * true`, because the VARCHAR type allows a precision parameter, as in
     * `VARCHAR(10)`.
     *  * `Varchar.allowsPrecScale(true, true)` returns `
     * true`, because the VARCHAR type does not allow a precision and a
     * scale parameter, as in `VARCHAR(10, 4)`.
     *  * `allowsPrecScale(false, true)` returns `false`
     * for every type.
     *
     *
     * @param precision Whether the precision/length field is part of the type
     * specification
     * @param scale     Whether the scale field is part of the type specification
     * @return Whether this combination of precision/scale is valid
     */
    fun allowsPrecScale(
        precision: Boolean,
        scale: Boolean
    ): Boolean {
        val mask =
            if (precision) if (scale) PrecScale.YES_YES else PrecScale.YES_NO else if (scale) 0 else PrecScale.NO_NO
        return signatures and mask != 0
    }

    /** Returns the default scale for this type if supported, otherwise -1 if
     * scale is either unsupported or must be specified explicitly.  */
    val defaultScale: Int
        get() = when (this) {
            DECIMAL -> 0
            INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> DEFAULT_INTERVAL_FRACTIONAL_SECOND_PRECISION
            else -> -1
        }

    /**
     * Gets the SqlTypeFamily containing this SqlTypeName.
     *
     * @return containing family, or null for none (SYMBOL, DISTINCT, STRUCTURED, ROW, OTHER)
     */
    @Nullable
    fun getFamily(): SqlTypeFamily? {
        return family
    }

    /**
     * Returns the limit of this datatype. For example,
     *
     * <table border="1">
     * <caption>Datatype limits</caption>
     * <tr>
     * <th>Datatype</th>
     * <th>sign</th>
     * <th>limit</th>
     * <th>beyond</th>
     * <th>precision</th>
     * <th>scale</th>
     * <th>Returns</th>
    </tr> *
     * <tr>
     * <td>Integer</td>
     * <td>true</td>
     * <td>true</td>
     * <td>false</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>2147483647 (2 ^ 31 -1 = MAXINT)</td>
    </tr> *
     * <tr>
     * <td>Integer</td>
     * <td>true</td>
     * <td>true</td>
     * <td>true</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>2147483648 (2 ^ 31 = MAXINT + 1)</td>
    </tr> *
     * <tr>
     * <td>Integer</td>
     * <td>false</td>
     * <td>true</td>
     * <td>false</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>-2147483648 (-2 ^ 31 = MININT)</td>
    </tr> *
     * <tr>
     * <td>Boolean</td>
     * <td>true</td>
     * <td>true</td>
     * <td>false</td>
     * <td>-1</td>
     * <td>-1</td>
     * <td>TRUE</td>
    </tr> *
     * <tr>
     * <td>Varchar</td>
     * <td>true</td>
     * <td>true</td>
     * <td>false</td>
     * <td>10</td>
     * <td>-1</td>
     * <td>'ZZZZZZZZZZ'</td>
    </tr> *
    </table> *
     *
     * @param sign      If true, returns upper limit, otherwise lower limit
     * @param limit     If true, returns value at or near to overflow; otherwise
     * value at or near to underflow
     * @param beyond    If true, returns the value just beyond the limit,
     * otherwise the value at the limit
     * @param precision Precision, or -1 if not applicable
     * @param scale     Scale, or -1 if not applicable
     * @return Limit value
     */
    @Nullable
    fun getLimit(
        sign: Boolean,
        limit: Limit,
        beyond: Boolean,
        precision: Int,
        scale: Int
    ): Object? {
        var sign = sign
        assert(allowsPrecScale(precision != -1, scale != -1)) { this }
        if (limit == Limit.ZERO) {
            if (beyond) {
                return null
            }
            sign = true
        }
        val calendar: Calendar
        return when (this) {
            BOOLEAN -> when (limit) {
                Limit.ZERO -> false
                Limit.UNDERFLOW -> null
                Limit.OVERFLOW -> if (beyond || !sign) {
                    null
                } else {
                    true
                }
                else -> throw Util.unexpected(limit)
            }
            TINYINT -> getNumericLimit(
                2,
                8,
                sign,
                limit,
                beyond
            )
            SMALLINT -> getNumericLimit(
                2,
                16,
                sign,
                limit,
                beyond
            )
            INTEGER -> getNumericLimit(
                2,
                32,
                sign,
                limit,
                beyond
            )
            BIGINT -> getNumericLimit(
                2,
                64,
                sign,
                limit,
                beyond
            )
            DECIMAL -> {
                var decimal: BigDecimal? =
                    getNumericLimit(10, precision, sign, limit, beyond)
                        ?: return null
                when (limit) {
                    Limit.OVERFLOW -> {
                        val other: BigDecimal? = getLimit(
                            sign,
                            limit,
                            beyond,
                            -1,
                            -1
                        ) as BigDecimal?
                        if (other != null && decimal.compareTo(other) === (if (sign) 1 else -1)) {
                            decimal = other
                        }
                    }
                    else -> {}
                }

                // Apply scale.
                if (scale == 0) {
                    // do nothing
                } else if (scale > 0) {
                    decimal = decimal.divide(BigDecimal.TEN.pow(scale))
                } else {
                    decimal = decimal.multiply(BigDecimal.TEN.pow(-scale))
                }
                decimal
            }
            CHAR, VARCHAR -> {
                if (!sign) {
                    return null // this type does not have negative values
                }
                val buf = StringBuilder()
                when (limit) {
                    Limit.ZERO -> {}
                    Limit.UNDERFLOW -> {
                        if (beyond) {
                            // There is no value between the empty string and the
                            // smallest non-empty string.
                            return null
                        }
                        buf.append("a")
                    }
                    Limit.OVERFLOW -> {
                        var i = 0
                        while (i < precision) {
                            buf.append("Z")
                            ++i
                        }
                        if (beyond) {
                            buf.append("Z")
                        }
                    }
                    else -> {}
                }
                buf.toString()
            }
            BINARY, VARBINARY -> {
                if (!sign) {
                    return null // this type does not have negative values
                }
                val bytes: ByteArray
                when (limit) {
                    Limit.ZERO -> bytes = ByteArray(0)
                    Limit.UNDERFLOW -> {
                        if (beyond) {
                            // There is no value between the empty string and the
                            // smallest value.
                            return null
                        }
                        bytes = byteArrayOf(0x00)
                    }
                    Limit.OVERFLOW -> {
                        bytes = ByteArray(precision + if (beyond) 1 else 0)
                        Arrays.fill(bytes, 0xff.toByte())
                    }
                    else -> throw Util.unexpected(limit)
                }
                bytes
            }
            DATE -> {
                calendar = Util.calendar()
                when (limit) {
                    Limit.ZERO -> {
                        // The epoch.
                        calendar.set(Calendar.YEAR, 1970)
                        calendar.set(Calendar.MONTH, 0)
                        calendar.set(Calendar.DAY_OF_MONTH, 1)
                    }
                    Limit.UNDERFLOW -> return null
                    Limit.OVERFLOW -> {
                        if (beyond) {
                            // It is impossible to represent an invalid year as a date
                            // literal. SQL dates are represented as 'yyyy-mm-dd', and
                            // 1 <= yyyy <= 9999 is valid. There is no year 0: the year
                            // before 1AD is 1BC, so SimpleDateFormat renders the day
                            // before 0001-01-01 (AD) as 0001-12-31 (BC), which looks
                            // like a valid date.
                            return null
                        }

                        // "SQL:2003 6.1 <data type> Access Rules 6" says that year is
                        // between 1 and 9999, and days/months are the valid Gregorian
                        // calendar values for these years.
                        if (sign) {
                            calendar.set(Calendar.YEAR, 9999)
                            calendar.set(Calendar.MONTH, 11)
                            calendar.set(Calendar.DAY_OF_MONTH, 31)
                        } else {
                            calendar.set(Calendar.YEAR, 1)
                            calendar.set(Calendar.MONTH, 0)
                            calendar.set(Calendar.DAY_OF_MONTH, 1)
                        }
                    }
                    else -> {}
                }
                calendar.set(Calendar.HOUR_OF_DAY, 0)
                calendar.set(Calendar.MINUTE, 0)
                calendar.set(Calendar.SECOND, 0)
                calendar
            }
            TIME -> {
                if (!sign) {
                    return null // this type does not have negative values
                }
                if (beyond) {
                    return null // invalid values are impossible to represent
                }
                calendar = Util.calendar()
                when (limit) {
                    Limit.ZERO -> {

                        // The epoch.
                        calendar.set(Calendar.HOUR_OF_DAY, 0)
                        calendar.set(Calendar.MINUTE, 0)
                        calendar.set(Calendar.SECOND, 0)
                        calendar.set(Calendar.MILLISECOND, 0)
                    }
                    Limit.UNDERFLOW -> return null
                    Limit.OVERFLOW -> {
                        calendar.set(Calendar.HOUR_OF_DAY, 23)
                        calendar.set(Calendar.MINUTE, 59)
                        calendar.set(Calendar.SECOND, 59)
                        val millis =
                            if (precision >= 3) 999 else if (precision == 2) 990 else if (precision == 1) 900 else 0
                        calendar.set(Calendar.MILLISECOND, millis)
                    }
                    else -> {}
                }
                calendar
            }
            TIMESTAMP -> {
                calendar = Util.calendar()
                when (limit) {
                    Limit.ZERO -> {
                        // The epoch.
                        calendar.set(Calendar.YEAR, 1970)
                        calendar.set(Calendar.MONTH, 0)
                        calendar.set(Calendar.DAY_OF_MONTH, 1)
                        calendar.set(Calendar.HOUR_OF_DAY, 0)
                        calendar.set(Calendar.MINUTE, 0)
                        calendar.set(Calendar.SECOND, 0)
                        calendar.set(Calendar.MILLISECOND, 0)
                    }
                    Limit.UNDERFLOW -> return null
                    Limit.OVERFLOW -> {
                        if (beyond) {
                            // It is impossible to represent an invalid year as a date
                            // literal. SQL dates are represented as 'yyyy-mm-dd', and
                            // 1 <= yyyy <= 9999 is valid. There is no year 0: the year
                            // before 1AD is 1BC, so SimpleDateFormat renders the day
                            // before 0001-01-01 (AD) as 0001-12-31 (BC), which looks
                            // like a valid date.
                            return null
                        }

                        // "SQL:2003 6.1 <data type> Access Rules 6" says that year is
                        // between 1 and 9999, and days/months are the valid Gregorian
                        // calendar values for these years.
                        if (sign) {
                            calendar.set(Calendar.YEAR, 9999)
                            calendar.set(Calendar.MONTH, 11)
                            calendar.set(Calendar.DAY_OF_MONTH, 31)
                            calendar.set(Calendar.HOUR_OF_DAY, 23)
                            calendar.set(Calendar.MINUTE, 59)
                            calendar.set(Calendar.SECOND, 59)
                            val millis =
                                if (precision >= 3) 999 else if (precision == 2) 990 else if (precision == 1) 900 else 0
                            calendar.set(Calendar.MILLISECOND, millis)
                        } else {
                            calendar.set(Calendar.YEAR, 1)
                            calendar.set(Calendar.MONTH, 0)
                            calendar.set(Calendar.DAY_OF_MONTH, 1)
                            calendar.set(Calendar.HOUR_OF_DAY, 0)
                            calendar.set(Calendar.MINUTE, 0)
                            calendar.set(Calendar.SECOND, 0)
                            calendar.set(Calendar.MILLISECOND, 0)
                        }
                    }
                    else -> {}
                }
                calendar
            }
            else -> throw Util.unexpected(this)
        }
    }

    /**
     * Returns the minimum precision (or length) allowed for this type, or -1 if
     * precision/length are not applicable for this type.
     *
     * @return Minimum allowed precision
     */
    val minPrecision: Int
        get() = when (this) {
            DECIMAL, VARCHAR, CHAR, VARBINARY, BINARY, TIME, TIME_WITH_LOCAL_TIME_ZONE, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> 1
            INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> MIN_INTERVAL_START_PRECISION
            else -> -1
        }

    /**
     * Returns the minimum scale (or fractional second precision in the case of
     * intervals) allowed for this type, or -1 if precision/length are not
     * applicable for this type.
     *
     * @return Minimum allowed scale
     */
    val minScale: Int
        get() = when (this) {
            INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> MIN_INTERVAL_FRACTIONAL_SECOND_PRECISION
            else -> -1
        }

    /** Returns `HOUR` for `HOUR TO SECOND` and
     * `HOUR`, `SECOND` for `SECOND`.  */
    val startUnit: TimeUnit
        get() = when (this) {
            INTERVAL_YEAR, INTERVAL_YEAR_MONTH -> TimeUnit.YEAR
            INTERVAL_MONTH -> TimeUnit.MONTH
            INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND -> TimeUnit.DAY
            INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND -> TimeUnit.HOUR
            INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND -> TimeUnit.MINUTE
            INTERVAL_SECOND -> TimeUnit.SECOND
            else -> throw AssertionError(this)
        }

    /** Returns `SECOND` for both `HOUR TO SECOND` and
     * `SECOND`.  */
    val endUnit: TimeUnit
        get() = when (this) {
            INTERVAL_YEAR -> TimeUnit.YEAR
            INTERVAL_YEAR_MONTH, INTERVAL_MONTH -> TimeUnit.MONTH
            INTERVAL_DAY -> TimeUnit.DAY
            INTERVAL_DAY_HOUR, INTERVAL_HOUR -> TimeUnit.HOUR
            INTERVAL_DAY_MINUTE, INTERVAL_HOUR_MINUTE, INTERVAL_MINUTE -> TimeUnit.MINUTE
            INTERVAL_DAY_SECOND, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> TimeUnit.SECOND
            else -> throw AssertionError(this)
        }
    val isYearMonth: Boolean
        get() = when (this) {
            INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH -> true
            else -> false
        }

    /** Limit.  */
    enum class Limit {
        ZERO, UNDERFLOW, OVERFLOW
    }

    fun createLiteral(o: Object, pos: SqlParserPos?): SqlLiteral {
        return when (this) {
            BOOLEAN -> SqlLiteral.createBoolean(o as Boolean, pos)
            TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL -> SqlLiteral.createExactNumeric(
                o.toString(),
                pos
            )
            VARCHAR, CHAR -> SqlLiteral.createCharString(
                o as String,
                pos
            )
            VARBINARY, BINARY -> SqlLiteral.createBinaryString(
                o as ByteArray,
                pos
            )
            DATE -> SqlLiteral.createDate(
                if (o is Calendar) DateString.fromCalendarFields(
                    o as Calendar
                ) else o as DateString, pos
            )
            TIME -> SqlLiteral.createTime(
                if (o is Calendar) TimeString.fromCalendarFields(
                    o as Calendar
                ) else o as TimeString, 0 /* todo */, pos
            )
            TIMESTAMP -> SqlLiteral.createTimestamp(
                if (o is Calendar) TimestampString.fromCalendarFields(
                    o as Calendar
                ) else o as TimestampString, 0 /* todo */, pos
            )
            else -> throw Util.unexpected(this)
        }
    }

    /** Returns the name of this type.  */
    override val name: String
        get() = toString()

    /**
     * Flags indicating precision/scale combinations.
     *
     *
     * Note: for intervals:
     *
     *
     *  * precision = start (leading field) precision
     *  * scale = fractional second precision
     *
     */
    private interface PrecScale {
        companion object {
            const val NO_NO = 1
            const val YES_NO = 2
            const val YES_YES = 4
        }
    }

    companion object {
        const val MAX_DATETIME_PRECISION = 3

        // Minimum and default interval precisions are  defined by SQL2003
        // Maximum interval precisions are implementation dependent,
        //  but must be at least the default value
        const val DEFAULT_INTERVAL_START_PRECISION = 2
        const val DEFAULT_INTERVAL_FRACTIONAL_SECOND_PRECISION = 6
        const val MIN_INTERVAL_START_PRECISION = 1
        const val MIN_INTERVAL_FRACTIONAL_SECOND_PRECISION = 1
        const val MAX_INTERVAL_START_PRECISION = 10
        const val MAX_INTERVAL_FRACTIONAL_SECOND_PRECISION = 9

        // Cached map of enum values
        private val VALUES_MAP: Map<String, SqlTypeName> = Util.enumConstants(SqlTypeName::class.java)

        // categorizations used by SqlTypeFamily definitions
        // you probably want to use JDK 1.5 support for treating enumeration
        // as collection instead; this is only here to support
        // SqlTypeFamily.ANY
        val ALL_TYPES: List<SqlTypeName> = ImmutableList.of(
            BOOLEAN,
            INTEGER,
            VARCHAR,
            DATE,
            TIME,
            TIMESTAMP,
            NULL,
            DECIMAL,
            ANY,
            CHAR,
            BINARY,
            VARBINARY,
            TINYINT,
            SMALLINT,
            BIGINT,
            REAL,
            DOUBLE,
            SYMBOL,
            INTERVAL_YEAR,
            INTERVAL_YEAR_MONTH,
            INTERVAL_MONTH,
            INTERVAL_DAY,
            INTERVAL_DAY_HOUR,
            INTERVAL_DAY_MINUTE,
            INTERVAL_DAY_SECOND,
            INTERVAL_HOUR,
            INTERVAL_HOUR_MINUTE,
            INTERVAL_HOUR_SECOND,
            INTERVAL_MINUTE,
            INTERVAL_MINUTE_SECOND,
            INTERVAL_SECOND,
            TIME_WITH_LOCAL_TIME_ZONE,
            TIMESTAMP_WITH_LOCAL_TIME_ZONE,
            FLOAT,
            MULTISET,
            DISTINCT,
            STRUCTURED,
            ROW,
            CURSOR,
            COLUMN_LIST
        )
        val BOOLEAN_TYPES: List<SqlTypeName> = ImmutableList.of(BOOLEAN)
        val BINARY_TYPES: List<SqlTypeName> = ImmutableList.of(BINARY, VARBINARY)
        val INT_TYPES: List<SqlTypeName> = ImmutableList.of(TINYINT, SMALLINT, INTEGER, BIGINT)
        val EXACT_TYPES = combine(INT_TYPES, ImmutableList.of(DECIMAL))
        val APPROX_TYPES: List<SqlTypeName> = ImmutableList.of(FLOAT, REAL, DOUBLE)
        val NUMERIC_TYPES = combine(EXACT_TYPES, APPROX_TYPES)
        val FRACTIONAL_TYPES = combine(APPROX_TYPES, ImmutableList.of(DECIMAL))
        val CHAR_TYPES: List<SqlTypeName> = ImmutableList.of(CHAR, VARCHAR)
        val STRING_TYPES = combine(CHAR_TYPES, BINARY_TYPES)
        val DATETIME_TYPES: List<SqlTypeName> =
            ImmutableList.of(DATE, TIME, TIME_WITH_LOCAL_TIME_ZONE, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE)
        val YEAR_INTERVAL_TYPES: Set<SqlTypeName> = Sets.immutableEnumSet(
            INTERVAL_YEAR,
            INTERVAL_YEAR_MONTH,
            INTERVAL_MONTH
        )
        val DAY_INTERVAL_TYPES: Set<SqlTypeName> = Sets.immutableEnumSet(
            INTERVAL_DAY,
            INTERVAL_DAY_HOUR,
            INTERVAL_DAY_MINUTE,
            INTERVAL_DAY_SECOND,
            INTERVAL_HOUR,
            INTERVAL_HOUR_MINUTE,
            INTERVAL_HOUR_SECOND,
            INTERVAL_MINUTE,
            INTERVAL_MINUTE_SECOND,
            INTERVAL_SECOND
        )
        val INTERVAL_TYPES: Set<SqlTypeName?> = Sets.immutableEnumSet(
            Iterables.concat(YEAR_INTERVAL_TYPES, DAY_INTERVAL_TYPES)
        )
        private val JDBC_TYPE_TO_NAME: Map<Integer, SqlTypeName> =
            ImmutableMap.< Integer, SqlTypeName>builder<Integer?, org.apache.calcite.sql.type.SqlTypeName?>()
        .put(Types.TINYINT, org.apache.calcite.sql.type.SqlTypeName.TINYINT)
        .put(Types.SMALLINT, org.apache.calcite.sql.type.SqlTypeName.SMALLINT)
        .put(Types.BIGINT, org.apache.calcite.sql.type.SqlTypeName.BIGINT)
        .put(Types.INTEGER, org.apache.calcite.sql.type.SqlTypeName.INTEGER)
        .put(Types.NUMERIC, org.apache.calcite.sql.type.SqlTypeName.DECIMAL) // REVIEW
        .put(Types.DECIMAL, org.apache.calcite.sql.type.SqlTypeName.DECIMAL)
        .put(Types.FLOAT, org.apache.calcite.sql.type.SqlTypeName.FLOAT)
        .put(Types.REAL, org.apache.calcite.sql.type.SqlTypeName.REAL)
        .put(Types.DOUBLE, org.apache.calcite.sql.type.SqlTypeName.DOUBLE)
        .put(Types.CHAR, org.apache.calcite.sql.type.SqlTypeName.CHAR)
        .put(Types.VARCHAR, org.apache.calcite.sql.type.SqlTypeName.VARCHAR) // TODO: provide real support for these eventually
        .put(ExtraSqlTypes.NCHAR, org.apache.calcite.sql.type.SqlTypeName.CHAR)
        .put(ExtraSqlTypes.NVARCHAR, org.apache.calcite.sql.type.SqlTypeName.VARCHAR) // TODO: additional types not yet supported. See ExtraSqlTypes.
        // .put(Types.LONGVARCHAR, Longvarchar)
        // .put(Types.CLOB, Clob)
        // .put(Types.LONGVARBINARY, Longvarbinary)
        // .put(Types.BLOB, Blob)
        // .put(Types.LONGNVARCHAR, Longnvarchar)
        // .put(Types.NCLOB, Nclob)
        // .put(Types.ROWID, Rowid)
        // .put(Types.SQLXML, Sqlxml)
        .put(Types.BINARY, org.apache.calcite.sql.type.SqlTypeName.BINARY)
        .put(Types.VARBINARY, org.apache.calcite.sql.type.SqlTypeName.VARBINARY)
        .put(Types.DATE, org.apache.calcite.sql.type.SqlTypeName.DATE)
        .put(Types.TIME, org.apache.calcite.sql.type.SqlTypeName.TIME)
        .put(Types.TIMESTAMP, org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP)
        .put(Types.BIT, org.apache.calcite.sql.type.SqlTypeName.BOOLEAN)
        .put(Types.BOOLEAN, org.apache.calcite.sql.type.SqlTypeName.BOOLEAN)
        .put(Types.DISTINCT, org.apache.calcite.sql.type.SqlTypeName.DISTINCT)
        .put(Types.STRUCT, org.apache.calcite.sql.type.SqlTypeName.STRUCTURED)
        .put(Types.ARRAY, org.apache.calcite.sql.type.SqlTypeName.ARRAY)
        .build()
        /**
         * Looks up a type name from its name.
         *
         * @return Type name, or null if not found
         */
        @Nullable
        operator fun get(name: String): SqlTypeName? {
            return if (false) {
                // The following code works OK, but the spurious exceptions are
                // annoying.
                try {
                    valueOf(name)
                } catch (e: IllegalArgumentException) {
                    null
                }
            } else VALUES_MAP[name]
        }

        private fun combine(
            list0: List<SqlTypeName>,
            list1: List<SqlTypeName>
        ): List<SqlTypeName> {
            return ImmutableList.< SqlTypeName > builder < org . apache . calcite . sql . type . SqlTypeName ? > ()
                .addAll(list0)
                .addAll(list1)
                .build()
        }

        /**
         * Gets the SqlTypeName corresponding to a JDBC type.
         *
         * @param jdbcType the JDBC type of interest
         * @return corresponding SqlTypeName, or null if the type is not known
         */
        @Nullable
        fun getNameForJdbcType(jdbcType: Int): SqlTypeName? {
            return JDBC_TYPE_TO_NAME[jdbcType]
        }

        @Nullable
        private fun getNumericLimit(
            radix: Int,
            exponent: Int,
            sign: Boolean,
            limit: Limit,
            beyond: Boolean
        ): BigDecimal? {
            var exponent = exponent
            return when (limit) {
                Limit.OVERFLOW -> {

                    // 2-based schemes run from -2^(N-1) to 2^(N-1)-1 e.g. -128 to +127
                    // 10-based schemas run from -(10^N-1) to 10^N-1 e.g. -99 to +99
                    val bigRadix: BigDecimal = BigDecimal.valueOf(radix)
                    if (radix == 2) {
                        --exponent
                    }
                    var decimal: BigDecimal = bigRadix.pow(exponent)
                    if (sign || radix != 2) {
                        decimal = decimal.subtract(BigDecimal.ONE)
                    }
                    if (beyond) {
                        decimal = decimal.add(BigDecimal.ONE)
                    }
                    if (!sign) {
                        decimal = decimal.negate()
                    }
                    decimal
                }
                Limit.UNDERFLOW -> if (beyond) null else if (sign) BigDecimal.ONE else BigDecimal.ONE.negate()
                Limit.ZERO -> BigDecimal.ZERO
                else -> throw Util.unexpected(limit)
            }
        }
    }
}
