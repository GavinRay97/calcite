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
package org.apache.calcite.rex

import org.apache.calcite.avatica.util.ByteString
import org.apache.calcite.avatica.util.DateTimeUtils
import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.config.CalciteSystemProperty
import org.apache.calcite.linq4j.function.Functions
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.runtime.FlatLists
import org.apache.calcite.runtime.GeoFunctions
import org.apache.calcite.runtime.Geometries
import org.apache.calcite.sql.SqlCollation
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserUtil
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.util.CompositeList
import org.apache.calcite.util.ConversionUtil
import org.apache.calcite.util.DateString
import org.apache.calcite.util.Litmus
import org.apache.calcite.util.NlsString
import org.apache.calcite.util.Sarg
import org.apache.calcite.util.TimeString
import org.apache.calcite.util.TimestampString
import org.apache.calcite.util.Util
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import org.checkerframework.checker.initialization.qual.UnknownInitialization
import org.checkerframework.checker.nullness.qual.PolyNull
import org.checkerframework.checker.nullness.qual.RequiresNonNull
import org.checkerframework.dataflow.qual.Pure
import java.io.PrintWriter
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.List
import java.util.Locale
import java.util.Map
import java.util.Objects
import java.util.TimeZone
import org.apache.calcite.linq4j.Nullness.castNonNull
import org.apache.calcite.rel.type.RelDataTypeImpl.NON_NULLABLE_SUFFIX
import java.util.Objects.requireNonNull

/**
 * Constant value in a row-expression.
 *
 *
 * There are several methods for creating literals in [RexBuilder]:
 * [RexBuilder.makeLiteral] and so forth.
 *
 *
 * How is the value stored? In that respect, the class is somewhat of a black
 * box. There is a [.getValue] method which returns the value as an
 * object, but the type of that value is implementation detail, and it is best
 * that your code does not depend upon that knowledge. It is better to use
 * task-oriented methods such as [.getValue2] and
 * [.toJavaString].
 *
 *
 * The allowable types and combinations are:
 *
 * <table>
 * <caption>Allowable types for RexLiteral instances</caption>
 * <tr>
 * <th>TypeName</th>
 * <th>Meaning</th>
 * <th>Value type</th>
</tr> *
 * <tr>
 * <td>[SqlTypeName.NULL]</td>
 * <td>The null value. It has its own special type.</td>
 * <td>null</td>
</tr> *
 * <tr>
 * <td>[SqlTypeName.BOOLEAN]</td>
 * <td>Boolean, namely `TRUE`, `FALSE` or `
 * UNKNOWN`.</td>
 * <td>[Boolean], or null represents the UNKNOWN value</td>
</tr> *
 * <tr>
 * <td>[SqlTypeName.DECIMAL]</td>
 * <td>Exact number, for example `0`, `-.5`, `
 * 12345`.</td>
 * <td>[BigDecimal]</td>
</tr> *
 * <tr>
 * <td>[SqlTypeName.DOUBLE]</td>
 * <td>Approximate number, for example `6.023E-23`.</td>
 * <td>[BigDecimal]</td>
</tr> *
 * <tr>
 * <td>[SqlTypeName.DATE]</td>
 * <td>Date, for example `DATE '1969-04'29'`</td>
 * <td>[Calendar];
 * also [Calendar] (UTC time zone)
 * and [Integer] (days since POSIX epoch)</td>
</tr> *
 * <tr>
 * <td>[SqlTypeName.TIME]</td>
 * <td>Time, for example `TIME '18:37:42.567'`</td>
 * <td>[Calendar];
 * also [Calendar] (UTC time zone)
 * and [Integer] (milliseconds since midnight)</td>
</tr> *
 * <tr>
 * <td>[SqlTypeName.TIMESTAMP]</td>
 * <td>Timestamp, for example `TIMESTAMP '1969-04-29
 * 18:37:42.567'`</td>
 * <td>[TimestampString];
 * also [Calendar] (UTC time zone)
 * and [Long] (milliseconds since POSIX epoch)</td>
</tr> *
 * <tr>
 * <td>[SqlTypeName.INTERVAL_DAY],
 * [SqlTypeName.INTERVAL_DAY_HOUR],
 * [SqlTypeName.INTERVAL_DAY_MINUTE],
 * [SqlTypeName.INTERVAL_DAY_SECOND],
 * [SqlTypeName.INTERVAL_HOUR],
 * [SqlTypeName.INTERVAL_HOUR_MINUTE],
 * [SqlTypeName.INTERVAL_HOUR_SECOND],
 * [SqlTypeName.INTERVAL_MINUTE],
 * [SqlTypeName.INTERVAL_MINUTE_SECOND],
 * [SqlTypeName.INTERVAL_SECOND]</td>
 * <td>Interval, for example `INTERVAL '4:3:2' HOUR TO SECOND`</td>
 * <td>[BigDecimal];
 * also [Long] (milliseconds)</td>
</tr> *
 * <tr>
 * <td>[SqlTypeName.INTERVAL_YEAR],
 * [SqlTypeName.INTERVAL_YEAR_MONTH],
 * [SqlTypeName.INTERVAL_MONTH]</td>
 * <td>Interval, for example `INTERVAL '2-3' YEAR TO MONTH`</td>
 * <td>[BigDecimal];
 * also [Integer] (months)</td>
</tr> *
 * <tr>
 * <td>[SqlTypeName.CHAR]</td>
 * <td>Character constant, for example `'Hello, world!'`, `
 * ''`, `_N'Bonjour'`, `_ISO-8859-1'It''s superman!'
 * COLLATE SHIFT_JIS$ja_JP$2`. These are always CHAR, never VARCHAR.</td>
 * <td>[NlsString];
 * also [String]</td>
</tr> *
 * <tr>
 * <td>[SqlTypeName.BINARY]</td>
 * <td>Binary constant, for example `X'7F34'`. (The number of hexits
 * must be even; see above.) These constants are always BINARY, never
 * VARBINARY.</td>
 * <td>[ByteBuffer];
 * also `byte[]`</td>
</tr> *
 * <tr>
 * <td>[SqlTypeName.SYMBOL]</td>
 * <td>A symbol is a special type used to make parsing easier; it is not part of
 * the SQL standard, and is not exposed to end-users. It is used to hold a flag,
 * such as the LEADING flag in a call to the function `
 * TRIM([LEADING|TRAILING|BOTH] chars FROM string)`.</td>
 * <td>An enum class</td>
</tr> *
</table> *
 */
class RexLiteral internal constructor(
    @Nullable value: Comparable?,
    type: RelDataType,
    typeName: SqlTypeName
) : RexNode() {
    //~ Instance fields --------------------------------------------------------
    /**
     * The value of this literal. Must be consistent with its type, as per
     * [.valueMatchesType]. For example, you can't store an
     * [Integer] value here just because you feel like it -- all numbers are
     * represented by a [BigDecimal]. But since this field is private, it
     * doesn't really matter how the values are stored.
     */
    @Nullable
    private val value: Comparable?

    /**
     * The real type of this literal, as reported by [.getType].
     */
    private override val type: RelDataType
    // TODO jvs 26-May-2006:  Use SqlTypeFamily instead; it exists
    // for exactly this purpose (to avoid the confusion which results
    // from overloading SqlTypeName).
    /**
     * An indication of the broad type of this literal -- even if its type isn't
     * a SQL type. Sometimes this will be different than the SQL type; for
     * example, all exact numbers, including integers have typeName
     * [SqlTypeName.DECIMAL]. See [.valueMatchesType] for the
     * definitive story.
     */
    private val typeName: SqlTypeName
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a `RexLiteral`.
     */
    init {
        this.value = value
        this.type = requireNonNull(type, "type")
        this.typeName = requireNonNull(typeName, "typeName")
        Preconditions.checkArgument(valueMatchesType(value, typeName, true))
        Preconditions.checkArgument(value == null == type.isNullable())
        Preconditions.checkArgument(typeName !== SqlTypeName.ANY)
        this.digest = computeDigest(RexDigestIncludeType.OPTIONAL)
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Returns a string which concisely describes the definition of this
     * rex literal. Two literals are equivalent if and only if their digests are the same.
     *
     *
     * The digest does not contain the expression's identity, but does include the identity
     * of children.
     *
     *
     * Technically speaking 1:INT differs from 1:FLOAT, so we need data type in the literal's
     * digest, however we want to avoid extra verbosity of the [RelNode.getDigest] for
     * readability purposes, so we omit type info in certain cases.
     * For instance, 1:INT becomes 1 (INT is implied by default), however 1:BIGINT always holds
     * the type
     *
     *
     * Here's a non-exhaustive list of the "well known cases":
     *  * Hide "NOT NULL" for not null literals
     *  * Hide INTEGER, BOOLEAN, SYMBOL, TIME(0), TIMESTAMP(0), DATE(0) types
     *  * Hide collation when it matches IMPLICIT/COERCIBLE
     *  * Hide charset when it matches default
     *  * Hide CHAR(xx) when literal length is equal to the precision of the type.
     * In other words, use 'Bob' instead of 'Bob':CHAR(3)
     *  * Hide BOOL for AND/OR arguments. In other words, AND(true, null) means
     * null is BOOL.
     *  * Hide types for literals in simple binary operations (e.g. +, -, *, /,
     * comparison) when type of the other argument is clear.
     * See [RexCall.computeDigest]
     * For instance: =(true. null) means null is BOOL. =($0, null) means the type
     * of null matches the type of $0.
     *
     *
     * @param includeType whether the digest should include type or not
     * @return digest
     */
    @RequiresNonNull(["typeName", "type"])
    fun computeDigest(
        includeType: RexDigestIncludeType
    ): String {
        var includeType: RexDigestIncludeType = includeType
        if (includeType === RexDigestIncludeType.OPTIONAL) {
            if (digest != null) {
                // digest is initialized with OPTIONAL, so cached value matches for
                // includeType=OPTIONAL as well
                return digest
            }
            // Compute we should include the type or not
            includeType = digestIncludesType()
        } else if (digest != null && includeType === digestIncludesType()) {
            // The digest is always computed with includeType=OPTIONAL
            // If it happened to omit the type, we want to optimize computeDigest(NO_TYPE) as well
            // If the digest includes the type, we want to optimize computeDigest(ALWAYS)
            return digest
        }
        return toJavaString(value, typeName, type, includeType)
    }

    /**
     * Returns true if [RexDigestIncludeType.OPTIONAL] digest would include data type.
     *
     * @see RexCall.computeDigest
     * @return true if [RexDigestIncludeType.OPTIONAL] digest would include data type
     */
    @RequiresNonNull("type")
    fun digestIncludesType(): RexDigestIncludeType {
        return shouldIncludeType(value, type)
    }

    private fun intervalString(v: BigDecimal): String {
        var v: BigDecimal = v
        val timeUnits: List<TimeUnit> = getTimeUnits(type.getSqlTypeName())
        val b = StringBuilder()
        for (timeUnit in timeUnits) {
            val result: Array<BigDecimal> = v.divideAndRemainder(timeUnit.multiplier)
            if (b.length() > 0) {
                b.append(timeUnit.separator)
            }
            val width = if (b.length() === 0) -1 else width(timeUnit) // don't pad 1st
            pad(b, result[0].toString(), width)
            v = result[1]
        }
        if (Util.last(timeUnits) === TimeUnit.MILLISECOND) {
            while (b.toString().matches(".*\\.[0-9]*0")) {
                if (b.toString().endsWith(".0")) {
                    b.setLength(b.length() - 2) // remove ".0"
                } else {
                    b.setLength(b.length() - 1) // remove "0"
                }
            }
        }
        return b.toString()
    }

    /**
     * Prints the value this literal as a Java string constant.
     */
    fun printAsJava(pw: PrintWriter?) {
        Util.asStringBuilder(pw) { sb ->
            appendAsJava(
                value, sb, typeName, type, true,
                RexDigestIncludeType.NO_TYPE
            )
        }
    }

    fun getTypeName(): SqlTypeName {
        return typeName
    }

    @Override
    fun getType(): RelDataType {
        return type
    }

    @get:Override
    override val kind: SqlKind
        get() = SqlKind.LITERAL

    /**
     * Returns whether this literal's value is null.
     */
    val isNull: Boolean
        get() = value == null

    /**
     * Returns the value of this literal.
     *
     *
     * For backwards compatibility, returns DATE. TIME and TIMESTAMP as a
     * [Calendar] value in UTC time zone.
     */
    @Pure
    @Nullable
    fun getValue(): Comparable? {
        assert(valueMatchesType(value, typeName, true)) { value }
        return if (value == null) {
            null
        } else when (typeName) {
            TIME, DATE, TIMESTAMP -> getValueAs(Calendar::class.java)
            else -> value
        }
    }

    /**
     * Returns the value of this literal, in the form that the calculator
     * program builder wants it.
     */
    @get:Nullable
    val value2: Object?
        get() = if (value == null) {
            null
        } else when (typeName) {
            CHAR -> getValueAs(String::class.java)
            DECIMAL, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> getValueAs(Long::class.java)
            DATE, TIME, TIME_WITH_LOCAL_TIME_ZONE -> getValueAs(Integer::class.java)
            else -> value
        }

    /**
     * Returns the value of this literal, in the form that the rex-to-lix
     * translator wants it.
     */
    @get:Nullable
    val value3: Object?
        get() = if (value == null) {
            null
        } else when (typeName) {
            DECIMAL -> {
                assert(value is BigDecimal)
                value
            }
            else -> value2
        }

    /**
     * Returns the value of this literal, in the form that [RexInterpreter]
     * wants it.
     */
    @get:Nullable
    val value4: Comparable?
        get() = if (value == null) {
            null
        } else when (typeName) {
            TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> getValueAs(Long::class.java)
            DATE, TIME, TIME_WITH_LOCAL_TIME_ZONE -> getValueAs(Integer::class.java)
            else -> value
        }

    /** Returns the value of this literal as an instance of the specified class.
     *
     *
     * The following SQL types allow more than one form:
     *
     *
     *  * CHAR as [NlsString] or [String]
     *  * TIME as [TimeString],
     * [Integer] (milliseconds since midnight),
     * [Calendar] (in UTC)
     *  * DATE as [DateString],
     * [Integer] (days since 1970-01-01),
     * [Calendar]
     *  * TIMESTAMP as [TimestampString],
     * [Long] (milliseconds since 1970-01-01 00:00:00),
     * [Calendar]
     *  * DECIMAL as [BigDecimal] or [Long]
     *
     *
     *
     * Called with `clazz` = [Comparable], returns the value in
     * its native form.
     *
     * @param clazz Desired return type
     * @param <T> Return type
     * @return Value of this literal in the desired type
    </T> */
    fun <T> getValueAs(clazz: Class<T>): @Nullable T? {
        if (value == null || clazz.isInstance(value)) {
            return clazz.cast(value)
        }
        when (typeName) {
            BINARY -> if (clazz === ByteArray::class.java) {
                return clazz.cast((value as ByteString).getBytes())
            }
            CHAR -> if (clazz === String::class.java) {
                return clazz.cast((value as NlsString).getValue())
            } else if (clazz === Character::class.java) {
                return clazz.cast((value as NlsString).getValue().charAt(0))
            }
            VARCHAR -> if (clazz === String::class.java) {
                return clazz.cast((value as NlsString).getValue())
            }
            DECIMAL -> {
                if (clazz === Long::class.java) {
                    return clazz.cast((value as BigDecimal).unscaledValue().longValue())
                }
                if (clazz === Long::class.java) {
                    return clazz.cast((value as BigDecimal).longValue())
                } else if (clazz === Integer::class.java) {
                    return clazz.cast((value as BigDecimal).intValue())
                } else if (clazz === Short::class.java) {
                    return clazz.cast((value as BigDecimal).shortValue())
                } else if (clazz === Byte::class.java) {
                    return clazz.cast((value as BigDecimal).byteValue())
                } else if (clazz === Double::class.java) {
                    return clazz.cast((value as BigDecimal).doubleValue())
                } else if (clazz === Float::class.java) {
                    return clazz.cast((value as BigDecimal).floatValue())
                }
            }
            BIGINT, INTEGER, SMALLINT, TINYINT, DOUBLE, REAL, FLOAT -> if (clazz === Long::class.java) {
                return clazz.cast((value as BigDecimal).longValue())
            } else if (clazz === Integer::class.java) {
                return clazz.cast((value as BigDecimal).intValue())
            } else if (clazz === Short::class.java) {
                return clazz.cast((value as BigDecimal).shortValue())
            } else if (clazz === Byte::class.java) {
                return clazz.cast((value as BigDecimal).byteValue())
            } else if (clazz === Double::class.java) {
                return clazz.cast((value as BigDecimal).doubleValue())
            } else if (clazz === Float::class.java) {
                return clazz.cast((value as BigDecimal).floatValue())
            }
            DATE -> if (clazz === Integer::class.java) {
                return clazz.cast((value as DateString).getDaysSinceEpoch())
            } else if (clazz === Calendar::class.java) {
                return clazz.cast((value as DateString).toCalendar())
            }
            TIME -> if (clazz === Integer::class.java) {
                return clazz.cast((value as TimeString).getMillisOfDay())
            } else if (clazz === Calendar::class.java) {
                // Note: Nanos are ignored
                return clazz.cast((value as TimeString).toCalendar())
            }
            TIME_WITH_LOCAL_TIME_ZONE -> if (clazz === Integer::class.java) {
                // Milliseconds since 1970-01-01 00:00:00
                return clazz.cast((value as TimeString).getMillisOfDay())
            }
            TIMESTAMP -> if (clazz === Long::class.java) {
                // Milliseconds since 1970-01-01 00:00:00
                return clazz.cast((value as TimestampString).getMillisSinceEpoch())
            } else if (clazz === Calendar::class.java) {
                // Note: Nanos are ignored
                return clazz.cast((value as TimestampString).toCalendar())
            }
            TIMESTAMP_WITH_LOCAL_TIME_ZONE -> if (clazz === Long::class.java) {
                // Milliseconds since 1970-01-01 00:00:00
                return clazz.cast((value as TimestampString).getMillisSinceEpoch())
            } else if (clazz === Calendar::class.java) {
                // Note: Nanos are ignored
                return clazz.cast((value as TimestampString).toCalendar())
            }
            INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> if (clazz === Integer::class.java) {
                return clazz.cast((value as BigDecimal).intValue())
            } else if (clazz === Long::class.java) {
                return clazz.cast((value as BigDecimal).longValue())
            } else if (clazz === String::class.java) {
                return clazz.cast(intervalString(castNonNull(getValueAs<T>(BigDecimal::class.java)).abs()))
            } else if (clazz === Boolean::class.java) {
                // return whether negative
                return clazz.cast(castNonNull(getValueAs<T>(BigDecimal::class.java)).signum() < 0)
            }
            else -> {}
        }
        throw AssertionError(
            "cannot convert " + typeName
                    + " literal to " + clazz
        )
    }

    @get:Override
    override val isAlwaysTrue: Boolean
        get() = if (typeName !== SqlTypeName.BOOLEAN) {
            false
        } else booleanValue(this)

    @get:Override
    override val isAlwaysFalse: Boolean
        get() = if (typeName !== SqlTypeName.BOOLEAN) {
            false
        } else !booleanValue(this)

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return if (this === obj) {
            true
        } else obj is RexLiteral
                && Objects.equals((obj as RexLiteral).value, value)
                && Objects.equals((obj as RexLiteral).type, type)
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(value, type)
    }

    @Override
    override fun <R> accept(visitor: RexVisitor<R>): R {
        return visitor.visitLiteral(this)
    }

    @Override
    override fun <R, P> accept(visitor: RexBiVisitor<R, P>, arg: P): R {
        return visitor.visitLiteral(this, arg)
    }

    companion object {
        private val TIME_UNITS: ImmutableList<TimeUnit> = ImmutableList.copyOf(TimeUnit.values())

        /** Returns whether a value is appropriate for its type. (We have rules about
         * these things!)  */
        fun valueMatchesType(
            @Nullable value: Comparable?,
            typeName: SqlTypeName?,
            strict: Boolean
        ): Boolean {
            return if (value == null) {
                true
            } else when (typeName) {
                BOOLEAN ->       // Unlike SqlLiteral, we do not allow boolean null.
                    value is Boolean
                NULL -> false // value should have been null
                INTEGER, TINYINT, SMALLINT -> {
                    if (strict) {
                        throw Util.unexpected(typeName)
                    }
                    value is BigDecimal
                }
                DECIMAL, DOUBLE, FLOAT, REAL, BIGINT -> value is BigDecimal
                DATE -> value is DateString
                TIME -> value is TimeString
                TIME_WITH_LOCAL_TIME_ZONE -> value is TimeString
                TIMESTAMP -> value is TimestampString
                TIMESTAMP_WITH_LOCAL_TIME_ZONE -> value is TimestampString
                INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND ->       // The value of a DAY-TIME interval (whatever the start and end units,
                    // even say HOUR TO MINUTE) is in milliseconds (perhaps fractional
                    // milliseconds). The value of a YEAR-MONTH interval is in months.
                    value is BigDecimal
                VARBINARY -> {
                    if (strict) {
                        throw Util.unexpected(typeName)
                    }
                    value is ByteString
                }
                BINARY -> value is ByteString
                VARCHAR -> {
                    if (strict) {
                        throw Util.unexpected(typeName)
                    }
                    // A SqlLiteral's charset and collation are optional; not so a
                    // RexLiteral.
                    (value is NlsString
                            && (value as NlsString).getCharset() != null
                            && (value as NlsString).getCollation() != null)
                }
                CHAR -> value is NlsString
                        && (value as NlsString).getCharset() != null
                        && (value as NlsString).getCollation() != null
                SARG -> value is Sarg
                SYMBOL -> value is Enum
                ROW, MULTISET -> value is List
                GEOMETRY -> value is Geometries.Geom
                ANY ->       // Literal of type ANY is not legal. "CAST(2 AS ANY)" remains
                    // an integer literal surrounded by a cast function.
                    false
                else -> throw Util.unexpected(typeName)
            }
        }

        /**
         * Returns the strict literal type for a given type. The rules should keep
         * sync with what [RexBuilder.makeLiteral] defines.
         */
        fun strictTypeName(type: RelDataType): SqlTypeName {
            val typeName: SqlTypeName = type.getSqlTypeName()
            return when (typeName) {
                INTEGER, TINYINT, SMALLINT -> SqlTypeName.DECIMAL
                REAL, FLOAT -> SqlTypeName.DOUBLE
                VARBINARY -> SqlTypeName.BINARY
                VARCHAR -> SqlTypeName.CHAR
                else -> typeName
            }
        }

        private fun toJavaString(
            @Nullable value: Comparable?,
            typeName: SqlTypeName, type: RelDataType,
            includeType: RexDigestIncludeType
        ): String {
            assert(includeType !== RexDigestIncludeType.OPTIONAL) { "toJavaString must not be called with includeType=OPTIONAL" }
            if (value == null) {
                return if (includeType === RexDigestIncludeType.NO_TYPE) "null" else "null:" + type.getFullTypeString()
            }
            val sb = StringBuilder()
            appendAsJava(value, sb, typeName, type, false, includeType)
            if (includeType !== RexDigestIncludeType.NO_TYPE) {
                sb.append(':')
                val fullTypeString: String = type.getFullTypeString()
                if (!fullTypeString.endsWith(NON_NULLABLE_SUFFIX)) {
                    sb.append(fullTypeString)
                } else {
                    // Trim " NOT NULL". Apparently, the literal is not null, so we just print the data type.
                    sb.append(
                        fullTypeString, 0,
                        fullTypeString.length() - NON_NULLABLE_SUFFIX.length()
                    )
                }
            }
            return sb.toString()
        }

        /**
         * Computes if data type can be omitted from the digset.
         *
         * For instance, `1:BIGINT` has to keep data type while `1:INT`
         * should be represented as just `1`.
         *
         *
         * Implementation assumption: this method should be fast. In fact might call
         * [NlsString.getValue] which could decode the string, however we rely on the cache there.
         *
         * @see RexLiteral.computeDigest
         * @param value value of the literal
         * @param type type of the literal
         * @return NO_TYPE when type can be omitted, ALWAYS otherwise
         */
        private fun shouldIncludeType(
            @Nullable value: Comparable?,
            type: RelDataType
        ): RexDigestIncludeType {
            if (type.isNullable()) {
                // This means "null literal", so we require a type for it
                // There might be exceptions like AND(null, true) which are handled by RexCall#computeDigest
                return RexDigestIncludeType.ALWAYS
            }
            // The variable here simplifies debugging (one can set a breakpoint at return)
            // final ensures we set the value in all the branches, and it ensures the value is set just once
            val includeType: RexDigestIncludeType
            includeType =
                if (type.getSqlTypeName() === SqlTypeName.BOOLEAN || type.getSqlTypeName() === SqlTypeName.INTEGER || type.getSqlTypeName() === SqlTypeName.SYMBOL) {
                    // We don't want false:BOOLEAN NOT NULL, so we don't print type information for
                    // non-nullable BOOLEAN and INTEGER
                    RexDigestIncludeType.NO_TYPE
                } else if (type.getSqlTypeName() === SqlTypeName.CHAR
                    && value is NlsString
                ) {
                    val nlsString: NlsString? = value as NlsString?

                    // Ignore type information for 'Bar':CHAR(3)
                    if (((nlsString.getCharset() != null
                                && Objects.equals(type.getCharset(), nlsString.getCharset()))
                                || (nlsString.getCharset() == null
                                && Objects.equals(SqlCollation.IMPLICIT.getCharset(), type.getCharset())))
                        && Objects.equals(nlsString.getCollation(), type.getCollation())
                        && (value as NlsString?).getValue().length() === type.getPrecision()
                    ) {
                        RexDigestIncludeType.NO_TYPE
                    } else {
                        RexDigestIncludeType.ALWAYS
                    }
                } else if (type.getPrecision() === 0 && (type.getSqlTypeName() === SqlTypeName.TIME || type.getSqlTypeName() === SqlTypeName.TIMESTAMP || type.getSqlTypeName() === SqlTypeName.DATE)) {
                    // Ignore type information for '12:23:20':TIME(0)
                    // Note that '12:23:20':TIME WITH LOCAL TIME ZONE
                    RexDigestIncludeType.NO_TYPE
                } else {
                    RexDigestIncludeType.ALWAYS
                }
            return includeType
        }

        /** Returns whether a value is valid as a constant value, using the same
         * criteria as [.valueMatchesType].  */
        fun validConstant(@Nullable o: Object?, litmus: Litmus): Boolean {
            return if (o == null || o is BigDecimal
                || o is NlsString
                || o is ByteString
                || o is Boolean
            ) {
                litmus.succeed()
            } else if (o is List) {
                for (o1 in o) {
                    if (!validConstant(o1, litmus)) {
                        return litmus.fail("not a constant: {}", o1)
                    }
                }
                litmus.succeed()
            } else if (o is Map) {
                for (entry in o.entrySet()) {
                    if (!validConstant(entry.getKey(), litmus)) {
                        return litmus.fail("not a constant: {}", entry.getKey())
                    }
                    if (!validConstant(entry.getValue(), litmus)) {
                        return litmus.fail("not a constant: {}", entry.getValue())
                    }
                }
                litmus.succeed()
            } else {
                litmus.fail("not a constant: {}", o)
            }
        }

        /** Returns a list of the time units covered by an interval type such
         * as HOUR TO SECOND. Adds MILLISECOND if the end is SECOND, to deal with
         * fractional seconds.  */
        private fun getTimeUnits(typeName: SqlTypeName): List<TimeUnit> {
            val start: TimeUnit = typeName.getStartUnit()
            val end: TimeUnit = typeName.getEndUnit()
            val list: ImmutableList<TimeUnit> = TIME_UNITS.subList(start.ordinal(), end.ordinal() + 1)
            return if (end === TimeUnit.SECOND) {
                CompositeList.of(list, ImmutableList.of(TimeUnit.MILLISECOND))
            } else list
        }

        private fun pad(b: StringBuilder, s: String, width: Int) {
            if (width >= 0) {
                for (i in s.length() until width) {
                    b.append('0')
                }
            }
            b.append(s)
        }

        private fun width(timeUnit: TimeUnit): Int {
            return when (timeUnit) {
                MILLISECOND -> 3
                HOUR, MINUTE, SECOND -> 2
                else -> -1
            }
        }

        /**
         * Appends the specified value in the provided destination as a Java string. The value must be
         * consistent with the type, as per [.valueMatchesType].
         *
         *
         * Typical return values:
         *
         *
         *  * true
         *  * null
         *  * "Hello, world!"
         *  * 1.25
         *  * 1234ABCD
         *
         *
         * @param value    Value to be appended to the provided destination as a Java string
         * @param sb       Destination to which to append the specified value
         * @param typeName Type name to be used for the transformation of the value to a Java string
         * @param type Type to be used for the transformation of the value to a Java string
         * @param includeType Whether to include the data type in the Java representation
         */
        private fun appendAsJava(
            @Nullable value: Comparable?, sb: StringBuilder,
            typeName: SqlTypeName, type: RelDataType, java: Boolean,
            includeType: RexDigestIncludeType
        ) {
            when (typeName) {
                CHAR -> {
                    val nlsString: NlsString = castNonNull(value) as NlsString
                    if (java) {
                        Util.printJavaString(
                            sb,
                            nlsString.getValue(),
                            true
                        )
                    } else {
                        val includeCharset = (nlsString.getCharsetName() != null
                                && !nlsString.getCharsetName().equals(
                            CalciteSystemProperty.DEFAULT_CHARSET.value()
                        ))
                        sb.append(nlsString.asSql(includeCharset, false))
                    }
                }
                BOOLEAN -> {
                    assert(value is Boolean)
                    sb.append(value.toString())
                }
                DECIMAL -> {
                    assert(value is BigDecimal)
                    sb.append(value.toString())
                }
                DOUBLE -> {
                    assert(value is BigDecimal)
                    sb.append(Util.toScientificNotation(value as BigDecimal?))
                }
                BIGINT -> {
                    assert(value is BigDecimal)
                    val narrowLong: Long = (value as BigDecimal?).longValue()
                    sb.append(String.valueOf(narrowLong))
                    sb.append('L')
                }
                BINARY -> {
                    assert(value is ByteString)
                    sb.append("X'")
                    sb.append((value as ByteString?).toString(16))
                    sb.append("'")
                }
                NULL -> {
                    assert(value == null)
                    sb.append("null")
                }
                SARG -> {
                    assert(value is Sarg)
                    Util.asStringBuilder(sb) { sb2 -> printSarg(sb2, value as Sarg?, type) }
                }
                SYMBOL -> {
                    assert(value is Enum)
                    sb.append("FLAG(")
                    sb.append(value.toString())
                    sb.append(")")
                }
                DATE -> {
                    assert(value is DateString)
                    sb.append(value.toString())
                }
                TIME, TIME_WITH_LOCAL_TIME_ZONE -> {
                    assert(value is TimeString)
                    sb.append(value.toString())
                }
                TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> {
                    assert(value is TimestampString)
                    sb.append(value.toString())
                }
                INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> {
                    assert(value is BigDecimal)
                    sb.append(value.toString())
                }
                MULTISET, ROW -> {
                    assert(value is List) { "value must implement List: $value" }
                    @SuppressWarnings("unchecked") val list = castNonNull(value) as List<RexLiteral>
                    Util.asStringBuilder(sb) { sb2 ->
                        Util.printList(
                            sb,
                            list.size()
                        ) { sb3, i -> sb3.append(list[i].computeDigest(includeType)) }
                    }
                }
                GEOMETRY -> {
                    val wkt: String = GeoFunctions.ST_AsWKT(castNonNull(value) as Geometries.Geom?)
                    sb.append(wkt)
                }
                else -> {
                    assert(valueMatchesType(value, typeName, true))
                    throw Util.needToImplement(typeName)
                }
            }
        }

        private fun <C : Comparable<C>?> printSarg(
            sb: StringBuilder,
            sarg: Sarg<C>?, type: RelDataType
        ) {
            sarg.printTo(sb) { sb2, value -> sb2.append(toLiteral(type, value)) }
        }

        /** Converts a value to a temporary literal, for the purposes of generating a
         * digest. Literals of type ROW and MULTISET require that their components are
         * also literals.  */
        private fun toLiteral(type: RelDataType, value: Comparable<*>): RexLiteral {
            val typeName: SqlTypeName = strictTypeName(type)
            return when (typeName) {
                ROW -> {
                    assert(value is List) { "value must implement List: $value" }
                    val fieldValues: List<Comparable<*>> = value as List
                    val fields: List<RelDataTypeField> = type.getFieldList()
                    val fieldLiterals: List<RexLiteral> = FlatLists.of(
                        Functions.generate(fieldValues.size()) { i ->
                            toLiteral(
                                fields[i].getType(),
                                fieldValues[i]
                            )
                        })
                    RexLiteral(fieldLiterals as Comparable, type, typeName)
                }
                MULTISET -> {
                    assert(value is List) { "value must implement List: $value" }
                    val elementValues: List<Comparable<*>> = value as List
                    val elementLiterals: List<RexLiteral> = FlatLists.of(
                        Functions.generate(elementValues.size()) { i ->
                            toLiteral(
                                castNonNull(type.getComponentType()),
                                elementValues[i]
                            )
                        })
                    RexLiteral(elementLiterals as Comparable, type, typeName)
                }
                else -> RexLiteral(value, type, typeName)
            }
        }

        /**
         * Converts a Jdbc string into a RexLiteral. This method accepts a string,
         * as returned by the Jdbc method ResultSet.getString(), and restores the
         * string into an equivalent RexLiteral. It allows one to use Jdbc strings
         * as a common format for data.
         *
         *
         * Returns null if and only if `literal` is null.
         *
         * @param type     data type of literal to be read
         * @param typeName type family of literal
         * @param literal  the (non-SQL encoded) string representation, as returned
         * by the Jdbc call to return a column as a string
         * @return a typed RexLiteral, or null
         */
        @PolyNull
        fun fromJdbcString(
            type: RelDataType,
            typeName: SqlTypeName,
            @PolyNull literal: String?
        ): RexLiteral? {
            return if (literal == null) {
                null
            } else when (typeName) {
                CHAR -> {
                    val charset: Charset = requireNonNull(type.getCharset()) { "charset for $type" }
                    val collation: SqlCollation = type.getCollation()
                    val str = NlsString(
                        literal,
                        charset.name(),
                        collation
                    )
                    RexLiteral(str, type, typeName)
                }
                BOOLEAN -> {
                    val b: Boolean = ConversionUtil.toBoolean(literal)
                    RexLiteral(b, type, typeName)
                }
                DECIMAL, DOUBLE -> {
                    val d = BigDecimal(literal)
                    RexLiteral(d, type, typeName)
                }
                BINARY -> {
                    val bytes: ByteArray = ConversionUtil.toByteArrayFromString(literal, 16)
                    RexLiteral(ByteString(bytes), type, typeName)
                }
                NULL -> RexLiteral(null, type, typeName)
                INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> {
                    val millis: Long = SqlParserUtil.intervalToMillis(
                        literal,
                        castNonNull(type.getIntervalQualifier())
                    )
                    RexLiteral(BigDecimal.valueOf(millis), type, typeName)
                }
                INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH -> {
                    val months: Long = SqlParserUtil.intervalToMonths(
                        literal,
                        castNonNull(type.getIntervalQualifier())
                    )
                    RexLiteral(BigDecimal.valueOf(months), type, typeName)
                }
                DATE, TIME, TIMESTAMP -> {
                    val format = getCalendarFormat(typeName)
                    val tz: TimeZone = DateTimeUtils.UTC_ZONE
                    val v: Comparable
                    v = when (typeName) {
                        DATE -> {
                            val cal: Calendar = DateTimeUtils.parseDateFormat(
                                literal,
                                SimpleDateFormat(format, Locale.ROOT),
                                tz
                            )
                                ?: throw AssertionError(
                                    "fromJdbcString: invalid date/time value '"
                                            + literal + "'"
                                )
                            DateString.fromCalendarFields(cal)
                        }
                        else -> {
                            assert(format != null)
                            val ts: DateTimeUtils.PrecisionTime = DateTimeUtils.parsePrecisionDateTimeLiteral(
                                literal,
                                SimpleDateFormat(format, Locale.ROOT), tz, -1
                            )
                                ?: throw AssertionError(
                                    "fromJdbcString: invalid date/time value '"
                                            + literal + "'"
                                )
                            when (typeName) {
                                TIMESTAMP -> TimestampString.fromCalendarFields(ts.getCalendar())
                                    .withFraction(ts.getFraction())
                                TIME -> TimeString.fromCalendarFields(ts.getCalendar())
                                    .withFraction(ts.getFraction())
                                else -> throw AssertionError()
                            }
                        }
                    }
                    RexLiteral(v, type, typeName)
                }
                SYMBOL -> throw AssertionError("fromJdbcString: unsupported type")
                else -> throw AssertionError("fromJdbcString: unsupported type")
            }
        }

        private fun getCalendarFormat(typeName: SqlTypeName): String {
            return when (typeName) {
                DATE -> DateTimeUtils.DATE_FORMAT_STRING
                TIME -> DateTimeUtils.TIME_FORMAT_STRING
                TIMESTAMP -> DateTimeUtils.TIMESTAMP_FORMAT_STRING
                else -> throw AssertionError("getCalendarFormat: unknown type")
            }
        }

        fun booleanValue(node: RexNode): Boolean {
            return castNonNull((node as RexLiteral).value) as Boolean
        }

        @Nullable
        fun value(node: RexNode): Comparable? {
            return findValue(node)
        }

        fun intValue(node: RexNode): Int {
            val value: Comparable = castNonNull(findValue(node))
            return (value as Number).intValue()
        }

        @Nullable
        fun stringValue(node: RexNode): String? {
            val value: Comparable? = findValue(node)
            return if (value == null) null else (value as NlsString).getValue()
        }

        @Nullable
        private fun findValue(node: RexNode): Comparable? {
            if (node is RexLiteral) {
                return node.value
            }
            if (node is RexCall) {
                val call: RexCall = node as RexCall
                val operator: SqlOperator = call.getOperator()
                if (operator === SqlStdOperatorTable.CAST) {
                    return findValue(call.getOperands().get(0))
                }
                if (operator === SqlStdOperatorTable.UNARY_MINUS) {
                    val value: BigDecimal? = findValue(call.getOperands().get(0)) as BigDecimal?
                    return requireNonNull(value) { "can't negate null in $node" }.negate()
                }
            }
            throw AssertionError("not a literal: $node")
        }

        fun isNullLiteral(node: RexNode?): Boolean {
            return (node is RexLiteral
                    && node.value == null)
        }
    }
}
