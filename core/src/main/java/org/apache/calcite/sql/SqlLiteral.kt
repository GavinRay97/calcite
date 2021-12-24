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
package org.apache.calcite.sql

import org.apache.calcite.avatica.util.TimeUnitRange

/**
 * A `SqlLiteral` is a constant. It is, appropriately, immutable.
 *
 *
 * How is the value stored? In that respect, the class is somewhat of a black
 * box. There is a [.getValue] method which returns the value as an
 * object, but the type of that value is implementation detail, and it is best
 * that your code does not depend upon that knowledge. It is better to use
 * task-oriented methods such as [.toSqlString] and
 * [.toValue].
 *
 *
 * If you really need to access the value directly, you should switch on the
 * value of the [.typeName] field, rather than making assumptions about
 * the runtime type of the [.value].
 *
 *
 * The allowable types and combinations are:
 *
 * <table>
 * <caption>Allowable types for SqlLiteral</caption>
 * <tr>
 * <th>TypeName</th>
 * <th>Meaing</th>
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
 * <td>[Calendar]</td>
</tr> *
 * <tr>
 * <td>[SqlTypeName.TIME]</td>
 * <td>Time, for example `TIME '18:37:42.567'`</td>
 * <td>[Calendar]</td>
</tr> *
 * <tr>
 * <td>[SqlTypeName.TIMESTAMP]</td>
 * <td>Timestamp, for example `TIMESTAMP '1969-04-29
 * 18:37:42.567'`</td>
 * <td>[Calendar]</td>
</tr> *
 * <tr>
 * <td>[SqlTypeName.CHAR]</td>
 * <td>Character constant, for example `'Hello, world!'`, `
 * ''`, `_N'Bonjour'`, `_ISO-8859-1'It''s superman!'
 * COLLATE SHIFT_JIS$ja_JP$2`. These are always CHAR, never VARCHAR.</td>
 * <td>[NlsString]</td>
</tr> *
 * <tr>
 * <td>[SqlTypeName.BINARY]</td>
 * <td>Binary constant, for example `X'ABC'`, `X'7F'`.
 * Note that strings with an odd number of hexits will later become values of
 * the BIT datatype, because they have an incomplete number of bytes. But here,
 * they are all binary constants, because that's how they were written. These
 * constants are always BINARY, never VARBINARY.</td>
 * <td>[BitString]</td>
</tr> *
 * <tr>
 * <td>[SqlTypeName.SYMBOL]</td>
 * <td>A symbol is a special type used to make parsing easier; it is not part of
 * the SQL standard, and is not exposed to end-users. It is used to hold a
 * symbol, such as the LEADING flag in a call to the function `
 * TRIM([LEADING|TRAILING|BOTH] chars FROM string)`.</td>
 * <td>An [Enum]</td>
</tr> *
 * <tr>
 * <td>[SqlTypeName.INTERVAL_YEAR]
 * .. [SqlTypeName.INTERVAL_SECOND]</td>
 * <td>Interval, for example `INTERVAL '1:34' HOUR`.</td>
 * <td>[SqlIntervalLiteral.IntervalValue].</td>
</tr> *
</table> *
 */
class SqlLiteral protected constructor(
    @Nullable value: Object?,
    typeName: SqlTypeName?,
    pos: SqlParserPos?
) : SqlNode(pos) {
    //~ Instance fields --------------------------------------------------------
    /**
     * The type with which this literal was declared. This type is very
     * approximate: the literal may have a different type once validated. For
     * example, all numeric literals have a type name of
     * [SqlTypeName.DECIMAL], but on validation may become
     * [SqlTypeName.INTEGER].
     */
    private val typeName: SqlTypeName?

    /**
     * The value of this literal. The type of the value must be appropriate for
     * the typeName, as defined by the [.valueMatchesType] method.
     */
    @Nullable
    val value: Object?
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a `SqlLiteral`.
     */
    init {
        this.value = value
        this.typeName = typeName
        assert(typeName != null)
        assert(valueMatchesType(value, typeName))
    }
    //~ Methods ----------------------------------------------------------------
    /** Returns the value of [.typeName].  */
    fun getTypeName(): SqlTypeName? {
        return typeName
    }

    @Override
    override fun clone(pos: SqlParserPos?): SqlLiteral {
        return SqlLiteral(value, typeName, pos)
    }

    @get:Override
    override val kind: org.apache.calcite.sql.SqlKind
        get() = SqlKind.LITERAL

    /**
     * Returns the value of this literal.
     *
     *
     * Try not to use this method! There are so many different kinds of
     * values, it's better to to let SqlLiteral do whatever it is you want to
     * do.
     *
     * @see .booleanValue
     * @see .symbolValue
     */
    @Nullable
    fun getValue(): Object? {
        return value
    }

    /**
     * Returns the value of this literal as a given Java type.
     *
     *
     * Which type you may ask for depends on [.typeName].
     * You may always ask for the type where we store the value internally
     * (as defined by [.valueMatchesType]), but may
     * ask for other convenient types.
     *
     *
     * For example, numeric literals' values are stored internally as
     * [BigDecimal], but other numeric types such as [Long] and
     * [Double] are also allowed.
     *
     *
     * The result is never null. For the NULL literal, returns
     * a [NullSentinel.INSTANCE].
     *
     * @param clazz Desired value type
     * @param <T> Value type
     * @return Value of the literal in desired type, never null
     *
     * @throws AssertionError if the value type is not supported
    </T> */
    fun <T : Object?> getValueAs(clazz: Class<T>): T {
        val value: Object? = value
        if (clazz.isInstance(value)) {
            return clazz.cast(value)
        }
        if (typeName === SqlTypeName.NULL) {
            return clazz.cast(NullSentinel.INSTANCE)
        }
        requireNonNull(value, "value")
        when (typeName) {
            CHAR -> if (clazz === String::class.java) {
                return clazz.cast((value as NlsString?).getValue())
            }
            BINARY -> if (clazz === ByteArray::class.java) {
                return clazz.cast((value as BitString?).getAsByteArray())
            }
            DECIMAL -> {
                if (clazz === Long::class.java) {
                    return clazz.cast((value as BigDecimal?).longValueExact())
                }
                if (clazz === Long::class.java) {
                    return clazz.cast((value as BigDecimal?).longValueExact())
                } else if (clazz === Integer::class.java) {
                    return clazz.cast((value as BigDecimal?).intValueExact())
                } else if (clazz === Short::class.java) {
                    return clazz.cast((value as BigDecimal?).shortValueExact())
                } else if (clazz === Byte::class.java) {
                    return clazz.cast((value as BigDecimal?).byteValueExact())
                } else if (clazz === Double::class.java) {
                    return clazz.cast((value as BigDecimal?).doubleValue())
                } else if (clazz === Float::class.java) {
                    return clazz.cast((value as BigDecimal?).floatValue())
                }
            }
            BIGINT, INTEGER, SMALLINT, TINYINT, DOUBLE, REAL, FLOAT -> if (clazz === Long::class.java) {
                return clazz.cast((value as BigDecimal?).longValueExact())
            } else if (clazz === Integer::class.java) {
                return clazz.cast((value as BigDecimal?).intValueExact())
            } else if (clazz === Short::class.java) {
                return clazz.cast((value as BigDecimal?).shortValueExact())
            } else if (clazz === Byte::class.java) {
                return clazz.cast((value as BigDecimal?).byteValueExact())
            } else if (clazz === Double::class.java) {
                return clazz.cast((value as BigDecimal?).doubleValue())
            } else if (clazz === Float::class.java) {
                return clazz.cast((value as BigDecimal?).floatValue())
            }
            DATE -> if (clazz === Calendar::class.java) {
                return clazz.cast((value as DateString?).toCalendar())
            }
            TIME -> if (clazz === Calendar::class.java) {
                return clazz.cast((value as TimeString?).toCalendar())
            }
            TIMESTAMP -> if (clazz === Calendar::class.java) {
                return clazz.cast((value as TimestampString?).toCalendar())
            }
            INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH -> {
                val valMonth: SqlIntervalLiteral.IntervalValue? = value
                if (clazz === Long::class.java) {
                    return clazz.cast(
                        valMonth.getSign()
                                * SqlParserUtil.intervalToMonths(valMonth)
                    )
                } else if (clazz === BigDecimal::class.java) {
                    return clazz.cast(BigDecimal.valueOf(getValueAs<T>(Long::class.java)))
                } else if (clazz === TimeUnitRange::class.java) {
                    return clazz.cast(valMonth!!.getIntervalQualifier()!!.timeUnitRange)
                } else if (clazz === SqlIntervalQualifier::class.java) {
                    return clazz.cast(valMonth!!.getIntervalQualifier())
                }
            }
            INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> {
                val valTime: SqlIntervalLiteral.IntervalValue? = value
                if (clazz === Long::class.java) {
                    return clazz.cast(
                        valTime.getSign()
                                * SqlParserUtil.intervalToMillis(valTime)
                    )
                } else if (clazz === BigDecimal::class.java) {
                    return clazz.cast(BigDecimal.valueOf(getValueAs<T>(Long::class.java)))
                } else if (clazz === TimeUnitRange::class.java) {
                    return clazz.cast(valTime!!.getIntervalQualifier()!!.timeUnitRange)
                } else if (clazz === SqlIntervalQualifier::class.java) {
                    return clazz.cast(valTime!!.getIntervalQualifier())
                }
            }
            else -> {}
        }
        throw AssertionError("cannot cast $value as $clazz")
    }

    /** Returns the value as a symbol.  */
    @Deprecated // to be removed before 2.0
    fun <E : Enum<E>?> symbolValue_(): @Nullable E? {
        return value
    }

    /** Returns the value as a symbol.  */
    fun <E : Enum<E>?> symbolValue(class_: Class<E>): @Nullable E? {
        return class_.cast(value)
    }

    /** Returns the value as a boolean.  */
    fun booleanValue(): Boolean {
        return getValueAs(Boolean::class.java)
    }

    /**
     * For calc program builder - value may be different than [.unparse].
     * Typical values:
     *
     *
     *  * Hello, world!
     *  * 12.34
     *  * {null}
     *  * 1969-04-29
     *
     *
     * @return string representation of the value
     */
    @Nullable
    fun toValue(): String? {
        return if (value == null) {
            null
        } else when (typeName) {
            CHAR ->
                // We want 'It''s superman!', not _ISO-8859-1'It''s superman!'
                (value as NlsString).getValue()
            else -> value.toString()
        }
    }

    @Override
    override fun validate(validator: SqlValidator, scope: SqlValidatorScope?) {
        validator.validateLiteral(this)
    }

    @Override
    override fun <R> accept(visitor: SqlVisitor<R>): R {
        return visitor.visit(this)
    }

    @Override
    override fun equalsDeep(@Nullable node: SqlNode?, litmus: Litmus): Boolean {
        if (node !is SqlLiteral) {
            return litmus.fail("{} != {}", this, node)
        }
        return if (!this.equals(node)) {
            litmus.fail("{} != {}", this, node)
        } else litmus.succeed()
    }

    @Override
    override fun getMonotonicity(@Nullable scope: SqlValidatorScope?): SqlMonotonicity {
        return SqlMonotonicity.CONSTANT
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        if (obj !is SqlLiteral) {
            return false
        }
        val that = obj as SqlLiteral
        return Objects.equals(value, that.value)
    }

    @Override
    override fun hashCode(): Int {
        return if (value == null) 0 else value.hashCode()
    }

    /**
     * Returns the integer value of this literal.
     *
     * @param exact Whether the value has to be exact. If true, and the literal
     * is a fraction (e.g. 3.14), throws. If false, discards the
     * fractional part of the value.
     * @return Integer value of this literal
     */
    fun intValue(exact: Boolean): Int {
        return when (typeName) {
            DECIMAL, DOUBLE -> {
                val bd: BigDecimal = requireNonNull(value, "value") as BigDecimal
                if (exact) {
                    try {
                        bd.intValueExact()
                    } catch (e: ArithmeticException) {
                        throw SqlUtil.newContextException(
                            getParserPosition(),
                            RESOURCE.numberLiteralOutOfRange(bd.toString())
                        )
                    }
                } else {
                    bd.intValue()
                }
            }
            else -> throw Util.unexpected(typeName)
        }
    }

    /**
     * Returns the long value of this literal.
     *
     * @param exact Whether the value has to be exact. If true, and the literal
     * is a fraction (e.g. 3.14), throws. If false, discards the
     * fractional part of the value.
     * @return Long value of this literal
     */
    fun longValue(exact: Boolean): Long {
        return when (typeName) {
            DECIMAL, DOUBLE -> {
                val bd: BigDecimal = requireNonNull(value, "value") as BigDecimal
                if (exact) {
                    try {
                        bd.longValueExact()
                    } catch (e: ArithmeticException) {
                        throw SqlUtil.newContextException(
                            getParserPosition(),
                            RESOURCE.numberLiteralOutOfRange(bd.toString())
                        )
                    }
                } else {
                    bd.longValue()
                }
            }
            else -> throw Util.unexpected(typeName)
        }
    }

    /**
     * Returns sign of value.
     *
     * @return -1, 0 or 1
     */
    @Deprecated // to be removed before 2.0
    fun signum(): Int {
        return castNonNull(bigDecimalValue()).compareTo(
            BigDecimal.ZERO
        )
    }

    /**
     * Returns a numeric literal's value as a [BigDecimal].
     */
    @Nullable
    fun bigDecimalValue(): BigDecimal? {
        return when (typeName) {
            DECIMAL, DOUBLE -> value as BigDecimal?
            else -> throw Util.unexpected(typeName)
        }
    }

    // to be removed before 2.0
    @get:Deprecated
    val stringValue: String
        get() = (requireNonNull(value, "value") as NlsString).getValue()

    @Override
    override fun unparse(
        writer: SqlWriter,
        leftPrec: Int,
        rightPrec: Int
    ) {
        when (typeName) {
            BOOLEAN -> writer.keyword(
                if (value == null) "UNKNOWN" else if (value) "TRUE" else "FALSE"
            )
            NULL -> writer.keyword("NULL")
            CHAR, DECIMAL, DOUBLE, BINARY -> throw Util.unexpected(typeName)
            SYMBOL -> writer.keyword(String.valueOf(value))
            else -> writer.literal(String.valueOf(value))
        }
    }

    fun createSqlType(typeFactory: RelDataTypeFactory): RelDataType {
        val bitString: BitString
        return when (typeName) {
            NULL, BOOLEAN -> {
                var ret: RelDataType = typeFactory.createSqlType(typeName)
                ret = typeFactory.createTypeWithNullability(ret, null == value)
                ret
            }
            BINARY -> {
                bitString = requireNonNull(value, "value") as BitString
                val bitCount: Int = bitString.getBitCount()
                typeFactory.createSqlType(SqlTypeName.BINARY, bitCount / 8)
            }
            CHAR -> {
                val string: NlsString = requireNonNull(value, "value") as NlsString
                var charset: Charset = string.getCharset()
                if (null == charset) {
                    charset = typeFactory.getDefaultCharset()
                }
                var collation: SqlCollation = string.getCollation()
                if (null == collation) {
                    collation = SqlCollation.COERCIBLE
                }
                var type: RelDataType = typeFactory.createSqlType(
                    SqlTypeName.CHAR,
                    string.getValue().length()
                )
                type = typeFactory.createTypeWithCharsetAndCollation(
                    type,
                    charset,
                    collation
                )
                type
            }
            INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> {
                val intervalValue: SqlIntervalLiteral.IntervalValue =
                    requireNonNull(value, "value") as SqlIntervalLiteral.IntervalValue
                typeFactory.createSqlIntervalType(
                    intervalValue.getIntervalQualifier()
                )
            }
            SYMBOL -> typeFactory.createSqlType(SqlTypeName.SYMBOL)
            INTEGER, TIME, VARCHAR, VARBINARY -> throw Util.needToImplement(toString() + ", operand=" + value)
            else -> throw Util.needToImplement(toString() + ", operand=" + value)
        }
    }

    /**
     * Transforms this literal (which must be of type character) into a new one
     * in which 4-digit Unicode escape sequences have been replaced with the
     * corresponding Unicode characters.
     *
     * @param unicodeEscapeChar escape character (e.g. backslash) for Unicode
     * numeric sequences; 0 implies no transformation
     * @return transformed literal
     */
    fun unescapeUnicode(unicodeEscapeChar: Char): SqlLiteral {
        if (unicodeEscapeChar.code == 0) {
            return this
        }
        assert(SqlTypeUtil.inCharFamily(getTypeName()))
        var ns: NlsString = requireNonNull(value, "value") as NlsString
        val s: String = ns.getValue()
        val sb = StringBuilder()
        val n: Int = s.length()
        var i = 0
        while (i < n) {
            val c: Char = s.charAt(i)
            if (c == unicodeEscapeChar) {
                if (n > i + 1) {
                    if (s.charAt(i + 1) === unicodeEscapeChar) {
                        sb.append(unicodeEscapeChar)
                        ++i
                        ++i
                        continue
                    }
                }
                if (i + 5 > n) {
                    throw SqlUtil.newContextException(
                        getParserPosition(),
                        RESOURCE.unicodeEscapeMalformed(i)
                    )
                }
                val u: String = s.substring(i + 1, i + 5)
                val v: Int
                v = try {
                    Integer.parseInt(u, 16)
                } catch (ex: NumberFormatException) {
                    throw SqlUtil.newContextException(
                        getParserPosition(),
                        RESOURCE.unicodeEscapeMalformed(i)
                    )
                }
                sb.append((v and 0xFFFF).toChar())

                // skip hexits
                i += 4
            } else {
                sb.append(c)
            }
            ++i
        }
        ns = NlsString(
            sb.toString(),
            ns.getCharsetName(),
            ns.getCollation()
        )
        return SqlCharStringLiteral(ns, getParserPosition())
    }
    //~ Inner Interfaces -------------------------------------------------------
    /**
     * A value must implement this interface if it is to be embedded as a
     * SqlLiteral of type SYMBOL. If the class is an [Enum] it trivially
     * implements this interface.
     *
     *
     * The [.toString] method should return how the symbol should be
     * unparsed, which is sometimes not the same as the enumerated value's name
     * (e.g. "UNBOUNDED PRECEDING" versus "UnboundedPreceeding").
     */
    @Deprecated // to be removed before 2.0
    interface SqlSymbol {
        fun name(): String?
        fun ordinal(): Int
    }

    companion object {
        /** Returns whether value is appropriate for its type. (We have rules about
         * these things!)  */
        fun valueMatchesType(
            @Nullable value: Object?,
            typeName: SqlTypeName?
        ): Boolean {
            return when (typeName) {
                BOOLEAN -> value == null || value is Boolean
                NULL -> value == null
                DECIMAL, DOUBLE -> value is BigDecimal
                DATE -> value is DateString
                TIME -> value is TimeString
                TIMESTAMP -> value is TimestampString
                INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> value is SqlIntervalLiteral.IntervalValue
                BINARY -> value is BitString
                CHAR -> value is NlsString
                SYMBOL -> value is Enum
                        || value is SqlSampleSpec
                MULTISET -> true
                INTEGER, VARCHAR, VARBINARY -> throw Util.unexpected(typeName)
                else -> throw Util.unexpected(typeName)
            }
        }

        /**
         * Extracts the [SqlSampleSpec] value from a symbol literal.
         *
         * @throws ClassCastException if the value is not a symbol literal
         * @see .createSymbol
         */
        fun sampleValue(node: SqlNode): SqlSampleSpec {
            return (node as SqlLiteral).getValueAs(SqlSampleSpec::class.java)
        }

        /**
         * Extracts the value from a literal.
         *
         *
         * Cases:
         *
         *  * If the node is a character literal, a chain of string
         * literals, or a CAST of a character literal, returns the value as a
         * [NlsString].
         *
         *  * If the node is a numeric literal, or a negated numeric literal,
         * returns the value as a [BigDecimal].
         *
         *  * If the node is a [SqlIntervalQualifier],
         * returns its [TimeUnitRange].
         *
         *  * If the node is INTERVAL_DAY_TIME_ in [SqlTypeFamily],
         * returns its sign multiplied by its millisecond equivalent value
         *
         *  * If the node is INTERVAL_YEAR_MONTH_ in [SqlTypeFamily],
         * returns its sign multiplied by its months equivalent value
         *
         *  * Otherwise throws [IllegalArgumentException].
         *
         */
        @Nullable
        @Throws(IllegalArgumentException::class)
        fun value(node: SqlNode): Comparable? {
            if (node is SqlLiteral) {
                val literal = node
                if (literal.getTypeName() === SqlTypeName.SYMBOL) {
                    return literal.value
                }
                when (requireNonNull(literal.getTypeName().getFamily())) {
                    CHARACTER -> return literal.value as NlsString?
                    NUMERIC -> return literal.value as BigDecimal?
                    INTERVAL_YEAR_MONTH -> {
                        val valMonth: SqlIntervalLiteral.IntervalValue =
                            literal.getValueAs(SqlIntervalLiteral.IntervalValue::class.java)
                        return valMonth.getSign() * SqlParserUtil.intervalToMonths(valMonth)
                    }
                    INTERVAL_DAY_TIME -> {
                        val valTime: SqlIntervalLiteral.IntervalValue =
                            literal.getValueAs(SqlIntervalLiteral.IntervalValue::class.java)
                        return valTime.getSign() * SqlParserUtil.intervalToMillis(valTime)
                    }
                    else -> {}
                }
            }
            if (SqlUtil.isLiteralChain(node)) {
                assert(node is SqlCall)
                val literal: SqlLiteral = SqlLiteralChainOperator.concatenateOperands(node as SqlCall)
                assert(SqlTypeUtil.inCharFamily(literal.getTypeName()))
                return literal.value as NlsString?
            }
            return when (node.getKind()) {
                INTERVAL_QUALIFIER -> (node as SqlIntervalQualifier).timeUnitRange
                CAST -> {
                    assert(node is SqlCall)
                    value((node as SqlCall).operand(0))
                }
                MINUS_PREFIX -> {
                    assert(node is SqlCall)
                    val o: Comparable<*>? = value((node as SqlCall).operand(0))
                    if (o is BigDecimal) {
                        val bigDecimal: BigDecimal? = o as BigDecimal?
                        return bigDecimal.negate()
                    }
                    throw IllegalArgumentException("not a literal: $node")
                }
                else -> throw IllegalArgumentException("not a literal: $node")
            }
        }

        /**
         * Extracts the string value from a string literal, a chain of string
         * literals, or a CAST of a string literal.
         *
         */
        @Deprecated // to be removed before 2.0
        @Deprecated("Use {@link #value(SqlNode)}")
        fun stringValue(node: SqlNode): String {
            return if (node is SqlLiteral) {
                val literal = node
                assert(SqlTypeUtil.inCharFamily(literal.getTypeName()))
                requireNonNull(literal.value).toString()
            } else if (SqlUtil.isLiteralChain(node)) {
                val literal: SqlLiteral = SqlLiteralChainOperator.concatenateOperands(node as SqlCall)
                assert(SqlTypeUtil.inCharFamily(literal.getTypeName()))
                requireNonNull(literal.value).toString()
            } else if (node is SqlCall
                && (node as SqlCall).getOperator() === SqlStdOperatorTable.CAST
            ) {
                stringValue((node as SqlCall).operand(0))
            } else {
                throw AssertionError("invalid string literal: $node")
            }
        }

        /**
         * Converts a chained string literals into regular literals; returns regular
         * literals unchanged.
         * @throws IllegalArgumentException if `node` is not a string literal
         * and cannot be unchained.
         */
        fun unchain(node: SqlNode): SqlLiteral {
            return when (node.getKind()) {
                LITERAL -> node as SqlLiteral
                LITERAL_CHAIN -> SqlLiteralChainOperator.concatenateOperands(node as SqlCall)
                INTERVAL_QUALIFIER -> {
                    val q: SqlIntervalQualifier = node as SqlIntervalQualifier
                    SqlLiteral(
                        IntervalValue(q, 1, q.toString()),
                        q.typeName(), q.pos
                    )
                }
                else -> throw IllegalArgumentException("invalid literal: $node")
            }
        }

        /**
         * Creates a NULL literal.
         *
         *
         * There's no singleton constant for a NULL literal. Instead, nulls must
         * be instantiated via createNull(), because different instances have
         * different context-dependent types.
         */
        fun createNull(pos: SqlParserPos?): SqlLiteral {
            return SqlLiteral(null, SqlTypeName.NULL, pos)
        }

        /**
         * Creates a boolean literal.
         */
        fun createBoolean(
            b: Boolean,
            pos: SqlParserPos?
        ): SqlLiteral {
            return if (b) SqlLiteral(Boolean.TRUE, SqlTypeName.BOOLEAN, pos) else SqlLiteral(
                Boolean.FALSE,
                SqlTypeName.BOOLEAN,
                pos
            )
        }

        fun createUnknown(pos: SqlParserPos?): SqlLiteral {
            return SqlLiteral(null, SqlTypeName.BOOLEAN, pos)
        }

        /**
         * Creates a literal which represents a parser symbol, for example the
         * `TRAILING` keyword in the call `Trim(TRAILING 'x' FROM
         * 'Hello world!')`.
         *
         * @see .symbolValue
         */
        fun createSymbol(@Nullable o: Enum<*>?, pos: SqlParserPos?): SqlLiteral {
            return SqlLiteral(o, SqlTypeName.SYMBOL, pos)
        }

        /**
         * Creates a literal which represents a sample specification.
         */
        fun createSample(
            sampleSpec: SqlSampleSpec?,
            pos: SqlParserPos?
        ): SqlLiteral {
            return SqlLiteral(sampleSpec, SqlTypeName.SYMBOL, pos)
        }

        @Deprecated // to be removed before 2.0
        fun createDate(
            calendar: Calendar?,
            pos: SqlParserPos?
        ): SqlDateLiteral {
            return createDate(DateString.fromCalendarFields(calendar), pos)
        }

        fun createDate(
            date: DateString?,
            pos: SqlParserPos?
        ): SqlDateLiteral {
            return SqlDateLiteral(date, pos)
        }

        @Deprecated // to be removed before 2.0
        fun createTimestamp(
            calendar: Calendar?,
            precision: Int,
            pos: SqlParserPos?
        ): SqlTimestampLiteral {
            return createTimestamp(
                TimestampString.fromCalendarFields(calendar),
                precision, pos
            )
        }

        fun createTimestamp(
            ts: TimestampString?,
            precision: Int,
            pos: SqlParserPos?
        ): SqlTimestampLiteral {
            return SqlTimestampLiteral(ts, precision, false, pos)
        }

        @Deprecated // to be removed before 2.0
        fun createTime(
            calendar: Calendar?,
            precision: Int,
            pos: SqlParserPos?
        ): SqlTimeLiteral {
            return createTime(TimeString.fromCalendarFields(calendar), precision, pos)
        }

        fun createTime(
            t: TimeString?,
            precision: Int,
            pos: SqlParserPos?
        ): SqlTimeLiteral {
            return SqlTimeLiteral(t, precision, false, pos)
        }

        /**
         * Creates an interval literal.
         *
         * @param intervalStr       input string of '1:23:04'
         * @param intervalQualifier describes the interval type and precision
         * @param pos               Parser position
         */
        fun createInterval(
            sign: Int,
            intervalStr: String,
            intervalQualifier: SqlIntervalQualifier,
            pos: SqlParserPos
        ): SqlIntervalLiteral {
            return SqlIntervalLiteral(
                sign, intervalStr, intervalQualifier,
                intervalQualifier.typeName(), pos
            )
        }

        fun createNegative(
            num: SqlNumericLiteral,
            pos: SqlParserPos?
        ): SqlNumericLiteral {
            return SqlNumericLiteral(
                (requireNonNull(num.getValue()) as BigDecimal).negate(),
                num.getPrec(),
                num.getScale(),
                num.isExact(),
                pos
            )
        }

        fun createExactNumeric(
            s: String,
            pos: SqlParserPos?
        ): SqlNumericLiteral {
            val value: BigDecimal
            val prec: Int
            val scale: Int
            val i: Int = s.indexOf('.')
            if (i >= 0 && s.length() - 1 !== i) {
                value = SqlParserUtil.parseDecimal(s)
                scale = s.length() - i - 1
                assert(scale == value.scale()) { s }
                prec = s.length() - 1
            } else if (i >= 0 && s.length() - 1 === i) {
                value = SqlParserUtil.parseInteger(s.substring(0, i))
                scale = 0
                prec = s.length() - 1
            } else {
                value = SqlParserUtil.parseInteger(s)
                scale = 0
                prec = s.length()
            }
            return SqlNumericLiteral(
                value,
                prec,
                scale,
                true,
                pos
            )
        }

        fun createApproxNumeric(
            s: String?,
            pos: SqlParserPos?
        ): SqlNumericLiteral {
            val value: BigDecimal = SqlParserUtil.parseDecimal(s)
            return SqlNumericLiteral(value, null, null, false, pos)
        }

        /**
         * Creates a literal like X'ABAB'. Although it matters when we derive a type
         * for this beastie, we don't care at this point whether the number of
         * hexits is odd or even.
         */
        fun createBinaryString(
            s: String?,
            pos: SqlParserPos?
        ): SqlBinaryStringLiteral {
            val bits: BitString
            bits = try {
                BitString.createFromHexString(s)
            } catch (e: NumberFormatException) {
                throw SqlUtil.newContextException(
                    pos,
                    RESOURCE.binaryLiteralInvalid()
                )
            }
            return SqlBinaryStringLiteral(bits, pos)
        }

        /**
         * Creates a literal like X'ABAB' from an array of bytes.
         *
         * @param bytes Contents of binary literal
         * @param pos   Parser position
         * @return Binary string literal
         */
        fun createBinaryString(
            bytes: ByteArray?,
            pos: SqlParserPos?
        ): SqlBinaryStringLiteral {
            val bits: BitString
            bits = try {
                BitString.createFromBytes(bytes)
            } catch (e: NumberFormatException) {
                throw SqlUtil.newContextException(pos, RESOURCE.binaryLiteralInvalid())
            }
            return SqlBinaryStringLiteral(bits, pos)
        }

        /**
         * Creates a string literal in the system character set.
         *
         * @param s   a string (without the sql single quotes)
         * @param pos Parser position
         */
        fun createCharString(
            s: String?,
            pos: SqlParserPos?
        ): SqlCharStringLiteral {
            // UnsupportedCharsetException not possible
            return createCharString(s, null, pos)
        }

        /**
         * Creates a string literal, with optional character-set.
         *
         * @param s       a string (without the sql single quotes)
         * @param charSet character set name, null means take system default
         * @param pos     Parser position
         * @return A string literal
         * @throws UnsupportedCharsetException if charSet is not null but there is
         * no character set with that name in this
         * environment
         */
        fun createCharString(
            s: String?,
            @Nullable charSet: String?,
            pos: SqlParserPos?
        ): SqlCharStringLiteral {
            val slit = NlsString(s, charSet, null)
            return SqlCharStringLiteral(slit, pos)
        }
    }
}
