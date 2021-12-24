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

import org.apache.calcite.avatica.util.TimeUnit

/**
 * Represents an INTERVAL qualifier.
 *
 *
 * INTERVAL qualifier is defined as follows:
 *
 * <blockquote>`
 * <interval qualifier> ::=<br></br>
 * &nbsp;&nbsp; <start field> TO <end field><br></br>
 * &nbsp;&nbsp;| <single datetime field><br></br>
 * <start field> ::=<br></br>
 * &nbsp;&nbsp; <non-second primary datetime field><br></br>
 * &nbsp;&nbsp; [ <left paren> <interval leading field precision>
 * <right paren> ]<br></br>
 * <end field> ::=<br></br>
 * &nbsp;&nbsp; <non-second primary datetime field><br></br>
 * &nbsp;&nbsp;| SECOND [ <left paren>
 * <interval fractional seconds precision> <right paren> ]<br></br>
 * <single datetime field> ::=<br></br>
 * &nbsp;&nbsp;<non-second primary datetime field><br></br>
 * &nbsp;&nbsp;[ <left paren> <interval leading field precision>
 * <right paren> ]<br></br>
 * &nbsp;&nbsp;| SECOND [ <left paren>
 * <interval leading field precision><br></br>
 * &nbsp;&nbsp;[ <comma> <interval fractional seconds precision> ]
 * <right paren> ]<br></br>
 * <primary datetime field> ::=<br></br>
 * &nbsp;&nbsp;<non-second primary datetime field><br></br>
 * &nbsp;&nbsp;| SECOND<br></br>
 * <non-second primary datetime field> ::= YEAR | MONTH | DAY | HOUR
 * | MINUTE<br></br>
 * <interval fractional seconds precision> ::=
 * <unsigned integer><br></br>
 * <interval leading field precision> ::= <unsigned integer>
`</blockquote> *
 *
 *
 * Examples include:
 *
 *
 *  * `INTERVAL '1:23:45.678' HOUR TO SECOND`
 *  * `INTERVAL '1 2:3:4' DAY TO SECOND`
 *  * `INTERVAL '1 2:3:4' DAY(4) TO SECOND(4)`
 *
 *
 *
 * An instance of this class is immutable.
 */
class SqlIntervalQualifier(
    startUnit: TimeUnit,
    startPrecision: Int,
    @Nullable endUnit: TimeUnit?,
    fractionalSecondPrecision: Int,
    pos: SqlParserPos?
) : SqlNode(pos) {
    //~ Instance fields --------------------------------------------------------
    val startPrecisionPreservingDefault: Int
    val timeUnitRange: TimeUnitRange
    private val fractionalSecondPrecision: Int

    constructor(
        startUnit: TimeUnit,
        @Nullable endUnit: TimeUnit?,
        pos: SqlParserPos?
    ) : this(
        startUnit,
        RelDataType.PRECISION_NOT_SPECIFIED,
        endUnit,
        RelDataType.PRECISION_NOT_SPECIFIED,
        pos
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val kind: org.apache.calcite.sql.SqlKind
        get() = SqlKind.INTERVAL_QUALIFIER

    fun typeName(): SqlTypeName {
        return when (timeUnitRange) {
            YEAR, ISOYEAR, CENTURY, DECADE, MILLENNIUM -> SqlTypeName.INTERVAL_YEAR
            YEAR_TO_MONTH -> SqlTypeName.INTERVAL_YEAR_MONTH
            MONTH, QUARTER -> SqlTypeName.INTERVAL_MONTH
            DOW, ISODOW, DOY, DAY, WEEK -> SqlTypeName.INTERVAL_DAY
            DAY_TO_HOUR -> SqlTypeName.INTERVAL_DAY_HOUR
            DAY_TO_MINUTE -> SqlTypeName.INTERVAL_DAY_MINUTE
            DAY_TO_SECOND -> SqlTypeName.INTERVAL_DAY_SECOND
            HOUR -> SqlTypeName.INTERVAL_HOUR
            HOUR_TO_MINUTE -> SqlTypeName.INTERVAL_HOUR_MINUTE
            HOUR_TO_SECOND -> SqlTypeName.INTERVAL_HOUR_SECOND
            MINUTE -> SqlTypeName.INTERVAL_MINUTE
            MINUTE_TO_SECOND -> SqlTypeName.INTERVAL_MINUTE_SECOND
            SECOND, MILLISECOND, EPOCH, MICROSECOND, NANOSECOND -> SqlTypeName.INTERVAL_SECOND
            else -> throw AssertionError(timeUnitRange)
        }
    }

    @Override
    override fun validate(
        validator: SqlValidator,
        scope: SqlValidatorScope?
    ) {
        validator.validateIntervalQualifier(this)
    }

    @Override
    override fun <R> accept(visitor: SqlVisitor<R>): R {
        return visitor.visit(this)
    }

    @Override
    override fun equalsDeep(@Nullable node: SqlNode?, litmus: Litmus): Boolean {
        if (node == null) {
            return litmus.fail("other==null")
        }
        val thisString = this.toString()
        val thatString: String = node.toString()
        return if (!thisString.equals(thatString)) {
            litmus.fail("{} != {}", this, node)
        } else litmus.succeed()
    }

    fun getStartPrecision(typeSystem: RelDataTypeSystem): Int {
        return if (startPrecisionPreservingDefault == RelDataType.PRECISION_NOT_SPECIFIED) {
            typeSystem.getDefaultPrecision(typeName())
        } else {
            startPrecisionPreservingDefault
        }
    }

    /** Returns `true` if start precision is not specified.  */
    fun useDefaultStartPrecision(): Boolean {
        return startPrecisionPreservingDefault == RelDataType.PRECISION_NOT_SPECIFIED
    }

    fun getFractionalSecondPrecision(typeSystem: RelDataTypeSystem?): Int {
        return if (fractionalSecondPrecision == RelDataType.PRECISION_NOT_SPECIFIED) {
            typeName().getDefaultScale()
        } else {
            fractionalSecondPrecision
        }
    }

    val fractionalSecondPrecisionPreservingDefault: Int
        get() = if (useDefaultFractionalSecondPrecision()) {
            RelDataType.PRECISION_NOT_SPECIFIED
        } else {
            fractionalSecondPrecision
        }

    /** Returns `true` if fractional second precision is not specified.  */
    fun useDefaultFractionalSecondPrecision(): Boolean {
        return fractionalSecondPrecision == RelDataType.PRECISION_NOT_SPECIFIED
    }

    val startUnit: TimeUnit
        get() = timeUnitRange.startUnit
    val endUnit: TimeUnit
        get() = timeUnitRange.endUnit

    /** Returns `SECOND` for both `HOUR TO SECOND` and
     * `SECOND`.  */
    val unit: TimeUnit
        get() = Util.first(timeUnitRange.endUnit, timeUnitRange.startUnit)

    @Override
    override fun clone(pos: SqlParserPos?): SqlNode {
        return SqlIntervalQualifier(
            timeUnitRange.startUnit, startPrecisionPreservingDefault,
            timeUnitRange.endUnit, fractionalSecondPrecision, pos
        )
    }

    @Override
    override fun unparse(
        writer: SqlWriter,
        leftPrec: Int,
        rightPrec: Int
    ) {
        writer.getDialect()
            .unparseSqlIntervalQualifier(writer, this, RelDataTypeSystem.DEFAULT)
    }

    /**
     * Returns whether this interval has a single datetime field.
     *
     *
     * Returns `true` if it is of the form `unit`,
     * `false` if it is of the form `unit TO unit`.
     */
    val isSingleDatetimeField: Boolean
        get() = timeUnitRange.endUnit == null
    val isYearMonth: Boolean
        get() = timeUnitRange.startUnit.yearMonth

    /**
     * Returns 1 or -1.
     */
    fun getIntervalSign(value: String): Int {
        var sign = 1 // positive until proven otherwise
        if (!Util.isNullOrEmpty(value)) {
            if ('-' == value.charAt(0)) {
                sign = -1 // Negative
            }
        }
        return sign
    }

    private fun isLeadFieldInRange(
        typeSystem: RelDataTypeSystem,
        value: BigDecimal, @SuppressWarnings("unused") unit: TimeUnit
    ): Boolean {
        // we should never get handed a negative field value
        assert(value.compareTo(ZERO) >= 0)

        // Leading fields are only restricted by startPrecision.
        val startPrecision = getStartPrecision(typeSystem)
        return if (startPrecision < POWERS10.size) value.compareTo(
            POWERS10[startPrecision]
        ) < 0 else value.compareTo(INT_MAX_VALUE_PLUS_ONE) < 0
    }

    private fun checkLeadFieldInRange(
        typeSystem: RelDataTypeSystem, sign: Int,
        value: BigDecimal, unit: TimeUnit, pos: SqlParserPos
    ) {
        if (!isLeadFieldInRange(typeSystem, value, unit)) {
            throw fieldExceedsPrecisionException(
                pos, sign, value, unit, getStartPrecision(typeSystem)
            )
        }
    }

    //~ Constructors -----------------------------------------------------------
    init {
        var endUnit: TimeUnit? = endUnit
        if (endUnit === startUnit) {
            endUnit = null
        }
        timeUnitRange = TimeUnitRange.of(Objects.requireNonNull(startUnit, "startUnit"), endUnit)
        startPrecisionPreservingDefault = startPrecision
        this.fractionalSecondPrecision = fractionalSecondPrecision
    }

    /**
     * Validates an INTERVAL literal against a YEAR interval qualifier.
     *
     * @throws org.apache.calcite.runtime.CalciteContextException if the interval
     * value is illegal
     */
    private fun evaluateIntervalLiteralAsYear(
        typeSystem: RelDataTypeSystem, sign: Int,
        value: String,
        originalValue: String,
        pos: SqlParserPos
    ): IntArray {
        val year: BigDecimal

        // validate as YEAR(startPrecision), e.g. 'YY'
        val intervalPattern = "(\\d+)"
        val m: Matcher = Pattern.compile(intervalPattern).matcher(value)
        return if (m.matches()) {
            // Break out  field values
            year = try {
                parseField(m, 1)
            } catch (e: NumberFormatException) {
                throw invalidValueException(pos, originalValue)
            }

            // Validate individual fields
            checkLeadFieldInRange(typeSystem, sign, year, TimeUnit.YEAR, pos)

            // package values up for return
            fillIntervalValueArray(sign, year, ZERO)
        } else {
            throw invalidValueException(pos, originalValue)
        }
    }

    /**
     * Validates an INTERVAL literal against a YEAR TO MONTH interval qualifier.
     *
     * @throws org.apache.calcite.runtime.CalciteContextException if the interval
     * value is illegal
     */
    private fun evaluateIntervalLiteralAsYearToMonth(
        typeSystem: RelDataTypeSystem, sign: Int,
        value: String,
        originalValue: String,
        pos: SqlParserPos
    ): IntArray {
        val year: BigDecimal
        val month: BigDecimal

        // validate as YEAR(startPrecision) TO MONTH, e.g. 'YY-DD'
        val intervalPattern = "(\\d+)-(\\d{1,2})"
        val m: Matcher = Pattern.compile(intervalPattern).matcher(value)
        return if (m.matches()) {
            // Break out  field values
            try {
                year = parseField(m, 1)
                month = parseField(m, 2)
            } catch (e: NumberFormatException) {
                throw invalidValueException(pos, originalValue)
            }

            // Validate individual fields
            checkLeadFieldInRange(typeSystem, sign, year, TimeUnit.YEAR, pos)
            if (!isSecondaryFieldInRange(month, TimeUnit.MONTH)) {
                throw invalidValueException(pos, originalValue)
            }

            // package values up for return
            fillIntervalValueArray(sign, year, month)
        } else {
            throw invalidValueException(pos, originalValue)
        }
    }

    /**
     * Validates an INTERVAL literal against a MONTH interval qualifier.
     *
     * @throws org.apache.calcite.runtime.CalciteContextException if the interval
     * value is illegal
     */
    private fun evaluateIntervalLiteralAsMonth(
        typeSystem: RelDataTypeSystem, sign: Int,
        value: String,
        originalValue: String,
        pos: SqlParserPos
    ): IntArray {
        val month: BigDecimal

        // validate as MONTH(startPrecision), e.g. 'MM'
        val intervalPattern = "(\\d+)"
        val m: Matcher = Pattern.compile(intervalPattern).matcher(value)
        return if (m.matches()) {
            // Break out  field values
            month = try {
                parseField(m, 1)
            } catch (e: NumberFormatException) {
                throw invalidValueException(pos, originalValue)
            }

            // Validate individual fields
            checkLeadFieldInRange(typeSystem, sign, month, TimeUnit.MONTH, pos)

            // package values up for return
            fillIntervalValueArray(sign, ZERO, month)
        } else {
            throw invalidValueException(pos, originalValue)
        }
    }

    /**
     * Validates an INTERVAL literal against a DAY interval qualifier.
     *
     * @throws org.apache.calcite.runtime.CalciteContextException if the interval
     * value is illegal
     */
    private fun evaluateIntervalLiteralAsDay(
        typeSystem: RelDataTypeSystem, sign: Int,
        value: String,
        originalValue: String,
        pos: SqlParserPos
    ): IntArray {
        val day: BigDecimal

        // validate as DAY(startPrecision), e.g. 'DD'
        val intervalPattern = "(\\d+)"
        val m: Matcher = Pattern.compile(intervalPattern).matcher(value)
        return if (m.matches()) {
            // Break out  field values
            day = try {
                parseField(m, 1)
            } catch (e: NumberFormatException) {
                throw invalidValueException(pos, originalValue)
            }

            // Validate individual fields
            checkLeadFieldInRange(typeSystem, sign, day, TimeUnit.DAY, pos)

            // package values up for return
            fillIntervalValueArray(sign, day, ZERO, ZERO, ZERO, ZERO)
        } else {
            throw invalidValueException(pos, originalValue)
        }
    }

    /**
     * Validates an INTERVAL literal against a DAY TO HOUR interval qualifier.
     *
     * @throws org.apache.calcite.runtime.CalciteContextException if the interval
     * value is illegal
     */
    private fun evaluateIntervalLiteralAsDayToHour(
        typeSystem: RelDataTypeSystem, sign: Int,
        value: String,
        originalValue: String,
        pos: SqlParserPos
    ): IntArray {
        val day: BigDecimal
        val hour: BigDecimal

        // validate as DAY(startPrecision) TO HOUR, e.g. 'DD HH'
        val intervalPattern = "(\\d+) (\\d{1,2})"
        val m: Matcher = Pattern.compile(intervalPattern).matcher(value)
        return if (m.matches()) {
            // Break out  field values
            try {
                day = parseField(m, 1)
                hour = parseField(m, 2)
            } catch (e: NumberFormatException) {
                throw invalidValueException(pos, originalValue)
            }

            // Validate individual fields
            checkLeadFieldInRange(typeSystem, sign, day, TimeUnit.DAY, pos)
            if (!isSecondaryFieldInRange(hour, TimeUnit.HOUR)) {
                throw invalidValueException(pos, originalValue)
            }

            // package values up for return
            fillIntervalValueArray(sign, day, hour, ZERO, ZERO, ZERO)
        } else {
            throw invalidValueException(pos, originalValue)
        }
    }

    /**
     * Validates an INTERVAL literal against a DAY TO MINUTE interval qualifier.
     *
     * @throws org.apache.calcite.runtime.CalciteContextException if the interval
     * value is illegal
     */
    private fun evaluateIntervalLiteralAsDayToMinute(
        typeSystem: RelDataTypeSystem, sign: Int,
        value: String,
        originalValue: String,
        pos: SqlParserPos
    ): IntArray {
        val day: BigDecimal
        val hour: BigDecimal
        val minute: BigDecimal

        // validate as DAY(startPrecision) TO MINUTE, e.g. 'DD HH:MM'
        val intervalPattern = "(\\d+) (\\d{1,2}):(\\d{1,2})"
        val m: Matcher = Pattern.compile(intervalPattern).matcher(value)
        return if (m.matches()) {
            // Break out  field values
            try {
                day = parseField(m, 1)
                hour = parseField(m, 2)
                minute = parseField(m, 3)
            } catch (e: NumberFormatException) {
                throw invalidValueException(pos, originalValue)
            }

            // Validate individual fields
            checkLeadFieldInRange(typeSystem, sign, day, TimeUnit.DAY, pos)
            if (!isSecondaryFieldInRange(hour, TimeUnit.HOUR)
                || !isSecondaryFieldInRange(minute, TimeUnit.MINUTE)
            ) {
                throw invalidValueException(pos, originalValue)
            }

            // package values up for return
            fillIntervalValueArray(sign, day, hour, minute, ZERO, ZERO)
        } else {
            throw invalidValueException(pos, originalValue)
        }
    }

    /**
     * Validates an INTERVAL literal against a DAY TO SECOND interval qualifier.
     *
     * @throws org.apache.calcite.runtime.CalciteContextException if the interval
     * value is illegal
     */
    private fun evaluateIntervalLiteralAsDayToSecond(
        typeSystem: RelDataTypeSystem, sign: Int,
        value: String,
        originalValue: String,
        pos: SqlParserPos
    ): IntArray {
        val day: BigDecimal
        val hour: BigDecimal
        val minute: BigDecimal
        val second: BigDecimal
        val secondFrac: BigDecimal
        val hasFractionalSecond: Boolean

        // validate as DAY(startPrecision) TO MINUTE,
        // e.g. 'DD HH:MM:SS' or 'DD HH:MM:SS.SSS'
        // Note: must check two patterns, since fractional second is optional
        val fractionalSecondPrecision = getFractionalSecondPrecision(typeSystem)
        val intervalPatternWithFracSec = ("(\\d+) (\\d{1,2}):(\\d{1,2}):(\\d{1,2})\\.(\\d{1,"
                + fractionalSecondPrecision + "})")
        val intervalPatternWithoutFracSec = "(\\d+) (\\d{1,2}):(\\d{1,2}):(\\d{1,2})"
        var m: Matcher = Pattern.compile(intervalPatternWithFracSec).matcher(value)
        if (m.matches()) {
            hasFractionalSecond = true
        } else {
            m = Pattern.compile(intervalPatternWithoutFracSec).matcher(value)
            hasFractionalSecond = false
        }
        return if (m.matches()) {
            // Break out  field values
            try {
                day = parseField(m, 1)
                hour = parseField(m, 2)
                minute = parseField(m, 3)
                second = parseField(m, 4)
            } catch (e: NumberFormatException) {
                throw invalidValueException(pos, originalValue)
            }
            secondFrac = if (hasFractionalSecond) {
                normalizeSecondFraction(castNonNull(m.group(5)))
            } else {
                ZERO
            }

            // Validate individual fields
            checkLeadFieldInRange(typeSystem, sign, day, TimeUnit.DAY, pos)
            if (!isSecondaryFieldInRange(hour, TimeUnit.HOUR)
                || !isSecondaryFieldInRange(minute, TimeUnit.MINUTE)
                || !isSecondaryFieldInRange(second, TimeUnit.SECOND)
                || !isFractionalSecondFieldInRange(secondFrac)
            ) {
                throw invalidValueException(pos, originalValue)
            }

            // package values up for return
            fillIntervalValueArray(
                sign,
                day,
                hour,
                minute,
                second,
                secondFrac
            )
        } else {
            throw invalidValueException(pos, originalValue)
        }
    }

    /**
     * Validates an INTERVAL literal against an HOUR interval qualifier.
     *
     * @throws org.apache.calcite.runtime.CalciteContextException if the interval
     * value is illegal
     */
    private fun evaluateIntervalLiteralAsHour(
        typeSystem: RelDataTypeSystem, sign: Int,
        value: String,
        originalValue: String,
        pos: SqlParserPos
    ): IntArray {
        val hour: BigDecimal

        // validate as HOUR(startPrecision), e.g. 'HH'
        val intervalPattern = "(\\d+)"
        val m: Matcher = Pattern.compile(intervalPattern).matcher(value)
        return if (m.matches()) {
            // Break out  field values
            hour = try {
                parseField(m, 1)
            } catch (e: NumberFormatException) {
                throw invalidValueException(pos, originalValue)
            }

            // Validate individual fields
            checkLeadFieldInRange(typeSystem, sign, hour, TimeUnit.HOUR, pos)

            // package values up for return
            fillIntervalValueArray(sign, ZERO, hour, ZERO, ZERO, ZERO)
        } else {
            throw invalidValueException(pos, originalValue)
        }
    }

    /**
     * Validates an INTERVAL literal against an HOUR TO MINUTE interval
     * qualifier.
     *
     * @throws org.apache.calcite.runtime.CalciteContextException if the interval
     * value is illegal
     */
    private fun evaluateIntervalLiteralAsHourToMinute(
        typeSystem: RelDataTypeSystem, sign: Int,
        value: String,
        originalValue: String,
        pos: SqlParserPos
    ): IntArray {
        val hour: BigDecimal
        val minute: BigDecimal

        // validate as HOUR(startPrecision) TO MINUTE, e.g. 'HH:MM'
        val intervalPattern = "(\\d+):(\\d{1,2})"
        val m: Matcher = Pattern.compile(intervalPattern).matcher(value)
        return if (m.matches()) {
            // Break out  field values
            try {
                hour = parseField(m, 1)
                minute = parseField(m, 2)
            } catch (e: NumberFormatException) {
                throw invalidValueException(pos, originalValue)
            }

            // Validate individual fields
            checkLeadFieldInRange(typeSystem, sign, hour, TimeUnit.HOUR, pos)
            if (!isSecondaryFieldInRange(minute, TimeUnit.MINUTE)) {
                throw invalidValueException(pos, originalValue)
            }

            // package values up for return
            fillIntervalValueArray(sign, ZERO, hour, minute, ZERO, ZERO)
        } else {
            throw invalidValueException(pos, originalValue)
        }
    }

    /**
     * Validates an INTERVAL literal against an HOUR TO SECOND interval
     * qualifier.
     *
     * @throws org.apache.calcite.runtime.CalciteContextException if the interval
     * value is illegal
     */
    private fun evaluateIntervalLiteralAsHourToSecond(
        typeSystem: RelDataTypeSystem, sign: Int,
        value: String,
        originalValue: String,
        pos: SqlParserPos
    ): IntArray {
        val hour: BigDecimal
        val minute: BigDecimal
        val second: BigDecimal
        val secondFrac: BigDecimal
        val hasFractionalSecond: Boolean

        // validate as HOUR(startPrecision) TO SECOND,
        // e.g. 'HH:MM:SS' or 'HH:MM:SS.SSS'
        // Note: must check two patterns, since fractional second is optional
        val fractionalSecondPrecision = getFractionalSecondPrecision(typeSystem)
        val intervalPatternWithFracSec = ("(\\d+):(\\d{1,2}):(\\d{1,2})\\.(\\d{1,"
                + fractionalSecondPrecision + "})")
        val intervalPatternWithoutFracSec = "(\\d+):(\\d{1,2}):(\\d{1,2})"
        var m: Matcher = Pattern.compile(intervalPatternWithFracSec).matcher(value)
        if (m.matches()) {
            hasFractionalSecond = true
        } else {
            m = Pattern.compile(intervalPatternWithoutFracSec).matcher(value)
            hasFractionalSecond = false
        }
        return if (m.matches()) {
            // Break out  field values
            try {
                hour = parseField(m, 1)
                minute = parseField(m, 2)
                second = parseField(m, 3)
            } catch (e: NumberFormatException) {
                throw invalidValueException(pos, originalValue)
            }
            secondFrac = if (hasFractionalSecond) {
                normalizeSecondFraction(castNonNull(m.group(4)))
            } else {
                ZERO
            }

            // Validate individual fields
            checkLeadFieldInRange(typeSystem, sign, hour, TimeUnit.HOUR, pos)
            if (!isSecondaryFieldInRange(minute, TimeUnit.MINUTE)
                || !isSecondaryFieldInRange(second, TimeUnit.SECOND)
                || !isFractionalSecondFieldInRange(secondFrac)
            ) {
                throw invalidValueException(pos, originalValue)
            }

            // package values up for return
            fillIntervalValueArray(
                sign,
                ZERO,
                hour,
                minute,
                second,
                secondFrac
            )
        } else {
            throw invalidValueException(pos, originalValue)
        }
    }

    /**
     * Validates an INTERVAL literal against an MINUTE interval qualifier.
     *
     * @throws org.apache.calcite.runtime.CalciteContextException if the interval
     * value is illegal
     */
    private fun evaluateIntervalLiteralAsMinute(
        typeSystem: RelDataTypeSystem, sign: Int,
        value: String,
        originalValue: String,
        pos: SqlParserPos
    ): IntArray {
        val minute: BigDecimal

        // validate as MINUTE(startPrecision), e.g. 'MM'
        val intervalPattern = "(\\d+)"
        val m: Matcher = Pattern.compile(intervalPattern).matcher(value)
        return if (m.matches()) {
            // Break out  field values
            minute = try {
                parseField(m, 1)
            } catch (e: NumberFormatException) {
                throw invalidValueException(pos, originalValue)
            }

            // Validate individual fields
            checkLeadFieldInRange(typeSystem, sign, minute, TimeUnit.MINUTE, pos)

            // package values up for return
            fillIntervalValueArray(sign, ZERO, ZERO, minute, ZERO, ZERO)
        } else {
            throw invalidValueException(pos, originalValue)
        }
    }

    /**
     * Validates an INTERVAL literal against an MINUTE TO SECOND interval
     * qualifier.
     *
     * @throws org.apache.calcite.runtime.CalciteContextException if the interval
     * value is illegal
     */
    private fun evaluateIntervalLiteralAsMinuteToSecond(
        typeSystem: RelDataTypeSystem, sign: Int,
        value: String,
        originalValue: String,
        pos: SqlParserPos
    ): IntArray {
        val minute: BigDecimal
        val second: BigDecimal
        val secondFrac: BigDecimal
        val hasFractionalSecond: Boolean

        // validate as MINUTE(startPrecision) TO SECOND,
        // e.g. 'MM:SS' or 'MM:SS.SSS'
        // Note: must check two patterns, since fractional second is optional
        val fractionalSecondPrecision = getFractionalSecondPrecision(typeSystem)
        val intervalPatternWithFracSec = "(\\d+):(\\d{1,2})\\.(\\d{1,$fractionalSecondPrecision})"
        val intervalPatternWithoutFracSec = "(\\d+):(\\d{1,2})"
        var m: Matcher = Pattern.compile(intervalPatternWithFracSec).matcher(value)
        if (m.matches()) {
            hasFractionalSecond = true
        } else {
            m = Pattern.compile(intervalPatternWithoutFracSec).matcher(value)
            hasFractionalSecond = false
        }
        return if (m.matches()) {
            // Break out  field values
            try {
                minute = parseField(m, 1)
                second = parseField(m, 2)
            } catch (e: NumberFormatException) {
                throw invalidValueException(pos, originalValue)
            }
            secondFrac = if (hasFractionalSecond) {
                normalizeSecondFraction(castNonNull(m.group(3)))
            } else {
                ZERO
            }

            // Validate individual fields
            checkLeadFieldInRange(typeSystem, sign, minute, TimeUnit.MINUTE, pos)
            if (!isSecondaryFieldInRange(second, TimeUnit.SECOND)
                || !isFractionalSecondFieldInRange(secondFrac)
            ) {
                throw invalidValueException(pos, originalValue)
            }

            // package values up for return
            fillIntervalValueArray(
                sign,
                ZERO,
                ZERO,
                minute,
                second,
                secondFrac
            )
        } else {
            throw invalidValueException(pos, originalValue)
        }
    }

    /**
     * Validates an INTERVAL literal against an SECOND interval qualifier.
     *
     * @throws org.apache.calcite.runtime.CalciteContextException if the interval
     * value is illegal
     */
    private fun evaluateIntervalLiteralAsSecond(
        typeSystem: RelDataTypeSystem,
        sign: Int,
        value: String,
        originalValue: String,
        pos: SqlParserPos
    ): IntArray {
        val second: BigDecimal
        val secondFrac: BigDecimal
        val hasFractionalSecond: Boolean

        // validate as SECOND(startPrecision, fractionalSecondPrecision)
        // e.g. 'SS' or 'SS.SSS'
        // Note: must check two patterns, since fractional second is optional
        val fractionalSecondPrecision = getFractionalSecondPrecision(typeSystem)
        val intervalPatternWithFracSec = "(\\d+)\\.(\\d{1,$fractionalSecondPrecision})"
        val intervalPatternWithoutFracSec = "(\\d+)"
        var m: Matcher = Pattern.compile(intervalPatternWithFracSec).matcher(value)
        if (m.matches()) {
            hasFractionalSecond = true
        } else {
            m = Pattern.compile(intervalPatternWithoutFracSec).matcher(value)
            hasFractionalSecond = false
        }
        return if (m.matches()) {
            // Break out  field values
            second = try {
                parseField(m, 1)
            } catch (e: NumberFormatException) {
                throw invalidValueException(pos, originalValue)
            }
            secondFrac = if (hasFractionalSecond) {
                normalizeSecondFraction(castNonNull(m.group(2)))
            } else {
                ZERO
            }

            // Validate individual fields
            checkLeadFieldInRange(typeSystem, sign, second, TimeUnit.SECOND, pos)
            if (!isFractionalSecondFieldInRange(secondFrac)) {
                throw invalidValueException(pos, originalValue)
            }

            // package values up for return
            fillIntervalValueArray(
                sign, ZERO, ZERO, ZERO, second, secondFrac
            )
        } else {
            throw invalidValueException(pos, originalValue)
        }
    }

    /**
     * Validates an INTERVAL literal according to the rules specified by the
     * interval qualifier. The assumption is made that the interval qualifier has
     * been validated prior to calling this method. Evaluating against an
     * invalid qualifier could lead to strange results.
     *
     * @return field values, never null
     *
     * @throws org.apache.calcite.runtime.CalciteContextException if the interval
     * value is illegal
     */
    fun evaluateIntervalLiteral(
        value: String, pos: SqlParserPos,
        typeSystem: RelDataTypeSystem
    ): IntArray {
        // save original value for if we have to throw
        var value = value
        val value0 = value

        // First strip off any leading whitespace
        value = value.trim()

        // check if the sign was explicitly specified.  Record
        // the explicit or implicit sign, and strip it off to
        // simplify pattern matching later.
        val sign = getIntervalSign(value)
        value = stripLeadingSign(value)

        // If we have an empty or null literal at this point,
        // it's illegal.  Complain and bail out.
        if (Util.isNullOrEmpty(value)) {
            throw invalidValueException(pos, value0)
        }
        return when (timeUnitRange) {
            YEAR -> evaluateIntervalLiteralAsYear(
                typeSystem, sign, value, value0,
                pos
            )
            YEAR_TO_MONTH -> evaluateIntervalLiteralAsYearToMonth(
                typeSystem, sign, value,
                value0, pos
            )
            MONTH -> evaluateIntervalLiteralAsMonth(
                typeSystem, sign, value, value0,
                pos
            )
            DAY -> evaluateIntervalLiteralAsDay(typeSystem, sign, value, value0, pos)
            DAY_TO_HOUR -> evaluateIntervalLiteralAsDayToHour(
                typeSystem, sign, value, value0,
                pos
            )
            DAY_TO_MINUTE -> evaluateIntervalLiteralAsDayToMinute(
                typeSystem, sign, value,
                value0, pos
            )
            DAY_TO_SECOND -> evaluateIntervalLiteralAsDayToSecond(
                typeSystem, sign, value,
                value0, pos
            )
            HOUR -> evaluateIntervalLiteralAsHour(
                typeSystem, sign, value, value0,
                pos
            )
            HOUR_TO_MINUTE -> evaluateIntervalLiteralAsHourToMinute(
                typeSystem, sign, value,
                value0, pos
            )
            HOUR_TO_SECOND -> evaluateIntervalLiteralAsHourToSecond(
                typeSystem, sign, value,
                value0, pos
            )
            MINUTE -> evaluateIntervalLiteralAsMinute(
                typeSystem, sign, value, value0,
                pos
            )
            MINUTE_TO_SECOND -> evaluateIntervalLiteralAsMinuteToSecond(
                typeSystem, sign, value,
                value0, pos
            )
            SECOND -> evaluateIntervalLiteralAsSecond(
                typeSystem, sign, value, value0,
                pos
            )
            else -> throw invalidValueException(pos, value0)
        }
    }

    private fun invalidValueException(
        pos: SqlParserPos,
        value: String
    ): CalciteContextException {
        return SqlUtil.newContextException(
            pos,
            RESOURCE.unsupportedIntervalLiteral(
                "'$value'", "INTERVAL " + toString()
            )
        )
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        private val ZERO: BigDecimal = BigDecimal.ZERO
        private val THOUSAND: BigDecimal = BigDecimal.valueOf(1000)
        private val INT_MAX_VALUE_PLUS_ONE: BigDecimal = BigDecimal.valueOf(Integer.MAX_VALUE).add(BigDecimal.ONE)
        fun combineStartPrecisionPreservingDefault(
            typeSystem: RelDataTypeSystem,
            qual1: SqlIntervalQualifier,
            qual2: SqlIntervalQualifier
        ): Int {
            val start1 = qual1.getStartPrecision(typeSystem)
            val start2 = qual2.getStartPrecision(typeSystem)
            return if (start1 > start2) {
                // qual1 is more precise, but if it has the default indicator
                // set, we need to return that indicator so result will also
                // use default
                qual1.startPrecisionPreservingDefault
            } else if (start1 < start2) {
                // qual2 is more precise, but if it has the default indicator
                // set, we need to return that indicator so result will also
                // use default
                qual2.startPrecisionPreservingDefault
            } else {
                // they are equal.  return default if both are default,
                // otherwise return exact precision
                if (qual1.useDefaultStartPrecision()
                    && qual2.useDefaultStartPrecision()
                ) {
                    qual1.startPrecisionPreservingDefault
                } else {
                    start1
                }
            }
        }

        fun combineFractionalSecondPrecisionPreservingDefault(
            typeSystem: RelDataTypeSystem?,
            qual1: SqlIntervalQualifier,
            qual2: SqlIntervalQualifier
        ): Int {
            val p1 = qual1.getFractionalSecondPrecision(typeSystem)
            val p2 = qual2.getFractionalSecondPrecision(typeSystem)
            return if (p1 > p2) {
                // qual1 is more precise, but if it has the default indicator
                // set, we need to return that indicator so result will also
                // use default
                qual1.fractionalSecondPrecisionPreservingDefault
            } else if (p1 < p2) {
                // qual2 is more precise, but if it has the default indicator
                // set, we need to return that indicator so result will also
                // use default
                qual2.fractionalSecondPrecisionPreservingDefault
            } else {
                // they are equal.  return default if both are default,
                // otherwise return exact precision
                if (qual1.useDefaultFractionalSecondPrecision()
                    && qual2.useDefaultFractionalSecondPrecision()
                ) {
                    qual1.fractionalSecondPrecisionPreservingDefault
                } else {
                    p1
                }
            }
        }

        private fun stripLeadingSign(value: String): String {
            var unsignedValue = value
            if (!Util.isNullOrEmpty(value)) {
                if ('-' == value.charAt(0) || '+' == value.charAt(0)) {
                    unsignedValue = value.substring(1)
                }
            }
            return unsignedValue
        }

        private val POWERS10: Array<BigDecimal> = arrayOf<BigDecimal>(
            ZERO,
            BigDecimal.valueOf(10),
            BigDecimal.valueOf(100),
            BigDecimal.valueOf(1000),
            BigDecimal.valueOf(10000),
            BigDecimal.valueOf(100000),
            BigDecimal.valueOf(1000000),
            BigDecimal.valueOf(10000000),
            BigDecimal.valueOf(100000000),
            BigDecimal.valueOf(1000000000)
        )

        private fun isFractionalSecondFieldInRange(field: BigDecimal): Boolean {
            // we should never get handed a negative field value
            assert(field.compareTo(ZERO) >= 0)

            // Fractional second fields are only restricted by precision, which
            // has already been checked for using pattern matching.
            // Therefore, always return true
            return true
        }

        private fun isSecondaryFieldInRange(field: BigDecimal, unit: TimeUnit?): Boolean {
            // we should never get handed a negative field value
            assert(field.compareTo(ZERO) >= 0)
            assert(unit != null)
            return when (unit) {
                YEAR, DAY -> throw Util.unexpected(unit)
                MONTH, HOUR, MINUTE, SECOND -> unit.isValidValue(field)
                else -> throw Util.unexpected(unit)
            }
        }

        private fun normalizeSecondFraction(secondFracStr: String): BigDecimal {
            // Decimal value can be more than 3 digits. So just get
            // the millisecond part.
            return BigDecimal("0.$secondFracStr").multiply(THOUSAND)
        }

        private fun fillIntervalValueArray(
            sign: Int,
            year: BigDecimal,
            month: BigDecimal
        ): IntArray {
            val ret = IntArray(3)
            ret[0] = sign
            ret[1] = year.intValue()
            ret[2] = month.intValue()
            return ret
        }

        private fun fillIntervalValueArray(
            sign: Int,
            day: BigDecimal,
            hour: BigDecimal,
            minute: BigDecimal,
            second: BigDecimal,
            secondFrac: BigDecimal
        ): IntArray {
            val ret = IntArray(6)
            ret[0] = sign
            ret[1] = day.intValue()
            ret[2] = hour.intValue()
            ret[3] = minute.intValue()
            ret[4] = second.intValue()
            ret[5] = secondFrac.intValue()
            return ret
        }

        private fun parseField(m: Matcher, i: Int): BigDecimal {
            return BigDecimal(castNonNull(m.group(i)))
        }

        private fun fieldExceedsPrecisionException(
            pos: SqlParserPos, sign: Int, value: BigDecimal, type: TimeUnit,
            precision: Int
        ): CalciteContextException {
            var value: BigDecimal = value
            if (sign == -1) {
                value = value.negate()
            }
            return SqlUtil.newContextException(
                pos,
                RESOURCE.intervalFieldExceedsPrecision(
                    value, type.name() + "(" + precision + ")"
                )
            )
        }
    }
}
