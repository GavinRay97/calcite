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
import org.apache.calcite.avatica.util.Spaces
import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.core.CorrelationId
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.runtime.FlatLists
import org.apache.calcite.runtime.Geometries
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.SqlCollation
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.SqlUtil
import org.apache.calcite.sql.`fun`.SqlCountAggFunction
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.type.ArraySqlType
import org.apache.calcite.sql.type.MapSqlType
import org.apache.calcite.sql.type.MultisetSqlType
import org.apache.calcite.sql.type.SqlTypeFamily
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.sql.type.SqlTypeUtil
import org.apache.calcite.util.DateString
import org.apache.calcite.util.NlsString
import org.apache.calcite.util.Pair
import org.apache.calcite.util.Sarg
import org.apache.calcite.util.TimeString
import org.apache.calcite.util.TimestampString
import org.apache.calcite.util.Util
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableRangeSet
import com.google.common.collect.Range
import com.google.common.collect.RangeSet
import com.google.common.collect.TreeRangeSet
import org.checkerframework.checker.nullness.qual.PolyNull
import java.math.BigDecimal
import java.math.MathContext
import java.math.RoundingMode
import java.nio.charset.Charset
import java.util.ArrayList
import java.util.Arrays
import java.util.Calendar
import java.util.List
import java.util.Locale
import java.util.Map
import java.util.Objects
import java.util.function.IntPredicate
import org.apache.calcite.linq4j.Nullness.castNonNull

/**
 * Factory for row expressions.
 *
 *
 * Some common literal values (NULL, TRUE, FALSE, 0, 1, '') are cached.
 */
class RexBuilder @SuppressWarnings("method.invocation.invalid") constructor(typeFactory: RelDataTypeFactory) {
    //~ Instance fields --------------------------------------------------------
    protected val typeFactory: RelDataTypeFactory
    private val booleanTrue: RexLiteral
    private val booleanFalse: RexLiteral
    private val charEmpty: RexLiteral
    private val constantNull: RexLiteral

    /**
     * Returns this RexBuilder's operator table.
     *
     * @return operator table
     */
    val opTab: SqlStdOperatorTable = SqlStdOperatorTable.instance()
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a RexBuilder.
     *
     * @param typeFactory Type factory
     */
    init {
        this.typeFactory = typeFactory
        booleanTrue = makeLiteral(
            Boolean.TRUE,
            typeFactory.createSqlType(SqlTypeName.BOOLEAN),
            SqlTypeName.BOOLEAN
        )
        booleanFalse = makeLiteral(
            Boolean.FALSE,
            typeFactory.createSqlType(SqlTypeName.BOOLEAN),
            SqlTypeName.BOOLEAN
        )
        charEmpty = makeLiteral(
            NlsString("", null, null),
            typeFactory.createSqlType(SqlTypeName.CHAR, 0),
            SqlTypeName.CHAR
        )
        constantNull = makeLiteral(
            null,
            typeFactory.createSqlType(SqlTypeName.NULL),
            SqlTypeName.NULL
        )
    }

    /** Creates a list of [org.apache.calcite.rex.RexInputRef] expressions,
     * projecting the fields of a given record type.  */
    fun identityProjects(rowType: RelDataType): List<RexNode> {
        return Util.transform(
            rowType.getFieldList()
        ) { input -> RexInputRef(input.getIndex(), input.getType()) }
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Returns this RexBuilder's type factory.
     *
     * @return type factory
     */
    fun getTypeFactory(): RelDataTypeFactory {
        return typeFactory
    }

    /**
     * Creates an expression accessing a given named field from a record.
     *
     *
     * NOTE: Be careful choosing the value of `caseSensitive`.
     * If the field name was supplied by an end-user (e.g. as a column alias in
     * SQL), use your session's case-sensitivity setting.
     * Only hard-code `true` if you are sure that the field name is
     * internally generated.
     * Hard-coding `false` is almost certainly wrong.
     *
     * @param expr      Expression yielding a record
     * @param fieldName Name of field in record
     * @param caseSensitive Whether match is case-sensitive
     * @return Expression accessing a given named field
     */
    fun makeFieldAccess(
        expr: RexNode, fieldName: String,
        caseSensitive: Boolean
    ): RexNode {
        val type: RelDataType = expr.getType()
        val field: RelDataTypeField = type.getField(fieldName, caseSensitive, false)
            ?: throw AssertionError(
                "Type '" + type + "' has no field '"
                        + fieldName + "'"
            )
        return makeFieldAccessInternal(expr, field)
    }

    /**
     * Creates an expression accessing a field with a given ordinal from a
     * record.
     *
     * @param expr Expression yielding a record
     * @param i    Ordinal of field
     * @return Expression accessing given field
     */
    fun makeFieldAccess(
        expr: RexNode,
        i: Int
    ): RexNode {
        val type: RelDataType = expr.getType()
        val fields: List<RelDataTypeField> = type.getFieldList()
        if (i < 0 || i >= fields.size()) {
            throw AssertionError(
                "Field ordinal " + i + " is invalid for "
                        + " type '" + type + "'"
            )
        }
        return makeFieldAccessInternal(expr, fields[i])
    }

    /**
     * Creates an expression accessing a given field from a record.
     *
     * @param expr  Expression yielding a record
     * @param field Field
     * @return Expression accessing given field
     */
    private fun makeFieldAccessInternal(
        expr: RexNode,
        field: RelDataTypeField
    ): RexNode {
        if (expr is RexRangeRef) {
            val range: RexRangeRef = expr as RexRangeRef
            return if (field.getIndex() < 0) {
                makeCall(
                    field.getType(),
                    GET_OPERATOR,
                    ImmutableList.of(
                        expr,
                        makeLiteral(field.getName())
                    )
                )
            } else RexInputRef(
                range.getOffset() + field.getIndex(),
                field.getType()
            )
        }
        return RexFieldAccess(expr, field)
    }

    /**
     * Creates a call with a list of arguments and a predetermined type.
     */
    fun makeCall(
        returnType: RelDataType?,
        op: SqlOperator,
        exprs: List<RexNode?>
    ): RexNode {
        return RexCall(returnType, op, exprs)
    }

    /**
     * Creates a call with an array of arguments.
     *
     *
     * If you already know the return type of the call, then
     * [.makeCall]
     * is preferred.
     */
    fun makeCall(
        op: SqlOperator,
        exprs: List<RexNode?>
    ): RexNode {
        val type: RelDataType = deriveReturnType(op, exprs)
        return RexCall(type, op, exprs)
    }

    /**
     * Creates a call with a list of arguments.
     *
     *
     * Equivalent to
     * `makeCall(op, exprList.toArray(new RexNode[exprList.size()]))`.
     */
    fun makeCall(
        op: SqlOperator,
        vararg exprs: RexNode?
    ): RexNode {
        return makeCall(op, ImmutableList.copyOf(exprs))
    }

    /**
     * Derives the return type of a call to an operator.
     *
     * @param op          the operator being called
     * @param exprs       actual operands
     * @return derived type
     */
    fun deriveReturnType(
        op: SqlOperator,
        exprs: List<RexNode?>?
    ): RelDataType {
        return op.inferReturnType(
            RexCallBinding(
                typeFactory, op, exprs,
                ImmutableList.of()
            )
        )
    }

    /**
     * Creates a reference to an aggregate call, checking for repeated calls.
     *
     *
     * Argument types help to optimize for repeated aggregates.
     * For instance count(42) is equivalent to count(*).
     *
     * @param aggCall aggregate call to be added
     * @param groupCount number of groups in the aggregate relation
     * @param aggCalls destination list of aggregate calls
     * @param aggCallMapping the dictionary of already added calls
     * @param isNullable Whether input field i is nullable
     *
     * @return Rex expression for the given aggregate call
     */
    fun addAggCall(
        aggCall: AggregateCall, groupCount: Int,
        aggCalls: List<AggregateCall?>,
        aggCallMapping: Map<AggregateCall?, RexNode?>,
        isNullable: IntPredicate
    ): RexNode? {
        var aggCall: AggregateCall = aggCall
        if (aggCall.getAggregation() is SqlCountAggFunction
            && !aggCall.isDistinct()
        ) {
            val args: List<Integer> = aggCall.getArgList()
            val nullableArgs: List<Integer> = nullableArgs(args, isNullable)
            aggCall = aggCall.withArgList(nullableArgs)
        }
        var rex: RexNode? = aggCallMapping[aggCall]
        if (rex == null) {
            val index: Int = aggCalls.size() + groupCount
            aggCalls.add(aggCall)
            rex = makeInputRef(aggCall.getType(), index)
            aggCallMapping.put(aggCall, rex)
        }
        return rex
    }

    @Deprecated // to be removed before 2.0
    fun addAggCall(
        aggCall: AggregateCall, groupCount: Int,
        aggCalls: List<AggregateCall?>,
        aggCallMapping: Map<AggregateCall?, RexNode?>,
        @Nullable aggArgTypes: List<RelDataType?>?
    ): RexNode? {
        return addAggCall(aggCall, groupCount, aggCalls, aggCallMapping, { i ->
            Objects.requireNonNull(aggArgTypes, "aggArgTypes")
                .get(aggCall.getArgList().indexOf(i)).isNullable()
        })
    }

    /**
     * Creates a reference to an aggregate call, checking for repeated calls.
     */
    @Deprecated // to be removed before 2.0
    fun addAggCall(
        aggCall: AggregateCall?, groupCount: Int,
        indicator: Boolean, aggCalls: List<AggregateCall?>?,
        aggCallMapping: Map<AggregateCall?, RexNode?>?,
        @Nullable aggArgTypes: List<RelDataType?>?
    ): RexNode {
        Preconditions.checkArgument(
            !indicator,
            "indicator is deprecated, use GROUPING function instead"
        )
        return addAggCall(
            aggCall, groupCount, aggCalls,
            aggCallMapping, aggArgTypes
        )
    }

    @Deprecated // to be removed before 2.0
    fun makeOver(
        type: RelDataType, operator: SqlAggFunction?,
        exprs: List<RexNode?>?, partitionKeys: List<RexNode?>?,
        orderKeys: ImmutableList<RexFieldCollation?>?,
        lowerBound: RexWindowBound, upperBound: RexWindowBound,
        rows: Boolean, allowPartial: Boolean, nullWhenCountZero: Boolean,
        distinct: Boolean
    ): RexNode {
        return makeOver(
            type, operator, exprs, partitionKeys, orderKeys, lowerBound,
            upperBound, rows, allowPartial, nullWhenCountZero, distinct, false
        )
    }

    /**
     * Creates a call to a windowed agg.
     */
    fun makeOver(
        type: RelDataType,
        operator: SqlAggFunction?,
        exprs: List<RexNode?>?,
        partitionKeys: List<RexNode?>?,
        orderKeys: ImmutableList<RexFieldCollation?>?,
        lowerBound: RexWindowBound,
        upperBound: RexWindowBound,
        rows: Boolean,
        allowPartial: Boolean,
        nullWhenCountZero: Boolean,
        distinct: Boolean,
        ignoreNulls: Boolean
    ): RexNode {
        val window: RexWindow = makeWindow(
            partitionKeys,
            orderKeys,
            lowerBound,
            upperBound,
            rows
        )
        val over = RexOver(
            type, operator, exprs, window,
            distinct, ignoreNulls
        )
        var result: RexNode = over

        // This should be correct but need time to go over test results.
        // Also want to look at combing with section below.
        if (nullWhenCountZero) {
            val bigintType: RelDataType = typeFactory.createSqlType(SqlTypeName.BIGINT)
            result = makeCall(
                SqlStdOperatorTable.CASE,
                makeCall(
                    SqlStdOperatorTable.GREATER_THAN,
                    RexOver(
                        bigintType,
                        SqlStdOperatorTable.COUNT,
                        exprs,
                        window,
                        distinct,
                        ignoreNulls
                    ),
                    makeLiteral(
                        BigDecimal.ZERO,
                        bigintType,
                        SqlTypeName.DECIMAL
                    )
                ),
                ensureType(
                    type,  // SUM0 is non-nullable, thus need a cast
                    RexOver(
                        typeFactory.createTypeWithNullability(type, false),
                        operator, exprs, window, distinct, ignoreNulls
                    ),
                    false
                ),
                makeNullLiteral(type)
            )
        }
        if (!allowPartial) {
            Preconditions.checkArgument(rows, "DISALLOW PARTIAL over RANGE")
            val bigintType: RelDataType = typeFactory.createSqlType(SqlTypeName.BIGINT)
            // todo: read bound
            result = makeCall(
                SqlStdOperatorTable.CASE,
                makeCall(
                    SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    RexOver(
                        bigintType,
                        SqlStdOperatorTable.COUNT,
                        ImmutableList.of(),
                        window,
                        distinct,
                        ignoreNulls
                    ),
                    makeLiteral(
                        BigDecimal.valueOf(2),
                        bigintType,
                        SqlTypeName.DECIMAL
                    )
                ),
                result,
                constantNull
            )
        }
        return result
    }

    /**
     * Creates a window specification.
     *
     * @param partitionKeys Partition keys
     * @param orderKeys     Order keys
     * @param lowerBound    Lower bound
     * @param upperBound    Upper bound
     * @param rows          Whether physical. True if row-based, false if
     * range-based
     * @return window specification
     */
    fun makeWindow(
        partitionKeys: List<RexNode?>?,
        orderKeys: ImmutableList<RexFieldCollation?>?,
        lowerBound: RexWindowBound,
        upperBound: RexWindowBound,
        rows: Boolean
    ): RexWindow {
        var rows = rows
        if (lowerBound.isUnbounded() && lowerBound.isPreceding()
            && upperBound.isUnbounded() && upperBound.isFollowing()
        ) {
            // RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            //   is equivalent to
            // ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            //   but we prefer "RANGE"
            rows = false
        }
        return RexWindow(
            partitionKeys,
            orderKeys,
            lowerBound,
            upperBound,
            rows
        )
    }

    /**
     * Creates a constant for the SQL `NULL` value.
     *
     */
    @Deprecated // to be removed before 2.0
    @Deprecated(
        """Use {@link #makeNullLiteral(RelDataType)}, which produces a
    NULL of the correct type"""
    )
    fun constantNull(): RexLiteral {
        return constantNull
    }

    /**
     * Creates an expression referencing a correlation variable.
     *
     * @param id Name of variable
     * @param type Type of variable
     * @return Correlation variable
     */
    fun makeCorrel(
        type: RelDataType?,
        id: CorrelationId
    ): RexNode {
        return RexCorrelVariable(id, type)
    }

    /**
     * Creates an invocation of the NEW operator.
     *
     * @param type  Type to be instantiated
     * @param exprs Arguments to NEW operator
     * @return Expression invoking NEW operator
     */
    fun makeNewInvocation(
        type: RelDataType?,
        exprs: List<RexNode?>
    ): RexNode {
        return RexCall(
            type,
            SqlStdOperatorTable.NEW,
            exprs
        )
    }

    /**
     * Creates a call to the CAST operator.
     *
     * @param type Type to cast to
     * @param exp  Expression being cast
     * @return Call to CAST operator
     */
    fun makeCast(
        type: RelDataType,
        exp: RexNode
    ): RexNode {
        return makeCast(type, exp, false)
    }

    /**
     * Creates a call to the CAST operator, expanding if possible, and optionally
     * also preserving nullability.
     *
     *
     * Tries to expand the cast, and therefore the result may be something
     * other than a [RexCall] to the CAST operator, such as a
     * [RexLiteral].
     *
     * @param type Type to cast to
     * @param exp  Expression being cast
     * @param matchNullability Whether to ensure the result has the same
     * nullability as `type`
     * @return Call to CAST operator
     */
    fun makeCast(
        type: RelDataType,
        exp: RexNode,
        matchNullability: Boolean
    ): RexNode {
        val sqlType: SqlTypeName = type.getSqlTypeName()
        if (exp is RexLiteral) {
            val literal: RexLiteral = exp as RexLiteral
            var value: Comparable = literal.getValueAs(Comparable::class.java)
            var typeName: SqlTypeName = literal.getTypeName()
            if (canRemoveCastFromLiteral(type, value, typeName)) {
                when (typeName) {
                    INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> {
                        assert(value is BigDecimal)
                        typeName = type.getSqlTypeName()
                        when (typeName) {
                            BIGINT, INTEGER, SMALLINT, TINYINT, FLOAT, REAL, DECIMAL -> {
                                val value2: BigDecimal = value as BigDecimal
                                val multiplier: BigDecimal = baseUnit(literal.getTypeName()).multiplier
                                val divider: BigDecimal = literal.getTypeName().getEndUnit().multiplier
                                value = value2.multiply(multiplier)
                                    .divide(divider, 0, RoundingMode.HALF_DOWN)
                            }
                            else -> {}
                        }
                        when (typeName) {
                            INTEGER -> typeName = SqlTypeName.BIGINT
                            else -> {}
                        }
                    }
                    else -> {}
                }
                val literal2: RexLiteral = makeLiteral(value, type, typeName)
                return if (type.isNullable()
                    && !literal2.getType().isNullable()
                    && matchNullability
                ) {
                    makeAbstractCast(type, literal2)
                } else literal2
            }
        } else if (SqlTypeUtil.isExactNumeric(type)
            && SqlTypeUtil.isInterval(exp.getType())
        ) {
            return makeCastIntervalToExact(type, exp)
        } else if (sqlType === SqlTypeName.BOOLEAN
            && SqlTypeUtil.isExactNumeric(exp.getType())
        ) {
            return makeCastExactToBoolean(type, exp)
        } else if (exp.getType().getSqlTypeName() === SqlTypeName.BOOLEAN
            && SqlTypeUtil.isExactNumeric(type)
        ) {
            return makeCastBooleanToExact(type, exp)
        }
        return makeAbstractCast(type, exp)
    }

    fun canRemoveCastFromLiteral(
        toType: RelDataType, @Nullable value: Comparable,
        fromTypeName: SqlTypeName
    ): Boolean {
        val sqlType: SqlTypeName = toType.getSqlTypeName()
        if (!RexLiteral.valueMatchesType(value, sqlType, false)) {
            return false
        }
        if (toType.getSqlTypeName() !== fromTypeName
            && SqlTypeFamily.DATETIME.getTypeNames().contains(fromTypeName)
        ) {
            return false
        }
        if (value is NlsString) {
            val length: Int = (value as NlsString).getValue().length()
            return when (toType.getSqlTypeName()) {
                CHAR -> SqlTypeUtil.comparePrecision(toType.getPrecision(), length) === 0
                VARCHAR -> SqlTypeUtil.comparePrecision(toType.getPrecision(), length) >= 0
                else -> throw AssertionError(toType)
            }
        }
        if (value is ByteString) {
            val length: Int = (value as ByteString).length()
            return when (toType.getSqlTypeName()) {
                BINARY -> SqlTypeUtil.comparePrecision(toType.getPrecision(), length) === 0
                VARBINARY -> SqlTypeUtil.comparePrecision(toType.getPrecision(), length) >= 0
                else -> throw AssertionError(toType)
            }
        }
        if (toType.getSqlTypeName() === SqlTypeName.DECIMAL) {
            val decimalValue: BigDecimal = value as BigDecimal
            return SqlTypeUtil.isValidDecimalValue(decimalValue, toType)
        }
        return true
    }

    private fun makeCastExactToBoolean(toType: RelDataType, exp: RexNode): RexNode {
        return makeCall(
            toType,
            SqlStdOperatorTable.NOT_EQUALS,
            ImmutableList.of(exp, makeZeroLiteral(exp.getType()))
        )
    }

    private fun makeCastBooleanToExact(toType: RelDataType, exp: RexNode): RexNode {
        val casted: RexNode = makeCall(
            SqlStdOperatorTable.CASE,
            exp,
            makeExactLiteral(BigDecimal.ONE, toType),
            makeZeroLiteral(toType)
        )
        return if (!exp.getType().isNullable()) {
            casted
        } else makeCall(
            toType,
            SqlStdOperatorTable.CASE,
            ImmutableList.of(
                makeCall(SqlStdOperatorTable.IS_NOT_NULL, exp),
                casted, makeNullLiteral(toType)
            )
        )
    }

    private fun makeCastIntervalToExact(toType: RelDataType, exp: RexNode): RexNode {
        val endUnit: TimeUnit = exp.getType().getSqlTypeName().getEndUnit()
        val baseUnit: TimeUnit = baseUnit(exp.getType().getSqlTypeName())
        val multiplier: BigDecimal = baseUnit.multiplier
        val divider: BigDecimal = endUnit.multiplier
        val value: RexNode = multiplyDivide(
            decodeIntervalOrDecimal(exp),
            multiplier, divider
        )
        return ensureType(toType, value, false)
    }

    fun multiplyDivide(
        e: RexNode?, multiplier: BigDecimal,
        divider: BigDecimal
    ): RexNode {
        assert(multiplier.signum() > 0)
        assert(divider.signum() > 0)
        return when (multiplier.compareTo(divider)) {
            0 -> e
            1 ->       // E.g. multiplyDivide(e, 1000, 10) ==> e * 100
                makeCall(
                    SqlStdOperatorTable.MULTIPLY, e,
                    makeExactLiteral(
                        multiplier.divide(divider, RoundingMode.UNNECESSARY)
                    )
                )
            -1 ->       // E.g. multiplyDivide(e, 10, 1000) ==> e / 100
                makeCall(
                    SqlStdOperatorTable.DIVIDE_INTEGER, e,
                    makeExactLiteral(
                        divider.divide(multiplier, RoundingMode.UNNECESSARY)
                    )
                )
            else -> throw AssertionError(multiplier.toString() + "/" + divider)
        }
    }

    /**
     * Casts a decimal's integer representation to a decimal node. If the
     * expression is not the expected integer type, then it is casted first.
     *
     *
     * An overflow check may be requested to ensure the internal value
     * does not exceed the maximum value of the decimal type.
     *
     * @param value         integer representation of decimal
     * @param type          type integer will be reinterpreted as
     * @param checkOverflow indicates whether an overflow check is required
     * when reinterpreting this particular value as the
     * decimal type. A check usually not required for
     * arithmetic, but is often required for rounding and
     * explicit casts.
     * @return the integer reinterpreted as an opaque decimal type
     */
    fun encodeIntervalOrDecimal(
        value: RexNode,
        type: RelDataType?,
        checkOverflow: Boolean
    ): RexNode {
        val bigintType: RelDataType = typeFactory.createSqlType(SqlTypeName.BIGINT)
        val cast: RexNode = ensureType(bigintType, value, true)
        return makeReinterpretCast(type, cast, makeLiteral(checkOverflow))
    }

    /**
     * Retrieves an INTERVAL or DECIMAL node's integer representation.
     *
     * @param node the interval or decimal value as an opaque type
     * @return an integer representation of the decimal value
     */
    fun decodeIntervalOrDecimal(node: RexNode): RexNode {
        assert(
            SqlTypeUtil.isDecimal(node.getType())
                    || SqlTypeUtil.isInterval(node.getType())
        )
        val bigintType: RelDataType = typeFactory.createSqlType(SqlTypeName.BIGINT)
        return makeReinterpretCast(
            matchNullability(bigintType, node), node, makeLiteral(false)
        )
    }

    /**
     * Creates a call to the CAST operator.
     *
     * @param type Type to cast to
     * @param exp  Expression being cast
     * @return Call to CAST operator
     */
    fun makeAbstractCast(
        type: RelDataType?,
        exp: RexNode?
    ): RexNode {
        return RexCall(
            type,
            SqlStdOperatorTable.CAST,
            ImmutableList.of(exp)
        )
    }

    /**
     * Makes a reinterpret cast.
     *
     * @param type          type returned by the cast
     * @param exp           expression to be casted
     * @param checkOverflow whether an overflow check is required
     * @return a RexCall with two operands and a special return type
     */
    fun makeReinterpretCast(
        type: RelDataType?,
        exp: RexNode?,
        checkOverflow: RexNode?
    ): RexNode {
        val args: List<RexNode>
        args = if (checkOverflow != null && checkOverflow.isAlwaysTrue()) {
            ImmutableList.of(exp, checkOverflow)
        } else {
            ImmutableList.of(exp)
        }
        return RexCall(
            type,
            SqlStdOperatorTable.REINTERPRET,
            args
        )
    }

    /**
     * Makes a cast of a value to NOT NULL;
     * no-op if the type already has NOT NULL.
     */
    fun makeNotNull(exp: RexNode): RexNode {
        val type: RelDataType = exp.getType()
        if (!type.isNullable()) {
            return exp
        }
        val notNullType: RelDataType = typeFactory.createTypeWithNullability(type, false)
        return makeAbstractCast(notNullType, exp)
    }

    /**
     * Creates a reference to all the fields in the row. That is, the whole row
     * as a single record object.
     *
     * @param input Input relational expression
     */
    fun makeRangeReference(input: RelNode): RexNode {
        return RexRangeRef(input.getRowType(), 0)
    }

    /**
     * Creates a reference to all the fields in the row.
     *
     *
     * For example, if the input row has type `T{f0,f1,f2,f3,f4}`
     * then `makeRangeReference(T{f0,f1,f2,f3,f4}, S{f3,f4}, 3)` is
     * an expression which yields the last 2 fields.
     *
     * @param type     Type of the resulting range record.
     * @param offset   Index of first field.
     * @param nullable Whether the record is nullable.
     */
    fun makeRangeReference(
        type: RelDataType,
        offset: Int,
        nullable: Boolean
    ): RexRangeRef {
        var type: RelDataType = type
        if (nullable && !type.isNullable()) {
            type = typeFactory.createTypeWithNullability(
                type,
                nullable
            )
        }
        return RexRangeRef(type, offset)
    }

    /**
     * Creates a reference to a given field of the input record.
     *
     * @param type Type of field
     * @param i    Ordinal of field
     * @return Reference to field
     */
    fun makeInputRef(
        type: RelDataType?,
        i: Int
    ): RexInputRef {
        var type: RelDataType? = type
        type = SqlTypeUtil.addCharsetAndCollation(type, typeFactory)
        return RexInputRef(i, type)
    }

    /**
     * Creates a reference to a given field of the input relational expression.
     *
     * @param input Input relational expression
     * @param i    Ordinal of field
     * @return Reference to field
     *
     * @see .identityProjects
     */
    fun makeInputRef(input: RelNode, i: Int): RexInputRef {
        return makeInputRef(input.getRowType().getFieldList().get(i).getType(), i)
    }

    /**
     * Creates a reference to a given field of the pattern.
     *
     * @param alpha the pattern name
     * @param type Type of field
     * @param i    Ordinal of field
     * @return Reference to field of pattern
     */
    fun makePatternFieldRef(alpha: String?, type: RelDataType?, i: Int): RexPatternFieldRef {
        var type: RelDataType? = type
        type = SqlTypeUtil.addCharsetAndCollation(type, typeFactory)
        return RexPatternFieldRef(alpha, i, type)
    }

    /**
     * Create a reference to local variable.
     *
     * @param type Type of variable
     * @param i    Ordinal of variable
     * @return  Reference to local variable
     */
    fun makeLocalRef(type: RelDataType?, i: Int): RexLocalRef {
        var type: RelDataType? = type
        type = SqlTypeUtil.addCharsetAndCollation(type, typeFactory)
        return RexLocalRef(i, type)
    }

    /**
     * Creates a literal representing a flag.
     *
     * @param flag Flag value
     */
    fun makeFlag(flag: Enum?): RexLiteral {
        assert(flag != null)
        return makeLiteral(
            flag,
            typeFactory.createSqlType(SqlTypeName.SYMBOL),
            SqlTypeName.SYMBOL
        )
    }

    /**
     * Internal method to create a call to a literal. Code outside this package
     * should call one of the type-specific methods such as
     * [.makeDateLiteral], [.makeLiteral],
     * [.makeLiteral].
     *
     * @param o        Value of literal, must be appropriate for the type
     * @param type     Type of literal
     * @param typeName SQL type of literal
     * @return Literal
     */
    protected fun makeLiteral(
        @Nullable o: Comparable?,
        type: RelDataType,
        typeName: SqlTypeName
    ): RexLiteral {
        // All literals except NULL have NOT NULL types.
        var o: Comparable? = o
        var type: RelDataType = type
        type = typeFactory.createTypeWithNullability(type, o == null)
        var p: Int
        when (typeName) {
            CHAR -> {
                assert(o is NlsString)
                val nlsString: NlsString? = o as NlsString?
                if (nlsString.getCollation() == null || nlsString.getCharset() == null || !Objects.equals(
                        nlsString.getCharset(),
                        type.getCharset()
                    )
                    || !Objects.equals(nlsString.getCollation(), type.getCollation())
                ) {
                    assert(
                        type.getSqlTypeName() === SqlTypeName.CHAR
                                || type.getSqlTypeName() === SqlTypeName.VARCHAR
                    )
                    val charset: Charset = type.getCharset()
                    assert(charset != null) { "type.getCharset() must not be null" }
                    assert(type.getCollation() != null) { "type.getCollation() must not be null" }
                    o = NlsString(
                        nlsString.getValue(),
                        charset.name(),
                        type.getCollation()
                    )
                }
            }
            TIME, TIME_WITH_LOCAL_TIME_ZONE -> {
                assert(o is TimeString)
                p = type.getPrecision()
                if (p == RelDataType.PRECISION_NOT_SPECIFIED) {
                    p = 0
                }
                o = (o as TimeString?).round(p)
            }
            TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> {
                assert(o is TimestampString)
                p = type.getPrecision()
                if (p == RelDataType.PRECISION_NOT_SPECIFIED) {
                    p = 0
                }
                o = (o as TimestampString?).round(p)
            }
            else -> {}
        }
        if (typeName === SqlTypeName.DECIMAL
            && !SqlTypeUtil.isValidDecimalValue(o as BigDecimal?, type)
        ) {
            throw IllegalArgumentException(
                "Cannot convert $o to $type due to overflow"
            )
        }
        return RexLiteral(o, type, typeName)
    }

    /**
     * Creates a boolean literal.
     */
    fun makeLiteral(b: Boolean): RexLiteral {
        return if (b) booleanTrue else booleanFalse
    }

    /**
     * Creates a numeric literal.
     */
    fun makeExactLiteral(bd: BigDecimal): RexLiteral {
        val relType: RelDataType
        val scale: Int = bd.scale()
        assert(scale >= 0)
        assert(scale <= typeFactory.getTypeSystem().getMaxNumericScale()) { scale }
        relType = if (scale == 0) {
            if (bd.compareTo(INT_MIN) >= 0 && bd.compareTo(INT_MAX) <= 0) {
                typeFactory.createSqlType(SqlTypeName.INTEGER)
            } else {
                typeFactory.createSqlType(SqlTypeName.BIGINT)
            }
        } else {
            val precision: Int = bd.unscaledValue().abs().toString().length()
            if (precision > scale) {
                // bd is greater than or equal to 1
                typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale)
            } else {
                // bd is less than 1
                typeFactory.createSqlType(SqlTypeName.DECIMAL, scale + 1, scale)
            }
        }
        return makeExactLiteral(bd, relType)
    }

    /**
     * Creates a BIGINT literal.
     */
    fun makeBigintLiteral(@Nullable bd: BigDecimal?): RexLiteral {
        val bigintType: RelDataType = typeFactory.createSqlType(
            SqlTypeName.BIGINT
        )
        return makeLiteral(bd, bigintType, SqlTypeName.DECIMAL)
    }

    /**
     * Creates a numeric literal.
     */
    fun makeExactLiteral(@Nullable bd: BigDecimal?, type: RelDataType?): RexLiteral {
        return makeLiteral(bd, type, SqlTypeName.DECIMAL)
    }

    /**
     * Creates a byte array literal.
     */
    fun makeBinaryLiteral(byteString: ByteString?): RexLiteral {
        return makeLiteral(
            byteString,
            typeFactory.createSqlType(SqlTypeName.BINARY, byteString.length()),
            SqlTypeName.BINARY
        )
    }

    /**
     * Creates a double-precision literal.
     */
    fun makeApproxLiteral(bd: BigDecimal): RexLiteral {
        // Validator should catch if underflow is allowed
        // If underflow is allowed, let underflow become zero
        var bd: BigDecimal = bd
        if (bd.doubleValue() === 0) {
            bd = BigDecimal.ZERO
        }
        return makeApproxLiteral(bd, typeFactory.createSqlType(SqlTypeName.DOUBLE))
    }

    /**
     * Creates an approximate numeric literal (double or float).
     *
     * @param bd   literal value
     * @param type approximate numeric type
     * @return new literal
     */
    fun makeApproxLiteral(@Nullable bd: BigDecimal?, type: RelDataType): RexLiteral {
        assert(
            SqlTypeFamily.APPROXIMATE_NUMERIC.getTypeNames().contains(
                type.getSqlTypeName()
            )
        )
        return makeLiteral(bd, type, SqlTypeName.DOUBLE)
    }

    /**
     * Creates a search argument literal.
     */
    fun makeSearchArgumentLiteral(s: Sarg?, type: RelDataType?): RexLiteral {
        return makeLiteral(Objects.requireNonNull(s, "s"), type, SqlTypeName.SARG)
    }

    /**
     * Creates a character string literal.
     */
    fun makeLiteral(s: String?): RexLiteral {
        assert(s != null)
        return makePreciseStringLiteral(s)
    }

    /**
     * Creates a character string literal with type CHAR and default charset and
     * collation.
     *
     * @param s String value
     * @return Character string literal
     */
    protected fun makePreciseStringLiteral(s: String?): RexLiteral {
        assert(s != null)
        return if (s!!.equals("")) {
            charEmpty
        } else makeCharLiteral(NlsString(s, null, null))
    }

    /**
     * Creates a character string literal with type CHAR.
     *
     * @param value       String value in bytes
     * @param charsetName SQL-level charset name
     * @param collation   Sql collation
     * @return String     literal
     */
    protected fun makePreciseStringLiteral(
        value: ByteString?,
        charsetName: String?, collation: SqlCollation?
    ): RexLiteral {
        return makeCharLiteral(NlsString(value, charsetName, collation))
    }

    /**
     * Ensures expression is interpreted as a specified type. The returned
     * expression may be wrapped with a cast.
     *
     * @param type             desired type
     * @param node             expression
     * @param matchNullability whether to correct nullability of specified
     * type to match the expression; this usually should
     * be true, except for explicit casts which can
     * override default nullability
     * @return a casted expression or the original expression
     */
    fun ensureType(
        type: RelDataType,
        node: RexNode,
        matchNullability: Boolean
    ): RexNode {
        var targetType: RelDataType = type
        if (matchNullability) {
            targetType = matchNullability(type, node)
        }
        if (targetType.getSqlTypeName() === SqlTypeName.ANY
            && (!matchNullability
                    || targetType.isNullable() === node.getType().isNullable())
        ) {
            return node
        }
        return if (!node.getType().equals(targetType)) {
            makeCast(targetType, node)
        } else node
    }

    /**
     * Ensures that a type's nullability matches a value's nullability.
     */
    fun matchNullability(
        type: RelDataType,
        value: RexNode
    ): RelDataType {
        val typeNullability: Boolean = type.isNullable()
        val valueNullability: Boolean = value.getType().isNullable()
        return if (typeNullability != valueNullability) {
            typeFactory.createTypeWithNullability(type, valueNullability)
        } else type
    }

    /**
     * Creates a character string literal from an [NlsString].
     *
     *
     * If the string's charset and collation are not set, uses the system
     * defaults.
     */
    fun makeCharLiteral(str: NlsString?): RexLiteral {
        assert(str != null)
        val type: RelDataType = SqlUtil.createNlsStringType(typeFactory, str)
        return makeLiteral(str, type, SqlTypeName.CHAR)
    }
    // CHECKSTYLE: IGNORE 1

    @Deprecated // to be removed before 2.0
    @Deprecated("Use {@link #makeDateLiteral(DateString)}. ")
    fun makeDateLiteral(calendar: Calendar?): RexLiteral {
        return makeDateLiteral(DateString.fromCalendarFields(calendar))
    }

    /**
     * Creates a Date literal.
     */
    fun makeDateLiteral(date: DateString?): RexLiteral {
        return makeLiteral(
            Objects.requireNonNull(date, "date"),
            typeFactory.createSqlType(SqlTypeName.DATE), SqlTypeName.DATE
        )
    }
    // CHECKSTYLE: IGNORE 1

    @Deprecated // to be removed before 2.0
    @Deprecated("Use {@link #makeTimeLiteral(TimeString, int)}. ")
    fun makeTimeLiteral(calendar: Calendar?, precision: Int): RexLiteral {
        return makeTimeLiteral(TimeString.fromCalendarFields(calendar), precision)
    }

    /**
     * Creates a Time literal.
     */
    fun makeTimeLiteral(time: TimeString?, precision: Int): RexLiteral {
        return makeLiteral(
            Objects.requireNonNull(time, "time"),
            typeFactory.createSqlType(SqlTypeName.TIME, precision),
            SqlTypeName.TIME
        )
    }

    /**
     * Creates a Time with local time-zone literal.
     */
    fun makeTimeWithLocalTimeZoneLiteral(
        time: TimeString?,
        precision: Int
    ): RexLiteral {
        return makeLiteral(
            Objects.requireNonNull(time, "time"),
            typeFactory.createSqlType(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE, precision),
            SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE
        )
    }
    // CHECKSTYLE: IGNORE 1

    @Deprecated // to be removed before 2.0
    @Deprecated("Use {@link #makeTimestampLiteral(TimestampString, int)}. ")
    fun makeTimestampLiteral(calendar: Calendar?, precision: Int): RexLiteral {
        return makeTimestampLiteral(
            TimestampString.fromCalendarFields(calendar),
            precision
        )
    }

    /**
     * Creates a Timestamp literal.
     */
    fun makeTimestampLiteral(
        timestamp: TimestampString?,
        precision: Int
    ): RexLiteral {
        return makeLiteral(
            Objects.requireNonNull(timestamp, "timestamp"),
            typeFactory.createSqlType(SqlTypeName.TIMESTAMP, precision),
            SqlTypeName.TIMESTAMP
        )
    }

    /**
     * Creates a Timestamp with local time-zone literal.
     */
    fun makeTimestampWithLocalTimeZoneLiteral(
        timestamp: TimestampString?,
        precision: Int
    ): RexLiteral {
        return makeLiteral(
            Objects.requireNonNull(timestamp, "timestamp"),
            typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, precision),
            SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
        )
    }

    /**
     * Creates a literal representing an interval type, for example
     * `YEAR TO MONTH` or `DOW`.
     */
    fun makeIntervalLiteral(
        intervalQualifier: SqlIntervalQualifier?
    ): RexLiteral {
        assert(intervalQualifier != null)
        return makeFlag(intervalQualifier.timeUnitRange)
    }

    /**
     * Creates a literal representing an interval value, for example
     * `INTERVAL '3-7' YEAR TO MONTH`.
     */
    fun makeIntervalLiteral(
        @Nullable v: BigDecimal?,
        intervalQualifier: SqlIntervalQualifier
    ): RexLiteral {
        return makeLiteral(
            v,
            typeFactory.createSqlIntervalType(intervalQualifier),
            intervalQualifier.typeName()
        )
    }

    /**
     * Creates a reference to a dynamic parameter.
     *
     * @param type  Type of dynamic parameter
     * @param index Index of dynamic parameter
     * @return Expression referencing dynamic parameter
     */
    fun makeDynamicParam(
        type: RelDataType?,
        index: Int
    ): RexDynamicParam {
        return RexDynamicParam(type, index)
    }

    /**
     * Creates a literal whose value is NULL, with a particular type.
     *
     *
     * The typing is necessary because RexNodes are strictly typed. For
     * example, in the Rex world the `NULL` parameter to `
     * SUBSTRING(NULL FROM 2 FOR 4)` must have a valid VARCHAR type so
     * that the result type can be determined.
     *
     * @param type Type to cast NULL to
     * @return NULL literal of given type
     */
    fun makeNullLiteral(type: RelDataType): RexLiteral {
        var type: RelDataType = type
        if (!type.isNullable()) {
            type = typeFactory.createTypeWithNullability(type, true)
        }
        return makeCast(type, constantNull) as RexLiteral
    }
    // CHECKSTYLE: IGNORE 1

    @Deprecated // to be removed before 2.0
    @Deprecated("Use {@link #makeNullLiteral(RelDataType)} ")
    fun makeNullLiteral(typeName: SqlTypeName?, precision: Int): RexNode {
        return makeNullLiteral(typeFactory.createSqlType(typeName, precision))
    }
    // CHECKSTYLE: IGNORE 1

    @Deprecated // to be removed before 2.0
    @Deprecated("Use {@link #makeNullLiteral(RelDataType)} ")
    fun makeNullLiteral(typeName: SqlTypeName?): RexNode {
        return makeNullLiteral(typeFactory.createSqlType(typeName))
    }

    /** Creates a [RexNode] representation a SQL "arg IN (point, ...)"
     * expression.
     *
     *
     * If all of the expressions are literals, creates a call [Sarg]
     * literal, "SEARCH(arg, SARG([point0..point0], [point1..point1], ...)";
     * otherwise creates a disjunction, "arg = point0 OR arg = point1 OR ...".  */
    fun makeIn(arg: RexNode, ranges: List<RexNode?>): RexNode {
        if (areAssignable(arg, ranges)) {
            val sarg: Sarg? = toSarg(Comparable::class.java, ranges, RexUnknownAs.UNKNOWN)
            if (sarg != null) {
                val range0: RexNode? = ranges[0]
                return makeCall(
                    SqlStdOperatorTable.SEARCH,
                    arg,
                    makeSearchArgumentLiteral(sarg, range0.getType())
                )
            }
        }
        return RexUtil.composeDisjunction(this, ranges.stream()
            .map { r -> makeCall(SqlStdOperatorTable.EQUALS, arg, r) }
            .collect(Util.toImmutableList()))
    }

    /** Creates a [RexNode] representation a SQL
     * "arg BETWEEN lower AND upper" expression.
     *
     *
     * If the expressions are all literals of compatible type, creates a call
     * to [Sarg] literal, `SEARCH(arg, SARG([lower..upper])`;
     * otherwise creates a disjunction, `arg >= lower AND arg <= upper`.  */
    @SuppressWarnings("BetaApi")
    fun makeBetween(arg: RexNode, lower: RexNode, upper: RexNode): RexNode {
        val lowerValue: Comparable = toComparable(Comparable::class.java, lower)
        val upperValue: Comparable = toComparable(Comparable::class.java, upper)
        if (lowerValue != null && upperValue != null && areAssignable(arg, Arrays.asList(lower, upper))) {
            val sarg: Sarg = Sarg.of(
                RexUnknownAs.UNKNOWN,
                ImmutableRangeSet.< Comparable > of < Comparable ? > Range.closed(lowerValue, upperValue)
            )
            return makeCall(
                SqlStdOperatorTable.SEARCH, arg,
                makeSearchArgumentLiteral(sarg, lower.getType())
            )
        }
        return makeCall(
            SqlStdOperatorTable.AND,
            makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, arg, lower),
            makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, arg, upper)
        )
    }

    /**
     * Creates a copy of an expression, which may have been created using a
     * different RexBuilder and/or [RelDataTypeFactory], using this
     * RexBuilder.
     *
     * @param expr Expression
     * @return Copy of expression
     * @see RelDataTypeFactory.copyType
     */
    fun copy(expr: RexNode): RexNode {
        return expr.accept(RexCopier(this))
    }

    /**
     * Creates a literal of the default value for the given type.
     *
     *
     * This value is:
     *
     *
     *  * 0 for numeric types;
     *  * FALSE for BOOLEAN;
     *  * The epoch for TIMESTAMP and DATE;
     *  * Midnight for TIME;
     *  * The empty string for string types (CHAR, BINARY, VARCHAR, VARBINARY).
     *
     *
     * @param type      Type
     * @return Simple literal
     */
    fun makeZeroLiteral(type: RelDataType): RexLiteral {
        return makeLiteral(zeroValue(type), type)
    }

    /**
     * Creates a literal of a given type, padding values of constant-width
     * types to match their type, not allowing casts.
     *
     * @param value     Value
     * @param type      Type
     * @return Simple literal
     */
    fun makeLiteral(@Nullable value: Object?, type: RelDataType): RexLiteral {
        return makeLiteral(value, type, false, false) as RexLiteral
    }

    /**
     * Creates a literal of a given type, padding values of constant-width
     * types to match their type.
     *
     * @param value     Value
     * @param type      Type
     * @param allowCast Whether to allow a cast. If false, value is always a
     * [RexLiteral] but may not be the exact type
     * @return Simple literal, or cast simple literal
     */
    fun makeLiteral(
        @Nullable value: Object?, type: RelDataType,
        allowCast: Boolean
    ): RexNode {
        return makeLiteral(value, type, allowCast, false)
    }

    /**
     * Creates a literal of a given type. The value is assumed to be
     * compatible with the type.
     *
     *
     * The `trim` parameter controls whether to trim values of
     * constant-width types such as `CHAR`. Consider a call to
     * `makeLiteral("foo ", CHAR(5)`, and note that the value is too short
     * for its type. If `trim` is true, the value is converted to "foo"
     * and the type to `CHAR(3)`; if `trim` is false, the value is
     * right-padded with spaces to `"foo  "`, to match the type
     * `CHAR(5)`.
     *
     * @param value     Value
     * @param type      Type
     * @param allowCast Whether to allow a cast. If false, value is always a
     * [RexLiteral] but may not be the exact type
     * @param trim      Whether to trim values and type to the shortest equivalent
     * value; for example whether to convert CHAR(4) 'foo '
     * to CHAR(3) 'foo'
     * @return Simple literal, or cast simple literal
     */
    fun makeLiteral(
        @Nullable value: Object?, type: RelDataType,
        allowCast: Boolean, trim: Boolean
    ): RexNode {
        var value: Object? = value
        var type: RelDataType = type
        if (value == null) {
            return makeCast(type, constantNull)
        }
        if (type.isNullable()) {
            val typeNotNull: RelDataType = typeFactory.createTypeWithNullability(type, false)
            if (allowCast) {
                val literalNotNull: RexNode = makeLiteral(value, typeNotNull, allowCast)
                return makeAbstractCast(type, literalNotNull)
            }
            type = typeNotNull
        }
        value = clean(value, type)
        val literal: RexLiteral
        val operands: List<RexNode>
        val sqlTypeName: SqlTypeName = type.getSqlTypeName()
        return when (sqlTypeName) {
            CHAR -> {
                val nlsString: NlsString? = value as NlsString?
                if (trim) {
                    makeCharLiteral(nlsString.rtrim())
                } else {
                    makeCharLiteral(padRight(nlsString, type.getPrecision()))
                }
            }
            VARCHAR -> {
                literal = makeCharLiteral(value as NlsString?)
                if (allowCast) {
                    makeCast(type, literal)
                } else {
                    literal
                }
            }
            BINARY -> makeBinaryLiteral(
                padRight(value as ByteString?, type.getPrecision())
            )
            VARBINARY -> {
                literal = makeBinaryLiteral(value as ByteString?)
                if (allowCast) {
                    makeCast(type, literal)
                } else {
                    literal
                }
            }
            TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL -> makeExactLiteral(value as BigDecimal?, type)
            FLOAT, REAL, DOUBLE -> makeApproxLiteral(value as BigDecimal?, type)
            BOOLEAN -> if ((value as Boolean?)!!) booleanTrue else booleanFalse
            TIME -> makeTimeLiteral(value as TimeString?, type.getPrecision())
            TIME_WITH_LOCAL_TIME_ZONE -> makeTimeWithLocalTimeZoneLiteral(value as TimeString?, type.getPrecision())
            DATE -> makeDateLiteral(value as DateString?)
            TIMESTAMP -> makeTimestampLiteral(value as TimestampString?, type.getPrecision())
            TIMESTAMP_WITH_LOCAL_TIME_ZONE -> makeTimestampWithLocalTimeZoneLiteral(
                value as TimestampString?,
                type.getPrecision()
            )
            INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> makeIntervalLiteral(
                value as BigDecimal?,
                castNonNull(type.getIntervalQualifier())
            )
            SYMBOL -> makeFlag(value as Enum?)
            MAP -> {
                val mapType: MapSqlType = type as MapSqlType
                operands = ArrayList()
                for (entry in value.entrySet()) {
                    operands.add(
                        makeLiteral(entry.getKey(), mapType.getKeyType(), allowCast)
                    )
                    operands.add(
                        makeLiteral(entry.getValue(), mapType.getValueType(), allowCast)
                    )
                }
                makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, operands)
            }
            ARRAY -> {
                val arrayType: ArraySqlType = type as ArraySqlType
                operands = ArrayList()
                for (entry in value) {
                    operands.add(
                        makeLiteral(entry, arrayType.getComponentType(), allowCast)
                    )
                }
                makeCall(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, operands)
            }
            MULTISET -> {
                val multisetType: MultisetSqlType = type as MultisetSqlType
                operands = ArrayList()
                for (entry in (value as List?)!!) {
                    val e: RexNode = if (entry is RexLiteral) entry as RexNode? else makeLiteral(
                        entry,
                        multisetType.getComponentType(),
                        allowCast
                    )
                    operands.add(e)
                }
                if (allowCast) {
                    makeCall(SqlStdOperatorTable.MULTISET_VALUE, operands)
                } else {
                    RexLiteral(
                        FlatLists.of(operands) as Comparable, type,
                        sqlTypeName
                    )
                }
            }
            ROW -> {
                operands = ArrayList()
                for (pair in Pair.zip(type.getFieldList(), value as List<Object?>?)) {
                    val e: RexNode = if (pair.right is RexLiteral) pair.right as RexNode else makeLiteral(
                        pair.right,
                        pair.left.getType(),
                        allowCast
                    )
                    operands.add(e)
                }
                RexLiteral(
                    FlatLists.of(operands) as Comparable, type,
                    sqlTypeName
                )
            }
            GEOMETRY -> RexLiteral(
                value as Comparable?, guessType(value),
                SqlTypeName.GEOMETRY
            )
            ANY -> makeLiteral(value, guessType(value), allowCast)
            else -> throw IllegalArgumentException(
                "Cannot create literal for type '$sqlTypeName'"
            )
        }
    }

    private fun guessType(@Nullable value: Object?): RelDataType {
        if (value == null) {
            return typeFactory.createSqlType(SqlTypeName.NULL)
        }
        if (value is Float || value is Double) {
            return typeFactory.createSqlType(SqlTypeName.DOUBLE)
        }
        if (value is Number) {
            return typeFactory.createSqlType(SqlTypeName.BIGINT)
        }
        if (value is Boolean) {
            return typeFactory.createSqlType(SqlTypeName.BOOLEAN)
        }
        if (value is String) {
            return typeFactory.createSqlType(
                SqlTypeName.CHAR,
                (value as String).length()
            )
        }
        if (value is ByteString) {
            return typeFactory.createSqlType(
                SqlTypeName.BINARY,
                (value as ByteString).length()
            )
        }
        if (value is Geometries.Geom) {
            return typeFactory.createSqlType(SqlTypeName.GEOMETRY)
        }
        throw AssertionError("unknown type " + value.getClass())
    }

    companion object {
        /**
         * Special operator that accesses an unadvertised field of an input record.
         * This operator cannot be used in SQL queries; it is introduced temporarily
         * during sql-to-rel translation, then replaced during the process that
         * trims unwanted fields.
         */
        val GET_OPERATOR: SqlSpecialOperator = SqlSpecialOperator("_get", SqlKind.OTHER_FUNCTION)

        /** The smallest valid `int` value, as a [BigDecimal].  */
        private val INT_MIN: BigDecimal = BigDecimal.valueOf(Integer.MIN_VALUE)

        /** The largest valid `int` value, as a [BigDecimal].  */
        private val INT_MAX: BigDecimal = BigDecimal.valueOf(Integer.MAX_VALUE)
        private fun nullableArgs(
            list0: List<Integer>,
            isNullable: IntPredicate
        ): List<Integer> {
            return list0.stream()
                .filter(isNullable::test)
                .collect(Util.toImmutableList())
        }

        /** Returns the lowest granularity unit for the given unit.
         * YEAR and MONTH intervals are stored as months;
         * HOUR, MINUTE, SECOND intervals are stored as milliseconds.  */
        protected fun baseUnit(unit: SqlTypeName): TimeUnit {
            return if (unit.isYearMonth()) {
                TimeUnit.MONTH
            } else {
                TimeUnit.MILLISECOND
            }
        }

        /** Returns whether and argument and bounds are have types that are
         * sufficiently compatible to be converted to a [Sarg].  */
        private fun areAssignable(arg: RexNode, bounds: List<RexNode?>): Boolean {
            for (bound in bounds) {
                if (!SqlTypeUtil.inSameFamily(arg.getType(), bound.getType())
                    && !(arg.getType().isStruct() && bound.getType().isStruct())
                ) {
                    return false
                }
            }
            return true
        }

        /** Converts a list of expressions to a search argument, or returns null if
         * not possible.  */
        @SuppressWarnings(["BetaApi", "UnstableApiUsage"])
        private fun <C : Comparable<C>?> toSarg(
            clazz: Class<C>,
            ranges: List<RexNode?>, unknownAs: RexUnknownAs
        ): @Nullable Sarg<C>? {
            if (ranges.isEmpty()) {
                // Cannot convert an empty list to a Sarg (by this interface, at least)
                // because we use the type of the first element.
                return null
            }
            val rangeSet: RangeSet<C> = TreeRangeSet.create()
            for (range in ranges) {
                val value: @Any C = toComparable(clazz, range) ?: return null
                rangeSet.add(Range.singleton(value))
            }
            return Sarg.of(unknownAs, rangeSet)
        }

        private fun <C : Comparable<C>?> toComparable(
            clazz: Class<C>,
            point: RexNode
        ): @Nullable C? {
            return when (point.getKind()) {
                LITERAL -> {
                    val literal: RexLiteral = point as RexLiteral
                    literal.getValueAs(clazz)
                }
                ROW -> {
                    val b: ImmutableList.Builder<Comparable> = ImmutableList.builder()
                    for (operand in point.operands) {
                        val value: @Any C = toComparable<C>(
                            Comparable::class.java, operand
                        )
                            ?: return null // not a constant value
                        b.add(value)
                    }
                    clazz.cast(FlatLists.ofComparable(b.build()))
                }
                else -> null // not a constant value
            }
        }

        private fun zeroValue(type: RelDataType): Comparable {
            return when (type.getSqlTypeName()) {
                CHAR -> NlsString(Spaces.of(type.getPrecision()), null, null)
                VARCHAR -> NlsString("", null, null)
                BINARY -> ByteString(ByteArray(type.getPrecision()))
                VARBINARY -> ByteString.EMPTY
                TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, FLOAT, REAL, DOUBLE -> BigDecimal.ZERO
                BOOLEAN -> false
                TIME, DATE, TIMESTAMP -> DateTimeUtils.ZERO_CALENDAR
                TIME_WITH_LOCAL_TIME_ZONE -> TimeString(0, 0, 0)
                TIMESTAMP_WITH_LOCAL_TIME_ZONE -> TimestampString(0, 0, 0, 0, 0, 0)
                else -> throw Util.unexpected(type.getSqlTypeName())
            }
        }

        /** Converts the type of a value to comply with
         * [org.apache.calcite.rex.RexLiteral.valueMatchesType].
         *
         *
         * Returns null if and only if `o` is null.  */
        @PolyNull
        private fun clean(@PolyNull o: Object?, type: RelDataType): Object? {
            return if (o == null) {
                o
            } else when (type.getSqlTypeName()) {
                TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> {
                    if (o is BigDecimal) {
                        return o
                    }
                    assert(!(o is Float || o is Double)) {
                        String.format(
                            Locale.ROOT,
                            "%s is not compatible with %s, try to use makeExactLiteral",
                            o.getClass().getCanonicalName(),
                            type.getSqlTypeName()
                        )
                    }
                    BigDecimal((o as Number).longValue())
                }
                FLOAT -> {
                    if (o is BigDecimal) {
                        o
                    } else BigDecimal((o as Number).doubleValue(), MathContext.DECIMAL32)
                        .stripTrailingZeros()
                }
                REAL, DOUBLE -> {
                    if (o is BigDecimal) {
                        o
                    } else BigDecimal((o as Number).doubleValue(), MathContext.DECIMAL64)
                        .stripTrailingZeros()
                }
                CHAR, VARCHAR -> {
                    if (o is NlsString) {
                        return o
                    }
                    assert(type.getCharset() != null) { type.toString() + ".getCharset() must not be null" }
                    NlsString(
                        o as String?, type.getCharset().name(),
                        type.getCollation()
                    )
                }
                TIME -> if (o is TimeString) {
                    o
                } else if (o is Calendar) {
                    if (!(o as Calendar).getTimeZone().equals(DateTimeUtils.UTC_ZONE)) {
                        throw AssertionError()
                    }
                    TimeString.fromCalendarFields(o as Calendar?)
                } else {
                    TimeString.fromMillisOfDay(o as Integer?)
                }
                TIME_WITH_LOCAL_TIME_ZONE -> if (o is TimeString) {
                    o
                } else {
                    TimeString.fromMillisOfDay(o as Integer?)
                }
                DATE -> if (o is DateString) {
                    o
                } else if (o is Calendar) {
                    if (!(o as Calendar).getTimeZone().equals(DateTimeUtils.UTC_ZONE)) {
                        throw AssertionError()
                    }
                    DateString.fromCalendarFields(o as Calendar?)
                } else {
                    DateString.fromDaysSinceEpoch(o as Integer?)
                }
                TIMESTAMP -> if (o is TimestampString) {
                    o
                } else if (o is Calendar) {
                    if (!(o as Calendar).getTimeZone().equals(DateTimeUtils.UTC_ZONE)) {
                        throw AssertionError()
                    }
                    TimestampString.fromCalendarFields(o as Calendar?)
                } else {
                    TimestampString.fromMillisSinceEpoch(o as Long?)
                }
                TIMESTAMP_WITH_LOCAL_TIME_ZONE -> if (o is TimestampString) {
                    o
                } else {
                    TimestampString.fromMillisSinceEpoch(o as Long?)
                }
                else -> o
            }
        }

        /** Returns an [NlsString] with spaces to make it at least a given
         * length.  */
        private fun padRight(s: NlsString, length: Int): NlsString {
            return if (s.getValue().length() >= length) {
                s
            } else s.copy(padRight(s.getValue(), length))
        }

        /** Returns a string padded with spaces to make it at least a given length.  */
        private fun padRight(s: String, length: Int): String {
            return if (s.length() >= length) {
                s
            } else StringBuilder()
                .append(s)
                .append(Spaces.MAX, s.length(), length)
                .toString()
        }

        /** Returns a byte-string padded with zero bytes to make it at least a given
         * length.  */
        private fun padRight(s: ByteString, length: Int): ByteString {
            return if (s.length() >= length) {
                s
            } else ByteString(Arrays.copyOf(s.getBytes(), length))
        }
    }
}
