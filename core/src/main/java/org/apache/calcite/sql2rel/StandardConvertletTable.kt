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
package org.apache.calcite.sql2rel

import org.apache.calcite.avatica.util.DateTimeUtils
import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.rel.type.RelDataTypeFamily
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexCallBinding
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexRangeRef
import org.apache.calcite.rex.RexUtil
import org.apache.calcite.runtime.SqlFunctions
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.SqlBasicCall
import org.apache.calcite.sql.SqlBinaryOperator
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlDataTypeSpec
import org.apache.calcite.sql.SqlFunction
import org.apache.calcite.sql.SqlFunctionCategory
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlIntervalLiteral
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.SqlJdbcFunctionCall
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlNodeList
import org.apache.calcite.sql.SqlNumericLiteral
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.SqlOperatorBinding
import org.apache.calcite.sql.SqlUtil
import org.apache.calcite.sql.SqlWindowTableFunction
import org.apache.calcite.sql.`fun`.SqlArrayValueConstructor
import org.apache.calcite.sql.`fun`.SqlBetweenOperator
import org.apache.calcite.sql.`fun`.SqlCase
import org.apache.calcite.sql.`fun`.SqlDatetimeSubtractionOperator
import org.apache.calcite.sql.`fun`.SqlExtractFunction
import org.apache.calcite.sql.`fun`.SqlJsonValueFunction
import org.apache.calcite.sql.`fun`.SqlLibrary
import org.apache.calcite.sql.`fun`.SqlLibraryOperators
import org.apache.calcite.sql.`fun`.SqlLiteralChainOperator
import org.apache.calcite.sql.`fun`.SqlMapValueConstructor
import org.apache.calcite.sql.`fun`.SqlMultisetQueryConstructor
import org.apache.calcite.sql.`fun`.SqlMultisetValueConstructor
import org.apache.calcite.sql.`fun`.SqlOverlapsOperator
import org.apache.calcite.sql.`fun`.SqlRowOperator
import org.apache.calcite.sql.`fun`.SqlSequenceValueOperator
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.`fun`.SqlSubstringFunction
import org.apache.calcite.sql.`fun`.SqlTrimFunction
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlOperandTypeChecker
import org.apache.calcite.sql.type.SqlTypeFamily
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.sql.type.SqlTypeUtil
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql.validate.SqlValidatorImpl
import org.apache.calcite.util.Pair
import org.apache.calcite.util.Util
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import org.checkerframework.checker.initialization.qual.UnknownInitialization
import java.math.BigDecimal
import java.math.RoundingMode
import java.util.ArrayList
import java.util.List
import java.util.Objects
import org.apache.calcite.sql.type.NonNullableAccessors.getComponentTypeOrThrow
import java.util.Objects.requireNonNull

/**
 * Standard implementation of [SqlRexConvertletTable].
 */
class StandardConvertletTable private constructor() : ReflectiveConvertletTable() {
    //~ Constructors -----------------------------------------------------------
    init {

        // Register aliases (operators which have a different name but
        // identical behavior to other operators).
        addAlias(
            SqlStdOperatorTable.CHARACTER_LENGTH,
            SqlStdOperatorTable.CHAR_LENGTH
        )
        addAlias(
            SqlStdOperatorTable.IS_UNKNOWN,
            SqlStdOperatorTable.IS_NULL
        )
        addAlias(
            SqlStdOperatorTable.IS_NOT_UNKNOWN,
            SqlStdOperatorTable.IS_NOT_NULL
        )
        addAlias(SqlStdOperatorTable.PERCENT_REMAINDER, SqlStdOperatorTable.MOD)

        // Register convertlets for specific objects.
        registerOp(SqlStdOperatorTable.CAST) { cx: SqlRexContext, call: SqlCall -> convertCast(cx, call) }
        registerOp(SqlLibraryOperators.INFIX_CAST) { cx: SqlRexContext, call: SqlCall -> convertCast(cx, call) }
        registerOp(
            SqlStdOperatorTable.IS_DISTINCT_FROM
        ) { cx, call -> convertIsDistinctFrom(cx, call, false) }
        registerOp(
            SqlStdOperatorTable.IS_NOT_DISTINCT_FROM
        ) { cx, call -> convertIsDistinctFrom(cx, call, true) }
        registerOp(SqlStdOperatorTable.PLUS) { cx: SqlRexContext, call: SqlCall -> convertPlus(cx, call) }
        registerOp(
            SqlStdOperatorTable.MINUS
        ) { cx, call ->
            val e: RexCall = convertCall(cx, call) as RexCall
            when (e.getOperands().get(0).getType().getSqlTypeName()) {
                DATE, TIME, TIMESTAMP -> return@registerOp convertDatetimeMinus(
                    cx, SqlStdOperatorTable.MINUS_DATE,
                    call
                )
                else -> return@registerOp e
            }
        }
        registerOp(
            SqlLibraryOperators.LTRIM,
            TrimConvertlet(SqlTrimFunction.Flag.LEADING)
        )
        registerOp(
            SqlLibraryOperators.RTRIM,
            TrimConvertlet(SqlTrimFunction.Flag.TRAILING)
        )
        registerOp(SqlLibraryOperators.GREATEST, GreatestConvertlet())
        registerOp(SqlLibraryOperators.LEAST, GreatestConvertlet())
        registerOp(
            SqlLibraryOperators.SUBSTR_BIG_QUERY,
            SubstrConvertlet(SqlLibrary.BIG_QUERY)
        )
        registerOp(
            SqlLibraryOperators.SUBSTR_MYSQL,
            SubstrConvertlet(SqlLibrary.MYSQL)
        )
        registerOp(
            SqlLibraryOperators.SUBSTR_ORACLE,
            SubstrConvertlet(SqlLibrary.ORACLE)
        )
        registerOp(
            SqlLibraryOperators.SUBSTR_POSTGRESQL,
            SubstrConvertlet(SqlLibrary.POSTGRESQL)
        )
        registerOp(SqlLibraryOperators.NVL) { cx: SqlRexContext, call: SqlCall -> convertNvl(cx, call) }
        registerOp(SqlLibraryOperators.DECODE) { cx: SqlRexContext, call: SqlCall -> convertDecode(cx, call) }
        registerOp(SqlLibraryOperators.IF) { cx: SqlRexContext, call: SqlCall -> convertIf(cx, call) }

        // Expand "x NOT LIKE y" into "NOT (x LIKE y)"
        registerOp(
            SqlStdOperatorTable.NOT_LIKE
        ) { cx, call ->
            cx.convertExpression(
                SqlStdOperatorTable.NOT.createCall(
                    SqlParserPos.ZERO,
                    SqlStdOperatorTable.LIKE.createCall(
                        SqlParserPos.ZERO,
                        call.getOperandList()
                    )
                )
            )
        }

        // Expand "x NOT ILIKE y" into "NOT (x ILIKE y)"
        registerOp(
            SqlLibraryOperators.NOT_ILIKE
        ) { cx, call ->
            cx.convertExpression(
                SqlStdOperatorTable.NOT.createCall(
                    SqlParserPos.ZERO,
                    SqlLibraryOperators.ILIKE.createCall(
                        SqlParserPos.ZERO,
                        call.getOperandList()
                    )
                )
            )
        }

        // Expand "x NOT RLIKE y" into "NOT (x RLIKE y)"
        registerOp(
            SqlLibraryOperators.NOT_RLIKE
        ) { cx, call ->
            cx.convertExpression(
                SqlStdOperatorTable.NOT.createCall(
                    SqlParserPos.ZERO,
                    SqlLibraryOperators.RLIKE.createCall(
                        SqlParserPos.ZERO,
                        call.getOperandList()
                    )
                )
            )
        }

        // Expand "x NOT SIMILAR y" into "NOT (x SIMILAR y)"
        registerOp(
            SqlStdOperatorTable.NOT_SIMILAR_TO
        ) { cx, call ->
            cx.convertExpression(
                SqlStdOperatorTable.NOT.createCall(
                    SqlParserPos.ZERO,
                    SqlStdOperatorTable.SIMILAR_TO.createCall(
                        SqlParserPos.ZERO,
                        call.getOperandList()
                    )
                )
            )
        }

        // Unary "+" has no effect, so expand "+ x" into "x".
        registerOp(
            SqlStdOperatorTable.UNARY_PLUS
        ) { cx, call -> cx.convertExpression(call.operand(0)) }

        // "DOT"
        registerOp(
            SqlStdOperatorTable.DOT
        ) { cx, call ->
            cx.getRexBuilder().makeFieldAccess(
                cx.convertExpression(call.operand(0)),
                call.operand(1).toString(), false
            )
        }
        // "ITEM"
        registerOp(SqlStdOperatorTable.ITEM) { cx: SqlRexContext, call: SqlCall -> convertItem(cx, call) }
        // "AS" has no effect, so expand "x AS id" into "x".
        registerOp(
            SqlStdOperatorTable.AS
        ) { cx, call -> cx.convertExpression(call.operand(0)) }
        // "SQRT(x)" is equivalent to "POWER(x, .5)"
        registerOp(
            SqlStdOperatorTable.SQRT
        ) { cx, call ->
            cx.convertExpression(
                SqlStdOperatorTable.POWER.createCall(
                    SqlParserPos.ZERO,
                    call.operand(0),
                    SqlLiteral.createExactNumeric("0.5", SqlParserPos.ZERO)
                )
            )
        }

        // REVIEW jvs 24-Apr-2006: This only seems to be working from within a
        // windowed agg.  I have added an optimizer rule
        // org.apache.calcite.rel.rules.AggregateReduceFunctionsRule which handles
        // other cases post-translation.  The reason I did that was to defer the
        // implementation decision; e.g. we may want to push it down to a foreign
        // server directly rather than decomposed; decomposition is easier than
        // recognition.

        // Convert "avg(<expr>)" to "cast(sum(<expr>) / count(<expr>) as
        // <type>)". We don't need to handle the empty set specially, because
        // the SUM is already supposed to come out as NULL in cases where the
        // COUNT is zero, so the null check should take place first and prevent
        // division by zero. We need the cast because SUM and COUNT may use
        // different types, say BIGINT.
        //
        // Similarly STDDEV_POP and STDDEV_SAMP, VAR_POP and VAR_SAMP.
        registerOp(
            SqlStdOperatorTable.AVG,
            AvgVarianceConvertlet(SqlKind.AVG)
        )
        registerOp(
            SqlStdOperatorTable.STDDEV_POP,
            AvgVarianceConvertlet(SqlKind.STDDEV_POP)
        )
        registerOp(
            SqlStdOperatorTable.STDDEV_SAMP,
            AvgVarianceConvertlet(SqlKind.STDDEV_SAMP)
        )
        registerOp(
            SqlStdOperatorTable.STDDEV,
            AvgVarianceConvertlet(SqlKind.STDDEV_SAMP)
        )
        registerOp(
            SqlStdOperatorTable.VAR_POP,
            AvgVarianceConvertlet(SqlKind.VAR_POP)
        )
        registerOp(
            SqlStdOperatorTable.VAR_SAMP,
            AvgVarianceConvertlet(SqlKind.VAR_SAMP)
        )
        registerOp(
            SqlStdOperatorTable.VARIANCE,
            AvgVarianceConvertlet(SqlKind.VAR_SAMP)
        )
        registerOp(
            SqlStdOperatorTable.COVAR_POP,
            RegrCovarianceConvertlet(SqlKind.COVAR_POP)
        )
        registerOp(
            SqlStdOperatorTable.COVAR_SAMP,
            RegrCovarianceConvertlet(SqlKind.COVAR_SAMP)
        )
        registerOp(
            SqlStdOperatorTable.REGR_SXX,
            RegrCovarianceConvertlet(SqlKind.REGR_SXX)
        )
        registerOp(
            SqlStdOperatorTable.REGR_SYY,
            RegrCovarianceConvertlet(SqlKind.REGR_SYY)
        )
        val floorCeilConvertlet: SqlRexConvertlet = FloorCeilConvertlet()
        registerOp(SqlStdOperatorTable.FLOOR, floorCeilConvertlet)
        registerOp(SqlStdOperatorTable.CEIL, floorCeilConvertlet)
        registerOp(SqlStdOperatorTable.TIMESTAMP_ADD, TimestampAddConvertlet())
        registerOp(
            SqlStdOperatorTable.TIMESTAMP_DIFF,
            TimestampDiffConvertlet()
        )
        registerOp(SqlStdOperatorTable.INTERVAL) { cx: SqlRexContext, call: SqlCall -> convertInterval(cx, call) }

        // Convert "element(<expr>)" to "$element_slice(<expr>)", if the
        // expression is a multiset of scalars.
        if (false) {
            registerOp(
                SqlStdOperatorTable.ELEMENT
            ) { cx, call ->
                assert(call.operandCount() === 1)
                val operand: SqlNode = call.operand(0)
                val type: RelDataType = cx.getValidator().getValidatedNodeType(operand)
                if (!getComponentTypeOrThrow(type).isStruct()) {
                    return@registerOp cx.convertExpression(
                        SqlStdOperatorTable.ELEMENT_SLICE.createCall(
                            SqlParserPos.ZERO, operand
                        )
                    )
                }
                convertCall(cx, call)
            }
        }

        // Convert "$element_slice(<expr>)" to "element(<expr>).field#0"
        if (false) {
            registerOp(
                SqlStdOperatorTable.ELEMENT_SLICE
            ) { cx, call ->
                assert(call.operandCount() === 1)
                val operand: SqlNode = call.operand(0)
                val expr: RexNode = cx.convertExpression(
                    SqlStdOperatorTable.ELEMENT.createCall(
                        SqlParserPos.ZERO,
                        operand
                    )
                )
                cx.getRexBuilder().makeFieldAccess(expr, 0)
            }
        }
    }

    /**
     * Converts a CASE expression.
     */
    fun convertCase(
        cx: SqlRexContext,
        call: SqlCase
    ): RexNode {
        val whenList: SqlNodeList = call.getWhenOperands()
        val thenList: SqlNodeList = call.getThenOperands()
        assert(whenList.size() === thenList.size())
        val rexBuilder: RexBuilder = cx.getRexBuilder()
        val exprList: List<RexNode> = ArrayList()
        val typeFactory: RelDataTypeFactory = rexBuilder.getTypeFactory()
        val unknownLiteral: RexLiteral = rexBuilder.makeNullLiteral(
            typeFactory.createSqlType(SqlTypeName.BOOLEAN)
        )
        val nullLiteral: RexLiteral = rexBuilder.makeNullLiteral(
            typeFactory.createSqlType(SqlTypeName.NULL)
        )
        for (i in 0 until whenList.size()) {
            if (SqlUtil.isNullLiteral(whenList.get(i), false)) {
                exprList.add(unknownLiteral)
            } else {
                exprList.add(cx.convertExpression(whenList.get(i)))
            }
            if (SqlUtil.isNullLiteral(thenList.get(i), false)) {
                exprList.add(nullLiteral)
            } else {
                exprList.add(cx.convertExpression(thenList.get(i)))
            }
        }
        val elseOperand: SqlNode = call.getElseOperand()
        if (SqlUtil.isNullLiteral(elseOperand, false)) {
            exprList.add(nullLiteral)
        } else {
            exprList.add(cx.convertExpression(requireNonNull(elseOperand, "elseOperand")))
        }
        val type: RelDataType = rexBuilder.deriveReturnType(call.getOperator(), exprList)
        for (i in elseArgs(exprList.size())) {
            exprList.set(
                i,
                rexBuilder.ensureType(type, exprList[i], false)
            )
        }
        return rexBuilder.makeCall(type, SqlStdOperatorTable.CASE, exprList)
    }

    fun convertMultiset(
        cx: SqlRexContext,
        op: SqlMultisetValueConstructor?,
        call: SqlCall?
    ): RexNode {
        val originalType: RelDataType = cx.getValidator().getValidatedNodeType(call)
        val rr: RexRangeRef = cx.getSubQueryExpr(call)
        assert(rr != null)
        val msType: RelDataType = rr.getType().getFieldList().get(0).getType()
        var expr: RexNode = cx.getRexBuilder().makeInputRef(
            msType,
            rr.getOffset()
        )
        assert(
            msType.getComponentType() != null && msType.getComponentType().isStruct()
        ) { "componentType of $msType must be struct" }
        assert(originalType.getComponentType() != null) { "componentType of $originalType must be struct" }
        if (!originalType.getComponentType().isStruct()) {
            // If the type is not a struct, the multiset operator will have
            // wrapped the type as a record. Add a call to the $SLICE operator
            // to compensate. For example,
            // if '<ms>' has type 'RECORD (INTEGER x) MULTISET',
            // then '$SLICE(<ms>) has type 'INTEGER MULTISET'.
            // This will be removed as the expression is translated.
            expr = cx.getRexBuilder().makeCall(
                originalType, SqlStdOperatorTable.SLICE,
                ImmutableList.of(expr)
            )
        }
        return expr
    }

    fun convertArray(
        cx: SqlRexContext,
        op: SqlArrayValueConstructor?,
        call: SqlCall
    ): RexNode {
        return convertCall(cx, call)
    }

    fun convertMap(
        cx: SqlRexContext,
        op: SqlMapValueConstructor?,
        call: SqlCall
    ): RexNode {
        return convertCall(cx, call)
    }

    fun convertMultisetQuery(
        cx: SqlRexContext,
        op: SqlMultisetQueryConstructor?,
        call: SqlCall?
    ): RexNode {
        val originalType: RelDataType = cx.getValidator().getValidatedNodeType(call)
        val rr: RexRangeRef = cx.getSubQueryExpr(call)
        assert(rr != null)
        val msType: RelDataType = rr.getType().getFieldList().get(0).getType()
        var expr: RexNode = cx.getRexBuilder().makeInputRef(
            msType,
            rr.getOffset()
        )
        assert(
            msType.getComponentType() != null && msType.getComponentType().isStruct()
        ) { "componentType of $msType must be struct" }
        assert(originalType.getComponentType() != null) { "componentType of $originalType must be struct" }
        if (!originalType.getComponentType().isStruct()) {
            // If the type is not a struct, the multiset operator will have
            // wrapped the type as a record. Add a call to the $SLICE operator
            // to compensate. For example,
            // if '<ms>' has type 'RECORD (INTEGER x) MULTISET',
            // then '$SLICE(<ms>) has type 'INTEGER MULTISET'.
            // This will be removed as the expression is translated.
            expr = cx.getRexBuilder().makeCall(SqlStdOperatorTable.SLICE, expr)
        }
        return expr
    }

    fun convertJdbc(
        cx: SqlRexContext,
        op: SqlJdbcFunctionCall,
        call: SqlCall?
    ): RexNode {
        // Yuck!! The function definition contains arguments!
        // TODO: adopt a more conventional definition/instance structure
        val convertedCall: SqlCall = op.getLookupCall()
        return cx.convertExpression(convertedCall)
    }

    protected fun convertCast(
        cx: SqlRexContext,
        call: SqlCall
    ): RexNode {
        val typeFactory: RelDataTypeFactory = cx.getTypeFactory()
        assert(call.getKind() === SqlKind.CAST)
        val left: SqlNode = call.operand(0)
        val right: SqlNode = call.operand(1)
        if (right is SqlIntervalQualifier) {
            val intervalQualifier: SqlIntervalQualifier = right as SqlIntervalQualifier
            if (left is SqlIntervalLiteral) {
                val sourceInterval: RexLiteral = cx.convertExpression(left) as RexLiteral
                val sourceValue: BigDecimal = sourceInterval.getValue() as BigDecimal
                val castedInterval: RexLiteral = cx.getRexBuilder().makeIntervalLiteral(
                    sourceValue,
                    intervalQualifier
                )
                return castToValidatedType(cx, call, castedInterval)
            } else if (left is SqlNumericLiteral) {
                val sourceInterval: RexLiteral = cx.convertExpression(left) as RexLiteral
                var sourceValue: BigDecimal = sourceInterval.getValue() as BigDecimal
                val multiplier: BigDecimal = intervalQualifier.getUnit().multiplier
                sourceValue = SqlFunctions.multiply(sourceValue, multiplier)
                val castedInterval: RexLiteral = cx.getRexBuilder().makeIntervalLiteral(
                    sourceValue,
                    intervalQualifier
                )
                return castToValidatedType(cx, call, castedInterval)
            }
            return castToValidatedType(cx, call, cx.convertExpression(left))
        }
        val dataType: SqlDataTypeSpec = right as SqlDataTypeSpec
        var type: RelDataType = dataType.deriveType(cx.getValidator())
        if (type == null) {
            type = cx.getValidator().getValidatedNodeType(dataType.getTypeName())
        }
        val arg: RexNode = cx.convertExpression(left)
        if (arg.getType().isNullable()) {
            type = typeFactory.createTypeWithNullability(type, true)
        }
        if (SqlUtil.isNullLiteral(left, false)) {
            val validator: SqlValidatorImpl = cx.getValidator() as SqlValidatorImpl
            validator.setValidatedNodeType(left, type)
            return cx.convertExpression(left)
        }
        if (null != dataType.getCollectionsTypeName()) {
            val argComponentType: RelDataType = requireNonNull(
                arg.getType().getComponentType()
            ) { "componentType of $arg" }
            val typeFinal: RelDataType = type
            val componentType: RelDataType = requireNonNull(
                type.getComponentType()
            ) { "componentType of $typeFinal" }
            if (argComponentType.isStruct()
                && !componentType.isStruct()
            ) {
                var tt: RelDataType = typeFactory.builder()
                    .add(
                        argComponentType.getFieldList().get(0).getName(),
                        componentType
                    )
                    .build()
                tt = typeFactory.createTypeWithNullability(
                    tt,
                    componentType.isNullable()
                )
                val isn: Boolean = type.isNullable()
                type = typeFactory.createMultisetType(tt, -1)
                type = typeFactory.createTypeWithNullability(type, isn)
            }
        }
        return cx.getRexBuilder().makeCast(type, arg)
    }

    protected fun convertFloorCeil(cx: SqlRexContext, call: SqlCall): RexNode {
        val floor = call.getKind() === SqlKind.FLOOR
        // Rewrite floor, ceil of interval
        if (call.operandCount() === 1
            && call.operand(0) is SqlIntervalLiteral
        ) {
            val literal: SqlIntervalLiteral = call.operand(0)
            val interval: SqlIntervalLiteral.IntervalValue =
                literal.getValueAs(SqlIntervalLiteral.IntervalValue::class.java)
            val `val`: BigDecimal = interval.getIntervalQualifier().getStartUnit().multiplier
            val rexInterval: RexNode = cx.convertExpression(literal)
            val rexBuilder: RexBuilder = cx.getRexBuilder()
            val zero: RexNode = rexBuilder.makeExactLiteral(BigDecimal.valueOf(0))
            val cond: RexNode = ge(rexBuilder, rexInterval, zero)
            val pad: RexNode = rexBuilder.makeExactLiteral(`val`.subtract(BigDecimal.ONE))
            val cast: RexNode = rexBuilder.makeReinterpretCast(
                rexInterval.getType(), pad, rexBuilder.makeLiteral(false)
            )
            val sum: RexNode = if (floor) minus(rexBuilder, rexInterval, cast) else plus(rexBuilder, rexInterval, cast)
            val kase: RexNode =
                if (floor) case_(rexBuilder, rexInterval, cond, sum) else case_(rexBuilder, sum, cond, rexInterval)
            val factor: RexNode = rexBuilder.makeExactLiteral(`val`)
            val div: RexNode = divideInt(rexBuilder, kase, factor)
            return multiply(rexBuilder, div, factor)
        }

        // normal floor, ceil function
        return convertFunction(cx, call.getOperator() as SqlFunction, call)
    }

    /**
     * Converts a call to the `EXTRACT` function.
     *
     *
     * Called automatically via reflection.
     */
    fun convertExtract(
        cx: SqlRexContext,
        op: SqlExtractFunction?,
        call: SqlCall
    ): RexNode {
        return convertFunction(cx, call.getOperator() as SqlFunction, call)
    }

    fun convertDatetimeMinus(
        cx: SqlRexContext,
        op: SqlDatetimeSubtractionOperator?,
        call: SqlCall
    ): RexNode {
        // Rewrite datetime minus
        val rexBuilder: RexBuilder = cx.getRexBuilder()
        val exprs: List<RexNode> = convertOperands(cx, call, SqlOperandTypeChecker.Consistency.NONE)
        val resType: RelDataType = cx.getValidator().getValidatedNodeType(call)
        return rexBuilder.makeCall(resType, op, exprs.subList(0, 2))
    }

    fun convertFunction(
        cx: SqlRexContext,
        `fun`: SqlFunction,
        call: SqlCall
    ): RexNode {
        val exprs: List<RexNode> = convertOperands(cx, call, SqlOperandTypeChecker.Consistency.NONE)
        if (`fun`.getFunctionType() === SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR) {
            return makeConstructorCall(cx, `fun`, exprs)
        }
        var returnType: RelDataType = cx.getValidator().getValidatedNodeTypeIfKnown(call)
        if (returnType == null) {
            returnType = cx.getRexBuilder().deriveReturnType(`fun`, exprs)
        }
        return cx.getRexBuilder().makeCall(returnType, `fun`, exprs)
    }

    fun convertWindowFunction(
        cx: SqlRexContext,
        `fun`: SqlWindowTableFunction?,
        call: SqlCall
    ): RexNode {
        // The first operand of window function is actually a query, skip that.
        val operands: List<SqlNode> = Util.skip(call.getOperandList())
        val exprs: List<RexNode> = convertOperands(
            cx, call, operands,
            SqlOperandTypeChecker.Consistency.NONE
        )
        var returnType: RelDataType = cx.getValidator().getValidatedNodeTypeIfKnown(call)
        if (returnType == null) {
            returnType = cx.getRexBuilder().deriveReturnType(`fun`, exprs)
        }
        return cx.getRexBuilder().makeCall(returnType, `fun`, exprs)
    }

    fun convertJsonValueFunction(
        cx: SqlRexContext,
        `fun`: SqlJsonValueFunction?,
        call: SqlCall
    ): RexNode {
        // For Expression with explicit return type:
        // i.e. json_value('{"foo":"bar"}', 'lax $.foo', returning varchar(2000))
        // use the specified type as the return type.
        var operands: List<SqlNode> = call.getOperandList()
        @SuppressWarnings("all") val hasExplicitReturningType: Boolean = SqlJsonValueFunction.hasExplicitTypeSpec(
            operands.toArray(SqlNode.EMPTY_ARRAY)
        )
        if (hasExplicitReturningType) {
            operands = SqlJsonValueFunction.removeTypeSpecOperands(call)
        }
        val exprs: List<RexNode> = convertOperands(
            cx, call, operands,
            SqlOperandTypeChecker.Consistency.NONE
        )
        val returnType: RelDataType = cx.getValidator().getValidatedNodeTypeIfKnown(call)
        requireNonNull(returnType) { "Unable to get type of $call" }
        return cx.getRexBuilder().makeCall(returnType, `fun`, exprs)
    }

    fun convertSequenceValue(
        cx: SqlRexContext,
        `fun`: SqlSequenceValueOperator?,
        call: SqlCall
    ): RexNode {
        val operands: List<SqlNode> = call.getOperandList()
        assert(operands.size() === 1)
        assert(operands[0] is SqlIdentifier)
        val id: SqlIdentifier = operands[0] as SqlIdentifier
        val key: String = Util.listToString(id.names)
        val returnType: RelDataType = cx.getValidator().getValidatedNodeType(call)
        return cx.getRexBuilder().makeCall(
            returnType, `fun`,
            ImmutableList.of(cx.getRexBuilder().makeLiteral(key))
        )
    }

    fun convertAggregateFunction(
        cx: SqlRexContext,
        `fun`: SqlAggFunction,
        call: SqlCall
    ): RexNode {
        val exprs: List<RexNode>
        exprs = if (call.isCountStar()) {
            ImmutableList.of()
        } else {
            convertOperands(
                cx,
                call,
                SqlOperandTypeChecker.Consistency.NONE
            )
        }
        var returnType: RelDataType = cx.getValidator().getValidatedNodeTypeIfKnown(call)
        val groupCount: Int = cx.getGroupCount()
        if (returnType == null) {
            val binding: RexCallBinding = object : RexCallBinding(
                cx.getTypeFactory(), `fun`, exprs,
                ImmutableList.of()
            ) {
                @get:Override
                val groupCount: Int
                    get() = groupCount
            }
            returnType = `fun`.inferReturnType(binding)
        }
        return cx.getRexBuilder().makeCall(returnType, `fun`, exprs)
    }

    private fun convertItem(
        cx: SqlRexContext,
        call: SqlCall
    ): RexNode {
        val rexBuilder: RexBuilder = cx.getRexBuilder()
        val op: SqlOperator = call.getOperator()
        val operandTypeChecker: SqlOperandTypeChecker = op.getOperandTypeChecker()
        val consistency: SqlOperandTypeChecker.Consistency =
            if (operandTypeChecker == null) SqlOperandTypeChecker.Consistency.NONE else operandTypeChecker.getConsistency()
        val exprs: List<RexNode> = convertOperands(cx, call, consistency)
        val collectionType: RelDataType = exprs[0].getType()
        val isRowTypeField: Boolean = SqlTypeUtil.isRow(collectionType)
        val isNumericIndex: Boolean = SqlTypeUtil.isIntType(exprs[1].getType())
        if (isRowTypeField && isNumericIndex) {
            val opBinding: SqlOperatorBinding = RexCallBinding(
                cx.getTypeFactory(), op, exprs, ImmutableList.of()
            )
            val operandType: RelDataType = opBinding.getOperandType(0)
            val index: Integer = opBinding.getOperandLiteralValue(1, Integer::class.java)
            return if (index == null || index < 1 || index > operandType.getFieldCount()) {
                throw AssertionError(
                    "Cannot access field at position "
                            + index + " within ROW type: " + operandType
                )
            } else {
                val relDataTypeField: RelDataTypeField = collectionType.getFieldList().get(index - 1)
                rexBuilder.makeFieldAccess(
                    exprs[0], relDataTypeField.getName(), false
                )
            }
        }
        val type: RelDataType = rexBuilder.deriveReturnType(op, exprs)
        return rexBuilder.makeCall(type, op, RexUtil.flatten(exprs, op))
    }

    /**
     * Converts a call to an operator into a [RexCall] to the same
     * operator.
     *
     *
     * Called automatically via reflection.
     *
     * @param cx   Context
     * @param call Call
     * @return Rex call
     */
    fun convertCall(
        cx: SqlRexContext,
        call: SqlCall
    ): RexNode {
        val op: SqlOperator = call.getOperator()
        val rexBuilder: RexBuilder = cx.getRexBuilder()
        val operandTypeChecker: SqlOperandTypeChecker = op.getOperandTypeChecker()
        val consistency: SqlOperandTypeChecker.Consistency =
            if (operandTypeChecker == null) SqlOperandTypeChecker.Consistency.NONE else operandTypeChecker.getConsistency()
        val exprs: List<RexNode> = convertOperands(cx, call, consistency)
        val type: RelDataType = rexBuilder.deriveReturnType(op, exprs)

        // Expand 'ROW (x0, x1, ...) = ROW (y0, y1, ...)'
        // to 'x0 = y0 AND x1 = y1 AND ...'
        if (op.kind === SqlKind.EQUALS) {
            val expr0: RexNode = RexUtil.removeCast(exprs[0])
            val expr1: RexNode = RexUtil.removeCast(exprs[1])
            if (expr0.getKind() === SqlKind.ROW && expr1.getKind() === SqlKind.ROW) {
                val call0: RexCall = expr0 as RexCall
                val call1: RexCall = expr1 as RexCall
                val eqList: List<RexNode> = ArrayList()
                Pair.forEach(call0.getOperands(), call1.getOperands()) { x, y ->
                    eqList.add(
                        rexBuilder.makeCall(
                            op,
                            x,
                            y
                        )
                    )
                }
                return RexUtil.composeConjunction(rexBuilder, eqList)
            }
        }
        return rexBuilder.makeCall(type, op, RexUtil.flatten(exprs, op))
    }

    private fun convertPlus(
        cx: SqlRexContext, call: SqlCall
    ): RexNode {
        val rex: RexNode = convertCall(cx, call)
        return when (rex.getType().getSqlTypeName()) {
            DATE, TIME, TIMESTAMP -> {
                // Use special "+" operator for datetime + interval.
                // Re-order operands, if necessary, so that interval is second.
                val rexBuilder: RexBuilder = cx.getRexBuilder()
                var operands: List<RexNode> = (rex as RexCall).getOperands()
                if (operands.size() === 2) {
                    val sqlTypeName: SqlTypeName = operands[0].getType().getSqlTypeName()
                    when (sqlTypeName) {
                        INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> operands =
                            ImmutableList.of(operands[1], operands[0])
                        else -> {}
                    }
                }
                rexBuilder.makeCall(
                    rex.getType(),
                    SqlStdOperatorTable.DATETIME_PLUS, operands
                )
            }
            else -> rex
        }
    }

    private fun convertIsDistinctFrom(
        cx: SqlRexContext,
        call: SqlCall,
        neg: Boolean
    ): RexNode {
        val op0: RexNode = cx.convertExpression(call.operand(0))
        val op1: RexNode = cx.convertExpression(call.operand(1))
        return RelOptUtil.isDistinctFrom(
            cx.getRexBuilder(), op0, op1, neg
        )
    }

    /**
     * Converts a BETWEEN expression.
     *
     *
     * Called automatically via reflection.
     */
    fun convertBetween(
        cx: SqlRexContext,
        op: SqlBetweenOperator,
        call: SqlCall
    ): RexNode {
        val operandTypeChecker: SqlOperandTypeChecker = op.getOperandTypeChecker()
        val consistency: SqlOperandTypeChecker.Consistency =
            if (operandTypeChecker == null) SqlOperandTypeChecker.Consistency.NONE else operandTypeChecker.getConsistency()
        val list: List<RexNode> = convertOperands(
            cx, call,
            consistency
        )
        val x: RexNode = list[SqlBetweenOperator.VALUE_OPERAND]
        val y: RexNode = list[SqlBetweenOperator.LOWER_OPERAND]
        val z: RexNode = list[SqlBetweenOperator.UPPER_OPERAND]
        val rexBuilder: RexBuilder = cx.getRexBuilder()
        val ge1: RexNode = ge(rexBuilder, x, y)
        val le1: RexNode = le(rexBuilder, x, z)
        val and1: RexNode = and(rexBuilder, ge1, le1)
        var res: RexNode
        val symmetric: SqlBetweenOperator.Flag = op.flag
        res = when (symmetric) {
            ASYMMETRIC -> and1
            SYMMETRIC -> {
                val ge2: RexNode = ge(rexBuilder, x, z)
                val le2: RexNode = le(rexBuilder, x, y)
                val and2: RexNode = and(rexBuilder, ge2, le2)
                or(rexBuilder, and1, and2)
            }
            else -> throw Util.unexpected(symmetric)
        }
        val betweenOp: SqlBetweenOperator = call.getOperator() as SqlBetweenOperator
        if (betweenOp.isNegated()) {
            res = rexBuilder.makeCall(SqlStdOperatorTable.NOT, res)
        }
        return res
    }

    /**
     * Converts a SUBSTRING expression.
     *
     *
     * Called automatically via reflection.
     */
    fun convertSubstring(
        cx: SqlRexContext,
        op: SqlSubstringFunction,
        call: SqlCall
    ): RexNode {
        val library: SqlLibrary = cx.getValidator().config().sqlConformance().semantics()
        val basicCall: SqlBasicCall = call as SqlBasicCall
        return when (library) {
            BIG_QUERY -> toRex(cx, basicCall, SqlLibraryOperators.SUBSTR_BIG_QUERY)
            MYSQL -> toRex(cx, basicCall, SqlLibraryOperators.SUBSTR_MYSQL)
            ORACLE -> toRex(cx, basicCall, SqlLibraryOperators.SUBSTR_ORACLE)
            POSTGRESQL -> convertFunction(cx, op, call)
            else -> convertFunction(cx, op, call)
        }
    }

    private fun toRex(cx: SqlRexContext, call: SqlBasicCall, f: SqlFunction): RexNode {
        val call2: SqlCall = SqlBasicCall(f, call.getOperandList(), call.getParserPosition())
        val convertlet: SqlRexConvertlet = requireNonNull(get(call2))
        return convertlet.convertCall(cx, call2)
    }

    /**
     * Converts a LiteralChain expression: that is, concatenates the operands
     * immediately, to produce a single literal string.
     *
     *
     * Called automatically via reflection.
     */
    fun convertLiteralChain(
        cx: SqlRexContext,
        op: SqlLiteralChainOperator?,
        call: SqlCall?
    ): RexNode {
        Util.discard(cx)
        val sum: SqlLiteral = SqlLiteralChainOperator.concatenateOperands(call)
        return cx.convertLiteral(sum)
    }

    /**
     * Converts a ROW.
     *
     *
     * Called automatically via reflection.
     */
    fun convertRow(
        cx: SqlRexContext,
        op: SqlRowOperator?,
        call: SqlCall
    ): RexNode {
        if (cx.getValidator().getValidatedNodeType(call).getSqlTypeName()
            !== SqlTypeName.COLUMN_LIST
        ) {
            return convertCall(cx, call)
        }
        val rexBuilder: RexBuilder = cx.getRexBuilder()
        val columns: List<RexNode> = ArrayList()
        for (operand in SqlIdentifier.simpleNames(call.getOperandList())) {
            columns.add(rexBuilder.makeLiteral(operand))
        }
        val type: RelDataType = rexBuilder.deriveReturnType(SqlStdOperatorTable.COLUMN_LIST, columns)
        return rexBuilder.makeCall(type, SqlStdOperatorTable.COLUMN_LIST, columns)
    }

    /**
     * Converts a call to OVERLAPS.
     *
     *
     * Called automatically via reflection.
     */
    fun convertOverlaps(
        cx: SqlRexContext,
        op: SqlOverlapsOperator,
        call: SqlCall
    ): RexNode {
        // for intervals [t0, t1] overlaps [t2, t3], we can find if the
        // intervals overlaps by: ~(t1 < t2 or t3 < t0)
        assert(call.getOperandList().size() === 2)
        val left: Pair<RexNode, RexNode> = convertOverlapsOperand(cx, call.getParserPosition(), call.operand(0))
        val r0: RexNode = left.left
        val r1: RexNode = left.right
        val right: Pair<RexNode, RexNode> = convertOverlapsOperand(cx, call.getParserPosition(), call.operand(1))
        val r2: RexNode = right.left
        val r3: RexNode = right.right

        // Sort end points into start and end, such that (s0 <= e0) and (s1 <= e1).
        val rexBuilder: RexBuilder = cx.getRexBuilder()
        val leftSwap: RexNode = le(rexBuilder, r0, r1)
        val s0: RexNode = case_(rexBuilder, leftSwap, r0, r1)
        val e0: RexNode = case_(rexBuilder, leftSwap, r1, r0)
        val rightSwap: RexNode = le(rexBuilder, r2, r3)
        val s1: RexNode = case_(rexBuilder, rightSwap, r2, r3)
        val e1: RexNode = case_(rexBuilder, rightSwap, r3, r2)
        return when (op.kind) {
            OVERLAPS -> and(
                rexBuilder,
                ge(rexBuilder, e0, s1),
                ge(rexBuilder, e1, s0)
            )
            CONTAINS -> and(
                rexBuilder,
                le(rexBuilder, s0, s1),
                ge(rexBuilder, e0, e1)
            )
            PERIOD_EQUALS -> and(
                rexBuilder,
                eq(rexBuilder, s0, s1),
                eq(rexBuilder, e0, e1)
            )
            PRECEDES -> le(rexBuilder, e0, s1)
            IMMEDIATELY_PRECEDES -> eq(rexBuilder, e0, s1)
            SUCCEEDS -> ge(rexBuilder, s0, e1)
            IMMEDIATELY_SUCCEEDS -> eq(rexBuilder, s0, e1)
            else -> throw AssertionError(op)
        }
    }

    /**
     * Casts a RexNode value to the validated type of a SqlCall. If the value
     * was already of the validated type, then the value is returned without an
     * additional cast.
     */
    fun castToValidatedType(
        cx: SqlRexContext,
        call: SqlCall?,
        value: RexNode
    ): RexNode {
        return castToValidatedType(
            call, value, cx.getValidator(),
            cx.getRexBuilder()
        )
    }

    /** Convertlet that handles `COVAR_POP`, `COVAR_SAMP`,
     * `REGR_SXX`, `REGR_SYY` windowed aggregate functions.
     */
    private class RegrCovarianceConvertlet internal constructor(kind: SqlKind) : SqlRexConvertlet {
        private val kind: SqlKind

        init {
            this.kind = kind
        }

        @Override
        fun convertCall(cx: SqlRexContext, call: SqlCall): RexNode {
            assert(call.operandCount() === 2)
            val arg1: SqlNode = call.operand(0)
            val arg2: SqlNode = call.operand(1)
            val expr: SqlNode
            val type: RelDataType = cx.getValidator().getValidatedNodeType(call)
            expr = when (kind) {
                COVAR_POP -> expandCovariance(
                    arg1,
                    arg2,
                    null,
                    type,
                    cx,
                    true
                )
                COVAR_SAMP -> expandCovariance(
                    arg1,
                    arg2,
                    null,
                    type,
                    cx,
                    false
                )
                REGR_SXX -> expandRegrSzz(
                    arg2,
                    arg1,
                    type,
                    cx,
                    true
                )
                REGR_SYY -> expandRegrSzz(
                    arg1,
                    arg2,
                    type,
                    cx,
                    true
                )
                else -> throw Util.unexpected(kind)
            }
            val rex: RexNode = cx.convertExpression(expr)
            return cx.getRexBuilder().ensureType(type, rex, true)
        }

        companion object {
            private fun expandRegrSzz(
                arg1: SqlNode, arg2: SqlNode,
                avgType: RelDataType, cx: SqlRexContext, variance: Boolean
            ): SqlNode {
                val pos: SqlParserPos = SqlParserPos.ZERO
                val count: SqlNode = SqlStdOperatorTable.REGR_COUNT.createCall(pos, arg1, arg2)
                val varPop: SqlNode = expandCovariance(arg1, if (variance) arg1 else arg2, arg2, avgType, cx, true)
                val varPopRex: RexNode = cx.convertExpression(varPop)
                val varPopCast: SqlNode
                varPopCast = getCastedSqlNode(varPop, avgType, pos, varPopRex)
                return SqlStdOperatorTable.MULTIPLY.createCall(pos, varPopCast, count)
            }

            private fun expandCovariance(
                arg0Input: SqlNode,
                arg1Input: SqlNode,
                @Nullable dependent: SqlNode?,
                varType: RelDataType,
                cx: SqlRexContext,
                biased: Boolean
            ): SqlNode {
                // covar_pop(x1, x2) ==>
                //     (sum(x1 * x2) - sum(x2) * sum(x1) / count(x1, x2))
                //     / count(x1, x2)
                //
                // covar_samp(x1, x2) ==>
                //     (sum(x1 * x2) - sum(x1) * sum(x2) / count(x1, x2))
                //     / (count(x1, x2) - 1)
                val pos: SqlParserPos = SqlParserPos.ZERO
                val nullLiteral: SqlLiteral = SqlLiteral.createNull(SqlParserPos.ZERO)
                val arg0Rex: RexNode = cx.convertExpression(arg0Input)
                val arg1Rex: RexNode = cx.convertExpression(arg1Input)
                val arg0: SqlNode = getCastedSqlNode(arg0Input, varType, pos, arg0Rex)
                val arg1: SqlNode = getCastedSqlNode(arg1Input, varType, pos, arg1Rex)
                val argSquared: SqlNode = SqlStdOperatorTable.MULTIPLY.createCall(pos, arg0, arg1)
                val sumArgSquared: SqlNode
                val sum0: SqlNode
                val sum1: SqlNode
                val count: SqlNode
                if (dependent == null) {
                    sumArgSquared = SqlStdOperatorTable.SUM.createCall(pos, argSquared)
                    sum0 = SqlStdOperatorTable.SUM.createCall(pos, arg0, arg1)
                    sum1 = SqlStdOperatorTable.SUM.createCall(pos, arg1, arg0)
                    count = SqlStdOperatorTable.REGR_COUNT.createCall(pos, arg0, arg1)
                } else {
                    sumArgSquared = SqlStdOperatorTable.SUM.createCall(pos, argSquared, dependent)
                    sum0 = SqlStdOperatorTable.SUM.createCall(
                        pos, arg0, if (Objects.equals(dependent, arg0Input)) arg1 else dependent
                    )
                    sum1 = SqlStdOperatorTable.SUM.createCall(
                        pos, arg1, if (Objects.equals(dependent, arg1Input)) arg0 else dependent
                    )
                    count = SqlStdOperatorTable.REGR_COUNT.createCall(
                        pos, arg0, if (Objects.equals(dependent, arg0Input)) arg1 else dependent
                    )
                }
                val sumSquared: SqlNode = SqlStdOperatorTable.MULTIPLY.createCall(pos, sum0, sum1)
                val countCasted: SqlNode = getCastedSqlNode(count, varType, pos, cx.convertExpression(count))
                val avgSumSquared: SqlNode = SqlStdOperatorTable.DIVIDE.createCall(pos, sumSquared, countCasted)
                val diff: SqlNode = SqlStdOperatorTable.MINUS.createCall(pos, sumArgSquared, avgSumSquared)
                val denominator: SqlNode
                if (biased) {
                    denominator = countCasted
                } else {
                    val one: SqlNumericLiteral = SqlLiteral.createExactNumeric("1", pos)
                    denominator = SqlCase(
                        SqlParserPos.ZERO, countCasted,
                        SqlNodeList.of(SqlStdOperatorTable.EQUALS.createCall(pos, countCasted, one)),
                        SqlNodeList.of(getCastedSqlNode(nullLiteral, varType, pos, null)),
                        SqlStdOperatorTable.MINUS.createCall(pos, countCasted, one)
                    )
                }
                return SqlStdOperatorTable.DIVIDE.createCall(pos, diff, denominator)
            }

            private fun getCastedSqlNode(
                argInput: SqlNode, varType: RelDataType,
                pos: SqlParserPos, @Nullable argRex: RexNode?
            ): SqlNode {
                val arg: SqlNode
                arg = if (argRex != null && !argRex.getType().equals(varType)) {
                    SqlStdOperatorTable.CAST.createCall(
                        pos, argInput, SqlTypeUtil.convertTypeToSpec(varType)
                    )
                } else {
                    argInput
                }
                return arg
            }
        }
    }

    /** Convertlet that handles `AVG` and `VARIANCE`
     * windowed aggregate functions.  */
    private class AvgVarianceConvertlet internal constructor(kind: SqlKind) : SqlRexConvertlet {
        private val kind: SqlKind

        init {
            this.kind = kind
        }

        @Override
        fun convertCall(cx: SqlRexContext, call: SqlCall): RexNode {
            assert(call.operandCount() === 1)
            val arg: SqlNode = call.operand(0)
            val expr: SqlNode
            val type: RelDataType = cx.getValidator().getValidatedNodeType(call)
            expr = when (kind) {
                AVG -> expandAvg(
                    arg,
                    type,
                    cx
                )
                STDDEV_POP -> expandVariance(
                    arg,
                    type,
                    cx,
                    true,
                    true
                )
                STDDEV_SAMP -> expandVariance(
                    arg,
                    type,
                    cx,
                    false,
                    true
                )
                VAR_POP -> expandVariance(
                    arg,
                    type,
                    cx,
                    true,
                    false
                )
                VAR_SAMP -> expandVariance(
                    arg,
                    type,
                    cx,
                    false,
                    false
                )
                else -> throw Util.unexpected(kind)
            }
            val rex: RexNode = cx.convertExpression(expr)
            return cx.getRexBuilder().ensureType(type, rex, true)
        }

        companion object {
            private fun expandAvg(
                arg: SqlNode, avgType: RelDataType, cx: SqlRexContext
            ): SqlNode {
                val pos: SqlParserPos = SqlParserPos.ZERO
                val sum: SqlNode = SqlStdOperatorTable.SUM.createCall(pos, arg)
                val sumRex: RexNode = cx.convertExpression(sum)
                val sumCast: SqlNode
                sumCast = getCastedSqlNode(sum, avgType, pos, sumRex)
                val count: SqlNode = SqlStdOperatorTable.COUNT.createCall(pos, arg)
                return SqlStdOperatorTable.DIVIDE.createCall(
                    pos, sumCast, count
                )
            }

            private fun expandVariance(
                argInput: SqlNode,
                varType: RelDataType,
                cx: SqlRexContext,
                biased: Boolean,
                sqrt: Boolean
            ): SqlNode {
                // stddev_pop(x) ==>
                //   power(
                //     (sum(x * x) - sum(x) * sum(x) / count(x))
                //     / count(x),
                //     .5)
                //
                // stddev_samp(x) ==>
                //   power(
                //     (sum(x * x) - sum(x) * sum(x) / count(x))
                //     / (count(x) - 1),
                //     .5)
                //
                // var_pop(x) ==>
                //     (sum(x * x) - sum(x) * sum(x) / count(x))
                //     / count(x)
                //
                // var_samp(x) ==>
                //     (sum(x * x) - sum(x) * sum(x) / count(x))
                //     / (count(x) - 1)
                val pos: SqlParserPos = SqlParserPos.ZERO
                val arg: SqlNode = getCastedSqlNode(argInput, varType, pos, cx.convertExpression(argInput))
                val argSquared: SqlNode = SqlStdOperatorTable.MULTIPLY.createCall(pos, arg, arg)
                val argSquaredCasted: SqlNode =
                    getCastedSqlNode(argSquared, varType, pos, cx.convertExpression(argSquared))
                val sumArgSquared: SqlNode = SqlStdOperatorTable.SUM.createCall(pos, argSquaredCasted)
                val sumArgSquaredCasted: SqlNode =
                    getCastedSqlNode(sumArgSquared, varType, pos, cx.convertExpression(sumArgSquared))
                val sum: SqlNode = SqlStdOperatorTable.SUM.createCall(pos, arg)
                val sumCasted: SqlNode = getCastedSqlNode(sum, varType, pos, cx.convertExpression(sum))
                val sumSquared: SqlNode = SqlStdOperatorTable.MULTIPLY.createCall(pos, sumCasted, sumCasted)
                val sumSquaredCasted: SqlNode =
                    getCastedSqlNode(sumSquared, varType, pos, cx.convertExpression(sumSquared))
                val count: SqlNode = SqlStdOperatorTable.COUNT.createCall(pos, arg)
                val countCasted: SqlNode = getCastedSqlNode(count, varType, pos, cx.convertExpression(count))
                val avgSumSquared: SqlNode = SqlStdOperatorTable.DIVIDE.createCall(pos, sumSquaredCasted, countCasted)
                val avgSumSquaredCasted: SqlNode =
                    getCastedSqlNode(avgSumSquared, varType, pos, cx.convertExpression(avgSumSquared))
                val diff: SqlNode = SqlStdOperatorTable.MINUS.createCall(pos, sumArgSquaredCasted, avgSumSquaredCasted)
                val diffCasted: SqlNode = getCastedSqlNode(diff, varType, pos, cx.convertExpression(diff))
                val denominator: SqlNode
                if (biased) {
                    denominator = countCasted
                } else {
                    val one: SqlNumericLiteral = SqlLiteral.createExactNumeric("1", pos)
                    val nullLiteral: SqlLiteral = SqlLiteral.createNull(SqlParserPos.ZERO)
                    denominator = SqlCase(
                        SqlParserPos.ZERO,
                        count,
                        SqlNodeList.of(SqlStdOperatorTable.EQUALS.createCall(pos, count, one)),
                        SqlNodeList.of(getCastedSqlNode(nullLiteral, varType, pos, null)),
                        SqlStdOperatorTable.MINUS.createCall(pos, count, one)
                    )
                }
                val div: SqlNode = SqlStdOperatorTable.DIVIDE.createCall(pos, diffCasted, denominator)
                val divCasted: SqlNode = getCastedSqlNode(div, varType, pos, cx.convertExpression(div))
                var result: SqlNode = div
                if (sqrt) {
                    val half: SqlNumericLiteral = SqlLiteral.createExactNumeric("0.5", pos)
                    result = SqlStdOperatorTable.POWER.createCall(pos, divCasted, half)
                }
                return result
            }

            private fun getCastedSqlNode(
                argInput: SqlNode, varType: RelDataType,
                pos: SqlParserPos, @Nullable argRex: RexNode?
            ): SqlNode {
                val arg: SqlNode
                arg = if (argRex != null && !argRex.getType().equals(varType)) {
                    SqlStdOperatorTable.CAST.createCall(
                        pos, argInput, SqlTypeUtil.convertTypeToSpec(varType)
                    )
                } else {
                    argInput
                }
                return arg
            }
        }
    }

    /** Convertlet that converts `LTRIM` and `RTRIM` to
     * `TRIM`.  */
    private class TrimConvertlet internal constructor(flag: SqlTrimFunction.Flag) : SqlRexConvertlet {
        private val flag: SqlTrimFunction.Flag

        init {
            this.flag = flag
        }

        @Override
        fun convertCall(cx: SqlRexContext, call: SqlCall): RexNode {
            val rexBuilder: RexBuilder = cx.getRexBuilder()
            val operand: RexNode = cx.convertExpression(call.getOperandList().get(0))
            return rexBuilder.makeCall(
                SqlStdOperatorTable.TRIM,
                rexBuilder.makeFlag(flag), rexBuilder.makeLiteral(" "), operand
            )
        }
    }

    /** Convertlet that converts `GREATEST` and `LEAST`.  */
    private class GreatestConvertlet : SqlRexConvertlet {
        @Override
        fun convertCall(cx: SqlRexContext, call: SqlCall): RexNode {
            // Translate
            //   GREATEST(a, b, c, d)
            // to
            //   CASE
            //   WHEN a IS NULL OR b IS NULL OR c IS NULL OR d IS NULL
            //   THEN NULL
            //   WHEN a > b AND a > c AND a > d
            //   THEN a
            //   WHEN b > c AND b > d
            //   THEN b
            //   WHEN c > d
            //   THEN c
            //   ELSE d
            //   END
            val rexBuilder: RexBuilder = cx.getRexBuilder()
            val type: RelDataType = cx.getValidator().getValidatedNodeType(call)
            val op: SqlBinaryOperator
            op = when (call.getKind()) {
                GREATEST -> SqlStdOperatorTable.GREATER_THAN
                LEAST -> SqlStdOperatorTable.LESS_THAN
                else -> throw AssertionError()
            }
            val exprs: List<RexNode> = convertOperands(cx, call, SqlOperandTypeChecker.Consistency.NONE)
            val list: List<RexNode> = ArrayList()
            val orList: List<RexNode> = ArrayList()
            for (expr in exprs) {
                orList.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, expr))
            }
            list.add(RexUtil.composeDisjunction(rexBuilder, orList))
            list.add(rexBuilder.makeNullLiteral(type))
            for (i in 0 until exprs.size() - 1) {
                val expr: RexNode = exprs[i]
                val andList: List<RexNode> = ArrayList()
                for (j in i + 1 until exprs.size()) {
                    val expr2: RexNode = exprs[j]
                    andList.add(rexBuilder.makeCall(op, expr, expr2))
                }
                list.add(RexUtil.composeConjunction(rexBuilder, andList))
                list.add(expr)
            }
            list.add(exprs[exprs.size() - 1])
            return rexBuilder.makeCall(type, SqlStdOperatorTable.CASE, list)
        }
    }

    /** Convertlet that handles `FLOOR` and `CEIL` functions.  */
    private inner class FloorCeilConvertlet : SqlRexConvertlet {
        @Override
        fun convertCall(cx: SqlRexContext, call: SqlCall): RexNode {
            return convertFloorCeil(cx, call)
        }
    }

    /** Convertlet that handles the `SUBSTR` function; various dialects
     * have slightly different specifications. PostgreSQL seems to comply with
     * the ISO standard for the `SUBSTRING` function, and therefore
     * Calcite's default behavior matches PostgreSQL.  */
    private class SubstrConvertlet internal constructor(library: SqlLibrary) : SqlRexConvertlet {
        private val library: SqlLibrary

        init {
            this.library = library
            Preconditions.checkArgument(library === SqlLibrary.ORACLE || library === SqlLibrary.MYSQL || library === SqlLibrary.BIG_QUERY || library === SqlLibrary.POSTGRESQL)
        }

        @Override
        fun convertCall(cx: SqlRexContext, call: SqlCall): RexNode {
            // Translate
            //   SUBSTR(value, start, length)
            //
            // to the following if we want PostgreSQL semantics:
            //   SUBSTRING(value, start, length)
            //
            // to the following if we want Oracle semantics:
            //   SUBSTRING(
            //     value
            //     FROM CASE
            //          WHEN start = 0
            //          THEN 1
            //          WHEN start + (length(value) + 1) < 1
            //          THEN length(value) + 1
            //          WHEN start < 0
            //          THEN start + (length(value) + 1)
            //          ELSE start)
            //     FOR CASE WHEN length < 0 THEN 0 ELSE length END)
            //
            // to the following in MySQL:
            //   SUBSTRING(
            //     value
            //     FROM CASE
            //          WHEN start = 0
            //          THEN length(value) + 1    -- different from Oracle
            //          WHEN start + (length(value) + 1) < 1
            //          THEN length(value) + 1
            //          WHEN start < 0
            //          THEN start + length(value) + 1
            //          ELSE start)
            //     FOR CASE WHEN length < 0 THEN 0 ELSE length END)
            //
            // to the following if we want BigQuery semantics:
            //   CASE
            //   WHEN start + (length(value) + 1) < 1
            //   THEN value
            //   ELSE SUBSTRING(
            //       value
            //       FROM CASE
            //            WHEN start = 0
            //            THEN 1
            //            WHEN start < 0
            //            THEN start + length(value) + 1
            //            ELSE start)
            //       FOR CASE WHEN length < 0 THEN 0 ELSE length END)
            val rexBuilder: RexBuilder = cx.getRexBuilder()
            val exprs: List<RexNode> = convertOperands(cx, call, SqlOperandTypeChecker.Consistency.NONE)
            val value: RexNode = exprs[0]
            val start: RexNode = exprs[1]
            val startType: RelDataType = start.getType()
            val zeroLiteral: RexLiteral = rexBuilder.makeLiteral(0, startType)
            val oneLiteral: RexLiteral = rexBuilder.makeLiteral(1, startType)
            val valueLength: RexNode = if (SqlTypeUtil.isBinary(value.getType())) rexBuilder.makeCall(
                SqlStdOperatorTable.OCTET_LENGTH,
                value
            ) else rexBuilder.makeCall(SqlStdOperatorTable.CHAR_LENGTH, value)
            val valueLengthPlusOne: RexNode = rexBuilder.makeCall(
                SqlStdOperatorTable.PLUS, valueLength,
                oneLiteral
            )
            val newStart: RexNode
            newStart = when (library) {
                POSTGRESQL -> if (call.operandCount() === 2) {
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.CASE,
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.LESS_THAN, start,
                            oneLiteral
                        ),
                        oneLiteral, start
                    )
                } else {
                    start
                }
                BIG_QUERY -> rexBuilder.makeCall(
                    SqlStdOperatorTable.CASE,
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.EQUALS, start,
                        zeroLiteral
                    ),
                    oneLiteral,
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.LESS_THAN, start,
                        zeroLiteral
                    ),
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.PLUS, start,
                        valueLengthPlusOne
                    ),
                    start
                )
                else -> rexBuilder.makeCall(
                    SqlStdOperatorTable.CASE,
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.EQUALS, start,
                        zeroLiteral
                    ),
                    if (library === SqlLibrary.MYSQL) valueLengthPlusOne else oneLiteral,
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.LESS_THAN,
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.PLUS, start,
                            valueLengthPlusOne
                        ),
                        oneLiteral
                    ),
                    valueLengthPlusOne,
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.LESS_THAN, start,
                        zeroLiteral
                    ),
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.PLUS, start,
                        valueLengthPlusOne
                    ),
                    start
                )
            }
            if (call.operandCount() === 2) {
                return rexBuilder.makeCall(
                    SqlStdOperatorTable.SUBSTRING, value,
                    newStart
                )
            }
            assert(call.operandCount() === 3)
            val length: RexNode = exprs[2]
            val newLength: RexNode
            newLength = when (library) {
                POSTGRESQL -> length
                else -> rexBuilder.makeCall(
                    SqlStdOperatorTable.CASE,
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.LESS_THAN, length,
                        zeroLiteral
                    ),
                    zeroLiteral, length
                )
            }
            val substringCall: RexNode = rexBuilder.makeCall(
                SqlStdOperatorTable.SUBSTRING, value, newStart,
                newLength
            )
            return when (library) {
                BIG_QUERY -> rexBuilder.makeCall(
                    SqlStdOperatorTable.CASE,
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.LESS_THAN,
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.PLUS, start,
                            valueLengthPlusOne
                        ), oneLiteral
                    ),
                    value, substringCall
                )
                else -> substringCall
            }
        }
    }

    /** Convertlet that handles the `TIMESTAMPADD` function.  */
    private class TimestampAddConvertlet : SqlRexConvertlet {
        @Override
        fun convertCall(cx: SqlRexContext, call: SqlCall): RexNode {
            // TIMESTAMPADD(unit, count, timestamp)
            //  => timestamp + count * INTERVAL '1' UNIT
            val rexBuilder: RexBuilder = cx.getRexBuilder()
            val unitLiteral: SqlLiteral = call.operand(0)
            val unit: TimeUnit = unitLiteral.getValueAs(TimeUnit::class.java)
            val interval2Add: RexNode
            val qualifier = SqlIntervalQualifier(unit, null, unitLiteral.getParserPosition())
            val op1: RexNode = cx.convertExpression(call.operand(1))
            interval2Add = when (unit) {
                MICROSECOND, NANOSECOND -> divide(
                    rexBuilder,
                    multiply(
                        rexBuilder,
                        rexBuilder.makeIntervalLiteral(BigDecimal.ONE, qualifier), op1
                    ),
                    BigDecimal.ONE.divide(
                        unit.multiplier,
                        RoundingMode.UNNECESSARY
                    )
                )
                else -> multiply(
                    rexBuilder,
                    rexBuilder.makeIntervalLiteral(unit.multiplier, qualifier), op1
                )
            }
            return rexBuilder.makeCall(
                SqlStdOperatorTable.DATETIME_PLUS,
                cx.convertExpression(call.operand(2)), interval2Add
            )
        }
    }

    /** Convertlet that handles the `TIMESTAMPDIFF` function.  */
    private class TimestampDiffConvertlet : SqlRexConvertlet {
        @Override
        fun convertCall(cx: SqlRexContext, call: SqlCall): RexNode {
            // TIMESTAMPDIFF(unit, t1, t2)
            //    => (t2 - t1) UNIT
            val rexBuilder: RexBuilder = cx.getRexBuilder()
            val unitLiteral: SqlLiteral = call.operand(0)
            var unit: TimeUnit = unitLiteral.getValueAs(TimeUnit::class.java)
            var multiplier: BigDecimal = BigDecimal.ONE
            var divider: BigDecimal = BigDecimal.ONE
            val sqlTypeName: SqlTypeName = if (unit === TimeUnit.NANOSECOND) SqlTypeName.BIGINT else SqlTypeName.INTEGER
            when (unit) {
                MICROSECOND, MILLISECOND, NANOSECOND, WEEK -> {
                    multiplier = BigDecimal.valueOf(DateTimeUtils.MILLIS_PER_SECOND)
                    divider = unit.multiplier
                    unit = TimeUnit.SECOND
                }
                QUARTER -> {
                    divider = unit.multiplier
                    unit = TimeUnit.MONTH
                }
                else -> {}
            }
            val qualifier = SqlIntervalQualifier(unit, null, SqlParserPos.ZERO)
            val op2: RexNode = cx.convertExpression(call.operand(2))
            val op1: RexNode = cx.convertExpression(call.operand(1))
            val intervalType: RelDataType = cx.getTypeFactory().createTypeWithNullability(
                cx.getTypeFactory().createSqlIntervalType(qualifier),
                op1.getType().isNullable() || op2.getType().isNullable()
            )
            val rexCall: RexCall = rexBuilder.makeCall(
                intervalType, SqlStdOperatorTable.MINUS_DATE,
                ImmutableList.of(op2, op1)
            ) as RexCall
            val intType: RelDataType = cx.getTypeFactory().createTypeWithNullability(
                cx.getTypeFactory().createSqlType(sqlTypeName),
                SqlTypeUtil.containsNullable(rexCall.getType())
            )
            val e: RexNode = rexBuilder.makeCast(intType, rexCall)
            return rexBuilder.multiplyDivide(e, multiplier, divider)
        }
    }

    companion object {
        /** Singleton instance.  */
        val INSTANCE = StandardConvertletTable()

        /** Converts a call to the NVL function.  */
        private fun convertNvl(cx: SqlRexContext, call: SqlCall): RexNode {
            val rexBuilder: RexBuilder = cx.getRexBuilder()
            val operand0: RexNode = cx.convertExpression(call.getOperandList().get(0))
            val operand1: RexNode = cx.convertExpression(call.getOperandList().get(1))
            val type: RelDataType = cx.getValidator().getValidatedNodeType(call)
            // Preserve Operand Nullability
            return rexBuilder.makeCall(
                type, SqlStdOperatorTable.CASE,
                ImmutableList.of(
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.IS_NOT_NULL,
                        operand0
                    ),
                    rexBuilder.makeCast(
                        cx.getTypeFactory()
                            .createTypeWithNullability(type, operand0.getType().isNullable()),
                        operand0
                    ),
                    rexBuilder.makeCast(
                        cx.getTypeFactory()
                            .createTypeWithNullability(type, operand1.getType().isNullable()),
                        operand1
                    )
                )
            )
        }

        /** Converts a call to the DECODE function.  */
        private fun convertDecode(cx: SqlRexContext, call: SqlCall): RexNode {
            val rexBuilder: RexBuilder = cx.getRexBuilder()
            val operands: List<RexNode> = convertOperands(cx, call, SqlOperandTypeChecker.Consistency.NONE)
            val type: RelDataType = cx.getValidator().getValidatedNodeType(call)
            val exprs: List<RexNode> = ArrayList()
            var i = 1
            while (i < operands.size() - 1) {
                exprs.add(
                    RelOptUtil.isDistinctFrom(
                        rexBuilder, operands[0],
                        operands[i], true
                    )
                )
                exprs.add(operands[i + 1])
                i += 2
            }
            if (operands.size() % 2 === 0) {
                exprs.add(Util.last(operands))
            } else {
                exprs.add(rexBuilder.makeNullLiteral(type))
            }
            return rexBuilder.makeCall(type, SqlStdOperatorTable.CASE, exprs)
        }

        /** Converts a call to the IF function.
         *
         *
         * `IF(b, x, y)`  `CASE WHEN b THEN x ELSE y END`.  */
        private fun convertIf(cx: SqlRexContext, call: SqlCall): RexNode {
            val rexBuilder: RexBuilder = cx.getRexBuilder()
            val operands: List<RexNode> = convertOperands(cx, call, SqlOperandTypeChecker.Consistency.NONE)
            val type: RelDataType = cx.getValidator().getValidatedNodeType(call)
            return rexBuilder.makeCall(type, SqlStdOperatorTable.CASE, operands)
        }

        /** Converts an interval expression to a numeric multiplied by an interval
         * literal.  */
        private fun convertInterval(cx: SqlRexContext, call: SqlCall): RexNode {
            // "INTERVAL n HOUR" becomes "n * INTERVAL '1' HOUR"
            val n: SqlNode = call.operand(0)
            val intervalQualifier: SqlIntervalQualifier = call.operand(1)
            val literal: SqlIntervalLiteral = SqlLiteral.createInterval(
                1, "1", intervalQualifier,
                call.getParserPosition()
            )
            val multiply: SqlCall = SqlStdOperatorTable.MULTIPLY.createCall(
                call.getParserPosition(), n,
                literal
            )
            return cx.convertExpression(multiply)
        }

        //~ Methods ----------------------------------------------------------------
        private fun or(rexBuilder: RexBuilder, a0: RexNode, a1: RexNode): RexNode {
            return rexBuilder.makeCall(SqlStdOperatorTable.OR, a0, a1)
        }

        private fun eq(rexBuilder: RexBuilder, a0: RexNode, a1: RexNode): RexNode {
            return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, a0, a1)
        }

        private fun ge(rexBuilder: RexBuilder, a0: RexNode, a1: RexNode): RexNode {
            return rexBuilder.makeCall(
                SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, a0,
                a1
            )
        }

        private fun le(rexBuilder: RexBuilder, a0: RexNode, a1: RexNode): RexNode {
            return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, a0, a1)
        }

        private fun and(rexBuilder: RexBuilder, a0: RexNode, a1: RexNode): RexNode {
            return rexBuilder.makeCall(SqlStdOperatorTable.AND, a0, a1)
        }

        private fun divideInt(
            rexBuilder: RexBuilder, a0: RexNode,
            a1: RexNode
        ): RexNode {
            return rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE_INTEGER, a0, a1)
        }

        private fun plus(rexBuilder: RexBuilder, a0: RexNode, a1: RexNode): RexNode {
            return rexBuilder.makeCall(SqlStdOperatorTable.PLUS, a0, a1)
        }

        private fun minus(rexBuilder: RexBuilder, a0: RexNode, a1: RexNode): RexNode {
            return rexBuilder.makeCall(SqlStdOperatorTable.MINUS, a0, a1)
        }

        private fun multiply(
            rexBuilder: RexBuilder, a0: RexNode,
            a1: RexNode
        ): RexNode {
            return rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, a0, a1)
        }

        private fun case_(rexBuilder: RexBuilder, vararg args: RexNode): RexNode {
            return rexBuilder.makeCall(SqlStdOperatorTable.CASE, args)
        }

        // SqlNode helpers
        private fun plus(pos: SqlParserPos, a0: SqlNode, a1: SqlNode): SqlCall {
            return SqlStdOperatorTable.PLUS.createCall(pos, a0, a1)
        }

        @SuppressWarnings("unused")
        private fun mod(
            rexBuilder: RexBuilder, resType: RelDataType, res: RexNode,
            `val`: BigDecimal
        ): RexNode {
            return if (`val`.equals(BigDecimal.ONE)) {
                res
            } else rexBuilder.makeCall(
                SqlStdOperatorTable.MOD, res,
                rexBuilder.makeExactLiteral(`val`, resType)
            )
        }

        private fun divide(
            rexBuilder: RexBuilder, res: RexNode,
            `val`: BigDecimal
        ): RexNode {
            if (`val`.equals(BigDecimal.ONE)) {
                return res
            }
            // If val is between 0 and 1, rather than divide by val, multiply by its
            // reciprocal. For example, rather than divide by 0.001 multiply by 1000.
            if (`val`.compareTo(BigDecimal.ONE) < 0
                && `val`.signum() === 1
            ) {
                try {
                    val reciprocal: BigDecimal = BigDecimal.ONE.divide(`val`, RoundingMode.UNNECESSARY)
                    return multiply(
                        rexBuilder, res,
                        rexBuilder.makeExactLiteral(reciprocal)
                    )
                } catch (e: ArithmeticException) {
                    // ignore - reciprocal is not an integer
                }
            }
            return divideInt(rexBuilder, res, rexBuilder.makeExactLiteral(`val`))
        }

        private fun makeConstructorCall(
            cx: SqlRexContext,
            constructor: SqlFunction,
            exprs: List<RexNode>
        ): RexNode {
            val rexBuilder: RexBuilder = cx.getRexBuilder()
            val type: RelDataType = rexBuilder.deriveReturnType(constructor, exprs)
            val n: Int = type.getFieldCount()
            val initializationExprs: ImmutableList.Builder<RexNode> = ImmutableList.builder()
            val initializerContext: InitializerContext = object : InitializerContext() {
                @get:Override
                val rexBuilder: RexBuilder
                    get() = rexBuilder

                @Override
                fun validateExpression(rowType: RelDataType?, expr: SqlNode?): SqlNode {
                    throw UnsupportedOperationException()
                }

                @Override
                fun convertExpression(e: SqlNode?): RexNode {
                    throw UnsupportedOperationException()
                }
            }
            for (i in 0 until n) {
                initializationExprs.add(
                    cx.getInitializerExpressionFactory().newAttributeInitializer(
                        type, constructor, i, exprs, initializerContext
                    )
                )
            }
            val defaultCasts: List<RexNode> = RexUtil.generateCastExpressions(
                rexBuilder,
                type,
                initializationExprs.build()
            )
            return rexBuilder.makeNewInvocation(type, defaultCasts)
        }

        private fun elseArgs(count: Int): List<Integer> {
            // If list is odd, e.g. [0, 1, 2, 3, 4] we get [1, 3, 4]
            // If list is even, e.g. [0, 1, 2, 3, 4, 5] we get [2, 4, 5]
            val list: List<Integer> = ArrayList()
            var i = count % 2
            while (true) {
                list.add(i)
                i += 2
                if (i >= count) {
                    list.add(i - 1)
                    break
                }
            }
            return list
        }

        private fun convertOperands(
            cx: SqlRexContext,
            call: SqlCall, consistency: SqlOperandTypeChecker.Consistency
        ): List<RexNode> {
            return convertOperands(cx, call, call.getOperandList(), consistency)
        }

        private fun convertOperands(
            cx: SqlRexContext,
            call: SqlCall, nodes: List<SqlNode>,
            consistency: SqlOperandTypeChecker.Consistency
        ): List<RexNode> {
            val exprs: List<RexNode> = ArrayList()
            for (node in nodes) {
                exprs.add(cx.convertExpression(node))
            }
            val operandTypes: List<RelDataType> = cx.getValidator().getValidatedOperandTypes(call)
            if (operandTypes != null) {
                val oldExprs: List<RexNode> = ArrayList(exprs)
                exprs.clear()
                Pair.forEach(oldExprs, operandTypes) { expr, type ->
                    exprs.add(
                        cx.getRexBuilder().ensureType(type, expr, true)
                    )
                }
            }
            if (exprs.size() > 1) {
                val type: RelDataType? = consistentType(cx, consistency, RexUtil.types(exprs))
                if (type != null) {
                    val oldExprs: List<RexNode> = ArrayList(exprs)
                    exprs.clear()
                    for (expr in oldExprs) {
                        exprs.add(cx.getRexBuilder().ensureType(type, expr, true))
                    }
                }
            }
            return exprs
        }

        @Nullable
        private fun consistentType(
            cx: SqlRexContext,
            consistency: SqlOperandTypeChecker.Consistency, types: List<RelDataType>
        ): RelDataType? {
            var types: List<RelDataType?> = types
            return when (consistency) {
                COMPARE -> {
                    if (SqlTypeUtil.areSameFamily(types)) {
                        // All arguments are of same family. No need for explicit casts.
                        return null
                    }
                    val nonCharacterTypes: List<RelDataType> = ArrayList()
                    for (type in types) {
                        if (type.getFamily() !== SqlTypeFamily.CHARACTER) {
                            nonCharacterTypes.add(type)
                        }
                    }
                    if (!nonCharacterTypes.isEmpty()) {
                        val typeCount: Int = types.size()
                        types = nonCharacterTypes
                        if (nonCharacterTypes.size() < typeCount) {
                            val family: RelDataTypeFamily = nonCharacterTypes[0].getFamily()
                            if (family is SqlTypeFamily) {
                                // The character arguments might be larger than the numeric
                                // argument. Give ourselves some headroom.
                                when (family as SqlTypeFamily) {
                                    INTEGER, NUMERIC -> nonCharacterTypes.add(
                                        cx.getTypeFactory().createSqlType(SqlTypeName.BIGINT)
                                    )
                                    else -> {}
                                }
                            }
                        }
                    }
                    cx.getTypeFactory().leastRestrictive(types)
                }
                LEAST_RESTRICTIVE -> cx.getTypeFactory().leastRestrictive(types)
                else -> null
            }
        }

        private fun convertOverlapsOperand(
            cx: SqlRexContext,
            pos: SqlParserPos, operand: SqlNode
        ): Pair<RexNode, RexNode> {
            val a0: SqlNode
            val a1: SqlNode
            when (operand.getKind()) {
                ROW -> {
                    a0 = (operand as SqlCall).operand(0)
                    val a10: SqlNode = (operand as SqlCall).operand(1)
                    val t1: RelDataType = cx.getValidator().getValidatedNodeType(a10)
                    if (SqlTypeUtil.isInterval(t1)) {
                        // make t1 = t0 + t1 when t1 is an interval.
                        a1 = plus(pos, a0, a10)
                    } else {
                        a1 = a10
                    }
                }
                else -> {
                    a0 = operand
                    a1 = operand
                }
            }
            val r0: RexNode = cx.convertExpression(a0)
            val r1: RexNode = cx.convertExpression(a1)
            return Pair.of(r0, r1)
        }

        /**
         * Casts a RexNode value to the validated type of a SqlCall. If the value
         * was already of the validated type, then the value is returned without an
         * additional cast.
         */
        fun castToValidatedType(
            node: SqlNode?, e: RexNode,
            validator: SqlValidator, rexBuilder: RexBuilder
        ): RexNode {
            val type: RelDataType = validator.getValidatedNodeType(node)
            return if (e.getType() === type) {
                e
            } else rexBuilder.makeCast(type, e)
        }
    }
}
