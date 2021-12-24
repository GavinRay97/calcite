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
package org.apache.calcite.rel.rules

import org.apache.calcite.linq4j.Ord

/**
 * Rule that reduces decimal operations (such as casts
 * or arithmetic) into operations involving more primitive types (such as longs
 * and doubles). The rule allows Calcite implementations to deal with decimals
 * in a consistent manner, while saving the effort of implementing them.
 *
 *
 * The rule can be applied to a
 * [org.apache.calcite.rel.logical.LogicalCalc] with a program for which
 * [RexUtil.requiresDecimalExpansion] returns true. The rule relies on a
 * [RexShuttle] to walk over relational expressions and replace them.
 *
 *
 * While decimals are generally not implemented by the Calcite runtime, the
 * rule is optionally applied, in order to support the situation in which we
 * would like to push down decimal operations to an external database.
 *
 * @see CoreRules.CALC_REDUCE_DECIMALS
 */
@Value.Enclosing
class ReduceDecimalsRule
/** Creates a ReduceDecimalsRule.  */
protected constructor(config: Config?) : RelRule<ReduceDecimalsRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @get:Nullable
    @get:Override
    val outConvention: Convention
        get() = Convention.NONE

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val calc: LogicalCalc = call.rel(0)

        // Expand decimals in every expression in this program. If no
        // expression changes, don't apply the rule.
        val program: RexProgram = calc.getProgram()
        if (!RexUtil.requiresDecimalExpansion(program, true)) {
            return
        }
        val rexBuilder: RexBuilder = calc.getCluster().getRexBuilder()
        val shuttle: RexShuttle = DecimalShuttle(rexBuilder)
        val programBuilder: RexProgramBuilder = RexProgramBuilder.create(
            rexBuilder,
            calc.getInput().getRowType(),
            program.getExprList(),
            program.getProjectList(),
            program.getCondition(),
            program.getOutputRowType(),
            shuttle,
            true
        )
        val newProgram: RexProgram = programBuilder.getProgram()
        val newCalc: LogicalCalc = LogicalCalc.create(calc.getInput(), newProgram)
        call.transformTo(newCalc)
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * A shuttle which converts decimal expressions to expressions based on
     * longs.
     */
    class DecimalShuttle internal constructor(rexBuilder: RexBuilder) : RexShuttle() {
        private val irreducible: Map<Pair<RexNode, String>, RexNode?>
        private val results: Map<Pair<RexNode, String>, RexNode>
        private val expanderMap: ExpanderMap

        init {
            irreducible = HashMap()
            results = HashMap()
            expanderMap = ExpanderMap(rexBuilder)
        }

        /**
         * Rewrites a call in place, from bottom up. Algorithm is as follows:
         *
         *
         *  1. visit operands
         *  1. visit call node
         *
         *
         *  1. rewrite call
         *  1. visit the rewritten call
         *
         *
         */
        @Override
        fun visitCall(call: RexCall): RexNode {
            val savedResult: RexNode? = lookup(call)
            if (savedResult != null) {
                return savedResult
            }
            var newCall: RexNode = call
            val rewrite: RexNode = rewriteCall(call)
            if (rewrite !== call) {
                newCall = rewrite.accept(this)
            }
            register(call, newCall)
            return newCall
        }

        /**
         * Registers node so it will not be computed again.
         */
        private fun register(node: RexNode, reducedNode: RexNode) {
            val key: Pair<RexNode, String> = RexUtil.makeKey(node)
            if (node === reducedNode) {
                irreducible.put(key, reducedNode)
            } else {
                results.put(key, reducedNode)
            }
        }

        /**
         * Looks up a registered node.
         */
        @Nullable
        private fun lookup(node: RexNode): RexNode? {
            val key: Pair<RexNode, String> = RexUtil.makeKey(node)
            return if (irreducible[key] != null) {
                node
            } else results[key]
        }

        /**
         * Rewrites a call, if required, or returns the original call.
         */
        private fun rewriteCall(call: RexCall): RexNode {
            val operator: SqlOperator = call.getOperator()
            if (!operator.requiresDecimalExpansion()) {
                return call
            }
            val expander = getExpander(call)
            return if (expander.canExpand(call)) {
                expander.expand(call)
            } else call
        }

        /**
         * Returns a [RexExpander] for a call.
         */
        private fun getExpander(call: RexCall): RexExpander {
            return expanderMap.getExpander(call)
        }
    }

    /**
     * Maps a RexCall to a RexExpander.
     */
    private class ExpanderMap(rexBuilder: RexBuilder) {
        private val map: Map<SqlOperator, RexExpander>
        private val defaultExpander: RexExpander

        init {
            map = HashMap()
            defaultExpander = CastArgAsDoubleExpander(rexBuilder)
            registerExpanders(map, rexBuilder)
        }

        fun getExpander(call: RexCall): RexExpander {
            val expander = map[call.getOperator()]
            return expander ?: defaultExpander
        }

        companion object {
            private fun registerExpanders(
                map: Map<SqlOperator, RexExpander>,
                rexBuilder: RexBuilder
            ) {
                val cast: RexExpander = CastExpander(rexBuilder)
                map.put(SqlStdOperatorTable.CAST, cast)
                val passThrough: RexExpander = PassThroughExpander(rexBuilder)
                map.put(SqlStdOperatorTable.UNARY_MINUS, passThrough)
                map.put(SqlStdOperatorTable.ABS, passThrough)
                map.put(SqlStdOperatorTable.IS_NULL, passThrough)
                map.put(SqlStdOperatorTable.IS_NOT_NULL, passThrough)
                val arithmetic: RexExpander = BinaryArithmeticExpander(rexBuilder)
                map.put(SqlStdOperatorTable.DIVIDE, arithmetic)
                map.put(SqlStdOperatorTable.MULTIPLY, arithmetic)
                map.put(SqlStdOperatorTable.PLUS, arithmetic)
                map.put(SqlStdOperatorTable.MINUS, arithmetic)
                map.put(SqlStdOperatorTable.MOD, arithmetic)
                map.put(SqlStdOperatorTable.EQUALS, arithmetic)
                map.put(SqlStdOperatorTable.GREATER_THAN, arithmetic)
                map.put(
                    SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    arithmetic
                )
                map.put(SqlStdOperatorTable.LESS_THAN, arithmetic)
                map.put(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, arithmetic)
                val floor: RexExpander = FloorExpander(rexBuilder)
                map.put(SqlStdOperatorTable.FLOOR, floor)
                val ceil: RexExpander = CeilExpander(rexBuilder)
                map.put(SqlStdOperatorTable.CEIL, ceil)
                val reinterpret: RexExpander = ReinterpretExpander(rexBuilder)
                map.put(SqlStdOperatorTable.REINTERPRET, reinterpret)
                val caseExpander: RexExpander = CaseExpander(rexBuilder)
                map.put(SqlStdOperatorTable.CASE, caseExpander)
            }
        }
    }

    /**
     * Rewrites a decimal expression for a specific set of SqlOperator's. In
     * general, most expressions are rewritten in such a way that SqlOperator's
     * do not have to deal with decimals. Decimals are represented by their
     * unscaled integer representations, similar to
     * [BigDecimal.unscaledValue] (i.e. 10^scale). Once decimals are
     * decoded, SqlOperators can then operate on the integer representations. The
     * value can later be recoded as a decimal.
     *
     *
     * For example, suppose one casts 2.0 as a decima(10,4). The value is
     * decoded (20), multiplied by a scale factor (1000), for a result of
     * (20000) which is encoded as a decimal(10,4), in this case 2.0000
     *
     *
     * To avoid the lengthy coding of RexNode expressions, this base class
     * provides succinct methods for building expressions used in rewrites.
     */
    abstract class RexExpander internal constructor(builder: RexBuilder) {
        /**
         * Factory for creating relational expressions.
         */
        val builder: RexBuilder

        /**
         * Type for the internal representation of decimals. This type is a
         * non-nullable type and requires extra work to make it nullable.
         */
        val int8: RelDataType

        /**
         * Type for doubles. This type is a non-nullable type and requires extra
         * work to make it nullable.
         */
        val real8: RelDataType

        /**
         * Creates a RexExpander.
         */
        init {
            this.builder = builder
            int8 = builder.getTypeFactory().createSqlType(SqlTypeName.BIGINT)
            real8 = builder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE)
        }

        /**
         * This defaults to the utility method,
         * [RexUtil.requiresDecimalExpansion] which checks
         * general guidelines on whether a rewrite should be considered at all.  In
         * general, it is helpful to update the utility method since that method is
         * often used to filter the somewhat expensive rewrite process.
         *
         *
         * However, this method provides another place for implementations of
         * RexExpander to make a more detailed analysis before deciding on
         * whether to perform a rewrite.
         */
        fun canExpand(call: RexCall?): Boolean {
            return RexUtil.requiresDecimalExpansion(call, false)
        }

        /**
         * Rewrites an expression containing decimals. Normally, this method
         * always performs a rewrite, but implementations may choose to return
         * the original expression if no change was required.
         */
        abstract fun expand(call: RexCall?): RexNode

        /**
         * Makes an exact numeric literal to be used for scaling.
         *
         * @param scale a scale from one to max precision - 1
         * @return 10^scale as an exact numeric value
         */
        protected fun makeScaleFactor(scale: Int): RexNode {
            assert(scale > 0)
            assert(
                scale
                        < builder.getTypeFactory().getTypeSystem().getMaxNumericPrecision()
            )
            return makeExactLiteral(powerOfTen(scale))
        }

        /**
         * Makes an approximate literal to be used for scaling.
         *
         * @param scale a scale from -99 to 99
         * @return 10^scale as an approximate value
         */
        protected fun makeApproxScaleFactor(scale: Int): RexNode {
            assert(-100 < scale && scale < 100) { "could not make approximate scale factor" }
            return if (scale >= 0) {
                makeApproxLiteral(BigDecimal.TEN.pow(scale))
            } else {
                val tenth: BigDecimal = BigDecimal.valueOf(1, 1)
                makeApproxLiteral(tenth.pow(-scale))
            }
        }

        /**
         * Makes an exact numeric value to be used for rounding.
         *
         * @param scale a scale from 1 to max precision - 1
         * @return 10^scale / 2 as an exact numeric value
         */
        protected fun makeRoundFactor(scale: Int): RexNode {
            assert(scale > 0)
            assert(
                scale
                        < builder.getTypeFactory().getTypeSystem().getMaxNumericPrecision()
            )
            return makeExactLiteral(powerOfTen(scale) / 2)
        }

        /**
         * Calculates a power of ten, as a long value.
         */
        protected fun powerOfTen(scale: Int): Long {
            assert(scale >= 0)
            assert(
                scale
                        < builder.getTypeFactory().getTypeSystem().getMaxNumericPrecision()
            )
            return BigInteger.TEN.pow(scale).longValue()
        }

        /**
         * Makes an exact, non-nullable literal of Bigint type.
         */
        protected fun makeExactLiteral(l: Long): RexNode {
            val bd: BigDecimal = BigDecimal.valueOf(l)
            return builder.makeExactLiteral(bd, int8)
        }

        /**
         * Makes an approximate literal of double precision.
         */
        protected fun makeApproxLiteral(bd: BigDecimal?): RexNode {
            return builder.makeApproxLiteral(bd)
        }

        /**
         * Scales up a decimal value and returns the scaled value as an exact
         * number.
         *
         * @param value the integer representation of a decimal
         * @param scale a value from zero to max precision - 1
         * @return value * 10^scale as an exact numeric value
         */
        protected fun scaleUp(value: RexNode, scale: Int): RexNode {
            assert(scale >= 0)
            assert(
                scale
                        < builder.getTypeFactory().getTypeSystem().getMaxNumericPrecision()
            )
            return if (scale == 0) {
                value
            } else builder.makeCall(
                SqlStdOperatorTable.MULTIPLY,
                value,
                makeScaleFactor(scale)
            )
        }

        /**
         * Scales down a decimal value, and returns the scaled value as an exact
         * numeric. with the rounding convention
         * [BigDecimal.ROUND_HALF_UP]. (Values midway
         * between two points are rounded away from zero.)
         *
         * @param value the integer representation of a decimal
         * @param scale a value from zero to max precision
         * @return value/10^scale, rounded away from zero and returned as an
         * exact numeric value
         */
        protected fun scaleDown(value: RexNode, scale: Int): RexNode {
            val maxPrecision: Int = builder.getTypeFactory().getTypeSystem().getMaxNumericPrecision()
            assert(scale >= 0 && scale <= maxPrecision)
            if (scale == 0) {
                return value
            }
            if (scale == maxPrecision) {
                val half: Long = BigInteger.TEN.pow(scale - 1).longValue() * 5
                return makeCase(
                    builder.makeCall(
                        SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                        value,
                        makeExactLiteral(half)
                    ),
                    makeExactLiteral(1),
                    builder.makeCall(
                        SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                        value,
                        makeExactLiteral(-half)
                    ),
                    makeExactLiteral(-1),
                    makeExactLiteral(0)
                )
            }
            val roundFactor: RexNode = makeRoundFactor(scale)
            val roundValue: RexNode = makeCase(
                builder.makeCall(
                    SqlStdOperatorTable.GREATER_THAN,
                    value,
                    makeExactLiteral(0)
                ),
                makePlus(value, roundFactor),
                makeMinus(value, roundFactor)
            )
            return makeDivide(
                roundValue,
                makeScaleFactor(scale)
            )
        }

        /**
         * Scales down a decimal value and returns the scaled value as a an
         * double precision approximate value. Scaling is implemented with
         * double precision arithmetic.
         *
         * @param value the integer representation of a decimal
         * @param scale a value from zero to max precision
         * @return value/10^scale as a double precision value
         */
        protected fun scaleDownDouble(value: RexNode?, scale: Int): RexNode {
            assert(scale >= 0)
            assert(
                scale
                        <= builder.getTypeFactory().getTypeSystem().getMaxNumericPrecision()
            )
            val cast: RexNode = ensureType(real8, value)
            return if (scale == 0) {
                cast
            } else makeDivide(
                cast,
                makeApproxScaleFactor(scale)
            )
        }

        /**
         * Ensures a value is of a required scale. If it is not, then the value
         * is multiplied by a scale factor. Scaling up an exact value is limited
         * to max precision - 1, because we cannot represent the result of
         * larger scales internally. Scaling up a floating point value is more
         * flexible since the value may be very small despite having a scale of
         * zero and the scaling may still produce a reasonable result
         *
         * @param value    integer representation of decimal, or a floating point
         * number
         * @param scale    current scale, 0 for floating point numbers
         * @param required required scale, must be at least the current scale;
         * the scale difference may not be greater than max
         * precision - 1 for exact numerics
         * @return value * 10^scale, returned as an exact or approximate value
         * corresponding to the input value
         */
        protected fun ensureScale(value: RexNode, scale: Int, required: Int): RexNode {
            val typeSystem: RelDataTypeSystem = builder.getTypeFactory().getTypeSystem()
            val maxPrecision: Int = typeSystem.getMaxNumericPrecision()
            assert(scale <= maxPrecision && required <= maxPrecision)
            assert(required >= scale)
            if (scale == required) {
                return value
            }
            val scaleDiff = required - scale
            if (SqlTypeUtil.isApproximateNumeric(value.getType())) {
                return makeMultiply(
                    value,
                    makeApproxScaleFactor(scaleDiff)
                )
            }

            // TODO: make a validator exception for this
            if (scaleDiff >= maxPrecision) {
                throw Util.needToImplement(
                    "Source type with scale " + scale
                            + " cannot be converted to target type with scale "
                            + required + " because the smallest value of the "
                            + "source type is too large to be encoded by the "
                            + "target type"
                )
            }
            return scaleUp(value, scaleDiff)
        }

        /**
         * Retrieves a decimal node's integer representation.
         *
         * @param decimalNode the decimal value as an opaque type
         * @return an integer representation of the decimal value
         */
        protected fun decodeValue(decimalNode: RexNode): RexNode {
            assert(SqlTypeUtil.isDecimal(decimalNode.getType()))
            return builder.decodeIntervalOrDecimal(decimalNode)
        }

        /**
         * Retrieves the primitive value of a numeric node. If the node is a
         * decimal, then it must first be decoded. Otherwise the original node
         * may be returned.
         *
         * @param node a numeric node, possibly a decimal
         * @return the primitive value of the numeric node
         */
        protected fun accessValue(node: RexNode): RexNode {
            assert(SqlTypeUtil.isNumeric(node.getType()))
            return if (SqlTypeUtil.isDecimal(node.getType())) {
                decodeValue(node)
            } else node
        }

        /**
         * Casts a decimal's integer representation to a decimal node. If the
         * expression is not the expected integer type, then it is casted first.
         *
         *
         * This method does not request an overflow check.
         *
         * @param value       integer representation of decimal
         * @param decimalType type integer will be reinterpreted as
         * @return the integer representation reinterpreted as a decimal type
         */
        protected fun encodeValue(value: RexNode?, decimalType: RelDataType?): RexNode {
            return encodeValue(value, decimalType, false)
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
         * @param decimalType   type integer will be reinterpreted as
         * @param checkOverflow indicates whether an overflow check is required
         * when reinterpreting this particular value as the
         * decimal type. A check usually not required for
         * arithmetic, but is often required for rounding and
         * explicit casts.
         * @return the integer reinterpreted as an opaque decimal type
         */
        protected fun encodeValue(
            value: RexNode?,
            decimalType: RelDataType?,
            checkOverflow: Boolean
        ): RexNode {
            return builder.encodeIntervalOrDecimal(
                value, decimalType, checkOverflow
            )
        }

        /**
         * Ensures expression is interpreted as a specified type. The returned
         * expression may be wrapped with a cast.
         *
         *
         * This method corrects the nullability of the specified type to
         * match the nullability of the expression.
         *
         * @param type desired type
         * @param node expression
         * @return a casted expression or the original expression
         */
        protected fun ensureType(type: RelDataType?, node: RexNode?): RexNode {
            return ensureType(type, node, true)
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
        protected fun ensureType(
            type: RelDataType?,
            node: RexNode?,
            matchNullability: Boolean
        ): RexNode {
            return builder.ensureType(type, node, matchNullability)
        }

        protected fun makeCase(
            condition: RexNode?,
            thenClause: RexNode?,
            elseClause: RexNode?
        ): RexNode {
            return builder.makeCall(
                SqlStdOperatorTable.CASE,
                condition,
                thenClause,
                elseClause
            )
        }

        protected fun makeCase(
            whenA: RexNode?,
            thenA: RexNode?,
            whenB: RexNode?,
            thenB: RexNode?,
            elseClause: RexNode?
        ): RexNode {
            return builder.makeCall(
                SqlStdOperatorTable.CASE,
                whenA,
                thenA,
                whenB,
                thenB,
                elseClause
            )
        }

        protected fun makePlus(
            a: RexNode?,
            b: RexNode?
        ): RexNode {
            return builder.makeCall(
                SqlStdOperatorTable.PLUS,
                a,
                b
            )
        }

        protected fun makeMinus(
            a: RexNode?,
            b: RexNode?
        ): RexNode {
            return builder.makeCall(
                SqlStdOperatorTable.MINUS,
                a,
                b
            )
        }

        protected fun makeDivide(
            a: RexNode?,
            b: RexNode?
        ): RexNode {
            return builder.makeCall(
                SqlStdOperatorTable.DIVIDE_INTEGER,
                a,
                b
            )
        }

        protected fun makeMultiply(
            a: RexNode?,
            b: RexNode?
        ): RexNode {
            return builder.makeCall(
                SqlStdOperatorTable.MULTIPLY,
                a,
                b
            )
        }

        protected fun makeIsPositive(
            a: RexNode?
        ): RexNode {
            return builder.makeCall(
                SqlStdOperatorTable.GREATER_THAN,
                a,
                makeExactLiteral(0)
            )
        }

        protected fun makeIsNegative(
            a: RexNode?
        ): RexNode {
            return builder.makeCall(
                SqlStdOperatorTable.LESS_THAN,
                a,
                makeExactLiteral(0)
            )
        }
    }

    /**
     * Expands a decimal cast expression.
     */
    private class CastExpander(builder: RexBuilder) : RexExpander(builder) {
        // implement RexExpander
        @Override
        override fun expand(call: RexCall): RexNode {
            val operands: List<RexNode> = call.operands
            assert(call.isA(SqlKind.CAST))
            assert(operands.size() === 1)
            assert(!RexLiteral.isNullLiteral(operands[0]))
            val operand: RexNode = operands[0]
            val fromType: RelDataType = operand.getType()
            val toType: RelDataType = call.getType()
            assert(
                SqlTypeUtil.isDecimal(fromType)
                        || SqlTypeUtil.isDecimal(toType)
            )
            if (SqlTypeUtil.isIntType(toType)) {
                // decimal to int
                return ensureType(
                    toType,
                    scaleDown(
                        decodeValue(operand),
                        fromType.getScale()
                    ),
                    false
                )
            } else if (SqlTypeUtil.isApproximateNumeric(toType)) {
                // decimal to floating point
                return ensureType(
                    toType,
                    scaleDownDouble(
                        decodeValue(operand),
                        fromType.getScale()
                    ),
                    false
                )
            } else if (SqlTypeUtil.isApproximateNumeric(fromType)) {
                // real to decimal
                return encodeValue(
                    ensureScale(
                        operand,
                        0,
                        toType.getScale()
                    ),
                    toType,
                    true
                )
            }
            if (!SqlTypeUtil.isExactNumeric(fromType)
                || !SqlTypeUtil.isExactNumeric(toType)
            ) {
                throw Util.needToImplement(
                    "Cast from '" + fromType.toString()
                        .toString() + "' to '" + toType.toString().toString() + "'"
                )
            }
            val fromScale: Int = fromType.getScale()
            val toScale: Int = toType.getScale()
            val fromDigits: Int = fromType.getPrecision() - fromScale
            val toDigits: Int = toType.getPrecision() - toScale

            // NOTE: precision 19 overflows when its underlying
            // bigint representation overflows
            var checkOverflow = toType.getPrecision() < 19 && toDigits < fromDigits
            return if (SqlTypeUtil.isIntType(fromType)) {
                // int to decimal
                encodeValue(
                    ensureScale(
                        operand,
                        0,
                        toType.getScale()
                    ),
                    toType,
                    checkOverflow
                )
            } else if (SqlTypeUtil.isDecimal(fromType)
                && SqlTypeUtil.isDecimal(toType)
            ) {
                // decimal to decimal
                val value: RexNode = decodeValue(operand)
                val scaled: RexNode
                if (fromScale <= toScale) {
                    scaled = ensureScale(value, fromScale, toScale)
                } else {
                    if (toDigits == fromDigits) {
                        // rounding away from zero may cause an overflow
                        // for example: cast(9.99 as decimal(2,1))
                        checkOverflow = true
                    }
                    scaled = scaleDown(value, fromScale - toScale)
                }
                encodeValue(scaled, toType, checkOverflow)
            } else {
                throw Util.needToImplement(
                    "Reduce decimal cast from $fromType to $toType"
                )
            }
        }
    }

    /**
     * Expands a decimal arithmetic expression.
     */
    private class BinaryArithmeticExpander(builder: RexBuilder) : RexExpander(builder) {
        @MonotonicNonNull
        var typeA: RelDataType? = null

        @MonotonicNonNull
        var typeB: RelDataType? = null
        var scaleA = 0
        var scaleB = 0
        @Override
        override fun expand(call: RexCall): RexNode {
            val operands: List<RexNode> = call.operands
            assert(operands.size() === 2)
            val typeA: RelDataType = operands[0].getType()
            val typeB: RelDataType = operands[1].getType()
            assert(
                SqlTypeUtil.isNumeric(typeA)
                        && SqlTypeUtil.isNumeric(typeB)
            )
            if (SqlTypeUtil.isApproximateNumeric(typeA)
                || SqlTypeUtil.isApproximateNumeric(typeB)
            ) {
                val newOperands: List<RexNode>
                newOperands = if (SqlTypeUtil.isApproximateNumeric(typeA)) {
                    ImmutableList.of(
                        operands[0],
                        ensureType(real8, operands[1])
                    )
                } else {
                    ImmutableList.of(
                        ensureType(real8, operands[0]),
                        operands[1]
                    )
                }
                return builder.makeCall(
                    call.getOperator(),
                    newOperands
                )
            }
            analyzeOperands(operands)
            return if (call.isA(SqlKind.PLUS)) {
                expandPlusMinus(call, operands)
            } else if (call.isA(SqlKind.MINUS)) {
                expandPlusMinus(call, operands)
            } else if (call.isA(SqlKind.DIVIDE)) {
                expandDivide(call, operands)
            } else if (call.isA(SqlKind.TIMES)) {
                expandTimes(call, operands)
            } else if (call.isA(SqlKind.COMPARISON)) {
                expandComparison(call, operands)
            } else if (call.getOperator() === SqlStdOperatorTable.MOD) {
                expandMod(call, operands)
            } else {
                throw AssertionError(
                    "ReduceDecimalsRule could not expand "
                            + call.getOperator()
                )
            }
        }

        /**
         * Convenience method for reading characteristics of operands (such as
         * scale, precision, whole digits) into an ArithmeticExpander. The
         * operands are restricted by the following contraints:
         *
         *
         *  * there are exactly two operands
         *  * both are exact numeric types
         *
         */
        private fun analyzeOperands(operands: List<RexNode>) {
            assert(operands.size() === 2)
            typeA = operands[0].getType()
            typeB = operands[1].getType()
            assert(
                SqlTypeUtil.isExactNumeric(typeA)
                        && SqlTypeUtil.isExactNumeric(typeB)
            )
            scaleA = typeA.getScale()
            scaleB = typeB.getScale()
        }

        private fun expandPlusMinus(call: RexCall, operands: List<RexNode>): RexNode {
            val outType: RelDataType = call.getType()
            val outScale: Int = outType.getScale()
            return encodeValue(
                builder.makeCall(
                    call.getOperator(),
                    ensureScale(
                        accessValue(operands[0]),
                        scaleA,
                        outScale
                    ),
                    ensureScale(
                        accessValue(operands[1]),
                        scaleB,
                        outScale
                    )
                ),
                outType
            )
        }

        private fun expandDivide(call: RexCall, operands: List<RexNode>): RexNode {
            val outType: RelDataType = call.getType()
            val dividend: RexNode = builder.makeCall(
                call.getOperator(),
                ensureType(
                    real8,
                    accessValue(operands[0])
                ),
                ensureType(
                    real8,
                    accessValue(operands[1])
                )
            )
            val scaleDifference: Int = outType.getScale() - scaleA + scaleB
            val rescale: RexNode = builder.makeCall(
                SqlStdOperatorTable.MULTIPLY,
                dividend,
                makeApproxScaleFactor(scaleDifference)
            )
            return encodeValue(rescale, outType)
        }

        private fun expandTimes(call: RexCall, operands: List<RexNode>): RexNode {
            // Multiplying the internal values of the two arguments leads to
            // a number with scale = scaleA + scaleB. If the result type has
            // a lower scale, then the number should be scaled down.
            val divisor: Int = scaleA + scaleB - call.getType().getScale()
            return if (builder.getTypeFactory().getTypeSystem().shouldUseDoubleMultiplication(
                    builder.getTypeFactory(),
                    requireNonNull(typeA, "typeA"),
                    requireNonNull(typeB, "typeB")
                )
            ) {
                // Approximate implementation:
                // cast (a as double) * cast (b as double)
                //     / 10^divisor
                val division: RexNode = makeDivide(
                    makeMultiply(
                        ensureType(real8, accessValue(operands[0])),
                        ensureType(real8, accessValue(operands[1]))
                    ),
                    makeApproxLiteral(BigDecimal.TEN.pow(divisor))
                )
                encodeValue(division, call.getType(), true)
            } else {
                // Exact implementation: scaleDown(a * b)
                encodeValue(
                    scaleDown(
                        builder.makeCall(
                            call.getOperator(),
                            accessValue(operands[0]),
                            accessValue(operands[1])
                        ),
                        divisor
                    ),
                    call.getType()
                )
            }
        }

        private fun expandComparison(call: RexCall, operands: List<RexNode>): RexNode {
            val commonScale: Int = Math.max(scaleA, scaleB)
            return builder.makeCall(
                call.getOperator(),
                ensureScale(
                    accessValue(operands[0]),
                    scaleA,
                    commonScale
                ),
                ensureScale(
                    accessValue(operands[1]),
                    scaleB,
                    commonScale
                )
            )
        }

        private fun expandMod(call: RexCall, operands: List<RexNode>): RexNode {
            assert(SqlTypeUtil.isExactNumeric(requireNonNull(typeA, "typeA")))
            assert(SqlTypeUtil.isExactNumeric(requireNonNull(typeB, "typeB")))
            if (scaleA != 0 || scaleB != 0) {
                throw RESOURCE.argumentMustHaveScaleZero(call.getOperator().getName())
                    .ex()
            }
            val result: RexNode = builder.makeCall(
                call.getOperator(),
                accessValue(operands[0]),
                accessValue(operands[1])
            )
            val retType: RelDataType = call.getType()
            return if (SqlTypeUtil.isDecimal(retType)) {
                encodeValue(result, retType)
            } else ensureType(
                call.getType(),
                result
            )
        }
    }

    /**
     * Expander that rewrites `FLOOR(DECIMAL)` expressions.
     * Rewrite is as follows:
     *
     * <blockquote><pre>
     * if (value &lt; 0)
     * (value - 0.99...) / (10^scale)
     * else
     * value / (10 ^ scale)
    </pre></blockquote> *
     */
    private class FloorExpander(rexBuilder: RexBuilder) : RexExpander(rexBuilder) {
        @Override
        override fun expand(call: RexCall): RexNode {
            assert(call.getOperator() === SqlStdOperatorTable.FLOOR)
            val decValue: RexNode = call.operands.get(0)
            val scale: Int = decValue.getType().getScale()
            val value: RexNode = decodeValue(decValue)
            val typeSystem: RelDataTypeSystem = builder.getTypeFactory().getTypeSystem()
            val rewrite: RexNode
            rewrite = if (scale == 0) {
                decValue
            } else if (scale == typeSystem.getMaxNumericPrecision()) {
                makeCase(
                    makeIsNegative(value),
                    makeExactLiteral(-1),
                    makeExactLiteral(0)
                )
            } else {
                val round: RexNode = makeExactLiteral(1 - powerOfTen(scale))
                val scaleFactor: RexNode = makeScaleFactor(scale)
                makeCase(
                    makeIsNegative(value),
                    makeDivide(
                        makePlus(value, round),
                        scaleFactor
                    ),
                    makeDivide(value, scaleFactor)
                )
            }
            return encodeValue(
                rewrite,
                call.getType()
            )
        }
    }

    /**
     * Expander that rewrites `CEILING(DECIMAL)` expressions.
     * Rewrite is as follows:
     *
     * <blockquote><pre>
     * if (value &gt; 0)
     * (value + 0.99...) / (10 ^ scale)
     * else
     * value / (10 ^ scale)
    </pre></blockquote> *
     */
    private class CeilExpander(rexBuilder: RexBuilder) : RexExpander(rexBuilder) {
        @Override
        override fun expand(call: RexCall): RexNode {
            assert(call.getOperator() === SqlStdOperatorTable.CEIL)
            val decValue: RexNode = call.operands.get(0)
            val scale: Int = decValue.getType().getScale()
            val value: RexNode = decodeValue(decValue)
            val typeSystem: RelDataTypeSystem = builder.getTypeFactory().getTypeSystem()
            val rewrite: RexNode
            rewrite = if (scale == 0) {
                decValue
            } else if (scale == typeSystem.getMaxNumericPrecision()) {
                makeCase(
                    makeIsPositive(value),
                    makeExactLiteral(1),
                    makeExactLiteral(0)
                )
            } else {
                val round: RexNode = makeExactLiteral(powerOfTen(scale) - 1)
                val scaleFactor: RexNode = makeScaleFactor(scale)
                makeCase(
                    makeIsPositive(value),
                    makeDivide(
                        makePlus(value, round),
                        scaleFactor
                    ),
                    makeDivide(value, scaleFactor)
                )
            }
            return encodeValue(
                rewrite,
                call.getType()
            )
        }
    }

    /**
     * Expander that rewrites case expressions, in place. Starting from:
     *
     * <blockquote><pre>(when $cond then $val)+ else $default</pre></blockquote>
     *
     *
     * this expander casts all values to the return type. If the target type is
     * a decimal, then the values are then decoded. The result of expansion is
     * that the case operator no longer deals with decimals args. (The return
     * value is encoded if necessary.)
     *
     *
     * Note: a decimal type is returned iff arguments have decimals.
     */
    private class CaseExpander(rexBuilder: RexBuilder) : RexExpander(rexBuilder) {
        @Override
        override fun expand(call: RexCall): RexNode {
            val retType: RelDataType = call.getType()
            val argCount: Int = call.operands.size()
            val opBuilder: ImmutableList.Builder<RexNode> = ImmutableList.builder()
            for (i in 0 until argCount) {
                // skip case conditions
                if (i % 2 == 0 && i != argCount - 1) {
                    opBuilder.add(call.operands.get(i))
                    continue
                }
                var expr: RexNode = ensureType(retType, call.operands.get(i), false)
                if (SqlTypeUtil.isDecimal(retType)) {
                    expr = decodeValue(expr)
                }
                opBuilder.add(expr)
            }
            var newCall: RexNode = builder.makeCall(retType, call.getOperator(), opBuilder.build())
            if (SqlTypeUtil.isDecimal(retType)) {
                newCall = encodeValue(newCall, retType)
            }
            return newCall
        }
    }

    /**
     * An expander that substitutes decimals with their integer representations.
     * If the output is decimal, the output is reinterpreted from the integer
     * representation into a decimal.
     */
    private class PassThroughExpander(builder: RexBuilder) : RexExpander(builder) {
        @Override
        override fun canExpand(call: RexCall?): Boolean {
            return RexUtil.requiresDecimalExpansion(call, false)
        }

        @Override
        override fun expand(call: RexCall): RexNode {
            val opBuilder: ImmutableList.Builder<RexNode> = ImmutableList.builder()
            for (operand in call.operands) {
                if (SqlTypeUtil.isNumeric(operand.getType())) {
                    opBuilder.add(accessValue(operand))
                } else {
                    opBuilder.add(operand)
                }
            }
            val newCall: RexNode = builder.makeCall(
                call.getType(), call.getOperator(),
                opBuilder.build()
            )
            return if (SqlTypeUtil.isDecimal(call.getType())) {
                encodeValue(
                    newCall,
                    call.getType()
                )
            } else {
                newCall
            }
        }
    }

    /**
     * Expander that casts DECIMAL arguments as DOUBLE.
     */
    private class CastArgAsDoubleExpander(builder: RexBuilder) : CastArgAsTypeExpander(builder) {
        @Override
        override fun getArgType(call: RexCall, ordinal: Int): RelDataType {
            var type: RelDataType = real8
            if (call.operands.get(ordinal).getType().isNullable()) {
                type = builder.getTypeFactory().createTypeWithNullability(
                    type,
                    true
                )
            }
            return type
        }
    }

    /**
     * Expander that casts DECIMAL arguments as another type.
     */
    private abstract class CastArgAsTypeExpander private constructor(builder: RexBuilder) : RexExpander(builder) {
        abstract fun getArgType(call: RexCall?, ordinal: Int): RelDataType
        @Override
        override fun expand(call: RexCall): RexNode {
            val opBuilder: ImmutableList.Builder<RexNode> = ImmutableList.builder()
            for (operand in Ord.zip(call.operands)) {
                val targetType: RelDataType = getArgType(call, operand.i)
                if (SqlTypeUtil.isDecimal(operand.e.getType())) {
                    opBuilder.add(ensureType(targetType, operand.e, true))
                } else {
                    opBuilder.add(operand.e)
                }
            }
            var ret: RexNode = builder.makeCall(
                call.getType(),
                call.getOperator(),
                opBuilder.build()
            )
            ret = ensureType(
                call.getType(),
                ret,
                true
            )
            return ret
        }
    }

    /**
     * An expander that simplifies reinterpret calls.
     *
     *
     * Consider (1.0+1)*1. The inner
     * operation encodes a decimal (Reinterpret(...)) which the outer operation
     * immediately decodes: (Reinterpret(Reinterpret(...))). Arithmetic overflow
     * is handled by underlying integer operations, so we don't have to consider
     * it. Simply remove the nested Reinterpret.
     */
    private class ReinterpretExpander(builder: RexBuilder) : RexExpander(builder) {
        @Override
        override fun canExpand(call: RexCall): Boolean {
            return (call.isA(SqlKind.REINTERPRET)
                    && call.operands.get(0).isA(SqlKind.REINTERPRET))
        }

        @Override
        override fun expand(call: RexCall): RexNode {
            val operands: List<RexNode> = call.operands
            val subCall: RexCall = operands[0] as RexCall
            val innerValue: RexNode = subCall.operands.get(0)
            return if (canSimplify(
                    call,
                    subCall,
                    innerValue
                )
            ) {
                innerValue
            } else call
        }

        companion object {
            /**
             * Detect, in a generic, but strict way, whether it is possible to
             * simplify a reinterpret cast. The rules are as follows:
             *
             *
             *  1. If value is not the same basic type as outer, then we cannot
             * simplify
             *  1. If the value is nullable but the inner or outer are not, then we
             * cannot simplify.
             *  1. If inner is nullable but outer is not, we cannot simplify.
             *  1. If an overflow check is required from either inner or outer, we
             * cannot simplify.
             *  1. Otherwise, given the same type, and sufficient nullability
             * constraints, we can simplify.
             *
             *
             * @param outer outer call to reinterpret
             * @param inner inner call to reinterpret
             * @param value inner value
             * @return whether the two reinterpret casts can be removed
             */
            private fun canSimplify(
                outer: RexCall,
                inner: RexCall,
                value: RexNode
            ): Boolean {
                val outerType: RelDataType = outer.getType()
                val innerType: RelDataType = inner.getType()
                val valueType: RelDataType = value.getType()
                val outerCheck: Boolean = RexUtil.canReinterpretOverflow(outer)
                val innerCheck: Boolean = RexUtil.canReinterpretOverflow(inner)
                if (outerType.getSqlTypeName() !== valueType.getSqlTypeName()
                    || outerType.getPrecision() !== valueType.getPrecision()
                    || outerType.getScale() !== valueType.getScale()
                ) {
                    return false
                }
                if (valueType.isNullable()
                    && (!innerType.isNullable() || !outerType.isNullable())
                ) {
                    return false
                }
                if (innerType.isNullable() && !outerType.isNullable()) {
                    return false
                }

                // One would think that we could go from Nullable -> Not Nullable
                // since we are substituting a general type with a more specific
                // type. However the optimizer doesn't like it.
                if (valueType.isNullable() !== outerType.isNullable()) {
                    return false
                }
                return if (innerCheck || outerCheck) {
                    false
                } else true
            }
        }
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): ReduceDecimalsRule? {
            return ReduceDecimalsRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableReduceDecimalsRule.Config.of()
                .withOperandSupplier { b -> b.operand(LogicalCalc::class.java).anyInputs() }
        }
    }
}
