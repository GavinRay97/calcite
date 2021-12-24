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

import org.apache.calcite.linq4j.Ord

/**
 * This class allows multiple existing [SqlOperandTypeChecker] rules to be
 * combined into one rule. For example, allowing an operand to be either string
 * or numeric could be done by:
 *
 * <blockquote>
 * <pre>`
 * CompositeOperandsTypeChecking newCompositeRule =
 * new CompositeOperandsTypeChecking(Composition.OR,
 * new SqlOperandTypeChecker[]{stringRule, numericRule});
`</pre> *
</blockquote> *
 *
 *
 * Similarly a rule that would only allow a numeric literal can be done by:
 *
 * <blockquote>
 * <pre>`
 * CompositeOperandsTypeChecking newCompositeRule =
 * new CompositeOperandsTypeChecking(Composition.AND,
 * new SqlOperandTypeChecker[]{numericRule, literalRule});
`</pre> *
</blockquote> *
 *
 *
 * Finally, creating a signature expecting a string for the first operand and
 * a numeric for the second operand can be done by:
 *
 * <blockquote>
 * <pre>`
 * CompositeOperandsTypeChecking newCompositeRule =
 * new CompositeOperandsTypeChecking(Composition.SEQUENCE,
 * new SqlOperandTypeChecker[]{stringRule, numericRule});
`</pre> *
</blockquote> *
 *
 *
 * For SEQUENCE composition, the rules must be instances of
 * SqlSingleOperandTypeChecker, and signature generation is not supported. For
 * AND composition, only the first rule is used for signature generation.
 */
class CompositeOperandTypeChecker internal constructor(
    composition: Composition,
    allowedRules: ImmutableList<out SqlOperandTypeChecker?>,
    @Nullable allowedSignatures: String?,
    @Nullable range: SqlOperandCountRange?
) : SqlOperandTypeChecker {
    @Nullable
    private val range: SqlOperandCountRange?
    //~ Enums ------------------------------------------------------------------
    /** How operands are composed.  */
    enum class Composition {
        AND, OR, SEQUENCE, REPEAT
    }

    //~ Instance fields --------------------------------------------------------
    // It is not clear if @UnknownKeyFor is needed here or not, however, checkerframework inference
    // fails otherwise, see https://github.com/typetools/checker-framework/issues/4048
    protected val allowedRules: ImmutableList<out SqlOperandTypeChecker?>
    protected val composition: Composition

    @Nullable
    private val allowedSignatures: String?
    //~ Constructors -----------------------------------------------------------
    /**
     * Package private. Use [OperandTypes.and],
     * [OperandTypes.or].
     */
    init {
        this.allowedRules = requireNonNull(allowedRules, "allowedRules")
        this.composition = requireNonNull(composition, "composition")
        this.allowedSignatures = allowedSignatures
        this.range = range
        assert(range != null == (composition == Composition.REPEAT))
        assert(allowedRules.size() + (if (range == null) 0 else 1) > 1)
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun isOptional(i: Int): Boolean {
        for (allowedRule in allowedRules) {
            if (allowedRule.isOptional(i)) {
                return true
            }
        }
        return false
    }

    val rules: ImmutableList<out SqlOperandTypeChecker?>
        get() = allowedRules

    @get:Override
    override val consistency: org.apache.calcite.sql.type.SqlOperandTypeChecker.Consistency?
        get() = Consistency.NONE

    @Override
    override fun getAllowedSignatures(op: SqlOperator?, opName: String?): String {
        if (allowedSignatures != null) {
            return allowedSignatures
        }
        if (composition == Composition.SEQUENCE) {
            throw AssertionError(
                "specify allowedSignatures or override getAllowedSignatures"
            )
        }
        val ret = StringBuilder()
        for (ord in Ord.< SqlOperandTypeChecker > zip < SqlOperandTypeChecker ? > allowedRules) {
            if (ord.i > 0) {
                ret.append(SqlOperator.NL)
            }
            ret.append(ord.e.getAllowedSignatures(op, opName))
            if (composition == Composition.AND) {
                break
            }
        }
        return ret.toString()
    }

    // Composite is not a simple range. Can't simplify,
    // so return the composite.
    @get:Override
    override val operandCountRange: SqlOperandCountRange
        get() = when (composition) {
            Composition.REPEAT -> requireNonNull(range, "range")
            Composition.SEQUENCE -> SqlOperandCountRanges.of(allowedRules.size())
            Composition.AND, Composition.OR -> {
                val ranges: List<SqlOperandCountRange> = object : AbstractList<SqlOperandCountRange?>() {
                    @Override
                    operator fun get(index: Int): SqlOperandCountRange {
                        return allowedRules.get(index).getOperandCountRange()
                    }

                    @Override
                    fun size(): Int {
                        return allowedRules.size()
                    }
                }
                val min = minMin(ranges)
                val max = maxMax(ranges)
                val composite: SqlOperandCountRange = object : SqlOperandCountRange() {
                    @Override
                    fun isValidCount(count: Int): Boolean {
                        return when (composition) {
                            Composition.AND -> {
                                for (range in ranges) {
                                    if (!range.isValidCount(count)) {
                                        return false
                                    }
                                }
                                true
                            }
                            Composition.OR -> {
                                for (range in ranges) {
                                    if (range.isValidCount(count)) {
                                        return true
                                    }
                                }
                                false
                            }
                            else -> {
                                for (range in ranges) {
                                    if (range.isValidCount(count)) {
                                        return true
                                    }
                                }
                                false
                            }
                        }
                    }

                    @get:Override
                    val min: Int
                        get() = min

                    @get:Override
                    val max: Int
                        get() = max
                }
                if (max >= 0) {
                    var i = min
                    while (i <= max) {
                        if (!composite.isValidCount(i)) {
                            // Composite is not a simple range. Can't simplify,
                            // so return the composite.
                            return composite
                        }
                        i++
                    }
                }
                if (min == max) SqlOperandCountRanges.of(min) else SqlOperandCountRanges.between(min, max)
            }
            else -> {
                val ranges: List<SqlOperandCountRange> = object : AbstractList<SqlOperandCountRange?>() {
                    @Override
                    operator fun get(index: Int): SqlOperandCountRange {
                        return allowedRules.get(index).getOperandCountRange()
                    }

                    @Override
                    fun size(): Int {
                        return allowedRules.size()
                    }
                }
                val min = minMin(ranges)
                val max = maxMax(ranges)
                val composite: SqlOperandCountRange = object : SqlOperandCountRange() {
                    @Override
                    fun isValidCount(count: Int): Boolean {
                        return when (composition) {
                            Composition.AND -> {
                                for (range in ranges) {
                                    if (!range.isValidCount(count)) {
                                        return false
                                    }
                                }
                                true
                            }
                            Composition.OR -> {
                                for (range in ranges) {
                                    if (range.isValidCount(count)) {
                                        return true
                                    }
                                }
                                false
                            }
                            else -> {
                                for (range in ranges) {
                                    if (range.isValidCount(count)) {
                                        return true
                                    }
                                }
                                false
                            }
                        }
                    }

                    @get:Override
                    val min: Int
                        get() = min

                    @get:Override
                    val max: Int
                        get() = max
                }
                if (max >= 0) {
                    var i = min
                    while (i <= max) {
                        if (!composite.isValidCount(i)) {
                            return composite
                        }
                        i++
                    }
                }
                if (min == max) SqlOperandCountRanges.of(min) else SqlOperandCountRanges.between(min, max)
            }
        }

    private fun maxMax(ranges: List<SqlOperandCountRange>): Int {
        var max: Int = Integer.MIN_VALUE
        for (range in ranges) {
            if (range.getMax() < 0) {
                if (composition == Composition.OR) {
                    return -1
                }
            } else {
                max = Math.max(max, range.getMax())
            }
        }
        return max
    }

    @Override
    override fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        // 1. Check eagerly for binary arithmetic expressions.
        // 2. Check the comparability.
        // 3. Check if the operands have the right type.
        if (callBinding.isTypeCoercionEnabled()) {
            val typeCoercion: TypeCoercion = callBinding.getValidator().getTypeCoercion()
            typeCoercion.binaryArithmeticCoercion(callBinding)
        }
        if (check(callBinding)) {
            return true
        }
        if (!throwOnFailure) {
            return false
        }
        if (composition == Composition.OR) {
            for (allowedRule in allowedRules) {
                allowedRule.checkOperandTypes(callBinding, true)
            }
        }
        throw callBinding.newValidationSignatureError()
    }

    private fun check(callBinding: SqlCallBinding): Boolean {
        return when (composition) {
            Composition.REPEAT -> {
                if (!requireNonNull(range, "range").isValidCount(callBinding.getOperandCount())) {
                    return false
                }
                for (operand in Util.range(callBinding.getOperandCount())) {
                    for (rule in allowedRules) {
                        if (!(rule as SqlSingleOperandTypeChecker).checkSingleOperandType(
                                callBinding,
                                callBinding.getCall().operand(operand),
                                0,
                                false
                            )
                        ) {
                            return if (callBinding.isTypeCoercionEnabled()) {
                                coerceOperands(callBinding, true)
                            } else false
                        }
                    }
                }
                true
            }
            Composition.SEQUENCE -> {
                if (callBinding.getOperandCount() !== allowedRules.size()) {
                    return false
                }
                for (ord in Ord.< SqlOperandTypeChecker > zip < SqlOperandTypeChecker ? > allowedRules) {
                    val rule: SqlOperandTypeChecker = ord.e
                    if (!(rule as SqlSingleOperandTypeChecker).checkSingleOperandType(
                            callBinding,
                            callBinding.getCall().operand(ord.i),
                            0,
                            false
                        )
                    ) {
                        return if (callBinding.isTypeCoercionEnabled()) {
                            coerceOperands(callBinding, false)
                        } else false
                    }
                }
                true
            }
            Composition.AND -> {
                for (ord in Ord.< SqlOperandTypeChecker > zip < SqlOperandTypeChecker ? > allowedRules) {
                    val rule: SqlOperandTypeChecker = ord.e
                    if (!rule.checkOperandTypes(callBinding, false)) {
                        // Avoid trying other rules in AND if the first one fails.
                        return false
                    }
                }
                true
            }
            Composition.OR -> {
                // If there is an ImplicitCastOperandTypeChecker, check it without type coercion first,
                // if all check fails, try type coercion if it is allowed (default true).
                if (checkWithoutTypeCoercion(callBinding)) {
                    return true
                }
                for (ord in Ord.< SqlOperandTypeChecker > zip < SqlOperandTypeChecker ? > allowedRules) {
                    val rule: SqlOperandTypeChecker = ord.e
                    if (rule.checkOperandTypes(callBinding, false)) {
                        return true
                    }
                }
                false
            }
            else -> throw AssertionError()
        }
    }

    /** Tries to coerce the operands based on the defined type families.  */
    private fun coerceOperands(callBinding: SqlCallBinding, repeat: Boolean): Boolean {
        // Type coercion for the call,
        // collect SqlTypeFamily and data type of all the operands.
        var families: List<SqlTypeFamily?> = allowedRules.stream()
            .filter { r -> r is ImplicitCastOperandTypeChecker } // All the rules are SqlSingleOperandTypeChecker.
            .map { r -> (r as ImplicitCastOperandTypeChecker).getOperandSqlTypeFamily(0) }
            .collect(Collectors.toList())
        if (families.size() < allowedRules.size()) {
            // Not all the checkers are ImplicitCastOperandTypeChecker, returns early.
            return false
        }
        if (repeat) {
            assert(families.size() === 1)
            families = Collections.nCopies(callBinding.getOperandCount(), families[0])
        }
        val operandTypes: List<RelDataType> = ArrayList()
        for (i in 0 until callBinding.getOperandCount()) {
            operandTypes.add(callBinding.getOperandType(i))
        }
        val typeCoercion: TypeCoercion = callBinding.getValidator().getTypeCoercion()
        return typeCoercion.builtinFunctionCoercion(
            callBinding,
            operandTypes, families
        )
    }

    private fun checkWithoutTypeCoercion(callBinding: SqlCallBinding): Boolean {
        if (!callBinding.isTypeCoercionEnabled()) {
            return false
        }
        for (rule in allowedRules) {
            if (rule is ImplicitCastOperandTypeChecker) {
                if (rule.checkOperandTypesWithoutTypeCoercion(callBinding, false)) {
                    return true
                }
            }
        }
        return false
    }

    @Override
    @Nullable
    override fun typeInference(): SqlOperandTypeInference? {
        if (composition == Composition.REPEAT) {
            val checker: SqlOperandTypeChecker = Iterables.getOnlyElement(allowedRules)
            if (checker is SqlOperandTypeInference) {
                return SqlOperandTypeInference { callBinding, returnType, operandTypes ->
                    for (i in 0 until callBinding.getOperandCount()) {
                        val operandTypes0: Array<RelDataType?> = arrayOfNulls<RelDataType>(1)
                        checker.inferOperandTypes(callBinding, returnType, operandTypes0)
                        operandTypes.get(i) = operandTypes0[0]
                    }
                }
            }
        }
        return null
    }

    companion object {
        private fun minMin(ranges: List<SqlOperandCountRange>): Int {
            var min: Int = Integer.MAX_VALUE
            for (range in ranges) {
                min = Math.min(min, range.getMin())
            }
            return min
        }
    }
}
