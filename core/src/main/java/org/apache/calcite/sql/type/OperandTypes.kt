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

import org.apache.calcite.rel.type.RelDataType

/**
 * Strategies for checking operand types.
 *
 *
 * This class defines singleton instances of strategy objects for operand
 * type-checking. [org.apache.calcite.sql.type.ReturnTypes]
 * and [org.apache.calcite.sql.type.InferTypes] provide similar strategies
 * for operand type inference and operator return type inference.
 *
 *
 * Note to developers: avoid anonymous inner classes here except for unique,
 * non-generalizable strategies; anything else belongs in a reusable top-level
 * class. If you find yourself copying and pasting an existing strategy's
 * anonymous inner class, you're making a mistake.
 *
 * @see org.apache.calcite.sql.type.SqlOperandTypeChecker
 *
 * @see org.apache.calcite.sql.type.ReturnTypes
 *
 * @see org.apache.calcite.sql.type.InferTypes
 */
object OperandTypes {
    /**
     * Creates a checker that passes if each operand is a member of a
     * corresponding family.
     */
    fun family(vararg families: SqlTypeFamily?): FamilyOperandTypeChecker {
        return FamilyOperandTypeChecker(
            ImmutableList.copyOf(families)
        ) { i -> false }
    }

    /**
     * Creates a checker that passes if each operand is a member of a
     * corresponding family, and allows specified parameters to be optional.
     */
    fun family(
        families: List<SqlTypeFamily?>?,
        optional: Predicate<Integer?>
    ): FamilyOperandTypeChecker {
        return FamilyOperandTypeChecker(families, optional)
    }

    /**
     * Creates a checker that passes if each operand is a member of a
     * corresponding family.
     */
    fun family(families: List<SqlTypeFamily?>?): FamilyOperandTypeChecker {
        return family(families, { i -> false })
    }

    /**
     * Creates a checker for user-defined functions (including user-defined
     * aggregate functions, table functions, and table macros).
     *
     *
     * Unlike built-in functions, there is a fixed number of parameters,
     * and the parameters have names. You can ask for the type of a parameter
     * without providing a particular call (and with it actual arguments) but you
     * do need to provide a type factory, and therefore the types are only good
     * for the duration of the current statement.
     */
    fun operandMetadata(
        families: List<SqlTypeFamily?>?,
        typesFactory: Function<RelDataTypeFactory?, List<RelDataType?>?>?,
        operandName: IntFunction<String?>, optional: Predicate<Integer?>
    ): SqlOperandMetadata {
        return OperandMetadataImpl(
            families, typesFactory, operandName,
            optional
        )
    }

    /**
     * Creates a checker that passes if any one of the rules passes.
     */
    fun or(vararg rules: SqlOperandTypeChecker?): SqlOperandTypeChecker {
        return CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.Composition.OR,
            ImmutableList.copyOf(rules), null, null
        )
    }

    /**
     * Creates a checker that passes if all of the rules pass.
     */
    fun and(vararg rules: SqlOperandTypeChecker?): SqlOperandTypeChecker {
        return CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.Composition.AND,
            ImmutableList.copyOf(rules), null, null
        )
    }

    /**
     * Creates a single-operand checker that passes if any one of the rules
     * passes.
     */
    fun or(
        vararg rules: SqlSingleOperandTypeChecker?
    ): SqlSingleOperandTypeChecker {
        return CompositeSingleOperandTypeChecker(
            CompositeOperandTypeChecker.Composition.OR,
            ImmutableList.copyOf(rules), null
        )
    }

    /**
     * Creates a single-operand checker that passes if all of the rules
     * pass.
     */
    fun and(
        vararg rules: SqlSingleOperandTypeChecker?
    ): SqlSingleOperandTypeChecker {
        return CompositeSingleOperandTypeChecker(
            CompositeOperandTypeChecker.Composition.AND,
            ImmutableList.copyOf(rules), null
        )
    }

    /**
     * Creates an operand checker from a sequence of single-operand checkers.
     */
    fun sequence(
        allowedSignatures: String?,
        vararg rules: SqlSingleOperandTypeChecker?
    ): SqlOperandTypeChecker {
        return CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.Composition.SEQUENCE,
            ImmutableList.copyOf(rules), allowedSignatures, null
        )
    }

    /**
     * Creates a checker that passes if all of the rules pass for each operand,
     * using a given operand count strategy.
     */
    fun repeat(
        range: SqlOperandCountRange?,
        vararg rules: SqlSingleOperandTypeChecker?
    ): SqlOperandTypeChecker {
        return CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.Composition.REPEAT,
            ImmutableList.copyOf(rules), null, range
        )
    }
    // ----------------------------------------------------------------------
    // SqlOperandTypeChecker definitions
    // ----------------------------------------------------------------------
    //~ Static fields/initializers ---------------------------------------------
    /**
     * Operand type-checking strategy for an operator which takes no operands.
     */
    val NILADIC: SqlSingleOperandTypeChecker = family()

    /**
     * Operand type-checking strategy for an operator with no restrictions on
     * number or type of operands.
     */
    val VARIADIC: SqlOperandTypeChecker = variadic(SqlOperandCountRanges.any())

    /** Operand type-checking strategy that allows one or more operands.  */
    val ONE_OR_MORE: SqlOperandTypeChecker = variadic(SqlOperandCountRanges.from(1))
    fun variadic(
        range: SqlOperandCountRange
    ): SqlOperandTypeChecker {
        return object : SqlOperandTypeChecker() {
            @Override
            override fun checkOperandTypes(
                callBinding: SqlCallBinding,
                throwOnFailure: Boolean
            ): Boolean {
                return range.isValidCount(callBinding.getOperandCount())
            }

            @get:Override
            override val operandCountRange: SqlOperandCountRange
                get() = range

            @Override
            fun getAllowedSignatures(op: SqlOperator?, opName: String): String {
                return "$opName(...)"
            }

            @Override
            override fun isOptional(i: Int): Boolean {
                return false
            }

            @get:Override
            override val consistency: org.apache.calcite.sql.type.SqlOperandTypeChecker.Consistency?
                get() = Consistency.NONE
        }
    }

    val BOOLEAN: SqlSingleOperandTypeChecker = family(SqlTypeFamily.BOOLEAN)
    val BOOLEAN_BOOLEAN: SqlSingleOperandTypeChecker = family(SqlTypeFamily.BOOLEAN, SqlTypeFamily.BOOLEAN)
    val NUMERIC: SqlSingleOperandTypeChecker = family(SqlTypeFamily.NUMERIC)
    val INTEGER: SqlSingleOperandTypeChecker = family(SqlTypeFamily.INTEGER)
    val NUMERIC_OPTIONAL_INTEGER: SqlSingleOperandTypeChecker = family(ImmutableList.of(
        SqlTypeFamily.NUMERIC,
        SqlTypeFamily.INTEGER
    ),  // Second operand optional (operand index 0, 1)
        { number -> number === 1 })
    val NUMERIC_INTEGER: SqlSingleOperandTypeChecker = family(SqlTypeFamily.NUMERIC, SqlTypeFamily.INTEGER)
    val NUMERIC_NUMERIC: SqlSingleOperandTypeChecker = family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)
    val EXACT_NUMERIC: SqlSingleOperandTypeChecker = family(SqlTypeFamily.EXACT_NUMERIC)
    val EXACT_NUMERIC_EXACT_NUMERIC: SqlSingleOperandTypeChecker =
        family(SqlTypeFamily.EXACT_NUMERIC, SqlTypeFamily.EXACT_NUMERIC)
    val BINARY: SqlSingleOperandTypeChecker = family(SqlTypeFamily.BINARY)
    val STRING: SqlSingleOperandTypeChecker = family(SqlTypeFamily.STRING)
    val STRING_STRING: FamilyOperandTypeChecker = family(SqlTypeFamily.STRING, SqlTypeFamily.STRING)
    val STRING_STRING_STRING: FamilyOperandTypeChecker =
        family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING)
    val STRING_STRING_OPTIONAL_STRING: FamilyOperandTypeChecker = family(ImmutableList.of(
        SqlTypeFamily.STRING,
        SqlTypeFamily.STRING,
        SqlTypeFamily.STRING
    ),  // Third operand optional (operand index 0, 1, 2)
        { number -> number === 2 })
    val CHARACTER: SqlSingleOperandTypeChecker = family(SqlTypeFamily.CHARACTER)
    val DATETIME: SqlSingleOperandTypeChecker = family(SqlTypeFamily.DATETIME)
    val DATE: SqlSingleOperandTypeChecker = family(SqlTypeFamily.DATE)
    val TIMESTAMP: SqlSingleOperandTypeChecker = family(SqlTypeFamily.TIMESTAMP)
    val INTERVAL: SqlSingleOperandTypeChecker = family(SqlTypeFamily.DATETIME_INTERVAL)
    val CHARACTER_CHARACTER_DATETIME: SqlSingleOperandTypeChecker =
        family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME)
    val PERIOD: SqlSingleOperandTypeChecker = PeriodOperandTypeChecker()
    val PERIOD_OR_DATETIME: SqlSingleOperandTypeChecker = or(PERIOD, DATETIME)
    val INTERVAL_INTERVAL: FamilyOperandTypeChecker =
        family(SqlTypeFamily.DATETIME_INTERVAL, SqlTypeFamily.DATETIME_INTERVAL)
    val MULTISET: SqlSingleOperandTypeChecker = family(SqlTypeFamily.MULTISET)
    val ARRAY: SqlSingleOperandTypeChecker = family(SqlTypeFamily.ARRAY)

    /** Checks that returns whether a value is a multiset or an array.
     * Cf Java, where list and set are collections but a map is not.  */
    val COLLECTION: SqlSingleOperandTypeChecker = or(
        family(SqlTypeFamily.MULTISET),
        family(SqlTypeFamily.ARRAY)
    )
    val COLLECTION_OR_MAP: SqlSingleOperandTypeChecker = or(
        family(SqlTypeFamily.MULTISET),
        family(SqlTypeFamily.ARRAY),
        family(SqlTypeFamily.MAP)
    )

    /**
     * Operand type-checking strategy where type must be a literal or NULL.
     */
    val NULLABLE_LITERAL: SqlSingleOperandTypeChecker = LiteralOperandTypeChecker(true)

    /**
     * Operand type-checking strategy type must be a non-NULL literal.
     */
    val LITERAL: SqlSingleOperandTypeChecker = LiteralOperandTypeChecker(false)

    /**
     * Operand type-checking strategy type must be a positive integer non-NULL
     * literal.
     */
    val POSITIVE_INTEGER_LITERAL: SqlSingleOperandTypeChecker =
        object : FamilyOperandTypeChecker(ImmutableList.of(SqlTypeFamily.INTEGER),
            { i -> false }) {
            @Override
            override fun checkSingleOperandType(
                callBinding: SqlCallBinding,
                node: SqlNode,
                iFormalOperand: Int,
                throwOnFailure: Boolean
            ): Boolean {
                if (!LITERAL.checkSingleOperandType(
                        callBinding,
                        node,
                        iFormalOperand,
                        throwOnFailure
                    )
                ) {
                    return false
                }
                if (!super.checkSingleOperandType(
                        callBinding,
                        node,
                        iFormalOperand,
                        throwOnFailure
                    )
                ) {
                    return false
                }
                val arg: SqlLiteral = node as SqlLiteral
                val value: BigDecimal = arg.getValueAs(BigDecimal::class.java)
                if (value.compareTo(BigDecimal.ZERO) < 0
                    || hasFractionalPart(value)
                ) {
                    if (throwOnFailure) {
                        throw callBinding.newError(
                            RESOURCE.argumentMustBePositiveInteger(
                                callBinding.getOperator().getName()
                            )
                        )
                    }
                    return false
                }
                if (value.compareTo(BigDecimal.valueOf(Integer.MAX_VALUE)) > 0) {
                    if (throwOnFailure) {
                        throw callBinding.newError(
                            RESOURCE.numberLiteralOutOfRange(value.toString())
                        )
                    }
                    return false
                }
                return true
            }

            /** Returns whether a number has any fractional part.
             *
             * @see BigDecimal.longValueExact
             */
            private fun hasFractionalPart(bd: BigDecimal): Boolean {
                return org.apache.calcite.sql.type.bd.precision() - org.apache.calcite.sql.type.bd.scale() <= 0
            }
        }

    /**
     * Operand type-checking strategy type must be a numeric non-NULL
     * literal in the range 0 and 1 inclusive.
     */
    val UNIT_INTERVAL_NUMERIC_LITERAL: SqlSingleOperandTypeChecker =
        object : FamilyOperandTypeChecker(ImmutableList.of(SqlTypeFamily.NUMERIC),
            { i -> false }) {
            @Override
            override fun checkSingleOperandType(
                callBinding: SqlCallBinding,
                node: SqlNode,
                iFormalOperand: Int,
                throwOnFailure: Boolean
            ): Boolean {
                if (!LITERAL.checkSingleOperandType(
                        callBinding,
                        node,
                        iFormalOperand,
                        throwOnFailure
                    )
                ) {
                    return false
                }
                if (!super.checkSingleOperandType(
                        callBinding,
                        node,
                        iFormalOperand,
                        throwOnFailure
                    )
                ) {
                    return false
                }
                val arg: SqlLiteral = node as SqlLiteral
                val value: BigDecimal = arg.getValueAs(BigDecimal::class.java)
                if (value.compareTo(BigDecimal.ZERO) < 0
                    || value.compareTo(BigDecimal.ONE) > 0
                ) {
                    if (throwOnFailure) {
                        throw callBinding.newError(
                            RESOURCE.argumentMustBeNumericLiteralInRange(
                                callBinding.getOperator().getName(), 0, 1
                            )
                        )
                    }
                    return false
                }
                return true
            }
        }

    /**
     * Operand type-checking strategy where two operands must both be in the
     * same type family.
     */
    val SAME_SAME: SqlSingleOperandTypeChecker = SameOperandTypeChecker(2)
    val SAME_SAME_INTEGER: SqlSingleOperandTypeChecker = SameOperandTypeExceptLastOperandChecker(3, "INTEGER")

    /**
     * Operand type-checking strategy where three operands must all be in the
     * same type family.
     */
    val SAME_SAME_SAME: SqlSingleOperandTypeChecker = SameOperandTypeChecker(3)

    /**
     * Operand type-checking strategy where any number of operands must all be
     * in the same type family.
     */
    val SAME_VARIADIC: SqlOperandTypeChecker = SameOperandTypeChecker(-1)

    /**
     * Operand type-checking strategy where any positive number of operands must all be
     * in the same type family.
     */
    val AT_LEAST_ONE_SAME_VARIADIC: SqlOperandTypeChecker = object : SameOperandTypeChecker(-1) {
        @get:Override
        override val operandCountRange: SqlOperandCountRange
            get() = SqlOperandCountRanges.from(1)
    }

    /**
     * Operand type-checking strategy where operand types must allow ordered
     * comparisons.
     */
    val COMPARABLE_ORDERED_COMPARABLE_ORDERED: SqlOperandTypeChecker = ComparableOperandTypeChecker(
        2, RelDataTypeComparability.ALL,
        SqlOperandTypeChecker.Consistency.COMPARE
    )

    /**
     * Operand type-checking strategy where operand type must allow ordered
     * comparisons. Used when instance comparisons are made on single operand
     * functions
     */
    val COMPARABLE_ORDERED: SqlOperandTypeChecker = ComparableOperandTypeChecker(
        1, RelDataTypeComparability.ALL,
        SqlOperandTypeChecker.Consistency.NONE
    )

    /**
     * Operand type-checking strategy where operand types must allow unordered
     * comparisons.
     */
    val COMPARABLE_UNORDERED_COMPARABLE_UNORDERED: SqlOperandTypeChecker = ComparableOperandTypeChecker(
        2, RelDataTypeComparability.UNORDERED,
        SqlOperandTypeChecker.Consistency.LEAST_RESTRICTIVE
    )

    /**
     * Operand type-checking strategy where two operands must both be in the
     * same string type family.
     */
    val STRING_SAME_SAME: SqlSingleOperandTypeChecker = and(STRING_STRING, SAME_SAME)

    /**
     * Operand type-checking strategy where three operands must all be in the
     * same string type family.
     */
    val STRING_SAME_SAME_SAME: SqlSingleOperandTypeChecker = and(STRING_STRING_STRING, SAME_SAME_SAME)
    val STRING_STRING_INTEGER: SqlSingleOperandTypeChecker =
        family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.INTEGER)
    val STRING_STRING_INTEGER_INTEGER: SqlSingleOperandTypeChecker = family(
        SqlTypeFamily.STRING, SqlTypeFamily.STRING,
        SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER
    )
    val STRING_INTEGER: SqlSingleOperandTypeChecker = family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER)
    val STRING_INTEGER_INTEGER: SqlSingleOperandTypeChecker = family(
        SqlTypeFamily.STRING, SqlTypeFamily.INTEGER,
        SqlTypeFamily.INTEGER
    )
    val STRING_INTEGER_OPTIONAL_INTEGER: SqlSingleOperandTypeChecker = family(
        ImmutableList.of(
            SqlTypeFamily.STRING, SqlTypeFamily.INTEGER,
            SqlTypeFamily.INTEGER
        ), { i -> i === 2 })

    /** Operand type-checking strategy where the first operand is a character or
     * binary string (CHAR, VARCHAR, BINARY or VARBINARY), and the second operand
     * is INTEGER.  */
    val CBSTRING_INTEGER: SqlSingleOperandTypeChecker = or(
        family(SqlTypeFamily.STRING, SqlTypeFamily.INTEGER),
        family(SqlTypeFamily.BINARY, SqlTypeFamily.INTEGER)
    )

    /**
     * Operand type-checking strategy where two operands must both be in the
     * same string type family and last type is INTEGER.
     */
    val STRING_SAME_SAME_INTEGER: SqlSingleOperandTypeChecker = and(STRING_STRING_INTEGER, SAME_SAME_INTEGER)
    val ANY: SqlSingleOperandTypeChecker = family(SqlTypeFamily.ANY)
    val ANY_ANY: SqlSingleOperandTypeChecker = family(SqlTypeFamily.ANY, SqlTypeFamily.ANY)
    val ANY_IGNORE: SqlSingleOperandTypeChecker = family(SqlTypeFamily.ANY, SqlTypeFamily.IGNORE)
    val IGNORE_ANY: SqlSingleOperandTypeChecker = family(SqlTypeFamily.IGNORE, SqlTypeFamily.ANY)
    val ANY_NUMERIC: SqlSingleOperandTypeChecker = family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC)
    val CURSOR: SqlSingleOperandTypeChecker = family(SqlTypeFamily.CURSOR)

    /**
     * Parameter type-checking strategy where type must a nullable time interval,
     * nullable time interval.
     */
    val INTERVAL_SAME_SAME: SqlSingleOperandTypeChecker = and(INTERVAL_INTERVAL, SAME_SAME)
    val NUMERIC_INTERVAL: SqlSingleOperandTypeChecker = family(SqlTypeFamily.NUMERIC, SqlTypeFamily.DATETIME_INTERVAL)
    val INTERVAL_NUMERIC: SqlSingleOperandTypeChecker = family(SqlTypeFamily.DATETIME_INTERVAL, SqlTypeFamily.NUMERIC)
    val DATETIME_INTERVAL: SqlSingleOperandTypeChecker = family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME_INTERVAL)
    val DATETIME_INTERVAL_INTERVAL: SqlSingleOperandTypeChecker = family(
        SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME_INTERVAL,
        SqlTypeFamily.DATETIME_INTERVAL
    )
    val DATETIME_INTERVAL_INTERVAL_TIME: SqlSingleOperandTypeChecker = family(
        SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME_INTERVAL,
        SqlTypeFamily.DATETIME_INTERVAL, SqlTypeFamily.TIME
    )
    val DATETIME_INTERVAL_TIME: SqlSingleOperandTypeChecker = family(
        SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME_INTERVAL,
        SqlTypeFamily.TIME
    )
    val INTERVAL_DATETIME: SqlSingleOperandTypeChecker = family(SqlTypeFamily.DATETIME_INTERVAL, SqlTypeFamily.DATETIME)
    val INTERVALINTERVAL_INTERVALDATETIME: SqlSingleOperandTypeChecker = or(INTERVAL_SAME_SAME, INTERVAL_DATETIME)

    // TODO: datetime+interval checking missing
    // TODO: interval+datetime checking missing
    val PLUS_OPERATOR: SqlSingleOperandTypeChecker = or(
        NUMERIC_NUMERIC, INTERVAL_SAME_SAME, DATETIME_INTERVAL,
        INTERVAL_DATETIME
    )

    /**
     * Type-checking strategy for the "*" operator.
     */
    val MULTIPLY_OPERATOR: SqlSingleOperandTypeChecker = or(NUMERIC_NUMERIC, INTERVAL_NUMERIC, NUMERIC_INTERVAL)

    /**
     * Type-checking strategy for the "/" operator.
     */
    val DIVISION_OPERATOR: SqlSingleOperandTypeChecker = or(NUMERIC_NUMERIC, INTERVAL_NUMERIC)
    val MINUS_OPERATOR: SqlSingleOperandTypeChecker =  // TODO:  compatibility check
        or(NUMERIC_NUMERIC, INTERVAL_SAME_SAME, DATETIME_INTERVAL)
    val MINUS_DATE_OPERATOR: FamilyOperandTypeChecker = object : FamilyOperandTypeChecker(
        ImmutableList.of(
            SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME,
            SqlTypeFamily.DATETIME_INTERVAL
        ),
        { i -> false }) {
        @Override
        override fun checkOperandTypes(
            callBinding: SqlCallBinding,
            throwOnFailure: Boolean
        ): Boolean {
            return if (!super.checkOperandTypes(callBinding, throwOnFailure)) {
                false
            } else SAME_SAME.checkOperandTypes(callBinding, throwOnFailure)
        }
    }
    val NUMERIC_OR_INTERVAL: SqlSingleOperandTypeChecker = or(NUMERIC, INTERVAL)
    val NUMERIC_OR_STRING: SqlSingleOperandTypeChecker = or(NUMERIC, STRING)

    /** Checker that returns whether a value is a multiset of records or an
     * array of records.
     *
     * @see .COLLECTION
     */
    val RECORD_COLLECTION: SqlSingleOperandTypeChecker = object : RecordTypeWithOneFieldChecker(
        Predicate<SqlTypeName> { sqlTypeName -> sqlTypeName !== SqlTypeName.ARRAY && sqlTypeName !== SqlTypeName.MULTISET }) {
        @Override
        override fun getAllowedSignatures(op: SqlOperator?, opName: String?): String {
            return "UNNEST(<MULTISET>)"
        }
    }

    /** Checker that returns whether a value is a collection (multiset or array)
     * of scalar or record values.  */
    val SCALAR_OR_RECORD_COLLECTION: SqlSingleOperandTypeChecker = or(COLLECTION, RECORD_COLLECTION)
    val SCALAR_OR_RECORD_COLLECTION_OR_MAP: SqlSingleOperandTypeChecker = or(
        COLLECTION_OR_MAP,
        object : RecordTypeWithOneFieldChecker(
            Predicate<SqlTypeName> { sqlTypeName -> sqlTypeName !== SqlTypeName.MULTISET && sqlTypeName !== SqlTypeName.ARRAY && sqlTypeName !== SqlTypeName.MAP }) {
            @Override
            override fun getAllowedSignatures(op: SqlOperator?, opName: String?): String {
                return "UNNEST(<MULTISET>)\nUNNEST(<ARRAY>)\nUNNEST(<MAP>)"
            }
        })
    val MULTISET_MULTISET: SqlOperandTypeChecker = MultisetOperandTypeChecker()

    /**
     * Operand type-checking strategy for a set operator (UNION, INTERSECT,
     * EXCEPT).
     */
    val SET_OP: SqlOperandTypeChecker = SetopOperandTypeChecker()
    val RECORD_TO_SCALAR: SqlOperandTypeChecker = object : SqlSingleOperandTypeChecker() {
        @Override
        override fun checkSingleOperandType(
            callBinding: SqlCallBinding,
            node: SqlNode?,
            iFormalOperand: Int,
            throwOnFailure: Boolean
        ): Boolean {
            assert(0 == iFormalOperand)
            val type: RelDataType = SqlTypeUtil.deriveType(callBinding, node)
            var validationError = false
            if (!type.isStruct()) {
                validationError = true
            } else if (type.getFieldList().size() !== 1) {
                validationError = true
            }
            if (validationError && throwOnFailure) {
                throw callBinding.newValidationSignatureError()
            }
            return !validationError
        }

        @Override
        override fun checkOperandTypes(
            callBinding: SqlCallBinding,
            throwOnFailure: Boolean
        ): Boolean {
            return checkSingleOperandType(
                callBinding,
                callBinding.operand(0),
                0,
                throwOnFailure
            )
        }

        @get:Override
        override val operandCountRange: SqlOperandCountRange
            get() = SqlOperandCountRanges.of(1)

        @Override
        override fun getAllowedSignatures(op: SqlOperator?, opName: String?): String {
            return SqlUtil.getAliasedSignature(
                op, opName,
                ImmutableList.of("RECORDTYPE(SINGLE FIELD)")
            )
        }

        @Override
        override fun isOptional(i: Int): Boolean {
            return false
        }

        @get:Override
        override val consistency: org.apache.calcite.sql.type.SqlOperandTypeChecker.Consistency?
            get() = Consistency.NONE
    }

    /**
     * Checker for record just has one field.
     */
    private abstract class RecordTypeWithOneFieldChecker(predicate: Predicate<SqlTypeName>) :
        SqlSingleOperandTypeChecker {
        private val typeNamePredicate: Predicate<SqlTypeName>

        init {
            typeNamePredicate = predicate
        }

        @Override
        override fun checkSingleOperandType(
            callBinding: SqlCallBinding,
            node: SqlNode?,
            iFormalOperand: Int,
            throwOnFailure: Boolean
        ): Boolean {
            assert(0 == iFormalOperand)
            val type: RelDataType = SqlTypeUtil.deriveType(callBinding, node)
            var validationError = false
            if (!type.isStruct()) {
                validationError = true
            } else if (type.getFieldList().size() !== 1) {
                validationError = true
            } else {
                val typeName: SqlTypeName = type.getFieldList().get(0).getType().getSqlTypeName()
                if (typeNamePredicate.test(typeName)) {
                    validationError = true
                }
            }
            if (validationError && throwOnFailure) {
                throw callBinding.newValidationSignatureError()
            }
            return !validationError
        }

        @Override
        override fun checkOperandTypes(
            callBinding: SqlCallBinding,
            throwOnFailure: Boolean
        ): Boolean {
            return checkSingleOperandType(
                callBinding,
                callBinding.operand(0),
                0,
                throwOnFailure
            )
        }

        @get:Override
        override val operandCountRange: SqlOperandCountRange
            get() = SqlOperandCountRanges.of(1)

        @Override
        override fun isOptional(i: Int): Boolean {
            return false
        }

        @get:Override
        override val consistency: org.apache.calcite.sql.type.SqlOperandTypeChecker.Consistency?
            get() = Consistency.NONE
    }

    /** Operand type-checker that accepts period types. Examples:
     *
     *
     *  * PERIOD (DATETIME, DATETIME)
     *  * PERIOD (DATETIME, INTERVAL)
     *  * [ROW] (DATETIME, DATETIME)
     *  * [ROW] (DATETIME, INTERVAL)
     *   */
    private class PeriodOperandTypeChecker : SqlSingleOperandTypeChecker {
        @Override
        override fun checkSingleOperandType(
            callBinding: SqlCallBinding,
            node: SqlNode?, iFormalOperand: Int, throwOnFailure: Boolean
        ): Boolean {
            assert(0 == iFormalOperand)
            val type: RelDataType = SqlTypeUtil.deriveType(callBinding, node)
            var valid = false
            if (type.isStruct() && type.getFieldList().size() === 2) {
                val t0: RelDataType = type.getFieldList().get(0).getType()
                val t1: RelDataType = type.getFieldList().get(1).getType()
                if (SqlTypeUtil.isDatetime(t0)) {
                    if (SqlTypeUtil.isDatetime(t1)) {
                        // t0 must be comparable with t1; (DATE, TIMESTAMP) is not valid
                        if (SqlTypeUtil.sameNamedType(t0, t1)) {
                            valid = true
                        }
                    } else if (SqlTypeUtil.isInterval(t1)) {
                        valid = true
                    }
                }
            }
            if (!valid && throwOnFailure) {
                throw callBinding.newValidationSignatureError()
            }
            return valid
        }

        @Override
        override fun checkOperandTypes(
            callBinding: SqlCallBinding,
            throwOnFailure: Boolean
        ): Boolean {
            return checkSingleOperandType(
                callBinding, callBinding.operand(0), 0,
                throwOnFailure
            )
        }

        @get:Override
        override val operandCountRange: SqlOperandCountRange
            get() = SqlOperandCountRanges.of(1)

        @Override
        override fun getAllowedSignatures(op: SqlOperator?, opName: String?): String {
            return SqlUtil.getAliasedSignature(
                op, opName,
                ImmutableList.of(
                    "PERIOD (DATETIME, INTERVAL)",
                    "PERIOD (DATETIME, DATETIME)"
                )
            )
        }

        @Override
        override fun isOptional(i: Int): Boolean {
            return false
        }

        @get:Override
        override val consistency: org.apache.calcite.sql.type.SqlOperandTypeChecker.Consistency?
            get() = Consistency.NONE
    }
}
