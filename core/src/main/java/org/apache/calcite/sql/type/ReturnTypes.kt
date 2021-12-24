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
 * A collection of return-type inference strategies.
 */
object ReturnTypes {
    /** Creates a return-type inference that applies a rule then a sequence of
     * rules, returning the first non-null result.
     *
     * @see SqlReturnTypeInference.orElse
     */
    fun chain(
        vararg rules: SqlReturnTypeInference?
    ): SqlReturnTypeInferenceChain {
        return SqlReturnTypeInferenceChain(rules)
    }

    /** Creates a return-type inference that applies a rule then a sequence of
     * transforms.
     *
     * @see SqlReturnTypeInference.andThen
     */
    fun cascade(
        rule: SqlReturnTypeInference?,
        vararg transforms: SqlTypeTransform?
    ): SqlTypeTransformCascade {
        return SqlTypeTransformCascade(rule, transforms)
    }

    fun explicit(
        protoType: RelProtoDataType?
    ): ExplicitReturnTypeInference {
        return ExplicitReturnTypeInference(protoType)
    }

    /**
     * Creates an inference rule which returns a copy of a given data type.
     */
    fun explicit(type: RelDataType?): ExplicitReturnTypeInference {
        return explicit(RelDataTypeImpl.proto(type))
    }

    /**
     * Creates an inference rule which returns a type with no precision or scale,
     * such as `DATE`.
     */
    fun explicit(typeName: SqlTypeName?): ExplicitReturnTypeInference {
        return explicit(RelDataTypeImpl.proto(typeName, false))
    }

    /**
     * Creates an inference rule which returns a type with precision but no scale,
     * such as `VARCHAR(100)`.
     */
    fun explicit(
        typeName: SqlTypeName?,
        precision: Int
    ): ExplicitReturnTypeInference {
        return explicit(RelDataTypeImpl.proto(typeName, precision, false))
    }

    /** Returns a return-type inference that first transforms a binding and
     * then applies an inference.
     *
     *
     * [.stripOrderBy] is an example of `bindingTransform`.  */
    fun andThen(
        bindingTransform: UnaryOperator<SqlOperatorBinding?>,
        typeInference: SqlReturnTypeInference
    ): SqlReturnTypeInference {
        return SqlReturnTypeInference { opBinding -> typeInference.inferReturnType(bindingTransform.apply(opBinding)) }
    }

    /** Converts a binding of `FOO(x, y ORDER BY z)`
     * or `FOO(x, y ORDER BY z SEPARATOR s)`
     * to a binding of `FOO(x, y)`.
     * Used for `STRING_AGG` and `GROUP_CONCAT`.  */
    fun stripOrderBy(
        operatorBinding: SqlOperatorBinding
    ): SqlOperatorBinding {
        if (operatorBinding is SqlCallBinding) {
            val callBinding: SqlCallBinding = operatorBinding as SqlCallBinding
            val call2: SqlCall = stripSeparator(callBinding.getCall())
            val call3: SqlCall = stripOrderBy(call2)
            if (call3 !== callBinding.getCall()) {
                return SqlCallBinding(
                    callBinding.getValidator(),
                    callBinding.getScope(), call3
                )
            }
        }
        return operatorBinding
    }

    fun stripOrderBy(call: SqlCall): SqlCall {
        return if (!call.getOperandList().isEmpty()
            && Util.last(call.getOperandList()) is SqlNodeList
        ) {
            // Remove the last argument if it is "ORDER BY". The parser stashes the
            // ORDER BY clause in the argument list but it does not take part in
            // type derivation.
            call.getOperator().createCall(
                call.getFunctionQuantifier(),
                call.getParserPosition(), Util.skipLast(call.getOperandList())
            )
        } else call
    }

    fun stripSeparator(call: SqlCall): SqlCall {
        return if (!call.getOperandList().isEmpty()
            && Util.last(call.getOperandList()).getKind() === SqlKind.SEPARATOR
        ) {
            // Remove the last argument if it is "SEPARATOR literal".
            call.getOperator().createCall(
                call.getFunctionQuantifier(),
                call.getParserPosition(), Util.skipLast(call.getOperandList())
            )
        } else call
    }

    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * the operand #0 (0-based).
     */
    val ARG0: SqlReturnTypeInference = OrdinalReturnTypeInference(0)

    /**
     * Type-inference strategy whereby the result type of a call is VARYING the
     * type of the first argument. The length returned is the same as length of
     * the first argument. If any of the other operands are nullable the
     * returned type will also be nullable. First Arg must be of string type.
     */
    val ARG0_NULLABLE_VARYING: SqlReturnTypeInference = ARG0.andThen(SqlTypeTransforms.TO_NULLABLE)
        .andThen(SqlTypeTransforms.TO_VARYING)

    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * the operand #0 (0-based). If any of the other operands are nullable the
     * returned type will also be nullable.
     */
    val ARG0_NULLABLE: SqlReturnTypeInference = ARG0.andThen(SqlTypeTransforms.TO_NULLABLE)

    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * the operand #0 (0-based), with nulls always allowed.
     */
    val ARG0_FORCE_NULLABLE: SqlReturnTypeInference = ARG0.andThen(SqlTypeTransforms.FORCE_NULLABLE)
    val ARG0_INTERVAL: SqlReturnTypeInference = MatchReturnTypeInference(
        0,
        SqlTypeFamily.DATETIME_INTERVAL.getTypeNames()
    )
    val ARG0_INTERVAL_NULLABLE: SqlReturnTypeInference = ARG0_INTERVAL.andThen(SqlTypeTransforms.TO_NULLABLE)

    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * the operand #0 (0-based), and nullable if the call occurs within a
     * "GROUP BY ()" query. E.g. in "select sum(1) as s from empty", s may be
     * null.
     */
    val ARG0_NULLABLE_IF_EMPTY: SqlReturnTypeInference = object : OrdinalReturnTypeInference(0) {
        @Override
        override fun inferReturnType(opBinding: SqlOperatorBinding): RelDataType {
            val type: RelDataType = super.inferReturnType(opBinding)
            return if (opBinding.getGroupCount() === 0 || opBinding.hasFilter()) {
                opBinding.getTypeFactory()
                    .createTypeWithNullability(type, true)
            } else {
                type
            }
        }
    }

    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * the operand #1 (0-based).
     */
    val ARG1: SqlReturnTypeInference = OrdinalReturnTypeInference(1)

    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * the operand #1 (0-based). If any of the other operands are nullable the
     * returned type will also be nullable.
     */
    val ARG1_NULLABLE: SqlReturnTypeInference = ARG1.andThen(SqlTypeTransforms.TO_NULLABLE)

    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * operand #2 (0-based).
     */
    val ARG2: SqlReturnTypeInference = OrdinalReturnTypeInference(2)

    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * operand #2 (0-based). If any of the other operands are nullable the
     * returned type will also be nullable.
     */
    val ARG2_NULLABLE: SqlReturnTypeInference = ARG2.andThen(SqlTypeTransforms.TO_NULLABLE)

    /**
     * Type-inference strategy whereby the result type of a call is Boolean.
     */
    val BOOLEAN: SqlReturnTypeInference = explicit(SqlTypeName.BOOLEAN)

    /**
     * Type-inference strategy whereby the result type of a call is Boolean,
     * with nulls allowed if any of the operands allow nulls.
     */
    val BOOLEAN_NULLABLE: SqlReturnTypeInference = BOOLEAN.andThen(SqlTypeTransforms.TO_NULLABLE)

    /**
     * Type-inference strategy with similar effect to [.BOOLEAN_NULLABLE],
     * which is more efficient, but can only be used if all arguments are
     * BOOLEAN.
     */
    val BOOLEAN_NULLABLE_OPTIMIZED: SqlReturnTypeInference = SqlReturnTypeInference { opBinding ->
        // Equivalent to
        //   cascade(ARG0, SqlTypeTransforms.TO_NULLABLE);
        // but implemented by hand because used in AND, which is a very common
        // operator.
        val n: Int = opBinding.getOperandCount()
        var type1: RelDataType? = null
        for (i in 0 until n) {
            type1 = opBinding.getOperandType(i)
            if (type1.isNullable()) {
                break
            }
        }
        type1
    }

    /**
     * Type-inference strategy whereby the result type of a call is a nullable
     * Boolean.
     */
    val BOOLEAN_FORCE_NULLABLE: SqlReturnTypeInference = BOOLEAN.andThen(SqlTypeTransforms.FORCE_NULLABLE)

    /**
     * Type-inference strategy whereby the result type of a call is BOOLEAN
     * NOT NULL.
     */
    val BOOLEAN_NOT_NULL: SqlReturnTypeInference = BOOLEAN.andThen(SqlTypeTransforms.TO_NOT_NULLABLE)

    /**
     * Type-inference strategy whereby the result type of a call is DATE.
     */
    val DATE: SqlReturnTypeInference = explicit(SqlTypeName.DATE)

    /**
     * Type-inference strategy whereby the result type of a call is nullable
     * DATE.
     */
    val DATE_NULLABLE: SqlReturnTypeInference = DATE.andThen(SqlTypeTransforms.TO_NULLABLE)

    /**
     * Type-inference strategy whereby the result type of a call is TIME(0).
     */
    val TIME: SqlReturnTypeInference = explicit(SqlTypeName.TIME, 0)

    /**
     * Type-inference strategy whereby the result type of a call is nullable
     * TIME(0).
     */
    val TIME_NULLABLE: SqlReturnTypeInference = TIME.andThen(SqlTypeTransforms.TO_NULLABLE)

    /**
     * Type-inference strategy whereby the result type of a call is TIMESTAMP.
     */
    val TIMESTAMP: SqlReturnTypeInference = explicit(SqlTypeName.TIMESTAMP)

    /**
     * Type-inference strategy whereby the result type of a call is nullable
     * TIMESTAMP.
     */
    val TIMESTAMP_NULLABLE: SqlReturnTypeInference = TIMESTAMP.andThen(SqlTypeTransforms.TO_NULLABLE)

    /**
     * Type-inference strategy whereby the result type of a call is Double.
     */
    val DOUBLE: SqlReturnTypeInference = explicit(SqlTypeName.DOUBLE)

    /**
     * Type-inference strategy whereby the result type of a call is Double with
     * nulls allowed if any of the operands allow nulls.
     */
    val DOUBLE_NULLABLE: SqlReturnTypeInference = DOUBLE.andThen(SqlTypeTransforms.TO_NULLABLE)

    /**
     * Type-inference strategy whereby the result type of a call is a Char.
     */
    val CHAR: SqlReturnTypeInference = explicit(SqlTypeName.CHAR)

    /**
     * Type-inference strategy whereby the result type of a call is an Integer.
     */
    val INTEGER: SqlReturnTypeInference = explicit(SqlTypeName.INTEGER)

    /**
     * Type-inference strategy whereby the result type of a call is an Integer
     * with nulls allowed if any of the operands allow nulls.
     */
    val INTEGER_NULLABLE: SqlReturnTypeInference = INTEGER.andThen(SqlTypeTransforms.TO_NULLABLE)

    /**
     * Type-inference strategy whereby the result type of a call is a BIGINT.
     */
    val BIGINT: SqlReturnTypeInference = explicit(SqlTypeName.BIGINT)

    /**
     * Type-inference strategy whereby the result type of a call is a nullable
     * BIGINT.
     */
    val BIGINT_FORCE_NULLABLE: SqlReturnTypeInference = BIGINT.andThen(SqlTypeTransforms.FORCE_NULLABLE)

    /**
     * Type-inference strategy whereby the result type of a call is a BIGINT
     * with nulls allowed if any of the operands allow nulls.
     */
    val BIGINT_NULLABLE: SqlReturnTypeInference = BIGINT.andThen(SqlTypeTransforms.TO_NULLABLE)

    /**
     * Type-inference strategy that always returns "VARCHAR(4)".
     */
    val VARCHAR_4: SqlReturnTypeInference = explicit(SqlTypeName.VARCHAR, 4)

    /**
     * Type-inference strategy that always returns "VARCHAR(4)" with nulls
     * allowed if any of the operands allow nulls.
     */
    val VARCHAR_4_NULLABLE: SqlReturnTypeInference = VARCHAR_4.andThen(SqlTypeTransforms.TO_NULLABLE)

    /**
     * Type-inference strategy that always returns "VARCHAR(2000)".
     */
    val VARCHAR_2000: SqlReturnTypeInference = explicit(SqlTypeName.VARCHAR, 2000)

    /**
     * Type-inference strategy that always returns "VARCHAR(2000)" with nulls
     * allowed if any of the operands allow nulls.
     */
    val VARCHAR_2000_NULLABLE: SqlReturnTypeInference = VARCHAR_2000.andThen(SqlTypeTransforms.TO_NULLABLE)

    /**
     * Type-inference strategy for Histogram agg support.
     */
    val HISTOGRAM: SqlReturnTypeInference = explicit(SqlTypeName.VARBINARY, 8)

    /**
     * Type-inference strategy that always returns "CURSOR".
     */
    val CURSOR: SqlReturnTypeInference = explicit(SqlTypeName.CURSOR)

    /**
     * Type-inference strategy that always returns "COLUMN_LIST".
     */
    val COLUMN_LIST: SqlReturnTypeInference = explicit(SqlTypeName.COLUMN_LIST)

    /**
     * Type-inference strategy whereby the result type of a call is using its
     * operands biggest type, using the SQL:1999 rules described in "Data types
     * of results of aggregations". These rules are used in union, except,
     * intersect, case and other places.
     *
     * @see Glossary.SQL99 SQL:1999 Part 2 Section 9.3
     */
    val LEAST_RESTRICTIVE: SqlReturnTypeInference = SqlReturnTypeInference { opBinding ->
        opBinding.getTypeFactory().leastRestrictive(
            opBinding.collectOperandTypes()
        )
    }

    /**
     * Returns the same type as the multiset carries. The multiset type returned
     * is the least restrictive of the call's multiset operands
     */
    val MULTISET: SqlReturnTypeInference = label@ SqlReturnTypeInference { opBinding ->
        val newBinding = ExplicitOperatorBinding(
            opBinding,
            object : AbstractList<RelDataType?>() {
                // CHECKSTYLE: IGNORE 12
                @Override
                operator fun get(index: Int): RelDataType {
                    val type: RelDataType = opBinding.getOperandType(index)
                        .getComponentType()
                    assert(type != null)
                    return@label type
                }

                @Override
                fun size(): Int {
                    return@label opBinding.getOperandCount()
                }
            })
        val biggestElementType: RelDataType = LEAST_RESTRICTIVE.inferReturnType(newBinding)
        opBinding.getTypeFactory().createMultisetType(
            requireNonNull(
                biggestElementType
            ) { "can't infer element type for multiset of $newBinding" },
            -1
        )
    }

    /**
     * Returns a MULTISET type.
     *
     *
     * For example, given `INTEGER`, returns
     * `INTEGER MULTISET`.
     */
    val TO_MULTISET: SqlReturnTypeInference = ARG0.andThen(SqlTypeTransforms.TO_MULTISET)

    /**
     * Returns the element type of a MULTISET.
     */
    val MULTISET_ELEMENT_NULLABLE: SqlReturnTypeInference = MULTISET.andThen(SqlTypeTransforms.TO_MULTISET_ELEMENT_TYPE)

    /**
     * Same as [.MULTISET] but returns with nullability if any of the
     * operands is nullable.
     */
    val MULTISET_NULLABLE: SqlReturnTypeInference = MULTISET.andThen(SqlTypeTransforms.TO_NULLABLE)

    /**
     * Returns the type of the only column of a multiset.
     *
     *
     * For example, given `RECORD(x INTEGER) MULTISET`, returns
     * `INTEGER MULTISET`.
     */
    val MULTISET_PROJECT_ONLY: SqlReturnTypeInference = MULTISET.andThen(SqlTypeTransforms.ONLY_COLUMN)

    /**
     * Returns an ARRAY type.
     *
     *
     * For example, given `INTEGER`, returns
     * `INTEGER ARRAY`.
     */
    val TO_ARRAY: SqlReturnTypeInference = ARG0.andThen(SqlTypeTransforms.TO_ARRAY)

    /**
     * Returns a MAP type.
     *
     *
     * For example, given `Record(f0: INTEGER, f1: DATE)`, returns
     * `(INTEGER, DATE) MAP`.
     */
    val TO_MAP: SqlReturnTypeInference = ARG0.andThen(SqlTypeTransforms.TO_MAP)

    /**
     * Type-inference strategy whereby the result type of a call is
     * [.ARG0_INTERVAL_NULLABLE] and [.LEAST_RESTRICTIVE]. These rules
     * are used for integer division.
     */
    val INTEGER_QUOTIENT_NULLABLE: SqlReturnTypeInference = ARG0_INTERVAL_NULLABLE.orElse(LEAST_RESTRICTIVE)

    /**
     * Type-inference strategy for a call where the first argument is a decimal.
     * The result type of a call is a decimal with a scale of 0, and the same
     * precision and nullability as the first argument.
     */
    val DECIMAL_SCALE0: SqlReturnTypeInference = label@ SqlReturnTypeInference { opBinding ->
        val type1: RelDataType = opBinding.getOperandType(0)
        if (SqlTypeUtil.isDecimal(type1)) {
            if (type1.getScale() === 0) {
                return@label type1
            } else {
                val p: Int = type1.getPrecision()
                var ret: RelDataType
                ret = opBinding.getTypeFactory().createSqlType(
                    SqlTypeName.DECIMAL,
                    p,
                    0
                )
                if (type1.isNullable()) {
                    ret = opBinding.getTypeFactory()
                        .createTypeWithNullability(ret, true)
                }
                return@label ret
            }
        }
        null
    }

    /**
     * Type-inference strategy whereby the result type of a call is
     * [.DECIMAL_SCALE0] with a fallback to [.ARG0] This rule
     * is used for floor, ceiling.
     */
    val ARG0_OR_EXACT_NO_SCALE: SqlReturnTypeInference = DECIMAL_SCALE0.orElse(ARG0)

    /**
     * Type-inference strategy whereby the result type of a call is the decimal
     * product of two exact numeric operands where at least one of the operands
     * is a decimal.
     */
    val DECIMAL_PRODUCT: SqlReturnTypeInference = SqlReturnTypeInference { opBinding ->
        val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
        val type1: RelDataType = opBinding.getOperandType(0)
        val type2: RelDataType = opBinding.getOperandType(1)
        typeFactory.getTypeSystem().deriveDecimalMultiplyType(typeFactory, type1, type2)
    }

    /**
     * Same as [.DECIMAL_PRODUCT] but returns with nullability if any of
     * the operands is nullable by using
     * [org.apache.calcite.sql.type.SqlTypeTransforms.TO_NULLABLE].
     */
    val DECIMAL_PRODUCT_NULLABLE: SqlReturnTypeInference = DECIMAL_PRODUCT.andThen(SqlTypeTransforms.TO_NULLABLE)

    /**
     * Type-inference strategy whereby the result type of a call is
     * [.DECIMAL_PRODUCT_NULLABLE] with a fallback to
     * [.ARG0_INTERVAL_NULLABLE]
     * and [.LEAST_RESTRICTIVE].
     * These rules are used for multiplication.
     */
    val PRODUCT_NULLABLE: SqlReturnTypeInference = DECIMAL_PRODUCT_NULLABLE.orElse(ARG0_INTERVAL_NULLABLE)
        .orElse(LEAST_RESTRICTIVE)

    /**
     * Type-inference strategy whereby the result type of a call is the decimal
     * quotient of two exact numeric operands where at least one of the operands
     * is a decimal.
     */
    val DECIMAL_QUOTIENT: SqlReturnTypeInference = SqlReturnTypeInference { opBinding ->
        val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
        val type1: RelDataType = opBinding.getOperandType(0)
        val type2: RelDataType = opBinding.getOperandType(1)
        typeFactory.getTypeSystem().deriveDecimalDivideType(typeFactory, type1, type2)
    }

    /**
     * Same as [.DECIMAL_QUOTIENT] but returns with nullability if any of
     * the operands is nullable by using
     * [org.apache.calcite.sql.type.SqlTypeTransforms.TO_NULLABLE].
     */
    val DECIMAL_QUOTIENT_NULLABLE: SqlReturnTypeInference = DECIMAL_QUOTIENT.andThen(SqlTypeTransforms.TO_NULLABLE)

    /**
     * Type-inference strategy whereby the result type of a call is
     * [.DECIMAL_QUOTIENT_NULLABLE] with a fallback to
     * [.ARG0_INTERVAL_NULLABLE] and [.LEAST_RESTRICTIVE]. These rules
     * are used for division.
     */
    val QUOTIENT_NULLABLE: SqlReturnTypeInference = DECIMAL_QUOTIENT_NULLABLE.orElse(ARG0_INTERVAL_NULLABLE)
        .orElse(LEAST_RESTRICTIVE)

    /**
     * Type-inference strategy whereby the result type of a call is the decimal
     * sum of two exact numeric operands where at least one of the operands is a
     * decimal.
     */
    val DECIMAL_SUM: SqlReturnTypeInference = SqlReturnTypeInference { opBinding ->
        val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
        val type1: RelDataType = opBinding.getOperandType(0)
        val type2: RelDataType = opBinding.getOperandType(1)
        typeFactory.getTypeSystem().deriveDecimalPlusType(typeFactory, type1, type2)
    }

    /**
     * Same as [.DECIMAL_SUM] but returns with nullability if any
     * of the operands is nullable by using
     * [org.apache.calcite.sql.type.SqlTypeTransforms.TO_NULLABLE].
     */
    val DECIMAL_SUM_NULLABLE: SqlReturnTypeInference = DECIMAL_SUM.andThen(SqlTypeTransforms.TO_NULLABLE)

    /**
     * Type-inference strategy whereby the result type of a call is
     * [.DECIMAL_SUM_NULLABLE] with a fallback to [.LEAST_RESTRICTIVE]
     * These rules are used for addition and subtraction.
     */
    val NULLABLE_SUM: SqlReturnTypeInference = SqlReturnTypeInferenceChain(DECIMAL_SUM_NULLABLE, LEAST_RESTRICTIVE)
    val DECIMAL_MOD: SqlReturnTypeInference = SqlReturnTypeInference { opBinding ->
        val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
        val type1: RelDataType = opBinding.getOperandType(0)
        val type2: RelDataType = opBinding.getOperandType(1)
        typeFactory.getTypeSystem().deriveDecimalModType(typeFactory, type1, type2)
    }

    /**
     * Type-inference strategy whereby the result type of a call is the decimal
     * modulus of two exact numeric operands where at least one of the operands is a
     * decimal.
     */
    val DECIMAL_MOD_NULLABLE: SqlReturnTypeInference = DECIMAL_MOD.andThen(SqlTypeTransforms.TO_NULLABLE)

    /**
     * Type-inference strategy whereby the result type of a call is
     * [.DECIMAL_MOD_NULLABLE] with a fallback to [.ARG1_NULLABLE]
     * These rules are used for modulus.
     */
    val NULLABLE_MOD: SqlReturnTypeInference = DECIMAL_MOD_NULLABLE.orElse(ARG1_NULLABLE)

    /**
     * Type-inference strategy for concatenating two string arguments. The result
     * type of a call is:
     *
     *
     *  * the same type as the input types but with the combined length of the
     * two first types
     *  * if types are of char type the type with the highest coercibility will
     * be used
     *  * result is varying if either input is; otherwise fixed
     *
     *
     *
     * Pre-requisites:
     *
     *
     *  * input types must be of the same string type
     *  * types must be comparable without casting
     *
     */
    val DYADIC_STRING_SUM_PRECISION: SqlReturnTypeInference = SqlReturnTypeInference { opBinding ->
        val argType0: RelDataType = opBinding.getOperandType(0)
        val argType1: RelDataType = opBinding.getOperandType(1)
        val containsAnyType = (argType0.getSqlTypeName() === SqlTypeName.ANY
                || argType1.getSqlTypeName() === SqlTypeName.ANY)
        val containsNullType = (argType0.getSqlTypeName() === SqlTypeName.NULL
                || argType1.getSqlTypeName() === SqlTypeName.NULL)
        if (!containsAnyType
            && !containsNullType
            && !(SqlTypeUtil.inCharOrBinaryFamilies(argType0)
                    && SqlTypeUtil.inCharOrBinaryFamilies(argType1))
        ) {
            Preconditions.checkArgument(
                SqlTypeUtil.sameNamedType(argType0, argType1)
            )
        }
        var pickedCollation: SqlCollation? = null
        if (!containsAnyType
            && !containsNullType
            && SqlTypeUtil.inCharFamily(argType0)
        ) {
            if (!SqlTypeUtil.isCharTypeComparable(
                    opBinding.collectOperandTypes().subList(0, 2)
                )
            ) {
                throw opBinding.newError(
                    RESOURCE.typeNotComparable(
                        argType0.getFullTypeString(),
                        argType1.getFullTypeString()
                    )
                )
            }
            pickedCollation = requireNonNull(
                SqlCollation.getCoercibilityDyadicOperator(
                    getCollation(argType0), getCollation(argType1)
                )
            ) { "getCoercibilityDyadicOperator is null for $argType0 and $argType1" }
        }

        // Determine whether result is variable-length
        var typeName: SqlTypeName = argType0.getSqlTypeName()
        if (SqlTypeUtil.isBoundedVariableWidth(argType1)) {
            typeName = argType1.getSqlTypeName()
        }
        var ret: RelDataType
        val typePrecision: Int
        val x = argType0.getPrecision() as Long + argType1.getPrecision() as Long
        val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
        val typeSystem: RelDataTypeSystem = typeFactory.getTypeSystem()
        typePrecision =
            if (argType0.getPrecision() === RelDataType.PRECISION_NOT_SPECIFIED || argType1.getPrecision() === RelDataType.PRECISION_NOT_SPECIFIED || x > typeSystem.getMaxPrecision(
                    typeName
                )
            ) {
                RelDataType.PRECISION_NOT_SPECIFIED
            } else {
                x.toInt()
            }
        ret = typeFactory.createSqlType(typeName, typePrecision)
        if (null != pickedCollation) {
            val pickedType: RelDataType
            pickedType = if (getCollation(argType0).equals(pickedCollation)) {
                argType0
            } else if (getCollation(argType1).equals(pickedCollation)) {
                argType1
            } else {
                throw AssertionError(
                    "should never come here, "
                            + "argType0=" + argType0 + ", argType1=" + argType1
                )
            }
            ret = typeFactory.createTypeWithCharsetAndCollation(
                ret,
                getCharset(pickedType), getCollation(pickedType)
            )
        }
        if (ret.getSqlTypeName() === SqlTypeName.NULL) {
            ret = typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.VARCHAR), true
            )
        }
        ret
    }

    /**
     * Type-inference strategy for String concatenation.
     * Result is varying if either input is; otherwise fixed.
     * For example,
     *
     *
     * concat(cast('a' as varchar(2)), cast('b' as varchar(3)),cast('c' as varchar(2)))
     * returns varchar(7).
     *
     *
     * concat(cast('a' as varchar), cast('b' as varchar(2), cast('c' as varchar(2))))
     * returns varchar.
     *
     *
     * concat(cast('a' as varchar(65535)), cast('b' as varchar(2)), cast('c' as varchar(2)))
     * returns varchar.
     */
    val MULTIVALENT_STRING_SUM_PRECISION: SqlReturnTypeInference = SqlReturnTypeInference { opBinding ->
        var hasPrecisionNotSpecifiedOperand = false
        var precisionOverflow = false
        val typePrecision: Int
        var amount: Long = 0
        val operandTypes: List<RelDataType> = opBinding.collectOperandTypes()
        val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
        val typeSystem: RelDataTypeSystem = typeFactory.getTypeSystem()
        for (operandType in operandTypes) {
            val operandPrecision: Int = operandType.getPrecision()
            amount = operandPrecision.toLong() + amount
            if (operandPrecision == RelDataType.PRECISION_NOT_SPECIFIED) {
                hasPrecisionNotSpecifiedOperand = true
                break
            }
            if (amount > typeSystem.getMaxPrecision(SqlTypeName.VARCHAR)) {
                precisionOverflow = true
                break
            }
        }
        typePrecision = if (hasPrecisionNotSpecifiedOperand || precisionOverflow) {
            RelDataType.PRECISION_NOT_SPECIFIED
        } else {
            amount.toInt()
        }
        opBinding.getTypeFactory()
            .createSqlType(SqlTypeName.VARCHAR, typePrecision)
    }

    /**
     * Same as [.MULTIVALENT_STRING_SUM_PRECISION] and using
     * [org.apache.calcite.sql.type.SqlTypeTransforms.TO_NULLABLE].
     */
    val MULTIVALENT_STRING_SUM_PRECISION_NULLABLE: SqlReturnTypeInference =
        MULTIVALENT_STRING_SUM_PRECISION.andThen(SqlTypeTransforms.TO_NULLABLE)

    /**
     * Same as [.DYADIC_STRING_SUM_PRECISION] and using
     * [org.apache.calcite.sql.type.SqlTypeTransforms.TO_NULLABLE],
     * [org.apache.calcite.sql.type.SqlTypeTransforms.TO_VARYING].
     */
    val DYADIC_STRING_SUM_PRECISION_NULLABLE_VARYING: SqlReturnTypeInference =
        DYADIC_STRING_SUM_PRECISION.andThen(SqlTypeTransforms.TO_NULLABLE)
            .andThen(SqlTypeTransforms.TO_VARYING)

    /**
     * Same as [.DYADIC_STRING_SUM_PRECISION] and using
     * [org.apache.calcite.sql.type.SqlTypeTransforms.TO_NULLABLE].
     */
    val DYADIC_STRING_SUM_PRECISION_NULLABLE: SqlReturnTypeInference =
        DYADIC_STRING_SUM_PRECISION.andThen(SqlTypeTransforms.TO_NULLABLE)

    /**
     * Type-inference strategy where the expression is assumed to be registered
     * as a [org.apache.calcite.sql.validate.SqlValidatorNamespace], and
     * therefore the result type of the call is the type of that namespace.
     */
    val SCOPE: SqlReturnTypeInference = SqlReturnTypeInference { opBinding ->
        val callBinding: SqlCallBinding = opBinding as SqlCallBinding
        val ns: SqlValidatorNamespace = getNamespace(callBinding)
        ns.getRowType()
    }

    /**
     * Returns a multiset of column #0 of a multiset. For example, given
     * `RECORD(x INTEGER, y DATE) MULTISET`, returns `INTEGER
     * MULTISET`.
     */
    val MULTISET_PROJECT0: SqlReturnTypeInference = SqlReturnTypeInference { opBinding ->
        assert(opBinding.getOperandCount() === 1)
        val recordMultisetType: RelDataType = opBinding.getOperandType(0)
        val multisetType: RelDataType = recordMultisetType.getComponentType()
        assert(multisetType != null) {
            ("expected a multiset type: "
                    + recordMultisetType)
        }
        val fields: List<RelDataTypeField> = multisetType.getFieldList()
        assert(fields.size() > 0)
        val firstColType: RelDataType = fields[0].getType()
        opBinding.getTypeFactory().createMultisetType(
            firstColType,
            -1
        )
    }

    /**
     * Returns a multiset of the first column of a multiset. For example, given
     * `INTEGER MULTISET`, returns `RECORD(x INTEGER)
     * MULTISET`.
     */
    val MULTISET_RECORD: SqlReturnTypeInference = SqlReturnTypeInference { opBinding ->
        assert(opBinding.getOperandCount() === 1)
        val multisetType: RelDataType = opBinding.getOperandType(0)
        val componentType: RelDataType = multisetType.getComponentType()
        assert(componentType != null) {
            ("expected a multiset type: "
                    + multisetType)
        }
        val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
        val type: RelDataType = typeFactory.builder()
            .add(SqlUtil.deriveAliasFromOrdinal(0), componentType).build()
        typeFactory.createMultisetType(type, -1)
    }

    /**
     * Returns the field type of a structured type which has only one field. For
     * example, given `RECORD(x INTEGER)` returns `INTEGER`.
     */
    val RECORD_TO_SCALAR: SqlReturnTypeInference = SqlReturnTypeInference { opBinding ->
        assert(opBinding.getOperandCount() === 1)
        val recordType: RelDataType = opBinding.getOperandType(0)
        val isStruct: Boolean = recordType.isStruct()
        val fieldCount: Int = recordType.getFieldCount()
        assert(isStruct && fieldCount == 1)
        val fieldType: RelDataTypeField = recordType.getFieldList().get(0)
        assert(fieldType != null) {
            ("expected a record type with one field: "
                    + recordType)
        }
        val firstColType: RelDataType = fieldType.getType()
        opBinding.getTypeFactory().createTypeWithNullability(
            firstColType,
            true
        )
    }

    /**
     * Type-inference strategy for SUM aggregate function inferred from the
     * operand type, and nullable if the call occurs within a "GROUP BY ()"
     * query. E.g. in "select sum(x) as s from empty", s may be null. Also,
     * with the default implementation of RelDataTypeSystem, s has the same
     * type name as x.
     */
    val AGG_SUM: SqlReturnTypeInference = label@ SqlReturnTypeInference { opBinding ->
        val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
        val type: RelDataType = typeFactory.getTypeSystem()
            .deriveSumType(typeFactory, opBinding.getOperandType(0))
        if (opBinding.getGroupCount() === 0 || opBinding.hasFilter()) {
            return@label typeFactory.createTypeWithNullability(type, true)
        } else {
            return@label type
        }
    }

    /**
     * Type-inference strategy for $SUM0 aggregate function inferred from the
     * operand type. By default the inferred type is identical to the operand
     * type. E.g. in "select $sum0(x) as s from empty", s has the same type as
     * x.
     */
    val AGG_SUM_EMPTY_IS_ZERO: SqlReturnTypeInference = SqlReturnTypeInference { opBinding ->
        val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
        val sumType: RelDataType = typeFactory.getTypeSystem()
            .deriveSumType(typeFactory, opBinding.getOperandType(0))
        typeFactory.createTypeWithNullability(sumType, false)
    }

    /**
     * Type-inference strategy for the `CUME_DIST` and `PERCENT_RANK`
     * aggregate functions.
     */
    val FRACTIONAL_RANK: SqlReturnTypeInference = SqlReturnTypeInference { opBinding ->
        val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
        typeFactory.getTypeSystem().deriveFractionalRankType(typeFactory)
    }

    /**
     * Type-inference strategy for the `NTILE`, `RANK`,
     * `DENSE_RANK`, and `ROW_NUMBER` aggregate functions.
     */
    val RANK: SqlReturnTypeInference = SqlReturnTypeInference { opBinding ->
        val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
        typeFactory.getTypeSystem().deriveRankType(typeFactory)
    }
    val AVG_AGG_FUNCTION: SqlReturnTypeInference = label@ SqlReturnTypeInference { opBinding ->
        val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
        val relDataType: RelDataType = typeFactory.getTypeSystem().deriveAvgAggType(
            typeFactory,
            opBinding.getOperandType(0)
        )
        if (opBinding.getGroupCount() === 0 || opBinding.hasFilter()) {
            return@label typeFactory.createTypeWithNullability(relDataType, true)
        } else {
            return@label relDataType
        }
    }
    val COVAR_REGR_FUNCTION: SqlReturnTypeInference = label@ SqlReturnTypeInference { opBinding ->
        val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
        val relDataType: RelDataType = typeFactory.getTypeSystem().deriveCovarType(
            typeFactory,
            opBinding.getOperandType(0), opBinding.getOperandType(1)
        )
        if (opBinding.getGroupCount() === 0 || opBinding.hasFilter()) {
            return@label typeFactory.createTypeWithNullability(relDataType, true)
        } else {
            return@label relDataType
        }
    }
}
