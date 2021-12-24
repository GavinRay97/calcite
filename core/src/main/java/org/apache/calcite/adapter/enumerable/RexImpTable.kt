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
package org.apache.calcite.adapter.enumerable

import org.apache.calcite.DataContext

/**
 * Contains implementations of Rex operators as Java code.
 */
class RexImpTable @SuppressWarnings("method.invocation.invalid") internal constructor() {
    private val map: Map<SqlOperator, RexCallImplementor> = HashMap()
    private val aggMap: Map<SqlAggFunction, Supplier<out AggImplementor?>> = HashMap()
    private val winAggMap: Map<SqlAggFunction, Supplier<out WinAggImplementor?>> = HashMap()
    private val matchMap: Map<SqlMatchFunction, Supplier<out MatchImplementor?>> = HashMap()
    private val tvfImplementorMap: Map<SqlOperator, Supplier<out TableFunctionCallImplementor?>> = HashMap()

    init {
        defineMethod(THROW_UNLESS, BuiltInMethod.THROW_UNLESS.method, NullPolicy.NONE)
        defineMethod(ROW, BuiltInMethod.ARRAY.method, NullPolicy.NONE)
        defineMethod(UPPER, BuiltInMethod.UPPER.method, NullPolicy.STRICT)
        defineMethod(LOWER, BuiltInMethod.LOWER.method, NullPolicy.STRICT)
        defineMethod(INITCAP, BuiltInMethod.INITCAP.method, NullPolicy.STRICT)
        defineMethod(TO_BASE64, BuiltInMethod.TO_BASE64.method, NullPolicy.STRICT)
        defineMethod(FROM_BASE64, BuiltInMethod.FROM_BASE64.method, NullPolicy.STRICT)
        defineMethod(MD5, BuiltInMethod.MD5.method, NullPolicy.STRICT)
        defineMethod(SHA1, BuiltInMethod.SHA1.method, NullPolicy.STRICT)
        defineMethod(SUBSTRING, BuiltInMethod.SUBSTRING.method, NullPolicy.STRICT)
        defineMethod(LEFT, BuiltInMethod.LEFT.method, NullPolicy.ANY)
        defineMethod(RIGHT, BuiltInMethod.RIGHT.method, NullPolicy.ANY)
        defineMethod(REPLACE, BuiltInMethod.REPLACE.method, NullPolicy.STRICT)
        defineMethod(TRANSLATE3, BuiltInMethod.TRANSLATE3.method, NullPolicy.STRICT)
        defineMethod(CHR, "chr", NullPolicy.STRICT)
        defineMethod(
            CHARACTER_LENGTH, BuiltInMethod.CHAR_LENGTH.method,
            NullPolicy.STRICT
        )
        defineMethod(
            CHAR_LENGTH, BuiltInMethod.CHAR_LENGTH.method,
            NullPolicy.STRICT
        )
        defineMethod(
            OCTET_LENGTH, BuiltInMethod.OCTET_LENGTH.method,
            NullPolicy.STRICT
        )
        defineMethod(
            CONCAT, BuiltInMethod.STRING_CONCAT.method,
            NullPolicy.STRICT
        )
        defineMethod(
            CONCAT_FUNCTION, BuiltInMethod.MULTI_STRING_CONCAT.method,
            NullPolicy.STRICT
        )
        defineMethod(CONCAT2, BuiltInMethod.STRING_CONCAT.method, NullPolicy.STRICT)
        defineMethod(OVERLAY, BuiltInMethod.OVERLAY.method, NullPolicy.STRICT)
        defineMethod(POSITION, BuiltInMethod.POSITION.method, NullPolicy.STRICT)
        defineMethod(ASCII, BuiltInMethod.ASCII.method, NullPolicy.STRICT)
        defineMethod(REPEAT, BuiltInMethod.REPEAT.method, NullPolicy.STRICT)
        defineMethod(SPACE, BuiltInMethod.SPACE.method, NullPolicy.STRICT)
        defineMethod(STRCMP, BuiltInMethod.STRCMP.method, NullPolicy.STRICT)
        defineMethod(SOUNDEX, BuiltInMethod.SOUNDEX.method, NullPolicy.STRICT)
        defineMethod(DIFFERENCE, BuiltInMethod.DIFFERENCE.method, NullPolicy.STRICT)
        defineMethod(REVERSE, BuiltInMethod.REVERSE.method, NullPolicy.STRICT)
        map.put(TRIM, TrimImplementor())

        // logical
        map.put(AND, LogicalAndImplementor())
        map.put(OR, LogicalOrImplementor())
        map.put(NOT, LogicalNotImplementor())

        // comparisons
        defineBinary(LESS_THAN, LessThan, NullPolicy.STRICT, "lt")
        defineBinary(LESS_THAN_OR_EQUAL, LessThanOrEqual, NullPolicy.STRICT, "le")
        defineBinary(GREATER_THAN, GreaterThan, NullPolicy.STRICT, "gt")
        defineBinary(
            GREATER_THAN_OR_EQUAL, GreaterThanOrEqual, NullPolicy.STRICT,
            "ge"
        )
        defineBinary(EQUALS, Equal, NullPolicy.STRICT, "eq")
        defineBinary(NOT_EQUALS, NotEqual, NullPolicy.STRICT, "ne")

        // arithmetic
        defineBinary(PLUS, Add, NullPolicy.STRICT, "plus")
        defineBinary(MINUS, Subtract, NullPolicy.STRICT, "minus")
        defineBinary(MULTIPLY, Multiply, NullPolicy.STRICT, "multiply")
        defineBinary(DIVIDE, Divide, NullPolicy.STRICT, "divide")
        defineBinary(DIVIDE_INTEGER, Divide, NullPolicy.STRICT, "divide")
        defineUnary(
            UNARY_MINUS, Negate, NullPolicy.STRICT,
            BuiltInMethod.BIG_DECIMAL_NEGATE.getMethodName()
        )
        defineUnary(UNARY_PLUS, UnaryPlus, NullPolicy.STRICT, null)
        defineMethod(MOD, "mod", NullPolicy.STRICT)
        defineMethod(EXP, "exp", NullPolicy.STRICT)
        defineMethod(POWER, "power", NullPolicy.STRICT)
        defineMethod(LN, "ln", NullPolicy.STRICT)
        defineMethod(LOG10, "log10", NullPolicy.STRICT)
        defineMethod(ABS, "abs", NullPolicy.STRICT)
        map.put(RAND, RandImplementor())
        map.put(RAND_INTEGER, RandIntegerImplementor())
        defineMethod(ACOS, "acos", NullPolicy.STRICT)
        defineMethod(ASIN, "asin", NullPolicy.STRICT)
        defineMethod(ATAN, "atan", NullPolicy.STRICT)
        defineMethod(ATAN2, "atan2", NullPolicy.STRICT)
        defineMethod(CBRT, "cbrt", NullPolicy.STRICT)
        defineMethod(COS, "cos", NullPolicy.STRICT)
        defineMethod(COSH, "cosh", NullPolicy.STRICT)
        defineMethod(COT, "cot", NullPolicy.STRICT)
        defineMethod(DEGREES, "degrees", NullPolicy.STRICT)
        defineMethod(RADIANS, "radians", NullPolicy.STRICT)
        defineMethod(ROUND, "sround", NullPolicy.STRICT)
        defineMethod(SIGN, "sign", NullPolicy.STRICT)
        defineMethod(SIN, "sin", NullPolicy.STRICT)
        defineMethod(SINH, "sinh", NullPolicy.STRICT)
        defineMethod(TAN, "tan", NullPolicy.STRICT)
        defineMethod(TANH, "tanh", NullPolicy.STRICT)
        defineMethod(TRUNCATE, "struncate", NullPolicy.STRICT)
        map.put(PI, PiImplementor())

        // datetime
        map.put(DATETIME_PLUS, DatetimeArithmeticImplementor())
        map.put(MINUS_DATE, DatetimeArithmeticImplementor())
        map.put(EXTRACT, ExtractImplementor())
        map.put(
            FLOOR,
            FloorImplementor(
                BuiltInMethod.FLOOR.method.getName(),
                BuiltInMethod.UNIX_TIMESTAMP_FLOOR.method,
                BuiltInMethod.UNIX_DATE_FLOOR.method
            )
        )
        map.put(
            CEIL,
            FloorImplementor(
                BuiltInMethod.CEIL.method.getName(),
                BuiltInMethod.UNIX_TIMESTAMP_CEIL.method,
                BuiltInMethod.UNIX_DATE_CEIL.method
            )
        )
        defineMethod(LAST_DAY, "lastDay", NullPolicy.STRICT)
        map.put(
            DAYNAME,
            PeriodNameImplementor(
                "dayName",
                BuiltInMethod.DAYNAME_WITH_TIMESTAMP,
                BuiltInMethod.DAYNAME_WITH_DATE
            )
        )
        map.put(
            MONTHNAME,
            PeriodNameImplementor(
                "monthName",
                BuiltInMethod.MONTHNAME_WITH_TIMESTAMP,
                BuiltInMethod.MONTHNAME_WITH_DATE
            )
        )
        defineMethod(TIMESTAMP_SECONDS, "timestampSeconds", NullPolicy.STRICT)
        defineMethod(TIMESTAMP_MILLIS, "timestampMillis", NullPolicy.STRICT)
        defineMethod(TIMESTAMP_MICROS, "timestampMicros", NullPolicy.STRICT)
        defineMethod(UNIX_SECONDS, "unixSeconds", NullPolicy.STRICT)
        defineMethod(UNIX_MILLIS, "unixMillis", NullPolicy.STRICT)
        defineMethod(UNIX_MICROS, "unixMicros", NullPolicy.STRICT)
        defineMethod(DATE_FROM_UNIX_DATE, "dateFromUnixDate", NullPolicy.STRICT)
        defineMethod(UNIX_DATE, "unixDate", NullPolicy.STRICT)
        map.put(IS_NULL, IsNullImplementor())
        map.put(IS_NOT_NULL, IsNotNullImplementor())
        map.put(IS_TRUE, IsTrueImplementor())
        map.put(IS_NOT_TRUE, IsNotTrueImplementor())
        map.put(IS_FALSE, IsFalseImplementor())
        map.put(IS_NOT_FALSE, IsNotFalseImplementor())

        // LIKE, ILIKE and SIMILAR
        map.put(
            LIKE,
            MethodImplementor(
                BuiltInMethod.LIKE.method, NullPolicy.STRICT,
                false
            )
        )
        map.put(
            ILIKE,
            MethodImplementor(
                BuiltInMethod.ILIKE.method, NullPolicy.STRICT,
                false
            )
        )
        map.put(
            RLIKE,
            MethodImplementor(
                BuiltInMethod.RLIKE.method, NullPolicy.STRICT,
                false
            )
        )
        map.put(
            SIMILAR_TO,
            MethodImplementor(
                BuiltInMethod.SIMILAR.method, NullPolicy.STRICT,
                false
            )
        )

        // POSIX REGEX
        val posixRegexImplementorCaseSensitive: MethodImplementor = PosixRegexMethodImplementor(true)
        val posixRegexImplementorCaseInsensitive: MethodImplementor = PosixRegexMethodImplementor(false)
        map.put(
            SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE,
            posixRegexImplementorCaseInsensitive
        )
        map.put(
            SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE,
            posixRegexImplementorCaseSensitive
        )
        map.put(
            SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_INSENSITIVE,
            NotImplementor.of(posixRegexImplementorCaseInsensitive)
        )
        map.put(
            SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_SENSITIVE,
            NotImplementor.of(posixRegexImplementorCaseSensitive)
        )
        map.put(REGEXP_REPLACE, RegexpReplaceImplementor())

        // Multisets & arrays
        defineMethod(
            CARDINALITY, BuiltInMethod.COLLECTION_SIZE.method,
            NullPolicy.STRICT
        )
        defineMethod(
            ARRAY_LENGTH, BuiltInMethod.COLLECTION_SIZE.method,
            NullPolicy.STRICT
        )
        defineMethod(SLICE, BuiltInMethod.SLICE.method, NullPolicy.NONE)
        defineMethod(ELEMENT, BuiltInMethod.ELEMENT.method, NullPolicy.STRICT)
        defineMethod(STRUCT_ACCESS, BuiltInMethod.STRUCT_ACCESS.method, NullPolicy.ANY)
        defineMethod(MEMBER_OF, BuiltInMethod.MEMBER_OF.method, NullPolicy.NONE)
        defineMethod(ARRAY_REVERSE, BuiltInMethod.ARRAY_REVERSE.method, NullPolicy.STRICT)
        map.put(ARRAY_CONCAT, ArrayConcatImplementor())
        val isEmptyImplementor = MethodImplementor(
            BuiltInMethod.IS_EMPTY.method, NullPolicy.NONE,
            false
        )
        map.put(IS_EMPTY, isEmptyImplementor)
        map.put(IS_NOT_EMPTY, NotImplementor.of(isEmptyImplementor))
        val isASetImplementor = MethodImplementor(
            BuiltInMethod.IS_A_SET.method, NullPolicy.NONE,
            false
        )
        map.put(IS_A_SET, isASetImplementor)
        map.put(IS_NOT_A_SET, NotImplementor.of(isASetImplementor))
        defineMethod(
            MULTISET_INTERSECT_DISTINCT,
            BuiltInMethod.MULTISET_INTERSECT_DISTINCT.method, NullPolicy.NONE
        )
        defineMethod(
            MULTISET_INTERSECT,
            BuiltInMethod.MULTISET_INTERSECT_ALL.method, NullPolicy.NONE
        )
        defineMethod(
            MULTISET_EXCEPT_DISTINCT,
            BuiltInMethod.MULTISET_EXCEPT_DISTINCT.method, NullPolicy.NONE
        )
        defineMethod(MULTISET_EXCEPT, BuiltInMethod.MULTISET_EXCEPT_ALL.method, NullPolicy.NONE)
        defineMethod(
            MULTISET_UNION_DISTINCT,
            BuiltInMethod.MULTISET_UNION_DISTINCT.method, NullPolicy.NONE
        )
        defineMethod(MULTISET_UNION, BuiltInMethod.MULTISET_UNION_ALL.method, NullPolicy.NONE)
        val subMultisetImplementor = MethodImplementor(BuiltInMethod.SUBMULTISET_OF.method, NullPolicy.NONE, false)
        map.put(SUBMULTISET_OF, subMultisetImplementor)
        map.put(NOT_SUBMULTISET_OF, NotImplementor.of(subMultisetImplementor))
        map.put(COALESCE, CoalesceImplementor())
        map.put(CAST, CastImplementor())
        map.put(DATE, CastImplementor())
        map.put(REINTERPRET, ReinterpretImplementor())
        val value: RexCallImplementor = ValueConstructorImplementor()
        map.put(MAP_VALUE_CONSTRUCTOR, value)
        map.put(ARRAY_VALUE_CONSTRUCTOR, value)
        map.put(ITEM, ItemImplementor())
        map.put(DEFAULT, DefaultImplementor())

        // Sequences
        defineMethod(
            CURRENT_VALUE, BuiltInMethod.SEQUENCE_CURRENT_VALUE.method,
            NullPolicy.STRICT
        )
        defineMethod(
            NEXT_VALUE, BuiltInMethod.SEQUENCE_NEXT_VALUE.method,
            NullPolicy.STRICT
        )

        // Compression Operators
        defineMethod(COMPRESS, BuiltInMethod.COMPRESS.method, NullPolicy.ARG0)

        // Xml Operators
        defineMethod(EXTRACT_VALUE, BuiltInMethod.EXTRACT_VALUE.method, NullPolicy.ARG0)
        defineMethod(XML_TRANSFORM, BuiltInMethod.XML_TRANSFORM.method, NullPolicy.ARG0)
        defineMethod(EXTRACT_XML, BuiltInMethod.EXTRACT_XML.method, NullPolicy.ARG0)
        defineMethod(EXISTS_NODE, BuiltInMethod.EXISTS_NODE.method, NullPolicy.ARG0)

        // Json Operators
        defineMethod(
            JSON_VALUE_EXPRESSION,
            BuiltInMethod.JSON_VALUE_EXPRESSION.method, NullPolicy.STRICT
        )
        defineMethod(JSON_EXISTS, BuiltInMethod.JSON_EXISTS.method, NullPolicy.ARG0)
        map.put(
            JSON_VALUE,
            JsonValueImplementor(BuiltInMethod.JSON_VALUE.method)
        )
        defineMethod(JSON_QUERY, BuiltInMethod.JSON_QUERY.method, NullPolicy.ARG0)
        defineMethod(JSON_TYPE, BuiltInMethod.JSON_TYPE.method, NullPolicy.ARG0)
        defineMethod(JSON_DEPTH, BuiltInMethod.JSON_DEPTH.method, NullPolicy.ARG0)
        defineMethod(JSON_KEYS, BuiltInMethod.JSON_KEYS.method, NullPolicy.ARG0)
        defineMethod(JSON_PRETTY, BuiltInMethod.JSON_PRETTY.method, NullPolicy.ARG0)
        defineMethod(JSON_LENGTH, BuiltInMethod.JSON_LENGTH.method, NullPolicy.ARG0)
        defineMethod(JSON_REMOVE, BuiltInMethod.JSON_REMOVE.method, NullPolicy.ARG0)
        defineMethod(JSON_STORAGE_SIZE, BuiltInMethod.JSON_STORAGE_SIZE.method, NullPolicy.ARG0)
        defineMethod(JSON_OBJECT, BuiltInMethod.JSON_OBJECT.method, NullPolicy.NONE)
        defineMethod(JSON_ARRAY, BuiltInMethod.JSON_ARRAY.method, NullPolicy.NONE)
        aggMap.put(
            JSON_OBJECTAGG.with(SqlJsonConstructorNullClause.ABSENT_ON_NULL),
            JsonObjectAggImplementor.supplierFor(BuiltInMethod.JSON_OBJECTAGG_ADD.method)
        )
        aggMap.put(
            JSON_OBJECTAGG.with(SqlJsonConstructorNullClause.NULL_ON_NULL),
            JsonObjectAggImplementor.supplierFor(BuiltInMethod.JSON_OBJECTAGG_ADD.method)
        )
        aggMap.put(
            JSON_ARRAYAGG.with(SqlJsonConstructorNullClause.ABSENT_ON_NULL),
            JsonArrayAggImplementor.supplierFor(BuiltInMethod.JSON_ARRAYAGG_ADD.method)
        )
        aggMap.put(
            JSON_ARRAYAGG.with(SqlJsonConstructorNullClause.NULL_ON_NULL),
            JsonArrayAggImplementor.supplierFor(BuiltInMethod.JSON_ARRAYAGG_ADD.method)
        )
        map.put(
            IS_JSON_VALUE,
            MethodImplementor(
                BuiltInMethod.IS_JSON_VALUE.method,
                NullPolicy.NONE, false
            )
        )
        map.put(
            IS_JSON_OBJECT,
            MethodImplementor(
                BuiltInMethod.IS_JSON_OBJECT.method,
                NullPolicy.NONE, false
            )
        )
        map.put(
            IS_JSON_ARRAY,
            MethodImplementor(
                BuiltInMethod.IS_JSON_ARRAY.method,
                NullPolicy.NONE, false
            )
        )
        map.put(
            IS_JSON_SCALAR,
            MethodImplementor(
                BuiltInMethod.IS_JSON_SCALAR.method,
                NullPolicy.NONE, false
            )
        )
        map.put(
            IS_NOT_JSON_VALUE,
            NotImplementor.of(
                MethodImplementor(
                    BuiltInMethod.IS_JSON_VALUE.method,
                    NullPolicy.NONE, false
                )
            )
        )
        map.put(
            IS_NOT_JSON_OBJECT,
            NotImplementor.of(
                MethodImplementor(
                    BuiltInMethod.IS_JSON_OBJECT.method,
                    NullPolicy.NONE, false
                )
            )
        )
        map.put(
            IS_NOT_JSON_ARRAY,
            NotImplementor.of(
                MethodImplementor(
                    BuiltInMethod.IS_JSON_ARRAY.method,
                    NullPolicy.NONE, false
                )
            )
        )
        map.put(
            IS_NOT_JSON_SCALAR,
            NotImplementor.of(
                MethodImplementor(
                    BuiltInMethod.IS_JSON_SCALAR.method,
                    NullPolicy.NONE, false
                )
            )
        )

        // System functions
        val systemFunctionImplementor = SystemFunctionImplementor()
        map.put(USER, systemFunctionImplementor)
        map.put(CURRENT_USER, systemFunctionImplementor)
        map.put(SESSION_USER, systemFunctionImplementor)
        map.put(SYSTEM_USER, systemFunctionImplementor)
        map.put(CURRENT_PATH, systemFunctionImplementor)
        map.put(CURRENT_ROLE, systemFunctionImplementor)
        map.put(CURRENT_CATALOG, systemFunctionImplementor)

        // Current time functions
        map.put(CURRENT_TIME, systemFunctionImplementor)
        map.put(CURRENT_TIMESTAMP, systemFunctionImplementor)
        map.put(CURRENT_DATE, systemFunctionImplementor)
        map.put(LOCALTIME, systemFunctionImplementor)
        map.put(LOCALTIMESTAMP, systemFunctionImplementor)
        aggMap.put(COUNT, constructorSupplier(CountImplementor::class.java))
        aggMap.put(REGR_COUNT, constructorSupplier(CountImplementor::class.java))
        aggMap.put(SUM0, constructorSupplier(SumImplementor::class.java))
        aggMap.put(SUM, constructorSupplier(SumImplementor::class.java))
        val minMax: Supplier<MinMaxImplementor> = constructorSupplier(
            MinMaxImplementor::class.java
        )
        aggMap.put(MIN, minMax)
        aggMap.put(MAX, minMax)
        aggMap.put(ANY_VALUE, minMax)
        aggMap.put(SOME, minMax)
        aggMap.put(EVERY, minMax)
        aggMap.put(BOOL_AND, minMax)
        aggMap.put(BOOL_OR, minMax)
        aggMap.put(LOGICAL_AND, minMax)
        aggMap.put(LOGICAL_OR, minMax)
        val bitop: Supplier<BitOpImplementor> = constructorSupplier(
            BitOpImplementor::class.java
        )
        aggMap.put(BIT_AND, bitop)
        aggMap.put(BIT_OR, bitop)
        aggMap.put(BIT_XOR, bitop)
        aggMap.put(SINGLE_VALUE, constructorSupplier(SingleValueImplementor::class.java))
        aggMap.put(COLLECT, constructorSupplier(CollectImplementor::class.java))
        aggMap.put(ARRAY_AGG, constructorSupplier(CollectImplementor::class.java))
        aggMap.put(LISTAGG, constructorSupplier(ListaggImplementor::class.java))
        aggMap.put(FUSION, constructorSupplier(FusionImplementor::class.java))
        aggMap.put(MODE, constructorSupplier(ModeImplementor::class.java))
        aggMap.put(ARRAY_CONCAT_AGG, constructorSupplier(FusionImplementor::class.java))
        aggMap.put(INTERSECTION, constructorSupplier(IntersectionImplementor::class.java))
        val grouping: Supplier<GroupingImplementor> = constructorSupplier(
            GroupingImplementor::class.java
        )
        aggMap.put(GROUPING, grouping)
        aggMap.put(GROUPING_ID, grouping)
        winAggMap.put(RANK, constructorSupplier(RankImplementor::class.java))
        winAggMap.put(DENSE_RANK, constructorSupplier(DenseRankImplementor::class.java))
        winAggMap.put(ROW_NUMBER, constructorSupplier(RowNumberImplementor::class.java))
        winAggMap.put(
            FIRST_VALUE,
            constructorSupplier(FirstValueImplementor::class.java)
        )
        winAggMap.put(NTH_VALUE, constructorSupplier(NthValueImplementor::class.java))
        winAggMap.put(LAST_VALUE, constructorSupplier(LastValueImplementor::class.java))
        winAggMap.put(LEAD, constructorSupplier(LeadImplementor::class.java))
        winAggMap.put(LAG, constructorSupplier(LagImplementor::class.java))
        winAggMap.put(NTILE, constructorSupplier(NtileImplementor::class.java))
        winAggMap.put(COUNT, constructorSupplier(CountWinImplementor::class.java))
        winAggMap.put(REGR_COUNT, constructorSupplier(CountWinImplementor::class.java))

        // Functions for MATCH_RECOGNIZE
        matchMap.put(CLASSIFIER) { ClassifierImplementor() }
        matchMap.put(LAST) { LastImplementor() }
        tvfImplementorMap.put(TUMBLE) { TumbleImplementor() }
        tvfImplementorMap.put(HOP) { HopImplementor() }
        tvfImplementorMap.put(SESSION) { SessionImplementor() }
    }

    private fun defineMethod(
        operator: SqlOperator, functionName: String,
        nullPolicy: NullPolicy
    ) {
        map.put(
            operator,
            MethodNameImplementor(functionName, nullPolicy, false)
        )
    }

    private fun defineMethod(
        operator: SqlOperator, method: Method,
        nullPolicy: NullPolicy
    ) {
        map.put(operator, MethodImplementor(method, nullPolicy, false))
    }

    private fun defineUnary(
        operator: SqlOperator, expressionType: ExpressionType,
        nullPolicy: NullPolicy, @Nullable backupMethodName: String?
    ) {
        map.put(operator, UnaryImplementor(expressionType, nullPolicy, backupMethodName))
    }

    private fun defineBinary(
        operator: SqlOperator, expressionType: ExpressionType,
        nullPolicy: NullPolicy, backupMethodName: String
    ) {
        map.put(
            operator,
            BinaryImplementor(
                nullPolicy, true, expressionType,
                backupMethodName
            )
        )
    }

    @Nullable
    operator fun get(operator: SqlOperator): RexCallImplementor? {
        if (operator is SqlUserDefinedFunction) {
            val udf: org.apache.calcite.schema.Function =
                (operator as SqlUserDefinedFunction).getFunction() as? ImplementableFunction
                    ?: throw IllegalStateException(
                        "User defined function " + operator
                                + " must implement ImplementableFunction"
                    )
            val implementor: CallImplementor = (udf as ImplementableFunction).getImplementor()
            return wrapAsRexCallImplementor(implementor)
        } else if (operator is SqlTypeConstructorFunction) {
            return map[SqlStdOperatorTable.ROW]
        }
        return map[operator]
    }

    @Nullable
    operator fun get(
        aggregation: SqlAggFunction,
        forWindowAggregate: Boolean
    ): AggImplementor? {
        if (aggregation is SqlUserDefinedAggFunction) {
            val udaf: SqlUserDefinedAggFunction = aggregation as SqlUserDefinedAggFunction
            if (udaf.function !is ImplementableAggFunction) {
                throw IllegalStateException(
                    "User defined aggregation "
                            + aggregation + " must implement ImplementableAggFunction"
                )
            }
            return (udaf.function as ImplementableAggFunction)
                .getImplementor(forWindowAggregate)
        }
        if (forWindowAggregate) {
            val winAgg: Supplier<out WinAggImplementor?>? = winAggMap[aggregation]
            if (winAgg != null) {
                return winAgg.get()
            }
            // Regular aggregates can be used in window context as well
        }
        val aggSupplier: Supplier<out AggImplementor?> = aggMap[aggregation] ?: return null
        return aggSupplier.get()
    }

    operator fun get(function: SqlMatchFunction): MatchImplementor {
        val supplier: Supplier<out MatchImplementor?>? = matchMap[function]
        return if (supplier != null) {
            supplier.get()
        } else {
            throw IllegalStateException("Supplier should not be null")
        }
    }

    operator fun get(operator: SqlWindowTableFunction): TableFunctionCallImplementor {
        val supplier: Supplier<out TableFunctionCallImplementor?>? = tvfImplementorMap[operator]
        return if (supplier != null) {
            supplier.get()
        } else {
            throw IllegalStateException("Supplier should not be null")
        }
    }

    /** Strategy what an operator should return if one of its
     * arguments is null.  */
    enum class NullAs {
        /** The most common policy among the SQL built-in operators. If
         * one of the arguments is null, returns null.  */
        NULL,

        /** If one of the arguments is null, the function returns
         * false. Example: `IS NOT NULL`.  */
        FALSE,

        /** If one of the arguments is null, the function returns
         * true. Example: `IS NULL`.  */
        TRUE,

        /** It is not possible for any of the arguments to be null.  If
         * the argument type is nullable, the enclosing code will already
         * have performed a not-null check. This may allow the operator
         * implementor to generate a more efficient implementation, for
         * example, by avoiding boxing or unboxing.  */
        NOT_POSSIBLE,

        /** Return false if result is not null, true if result is null.  */
        IS_NULL,

        /** Return true if result is not null, false if result is null.  */
        IS_NOT_NULL;

        /** Adapts an expression with "normal" result to one that adheres to
         * this particular policy.  */
        fun handle(x: Expression): Expression? {
            when (Primitive.flavor(x.getType())) {
                PRIMITIVE -> return when (this) {
                    NULL, NOT_POSSIBLE, FALSE, TRUE -> x
                    IS_NULL -> FALSE_EXPR
                    IS_NOT_NULL -> TRUE_EXPR
                    else -> throw AssertionError()
                }
                BOX -> when (this) {
                    NOT_POSSIBLE -> return EnumUtils.convert(
                        x,
                        Primitive.unbox(x.getType())
                    )
                    else -> {}
                }
                else -> {}
            }
            return when (this) {
                NULL, NOT_POSSIBLE -> x
                FALSE -> Expressions.call(
                    BuiltInMethod.IS_TRUE.method,
                    x
                )
                TRUE -> Expressions.call(
                    BuiltInMethod.IS_NOT_FALSE.method,
                    x
                )
                IS_NULL -> Expressions.equal(
                    x,
                    NULL_EXPR
                )
                IS_NOT_NULL -> Expressions.notEqual(
                    x,
                    NULL_EXPR
                )
                else -> throw AssertionError()
            }
        }

        companion object {
            fun of(nullable: Boolean): NullAs {
                return if (nullable) NULL else NOT_POSSIBLE
            }
        }
    }

    /** Implementor for the `COUNT` aggregate function.  */
    internal class CountImplementor : StrictAggImplementor() {
        @Override
        override fun implementNotNullAdd(
            info: AggContext?,
            add: AggAddContext
        ) {
            add.currentBlock().add(
                Expressions.statement(
                    Expressions.postIncrementAssign(add.accumulator().get(0))
                )
            )
        }
    }

    /** Implementor for the `COUNT` windowed aggregate function.  */
    internal class CountWinImplementor : StrictWinAggImplementor() {
        var justFrameRowCount = false

        @Override
        override fun getNotNullState(info: WinAggContext): List<Type> {
            var hasNullable = false
            for (type in info.parameterRelTypes()) {
                if (type.isNullable()) {
                    hasNullable = true
                    break
                }
            }
            if (!hasNullable) {
                justFrameRowCount = true
                return Collections.emptyList()
            }
            return super.getNotNullState(info)
        }

        @Override
        override fun implementNotNullAdd(
            info: WinAggContext?,
            add: WinAggAddContext
        ) {
            if (justFrameRowCount) {
                return
            }
            add.currentBlock().add(
                Expressions.statement(
                    Expressions.postIncrementAssign(add.accumulator().get(0))
                )
            )
        }

        @Override
        protected override fun implementNotNullResult(
            info: WinAggContext?,
            result: WinAggResultContext
        ): Expression {
            return if (justFrameRowCount) {
                result.getFrameRowCount()
            } else super.implementNotNullResult(info, result)
        }
    }

    /** Implementor for the `SUM` windowed aggregate function.  */
    internal class SumImplementor : StrictAggImplementor() {
        @Override
        protected override fun implementNotNullReset(
            info: AggContext,
            reset: AggResetContext
        ) {
            val start: Expression =
                if (info.returnType() === BigDecimal::class.java) Expressions.constant(BigDecimal.ZERO) else Expressions.constant(
                    0
                )
            reset.currentBlock().add(
                Expressions.statement(
                    Expressions.assign(reset.accumulator().get(0), start)
                )
            )
        }

        @Override
        override fun implementNotNullAdd(
            info: AggContext,
            add: AggAddContext
        ) {
            val acc: Expression = add.accumulator().get(0)
            val next: Expression
            next = if (info.returnType() === BigDecimal::class.java) {
                Expressions.call(acc, "add", add.arguments().get(0))
            } else {
                Expressions.add(
                    acc,
                    EnumUtils.convert(add.arguments().get(0), acc.type)
                )
            }
            accAdvance(add, acc, next)
        }

        @Override
        override fun implementNotNullResult(
            info: AggContext?,
            result: AggResultContext
        ): Expression {
            return super.implementNotNullResult(info, result)
        }
    }

    /** Implementor for the `MIN` and `MAX` aggregate functions.  */
    internal class MinMaxImplementor : StrictAggImplementor() {
        @Override
        protected override fun implementNotNullReset(
            info: AggContext,
            reset: AggResetContext
        ) {
            val acc: Expression = reset.accumulator().get(0)
            val p: Primitive = Primitive.of(acc.getType())
            val isMin = info.aggregation().kind === SqlKind.MIN
            val inf: Object? = if (p == null) null else if (isMin) p.max else p.min
            reset.currentBlock().add(
                Expressions.statement(
                    Expressions.assign(
                        acc,
                        Expressions.constant(inf, acc.getType())
                    )
                )
            )
        }

        @Override
        override fun implementNotNullAdd(
            info: AggContext,
            add: AggAddContext
        ) {
            val acc: Expression = add.accumulator().get(0)
            val arg: Expression = add.arguments().get(0)
            val isMin = info.aggregation().kind === SqlKind.MIN
            val method: Method = (if (isMin) BuiltInMethod.LESSER else BuiltInMethod.GREATER).method
            val next: Expression = Expressions.call(
                method.getDeclaringClass(),
                method.getName(),
                acc,
                Expressions.unbox(arg)
            )
            accAdvance(add, acc, next)
        }
    }

    /** Implementor for the `SINGLE_VALUE` aggregate function.  */
    internal class SingleValueImplementor : AggImplementor {
        @Override
        fun getStateType(info: AggContext): List<Type> {
            return Arrays.asList(Boolean::class.javaPrimitiveType, info.returnType())
        }

        @Override
        fun implementReset(info: AggContext?, reset: AggResetContext) {
            val acc: List<Expression> = reset.accumulator()
            reset.currentBlock().add(
                Expressions.statement(
                    Expressions.assign(acc[0], Expressions.constant(false))
                )
            )
            reset.currentBlock().add(
                Expressions.statement(
                    Expressions.assign(
                        acc[1],
                        getDefaultValue(acc[1].getType())
                    )
                )
            )
        }

        @Override
        fun implementAdd(info: AggContext, add: AggAddContext) {
            val acc: List<Expression> = add.accumulator()
            val flag: Expression = acc[0]
            add.currentBlock().add(
                Expressions.ifThen(
                    flag,
                    Expressions.throw_(
                        Expressions.new_(
                            IllegalStateException::class.java,
                            Expressions.constant(
                                "more than one value in agg "
                                        + info.aggregation()
                            )
                        )
                    )
                )
            )
            add.currentBlock().add(
                Expressions.statement(
                    Expressions.assign(flag, Expressions.constant(true))
                )
            )
            add.currentBlock().add(
                Expressions.statement(
                    Expressions.assign(acc[1], add.arguments().get(0))
                )
            )
        }

        @Override
        fun implementResult(
            info: AggContext,
            result: AggResultContext
        ): Expression? {
            return EnumUtils.convert(
                result.accumulator().get(1),
                info.returnType()
            )
        }
    }

    /** Implementor for the `COLLECT` and `ARRAY_AGG`
     * aggregate functions.  */
    internal class CollectImplementor : StrictAggImplementor() {
        @Override
        protected override fun implementNotNullReset(
            info: AggContext?,
            reset: AggResetContext
        ) {
            // acc[0] = new ArrayList();
            reset.currentBlock().add(
                Expressions.statement(
                    Expressions.assign(
                        reset.accumulator().get(0),
                        Expressions.new_(ArrayList::class.java)
                    )
                )
            )
        }

        @Override
        override fun implementNotNullAdd(
            info: AggContext?,
            add: AggAddContext
        ) {
            // acc[0].add(arg);
            add.currentBlock().add(
                Expressions.statement(
                    Expressions.call(
                        add.accumulator().get(0),
                        BuiltInMethod.COLLECTION_ADD.method,
                        add.arguments().get(0)
                    )
                )
            )
        }
    }

    /** Implementor for the `LISTAGG` aggregate function.  */
    internal class ListaggImplementor : StrictAggImplementor() {
        @Override
        protected override fun implementNotNullReset(
            info: AggContext?,
            reset: AggResetContext
        ) {
            reset.currentBlock().add(
                Expressions.statement(
                    Expressions.assign(reset.accumulator().get(0), NULL_EXPR)
                )
            )
        }

        @Override
        override fun implementNotNullAdd(
            info: AggContext?,
            add: AggAddContext
        ) {
            val accValue: Expression = add.accumulator().get(0)
            val arg0: Expression = add.arguments().get(0)
            val arg1: Expression = if (add.arguments().size() === 2) add.arguments().get(1) else COMMA_EXPR
            val result: Expression = Expressions.condition(
                Expressions.equal(NULL_EXPR, accValue),
                arg0,
                Expressions.call(
                    BuiltInMethod.STRING_CONCAT.method, accValue,
                    Expressions.call(BuiltInMethod.STRING_CONCAT.method, arg1, arg0)
                )
            )
            add.currentBlock().add(Expressions.statement(Expressions.assign(accValue, result)))
        }
    }

    /** Implementor for the `INTERSECTION` aggregate function.  */
    internal class IntersectionImplementor : StrictAggImplementor() {
        @Override
        protected override fun implementNotNullReset(info: AggContext?, reset: AggResetContext) {
            reset.currentBlock().add(
                Expressions.statement(
                    Expressions.assign(reset.accumulator().get(0), Expressions.constant(null))
                )
            )
        }

        @Override
        override fun implementNotNullAdd(info: AggContext?, add: AggAddContext) {
            val accumulatorIsNull = BlockBuilder()
            accumulatorIsNull.add(
                Expressions.statement(
                    Expressions.assign(add.accumulator().get(0), Expressions.new_(ArrayList::class.java))
                )
            )
            accumulatorIsNull.add(
                Expressions.statement(
                    Expressions.call(
                        add.accumulator().get(0),
                        BuiltInMethod.COLLECTION_ADDALL.method, add.arguments().get(0)
                    )
                )
            )
            val accumulatorNotNull = BlockBuilder()
            accumulatorNotNull.add(
                Expressions.statement(
                    Expressions.call(
                        add.accumulator().get(0),
                        BuiltInMethod.COLLECTION_RETAIN_ALL.method,
                        add.arguments().get(0)
                    )
                )
            )
            add.currentBlock().add(
                Expressions.ifThenElse(
                    Expressions.equal(add.accumulator().get(0), Expressions.constant(null)),
                    accumulatorIsNull.toBlock(),
                    accumulatorNotNull.toBlock()
                )
            )
        }
    }

    /** Implementor for the `MODE` aggregate function.  */
    internal class ModeImplementor : StrictAggImplementor() {
        @Override
        protected override fun implementNotNullReset(
            info: AggContext?,
            reset: AggResetContext
        ) {
            // acc[0] = null;
            reset.currentBlock().add(
                Expressions.statement(
                    Expressions.assign(
                        reset.accumulator().get(0),
                        Expressions.constant(null)
                    )
                )
            )
            // acc[1] = new HashMap<>();
            reset.currentBlock().add(
                Expressions.statement(
                    Expressions.assign(
                        reset.accumulator().get(1),
                        Expressions.new_(HashMap::class.java)
                    )
                )
            )
            // acc[2] = Long.valueOf(0);
            reset.currentBlock().add(
                Expressions.statement(
                    Expressions.assign(
                        reset.accumulator().get(2),
                        Expressions.constant(0, Long::class.java)
                    )
                )
            )
        }

        @Override
        protected override fun implementNotNullAdd(
            info: AggContext?,
            add: AggAddContext
        ) {
            val currentArg: Expression = add.arguments().get(0)
            val currentResult: Expression = add.accumulator().get(0)
            val accMap: Expression = add.accumulator().get(1)
            val currentMaxNumber: Expression = add.accumulator().get(2)
            // the default number of occurrences is 0
            val getOrDefaultExpression: Expression = Expressions.call(
                accMap, BuiltInMethod.MAP_GET_OR_DEFAULT.method, currentArg,
                Expressions.constant(0, Long::class.java)
            )
            // declare and assign the occurrences number about current value
            val currentNumber: ParameterExpression = Expressions.parameter(
                Long::class.java, add.currentBlock().newName("currentNumber")
            )
            add.currentBlock().add(Expressions.declare(0, currentNumber, null))
            add.currentBlock().add(
                Expressions.statement(
                    Expressions.assign(
                        currentNumber,
                        Expressions.add(
                            Expressions.convert_(getOrDefaultExpression, Long::class.java),
                            Expressions.constant(1, Long::class.java)
                        )
                    )
                )
            )
            // update the occurrences number about current value
            val methodCallExpression2: Expression =
                Expressions.call(accMap, BuiltInMethod.MAP_PUT.method, currentArg, currentNumber)
            add.currentBlock().add(Expressions.statement(methodCallExpression2))
            // update the most frequent value
            val thenBlock = BlockBuilder(true, add.currentBlock())
            thenBlock.add(
                Expressions.statement(
                    Expressions.assign(
                        currentMaxNumber, Expressions.convert_(currentNumber, Long::class.java)
                    )
                )
            )
            thenBlock.add(
                Expressions.statement(Expressions.assign(currentResult, currentArg))
            )
            // if the maximum number of occurrences less than current value's occurrences number
            // than update
            add.currentBlock().add(
                Expressions.ifThen(
                    Expressions.lessThan(currentMaxNumber, currentNumber), thenBlock.toBlock()
                )
            )
        }

        @Override
        protected override fun implementNotNullResult(
            info: AggContext?,
            result: AggResultContext
        ): Expression {
            return result.accumulator().get(0)
        }

        @Override
        override fun getNotNullState(info: AggContext?): List<Type> {
            val types: List<Type> = ArrayList()
            // the most frequent value
            types.add(Object::class.java)
            // hashmap's key: value, hashmap's value: number of occurrences
            types.add(HashMap::class.java)
            // maximum number of occurrences about frequent value
            types.add(Long::class.java)
            return types
        }
    }

    /** Implementor for the `FUSION` and `ARRAY_CONCAT_AGG`
     * aggregate functions.  */
    internal class FusionImplementor : StrictAggImplementor() {
        @Override
        protected override fun implementNotNullReset(
            info: AggContext?,
            reset: AggResetContext
        ) {
            // acc[0] = new ArrayList();
            reset.currentBlock().add(
                Expressions.statement(
                    Expressions.assign(
                        reset.accumulator().get(0),
                        Expressions.new_(ArrayList::class.java)
                    )
                )
            )
        }

        @Override
        override fun implementNotNullAdd(
            info: AggContext?,
            add: AggAddContext
        ) {
            // acc[0].add(arg);
            add.currentBlock().add(
                Expressions.statement(
                    Expressions.call(
                        add.accumulator().get(0),
                        BuiltInMethod.COLLECTION_ADDALL.method,
                        add.arguments().get(0)
                    )
                )
            )
        }
    }

    /** Implementor for the `BIT_AND`, `BIT_OR` and `BIT_XOR` aggregate function.  */
    internal class BitOpImplementor : StrictAggImplementor() {
        @Override
        protected override fun implementNotNullReset(
            info: AggContext,
            reset: AggResetContext
        ) {
            val start: Expression
            start = if (SqlTypeUtil.isBinary(info.returnRelType())) {
                Expressions.field(null, ByteString::class.java, "EMPTY")
            } else {
                val initValue: Object = if (info.aggregation() === BIT_AND) -1L else 0
                Expressions.constant(initValue, info.returnType())
            }
            reset.currentBlock().add(
                Expressions.statement(
                    Expressions.assign(reset.accumulator().get(0), start)
                )
            )
        }

        @Override
        override fun implementNotNullAdd(
            info: AggContext,
            add: AggAddContext
        ) {
            val acc: Expression = add.accumulator().get(0)
            val arg: Expression = add.arguments().get(0)
            val aggregation: SqlAggFunction = info.aggregation()
            val builtInMethod: BuiltInMethod
            builtInMethod = when (aggregation.kind) {
                BIT_AND -> BuiltInMethod.BIT_AND
                BIT_OR -> BuiltInMethod.BIT_OR
                BIT_XOR -> BuiltInMethod.BIT_XOR
                else -> throw IllegalArgumentException(
                    "Unknown " + aggregation.getName()
                        .toString() + ". Only support bit_and, bit_or and bit_xor for bit aggregation function"
                )
            }
            val method: Method = builtInMethod.method
            val next: Expression = Expressions.call(
                method.getDeclaringClass(),
                method.getName(),
                acc,
                Expressions.unbox(arg)
            )
            accAdvance(add, acc, next)
        }
    }

    /** Implementor for the `GROUPING` aggregate function.  */
    internal class GroupingImplementor : AggImplementor {
        @Override
        fun getStateType(info: AggContext?): List<Type> {
            return ImmutableList.of()
        }

        @Override
        fun implementReset(info: AggContext?, reset: AggResetContext?) {
        }

        @Override
        fun implementAdd(info: AggContext?, add: AggAddContext?) {
        }

        @Override
        fun implementResult(
            info: AggContext,
            result: AggResultContext
        ): Expression? {
            val keys: List<Integer>
            keys = when (info.aggregation().kind) {
                GROUPING -> result.call().getArgList()
                else -> throw AssertionError()
            }
            var e: Expression? = null
            if (info.groupSets().size() > 1) {
                val keyOrdinals: List<Integer> = info.keyOrdinals()
                var x = 1L shl keys.size() - 1
                for (k in keys) {
                    val i = keyOrdinals.indexOf(k)
                    assert(i >= 0)
                    val e2: Expression = Expressions.condition(
                        result.keyField(keyOrdinals.size() + i),
                        Expressions.constant(x),
                        Expressions.constant(0L)
                    )
                    e = if (e == null) {
                        e2
                    } else {
                        Expressions.add(e, e2)
                    }
                    x = x shr 1
                }
            }
            return if (e != null) e else Expressions.constant(0, info.returnType())
        }
    }

    /** Implementor for user-defined aggregate functions.  */
    class UserDefinedAggReflectiveImplementor(afi: AggregateFunctionImpl) : StrictAggImplementor() {
        private val afi: AggregateFunctionImpl

        init {
            this.afi = afi
        }

        @Override
        override fun getNotNullState(info: AggContext?): List<Type> {
            return if (afi.isStatic) {
                Collections.singletonList(afi.accumulatorType)
            } else Arrays.asList(afi.accumulatorType, afi.declaringClass)
        }

        @Override
        protected override fun implementNotNullReset(
            info: AggContext?,
            reset: AggResetContext
        ) {
            val acc: List<Expression> = reset.accumulator()
            if (!afi.isStatic) {
                reset.currentBlock().add(
                    Expressions.statement(
                        Expressions.assign(acc[1], makeNew(afi))
                    )
                )
            }
            reset.currentBlock().add(
                Expressions.statement(
                    Expressions.assign(
                        acc[0],
                        Expressions.call(if (afi.isStatic) null else acc[1], afi.initMethod)
                    )
                )
            )
        }

        @Override
        protected override fun implementNotNullAdd(
            info: AggContext?,
            add: AggAddContext
        ) {
            val acc: List<Expression> = add.accumulator()
            val aggArgs: List<Expression> = add.arguments()
            val args: List<Expression> = ArrayList(aggArgs.size() + 1)
            args.add(acc[0])
            args.addAll(aggArgs)
            add.currentBlock().add(
                Expressions.statement(
                    Expressions.assign(
                        acc[0],
                        Expressions.call(
                            if (afi.isStatic) null else acc[1], afi.addMethod,
                            args
                        )
                    )
                )
            )
        }

        @Override
        protected override fun implementNotNullResult(
            info: AggContext?,
            result: AggResultContext
        ): Expression {
            val acc: List<Expression> = result.accumulator()
            return Expressions.call(
                if (afi.isStatic) null else acc[1],
                requireNonNull(
                    afi.resultMethod
                ) { "resultMethod is null. Does " + afi.declaringClass.toString() + " declare result method?" },
                acc[0]
            )
        }

        companion object {
            private fun makeNew(afi: AggregateFunctionImpl): NewExpression {
                try {
                    val constructor: Constructor<*> = afi.declaringClass.getConstructor()
                    Objects.requireNonNull(constructor, "constructor")
                    return Expressions.new_(afi.declaringClass)
                } catch (e: NoSuchMethodException) {
                    // ignore, and try next constructor
                }
                return try {
                    val constructor: Constructor<*> = afi.declaringClass.getConstructor(FunctionContext::class.java)
                    Objects.requireNonNull(constructor, "constructor")
                    Expressions.new_(
                        afi.declaringClass,
                        Expressions.call(
                            BuiltInMethod.FUNCTION_CONTEXTS_OF.method,
                            DataContext.ROOT,  // TODO: pass in the values of arguments that are literals
                            Expressions.newArrayBounds(
                                Object::class.java, 1,
                                Expressions.constant(afi.getParameters().size())
                            )
                        )
                    )
                } catch (e: NoSuchMethodException) {
                    // This should never happen: validator should have made sure that the
                    // class had an appropriate constructor.
                    throw AssertionError("no valid constructor for $afi")
                }
            }
        }
    }

    /** Implementor for the `RANK` windowed aggregate function.  */
    internal class RankImplementor : StrictWinAggImplementor() {
        @Override
        protected override fun implementNotNullAdd(
            info: WinAggContext?,
            add: WinAggAddContext
        ) {
            val acc: Expression = add.accumulator().get(0)
            // This is an example of the generated code
            if (false) {
                object : Object() {
                    var curentPosition // position in for-win-agg-loop
                            = 0
                    var startIndex // index of start of window
                            = 0
                    var rows // accessed via WinAggAddContext.compareRows
                            : @Nullable Array<Comparable>?

                    @SuppressWarnings("nullness")
                    fun sample() {
                        if (curentPosition > startIndex) {
                            if (rows!![curentPosition - 1].compareTo(rows!![curentPosition])
                                > 0
                            ) {
                                // update rank
                            }
                        }
                    }
                }
            }
            val builder: BlockBuilder = add.nestBlock()
            add.currentBlock().add(
                Expressions.ifThen(
                    Expressions.lessThan(
                        add.compareRows(
                            Expressions.subtract(
                                add.currentPosition(),
                                Expressions.constant(1)
                            ),
                            add.currentPosition()
                        ),
                        Expressions.constant(0)
                    ),
                    Expressions.statement(
                        Expressions.assign(acc, computeNewRank(acc, add))
                    )
                )
            )
            add.exitBlock()
            add.currentBlock().add(
                Expressions.ifThen(
                    Expressions.greaterThan(
                        add.currentPosition(),
                        add.startIndex()
                    ),
                    builder.toBlock()
                )
            )
        }

        protected fun computeNewRank(acc: Expression?, add: WinAggAddContext): Expression? {
            var pos: Expression = add.currentPosition()
            if (!add.startIndex().equals(Expressions.constant(0))) {
                // In general, currentPosition-startIndex should be used
                // However, rank/dense_rank does not allow preceding/following clause
                // so we always result in startIndex==0.
                pos = Expressions.subtract(pos, add.startIndex())
            }
            return pos
        }

        @Override
        protected override fun implementNotNullResult(
            info: WinAggContext?, result: WinAggResultContext?
        ): Expression {
            // Rank is 1-based
            return Expressions.add(
                super.implementNotNullResult(info, result),
                Expressions.constant(1)
            )
        }
    }

    /** Implementor for the `DENSE_RANK` windowed aggregate function.  */
    internal class DenseRankImplementor : RankImplementor() {
        @Override
        override fun computeNewRank(
            acc: Expression?,
            add: WinAggAddContext?
        ): Expression {
            return Expressions.add(acc, Expressions.constant(1))
        }
    }

    /** Implementor for the `FIRST_VALUE` and `LAST_VALUE`
     * windowed aggregate functions.  */
    internal class FirstLastValueImplementor protected constructor(seekType: SeekType) : WinAggImplementor {
        private val seekType: SeekType

        init {
            this.seekType = seekType
        }

        @Override
        fun getStateType(info: AggContext?): List<Type> {
            return Collections.emptyList()
        }

        @Override
        fun implementReset(info: AggContext?, reset: AggResetContext?) {
            // no op
        }

        @Override
        fun implementAdd(info: AggContext?, add: AggAddContext?) {
            // no op
        }

        @Override
        override fun needCacheWhenFrameIntact(): Boolean {
            return true
        }

        @Override
        fun implementResult(
            info: AggContext,
            result: AggResultContext
        ): Expression {
            val winResult: WinAggResultContext = result
            return Expressions.condition(
                winResult.hasRows(),
                winResult.rowTranslator(
                    winResult.computeIndex(Expressions.constant(0), seekType)
                )
                    .translate(winResult.rexArguments().get(0), info.returnType()),
                getDefaultValue(info.returnType())
            )
        }
    }

    /** Implementor for the `FIRST_VALUE` windowed aggregate function.  */
    internal class FirstValueImplementor protected constructor() : FirstLastValueImplementor(SeekType.START)

    /** Implementor for the `LAST_VALUE` windowed aggregate function.  */
    internal class LastValueImplementor protected constructor() : FirstLastValueImplementor(SeekType.END)

    /** Implementor for the `NTH_VALUE`
     * windowed aggregate function.  */
    internal class NthValueImplementor : WinAggImplementor {
        @Override
        fun getStateType(info: AggContext?): List<Type> {
            return Collections.emptyList()
        }

        @Override
        fun implementReset(info: AggContext?, reset: AggResetContext?) {
            // no op
        }

        @Override
        fun implementAdd(info: AggContext?, add: AggAddContext?) {
            // no op
        }

        @Override
        override fun needCacheWhenFrameIntact(): Boolean {
            return true
        }

        @Override
        fun implementResult(
            info: AggContext,
            result: AggResultContext
        ): Expression {
            val winResult: WinAggResultContext = result
            val rexArgs: List<RexNode?> = winResult.rexArguments()
            val res: ParameterExpression = Expressions.parameter(
                0, info.returnType(),
                result.currentBlock().newName("nth")
            )
            val currentRowTranslator: RexToLixTranslator = winResult.rowTranslator(
                winResult.computeIndex(Expressions.constant(0), SeekType.START)
            )!!
            val dstIndex: Expression = winResult.computeIndex(
                Expressions.subtract(
                    currentRowTranslator.translate(rexArgs[1], Int::class.javaPrimitiveType),
                    Expressions.constant(1)
                ), SeekType.START
            )
            val rowInRange: Expression = winResult.rowInPartition(dstIndex)
            val thenBlock: BlockBuilder = result.nestBlock()
            val nthValue: Expression = winResult.rowTranslator(dstIndex)
                .translate(rexArgs[0], res.type)
            thenBlock.add(Expressions.statement(Expressions.assign(res, nthValue)))
            result.exitBlock()
            val thenBranch: BlockStatement = thenBlock.toBlock()
            val defaultValue: Expression = getDefaultValue(res.type)
            result.currentBlock().add(Expressions.declare(0, res, null))
            result.currentBlock().add(
                Expressions.ifThenElse(
                    rowInRange, thenBranch,
                    Expressions.statement(Expressions.assign(res, defaultValue))
                )
            )
            return res
        }
    }

    /** Implementor for the `LEAD` and `LAG` windowed
     * aggregate functions.  */
    class LeadLagImplementor protected constructor(private val isLead: Boolean) : WinAggImplementor {
        @Override
        fun getStateType(info: AggContext?): List<Type> {
            return Collections.emptyList()
        }

        @Override
        fun implementReset(info: AggContext?, reset: AggResetContext?) {
            // no op
        }

        @Override
        fun implementAdd(info: AggContext?, add: AggAddContext?) {
            // no op
        }

        @Override
        override fun needCacheWhenFrameIntact(): Boolean {
            return false
        }

        @Override
        fun implementResult(
            info: AggContext,
            result: AggResultContext
        ): Expression {
            val winResult: WinAggResultContext = result
            val rexArgs: List<RexNode?> = winResult.rexArguments()
            val res: ParameterExpression = Expressions.parameter(
                0, info.returnType(),
                result.currentBlock().newName(if (isLead) "lead" else "lag")
            )
            var offset: Expression
            val currentRowTranslator: RexToLixTranslator = winResult.rowTranslator(
                winResult.computeIndex(Expressions.constant(0), SeekType.SET)
            )!!
            offset = if (rexArgs.size() >= 2) {
                // lead(x, offset) or lead(x, offset, default)
                currentRowTranslator.translate(
                    rexArgs[1], Int::class.javaPrimitiveType
                )
            } else {
                Expressions.constant(1)
            }
            if (!isLead) {
                offset = Expressions.negate(offset)
            }
            val dstIndex: Expression = winResult.computeIndex(offset, SeekType.SET)
            val rowInRange: Expression = winResult.rowInPartition(dstIndex)
            val thenBlock: BlockBuilder = result.nestBlock()
            val lagResult: Expression = winResult.rowTranslator(dstIndex).translate(
                rexArgs[0], res.type
            )
            thenBlock.add(Expressions.statement(Expressions.assign(res, lagResult)))
            result.exitBlock()
            val thenBranch: BlockStatement = thenBlock.toBlock()
            val defaultValue: Expression = if (rexArgs.size() === 3) currentRowTranslator.translate(
                rexArgs[2],
                res.type
            ) else getDefaultValue(res.type)
            result.currentBlock().add(Expressions.declare(0, res, null))
            result.currentBlock().add(
                Expressions.ifThenElse(
                    rowInRange, thenBranch,
                    Expressions.statement(Expressions.assign(res, defaultValue))
                )
            )
            return res
        }
    }

    /** Implementor for the `LEAD` windowed aggregate function.  */
    class LeadImplementor protected constructor() : LeadLagImplementor(true)

    /** Implementor for the `LAG` windowed aggregate function.  */
    class LagImplementor protected constructor() : LeadLagImplementor(false)

    /** Implementor for the `NTILE` windowed aggregate function.  */
    internal class NtileImplementor : WinAggImplementor {
        @Override
        fun getStateType(info: AggContext?): List<Type> {
            return Collections.emptyList()
        }

        @Override
        fun implementReset(info: AggContext?, reset: AggResetContext?) {
            // no op
        }

        @Override
        fun implementAdd(info: AggContext?, add: AggAddContext?) {
            // no op
        }

        @Override
        override fun needCacheWhenFrameIntact(): Boolean {
            return false
        }

        @Override
        fun implementResult(
            info: AggContext?,
            result: AggResultContext
        ): Expression {
            val winResult: WinAggResultContext = result
            val rexArgs: List<RexNode?> = winResult.rexArguments()
            val tiles: Expression = winResult.rowTranslator(winResult.index()).translate(
                rexArgs[0], Int::class.javaPrimitiveType
            )
            return Expressions.add(
                Expressions.constant(1),
                Expressions.divide(
                    Expressions.multiply(
                        tiles,
                        Expressions.subtract(
                            winResult.index(), winResult.startIndex()
                        )
                    ),
                    winResult.getPartitionRowCount()
                )
            )
        }
    }

    /** Implementor for the `ROW_NUMBER` windowed aggregate function.  */
    internal class RowNumberImplementor : StrictWinAggImplementor() {
        @Override
        override fun getNotNullState(info: WinAggContext?): List<Type> {
            return Collections.emptyList()
        }

        @Override
        protected override fun implementNotNullAdd(
            info: WinAggContext?,
            add: WinAggAddContext?
        ) {
            // no op
        }

        @Override
        protected override fun implementNotNullResult(
            info: WinAggContext?, result: WinAggResultContext
        ): Expression {
            // Window cannot be empty since ROWS/RANGE is not possible for ROW_NUMBER
            return Expressions.add(
                Expressions.subtract(result.index(), result.startIndex()),
                Expressions.constant(1)
            )
        }
    }

    /** Implementor for the `JSON_OBJECTAGG` aggregate function.  */
    internal class JsonObjectAggImplementor(m: Method) : AggImplementor {
        private val m: Method

        init {
            this.m = m
        }

        @Override
        fun getStateType(info: AggContext?): List<Type> {
            return Collections.singletonList(Map::class.java)
        }

        @Override
        fun implementReset(
            info: AggContext?,
            reset: AggResetContext
        ) {
            reset.currentBlock().add(
                Expressions.statement(
                    Expressions.assign(
                        reset.accumulator().get(0),
                        Expressions.new_(HashMap::class.java)
                    )
                )
            )
        }

        @Override
        fun implementAdd(info: AggContext, add: AggAddContext) {
            val function: SqlJsonObjectAggAggFunction = info.aggregation() as SqlJsonObjectAggAggFunction
            add.currentBlock().add(
                Expressions.statement(
                    Expressions.call(
                        m,
                        Iterables.concat(
                            Collections.singletonList(add.accumulator().get(0)),
                            add.arguments(),
                            Collections.singletonList(
                                Expressions.constant(function.getNullClause())
                            )
                        )
                    )
                )
            )
        }

        @Override
        fun implementResult(
            info: AggContext?,
            result: AggResultContext
        ): Expression {
            return Expressions.call(
                BuiltInMethod.JSONIZE.method,
                result.accumulator().get(0)
            )
        }

        companion object {
            fun supplierFor(m: Method): Supplier<JsonObjectAggImplementor> {
                return Supplier<JsonObjectAggImplementor> { JsonObjectAggImplementor(m) }
            }
        }
    }

    /** Implementor for the `JSON_ARRAYAGG` aggregate function.  */
    internal class JsonArrayAggImplementor(m: Method) : AggImplementor {
        private val m: Method

        init {
            this.m = m
        }

        @Override
        fun getStateType(info: AggContext?): List<Type> {
            return Collections.singletonList(List::class.java)
        }

        @Override
        fun implementReset(
            info: AggContext?,
            reset: AggResetContext
        ) {
            reset.currentBlock().add(
                Expressions.statement(
                    Expressions.assign(
                        reset.accumulator().get(0),
                        Expressions.new_(ArrayList::class.java)
                    )
                )
            )
        }

        @Override
        fun implementAdd(
            info: AggContext,
            add: AggAddContext
        ) {
            val function: SqlJsonArrayAggAggFunction = info.aggregation() as SqlJsonArrayAggAggFunction
            add.currentBlock().add(
                Expressions.statement(
                    Expressions.call(
                        m,
                        Iterables.concat(
                            Collections.singletonList(add.accumulator().get(0)),
                            add.arguments(),
                            Collections.singletonList(
                                Expressions.constant(function.getNullClause())
                            )
                        )
                    )
                )
            )
        }

        @Override
        fun implementResult(
            info: AggContext?,
            result: AggResultContext
        ): Expression {
            return Expressions.call(
                BuiltInMethod.JSONIZE.method,
                result.accumulator().get(0)
            )
        }

        companion object {
            fun supplierFor(m: Method): Supplier<JsonArrayAggImplementor> {
                return Supplier<JsonArrayAggImplementor> { JsonArrayAggImplementor(m) }
            }
        }
    }

    /** Implementor for the `TRIM` function.  */
    private class TrimImplementor internal constructor() : AbstractRexCallImplementor(NullPolicy.STRICT, false) {
        @get:Override
        override val variableName: String
            get() = "trim"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator,
            call: RexCall?, argValueList: List<Expression?>
        ): Expression {
            val strict: Boolean = !translator.conformance.allowExtendedTrim()
            val value: Object = translator.getLiteralValue(argValueList[0])
            val flag: SqlTrimFunction.Flag = value as SqlTrimFunction.Flag
            return Expressions.call(
                BuiltInMethod.TRIM.method,
                Expressions.constant(
                    flag === SqlTrimFunction.Flag.BOTH
                            || flag === SqlTrimFunction.Flag.LEADING
                ),
                Expressions.constant(
                    flag === SqlTrimFunction.Flag.BOTH
                            || flag === SqlTrimFunction.Flag.TRAILING
                ),
                argValueList[1],
                argValueList[2],
                Expressions.constant(strict)
            )
        }
    }

    /** Implementor for the `MONTHNAME` and `DAYNAME` functions.
     * Each takes a [java.util.Locale] argument.  */
    private class PeriodNameImplementor internal constructor(
        methodName: String, timestampMethod: BuiltInMethod,
        dateMethod: BuiltInMethod
    ) : MethodNameImplementor(methodName, NullPolicy.STRICT, false) {
        private val timestampMethod: BuiltInMethod
        private val dateMethod: BuiltInMethod

        init {
            this.timestampMethod = timestampMethod
            this.dateMethod = dateMethod
        }

        @get:Override
        override val variableName: String
            get() = "periodName"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator,
            call: RexCall, argValueList: List<Expression?>
        ): Expression {
            val operand: Expression? = argValueList[0]
            val type: RelDataType = call.operands.get(0).getType()
            return when (type.getSqlTypeName()) {
                TIMESTAMP -> getExpression(translator, operand, timestampMethod)
                DATE -> getExpression(translator, operand, dateMethod)
                else -> throw AssertionError("unknown type $type")
            }
        }

        protected fun getExpression(
            translator: RexToLixTranslator,
            operand: Expression?, builtInMethod: BuiltInMethod
        ): Expression {
            val locale: MethodCallExpression = Expressions.call(BuiltInMethod.LOCALE.method, translator.getRoot())
            return Expressions.call(
                builtInMethod.method.getDeclaringClass(),
                builtInMethod.method.getName(), operand, locale
            )
        }
    }

    /** Implementor for the `FLOOR` and `CEIL` functions.  */
    private class FloorImplementor internal constructor(
        methodName: String, timestampMethod: Method,
        dateMethod: Method
    ) : MethodNameImplementor(methodName, NullPolicy.STRICT, false) {
        val timestampMethod: Method
        val dateMethod: Method

        init {
            this.timestampMethod = timestampMethod
            this.dateMethod = dateMethod
        }

        @get:Override
        override val variableName: String
            get() = "floor"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator,
            call: RexCall, argValueList: List<Expression>
        ): Expression {
            return when (call.getOperands().size()) {
                1 -> when (call.getType().getSqlTypeName()) {
                    BIGINT, INTEGER, SMALLINT, TINYINT -> argValueList[0]
                    else -> super.implementSafe(translator, call, argValueList)
                }
                2 -> {
                    val type: Type
                    val floorMethod: Method
                    val preFloor: Boolean
                    var operand: Expression = argValueList[0]
                    when (call.getType().getSqlTypeName()) {
                        TIMESTAMP_WITH_LOCAL_TIME_ZONE -> {
                            operand = Expressions.call(
                                BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
                                operand,
                                Expressions.call(BuiltInMethod.TIME_ZONE.method, translator.getRoot())
                            )
                            type = Long::class.javaPrimitiveType
                            floorMethod = timestampMethod
                            preFloor = true
                        }
                        TIMESTAMP -> {
                            type = Long::class.javaPrimitiveType
                            floorMethod = timestampMethod
                            preFloor = true
                        }
                        else -> {
                            type = Int::class.javaPrimitiveType
                            floorMethod = dateMethod
                            preFloor = false
                        }
                    }
                    val timeUnitRange: TimeUnitRange = requireNonNull(
                        translator.getLiteralValue(argValueList[1]),
                        "timeUnitRange"
                    ) as TimeUnitRange
                    when (timeUnitRange) {
                        YEAR, QUARTER, MONTH, WEEK, DAY -> {
                            val operand1: Expression = if (preFloor) call(operand, type, TimeUnit.DAY) else operand
                            Expressions.call(
                                floorMethod,
                                translator.getLiteral(argValueList[1]), operand1
                            )
                        }
                        NANOSECOND -> call(operand, type, timeUnitRange.startUnit)
                        else -> call(operand, type, timeUnitRange.startUnit)
                    }
                }
                else -> throw AssertionError()
            }
        }

        private fun call(
            operand: Expression, type: Type,
            timeUnit: TimeUnit
        ): Expression {
            return Expressions.call(
                SqlFunctions::class.java, methodName,
                EnumUtils.convert(operand, type),
                EnumUtils.convert(
                    Expressions.constant(timeUnit.multiplier), type
                )
            )
        }
    }

    /** Implementor for a function that generates calls to a given method.  */
    private class MethodImplementor internal constructor(
        method: Method,
        @Nullable nullPolicy: NullPolicy?,
        harmonize: Boolean
    ) : AbstractRexCallImplementor(nullPolicy, harmonize) {
        protected val method: Method

        init {
            this.method = method
        }

        @get:Override
        override val variableName: String
            get() = "method_call"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall?, argValueList: List<Expression?>
        ): Expression {
            return if (Modifier.isStatic(method.getModifiers())) {
                call(method, null, argValueList)
            } else {
                call(
                    method,
                    argValueList[0],
                    Util.skip(argValueList, 1)
                )
            }
        }

        companion object {
            fun call(
                method: Method, @Nullable target: Expression?,
                args: List<Expression?>
            ): Expression {
                return if (method.isVarArgs()) {
                    val last: Int = method.getParameterCount() - 1
                    val vargs: ImmutableList<Expression> = ImmutableList.< Expression > builder < Expression ? > ()
                        .addAll(args.subList(0, last))
                        .add(
                            Expressions.newArrayInit(
                                method.getParameterTypes().get(last),
                                args.subList(last, args.size()).toArray(arrayOfNulls<Expression>(0))
                            )
                        )
                        .build()
                    Expressions.call(target, method, vargs)
                } else {
                    val clazz: Class<*> = method.getDeclaringClass()
                    EnumUtils.call(target, clazz, method.getName(), args)
                }
            }
        }
    }

    /** Implementor for [org.apache.calcite.sql.fun.SqlPosixRegexOperator]s.  */
    private class PosixRegexMethodImplementor internal constructor(protected val caseSensitive: Boolean) :
        MethodImplementor(BuiltInMethod.POSIX_REGEX.method, NullPolicy.STRICT, false) {
        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall?, argValueList: List<Expression?>
        ): Expression {
            assert(argValueList.size() === 2)
            // Add extra parameter (caseSensitive boolean flag), required by SqlFunctions#posixRegex.
            val newOperands: List<Expression> = ArrayList(argValueList)
            newOperands.add(Expressions.constant(caseSensitive))
            return super.implementSafe(translator, call, newOperands)
        }
    }

    /**
     * Implementor for JSON_VALUE function, convert to solid format
     * "JSON_VALUE(json_doc, path, empty_behavior, empty_default, error_behavior, error default)"
     * in order to simplify the runtime implementation.
     *
     *
     * We should avoid this when we support
     * variable arguments function.
     */
    private class JsonValueImplementor internal constructor(method: Method) :
        MethodImplementor(method, NullPolicy.ARG0, false) {
        @Override
        override fun implementSafe(
            translator: RexToLixTranslator,
            call: RexCall, argValueList: List<Expression?>
        ): Expression? {
            val expression: Expression
            val newOperands: List<Expression> = ArrayList()
            newOperands.add(argValueList[0])
            newOperands.add(argValueList[1])
            val leftExprs: List<Expression> = Util.skip(argValueList, 2)
            // Default value for JSON_VALUE behaviors.
            var emptyBehavior: Expression = Expressions.constant(SqlJsonValueEmptyOrErrorBehavior.NULL)
            var defaultValueOnEmpty: Expression = Expressions.constant(null)
            var errorBehavior: Expression = Expressions.constant(SqlJsonValueEmptyOrErrorBehavior.NULL)
            var defaultValueOnError: Expression = Expressions.constant(null)
            // Patched up with user defines.
            if (leftExprs.size() > 0) {
                for (i in 0 until leftExprs.size()) {
                    val expr: Expression = leftExprs[i]
                    val exprVal: Object = translator.getLiteralValue(expr)
                    if (exprVal != null) {
                        val defaultSymbolIdx: Int = i - 2
                        if (exprVal === SqlJsonEmptyOrError.EMPTY) {
                            if (defaultSymbolIdx >= 0
                                && translator.getLiteralValue(leftExprs[defaultSymbolIdx])
                                === SqlJsonValueEmptyOrErrorBehavior.DEFAULT
                            ) {
                                defaultValueOnEmpty = leftExprs[i - 1]
                                emptyBehavior = leftExprs[defaultSymbolIdx]
                            } else {
                                emptyBehavior = leftExprs[i - 1]
                            }
                        } else if (exprVal === SqlJsonEmptyOrError.ERROR) {
                            if (defaultSymbolIdx >= 0
                                && translator.getLiteralValue(leftExprs[defaultSymbolIdx])
                                === SqlJsonValueEmptyOrErrorBehavior.DEFAULT
                            ) {
                                defaultValueOnError = leftExprs[i - 1]
                                errorBehavior = leftExprs[defaultSymbolIdx]
                            } else {
                                errorBehavior = leftExprs[i - 1]
                            }
                        }
                    }
                }
            }
            newOperands.add(emptyBehavior)
            newOperands.add(defaultValueOnEmpty)
            newOperands.add(errorBehavior)
            newOperands.add(defaultValueOnError)
            val clazz: Class = method.getDeclaringClass()
            expression = EnumUtils.call(null, clazz, method.getName(), newOperands)
            val returnType: Type = translator.typeFactory.getJavaClass(call.getType())
            return EnumUtils.convert(expression, returnType)
        }
    }

    /** Implementor for SQL functions that generates calls to a given method name.
     *
     *
     * Use this, as opposed to [MethodImplementor], if the SQL function
     * is overloaded; then you can use one implementor for several overloads.  */
    private class MethodNameImplementor internal constructor(
        protected val methodName: String,
        nullPolicy: NullPolicy?, harmonize: Boolean
    ) : AbstractRexCallImplementor(nullPolicy, harmonize) {
        @get:Override
        override val variableName: String
            get() = "method_name_call"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall?, argValueList: List<Expression?>
        ): Expression {
            return EnumUtils.call(null, SqlFunctions::class.java, methodName, argValueList)
        }
    }

    /** Implementor for binary operators.  */
    private class BinaryImplementor internal constructor(
        nullPolicy: NullPolicy?, harmonize: Boolean,
        expressionType: ExpressionType, backupMethodName: String?
    ) : AbstractRexCallImplementor(nullPolicy, harmonize) {
        private val expressionType: ExpressionType
        private val backupMethodName: String?

        init {
            this.expressionType = expressionType
            this.backupMethodName = backupMethodName
        }

        @get:Override
        override val variableName: String
            get() = "binary_call"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall,
            argValueList: List<Expression>
        ): Expression {
            // neither nullable:
            //   return x OP y
            // x nullable
            //   null_returns_null
            //     return x == null ? null : x OP y
            //   ignore_null
            //     return x == null ? null : y
            // x, y both nullable
            //   null_returns_null
            //     return x == null || y == null ? null : x OP y
            //   ignore_null
            //     return x == null ? y : y == null ? x : x OP y
            if (backupMethodName != null) {
                // If one or both operands have ANY type, use the late-binding backup
                // method.
                if (anyAnyOperands(call)) {
                    return callBackupMethodAnyType(argValueList)
                }
                val type0: Type = argValueList[0].getType()
                val type1: Type = argValueList[1].getType()
                val op: SqlBinaryOperator = call.getOperator() as SqlBinaryOperator
                val relDataType0: RelDataType = call.getOperands().get(0).getType()
                val fieldComparator: Expression = generateCollatorExpression(relDataType0.getCollation())
                if (fieldComparator != null) {
                    argValueList.add(fieldComparator)
                }
                val primitive: Primitive = Primitive.ofBoxOr(type0)
                if (primitive == null || type1 === BigDecimal::class.java || (COMPARISON_OPERATORS.contains(op)
                            && !COMP_OP_TYPES.contains(primitive))
                ) {
                    return Expressions.call(
                        SqlFunctions::class.java, backupMethodName,
                        argValueList
                    )
                }
                // When checking equals or not equals on two primitive boxing classes
                // (i.e. Long x, Long y), we should fall back to call `SqlFunctions.eq(x, y)`
                // or `SqlFunctions.ne(x, y)`, rather than `x == y`
                val boxPrimitive0: Primitive = Primitive.ofBox(type0)
                val boxPrimitive1: Primitive = Primitive.ofBox(type1)
                if (EQUALS_OPERATORS.contains(op)
                    && boxPrimitive0 != null && boxPrimitive1 != null
                ) {
                    return Expressions.call(
                        SqlFunctions::class.java, backupMethodName,
                        argValueList
                    )
                }
            }
            return Expressions.makeBinary(
                expressionType,
                argValueList[0], argValueList[1]
            )
        }

        private fun callBackupMethodAnyType(expressions: List<Expression>): Expression {
            val backupMethodNameForAnyType = backupMethodName + METHOD_POSTFIX_FOR_ANY_TYPE

            // one or both of parameter(s) is(are) ANY type
            val expression0: Expression = maybeBox(
                expressions[0]
            )
            val expression1: Expression = maybeBox(
                expressions[1]
            )
            return Expressions.call(
                SqlFunctions::class.java, backupMethodNameForAnyType,
                expression0, expression1
            )
        }

        companion object {
            /** Types that can be arguments to comparison operators such as
             * `<`.  */
            private val COMP_OP_TYPES: List<Primitive?> = ImmutableList.of(
                Primitive.BYTE,
                Primitive.CHAR,
                Primitive.SHORT,
                Primitive.INT,
                Primitive.LONG,
                Primitive.FLOAT,
                Primitive.DOUBLE
            )
            private val COMPARISON_OPERATORS: List<SqlBinaryOperator> = ImmutableList.of(
                SqlStdOperatorTable.LESS_THAN,
                SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                SqlStdOperatorTable.GREATER_THAN,
                SqlStdOperatorTable.GREATER_THAN_OR_EQUAL
            )
            private val EQUALS_OPERATORS: List<SqlBinaryOperator> = ImmutableList.of(
                SqlStdOperatorTable.EQUALS,
                SqlStdOperatorTable.NOT_EQUALS
            )
            const val METHOD_POSTFIX_FOR_ANY_TYPE = "Any"

            /** Returns whether any of a call's operands have ANY type.  */
            private fun anyAnyOperands(call: RexCall): Boolean {
                for (operand in call.operands) {
                    if (operand.getType().getSqlTypeName() === SqlTypeName.ANY) {
                        return true
                    }
                }
                return false
            }

            private fun maybeBox(expression: Expression): Expression {
                var expression: Expression = expression
                val primitive: Primitive = Primitive.of(expression.getType())
                if (primitive != null) {
                    expression = Expressions.box(expression, primitive)
                }
                return expression
            }
        }
    }

    /** Implementor for unary operators.  */
    private class UnaryImplementor internal constructor(
        expressionType: ExpressionType, nullPolicy: NullPolicy?,
        @Nullable backupMethodName: String?
    ) : AbstractRexCallImplementor(nullPolicy, false) {
        private val expressionType: ExpressionType

        @Nullable
        private val backupMethodName: String?

        init {
            this.expressionType = expressionType
            this.backupMethodName = backupMethodName
        }

        @get:Override
        override val variableName: String
            get() = "unary_call"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall?, argValueList: List<Expression>
        ): Expression {
            val argValue: Expression = argValueList[0]
            val e: Expression
            //Special case for implementing unary minus with BigDecimal type
            //for other data type(except BigDecimal) '-' operator is OK, but for
            //BigDecimal, we should call negate method of BigDecimal
            e =
                if (expressionType === ExpressionType.Negate && argValue.type === BigDecimal::class.java && null != backupMethodName) {
                    Expressions.call(argValue, backupMethodName)
                } else {
                    Expressions.makeUnary(expressionType, argValue)
                }
            return if (e.type.equals(argValue.type)) {
                e
            } else Expressions.convert_(e, argValue.type)
            // Certain unary operators do not preserve type. For example, the "-"
            // operator applied to a "byte" expression returns an "int".
        }
    }

    /** Implementor for the `EXTRACT(unit FROM datetime)` function.  */
    private class ExtractImplementor internal constructor() : AbstractRexCallImplementor(NullPolicy.STRICT, false) {
        @get:Override
        override val variableName: String
            get() = "extract"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator,
            call: RexCall, argValueList: List<Expression?>
        ): Expression? {
            val timeUnitRange: TimeUnitRange = translator.getLiteralValue(argValueList[0]) as TimeUnitRange
            val unit: TimeUnit = requireNonNull(timeUnitRange, "timeUnitRange").startUnit
            var operand: Expression? = argValueList[1]
            val sqlTypeName: SqlTypeName = call.operands.get(1).getType().getSqlTypeName()
            when (unit) {
                MILLENNIUM, CENTURY, YEAR, QUARTER, MONTH, DAY, DOW, DECADE, DOY, ISODOW, ISOYEAR, WEEK -> when (sqlTypeName) {
                    INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> {}
                    TIMESTAMP_WITH_LOCAL_TIME_ZONE -> {
                        operand = Expressions.call(
                            BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
                            operand,
                            Expressions.call(BuiltInMethod.TIME_ZONE.method, translator.getRoot())
                        )
                        operand = Expressions.call(
                            BuiltInMethod.FLOOR_DIV.method,
                            operand, Expressions.constant(TimeUnit.DAY.multiplier.longValue())
                        )
                        return Expressions.call(
                            BuiltInMethod.UNIX_DATE_EXTRACT.method,
                            argValueList[0], operand
                        )
                    }
                    TIMESTAMP -> {
                        operand = Expressions.call(
                            BuiltInMethod.FLOOR_DIV.method,
                            operand, Expressions.constant(TimeUnit.DAY.multiplier.longValue())
                        )
                        return Expressions.call(
                            BuiltInMethod.UNIX_DATE_EXTRACT.method,
                            argValueList[0], operand
                        )
                    }
                    DATE -> return Expressions.call(
                        BuiltInMethod.UNIX_DATE_EXTRACT.method,
                        argValueList[0], operand
                    )
                    else -> throw AssertionError("unexpected $sqlTypeName")
                }
                MILLISECOND, MICROSECOND, NANOSECOND -> {
                    if (sqlTypeName === SqlTypeName.DATE) {
                        return Expressions.constant(0L)
                    }
                    operand = mod(operand, TimeUnit.MINUTE.multiplier.longValue())
                    return Expressions.multiply(
                        operand, Expressions.constant((1 / unit.multiplier.doubleValue()) as Long)
                    )
                }
                EPOCH -> when (sqlTypeName) {
                    DATE -> {
                        // convert to milliseconds
                        operand = Expressions.multiply(
                            operand,
                            Expressions.constant(TimeUnit.DAY.multiplier.longValue())
                        )
                        // convert to seconds
                        return Expressions.divide(
                            operand,
                            Expressions.constant(TimeUnit.SECOND.multiplier.longValue())
                        )
                    }
                    TIMESTAMP -> return Expressions.divide(
                        operand,
                        Expressions.constant(TimeUnit.SECOND.multiplier.longValue())
                    )
                    TIMESTAMP_WITH_LOCAL_TIME_ZONE -> {
                        operand = Expressions.call(
                            BuiltInMethod.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIMESTAMP.method,
                            operand,
                            Expressions.call(BuiltInMethod.TIME_ZONE.method, translator.getRoot())
                        )
                        return Expressions.divide(
                            operand,
                            Expressions.constant(TimeUnit.SECOND.multiplier.longValue())
                        )
                    }
                    INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> throw AssertionError(
                        "unexpected $sqlTypeName"
                    )
                    else -> {}
                }
                HOUR, MINUTE, SECOND -> when (sqlTypeName) {
                    DATE -> return Expressions.multiply(operand, Expressions.constant(0L))
                    else -> {}
                }
            }
            operand = mod(operand, getFactor(unit))
            if (unit === TimeUnit.QUARTER) {
                operand = Expressions.subtract(operand, Expressions.constant(1L))
            }
            operand = Expressions.divide(
                operand,
                Expressions.constant(unit.multiplier.longValue())
            )
            if (unit === TimeUnit.QUARTER) {
                operand = Expressions.add(operand, Expressions.constant(1L))
            }
            return operand
        }
    }

    /** Implementor for the SQL `COALESCE` operator.  */
    private class CoalesceImplementor internal constructor() : AbstractRexCallImplementor(NullPolicy.NONE, false) {
        @get:Override
        override val variableName: String
            get() = "coalesce"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator,
            call: RexCall?, argValueList: List<Expression>
        ): Expression {
            return implementRecurse(translator, argValueList)
        }

        companion object {
            private fun implementRecurse(
                translator: RexToLixTranslator,
                argValueList: List<Expression>
            ): Expression {
                return if (argValueList.size() === 1) {
                    argValueList[0]
                } else {
                    Expressions.condition(
                        translator.checkNotNull(argValueList[0]),
                        argValueList[0],
                        implementRecurse(
                            translator,
                            Util.skip(argValueList)
                        )
                    )
                }
            }
        }
    }

    /** Implementor for the SQL `CAST` operator.  */
    private class CastImplementor internal constructor() : AbstractRexCallImplementor(NullPolicy.STRICT, false) {
        @get:Override
        override val variableName: String
            get() = "cast"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator,
            call: RexCall, argValueList: List<Expression>
        ): Expression? {
            assert(call.getOperands().size() === 1)
            val sourceType: RelDataType = call.getOperands().get(0).getType()

            // Short-circuit if no cast is required
            val arg: RexNode = call.getOperands().get(0)
            if (call.getType().equals(sourceType)) {
                // No cast required, omit cast
                return argValueList[0]
            }
            if (SqlTypeUtil.equalSansNullability(
                    translator.typeFactory,
                    call.getType(), arg.getType()
                )
                && translator.deref(arg) is RexLiteral
            ) {
                return RexToLixTranslator.translateLiteral(
                    translator.deref(arg) as RexLiteral, call.getType(),
                    translator.typeFactory, NullAs.NULL
                )
            }
            val targetType: RelDataType = nullifyType(translator.typeFactory, call.getType(), false)
            return translator.translateCast(
                sourceType,
                targetType, argValueList[0]
            )
        }

        companion object {
            private fun nullifyType(
                typeFactory: JavaTypeFactory,
                type: RelDataType, nullable: Boolean
            ): RelDataType {
                if (type is RelDataTypeFactoryImpl.JavaType) {
                    val javaClass: Class<*> = (type as RelDataTypeFactoryImpl.JavaType).getJavaClass()
                    val primitive: Class<*> = Primitive.unbox(javaClass)
                    if (primitive !== javaClass) {
                        return typeFactory.createJavaType(primitive)
                    }
                }
                return typeFactory.createTypeWithNullability(type, nullable)
            }
        }
    }

    /** Implementor for the `REINTERPRET` internal SQL operator.  */
    private class ReinterpretImplementor internal constructor() : AbstractRexCallImplementor(NullPolicy.STRICT, false) {
        @get:Override
        override val variableName: String
            get() = "reInterpret"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall, argValueList: List<Expression?>
        ): Expression? {
            assert(call.getOperands().size() === 1)
            return argValueList[0]
        }
    }

    /** Implementor for a array concat.  */
    private class ArrayConcatImplementor internal constructor() : AbstractRexCallImplementor(NullPolicy.STRICT, false) {
        @get:Override
        override val variableName: String
            get() = "array_concat"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator, call: RexCall?,
            argValueList: List<Expression?>
        ): Expression {
            val blockBuilder: BlockBuilder = translator.getBlockBuilder()
            val list: Expression = blockBuilder.append("list", Expressions.new_(ArrayList::class.java), false)
            val nullValue: Expression = Expressions.constant(null)
            for (expression in argValueList) {
                blockBuilder.add(
                    Expressions.ifThenElse(
                        Expressions.or(
                            Expressions.equal(nullValue, list),
                            Expressions.equal(nullValue, expression)
                        ),
                        Expressions.assign(list, nullValue),
                        Expressions.statement(
                            Expressions.call(list, BuiltInMethod.COLLECTION_ADDALL.method, expression)
                        )
                    )
                )
            }
            return list
        }
    }

    /** Implementor for a value-constructor.  */
    private class ValueConstructorImplementor internal constructor() :
        AbstractRexCallImplementor(NullPolicy.NONE, false) {
        @get:Override
        override val variableName: String
            get() = "value_constructor"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator,
            call: RexCall, argValueList: List<Expression?>
        ): Expression {
            val kind: SqlKind = call.getOperator().getKind()
            val blockBuilder: BlockBuilder = translator.getBlockBuilder()
            return when (kind) {
                MAP_VALUE_CONSTRUCTOR -> {
                    val map: Expression = blockBuilder.append(
                        "map", Expressions.new_(LinkedHashMap::class.java),
                        false
                    )
                    var i = 0
                    while (i < argValueList.size()) {
                        val key: Expression? = argValueList[i++]
                        val value: Expression? = argValueList[i]
                        blockBuilder.add(
                            Expressions.statement(
                                Expressions.call(
                                    map, BuiltInMethod.MAP_PUT.method,
                                    Expressions.box(key), Expressions.box(value)
                                )
                            )
                        )
                        i++
                    }
                    map
                }
                ARRAY_VALUE_CONSTRUCTOR -> {
                    val lyst: Expression = blockBuilder.append(
                        "list", Expressions.new_(ArrayList::class.java),
                        false
                    )
                    for (value in argValueList) {
                        blockBuilder.add(
                            Expressions.statement(
                                Expressions.call(
                                    lyst, BuiltInMethod.COLLECTION_ADD.method,
                                    Expressions.box(value)
                                )
                            )
                        )
                    }
                    lyst
                }
                else -> throw AssertionError("unexpected: $kind")
            }
        }
    }

    /** Implementor for the `ITEM` SQL operator.  */
    private class ItemImplementor internal constructor() : AbstractRexCallImplementor(NullPolicy.STRICT, false) {
        @get:Override
        override val variableName: String
            get() = "item"

        // Since we follow PostgreSQL's semantics that an out-of-bound reference
        // returns NULL, x[y] can return null even if x and y are both NOT NULL.
        // (In SQL standard semantics, an out-of-bound reference to an array
        // throws an exception.)
        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall, argValueList: List<Expression?>
        ): Expression {
            val implementor = getImplementor(call.getOperands().get(0).getType().getSqlTypeName())
            return implementor.implementSafe(translator, call, argValueList)
        }

        private fun getImplementor(sqlTypeName: SqlTypeName): MethodImplementor {
            return when (sqlTypeName) {
                ARRAY -> MethodImplementor(
                    BuiltInMethod.ARRAY_ITEM.method,
                    nullPolicy,
                    false
                )
                MAP -> MethodImplementor(
                    BuiltInMethod.MAP_ITEM.method,
                    nullPolicy,
                    false
                )
                else -> MethodImplementor(
                    BuiltInMethod.ANY_ITEM.method,
                    nullPolicy,
                    false
                )
            }
        }
    }

    /** Implementor for SQL system functions.
     *
     *
     * Several of these are represented internally as constant values, set
     * per execution.  */
    private class SystemFunctionImplementor internal constructor() :
        AbstractRexCallImplementor(NullPolicy.NONE, false) {
        @get:Override
        override val variableName: String
            get() = "system_func"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator,
            call: RexCall, argValueList: List<Expression?>?
        ): Expression {
            val op: SqlOperator = call.getOperator()
            val root: Expression = translator.getRoot()
            return if (op === CURRENT_USER || op === SESSION_USER || op === USER) {
                Expressions.call(BuiltInMethod.USER.method, root)
            } else if (op === SYSTEM_USER) {
                Expressions.call(BuiltInMethod.SYSTEM_USER.method, root)
            } else if (op === CURRENT_PATH || op === CURRENT_ROLE || op === CURRENT_CATALOG) {
                // By default, the CURRENT_ROLE and CURRENT_CATALOG functions return the
                // empty string because a role or a catalog has to be set explicitly.
                Expressions.constant("")
            } else if (op === CURRENT_TIMESTAMP) {
                Expressions.call(BuiltInMethod.CURRENT_TIMESTAMP.method, root)
            } else if (op === CURRENT_TIME) {
                Expressions.call(BuiltInMethod.CURRENT_TIME.method, root)
            } else if (op === CURRENT_DATE) {
                Expressions.call(BuiltInMethod.CURRENT_DATE.method, root)
            } else if (op === LOCALTIMESTAMP) {
                Expressions.call(BuiltInMethod.LOCAL_TIMESTAMP.method, root)
            } else if (op === LOCALTIME) {
                Expressions.call(BuiltInMethod.LOCAL_TIME.method, root)
            } else {
                throw AssertionError("unknown function $op")
            }
        }
    }

    /** Implementor for the `NOT` operator.  */
    private class NotImplementor private constructor(private val implementor: AbstractRexCallImplementor) :
        AbstractRexCallImplementor(null, false) {
        @get:Override
        override val variableName: String
            get() = "not"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall?, argValueList: List<Expression?>?
        ): Expression {
            val expression: Expression = implementor.implementSafe(translator, call, argValueList)
            return Expressions.not(expression)
        }

        companion object {
            fun of(implementor: AbstractRexCallImplementor): AbstractRexCallImplementor {
                return NotImplementor(implementor)
            }
        }
    }

    /** Implementor for various datetime arithmetic.  */
    private class DatetimeArithmeticImplementor internal constructor() :
        AbstractRexCallImplementor(NullPolicy.STRICT, false) {
        @get:Override
        override val variableName: String
            get() = "dateTime_arithmetic"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall, argValueList: List<Expression?>
        ): Expression? {
            val operand0: RexNode = call.getOperands().get(0)
            var trop0: Expression? = argValueList[0]
            val typeName1: SqlTypeName = call.getOperands().get(1).getType().getSqlTypeName()
            var trop1: Expression? = argValueList[1]
            val typeName: SqlTypeName = call.getType().getSqlTypeName()
            when (operand0.getType().getSqlTypeName()) {
                DATE -> when (typeName) {
                    TIMESTAMP -> trop0 = Expressions.convert_(
                        Expressions.multiply(
                            trop0,
                            Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)
                        ),
                        Long::class.javaPrimitiveType
                    )
                    else -> when (typeName1) {
                        INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> trop1 =
                            Expressions.convert_(
                                Expressions.divide(
                                    trop1,
                                    Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)
                                ),
                                Int::class.javaPrimitiveType
                            )
                        else -> {}
                    }
                }
                TIME -> trop1 = Expressions.convert_(trop1, Int::class.javaPrimitiveType)
                else -> {}
            }
            return when (typeName1) {
                INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH -> {
                    when (call.getKind()) {
                        MINUS -> trop1 = Expressions.negate(trop1)
                        else -> {}
                    }
                    when (typeName) {
                        TIME -> Expressions.convert_(trop0, Long::class.javaPrimitiveType)
                        else -> {
                            val method: BuiltInMethod = if (operand0.getType()
                                    .getSqlTypeName() === SqlTypeName.TIMESTAMP
                            ) BuiltInMethod.ADD_MONTHS else BuiltInMethod.ADD_MONTHS_INT
                            Expressions.call(method.method, trop0, trop1)
                        }
                    }
                }
                INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> when (call.getKind()) {
                    MINUS -> normalize(
                        typeName,
                        Expressions.subtract(trop0, trop1)
                    )
                    else -> normalize(
                        typeName,
                        Expressions.add(trop0, trop1)
                    )
                }
                else -> when (call.getKind()) {
                    MINUS -> {
                        when (typeName) {
                            INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH -> return Expressions.call(
                                BuiltInMethod.SUBTRACT_MONTHS.method,
                                trop0, trop1
                            )
                            else -> {}
                        }
                        val fromUnit: TimeUnit =
                            if (typeName1 === SqlTypeName.DATE) TimeUnit.DAY else TimeUnit.MILLISECOND
                        val toUnit: TimeUnit = TimeUnit.MILLISECOND
                        multiplyDivide(
                            Expressions.convert_(
                                Expressions.subtract(trop0, trop1),
                                Long::class.javaPrimitiveType as Class?
                            ),
                            fromUnit.multiplier, toUnit.multiplier
                        )
                    }
                    else -> throw AssertionError(call)
                }
            }
        }

        companion object {
            /** Normalizes a TIME value into 00:00:00..23:59:39.  */
            private fun normalize(typeName: SqlTypeName, e: Expression): Expression {
                return when (typeName) {
                    TIME -> Expressions.call(
                        BuiltInMethod.FLOOR_MOD.method, e,
                        Expressions.constant(DateTimeUtils.MILLIS_PER_DAY)
                    )
                    else -> e
                }
            }
        }
    }

    /** Implements CLASSIFIER match-recognize function.  */
    private class ClassifierImplementor : MatchImplementor {
        @Override
        override fun implement(
            translator: RexToLixTranslator?, call: RexCall?,
            row: ParameterExpression?, rows: ParameterExpression?,
            symbols: ParameterExpression?, i: ParameterExpression?
        ): Expression? {
            return EnumUtils.convert(
                Expressions.call(symbols, BuiltInMethod.LIST_GET.method, i),
                String::class.java
            )
        }
    }

    /** Implements the LAST match-recognize function.  */
    private class LastImplementor : MatchImplementor {
        @Override
        fun implement(
            translator: RexToLixTranslator, call: RexCall,
            row: ParameterExpression?, rows: ParameterExpression?,
            symbols: ParameterExpression?, i: ParameterExpression?
        ): Expression {
            val node: RexNode = call.getOperands().get(0)
            val alpha: String = (call.getOperands().get(0) as RexPatternFieldRef).getAlpha()

            // TODO: verify if the variable is needed
            @SuppressWarnings("unused") val lastIndex: BinaryExpression = Expressions.subtract(
                Expressions.call(rows, BuiltInMethod.COLLECTION_SIZE.method),
                Expressions.constant(1)
            )

            // Just take the last one, if exists
            return if ("*".equals(alpha)) {
                setInputGetterIndex(translator, i)
                // Important, unbox the node / expression to avoid NullAs.NOT_POSSIBLE
                val ref: RexPatternFieldRef = node as RexPatternFieldRef
                val newRef = RexPatternFieldRef(
                    ref.getAlpha(),
                    ref.getIndex(),
                    translator.typeFactory.createTypeWithNullability(
                        ref.getType(),
                        true
                    )
                )
                val expression: Expression =
                    translator.translate(newRef, NullAs.NULL)
                setInputGetterIndex(
                    translator,
                    null
                )
                expression
            } else {
                // Alpha != "*" so we have to search for a specific one to find and use that, if found
                setInputGetterIndex(
                    translator,
                    Expressions.call(
                        BuiltInMethod.MATCH_UTILS_LAST_WITH_SYMBOL.method,
                        Expressions.constant(alpha), rows, symbols, i
                    )
                )

                // Important, unbox the node / expression to avoid NullAs.NOT_POSSIBLE
                val ref: RexPatternFieldRef = node as RexPatternFieldRef
                val newRef = RexPatternFieldRef(
                    ref.getAlpha(),
                    ref.getIndex(),
                    translator.typeFactory.createTypeWithNullability(
                        ref.getType(),
                        true
                    )
                )
                val expression: Expression =
                    translator.translate(newRef, NullAs.NULL)
                setInputGetterIndex(
                    translator,
                    null
                )
                expression
            }
        }

        companion object {
            private fun setInputGetterIndex(translator: RexToLixTranslator, @Nullable o: Expression?) {
                requireNonNull(
                    translator.inputGetter as EnumerableMatch.PassedRowsInputGetter,
                    "inputGetter"
                ).setIndex(o)
            }
        }
    }

    /** Null-safe implementor of `RexCall`s.  */
    interface RexCallImplementor {
        fun implement(
            translator: RexToLixTranslator?,
            call: RexCall?,
            arguments: List<RexToLixTranslator.Result?>?
        ): RexToLixTranslator.Result
    }

    /**
     * Abstract implementation of the [RexCallImplementor] interface.
     *
     *
     * It is not always safe to execute the [RexCall] directly due to
     * the special null arguments. Therefore, the generated code logic is
     * conditional correspondingly.
     *
     *
     * For example, `a + b` will generate two declaration statements:
     *
     * <blockquote>
     * `
     * final Integer xxx_value = (a_isNull || b_isNull) ? null : plus(a, b);<br></br>
     * final boolean xxx_isNull = xxx_value == null;
    ` *
    </blockquote> *
     */
    private abstract class AbstractRexCallImplementor internal constructor(
        @Nullable nullPolicy: NullPolicy?,
        harmonize: Boolean
    ) : RexCallImplementor {
        @Nullable
        val nullPolicy: NullPolicy?
        private val harmonize: Boolean

        init {
            this.nullPolicy = nullPolicy
            this.harmonize = harmonize
        }

        @Override
        override fun implement(
            translator: RexToLixTranslator,
            call: RexCall,
            arguments: List<RexToLixTranslator.Result>
        ): RexToLixTranslator.Result {
            val argIsNullList: List<Expression> = ArrayList()
            val argValueList: List<Expression> = ArrayList()
            for (result in arguments) {
                argIsNullList.add(result.isNullVariable)
                argValueList.add(result.valueVariable)
            }
            val condition: Expression = getCondition(argIsNullList)
            val valueVariable: ParameterExpression = genValueStatement(translator, call, argValueList, condition)
            val isNullVariable: ParameterExpression = genIsNullStatement(translator, valueVariable)
            return Result(isNullVariable, valueVariable)
        }

        // Variable name facilitates reasoning about issues when necessary
        abstract val variableName: String

        /** Figures out conditional expression according to NullPolicy.  */
        fun getCondition(argIsNullList: List<Expression>): Expression {
            if (argIsNullList.size() === 0 || nullPolicy == null || nullPolicy === NullPolicy.NONE) {
                return FALSE_EXPR
            }
            return if (nullPolicy === NullPolicy.ARG0) {
                argIsNullList[0]
            } else Expressions.foldOr(argIsNullList)
        }

        // E.g., "final Integer xxx_value = (a_isNull || b_isNull) ? null : plus(a, b)"
        private fun genValueStatement(
            translator: RexToLixTranslator,
            call: RexCall, argValueList: List<Expression>,
            condition: Expression
        ): ParameterExpression {
            var optimizedArgValueList: List<Expression> = argValueList
            if (harmonize) {
                optimizedArgValueList = harmonize(optimizedArgValueList, translator, call)
            }
            optimizedArgValueList = unboxIfNecessary(optimizedArgValueList)
            val callValue: Expression = implementSafe(translator, call, optimizedArgValueList)

            // In general, RexCall's type is correct for code generation
            // and thus we should ensure the consistency.
            // However, for some special cases (e.g., TableFunction),
            // the implementation's type is correct, we can't convert it.
            val op: SqlOperator = call.getOperator()
            val returnType: Type = translator.typeFactory.getJavaClass(call.getType())
            val noConvert = (returnType == null
                    || returnType === callValue.getType()
                    || op is SqlUserDefinedTableMacro
                    || op is SqlUserDefinedTableFunction)
            val convertedCallValue: Expression = if (noConvert) callValue else EnumUtils.convert(callValue, returnType)
            val valueExpression: Expression = Expressions.condition(
                condition,
                getIfTrue(convertedCallValue.getType(), argValueList),
                convertedCallValue
            )
            val value: ParameterExpression = Expressions.parameter(
                convertedCallValue.getType(),
                translator.getBlockBuilder().newName(getVariableName().toString() + "_value")
            )
            translator.getBlockBuilder().add(
                Expressions.declare(Modifier.FINAL, value, valueExpression)
            )
            return value
        }

        fun getIfTrue(type: Type?, argValueList: List<Expression?>?): Expression {
            return getDefaultValue(type)
        }

        // E.g., "final boolean xxx_isNull = xxx_value == null"
        private fun genIsNullStatement(
            translator: RexToLixTranslator, value: ParameterExpression
        ): ParameterExpression {
            val isNullVariable: ParameterExpression = Expressions.parameter(
                Boolean.TYPE,
                translator.getBlockBuilder().newName(getVariableName().toString() + "_isNull")
            )
            val isNullExpression: Expression = translator.checkNull(value)
            translator.getBlockBuilder().add(
                Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression)
            )
            return isNullVariable
        }

        /** Under null check, it is safe to unbox the operands before entering the
         * implementor.  */
        private fun unboxIfNecessary(argValueList: List<Expression>): List<Expression> {
            var unboxValueList: List<Expression> = argValueList
            if (nullPolicy === NullPolicy.STRICT || nullPolicy === NullPolicy.ANY || nullPolicy === NullPolicy.SEMI_STRICT) {
                unboxValueList = argValueList.stream()
                    .map { argValue: Expression -> unboxExpression(argValue) }
                    .collect(Collectors.toList())
            }
            if (nullPolicy === NullPolicy.ARG0 && argValueList.size() > 0) {
                val unboxArg0: Expression? = unboxExpression(
                    unboxValueList[0]
                )
                unboxValueList.set(0, unboxArg0)
            }
            return unboxValueList
        }

        abstract fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall?, argValueList: List<Expression?>?
        ): Expression

        companion object {
            /** Ensures that operands have identical type.  */
            private fun harmonize(
                argValueList: List<Expression>,
                translator: RexToLixTranslator, call: RexCall
            ): List<Expression> {
                var nullCount = 0
                val types: List<RelDataType> = ArrayList()
                val typeFactory: RelDataTypeFactory = translator.builder.getTypeFactory()
                for (operand in call.getOperands()) {
                    var type: RelDataType = operand.getType()
                    type = toSql(typeFactory, type)
                    if (translator.isNullable(operand)) {
                        ++nullCount
                    } else {
                        type = typeFactory.createTypeWithNullability(type, false)
                    }
                    types.add(type)
                }
                if (allSame<Any>(types)) {
                    // Operands have the same nullability and type. Return them
                    // unchanged.
                    return argValueList
                }
                val type: RelDataType = typeFactory.leastRestrictive(types)
                    ?: // There is no common type. Presumably this is a binary operator with
                    // asymmetric arguments (e.g. interval / integer) which is not intended
                    // to be harmonized.
                    return argValueList
                assert(nullCount > 0 == type.isNullable())
                val javaClass: Type = translator.typeFactory.getJavaClass(type)
                val harmonizedArgValues: List<Expression> = ArrayList()
                for (argValue in argValueList) {
                    harmonizedArgValues.add(
                        EnumUtils.convert(argValue, javaClass)
                    )
                }
                return harmonizedArgValues
            }

            private fun unboxExpression(argValue: Expression): Expression? {
                val fromBox: Primitive = Primitive.ofBox(argValue.getType())
                if (fromBox == null || fromBox === Primitive.VOID) {
                    return argValue
                }
                // Optimization: for "long x";
                // "Long.valueOf(x)" generates "x"
                if (argValue is MethodCallExpression) {
                    val mce: MethodCallExpression = argValue as MethodCallExpression
                    if (mce.method.getName().equals("valueOf") && mce.expressions.size() === 1) {
                        val originArg: Expression = mce.expressions.get(0)
                        if (Primitive.of(originArg.type) === fromBox) {
                            return originArg
                        }
                    }
                }
                return NullAs.NOT_POSSIBLE.handle(argValue)
            }
        }
    }

    /**
     * Implementor for the `AND` operator.
     *
     *
     * If any of the arguments are false, result is false;
     * else if any arguments are null, result is null;
     * else true.
     */
    private class LogicalAndImplementor internal constructor() : AbstractRexCallImplementor(NullPolicy.NONE, true) {
        @get:Override
        override val variableName: String
            get() = "logical_and"

        @Override
        override fun implement(
            translator: RexToLixTranslator,
            call: RexCall?, arguments: List<RexToLixTranslator.Result>
        ): RexToLixTranslator.Result {
            val argIsNullList: List<Expression> = ArrayList()
            for (result in arguments) {
                argIsNullList.add(result.isNullVariable)
            }
            val nullAsTrue: List<Expression> = arguments.stream()
                .map { result ->
                    Expressions.condition(
                        result.isNullVariable, TRUE_EXPR,
                        result.valueVariable
                    )
                }
                .collect(Collectors.toList())
            val hasFalse: Expression = Expressions.not(Expressions.foldAnd(nullAsTrue))
            val hasNull: Expression = Expressions.foldOr(argIsNullList)
            val callExpression: Expression = Expressions.condition(
                hasFalse, BOXED_FALSE_EXPR,
                Expressions.condition(hasNull, NULL_EXPR, BOXED_TRUE_EXPR)
            )
            val nullAs = if (translator.isNullable(call)) NullAs.NULL else NullAs.NOT_POSSIBLE
            val valueExpression: Expression? = nullAs.handle(callExpression)
            val valueVariable: ParameterExpression = Expressions.parameter(
                valueExpression.getType(),
                translator.getBlockBuilder().newName(getVariableName().toString() + "_value")
            )
            val isNullExpression: Expression = translator.checkNull(valueVariable)
            val isNullVariable: ParameterExpression = Expressions.parameter(
                Boolean.TYPE,
                translator.getBlockBuilder().newName(getVariableName().toString() + "_isNull")
            )
            translator.getBlockBuilder().add(
                Expressions.declare(Modifier.FINAL, valueVariable, valueExpression)
            )
            translator.getBlockBuilder().add(
                Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression)
            )
            return Result(isNullVariable, valueVariable)
        }

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall?, argValueList: List<Expression?>?
        ): Expression {
            throw IllegalStateException(
                "This implementSafe should not be called,"
                        + " please call implement(...)"
            )
        }
    }

    /**
     * Implementor for the `OR` operator.
     *
     *
     * If any of the arguments are true, result is true;
     * else if any arguments are null, result is null;
     * else false.
     */
    private class LogicalOrImplementor internal constructor() : AbstractRexCallImplementor(NullPolicy.NONE, true) {
        @get:Override
        override val variableName: String
            get() = "logical_or"

        @Override
        override fun implement(
            translator: RexToLixTranslator,
            call: RexCall?, arguments: List<RexToLixTranslator.Result>
        ): RexToLixTranslator.Result {
            val argIsNullList: List<Expression> = ArrayList()
            for (result in arguments) {
                argIsNullList.add(result.isNullVariable)
            }
            val nullAsFalse: List<Expression> = arguments.stream()
                .map { result ->
                    Expressions.condition(
                        result.isNullVariable, FALSE_EXPR,
                        result.valueVariable
                    )
                }
                .collect(Collectors.toList())
            val hasTrue: Expression = Expressions.foldOr(nullAsFalse)
            val hasNull: Expression = Expressions.foldOr(argIsNullList)
            val callExpression: Expression = Expressions.condition(
                hasTrue, BOXED_TRUE_EXPR,
                Expressions.condition(hasNull, NULL_EXPR, BOXED_FALSE_EXPR)
            )
            val nullAs = if (translator.isNullable(call)) NullAs.NULL else NullAs.NOT_POSSIBLE
            val valueExpression: Expression? = nullAs.handle(callExpression)
            val valueVariable: ParameterExpression = Expressions.parameter(
                valueExpression.getType(),
                translator.getBlockBuilder().newName(getVariableName().toString() + "_value")
            )
            val isNullExpression: Expression = translator.checkNull(valueExpression)
            val isNullVariable: ParameterExpression = Expressions.parameter(
                Boolean.TYPE,
                translator.getBlockBuilder().newName(getVariableName().toString() + "_isNull")
            )
            translator.getBlockBuilder().add(
                Expressions.declare(Modifier.FINAL, valueVariable, valueExpression)
            )
            translator.getBlockBuilder().add(
                Expressions.declare(Modifier.FINAL, isNullVariable, isNullExpression)
            )
            return Result(isNullVariable, valueVariable)
        }

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall?, argValueList: List<Expression?>?
        ): Expression {
            throw IllegalStateException(
                "This implementSafe should not be called,"
                        + " please call implement(...)"
            )
        }
    }

    /**
     * Implementor for the `NOT` operator.
     *
     *
     * If any of the arguments are false, result is true;
     * else if any arguments are null, result is null;
     * else false.
     */
    private class LogicalNotImplementor internal constructor() : AbstractRexCallImplementor(NullPolicy.NONE, true) {
        @get:Override
        override val variableName: String
            get() = "logical_not"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall?, argValueList: List<Expression?>?
        ): Expression {
            return Expressions.call(BuiltInMethod.NOT.method, argValueList)
        }
    }

    /**
     * Implementation that calls a given [java.lang.reflect.Method].
     *
     *
     * When method is not static, a new instance of the required class is
     * created.
     */
    private class ReflectiveImplementor internal constructor(method: Method, @Nullable nullPolicy: NullPolicy?) :
        AbstractRexCallImplementor(nullPolicy, false) {
        protected val method: Method

        init {
            this.method = method
        }

        @get:Override
        override val variableName: String
            get() = "reflective_" + method.getName()

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall?, argValueList: List<Expression?>
        ): Expression {
            val argValueList0: List<Expression> = EnumUtils.fromInternal(method.getParameterTypes(), argValueList)
            return if (method.getModifiers() and Modifier.STATIC !== 0) {
                Expressions.call(method, argValueList0)
            } else {
                // The UDF class must have a public zero-args constructor.
                // Assume that the validator checked already.
                val target: Expression = Expressions.new_(method.getDeclaringClass())
                Expressions.call(target, method, argValueList0)
            }
        }
    }

    /** Implementor for the `RAND` function.  */
    private class RandImplementor internal constructor() : AbstractRexCallImplementor(NullPolicy.STRICT, false) {
        private val implementors = arrayOf<AbstractRexCallImplementor>(
            ReflectiveImplementor(BuiltInMethod.RAND.method, nullPolicy),
            ReflectiveImplementor(BuiltInMethod.RAND_SEED.method, nullPolicy)
        )

        @get:Override
        override val variableName: String
            get() = "rand"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall, argValueList: List<Expression?>?
        ): Expression {
            return implementors[call.getOperands().size()]
                .implementSafe(translator, call, argValueList)
        }
    }

    /** Implementor for the `RAND_INTEGER` function.  */
    private class RandIntegerImplementor internal constructor() : AbstractRexCallImplementor(NullPolicy.STRICT, false) {
        private val implementors = arrayOf<AbstractRexCallImplementor>(
            ReflectiveImplementor(BuiltInMethod.RAND_INTEGER.method, nullPolicy),
            ReflectiveImplementor(BuiltInMethod.RAND_INTEGER_SEED.method, nullPolicy)
        )

        @get:Override
        override val variableName: String
            get() = "rand_integer"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall, argValueList: List<Expression?>?
        ): Expression {
            return implementors[call.getOperands().size() - 1]
                .implementSafe(translator, call, argValueList)
        }
    }

    /** Implementor for the `PI` operator.  */
    private class PiImplementor internal constructor() : AbstractRexCallImplementor(NullPolicy.NONE, false) {
        @get:Override
        override val variableName: String
            get() = "pi"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall?, argValueList: List<Expression?>?
        ): Expression {
            return Expressions.constant(Math.PI)
        }
    }

    /** Implementor for the `IS FALSE` SQL operator.  */
    private class IsFalseImplementor internal constructor() : AbstractRexCallImplementor(NullPolicy.STRICT, false) {
        @get:Override
        override val variableName: String
            get() = "is_false"

        @Override
        override fun getIfTrue(type: Type?, argValueList: List<Expression?>?): Expression {
            return Expressions.constant(false, type)
        }

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall?, argValueList: List<Expression?>
        ): Expression {
            return Expressions.equal(argValueList[0], FALSE_EXPR)
        }
    }

    /** Implementor for the `IS NOT FALSE` SQL operator.  */
    private class IsNotFalseImplementor internal constructor() : AbstractRexCallImplementor(NullPolicy.STRICT, false) {
        @get:Override
        override val variableName: String
            get() = "is_not_false"

        @Override
        override fun getIfTrue(type: Type?, argValueList: List<Expression?>?): Expression {
            return Expressions.constant(true, type)
        }

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall?, argValueList: List<Expression?>
        ): Expression {
            return Expressions.notEqual(argValueList[0], FALSE_EXPR)
        }
    }

    /** Implementor for the `IS NOT NULL` SQL operator.  */
    private class IsNotNullImplementor internal constructor() : AbstractRexCallImplementor(NullPolicy.STRICT, false) {
        @get:Override
        override val variableName: String
            get() = "is_not_null"

        @Override
        override fun getIfTrue(type: Type?, argValueList: List<Expression?>?): Expression {
            return Expressions.constant(false, type)
        }

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall?, argValueList: List<Expression?>
        ): Expression {
            return Expressions.notEqual(argValueList[0], NULL_EXPR)
        }
    }

    /** Implementor for the `IS NOT TRUE` SQL operator.  */
    private class IsNotTrueImplementor internal constructor() : AbstractRexCallImplementor(NullPolicy.STRICT, false) {
        @get:Override
        override val variableName: String
            get() = "is_not_true"

        @Override
        override fun getIfTrue(type: Type?, argValueList: List<Expression?>?): Expression {
            return Expressions.constant(true, type)
        }

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall?, argValueList: List<Expression?>
        ): Expression {
            return Expressions.notEqual(argValueList[0], TRUE_EXPR)
        }
    }

    /** Implementor for the `IS NULL` SQL operator.  */
    private class IsNullImplementor internal constructor() : AbstractRexCallImplementor(NullPolicy.STRICT, false) {
        @get:Override
        override val variableName: String
            get() = "is_null"

        @Override
        override fun getIfTrue(type: Type?, argValueList: List<Expression?>?): Expression {
            return Expressions.constant(true, type)
        }

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall?, argValueList: List<Expression?>
        ): Expression {
            return Expressions.equal(argValueList[0], NULL_EXPR)
        }
    }

    /** Implementor for the `IS TRUE` SQL operator.  */
    private class IsTrueImplementor internal constructor() : AbstractRexCallImplementor(NullPolicy.STRICT, false) {
        @get:Override
        override val variableName: String
            get() = "is_true"

        @Override
        override fun getIfTrue(type: Type?, argValueList: List<Expression?>?): Expression {
            return Expressions.constant(false, type)
        }

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall?, argValueList: List<Expression?>
        ): Expression {
            return Expressions.equal(argValueList[0], TRUE_EXPR)
        }
    }

    /** Implementor for the `REGEXP_REPLACE` function.  */
    private class RegexpReplaceImplementor internal constructor() :
        AbstractRexCallImplementor(NullPolicy.STRICT, false) {
        private val implementors = arrayOf<AbstractRexCallImplementor>(
            ReflectiveImplementor(BuiltInMethod.REGEXP_REPLACE3.method, nullPolicy),
            ReflectiveImplementor(BuiltInMethod.REGEXP_REPLACE4.method, nullPolicy),
            ReflectiveImplementor(BuiltInMethod.REGEXP_REPLACE5.method, nullPolicy),
            ReflectiveImplementor(BuiltInMethod.REGEXP_REPLACE6.method, nullPolicy)
        )

        @get:Override
        override val variableName: String
            get() = "regexp_replace"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall, argValueList: List<Expression?>?
        ): Expression {
            return implementors[call.getOperands().size() - 3]
                .implementSafe(translator, call, argValueList)
        }
    }

    /** Implementor for the `DEFAULT` function.  */
    private class DefaultImplementor internal constructor() : AbstractRexCallImplementor(NullPolicy.NONE, false) {
        @get:Override
        override val variableName: String
            get() = "default"

        @Override
        override fun implementSafe(
            translator: RexToLixTranslator?,
            call: RexCall?, argValueList: List<Expression?>?
        ): Expression {
            return Expressions.constant(null)
        }
    }

    /** Implements the `TUMBLE` table function.  */
    private class TumbleImplementor : TableFunctionCallImplementor {
        @Override
        fun implement(
            translator: RexToLixTranslator,
            inputEnumerable: Expression?,
            call: RexCall, inputPhysType: PhysType, outputPhysType: PhysType
        ): Expression {
            // The table operand is removed from the RexCall because it
            // represents the input, see StandardConvertletTable#convertWindowFunction.
            val intervalExpression: Expression = translator.translate(call.getOperands().get(1))
            val descriptor: RexCall = call.getOperands().get(0) as RexCall
            val parameter: ParameterExpression = Expressions.parameter(
                Primitive.box(inputPhysType.getJavaRowType()),
                "_input"
            )
            val wmColExpr: Expression = inputPhysType.fieldReference(
                parameter,
                (descriptor.getOperands().get(0) as RexInputRef).getIndex(),
                outputPhysType.getJavaFieldType(
                    inputPhysType.getRowType().getFieldCount()
                )
            )

            // handle the optional offset parameter. Use 0 for the default value when offset
            // parameter is not set.
            val offsetExpr: Expression = if (call.getOperands().size() > 2) translator.translate(
                call.getOperands().get(2)
            ) else Expressions.constant(0, Long::class.javaPrimitiveType)
            return Expressions.call(
                BuiltInMethod.TUMBLING.method,
                inputEnumerable,
                EnumUtils.tumblingWindowSelector(
                    inputPhysType,
                    outputPhysType,
                    wmColExpr,
                    intervalExpression,
                    offsetExpr
                )
            )
        }
    }

    /** Implements the `HOP` table function.  */
    private class HopImplementor : TableFunctionCallImplementor {
        @Override
        fun implement(
            translator: RexToLixTranslator,
            inputEnumerable: Expression?, call: RexCall, inputPhysType: PhysType?, outputPhysType: PhysType?
        ): Expression {
            val slidingInterval: Expression = translator.translate(call.getOperands().get(1))
            val windowSize: Expression = translator.translate(call.getOperands().get(2))
            val descriptor: RexCall = call.getOperands().get(0) as RexCall
            val wmColIndexExpr: Expression =
                Expressions.constant((descriptor.getOperands().get(0) as RexInputRef).getIndex())

            // handle the optional offset parameter. Use 0 for the default value when offset
            // parameter is not set.
            val offsetExpr: Expression = if (call.getOperands().size() > 3) translator.translate(
                call.getOperands().get(3)
            ) else Expressions.constant(0, Long::class.javaPrimitiveType)
            return Expressions.call(
                BuiltInMethod.HOPPING.method,
                Expressions.list(
                    Expressions.call(
                        inputEnumerable,
                        BuiltInMethod.ENUMERABLE_ENUMERATOR.method
                    ),
                    wmColIndexExpr,
                    slidingInterval,
                    windowSize,
                    offsetExpr
                )
            )
        }
    }

    /** Implements the `SESSION` table function.  */
    private class SessionImplementor : TableFunctionCallImplementor {
        @Override
        fun implement(
            translator: RexToLixTranslator,
            inputEnumerable: Expression?, call: RexCall, inputPhysType: PhysType?, outputPhysType: PhysType?
        ): Expression {
            val timestampDescriptor: RexCall = call.getOperands().get(0) as RexCall
            val keyDescriptor: RexCall = call.getOperands().get(1) as RexCall
            val gapInterval: Expression = translator.translate(call.getOperands().get(2))
            val wmColIndexExpr: Expression =
                Expressions.constant((timestampDescriptor.getOperands().get(0) as RexInputRef).getIndex())
            val keyColIndexExpr: Expression =
                Expressions.constant((keyDescriptor.getOperands().get(0) as RexInputRef).getIndex())
            return Expressions.call(
                BuiltInMethod.SESSIONIZATION.method,
                Expressions.list(
                    Expressions.call(inputEnumerable, BuiltInMethod.ENUMERABLE_ENUMERATOR.method),
                    wmColIndexExpr,
                    keyColIndexExpr,
                    gapInterval
                )
            )
        }
    }

    companion object {
        val INSTANCE = RexImpTable()
        val NULL_EXPR: ConstantExpression = Expressions.constant(null)
        val FALSE_EXPR: ConstantExpression = Expressions.constant(false)
        val TRUE_EXPR: ConstantExpression = Expressions.constant(true)
        val COMMA_EXPR: ConstantExpression = Expressions.constant(",")
        val BOXED_FALSE_EXPR: MemberExpression = Expressions.field(null, Boolean::class.java, "FALSE")
        val BOXED_TRUE_EXPR: MemberExpression = Expressions.field(null, Boolean::class.java, "TRUE")
        private fun <T> constructorSupplier(klass: Class<T>): Supplier<T> {
            val constructor: Constructor<T>
            constructor = try {
                klass.getDeclaredConstructor()
            } catch (e: NoSuchMethodException) {
                throw IllegalArgumentException(klass.toString() + " should implement zero arguments constructor")
            }
            return label@ Supplier<T> {
                try {
                    return@label constructor.newInstance()
                } catch (e: InstantiationException) {
                    throw IllegalStateException(
                        "Error while creating aggregate implementor $constructor", e
                    )
                } catch (e: IllegalAccessException) {
                    throw IllegalStateException(
                        "Error while creating aggregate implementor $constructor", e
                    )
                } catch (e: InvocationTargetException) {
                    throw IllegalStateException(
                        "Error while creating aggregate implementor $constructor", e
                    )
                }
            }
        }

        fun createImplementor(
            implementor: NotNullImplementor,
            nullPolicy: NullPolicy,
            harmonize: Boolean
        ): CallImplementor {
            return CallImplementor { translator, call, nullAs ->
                val rexCallImplementor = createRexCallImplementor(implementor, nullPolicy, harmonize)
                val arguments: List<RexToLixTranslator.Result> = translator.getCallOperandResult(call)
                assert(arguments != null)
                val result: RexToLixTranslator.Result = rexCallImplementor.implement(translator, call, arguments)
                nullAs.handle(result.valueVariable)
            }
        }

        private fun createRexCallImplementor(
            implementor: NotNullImplementor,
            nullPolicy: NullPolicy,
            harmonize: Boolean
        ): RexCallImplementor {
            return object : AbstractRexCallImplementor(nullPolicy, harmonize) {
                @get:Override
                override val variableName: String
                    get() = "not_null_udf"

                @Override
                override fun implementSafe(
                    translator: RexToLixTranslator?,
                    call: RexCall?, argValueList: List<Expression?>?
                ): Expression {
                    return implementor.implement(translator, call, argValueList)
                }
            }
        }

        private fun wrapAsRexCallImplementor(
            implementor: CallImplementor
        ): RexCallImplementor {
            return object : AbstractRexCallImplementor(NullPolicy.NONE, false) {
                @get:Override
                override val variableName: String
                    get() = "udf"

                @Override
                override fun implementSafe(
                    translator: RexToLixTranslator?,
                    call: RexCall?, argValueList: List<Expression?>?
                ): Expression {
                    return implementor.implement(translator, call, NullAs.NULL)
                }
            }
        }

        fun optimize(expression: Expression): Expression {
            return expression.accept(OptimizeShuttle())
        }

        fun optimize2(operand: Expression, expression: Expression): Expression {
            return if (Primitive.`is`(operand.getType())) {
                // Primitive values cannot be null
                optimize(expression)
            } else {
                optimize(
                    Expressions.condition(
                        Expressions.equal(operand, NULL_EXPR),
                        NULL_EXPR,
                        expression
                    )
                )
            }
        }

        private fun toSql(
            typeFactory: RelDataTypeFactory,
            type: RelDataType
        ): RelDataType {
            if (type is RelDataTypeFactoryImpl.JavaType) {
                val typeName: SqlTypeName = type.getSqlTypeName()
                if (typeName != null && typeName !== SqlTypeName.OTHER) {
                    return typeFactory.createTypeWithNullability(
                        typeFactory.createSqlType(typeName),
                        type.isNullable()
                    )
                }
            }
            return type
        }

        private fun <E> allSame(list: List<E>): Boolean {
            var prev: E? = null
            for (e in list) {
                if (prev != null && !prev.equals(e)) {
                    return false
                }
                prev = e
            }
            return true
        }

        fun getDefaultValue(type: Type?): Expression {
            val p: Primitive = Primitive.of(type)
            return if (p != null) {
                Expressions.constant(p.defaultValue, type)
            } else Expressions.constant(null, type)
        }

        /** Multiplies an expression by a constant and divides by another constant,
         * optimizing appropriately.
         *
         *
         * For example, `multiplyDivide(e, 10, 1000)` returns
         * `e / 100`.  */
        fun multiplyDivide(
            e: Expression?, multiplier: BigDecimal,
            divider: BigDecimal
        ): Expression? {
            if (multiplier.equals(BigDecimal.ONE)) {
                return if (divider.equals(BigDecimal.ONE)) {
                    e
                } else Expressions.divide(
                    e,
                    Expressions.constant(divider.intValueExact())
                )
            }
            val x: BigDecimal = multiplier.divide(divider, RoundingMode.UNNECESSARY)
            return when (x.compareTo(BigDecimal.ONE)) {
                0 -> e
                1 -> Expressions.multiply(e, Expressions.constant(x.intValueExact()))
                -1 -> multiplyDivide(e, BigDecimal.ONE, x)
                else -> throw AssertionError()
            }
        }

        private fun mod(operand: Expression?, factor: Long): Expression? {
            return if (factor == 1L) {
                operand
            } else {
                Expressions.call(
                    BuiltInMethod.FLOOR_MOD.method,
                    operand, Expressions.constant(factor)
                )
            }
        }

        private fun getFactor(unit: TimeUnit): Long {
            return when (unit) {
                DAY -> 1L
                HOUR -> TimeUnit.DAY.multiplier.longValue()
                MINUTE -> TimeUnit.HOUR.multiplier.longValue()
                SECOND -> TimeUnit.MINUTE.multiplier.longValue()
                MILLISECOND -> TimeUnit.SECOND.multiplier.longValue()
                MONTH -> TimeUnit.YEAR.multiplier.longValue()
                QUARTER -> TimeUnit.YEAR.multiplier.longValue()
                YEAR, DECADE, CENTURY, MILLENNIUM -> 1L
                else -> throw Util.unexpected(unit)
            }
        }
    }
}
