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
package org.apache.calcite.sql.`fun`

import org.apache.calcite.avatica.util.TimeUnit

/**
 * Implementation of [org.apache.calcite.sql.SqlOperatorTable] containing
 * the standard operators and functions.
 */
object SqlStdOperatorTable : ReflectiveSqlOperatorTable() {
    //~ Static fields/initializers ---------------------------------------------
    /**
     * The standard operator table.
     */
    @MonotonicNonNull
    private var instance: SqlStdOperatorTable? = null

    //-------------------------------------------------------------
    //                   SET OPERATORS
    //-------------------------------------------------------------
    // The set operators can be compared to the arithmetic operators
    // UNION -> +
    // EXCEPT -> -
    // INTERSECT -> *
    // which explains the different precedence values
    val UNION: SqlSetOperator = SqlSetOperator("UNION", SqlKind.UNION, 12, false)
    val UNION_ALL: SqlSetOperator = SqlSetOperator("UNION ALL", SqlKind.UNION, 12, true)
    val EXCEPT: SqlSetOperator = SqlSetOperator("EXCEPT", SqlKind.EXCEPT, 12, false)
    val EXCEPT_ALL: SqlSetOperator = SqlSetOperator("EXCEPT ALL", SqlKind.EXCEPT, 12, true)
    val INTERSECT: SqlSetOperator = SqlSetOperator("INTERSECT", SqlKind.INTERSECT, 14, false)
    val INTERSECT_ALL: SqlSetOperator = SqlSetOperator("INTERSECT ALL", SqlKind.INTERSECT, 14, true)

    /**
     * The `MULTISET UNION DISTINCT` operator.
     */
    val MULTISET_UNION_DISTINCT: SqlMultisetSetOperator = SqlMultisetSetOperator("MULTISET UNION DISTINCT", 12, false)

    /**
     * The `MULTISET UNION [ALL]` operator.
     */
    val MULTISET_UNION: SqlMultisetSetOperator = SqlMultisetSetOperator("MULTISET UNION ALL", 12, true)

    /**
     * The `MULTISET EXCEPT DISTINCT` operator.
     */
    val MULTISET_EXCEPT_DISTINCT: SqlMultisetSetOperator = SqlMultisetSetOperator("MULTISET EXCEPT DISTINCT", 12, false)

    /**
     * The `MULTISET EXCEPT [ALL]` operator.
     */
    val MULTISET_EXCEPT: SqlMultisetSetOperator = SqlMultisetSetOperator("MULTISET EXCEPT ALL", 12, true)

    /**
     * The `MULTISET INTERSECT DISTINCT` operator.
     */
    val MULTISET_INTERSECT_DISTINCT: SqlMultisetSetOperator =
        SqlMultisetSetOperator("MULTISET INTERSECT DISTINCT", 14, false)

    /**
     * The `MULTISET INTERSECT [ALL]` operator.
     */
    val MULTISET_INTERSECT: SqlMultisetSetOperator = SqlMultisetSetOperator("MULTISET INTERSECT ALL", 14, true)
    //-------------------------------------------------------------
    //                   BINARY OPERATORS
    //-------------------------------------------------------------
    /**
     * Logical `AND` operator.
     */
    val AND: SqlBinaryOperator = SqlBinaryOperator(
        "AND",
        SqlKind.AND,
        24,
        true,
        ReturnTypes.BOOLEAN_NULLABLE_OPTIMIZED,
        InferTypes.BOOLEAN,
        OperandTypes.BOOLEAN_BOOLEAN
    )

    /**
     * `AS` operator associates an expression in the SELECT clause
     * with an alias.
     */
    val AS: SqlAsOperator = SqlAsOperator()

    /**
     * `ARGUMENT_ASSIGNMENT` operator (`=<`)
     * assigns an argument to a function call to a particular named parameter.
     */
    val ARGUMENT_ASSIGNMENT: SqlSpecialOperator = SqlArgumentAssignmentOperator()

    /**
     * `DEFAULT` operator indicates that an argument to a function call
     * is to take its default value..
     */
    val DEFAULT: SqlSpecialOperator = SqlDefaultOperator()

    /** `FILTER` operator filters which rows are included in an
     * aggregate function.  */
    val FILTER: SqlFilterOperator = SqlFilterOperator()

    /** `WITHIN_GROUP` operator performs aggregations on ordered data input.  */
    val WITHIN_GROUP: SqlWithinGroupOperator = SqlWithinGroupOperator()

    /** `WITHIN_DISTINCT` operator performs aggregations on distinct
     * data input.  */
    val WITHIN_DISTINCT: SqlWithinDistinctOperator = SqlWithinDistinctOperator()

    /** `CUBE` operator, occurs within `GROUP BY` clause
     * or nested within a `GROUPING SETS`.  */
    val CUBE: SqlInternalOperator = SqlRollupOperator("CUBE", SqlKind.CUBE)

    /** `ROLLUP` operator, occurs within `GROUP BY` clause
     * or nested within a `GROUPING SETS`.  */
    val ROLLUP: SqlInternalOperator = SqlRollupOperator("ROLLUP", SqlKind.ROLLUP)

    /** `GROUPING SETS` operator, occurs within `GROUP BY` clause
     * or nested within a `GROUPING SETS`.  */
    val GROUPING_SETS: SqlInternalOperator = SqlRollupOperator("GROUPING SETS", SqlKind.GROUPING_SETS)

    /** `GROUPING(c1 [, c2, ...])` function.
     *
     *
     * Occurs in similar places to an aggregate
     * function (`SELECT`, `HAVING` clause, etc. of an aggregate
     * query), but not technically an aggregate function.  */
    val GROUPING: SqlAggFunction = SqlGroupingFunction("GROUPING")

    /** `GROUP_ID()` function. (Oracle-specific.)  */
    val GROUP_ID: SqlAggFunction = SqlGroupIdFunction()

    /** `GROUPING_ID` function is a synonym for `GROUPING`.
     *
     *
     * Some history. The `GROUPING` function is in the SQL standard,
     * and originally supported only one argument. `GROUPING_ID` is not
     * standard (though supported in Oracle and SQL Server) and supports one or
     * more arguments.
     *
     *
     * The SQL standard has changed to allow `GROUPING` to have multiple
     * arguments. It is now equivalent to `GROUPING_ID`, so we made
     * `GROUPING_ID` a synonym for `GROUPING`.  */
    val GROUPING_ID: SqlAggFunction = SqlGroupingFunction("GROUPING_ID")

    /** `EXTEND` operator.  */
    val EXTEND: SqlInternalOperator = SqlExtendOperator()

    /**
     * String concatenation operator, '`||`'.
     *
     * @see SqlLibraryOperators.CONCAT_FUNCTION
     */
    val CONCAT: SqlBinaryOperator = SqlBinaryOperator(
        "||",
        SqlKind.OTHER,
        60,
        true,
        ReturnTypes.DYADIC_STRING_SUM_PRECISION_NULLABLE,
        null,
        OperandTypes.STRING_SAME_SAME
    )

    /**
     * Arithmetic division operator, '`/`'.
     */
    val DIVIDE: SqlBinaryOperator = SqlBinaryOperator(
        "/",
        SqlKind.DIVIDE,
        60,
        true,
        ReturnTypes.QUOTIENT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.DIVISION_OPERATOR
    )

    /**
     * Arithmetic remainder operator, '`%`',
     * an alternative to [.MOD] allowed if under certain conformance levels.
     *
     * @see SqlConformance.isPercentRemainderAllowed
     */
    val PERCENT_REMAINDER: SqlBinaryOperator = SqlBinaryOperator(
        "%",
        SqlKind.MOD,
        60,
        true,
        ReturnTypes.ARG1_NULLABLE,
        null,
        OperandTypes.EXACT_NUMERIC_EXACT_NUMERIC
    )

    /** The `RAND_INTEGER([seed, ] bound)` function, which yields a random
     * integer, optionally with seed.  */
    val RAND_INTEGER: SqlRandIntegerFunction = SqlRandIntegerFunction()

    /** The `RAND([seed])` function, which yields a random double,
     * optionally with seed.  */
    val RAND: SqlRandFunction = SqlRandFunction()

    /**
     * Internal integer arithmetic division operator, '`/INT`'. This
     * is only used to adjust scale for numerics. We distinguish it from
     * user-requested division since some personalities want a floating-point
     * computation, whereas for the internal scaling use of division, we always
     * want integer division.
     */
    val DIVIDE_INTEGER: SqlBinaryOperator = SqlBinaryOperator(
        "/INT",
        SqlKind.DIVIDE,
        60,
        true,
        ReturnTypes.INTEGER_QUOTIENT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.DIVISION_OPERATOR
    )

    /**
     * Dot operator, '`.`', used for referencing fields of records.
     */
    val DOT: SqlOperator = SqlDotOperator()

    /**
     * Logical equals operator, '`=`'.
     */
    val EQUALS: SqlBinaryOperator = SqlBinaryOperator(
        "=",
        SqlKind.EQUALS,
        30,
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED
    )

    /**
     * Logical greater-than operator, '`>`'.
     */
    val GREATER_THAN: SqlBinaryOperator = SqlBinaryOperator(
        ">",
        SqlKind.GREATER_THAN,
        30,
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED
    )

    /**
     * `IS DISTINCT FROM` operator.
     */
    val IS_DISTINCT_FROM: SqlBinaryOperator = SqlBinaryOperator(
        "IS DISTINCT FROM",
        SqlKind.IS_DISTINCT_FROM,
        30,
        true,
        ReturnTypes.BOOLEAN,
        InferTypes.FIRST_KNOWN,
        OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED
    )

    /**
     * `IS NOT DISTINCT FROM` operator. Is equivalent to `NOT(x
     * IS DISTINCT FROM y)`
     */
    val IS_NOT_DISTINCT_FROM: SqlBinaryOperator = SqlBinaryOperator(
        "IS NOT DISTINCT FROM",
        SqlKind.IS_NOT_DISTINCT_FROM,
        30,
        true,
        ReturnTypes.BOOLEAN,
        InferTypes.FIRST_KNOWN,
        OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED
    )

    /**
     * The internal `$IS_DIFFERENT_FROM` operator is the same as the
     * user-level [.IS_DISTINCT_FROM] in all respects except that
     * the test for equality on character datatypes treats trailing spaces as
     * significant.
     */
    val IS_DIFFERENT_FROM: SqlBinaryOperator = SqlBinaryOperator(
        "\$IS_DIFFERENT_FROM",
        SqlKind.OTHER,
        30,
        true,
        ReturnTypes.BOOLEAN,
        InferTypes.FIRST_KNOWN,
        OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED
    )

    /**
     * Logical greater-than-or-equal operator, '`>=`'.
     */
    val GREATER_THAN_OR_EQUAL: SqlBinaryOperator = SqlBinaryOperator(
        ">=",
        SqlKind.GREATER_THAN_OR_EQUAL,
        30,
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED
    )

    /**
     * `IN` operator tests for a value's membership in a sub-query or
     * a list of values.
     */
    val IN: SqlBinaryOperator = SqlInOperator(SqlKind.IN)

    /**
     * `NOT IN` operator tests for a value's membership in a sub-query
     * or a list of values.
     */
    val NOT_IN: SqlBinaryOperator = SqlInOperator(SqlKind.NOT_IN)

    /** Operator that tests whether its left operand is included in the range of
     * values covered by search arguments.  */
    val SEARCH: SqlInternalOperator = SqlSearchOperator()

    /**
     * The `< SOME` operator (synonymous with
     * `< ANY`).
     */
    val SOME_LT: SqlQuantifyOperator = SqlQuantifyOperator(SqlKind.SOME, SqlKind.LESS_THAN)
    val SOME_LE: SqlQuantifyOperator = SqlQuantifyOperator(SqlKind.SOME, SqlKind.LESS_THAN_OR_EQUAL)
    val SOME_GT: SqlQuantifyOperator = SqlQuantifyOperator(SqlKind.SOME, SqlKind.GREATER_THAN)
    val SOME_GE: SqlQuantifyOperator = SqlQuantifyOperator(SqlKind.SOME, SqlKind.GREATER_THAN_OR_EQUAL)
    val SOME_EQ: SqlQuantifyOperator = SqlQuantifyOperator(SqlKind.SOME, SqlKind.EQUALS)
    val SOME_NE: SqlQuantifyOperator = SqlQuantifyOperator(SqlKind.SOME, SqlKind.NOT_EQUALS)

    /**
     * The `< ALL` operator.
     */
    val ALL_LT: SqlQuantifyOperator = SqlQuantifyOperator(SqlKind.ALL, SqlKind.LESS_THAN)
    val ALL_LE: SqlQuantifyOperator = SqlQuantifyOperator(SqlKind.ALL, SqlKind.LESS_THAN_OR_EQUAL)
    val ALL_GT: SqlQuantifyOperator = SqlQuantifyOperator(SqlKind.ALL, SqlKind.GREATER_THAN)
    val ALL_GE: SqlQuantifyOperator = SqlQuantifyOperator(SqlKind.ALL, SqlKind.GREATER_THAN_OR_EQUAL)
    val ALL_EQ: SqlQuantifyOperator = SqlQuantifyOperator(SqlKind.ALL, SqlKind.EQUALS)
    val ALL_NE: SqlQuantifyOperator = SqlQuantifyOperator(SqlKind.ALL, SqlKind.NOT_EQUALS)

    /**
     * Logical less-than operator, '`<`'.
     */
    val LESS_THAN: SqlBinaryOperator = SqlBinaryOperator(
        "<",
        SqlKind.LESS_THAN,
        30,
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED
    )

    /**
     * Logical less-than-or-equal operator, '`<=`'.
     */
    val LESS_THAN_OR_EQUAL: SqlBinaryOperator = SqlBinaryOperator(
        "<=",
        SqlKind.LESS_THAN_OR_EQUAL,
        30,
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.COMPARABLE_ORDERED_COMPARABLE_ORDERED
    )

    /**
     * Infix arithmetic minus operator, '`-`'.
     *
     *
     * Its precedence is less than the prefix [+][.UNARY_PLUS]
     * and [-][.UNARY_MINUS] operators.
     */
    val MINUS: SqlBinaryOperator = SqlMonotonicBinaryOperator(
        "-",
        SqlKind.MINUS,
        40,
        true,  // Same type inference strategy as sum
        ReturnTypes.NULLABLE_SUM,
        InferTypes.FIRST_KNOWN,
        OperandTypes.MINUS_OPERATOR
    )

    /**
     * Arithmetic multiplication operator, '`*`'.
     */
    val MULTIPLY: SqlBinaryOperator = SqlMonotonicBinaryOperator(
        "*",
        SqlKind.TIMES,
        60,
        true,
        ReturnTypes.PRODUCT_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.MULTIPLY_OPERATOR
    )

    /**
     * Logical not-equals operator, '`<>`'.
     */
    val NOT_EQUALS: SqlBinaryOperator = SqlBinaryOperator(
        "<>",
        SqlKind.NOT_EQUALS,
        30,
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED
    )

    /**
     * Logical `OR` operator.
     */
    val OR: SqlBinaryOperator = SqlBinaryOperator(
        "OR",
        SqlKind.OR,
        22,
        true,
        ReturnTypes.BOOLEAN_NULLABLE_OPTIMIZED,
        InferTypes.BOOLEAN,
        OperandTypes.BOOLEAN_BOOLEAN
    )

    /**
     * Infix arithmetic plus operator, '`+`'.
     */
    val PLUS: SqlBinaryOperator = SqlMonotonicBinaryOperator(
        "+",
        SqlKind.PLUS,
        40,
        true,
        ReturnTypes.NULLABLE_SUM,
        InferTypes.FIRST_KNOWN,
        OperandTypes.PLUS_OPERATOR
    )

    /**
     * Infix datetime plus operator, '`DATETIME + INTERVAL`'.
     */
    val DATETIME_PLUS: SqlSpecialOperator = SqlDatetimePlusOperator()

    /**
     * Interval expression, '`INTERVAL n timeUnit`'.
     */
    val INTERVAL: SqlSpecialOperator = SqlIntervalOperator()

    /**
     * Multiset `MEMBER OF`, which returns whether a element belongs to a
     * multiset.
     *
     *
     * For example, the following returns `false`:
     *
     * <blockquote>
     * `'green' MEMBER OF MULTISET ['red','almost green','blue']`
    </blockquote> *
     */
    val MEMBER_OF: SqlBinaryOperator = SqlMultisetMemberOfOperator()

    /**
     * Submultiset. Checks to see if an multiset is a sub-set of another
     * multiset.
     *
     *
     * For example, the following returns `false`:
     *
     * <blockquote>
     * `MULTISET ['green'] SUBMULTISET OF
     * MULTISET['red', 'almost green', 'blue']`
    </blockquote> *
     *
     *
     * The following returns `true`, in part because multisets are
     * order-independent:
     *
     * <blockquote>
     * `MULTISET ['blue', 'red'] SUBMULTISET OF
     * MULTISET ['red', 'almost green', 'blue']`
    </blockquote> *
     */
    val SUBMULTISET_OF: SqlBinaryOperator =  // TODO: check if precedence is correct
        SqlBinaryOperator(
            "SUBMULTISET OF",
            SqlKind.OTHER,
            30,
            true,
            ReturnTypes.BOOLEAN_NULLABLE,
            null,
            OperandTypes.MULTISET_MULTISET
        )
    val NOT_SUBMULTISET_OF: SqlBinaryOperator =  // TODO: check if precedence is correct
        SqlBinaryOperator(
            "NOT SUBMULTISET OF",
            SqlKind.OTHER,
            30,
            true,
            ReturnTypes.BOOLEAN_NULLABLE,
            null,
            OperandTypes.MULTISET_MULTISET
        )

    //-------------------------------------------------------------
    //                   POSTFIX OPERATORS
    //-------------------------------------------------------------
    val DESC: SqlPostfixOperator = SqlPostfixOperator(
        "DESC",
        SqlKind.DESCENDING,
        20,
        ReturnTypes.ARG0,
        InferTypes.RETURN_TYPE,
        OperandTypes.ANY
    )
    val NULLS_FIRST: SqlPostfixOperator = SqlPostfixOperator(
        "NULLS FIRST",
        SqlKind.NULLS_FIRST,
        18,
        ReturnTypes.ARG0,
        InferTypes.RETURN_TYPE,
        OperandTypes.ANY
    )
    val NULLS_LAST: SqlPostfixOperator = SqlPostfixOperator(
        "NULLS LAST",
        SqlKind.NULLS_LAST,
        18,
        ReturnTypes.ARG0,
        InferTypes.RETURN_TYPE,
        OperandTypes.ANY
    )
    val IS_NOT_NULL: SqlPostfixOperator = SqlPostfixOperator(
        "IS NOT NULL",
        SqlKind.IS_NOT_NULL,
        28,
        ReturnTypes.BOOLEAN_NOT_NULL,
        InferTypes.VARCHAR_1024,
        OperandTypes.ANY
    )
    val IS_NULL: SqlPostfixOperator = SqlPostfixOperator(
        "IS NULL",
        SqlKind.IS_NULL,
        28,
        ReturnTypes.BOOLEAN_NOT_NULL,
        InferTypes.VARCHAR_1024,
        OperandTypes.ANY
    )
    val IS_NOT_TRUE: SqlPostfixOperator = SqlPostfixOperator(
        "IS NOT TRUE",
        SqlKind.IS_NOT_TRUE,
        28,
        ReturnTypes.BOOLEAN_NOT_NULL,
        InferTypes.BOOLEAN,
        OperandTypes.BOOLEAN
    )
    val IS_TRUE: SqlPostfixOperator = SqlPostfixOperator(
        "IS TRUE",
        SqlKind.IS_TRUE,
        28,
        ReturnTypes.BOOLEAN_NOT_NULL,
        InferTypes.BOOLEAN,
        OperandTypes.BOOLEAN
    )
    val IS_NOT_FALSE: SqlPostfixOperator = SqlPostfixOperator(
        "IS NOT FALSE",
        SqlKind.IS_NOT_FALSE,
        28,
        ReturnTypes.BOOLEAN_NOT_NULL,
        InferTypes.BOOLEAN,
        OperandTypes.BOOLEAN
    )
    val IS_FALSE: SqlPostfixOperator = SqlPostfixOperator(
        "IS FALSE",
        SqlKind.IS_FALSE,
        28,
        ReturnTypes.BOOLEAN_NOT_NULL,
        InferTypes.BOOLEAN,
        OperandTypes.BOOLEAN
    )
    val IS_NOT_UNKNOWN: SqlPostfixOperator = SqlPostfixOperator(
        "IS NOT UNKNOWN",
        SqlKind.IS_NOT_NULL,
        28,
        ReturnTypes.BOOLEAN_NOT_NULL,
        InferTypes.BOOLEAN,
        OperandTypes.BOOLEAN
    )
    val IS_UNKNOWN: SqlPostfixOperator = SqlPostfixOperator(
        "IS UNKNOWN",
        SqlKind.IS_NULL,
        28,
        ReturnTypes.BOOLEAN_NOT_NULL,
        InferTypes.BOOLEAN,
        OperandTypes.BOOLEAN
    )
    val IS_A_SET: SqlPostfixOperator = SqlPostfixOperator(
        "IS A SET",
        SqlKind.OTHER,
        28,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.MULTISET
    )
    val IS_NOT_A_SET: SqlPostfixOperator = SqlPostfixOperator(
        "IS NOT A SET",
        SqlKind.OTHER,
        28,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.MULTISET
    )
    val IS_EMPTY: SqlPostfixOperator = SqlPostfixOperator(
        "IS EMPTY",
        SqlKind.OTHER,
        28,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.COLLECTION_OR_MAP
    )
    val IS_NOT_EMPTY: SqlPostfixOperator = SqlPostfixOperator(
        "IS NOT EMPTY",
        SqlKind.OTHER,
        28,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.COLLECTION_OR_MAP
    )
    val IS_JSON_VALUE: SqlPostfixOperator = SqlPostfixOperator(
        "IS JSON VALUE",
        SqlKind.OTHER,
        28,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.CHARACTER
    )
    val IS_NOT_JSON_VALUE: SqlPostfixOperator = SqlPostfixOperator(
        "IS NOT JSON VALUE",
        SqlKind.OTHER,
        28,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.CHARACTER
    )
    val IS_JSON_OBJECT: SqlPostfixOperator = SqlPostfixOperator(
        "IS JSON OBJECT",
        SqlKind.OTHER,
        28,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.CHARACTER
    )
    val IS_NOT_JSON_OBJECT: SqlPostfixOperator = SqlPostfixOperator(
        "IS NOT JSON OBJECT",
        SqlKind.OTHER,
        28,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.CHARACTER
    )
    val IS_JSON_ARRAY: SqlPostfixOperator = SqlPostfixOperator(
        "IS JSON ARRAY",
        SqlKind.OTHER,
        28,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.CHARACTER
    )
    val IS_NOT_JSON_ARRAY: SqlPostfixOperator = SqlPostfixOperator(
        "IS NOT JSON ARRAY",
        SqlKind.OTHER,
        28,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.CHARACTER
    )
    val IS_JSON_SCALAR: SqlPostfixOperator = SqlPostfixOperator(
        "IS JSON SCALAR",
        SqlKind.OTHER,
        28,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.CHARACTER
    )
    val IS_NOT_JSON_SCALAR: SqlPostfixOperator = SqlPostfixOperator(
        "IS NOT JSON SCALAR",
        SqlKind.OTHER,
        28,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.CHARACTER
    )
    val JSON_VALUE_EXPRESSION: SqlPostfixOperator = SqlJsonValueExpressionOperator()

    //-------------------------------------------------------------
    //                   PREFIX OPERATORS
    //-------------------------------------------------------------
    val EXISTS: SqlPrefixOperator = object : SqlPrefixOperator(
        "EXISTS",
        SqlKind.this,
        40,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY
    ) {
        @Override
        fun argumentMustBeScalar(ordinal: Int): Boolean {
            return false
        }

        @Override
        fun validRexOperands(count: Int, litmus: Litmus): Boolean {
            return if (count != 0) {
                litmus.fail("wrong operand count {} for {}", count, this)
            } else litmus.succeed()
        }
    }
    val UNIQUE: SqlPrefixOperator = object : SqlPrefixOperator(
        "UNIQUE",
        SqlKind.this,
        40,
        ReturnTypes.BOOLEAN,
        null,
        OperandTypes.ANY
    ) {
        @Override
        fun argumentMustBeScalar(ordinal: Int): Boolean {
            return false
        }

        @Override
        fun validRexOperands(count: Int, litmus: Litmus): Boolean {
            return if (count != 0) {
                litmus.fail("wrong operand count {} for {}", count, this)
            } else litmus.succeed()
        }
    }
    val NOT: SqlPrefixOperator = SqlPrefixOperator(
        "NOT",
        SqlKind.NOT,
        26,
        ReturnTypes.ARG0,
        InferTypes.BOOLEAN,
        OperandTypes.BOOLEAN
    )

    /**
     * Prefix arithmetic minus operator, '`-`'.
     *
     *
     * Its precedence is greater than the infix '[+][.PLUS]' and
     * '[-][.MINUS]' operators.
     */
    val UNARY_MINUS: SqlPrefixOperator = SqlPrefixOperator(
        "-",
        SqlKind.MINUS_PREFIX,
        80,
        ReturnTypes.ARG0,
        InferTypes.RETURN_TYPE,
        OperandTypes.NUMERIC_OR_INTERVAL
    )

    /**
     * Prefix arithmetic plus operator, '`+`'.
     *
     *
     * Its precedence is greater than the infix '[+][.PLUS]' and
     * '[-][.MINUS]' operators.
     */
    val UNARY_PLUS: SqlPrefixOperator = SqlPrefixOperator(
        "+",
        SqlKind.PLUS_PREFIX,
        80,
        ReturnTypes.ARG0,
        InferTypes.RETURN_TYPE,
        OperandTypes.NUMERIC_OR_INTERVAL
    )

    /**
     * Keyword which allows an identifier to be explicitly flagged as a table.
     * For example, `select * from (TABLE t)` or `TABLE
     * t`. See also [.COLLECTION_TABLE].
     */
    val EXPLICIT_TABLE: SqlPrefixOperator = SqlPrefixOperator(
        "TABLE",
        SqlKind.EXPLICIT_TABLE,
        2,
        null,
        null,
        null
    )

    /** `FINAL` function to be used within `MATCH_RECOGNIZE`.  */
    val FINAL: SqlPrefixOperator = SqlPrefixOperator(
        "FINAL",
        SqlKind.FINAL,
        80,
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.ANY
    )

    /** `RUNNING` function to be used within `MATCH_RECOGNIZE`.  */
    val RUNNING: SqlPrefixOperator = SqlPrefixOperator(
        "RUNNING",
        SqlKind.RUNNING,
        80,
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.ANY
    )
    //-------------------------------------------------------------
    // AGGREGATE OPERATORS
    //-------------------------------------------------------------
    /**
     * `SUM` aggregate function.
     */
    val SUM: SqlAggFunction = SqlSumAggFunction(castNonNull(null))

    /**
     * `COUNT` aggregate function.
     */
    val COUNT: SqlAggFunction = SqlCountAggFunction("COUNT")

    /**
     * `MODE` aggregate function.
     */
    val MODE: SqlAggFunction = SqlBasicAggFunction
        .create(
            "MODE", SqlKind.MODE, ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
            OperandTypes.ANY
        )
        .withGroupOrder(Optionality.FORBIDDEN)
        .withFunctionType(SqlFunctionCategory.SYSTEM)

    /**
     * `APPROX_COUNT_DISTINCT` aggregate function.
     */
    val APPROX_COUNT_DISTINCT: SqlAggFunction = SqlCountAggFunction("APPROX_COUNT_DISTINCT")

    /**
     * `MIN` aggregate function.
     */
    val MIN: SqlAggFunction = SqlMinMaxAggFunction(SqlKind.MIN)

    /**
     * `MAX` aggregate function.
     */
    val MAX: SqlAggFunction = SqlMinMaxAggFunction(SqlKind.MAX)

    /**
     * `EVERY` aggregate function.
     */
    val EVERY: SqlAggFunction = SqlMinMaxAggFunction("EVERY", SqlKind.MIN, OperandTypes.BOOLEAN)

    /**
     * `SOME` aggregate function.
     */
    val SOME: SqlAggFunction = SqlMinMaxAggFunction("SOME", SqlKind.MAX, OperandTypes.BOOLEAN)

    /**
     * `LAST_VALUE` aggregate function.
     */
    val LAST_VALUE: SqlAggFunction = SqlFirstLastValueAggFunction(SqlKind.LAST_VALUE)

    /**
     * `ANY_VALUE` aggregate function.
     */
    val ANY_VALUE: SqlAggFunction = SqlAnyValueAggFunction(SqlKind.ANY_VALUE)

    /**
     * `FIRST_VALUE` aggregate function.
     */
    val FIRST_VALUE: SqlAggFunction = SqlFirstLastValueAggFunction(SqlKind.FIRST_VALUE)

    /**
     * `NTH_VALUE` aggregate function.
     */
    val NTH_VALUE: SqlAggFunction = SqlNthValueAggFunction(SqlKind.NTH_VALUE)

    /**
     * `LEAD` aggregate function.
     */
    val LEAD: SqlAggFunction = SqlLeadLagAggFunction(SqlKind.LEAD)

    /**
     * `LAG` aggregate function.
     */
    val LAG: SqlAggFunction = SqlLeadLagAggFunction(SqlKind.LAG)

    /**
     * `NTILE` aggregate function.
     */
    val NTILE: SqlAggFunction = SqlNtileAggFunction()

    /**
     * `SINGLE_VALUE` aggregate function.
     */
    val SINGLE_VALUE: SqlAggFunction = SqlSingleValueAggFunction(castNonNull(null))

    /**
     * `AVG` aggregate function.
     */
    val AVG: SqlAggFunction = SqlAvgAggFunction(SqlKind.AVG)

    /**
     * `STDDEV_POP` aggregate function.
     */
    val STDDEV_POP: SqlAggFunction = SqlAvgAggFunction(SqlKind.STDDEV_POP)

    /**
     * `REGR_COUNT` aggregate function.
     */
    val REGR_COUNT: SqlAggFunction = SqlRegrCountAggFunction(SqlKind.REGR_COUNT)

    /**
     * `REGR_SXX` aggregate function.
     */
    val REGR_SXX: SqlAggFunction = SqlCovarAggFunction(SqlKind.REGR_SXX)

    /**
     * `REGR_SYY` aggregate function.
     */
    val REGR_SYY: SqlAggFunction = SqlCovarAggFunction(SqlKind.REGR_SYY)

    /**
     * `COVAR_POP` aggregate function.
     */
    val COVAR_POP: SqlAggFunction = SqlCovarAggFunction(SqlKind.COVAR_POP)

    /**
     * `COVAR_SAMP` aggregate function.
     */
    val COVAR_SAMP: SqlAggFunction = SqlCovarAggFunction(SqlKind.COVAR_SAMP)

    /**
     * `STDDEV_SAMP` aggregate function.
     */
    val STDDEV_SAMP: SqlAggFunction = SqlAvgAggFunction(SqlKind.STDDEV_SAMP)

    /**
     * `STDDEV` aggregate function.
     */
    val STDDEV: SqlAggFunction = SqlAvgAggFunction("STDDEV", SqlKind.STDDEV_SAMP)

    /**
     * `VAR_POP` aggregate function.
     */
    val VAR_POP: SqlAggFunction = SqlAvgAggFunction(SqlKind.VAR_POP)

    /**
     * `VAR_SAMP` aggregate function.
     */
    val VAR_SAMP: SqlAggFunction = SqlAvgAggFunction(SqlKind.VAR_SAMP)

    /**
     * `VARIANCE` aggregate function.
     */
    val VARIANCE: SqlAggFunction = SqlAvgAggFunction("VARIANCE", SqlKind.VAR_SAMP)

    /**
     * `BIT_AND` aggregate function.
     */
    val BIT_AND: SqlAggFunction = SqlBitOpAggFunction(SqlKind.BIT_AND)

    /**
     * `BIT_OR` aggregate function.
     */
    val BIT_OR: SqlAggFunction = SqlBitOpAggFunction(SqlKind.BIT_OR)

    /**
     * `BIT_XOR` aggregate function.
     */
    val BIT_XOR: SqlAggFunction = SqlBitOpAggFunction(SqlKind.BIT_XOR)
    //-------------------------------------------------------------
    // WINDOW Aggregate Functions
    //-------------------------------------------------------------
    /**
     * `HISTOGRAM` aggregate function support. Used by window
     * aggregate versions of MIN/MAX
     */
    val HISTOGRAM_AGG: SqlAggFunction = SqlHistogramAggFunction(castNonNull(null))

    /**
     * `HISTOGRAM_MIN` window aggregate function.
     */
    val HISTOGRAM_MIN: SqlFunction = SqlFunction(
        "\$HISTOGRAM_MIN",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.NUMERIC_OR_STRING,
        SqlFunctionCategory.NUMERIC
    )

    /**
     * `HISTOGRAM_MAX` window aggregate function.
     */
    val HISTOGRAM_MAX: SqlFunction = SqlFunction(
        "\$HISTOGRAM_MAX",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.NUMERIC_OR_STRING,
        SqlFunctionCategory.NUMERIC
    )

    /**
     * `HISTOGRAM_FIRST_VALUE` window aggregate function.
     */
    val HISTOGRAM_FIRST_VALUE: SqlFunction = SqlFunction(
        "\$HISTOGRAM_FIRST_VALUE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.NUMERIC_OR_STRING,
        SqlFunctionCategory.NUMERIC
    )

    /**
     * `HISTOGRAM_LAST_VALUE` window aggregate function.
     */
    val HISTOGRAM_LAST_VALUE: SqlFunction = SqlFunction(
        "\$HISTOGRAM_LAST_VALUE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.NUMERIC_OR_STRING,
        SqlFunctionCategory.NUMERIC
    )

    /**
     * `SUM0` aggregate function.
     */
    val SUM0: SqlAggFunction = SqlSumEmptyIsZeroAggFunction()
    //-------------------------------------------------------------
    // WINDOW Rank Functions
    //-------------------------------------------------------------
    /**
     * `CUME_DIST` window function.
     */
    val CUME_DIST: SqlRankFunction = SqlRankFunction(SqlKind.CUME_DIST, ReturnTypes.FRACTIONAL_RANK, true)

    /**
     * `DENSE_RANK` window function.
     */
    val DENSE_RANK: SqlRankFunction = SqlRankFunction(SqlKind.DENSE_RANK, ReturnTypes.RANK, true)

    /**
     * `PERCENT_RANK` window function.
     */
    val PERCENT_RANK: SqlRankFunction = SqlRankFunction(
        SqlKind.PERCENT_RANK,
        ReturnTypes.FRACTIONAL_RANK,
        true
    )

    /**
     * `RANK` window function.
     */
    val RANK: SqlRankFunction = SqlRankFunction(SqlKind.RANK, ReturnTypes.RANK, true)

    /**
     * `ROW_NUMBER` window function.
     */
    val ROW_NUMBER: SqlRankFunction = SqlRankFunction(SqlKind.ROW_NUMBER, ReturnTypes.RANK, false)

    //-------------------------------------------------------------
    //                   SPECIAL OPERATORS
    //-------------------------------------------------------------
    val ROW: SqlRowOperator = SqlRowOperator("ROW")

    /** `IGNORE NULLS` operator.  */
    val IGNORE_NULLS: SqlNullTreatmentOperator = SqlNullTreatmentOperator(SqlKind.IGNORE_NULLS)

    /** `RESPECT NULLS` operator.  */
    val RESPECT_NULLS: SqlNullTreatmentOperator = SqlNullTreatmentOperator(SqlKind.RESPECT_NULLS)

    /**
     * A special operator for the subtraction of two DATETIMEs. The format of
     * DATETIME subtraction is:
     *
     * <blockquote>`"(" <datetime> "-" <datetime> ")"
     * <interval qualifier>`</blockquote>
     *
     *
     * This operator is special since it needs to hold the
     * additional interval qualifier specification.
     */
    val MINUS_DATE: SqlDatetimeSubtractionOperator = SqlDatetimeSubtractionOperator()

    /**
     * The MULTISET Value Constructor. e.g. "`MULTISET[1,2,3]`".
     */
    val MULTISET_VALUE: SqlMultisetValueConstructor = SqlMultisetValueConstructor()

    /**
     * The MULTISET Query Constructor. e.g. "`SELECT dname, MULTISET(SELECT
     * FROM emp WHERE deptno = dept.deptno) FROM dept`".
     */
    val MULTISET_QUERY: SqlMultisetQueryConstructor = SqlMultisetQueryConstructor()

    /**
     * The ARRAY Query Constructor. e.g. "`SELECT dname, ARRAY(SELECT
     * FROM emp WHERE deptno = dept.deptno) FROM dept`".
     */
    val ARRAY_QUERY: SqlMultisetQueryConstructor = SqlArrayQueryConstructor()

    /**
     * The MAP Query Constructor. e.g. "`MAP(SELECT empno, deptno
     * FROM emp)`".
     */
    val MAP_QUERY: SqlMultisetQueryConstructor = SqlMapQueryConstructor()

    /**
     * The CURSOR constructor. e.g. "`SELECT * FROM
     * TABLE(DEDUP(CURSOR(SELECT * FROM EMPS), 'name'))`".
     */
    val CURSOR: SqlCursorConstructor = SqlCursorConstructor()

    /**
     * The COLUMN_LIST constructor. e.g. the ROW() call in "`SELECT * FROM
     * TABLE(DEDUP(CURSOR(SELECT * FROM EMPS), ROW(name, empno)))`".
     */
    val COLUMN_LIST: SqlColumnListConstructor = SqlColumnListConstructor()

    /**
     * The `UNNEST` operator.
     */
    val UNNEST: SqlUnnestOperator = SqlUnnestOperator(false)

    /**
     * The `UNNEST WITH ORDINALITY` operator.
     */
    val UNNEST_WITH_ORDINALITY: SqlUnnestOperator = SqlUnnestOperator(true)

    /**
     * The `LATERAL` operator.
     */
    val LATERAL: SqlSpecialOperator = SqlLateralOperator(SqlKind.LATERAL)

    /**
     * The "table function derived table" operator, which a table-valued
     * function into a relation, e.g. "`SELECT * FROM
     * TABLE(ramp(5))`".
     *
     *
     * This operator has function syntax (with one argument), whereas
     * [.EXPLICIT_TABLE] is a prefix operator.
     */
    val COLLECTION_TABLE: SqlSpecialOperator = SqlCollectionTableOperator("TABLE", SqlModality.RELATION)
    val OVERLAPS: SqlOverlapsOperator = SqlOverlapsOperator(SqlKind.OVERLAPS)
    val CONTAINS: SqlOverlapsOperator = SqlOverlapsOperator(SqlKind.CONTAINS)
    val PRECEDES: SqlOverlapsOperator = SqlOverlapsOperator(SqlKind.PRECEDES)
    val IMMEDIATELY_PRECEDES: SqlOverlapsOperator = SqlOverlapsOperator(SqlKind.IMMEDIATELY_PRECEDES)
    val SUCCEEDS: SqlOverlapsOperator = SqlOverlapsOperator(SqlKind.SUCCEEDS)
    val IMMEDIATELY_SUCCEEDS: SqlOverlapsOperator = SqlOverlapsOperator(SqlKind.IMMEDIATELY_SUCCEEDS)
    val PERIOD_EQUALS: SqlOverlapsOperator = SqlOverlapsOperator(SqlKind.PERIOD_EQUALS)
    val VALUES: SqlSpecialOperator = SqlValuesOperator()
    val LITERAL_CHAIN: SqlLiteralChainOperator = SqlLiteralChainOperator()
    val THROW: SqlThrowOperator = SqlThrowOperator()
    val JSON_EXISTS: SqlFunction = SqlJsonExistsFunction()
    val JSON_VALUE: SqlFunction = SqlJsonValueFunction("JSON_VALUE")
    val JSON_QUERY: SqlFunction = SqlJsonQueryFunction()
    val JSON_OBJECT: SqlFunction = SqlJsonObjectFunction()
    val JSON_OBJECTAGG: SqlJsonObjectAggAggFunction = SqlJsonObjectAggAggFunction(
        SqlKind.JSON_OBJECTAGG,
        SqlJsonConstructorNullClause.NULL_ON_NULL
    )
    val JSON_ARRAY: SqlFunction = SqlJsonArrayFunction()

    @Deprecated // to be removed before 2.0
    val JSON_TYPE: SqlFunction = SqlLibraryOperators.JSON_TYPE

    @Deprecated // to be removed before 2.0
    val JSON_DEPTH: SqlFunction = SqlLibraryOperators.JSON_DEPTH

    @Deprecated // to be removed before 2.0
    val JSON_LENGTH: SqlFunction = SqlLibraryOperators.JSON_LENGTH

    @Deprecated // to be removed before 2.0
    val JSON_KEYS: SqlFunction = SqlLibraryOperators.JSON_KEYS

    @Deprecated // to be removed before 2.0
    val JSON_PRETTY: SqlFunction = SqlLibraryOperators.JSON_PRETTY

    @Deprecated // to be removed before 2.0
    val JSON_REMOVE: SqlFunction = SqlLibraryOperators.JSON_REMOVE

    @Deprecated // to be removed before 2.0
    val JSON_STORAGE_SIZE: SqlFunction = SqlLibraryOperators.JSON_STORAGE_SIZE
    val JSON_ARRAYAGG: SqlJsonArrayAggAggFunction = SqlJsonArrayAggAggFunction(
        SqlKind.JSON_ARRAYAGG,
        SqlJsonConstructorNullClause.ABSENT_ON_NULL
    )
    val BETWEEN: SqlBetweenOperator = SqlBetweenOperator(
        SqlBetweenOperator.Flag.ASYMMETRIC,
        false
    )
    val SYMMETRIC_BETWEEN: SqlBetweenOperator = SqlBetweenOperator(
        SqlBetweenOperator.Flag.SYMMETRIC,
        false
    )
    val NOT_BETWEEN: SqlBetweenOperator = SqlBetweenOperator(
        SqlBetweenOperator.Flag.ASYMMETRIC,
        true
    )
    val SYMMETRIC_NOT_BETWEEN: SqlBetweenOperator = SqlBetweenOperator(
        SqlBetweenOperator.Flag.SYMMETRIC,
        true
    )
    val NOT_LIKE: SqlSpecialOperator = SqlLikeOperator("NOT LIKE", SqlKind.LIKE, true, true)
    val LIKE: SqlSpecialOperator = SqlLikeOperator("LIKE", SqlKind.LIKE, false, true)
    val NOT_SIMILAR_TO: SqlSpecialOperator = SqlLikeOperator("NOT SIMILAR TO", SqlKind.SIMILAR, true, true)
    val SIMILAR_TO: SqlSpecialOperator = SqlLikeOperator("SIMILAR TO", SqlKind.SIMILAR, false, true)
    val POSIX_REGEX_CASE_SENSITIVE: SqlBinaryOperator = SqlPosixRegexOperator(
        "POSIX REGEX CASE SENSITIVE",
        SqlKind.POSIX_REGEX_CASE_SENSITIVE, true, false
    )
    val POSIX_REGEX_CASE_INSENSITIVE: SqlBinaryOperator = SqlPosixRegexOperator(
        "POSIX REGEX CASE INSENSITIVE",
        SqlKind.POSIX_REGEX_CASE_INSENSITIVE, false, false
    )
    val NEGATED_POSIX_REGEX_CASE_SENSITIVE: SqlBinaryOperator = SqlPosixRegexOperator(
        "NEGATED POSIX REGEX CASE SENSITIVE",
        SqlKind.POSIX_REGEX_CASE_SENSITIVE, true, true
    )
    val NEGATED_POSIX_REGEX_CASE_INSENSITIVE: SqlBinaryOperator = SqlPosixRegexOperator(
        "NEGATED POSIX REGEX CASE INSENSITIVE",
        SqlKind.POSIX_REGEX_CASE_INSENSITIVE, false, true
    )

    /**
     * Internal operator used to represent the ESCAPE clause of a LIKE or
     * SIMILAR TO expression.
     */
    val ESCAPE: SqlSpecialOperator = SqlSpecialOperator("ESCAPE", SqlKind.ESCAPE, 0)
    val CASE: SqlCaseOperator = SqlCaseOperator.INSTANCE
    val PROCEDURE_CALL: SqlOperator = SqlProcedureCallOperator()
    val NEW: SqlOperator = SqlNewOperator()

    /**
     * The `OVER` operator, which applies an aggregate functions to a
     * [window][SqlWindow].
     *
     *
     * Operands are as follows:
     *
     *
     *  1. name of window function ([org.apache.calcite.sql.SqlCall])
     *  1. window name ([org.apache.calcite.sql.SqlLiteral]) or window
     * in-line specification (`org.apache.calcite.sql.SqlWindow.SqlWindowOperator`)
     *
     */
    val OVER: SqlBinaryOperator = SqlOverOperator()

    /**
     * An `REINTERPRET` operator is internal to the planner. When the
     * physical storage of two types is the same, this operator may be used to
     * reinterpret values of one type as the other. This operator is similar to
     * a cast, except that it does not alter the data value. Like a regular cast
     * it accepts one operand and stores the target type as the return type. It
     * performs an overflow check if it has *any* second operand, whether
     * true or not.
     */
    val REINTERPRET: SqlSpecialOperator = object : SqlSpecialOperator("Reinterpret", SqlKind.this) {
        @get:Override
        val operandCountRange: SqlOperandCountRange
            get() = SqlOperandCountRanges.between(1, 2)
    }
    //-------------------------------------------------------------
    //                   FUNCTIONS
    //-------------------------------------------------------------
    /**
     * The character substring function: `SUBSTRING(string FROM start [FOR
     * length])`.
     *
     *
     * If the length parameter is a constant, the length of the result is the
     * minimum of the length of the input and that length. Otherwise it is the
     * length of the input.
     */
    val SUBSTRING: SqlFunction = SqlSubstringFunction()

    /** The `REPLACE(string, search, replace)` function. Not standard SQL,
     * but in Oracle and Postgres.  */
    val REPLACE: SqlFunction = SqlFunction(
        "REPLACE", SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE_VARYING, null,
        OperandTypes.STRING_STRING_STRING, SqlFunctionCategory.STRING
    )
    val CONVERT: SqlFunction = SqlConvertFunction("CONVERT")

    /**
     * The `TRANSLATE(*char_value* USING *translation_name*)` function
     * alters the character set of a string value from one base character set to another.
     *
     *
     * It is defined in the SQL standard. See also the non-standard
     * [SqlLibraryOperators.TRANSLATE3], which has a different purpose.
     */
    val TRANSLATE: SqlFunction = SqlConvertFunction("TRANSLATE")
    val OVERLAY: SqlFunction = SqlOverlayFunction()

    /** The "TRIM" function.  */
    val TRIM: SqlFunction = SqlTrimFunction.INSTANCE
    val POSITION: SqlFunction = SqlPositionFunction()
    val CHAR_LENGTH: SqlFunction = SqlFunction(
        "CHAR_LENGTH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        null,
        OperandTypes.CHARACTER,
        SqlFunctionCategory.NUMERIC
    )

    /** Alias for [.CHAR_LENGTH].  */
    val CHARACTER_LENGTH: SqlFunction = SqlFunction(
        "CHARACTER_LENGTH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        null,
        OperandTypes.CHARACTER,
        SqlFunctionCategory.NUMERIC
    )
    val OCTET_LENGTH: SqlFunction = SqlFunction(
        "OCTET_LENGTH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        null,
        OperandTypes.BINARY,
        SqlFunctionCategory.NUMERIC
    )
    val UPPER: SqlFunction = SqlFunction(
        "UPPER",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.CHARACTER,
        SqlFunctionCategory.STRING
    )
    val LOWER: SqlFunction = SqlFunction(
        "LOWER",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.CHARACTER,
        SqlFunctionCategory.STRING
    )
    val INITCAP: SqlFunction = SqlFunction(
        "INITCAP",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.CHARACTER,
        SqlFunctionCategory.STRING
    )
    val ASCII: SqlFunction = SqlFunction(
        "ASCII",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        null,
        OperandTypes.CHARACTER,
        SqlFunctionCategory.STRING
    )

    /**
     * Uses SqlOperatorTable.useDouble for its return type since we don't know
     * what the result type will be by just looking at the operand types. For
     * example POW(int, int) can return a non integer if the second operand is
     * negative.
     */
    val POWER: SqlFunction = SqlFunction(
        "POWER",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.NUMERIC_NUMERIC,
        SqlFunctionCategory.NUMERIC
    )
    val SQRT: SqlFunction = SqlFunction(
        "SQRT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC
    )

    /**
     * Arithmetic remainder function `MOD`.
     *
     * @see .PERCENT_REMAINDER
     */
    val MOD: SqlFunction =  // Return type is same as divisor (2nd operand)
        // SQL2003 Part2 Section 6.27, Syntax Rules 9
        SqlFunction(
            "MOD",
            SqlKind.MOD,
            ReturnTypes.NULLABLE_MOD,
            null,
            OperandTypes.EXACT_NUMERIC_EXACT_NUMERIC,
            SqlFunctionCategory.NUMERIC
        )
    val LN: SqlFunction = SqlFunction(
        "LN",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC
    )
    val LOG10: SqlFunction = SqlFunction(
        "LOG10",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC
    )
    val ABS: SqlFunction = SqlFunction(
        "ABS",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.NUMERIC_OR_INTERVAL,
        SqlFunctionCategory.NUMERIC
    )
    val ACOS: SqlFunction = SqlFunction(
        "ACOS",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC
    )
    val ASIN: SqlFunction = SqlFunction(
        "ASIN",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC
    )
    val ATAN: SqlFunction = SqlFunction(
        "ATAN",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC
    )
    val ATAN2: SqlFunction = SqlFunction(
        "ATAN2",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.NUMERIC_NUMERIC,
        SqlFunctionCategory.NUMERIC
    )
    val CBRT: SqlFunction = SqlFunction(
        "CBRT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC
    )
    val COS: SqlFunction = SqlFunction(
        "COS",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC
    )
    val COT: SqlFunction = SqlFunction(
        "COT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC
    )
    val DEGREES: SqlFunction = SqlFunction(
        "DEGREES",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC
    )
    val EXP: SqlFunction = SqlFunction(
        "EXP",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC
    )
    val RADIANS: SqlFunction = SqlFunction(
        "RADIANS",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC
    )
    val ROUND: SqlFunction = SqlFunction(
        "ROUND",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.NUMERIC_OPTIONAL_INTEGER,
        SqlFunctionCategory.NUMERIC
    )
    val SIGN: SqlFunction = SqlFunction(
        "SIGN",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC
    )
    val SIN: SqlFunction = SqlFunction(
        "SIN",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC
    )
    val TAN: SqlFunction = SqlFunction(
        "TAN",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC
    )
    val TRUNCATE: SqlFunction = SqlFunction(
        "TRUNCATE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.NUMERIC_OPTIONAL_INTEGER,
        SqlFunctionCategory.NUMERIC
    )
    val PI: SqlFunction = object : SqlFunction(
        "PI",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE,
        null,
        OperandTypes.NILADIC,
        SqlFunctionCategory.NUMERIC
    ) {
        @get:Override
        val syntax: SqlSyntax
            get() = SqlSyntax.FUNCTION_ID
    }

    /** `FIRST` function to be used within `MATCH_RECOGNIZE`.  */
    val FIRST: SqlFunction = SqlFunction(
        "FIRST", SqlKind.FIRST, ReturnTypes.ARG0_NULLABLE,
        null, OperandTypes.ANY_NUMERIC, SqlFunctionCategory.MATCH_RECOGNIZE
    )

    /** `LAST` function to be used within `MATCH_RECOGNIZE`.  */
    val LAST: SqlMatchFunction = SqlMatchFunction(
        "LAST", SqlKind.LAST, ReturnTypes.ARG0_NULLABLE,
        null, OperandTypes.ANY_NUMERIC, SqlFunctionCategory.MATCH_RECOGNIZE
    )

    /** `PREV` function to be used within `MATCH_RECOGNIZE`.  */
    val PREV: SqlMatchFunction = SqlMatchFunction(
        "PREV", SqlKind.PREV, ReturnTypes.ARG0_NULLABLE,
        null, OperandTypes.ANY_NUMERIC, SqlFunctionCategory.MATCH_RECOGNIZE
    )

    /** `NEXT` function to be used within `MATCH_RECOGNIZE`.  */
    val NEXT: SqlFunction = SqlFunction(
        "NEXT", SqlKind.NEXT, ReturnTypes.ARG0_NULLABLE, null,
        OperandTypes.ANY_NUMERIC, SqlFunctionCategory.MATCH_RECOGNIZE
    )

    /** `CLASSIFIER` function to be used within `MATCH_RECOGNIZE`.  */
    val CLASSIFIER: SqlMatchFunction = SqlMatchFunction(
        "CLASSIFIER", SqlKind.CLASSIFIER, ReturnTypes.VARCHAR_2000,
        null, OperandTypes.NILADIC, SqlFunctionCategory.MATCH_RECOGNIZE
    )

    /** `MATCH_NUMBER` function to be used within `MATCH_RECOGNIZE`.  */
    val MATCH_NUMBER: SqlFunction = SqlFunction(
        "MATCH_NUMBER ", SqlKind.MATCH_NUMBER, ReturnTypes.BIGINT_NULLABLE,
        null, OperandTypes.NILADIC, SqlFunctionCategory.MATCH_RECOGNIZE
    )
    val NULLIF: SqlFunction = SqlNullifFunction()

    /**
     * The COALESCE builtin function.
     */
    val COALESCE: SqlFunction = SqlCoalesceFunction()

    /**
     * The `FLOOR` function.
     */
    val FLOOR: SqlFunction = SqlFloorFunction(SqlKind.FLOOR)

    /**
     * The `CEIL` function.
     */
    val CEIL: SqlFunction = SqlFloorFunction(SqlKind.CEIL)

    /**
     * The `USER` function.
     */
    val USER: SqlFunction = SqlStringContextVariable("USER")

    /**
     * The `CURRENT_USER` function.
     */
    val CURRENT_USER: SqlFunction = SqlStringContextVariable("CURRENT_USER")

    /**
     * The `SESSION_USER` function.
     */
    val SESSION_USER: SqlFunction = SqlStringContextVariable("SESSION_USER")

    /**
     * The `SYSTEM_USER` function.
     */
    val SYSTEM_USER: SqlFunction = SqlStringContextVariable("SYSTEM_USER")

    /**
     * The `CURRENT_PATH` function.
     */
    val CURRENT_PATH: SqlFunction = SqlStringContextVariable("CURRENT_PATH")

    /**
     * The `CURRENT_ROLE` function.
     */
    val CURRENT_ROLE: SqlFunction = SqlStringContextVariable("CURRENT_ROLE")

    /**
     * The `CURRENT_CATALOG` function.
     */
    val CURRENT_CATALOG: SqlFunction = SqlStringContextVariable("CURRENT_CATALOG")

    /**
     * The `CURRENT_SCHEMA` function.
     */
    val CURRENT_SCHEMA: SqlFunction = SqlStringContextVariable("CURRENT_SCHEMA")

    /**
     * The `LOCALTIME [(*precision*)]` function.
     */
    val LOCALTIME: SqlFunction = SqlAbstractTimeFunction("LOCALTIME", SqlTypeName.TIME)

    /**
     * The `LOCALTIMESTAMP [(*precision*)]` function.
     */
    val LOCALTIMESTAMP: SqlFunction = SqlAbstractTimeFunction("LOCALTIMESTAMP", SqlTypeName.TIMESTAMP)

    /**
     * The `CURRENT_TIME [(*precision*)]` function.
     */
    val CURRENT_TIME: SqlFunction = SqlAbstractTimeFunction("CURRENT_TIME", SqlTypeName.TIME)

    /**
     * The `CURRENT_TIMESTAMP [(*precision*)]` function.
     */
    val CURRENT_TIMESTAMP: SqlFunction = SqlAbstractTimeFunction("CURRENT_TIMESTAMP", SqlTypeName.TIMESTAMP)

    /**
     * The `CURRENT_DATE` function.
     */
    val CURRENT_DATE: SqlFunction = SqlCurrentDateFunction()

    /** The `TIMESTAMPADD` function.  */
    val TIMESTAMP_ADD: SqlFunction = SqlTimestampAddFunction()

    /** The `TIMESTAMPDIFF` function.  */
    val TIMESTAMP_DIFF: SqlFunction = SqlTimestampDiffFunction()

    /**
     * Use of the `IN_FENNEL` operator forces the argument to be
     * evaluated in Fennel. Otherwise acts as identity function.
     */
    val IN_FENNEL: SqlFunction = SqlMonotonicUnaryFunction(
        "\$IN_FENNEL",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.SYSTEM
    )

    /**
     * The SQL `CAST` operator.
     *
     *
     * The SQL syntax is
     *
     * <blockquote>`CAST(*expression* AS *type*)`
    </blockquote> *
     *
     *
     * When the CAST operator is applies as a [SqlCall], it has two
     * arguments: the expression and the type. The type must not include a
     * constraint, so `CAST(x AS INTEGER NOT NULL)`, for instance, is
     * invalid.
     *
     *
     * When the CAST operator is applied as a `RexCall`, the
     * target type is simply stored as the return type, not an explicit operand.
     * For example, the expression `CAST(1 + 2 AS DOUBLE)` will
     * become a call to `CAST` with the expression `1 + 2`
     * as its only operand.
     *
     *
     * The `RexCall` form can also have a type which contains a
     * `NOT NULL` constraint. When this expression is implemented, if
     * the value is NULL, an exception will be thrown.
     */
    val CAST: SqlFunction = SqlCastFunction()

    /**
     * The SQL `EXTRACT` operator. Extracts a specified field value
     * from a DATETIME or an INTERVAL. E.g.<br></br>
     * `EXTRACT(HOUR FROM INTERVAL '364 23:59:59')` returns `
     * 23`
     */
    val EXTRACT: SqlFunction = SqlExtractFunction()

    /**
     * The SQL `YEAR` operator. Returns the Year
     * from a DATETIME  E.g.<br></br>
     * `YEAR(date '2008-9-23')` returns `
     * 2008`
     */
    val YEAR: SqlDatePartFunction = SqlDatePartFunction("YEAR", TimeUnit.YEAR)

    /**
     * The SQL `QUARTER` operator. Returns the Quarter
     * from a DATETIME  E.g.<br></br>
     * `QUARTER(date '2008-9-23')` returns `
     * 3`
     */
    val QUARTER: SqlDatePartFunction = SqlDatePartFunction("QUARTER", TimeUnit.QUARTER)

    /**
     * The SQL `MONTH` operator. Returns the Month
     * from a DATETIME  E.g.<br></br>
     * `MONTH(date '2008-9-23')` returns `
     * 9`
     */
    val MONTH: SqlDatePartFunction = SqlDatePartFunction("MONTH", TimeUnit.MONTH)

    /**
     * The SQL `WEEK` operator. Returns the Week
     * from a DATETIME  E.g.<br></br>
     * `WEEK(date '2008-9-23')` returns `
     * 39`
     */
    val WEEK: SqlDatePartFunction = SqlDatePartFunction("WEEK", TimeUnit.WEEK)

    /**
     * The SQL `DAYOFYEAR` operator. Returns the DOY
     * from a DATETIME  E.g.<br></br>
     * `DAYOFYEAR(date '2008-9-23')` returns `
     * 267`
     */
    val DAYOFYEAR: SqlDatePartFunction = SqlDatePartFunction("DAYOFYEAR", TimeUnit.DOY)

    /**
     * The SQL `DAYOFMONTH` operator. Returns the Day
     * from a DATETIME  E.g.<br></br>
     * `DAYOFMONTH(date '2008-9-23')` returns `
     * 23`
     */
    val DAYOFMONTH: SqlDatePartFunction = SqlDatePartFunction("DAYOFMONTH", TimeUnit.DAY)

    /**
     * The SQL `DAYOFWEEK` operator. Returns the DOW
     * from a DATETIME  E.g.<br></br>
     * `DAYOFWEEK(date '2008-9-23')` returns `
     * 2`
     */
    val DAYOFWEEK: SqlDatePartFunction = SqlDatePartFunction("DAYOFWEEK", TimeUnit.DOW)

    /**
     * The SQL `HOUR` operator. Returns the Hour
     * from a DATETIME  E.g.<br></br>
     * `HOUR(timestamp '2008-9-23 01:23:45')` returns `
     * 1`
     */
    val HOUR: SqlDatePartFunction = SqlDatePartFunction("HOUR", TimeUnit.HOUR)

    /**
     * The SQL `MINUTE` operator. Returns the Minute
     * from a DATETIME  E.g.<br></br>
     * `MINUTE(timestamp '2008-9-23 01:23:45')` returns `
     * 23`
     */
    val MINUTE: SqlDatePartFunction = SqlDatePartFunction("MINUTE", TimeUnit.MINUTE)

    /**
     * The SQL `SECOND` operator. Returns the Second
     * from a DATETIME  E.g.<br></br>
     * `SECOND(timestamp '2008-9-23 01:23:45')` returns `
     * 45`
     */
    val SECOND: SqlDatePartFunction = SqlDatePartFunction("SECOND", TimeUnit.SECOND)
    val LAST_DAY: SqlFunction = SqlFunction(
        "LAST_DAY",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DATE_NULLABLE,
        null,
        OperandTypes.DATETIME,
        SqlFunctionCategory.TIMEDATE
    )

    /**
     * The ELEMENT operator, used to convert a multiset with only one item to a
     * "regular" type. Example ... log(ELEMENT(MULTISET[1])) ...
     */
    val ELEMENT: SqlFunction = SqlFunction(
        "ELEMENT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.MULTISET_ELEMENT_NULLABLE,
        null,
        OperandTypes.COLLECTION,
        SqlFunctionCategory.SYSTEM
    )

    /**
     * The item operator `[ ... ]`, used to access a given element of an
     * array, map or struct. For example, `myArray[3]`, `"myMap['foo']"`,
     * `myStruct[2]` or `myStruct['fieldName']`.
     *
     *
     * The SQL standard calls the ARRAY variant a
     * &lt;array element reference&gt;. Index is 1-based. The standard says
     * to raise "data exception - array element error" but we currently return
     * null.
     *
     *
     * MAP is not standard SQL.
     */
    val ITEM: SqlOperator = SqlItemOperator()

    /**
     * The ARRAY Value Constructor. e.g. "`ARRAY[1, 2, 3]`".
     */
    val ARRAY_VALUE_CONSTRUCTOR: SqlArrayValueConstructor = SqlArrayValueConstructor()

    /**
     * The MAP Value Constructor,
     * e.g. "`MAP['washington', 1, 'obama', 44]`".
     */
    val MAP_VALUE_CONSTRUCTOR: SqlMapValueConstructor = SqlMapValueConstructor()

    /**
     * The internal "$SLICE" operator takes a multiset of records and returns a
     * multiset of the first column of those records.
     *
     *
     * It is introduced when multisets of scalar types are created, in order
     * to keep types consistent. For example, `MULTISET [5]` has type
     * `INTEGER MULTISET` but is translated to an expression of type
     * `RECORD(INTEGER EXPR$0) MULTISET` because in our internal
     * representation of multisets, every element must be a record. Applying the
     * "$SLICE" operator to this result converts the type back to an `
     * INTEGER MULTISET` multiset value.
     *
     *
     * `$SLICE` is often translated away when the multiset type is
     * converted back to scalar values.
     */
    val SLICE: SqlInternalOperator = object : SqlInternalOperator(
        "\$SLICE",
        SqlKind.OTHER,
        0,
        false,
        ReturnTypes.MULTISET_PROJECT0,
        null,
        OperandTypes.RECORD_COLLECTION
    ) {}

    /**
     * The internal "$ELEMENT_SLICE" operator returns the first field of the
     * only element of a multiset.
     *
     *
     * It is introduced when multisets of scalar types are created, in order
     * to keep types consistent. For example, `ELEMENT(MULTISET [5])`
     * is translated to `$ELEMENT_SLICE(MULTISET (VALUES ROW (5
     * EXPR$0))` It is translated away when the multiset type is converted
     * back to scalar values.
     *
     *
     * NOTE: jhyde, 2006/1/9: Usages of this operator are commented out, but
     * I'm not deleting the operator, because some multiset tests are disabled,
     * and we may need this operator to get them working!
     */
    val ELEMENT_SLICE: SqlInternalOperator = object : SqlInternalOperator(
        "\$ELEMENT_SLICE",
        SqlKind.OTHER,
        0,
        false,
        ReturnTypes.MULTISET_RECORD,
        null,
        OperandTypes.MULTISET
    ) {
        @Override
        fun unparse(
            writer: SqlWriter?,
            call: SqlCall?,
            leftPrec: Int,
            rightPrec: Int
        ) {
            SqlUtil.unparseFunctionSyntax(this, writer, call, false)
        }
    }

    /**
     * The internal "$SCALAR_QUERY" operator returns a scalar value from a
     * record type. It assumes the record type only has one field, and returns
     * that field as the output.
     */
    val SCALAR_QUERY: SqlInternalOperator = object : SqlInternalOperator(
        "\$SCALAR_QUERY",
        SqlKind.this,
        0,
        false,
        ReturnTypes.RECORD_TO_SCALAR,
        null,
        OperandTypes.RECORD_TO_SCALAR
    ) {
        @Override
        fun unparse(
            writer: SqlWriter,
            call: SqlCall,
            leftPrec: Int,
            rightPrec: Int
        ) {
            val frame: SqlWriter.Frame = writer.startList("(", ")")
            call.operand(0).unparse(writer, 0, 0)
            writer.endList(frame)
        }

        @Override
        fun argumentMustBeScalar(ordinal: Int): Boolean {
            // Obvious, really.
            return false
        }
    }

    /**
     * The internal `$STRUCT_ACCESS` operator is used to access a
     * field of a record.
     *
     *
     * In contrast with [.DOT] operator, it never appears in an
     * [SqlNode] tree and allows to access fields by position and
     * not by name.
     */
    val STRUCT_ACCESS: SqlInternalOperator = SqlInternalOperator("\$STRUCT_ACCESS", SqlKind.OTHER)

    /**
     * The CARDINALITY operator, used to retrieve the number of elements in a
     * MULTISET, ARRAY or MAP.
     */
    val CARDINALITY: SqlFunction = SqlFunction(
        "CARDINALITY",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        null,
        OperandTypes.COLLECTION_OR_MAP,
        SqlFunctionCategory.SYSTEM
    )

    /**
     * The COLLECT operator. Multiset aggregator function.
     */
    val COLLECT: SqlAggFunction = SqlBasicAggFunction
        .create(SqlKind.COLLECT, ReturnTypes.TO_MULTISET, OperandTypes.ANY)
        .withFunctionType(SqlFunctionCategory.SYSTEM)
        .withGroupOrder(Optionality.OPTIONAL)

    /**
     * `PERCENTILE_CONT` inverse distribution aggregate function.
     *
     *
     * The argument must be a numeric literal in the range 0 to 1 inclusive
     * (representing a percentage), and the return type is `DOUBLE`.
     */
    val PERCENTILE_CONT: SqlAggFunction = SqlBasicAggFunction
        .create(
            SqlKind.PERCENTILE_CONT, ReturnTypes.DOUBLE,
            OperandTypes.UNIT_INTERVAL_NUMERIC_LITERAL
        )
        .withFunctionType(SqlFunctionCategory.SYSTEM)
        .withGroupOrder(Optionality.MANDATORY)
        .withPercentile(true)

    /**
     * `PERCENTILE_DISC` inverse distribution aggregate function.
     *
     *
     * The argument must be a numeric literal in the range 0 to 1 inclusive
     * (representing a percentage), and the return type is `DOUBLE`.
     * (The return type should determined by the type of the `ORDER BY`
     * expression, but this cannot be determined by the function itself.)
     */
    val PERCENTILE_DISC: SqlAggFunction = SqlBasicAggFunction
        .create(
            SqlKind.PERCENTILE_DISC, ReturnTypes.DOUBLE,
            OperandTypes.UNIT_INTERVAL_NUMERIC_LITERAL
        )
        .withFunctionType(SqlFunctionCategory.SYSTEM)
        .withGroupOrder(Optionality.MANDATORY)
        .withPercentile(true)

    /**
     * The LISTAGG operator. String aggregator function.
     */
    val LISTAGG: SqlAggFunction = SqlListaggAggFunction(SqlKind.LISTAGG, ReturnTypes.ARG0_NULLABLE)

    /**
     * The FUSION operator. Multiset aggregator function.
     */
    val FUSION: SqlAggFunction = SqlBasicAggFunction
        .create(SqlKind.FUSION, ReturnTypes.ARG0, OperandTypes.MULTISET)
        .withFunctionType(SqlFunctionCategory.SYSTEM)

    /**
     * The INTERSECTION operator. Multiset aggregator function.
     */
    val INTERSECTION: SqlAggFunction = SqlBasicAggFunction
        .create(SqlKind.INTERSECTION, ReturnTypes.ARG0, OperandTypes.MULTISET)
        .withFunctionType(SqlFunctionCategory.SYSTEM)

    /** The sequence next value function: `NEXT VALUE FOR sequence`.  */
    val NEXT_VALUE: SqlOperator = SqlSequenceValueOperator(SqlKind.NEXT_VALUE)

    /** The sequence current value function: `CURRENT VALUE FOR
     * sequence`.  */
    val CURRENT_VALUE: SqlOperator = SqlSequenceValueOperator(SqlKind.CURRENT_VALUE)

    /**
     * The `TABLESAMPLE` operator.
     *
     *
     * Examples:
     *
     *
     *  * `<query> TABLESAMPLE SUBSTITUTE('sampleName')`
     * (non-standard)
     *  * `<query> TABLESAMPLE BERNOULLI(<percent>)
     * [REPEATABLE(<seed>)]` (standard, but not implemented for FTRS
     * yet)
     *  * `<query> TABLESAMPLE SYSTEM(<percent>)
     * [REPEATABLE(<seed>)]` (standard, but not implemented for FTRS
     * yet)
     *
     *
     *
     * Operand #0 is a query or table; Operand #1 is a [SqlSampleSpec]
     * wrapped in a [SqlLiteral].
     */
    val TABLESAMPLE: SqlSpecialOperator = object : SqlSpecialOperator(
        "TABLESAMPLE",
        SqlKind.this,
        20,
        true,
        ReturnTypes.ARG0,
        null,
        OperandTypes.VARIADIC
    ) {
        @Override
        fun unparse(
            writer: SqlWriter,
            call: SqlCall,
            leftPrec: Int,
            rightPrec: Int
        ) {
            call.operand(0).unparse(writer, leftPrec, 0)
            writer.keyword("TABLESAMPLE")
            call.operand(1).unparse(writer, 0, rightPrec)
        }
    }

    /** DESCRIPTOR(column_name, ...).  */
    val DESCRIPTOR: SqlOperator = SqlDescriptorOperator()

    /** TUMBLE as a table function.  */
    val TUMBLE: SqlFunction = SqlTumbleTableFunction()

    /** HOP as a table function.  */
    val HOP: SqlFunction = SqlHopTableFunction()

    /** SESSION as a table function.  */
    val SESSION: SqlFunction = SqlSessionTableFunction()

    /** The `TUMBLE` group function.
     *
     *
     * This operator is named "$TUMBLE" (not "TUMBLE") because it is created
     * directly by the parser, not by looking up an operator by name.
     *
     *
     * Why did we add TUMBLE to the parser? Because we plan to support TUMBLE
     * as a table function (see [CALCITE-3272]); "TUMBLE" as a name will only be
     * used by the TUMBLE table function.
     *
     *
     * After the TUMBLE table function is introduced, we plan to deprecate
     * this TUMBLE group function, and in fact all group functions. See
     * [CALCITE-3340] for details.
     */
    val TUMBLE_OLD: SqlGroupedWindowFunction = object : SqlGroupedWindowFunction(
        "\$TUMBLE", SqlKind.TUMBLE,
        null, ReturnTypes.ARG0, null,
        OperandTypes.or(
            OperandTypes.DATETIME_INTERVAL,
            OperandTypes.DATETIME_INTERVAL_TIME
        ),
        SqlFunctionCategory.SYSTEM
    ) {
        @get:Override
        val auxiliaryFunctions: List<Any>
            get() = ImmutableList.of(TUMBLE_START, TUMBLE_END)
    }

    /** The `TUMBLE_START` auxiliary function of
     * the `TUMBLE` group function.  */
    val TUMBLE_START: SqlGroupedWindowFunction = TUMBLE_OLD.auxiliary(SqlKind.TUMBLE_START)

    /** The `TUMBLE_END` auxiliary function of
     * the `TUMBLE` group function.  */
    val TUMBLE_END: SqlGroupedWindowFunction = TUMBLE_OLD.auxiliary(SqlKind.TUMBLE_END)

    /** The `HOP` group function.  */
    val HOP_OLD: SqlGroupedWindowFunction = object : SqlGroupedWindowFunction(
        "\$HOP", SqlKind.HOP, null,
        ReturnTypes.ARG0, null,
        OperandTypes.or(
            OperandTypes.DATETIME_INTERVAL_INTERVAL,
            OperandTypes.DATETIME_INTERVAL_INTERVAL_TIME
        ),
        SqlFunctionCategory.SYSTEM
    ) {
        @get:Override
        val auxiliaryFunctions: List<Any>
            get() = ImmutableList.of(HOP_START, HOP_END)
    }

    /** The `HOP_START` auxiliary function of
     * the `HOP` group function.  */
    val HOP_START: SqlGroupedWindowFunction = HOP_OLD.auxiliary(SqlKind.HOP_START)

    /** The `HOP_END` auxiliary function of
     * the `HOP` group function.  */
    val HOP_END: SqlGroupedWindowFunction = HOP_OLD.auxiliary(SqlKind.HOP_END)

    /** The `SESSION` group function.  */
    val SESSION_OLD: SqlGroupedWindowFunction = object : SqlGroupedWindowFunction(
        "\$SESSION", SqlKind.SESSION,
        null, ReturnTypes.ARG0, null,
        OperandTypes.or(
            OperandTypes.DATETIME_INTERVAL,
            OperandTypes.DATETIME_INTERVAL_TIME
        ),
        SqlFunctionCategory.SYSTEM
    ) {
        @get:Override
        val auxiliaryFunctions: List<Any>
            get() = ImmutableList.of(SESSION_START, SESSION_END)
    }

    /** The `SESSION_START` auxiliary function of
     * the `SESSION` group function.  */
    val SESSION_START: SqlGroupedWindowFunction = SESSION_OLD.auxiliary(SqlKind.SESSION_START)

    /** The `SESSION_END` auxiliary function of
     * the `SESSION` group function.  */
    val SESSION_END: SqlGroupedWindowFunction = SESSION_OLD.auxiliary(SqlKind.SESSION_END)

    /** `|` operator to create alternate patterns
     * within `MATCH_RECOGNIZE`.
     *
     *
     * If `p1` and `p2` are patterns then `p1 | p2` is a
     * pattern that matches `p1` or `p2`.  */
    val PATTERN_ALTER: SqlBinaryOperator = SqlBinaryOperator("|", SqlKind.PATTERN_ALTER, 70, true, null, null, null)

    /** Operator to concatenate patterns within `MATCH_RECOGNIZE`.
     *
     *
     * If `p1` and `p2` are patterns then `p1 p2` is a
     * pattern that matches `p1` followed by `p2`.  */
    val PATTERN_CONCAT: SqlBinaryOperator = SqlBinaryOperator("", SqlKind.PATTERN_CONCAT, 80, true, null, null, null)

    /** Operator to quantify patterns within `MATCH_RECOGNIZE`.
     *
     *
     * If `p` is a pattern then `p{3, 5}` is a
     * pattern that matches between 3 and 5 occurrences of `p`.  */
    val PATTERN_QUANTIFIER: SqlSpecialOperator = object : SqlSpecialOperator(
        "PATTERN_QUANTIFIER", SqlKind.this,
        90
    ) {
        @Override
        fun unparse(
            writer: SqlWriter, call: SqlCall,
            leftPrec: Int, rightPrec: Int
        ) {
            call.operand(0).unparse(writer, this.getLeftPrec(), this.getRightPrec())
            val startNum: Int = (call.operand(1) as SqlNumericLiteral).intValue(true)
            val endRepNum: SqlNumericLiteral = call.operand(2)
            val isReluctant: Boolean = (call.operand(3) as SqlLiteral).booleanValue()
            val endNum: Int = endRepNum.intValue(true)
            if (startNum == endNum) {
                writer.keyword("{ $startNum }")
            } else {
                if (endNum == -1) {
                    if (startNum == 0) {
                        writer.keyword("*")
                    } else if (startNum == 1) {
                        writer.keyword("+")
                    } else {
                        writer.keyword("{ $startNum, }")
                    }
                } else {
                    if (startNum == 0 && endNum == 1) {
                        writer.keyword("?")
                    } else if (startNum == -1) {
                        writer.keyword("{ , $endNum }")
                    } else {
                        writer.keyword("{ $startNum, $endNum }")
                    }
                }
                if (isReluctant) {
                    writer.keyword("?")
                }
            }
        }
    }

    /** `PERMUTE` operator to combine patterns within
     * `MATCH_RECOGNIZE`.
     *
     *
     * If `p1` and `p2` are patterns then `PERMUTE (p1, p2)`
     * is a pattern that matches all permutations of `p1` and
     * `p2`.  */
    val PATTERN_PERMUTE: SqlSpecialOperator = object : SqlSpecialOperator("PATTERN_PERMUTE", SqlKind.this, 100) {
        @Override
        fun unparse(
            writer: SqlWriter, call: SqlCall,
            leftPrec: Int, rightPrec: Int
        ) {
            writer.keyword("PERMUTE")
            val frame: SqlWriter.Frame = writer.startList("(", ")")
            for (i in 0 until call.getOperandList().size()) {
                val pattern: SqlNode = call.getOperandList().get(i)
                pattern.unparse(writer, 0, 0)
                if (i != call.getOperandList().size() - 1) {
                    writer.print(",")
                }
            }
            writer.endList(frame)
        }
    }

    /** `EXCLUDE` operator within `MATCH_RECOGNIZE`.
     *
     *
     * If `p` is a pattern then `{- p -} `} is a
     * pattern that excludes `p` from the output.  */
    val PATTERN_EXCLUDE: SqlSpecialOperator = object : SqlSpecialOperator(
        "PATTERN_EXCLUDE", SqlKind.PATTERN_EXCLUDED,
        100
    ) {
        @Override
        fun unparse(
            writer: SqlWriter, call: SqlCall,
            leftPrec: Int, rightPrec: Int
        ) {
            val frame: SqlWriter.Frame = writer.startList("{-", "-}")
            val node: SqlNode = call.getOperandList().get(0)
            node.unparse(writer, 0, 0)
            writer.endList(frame)
        }
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Returns the standard operator table, creating it if necessary.
     */
    @Synchronized
    fun instance(): SqlStdOperatorTable? {
        if (instance == null) {
            // Creates and initializes the standard operator table.
            // Uses two-phase construction, because we can't initialize the
            // table until the constructor of the sub-class has completed.
            instance = SqlStdOperatorTable()
            instance.init()
        }
        return instance
    }

    /** Returns the group function for which a given kind is an auxiliary
     * function, or null if it is not an auxiliary function.  */
    @Nullable
    fun auxiliaryToGroup(kind: SqlKind?): SqlGroupedWindowFunction? {
        return when (kind) {
            TUMBLE_START, TUMBLE_END -> TUMBLE_OLD
            HOP_START, HOP_END -> HOP_OLD
            SESSION_START, SESSION_END -> SESSION_OLD
            else -> null
        }
    }

    /** Converts a call to a grouped auxiliary function
     * to a call to the grouped window function. For other calls returns null.
     *
     *
     * For example, converts `TUMBLE_START(rowtime, INTERVAL '1' HOUR))`
     * to `TUMBLE(rowtime, INTERVAL '1' HOUR))`.  */
    @Nullable
    fun convertAuxiliaryToGroupCall(call: SqlCall): SqlCall? {
        val op: SqlOperator = call.getOperator()
        if (op is SqlGroupedWindowFunction
            && op.isGroupAuxiliary()
        ) {
            val groupFunction: SqlGroupedWindowFunction = (op as SqlGroupedWindowFunction).groupFunction
            return copy(call, requireNonNull(groupFunction, "groupFunction"))
        }
        return null
    }

    /** Converts a call to a grouped window function to a call to its auxiliary
     * window function(s). For other calls returns null.
     *
     *
     * For example, converts `TUMBLE_START(rowtime, INTERVAL '1' HOUR))`
     * to `TUMBLE(rowtime, INTERVAL '1' HOUR))`.  */
    fun convertGroupToAuxiliaryCalls(
        call: SqlCall
    ): List<Pair<SqlNode, AuxiliaryConverter>> {
        val op: SqlOperator = call.getOperator()
        if (op is SqlGroupedWindowFunction
            && op.isGroup()
        ) {
            val builder: ImmutableList.Builder<Pair<SqlNode, AuxiliaryConverter>> = ImmutableList.builder()
            for (f in (op as SqlGroupedWindowFunction).getAuxiliaryFunctions()) {
                builder.add(
                    Pair.of(
                        copy(call, f),
                        Impl(f)
                    )
                )
            }
            return builder.build()
        }
        return ImmutableList.of()
    }

    /** Creates a copy of a call with a new operator.  */
    private fun copy(call: SqlCall, operator: SqlOperator): SqlCall {
        return SqlBasicCall(
            operator, call.getOperandList(),
            call.getParserPosition()
        )
    }

    /** Returns the operator for `SOME comparisonKind`.  */
    fun some(comparisonKind: SqlKind?): SqlQuantifyOperator {
        return when (comparisonKind) {
            EQUALS -> SOME_EQ
            NOT_EQUALS -> SOME_NE
            LESS_THAN -> SOME_LT
            LESS_THAN_OR_EQUAL -> SOME_LE
            GREATER_THAN -> SOME_GT
            GREATER_THAN_OR_EQUAL -> SOME_GE
            else -> throw AssertionError(comparisonKind)
        }
    }

    /** Returns the operator for `ALL comparisonKind`.  */
    fun all(comparisonKind: SqlKind?): SqlQuantifyOperator {
        return when (comparisonKind) {
            EQUALS -> ALL_EQ
            NOT_EQUALS -> ALL_NE
            LESS_THAN -> ALL_LT
            LESS_THAN_OR_EQUAL -> ALL_LE
            GREATER_THAN -> ALL_GT
            GREATER_THAN_OR_EQUAL -> ALL_GE
            else -> throw AssertionError(comparisonKind)
        }
    }

    /** Returns the binary operator that corresponds to this operator but in the opposite
     * direction. Or returns this, if its kind is not reversible.
     *
     *
     * For example, `reverse(GREATER_THAN)` returns [.LESS_THAN].
     *
     */
    @Deprecated // to be removed before 2.0
    @Deprecated(
        """Use {@link SqlOperator#reverse()}, but beware that it has
    slightly different semantics"""
    )
    fun reverse(operator: SqlOperator): SqlOperator {
        return when (operator.getKind()) {
            GREATER_THAN -> LESS_THAN
            GREATER_THAN_OR_EQUAL -> LESS_THAN_OR_EQUAL
            LESS_THAN -> GREATER_THAN
            LESS_THAN_OR_EQUAL -> GREATER_THAN_OR_EQUAL
            else -> operator
        }
    }

    /** Returns the operator for `LIKE` with given case-sensitivity,
     * optionally negated.  */
    fun like(negated: Boolean, caseSensitive: Boolean): SqlOperator {
        return if (negated) {
            if (caseSensitive) {
                NOT_LIKE
            } else {
                SqlLibraryOperators.NOT_ILIKE
            }
        } else {
            if (caseSensitive) {
                LIKE
            } else {
                SqlLibraryOperators.ILIKE
            }
        }
    }
}
