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

import org.apache.calcite.rel.type.RelDataType

/**
 * Defines functions and operators that are not part of standard SQL but
 * belong to one or more other dialects of SQL.
 *
 *
 * They are read by [SqlLibraryOperatorTableFactory] into instances
 * of [SqlOperatorTable] that contain functions and operators for
 * particular libraries.
 */
object SqlLibraryOperators {
    /** The "CONVERT_TIMEZONE(tz1, tz2, datetime)" function;
     * converts the timezone of `datetime` from `tz1` to `tz2`.
     * This function is only on Redshift, but we list it in PostgreSQL
     * because Redshift does not have its own library.  */
    @LibraryOperator(libraries = [POSTGRESQL])
    val CONVERT_TIMEZONE: SqlFunction = SqlFunction(
        "CONVERT_TIMEZONE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DATE_NULLABLE,
        null,
        OperandTypes.CHARACTER_CHARACTER_DATETIME,
        SqlFunctionCategory.TIMEDATE
    )

    /** Return type inference for `DECODE`.  */
    private val DECODE_RETURN_TYPE: SqlReturnTypeInference = SqlReturnTypeInference { opBinding ->
        val list: List<RelDataType> = ArrayList()
        var i = 1
        val n: Int = opBinding.getOperandCount()
        while (i < n) {
            if (i < n - 1) {
                ++i
            }
            list.add(opBinding.getOperandType(i))
            i++
        }
        val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
        var type: RelDataType = typeFactory.leastRestrictive(list)
        if (type != null && opBinding.getOperandCount() % 2 === 1) {
            type = typeFactory.createTypeWithNullability(type, true)
        }
        type
    }

    /** The "DECODE(v, v1, result1, [v2, result2, ...], resultN)" function.  */
    @LibraryOperator(libraries = [ORACLE])
    val DECODE: SqlFunction = SqlFunction(
        "DECODE", SqlKind.DECODE, DECODE_RETURN_TYPE, null,
        OperandTypes.VARIADIC, SqlFunctionCategory.SYSTEM
    )

    /** The "IF(condition, thenValue, elseValue)" function.  */
    @LibraryOperator(libraries = [BIG_QUERY, HIVE, SPARK])
    val IF: SqlFunction = object : SqlFunction(
        "IF",
        SqlKind.this,
        { obj: SqlLibraryOperators?, opBinding: SqlOperatorBinding -> inferIfReturnType(opBinding) },
        null,
        OperandTypes.and(
            OperandTypes.family(
                SqlTypeFamily.BOOLEAN, SqlTypeFamily.ANY,
                SqlTypeFamily.ANY
            ),  // Arguments 1 and 2 must have same type
            object : SameOperandTypeChecker(3) {
                @Override
                protected fun getOperandList(operandCount: Int): List<Integer> {
                    return ImmutableList.of(1, 2)
                }
            }),
        SqlFunctionCategory.SYSTEM
    ) {
        @Override
        fun validRexOperands(count: Int, litmus: Litmus): Boolean {
            // IF is translated to RexNode by expanding to CASE.
            return litmus.fail("not a rex operator")
        }
    }

    /** Infers the return type of `IF(b, x, y)`,
     * namely the least restrictive of the types of x and y.
     * Similar to [ReturnTypes.LEAST_RESTRICTIVE].  */
    @Nullable
    private fun inferIfReturnType(opBinding: SqlOperatorBinding): RelDataType {
        return opBinding.getTypeFactory()
            .leastRestrictive(opBinding.collectOperandTypes().subList(1, 3))
    }

    /** The "NVL(value, value)" function.  */
    @LibraryOperator(libraries = [ORACLE])
    val NVL: SqlFunction = SqlFunction(
        "NVL", SqlKind.NVL,
        ReturnTypes.LEAST_RESTRICTIVE
            .andThen(SqlTypeTransforms.TO_NULLABLE_ALL),
        null, OperandTypes.SAME_SAME, SqlFunctionCategory.SYSTEM
    )

    /** The "LTRIM(string)" function.  */
    @LibraryOperator(libraries = [ORACLE])
    val LTRIM: SqlFunction = SqlFunction(
        "LTRIM", SqlKind.LTRIM,
        ReturnTypes.ARG0.andThen(SqlTypeTransforms.TO_NULLABLE)
            .andThen(SqlTypeTransforms.TO_VARYING), null,
        OperandTypes.STRING, SqlFunctionCategory.STRING
    )

    /** The "RTRIM(string)" function.  */
    @LibraryOperator(libraries = [ORACLE])
    val RTRIM: SqlFunction = SqlFunction(
        "RTRIM", SqlKind.RTRIM,
        ReturnTypes.ARG0.andThen(SqlTypeTransforms.TO_NULLABLE)
            .andThen(SqlTypeTransforms.TO_VARYING), null,
        OperandTypes.STRING, SqlFunctionCategory.STRING
    )

    /** BigQuery's "SUBSTR(string, position [, substringLength ])" function.  */
    @LibraryOperator(libraries = [BIG_QUERY])
    val SUBSTR_BIG_QUERY: SqlFunction = SqlFunction(
        "SUBSTR", SqlKind.SUBSTR_BIG_QUERY,
        ReturnTypes.ARG0_NULLABLE_VARYING, null,
        OperandTypes.STRING_INTEGER_OPTIONAL_INTEGER,
        SqlFunctionCategory.STRING
    )

    /** MySQL's "SUBSTR(string, position [, substringLength ])" function.  */
    @LibraryOperator(libraries = [MYSQL])
    val SUBSTR_MYSQL: SqlFunction = SqlFunction(
        "SUBSTR", SqlKind.SUBSTR_MYSQL,
        ReturnTypes.ARG0_NULLABLE_VARYING, null,
        OperandTypes.STRING_INTEGER_OPTIONAL_INTEGER,
        SqlFunctionCategory.STRING
    )

    /** Oracle's "SUBSTR(string, position [, substringLength ])" function.
     *
     *
     * It has different semantics to standard SQL's
     * [SqlStdOperatorTable.SUBSTRING] function:
     *
     *
     *  * If `substringLength`  0, result is the empty string
     * (Oracle would return null, because it treats the empty string as null,
     * but Calcite does not have these semantics);
     *  * If `position` = 0, treat `position` as 1;
     *  * If `position` &lt; 0, treat `position` as
     * "length(string) + position + 1".
     *
     */
    @LibraryOperator(libraries = [ORACLE])
    val SUBSTR_ORACLE: SqlFunction = SqlFunction(
        "SUBSTR", SqlKind.SUBSTR_ORACLE,
        ReturnTypes.ARG0_NULLABLE_VARYING, null,
        OperandTypes.STRING_INTEGER_OPTIONAL_INTEGER,
        SqlFunctionCategory.STRING
    )

    /** PostgreSQL's "SUBSTR(string, position [, substringLength ])" function.  */
    @LibraryOperator(libraries = [POSTGRESQL])
    val SUBSTR_POSTGRESQL: SqlFunction = SqlFunction(
        "SUBSTR", SqlKind.SUBSTR_POSTGRESQL,
        ReturnTypes.ARG0_NULLABLE_VARYING, null,
        OperandTypes.STRING_INTEGER_OPTIONAL_INTEGER,
        SqlFunctionCategory.STRING
    )

    /** The "GREATEST(value, value)" function.  */
    @LibraryOperator(libraries = [ORACLE])
    val GREATEST: SqlFunction = SqlFunction(
        "GREATEST", SqlKind.GREATEST,
        ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.TO_NULLABLE),
        null, OperandTypes.SAME_VARIADIC, SqlFunctionCategory.SYSTEM
    )

    /** The "LEAST(value, value)" function.  */
    @LibraryOperator(libraries = [ORACLE])
    val LEAST: SqlFunction = SqlFunction(
        "LEAST", SqlKind.LEAST,
        ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.TO_NULLABLE),
        null, OperandTypes.SAME_VARIADIC, SqlFunctionCategory.SYSTEM
    )

    /**
     * The `TRANSLATE(*string_expr*, *search_chars*,
     * *replacement_chars*)` function returns *string_expr* with
     * all occurrences of each character in *search_chars* replaced by its
     * corresponding character in *replacement_chars*.
     *
     *
     * It is not defined in the SQL standard, but occurs in Oracle and
     * PostgreSQL.
     */
    @LibraryOperator(libraries = [ORACLE, POSTGRESQL])
    val TRANSLATE3: SqlFunction = SqlTranslate3Function()

    @LibraryOperator(libraries = [MYSQL])
    val JSON_TYPE: SqlFunction = SqlJsonTypeFunction()

    @LibraryOperator(libraries = [MYSQL])
    val JSON_DEPTH: SqlFunction = SqlJsonDepthFunction()

    @LibraryOperator(libraries = [MYSQL])
    val JSON_LENGTH: SqlFunction = SqlJsonLengthFunction()

    @LibraryOperator(libraries = [MYSQL])
    val JSON_KEYS: SqlFunction = SqlJsonKeysFunction()

    @LibraryOperator(libraries = [MYSQL])
    val JSON_PRETTY: SqlFunction = SqlJsonPrettyFunction()

    @LibraryOperator(libraries = [MYSQL])
    val JSON_REMOVE: SqlFunction = SqlJsonRemoveFunction()

    @LibraryOperator(libraries = [MYSQL])
    val JSON_STORAGE_SIZE: SqlFunction = SqlJsonStorageSizeFunction()

    @LibraryOperator(libraries = [MYSQL, ORACLE])
    val REGEXP_REPLACE: SqlFunction = SqlRegexpReplaceFunction()

    @LibraryOperator(libraries = [MYSQL])
    val COMPRESS: SqlFunction = SqlFunction(
        "COMPRESS", SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY)
            .andThen(SqlTypeTransforms.TO_NULLABLE),
        null, OperandTypes.STRING, SqlFunctionCategory.STRING
    )

    @LibraryOperator(libraries = [MYSQL])
    val EXTRACT_VALUE: SqlFunction = SqlFunction(
        "EXTRACTVALUE", SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000.andThen(SqlTypeTransforms.FORCE_NULLABLE),
        null, OperandTypes.STRING_STRING, SqlFunctionCategory.SYSTEM
    )

    @LibraryOperator(libraries = [ORACLE])
    val XML_TRANSFORM: SqlFunction = SqlFunction(
        "XMLTRANSFORM", SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000.andThen(SqlTypeTransforms.FORCE_NULLABLE),
        null, OperandTypes.STRING_STRING, SqlFunctionCategory.SYSTEM
    )

    @LibraryOperator(libraries = [ORACLE])
    val EXTRACT_XML: SqlFunction = SqlFunction(
        "EXTRACT", SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000.andThen(SqlTypeTransforms.FORCE_NULLABLE),
        null, OperandTypes.STRING_STRING_OPTIONAL_STRING,
        SqlFunctionCategory.SYSTEM
    )

    @LibraryOperator(libraries = [ORACLE])
    val EXISTS_NODE: SqlFunction = SqlFunction(
        "EXISTSNODE", SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE
            .andThen(SqlTypeTransforms.FORCE_NULLABLE), null,
        OperandTypes.STRING_STRING_OPTIONAL_STRING, SqlFunctionCategory.SYSTEM
    )

    /** The "BOOL_AND(condition)" aggregate function, PostgreSQL and Redshift's
     * equivalent to [SqlStdOperatorTable.EVERY].  */
    @LibraryOperator(libraries = [POSTGRESQL])
    val BOOL_AND: SqlAggFunction = SqlMinMaxAggFunction("BOOL_AND", SqlKind.MIN, OperandTypes.BOOLEAN)

    /** The "BOOL_OR(condition)" aggregate function, PostgreSQL and Redshift's
     * equivalent to [SqlStdOperatorTable.SOME].  */
    @LibraryOperator(libraries = [POSTGRESQL])
    val BOOL_OR: SqlAggFunction = SqlMinMaxAggFunction("BOOL_OR", SqlKind.MAX, OperandTypes.BOOLEAN)

    /** The "LOGICAL_AND(condition)" aggregate function, BigQuery's
     * equivalent to [SqlStdOperatorTable.EVERY].  */
    @LibraryOperator(libraries = [BIG_QUERY])
    val LOGICAL_AND: SqlAggFunction = SqlMinMaxAggFunction("LOGICAL_AND", SqlKind.MIN, OperandTypes.BOOLEAN)

    /** The "LOGICAL_OR(condition)" aggregate function, BigQuery's
     * equivalent to [SqlStdOperatorTable.SOME].  */
    @LibraryOperator(libraries = [BIG_QUERY])
    val LOGICAL_OR: SqlAggFunction = SqlMinMaxAggFunction("LOGICAL_OR", SqlKind.MAX, OperandTypes.BOOLEAN)

    /** The "COUNTIF(condition) [OVER (...)]" function, in BigQuery,
     * returns the count of TRUE values for expression.
     *
     *
     * `COUNTIF(b)` is equivalent to
     * `COUNT(*) FILTER (WHERE b)`.  */
    @LibraryOperator(libraries = [BIG_QUERY])
    val COUNTIF: SqlAggFunction = SqlBasicAggFunction
        .create(SqlKind.COUNTIF, ReturnTypes.BIGINT, OperandTypes.BOOLEAN)
        .withDistinct(Optionality.FORBIDDEN)

    /** The "ARRAY_AGG(value [ ORDER BY ...])" aggregate function,
     * in BigQuery and PostgreSQL, gathers values into arrays.  */
    @LibraryOperator(libraries = [POSTGRESQL, BIG_QUERY])
    val ARRAY_AGG: SqlAggFunction = SqlBasicAggFunction
        .create(
            SqlKind.ARRAY_AGG,
            ReturnTypes.andThen(
                ReturnTypes::stripOrderBy,
                ReturnTypes.TO_ARRAY
            ), OperandTypes.ANY
        )
        .withFunctionType(SqlFunctionCategory.SYSTEM)
        .withSyntax(SqlSyntax.ORDERED_FUNCTION)
        .withAllowsNullTreatment(true)

    /** The "ARRAY_CONCAT_AGG(value [ ORDER BY ...])" aggregate function,
     * in BigQuery and PostgreSQL, concatenates array values into arrays.  */
    @LibraryOperator(libraries = [POSTGRESQL, BIG_QUERY])
    val ARRAY_CONCAT_AGG: SqlAggFunction = SqlBasicAggFunction
        .create(
            SqlKind.ARRAY_CONCAT_AGG, ReturnTypes.ARG0,
            OperandTypes.ARRAY
        )
        .withFunctionType(SqlFunctionCategory.SYSTEM)
        .withSyntax(SqlSyntax.ORDERED_FUNCTION)

    /** The "STRING_AGG(value [, separator ] [ ORDER BY ...])" aggregate function,
     * BigQuery and PostgreSQL's equivalent of
     * [SqlStdOperatorTable.LISTAGG].
     *
     *
     * `STRING_AGG(v, sep ORDER BY x, y)` is implemented by
     * rewriting to `LISTAGG(v, sep) WITHIN GROUP (ORDER BY x, y)`.  */
    @LibraryOperator(libraries = [POSTGRESQL, BIG_QUERY])
    val STRING_AGG: SqlAggFunction = SqlBasicAggFunction
        .create(
            SqlKind.STRING_AGG, ReturnTypes.ARG0_NULLABLE,
            OperandTypes.or(OperandTypes.STRING, OperandTypes.STRING_STRING)
        )
        .withFunctionType(SqlFunctionCategory.SYSTEM)
        .withSyntax(SqlSyntax.ORDERED_FUNCTION)

    /** The "GROUP_CONCAT([DISTINCT] expr [, ...] [ORDER BY ...] [SEPARATOR sep])"
     * aggregate function, MySQL's equivalent of
     * [SqlStdOperatorTable.LISTAGG].
     *
     *
     * `GROUP_CONCAT(v ORDER BY x, y SEPARATOR s)` is implemented by
     * rewriting to `LISTAGG(v, s) WITHIN GROUP (ORDER BY x, y)`.  */
    @LibraryOperator(libraries = [MYSQL])
    val GROUP_CONCAT: SqlAggFunction = SqlBasicAggFunction
        .create(
            SqlKind.GROUP_CONCAT,
            ReturnTypes.andThen(
                ReturnTypes::stripOrderBy,
                ReturnTypes.ARG0_NULLABLE
            ),
            OperandTypes.or(OperandTypes.STRING, OperandTypes.STRING_STRING)
        )
        .withFunctionType(SqlFunctionCategory.SYSTEM)
        .withAllowsNullTreatment(false)
        .withAllowsSeparator(true)
        .withSyntax(SqlSyntax.ORDERED_FUNCTION)

    /** The "DATE(string)" function, equivalent to "CAST(string AS DATE).  */
    @LibraryOperator(libraries = [BIG_QUERY])
    val DATE: SqlFunction = SqlFunction(
        "DATE", SqlKind.OTHER_FUNCTION,
        ReturnTypes.DATE_NULLABLE, null, OperandTypes.STRING,
        SqlFunctionCategory.TIMEDATE
    )

    /** The "CURRENT_DATETIME([timezone])" function.  */
    @LibraryOperator(libraries = [BIG_QUERY])
    val CURRENT_DATETIME: SqlFunction = SqlFunction(
        "CURRENT_DATETIME", SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIMESTAMP.andThen(SqlTypeTransforms.TO_NULLABLE), null,
        OperandTypes.or(OperandTypes.NILADIC, OperandTypes.STRING),
        SqlFunctionCategory.TIMEDATE
    )

    /** The "DATE_FROM_UNIX_DATE(integer)" function; returns a DATE value
     * a given number of seconds after 1970-01-01.  */
    @LibraryOperator(libraries = [BIG_QUERY])
    val DATE_FROM_UNIX_DATE: SqlFunction = SqlFunction(
        "DATE_FROM_UNIX_DATE", SqlKind.OTHER_FUNCTION,
        ReturnTypes.DATE_NULLABLE, null, OperandTypes.INTEGER,
        SqlFunctionCategory.TIMEDATE
    )

    /** The "UNIX_DATE(date)" function; returns the number of days since
     * 1970-01-01.  */
    @LibraryOperator(libraries = [BIG_QUERY])
    val UNIX_DATE: SqlFunction = SqlFunction(
        "UNIX_DATE", SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE, null, OperandTypes.DATE,
        SqlFunctionCategory.TIMEDATE
    )

    /** The "MONTHNAME(datetime)" function; returns the name of the month,
     * in the current locale, of a TIMESTAMP or DATE argument.  */
    @LibraryOperator(libraries = [MYSQL])
    val MONTHNAME: SqlFunction = SqlFunction(
        "MONTHNAME", SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000, null, OperandTypes.DATETIME,
        SqlFunctionCategory.TIMEDATE
    )

    /** The "DAYNAME(datetime)" function; returns the name of the day of the week,
     * in the current locale, of a TIMESTAMP or DATE argument.  */
    @LibraryOperator(libraries = [MYSQL])
    val DAYNAME: SqlFunction = SqlFunction(
        "DAYNAME", SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000, null, OperandTypes.DATETIME,
        SqlFunctionCategory.TIMEDATE
    )

    @LibraryOperator(libraries = [MYSQL, POSTGRESQL])
    val LEFT: SqlFunction = SqlFunction(
        "LEFT", SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE_VARYING, null,
        OperandTypes.CBSTRING_INTEGER, SqlFunctionCategory.STRING
    )

    @LibraryOperator(libraries = [MYSQL, POSTGRESQL])
    val REPEAT: SqlFunction = SqlFunction(
        "REPEAT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE_VARYING,
        null,
        OperandTypes.STRING_INTEGER,
        SqlFunctionCategory.STRING
    )

    @LibraryOperator(libraries = [MYSQL, POSTGRESQL])
    val RIGHT: SqlFunction = SqlFunction(
        "RIGHT", SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE_VARYING, null,
        OperandTypes.CBSTRING_INTEGER, SqlFunctionCategory.STRING
    )

    @LibraryOperator(libraries = [MYSQL])
    val SPACE: SqlFunction = SqlFunction(
        "SPACE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000_NULLABLE,
        null,
        OperandTypes.INTEGER,
        SqlFunctionCategory.STRING
    )

    @LibraryOperator(libraries = [MYSQL])
    val STRCMP: SqlFunction = SqlFunction(
        "STRCMP",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        null,
        OperandTypes.STRING_STRING,
        SqlFunctionCategory.STRING
    )

    @LibraryOperator(libraries = [MYSQL, POSTGRESQL, ORACLE])
    val SOUNDEX: SqlFunction = SqlFunction(
        "SOUNDEX",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_4_NULLABLE,
        null,
        OperandTypes.CHARACTER,
        SqlFunctionCategory.STRING
    )

    @LibraryOperator(libraries = [POSTGRESQL])
    val DIFFERENCE: SqlFunction = SqlFunction(
        "DIFFERENCE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        null,
        OperandTypes.STRING_STRING,
        SqlFunctionCategory.STRING
    )

    /** The case-insensitive variant of the LIKE operator.  */
    @LibraryOperator(libraries = [POSTGRESQL])
    val ILIKE: SqlSpecialOperator = SqlLikeOperator("ILIKE", SqlKind.LIKE, false, false)

    /** The case-insensitive variant of the NOT LIKE operator.  */
    @LibraryOperator(libraries = [POSTGRESQL])
    val NOT_ILIKE: SqlSpecialOperator = SqlLikeOperator("NOT ILIKE", SqlKind.LIKE, true, false)

    /** The regex variant of the LIKE operator.  */
    @LibraryOperator(libraries = [SPARK, HIVE])
    val RLIKE: SqlSpecialOperator = SqlLikeOperator("RLIKE", SqlKind.RLIKE, false, true)

    /** The regex variant of the NOT LIKE operator.  */
    @LibraryOperator(libraries = [SPARK, HIVE])
    val NOT_RLIKE: SqlSpecialOperator = SqlLikeOperator("NOT RLIKE", SqlKind.RLIKE, true, true)

    /** The "CONCAT(arg, ...)" function that concatenates strings.
     * For example, "CONCAT('a', 'bc', 'd')" returns "abcd".  */
    @LibraryOperator(libraries = [MYSQL, POSTGRESQL])
    val CONCAT_FUNCTION: SqlFunction = SqlFunction(
        "CONCAT",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.MULTIVALENT_STRING_SUM_PRECISION_NULLABLE,
        InferTypes.RETURN_TYPE,
        OperandTypes.repeat(
            SqlOperandCountRanges.from(2),
            OperandTypes.STRING
        ),
        SqlFunctionCategory.STRING
    )

    /** The "CONCAT(arg0, arg1)" function that concatenates strings.
     * For example, "CONCAT('a', 'bc')" returns "abc".
     *
     *
     * It is assigned [SqlKind.CONCAT2] to make it not equal to
     * [.CONCAT_FUNCTION].  */
    @LibraryOperator(libraries = [ORACLE])
    val CONCAT2: SqlFunction = SqlFunction(
        "CONCAT",
        SqlKind.CONCAT2,
        ReturnTypes.MULTIVALENT_STRING_SUM_PRECISION_NULLABLE,
        InferTypes.RETURN_TYPE,
        OperandTypes.STRING_SAME_SAME,
        SqlFunctionCategory.STRING
    )

    /** The "ARRAY_LENGTH(array)" function.  */
    @LibraryOperator(libraries = [BIG_QUERY])
    val ARRAY_LENGTH: SqlFunction = SqlFunction(
        "ARRAY_LENGTH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER_NULLABLE,
        null,
        OperandTypes.ARRAY,
        SqlFunctionCategory.SYSTEM
    )

    /** The "ARRAY_REVERSE(array)" function.  */
    @LibraryOperator(libraries = [BIG_QUERY])
    val ARRAY_REVERSE: SqlFunction = SqlFunction(
        "ARRAY_REVERSE",
        SqlKind.ARRAY_REVERSE,
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.ARRAY,
        SqlFunctionCategory.SYSTEM
    )

    /** The "ARRAY_CONCAT(array [, array]*)" function.  */
    @LibraryOperator(libraries = [BIG_QUERY])
    val ARRAY_CONCAT: SqlFunction = SqlFunction(
        "ARRAY_CONCAT",
        SqlKind.ARRAY_CONCAT,
        ReturnTypes.LEAST_RESTRICTIVE,
        null,
        OperandTypes.AT_LEAST_ONE_SAME_VARIADIC,
        SqlFunctionCategory.SYSTEM
    )

    @LibraryOperator(libraries = [MYSQL])
    val REVERSE: SqlFunction = SqlFunction(
        "REVERSE",
        SqlKind.REVERSE,
        ReturnTypes.ARG0_NULLABLE_VARYING,
        null,
        OperandTypes.CHARACTER,
        SqlFunctionCategory.STRING
    )

    @LibraryOperator(libraries = [MYSQL])
    val FROM_BASE64: SqlFunction = SqlFunction(
        "FROM_BASE64", SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY)
            .andThen(SqlTypeTransforms.TO_NULLABLE),
        null, OperandTypes.STRING, SqlFunctionCategory.STRING
    )

    @LibraryOperator(libraries = [MYSQL])
    val TO_BASE64: SqlFunction = SqlFunction(
        "TO_BASE64", SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARCHAR)
            .andThen(SqlTypeTransforms.TO_NULLABLE),
        null, OperandTypes.or(OperandTypes.STRING, OperandTypes.BINARY),
        SqlFunctionCategory.STRING
    )

    /** The "TO_DATE(string1, string2)" function; casts string1
     * to a DATE using the format specified in string2.  */
    @LibraryOperator(libraries = [POSTGRESQL, ORACLE])
    val TO_DATE: SqlFunction = SqlFunction(
        "TO_DATE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DATE_NULLABLE,
        null,
        OperandTypes.STRING_STRING,
        SqlFunctionCategory.TIMEDATE
    )

    /** The "TO_TIMESTAMP(string1, string2)" function; casts string1
     * to a TIMESTAMP using the format specified in string2.  */
    @LibraryOperator(libraries = [POSTGRESQL, ORACLE])
    val TO_TIMESTAMP: SqlFunction = SqlFunction(
        "TO_TIMESTAMP",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DATE_NULLABLE,
        null,
        OperandTypes.STRING_STRING,
        SqlFunctionCategory.TIMEDATE
    )

    /** The "TIMESTAMP_SECONDS(bigint)" function; returns a TIMESTAMP value
     * a given number of seconds after 1970-01-01 00:00:00.  */
    @LibraryOperator(libraries = [BIG_QUERY])
    val TIMESTAMP_SECONDS: SqlFunction = SqlFunction(
        "TIMESTAMP_SECONDS", SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIMESTAMP_NULLABLE, null, OperandTypes.INTEGER,
        SqlFunctionCategory.TIMEDATE
    )

    /** The "TIMESTAMP_MILLIS(bigint)" function; returns a TIMESTAMP value
     * a given number of milliseconds after 1970-01-01 00:00:00.  */
    @LibraryOperator(libraries = [BIG_QUERY])
    val TIMESTAMP_MILLIS: SqlFunction = SqlFunction(
        "TIMESTAMP_MILLIS", SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIMESTAMP_NULLABLE, null, OperandTypes.INTEGER,
        SqlFunctionCategory.TIMEDATE
    )

    /** The "TIMESTAMP_MICROS(bigint)" function; returns a TIMESTAMP value
     * a given number of micro-seconds after 1970-01-01 00:00:00.  */
    @LibraryOperator(libraries = [BIG_QUERY])
    val TIMESTAMP_MICROS: SqlFunction = SqlFunction(
        "TIMESTAMP_MICROS", SqlKind.OTHER_FUNCTION,
        ReturnTypes.TIMESTAMP_NULLABLE, null, OperandTypes.INTEGER,
        SqlFunctionCategory.TIMEDATE
    )

    /** The "UNIX_SECONDS(bigint)" function; returns the number of seconds
     * since 1970-01-01 00:00:00.  */
    @LibraryOperator(libraries = [BIG_QUERY])
    val UNIX_SECONDS: SqlFunction = SqlFunction(
        "UNIX_SECONDS", SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE, null, OperandTypes.TIMESTAMP,
        SqlFunctionCategory.TIMEDATE
    )

    /** The "UNIX_MILLIS(bigint)" function; returns the number of milliseconds
     * since 1970-01-01 00:00:00.  */
    @LibraryOperator(libraries = [BIG_QUERY])
    val UNIX_MILLIS: SqlFunction = SqlFunction(
        "UNIX_MILLIS", SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE, null, OperandTypes.TIMESTAMP,
        SqlFunctionCategory.TIMEDATE
    )

    /** The "UNIX_MICROS(bigint)" function; returns the number of microseconds
     * since 1970-01-01 00:00:00.  */
    @LibraryOperator(libraries = [BIG_QUERY])
    val UNIX_MICROS: SqlFunction = SqlFunction(
        "UNIX_MICROS", SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE, null, OperandTypes.TIMESTAMP,
        SqlFunctionCategory.TIMEDATE
    )

    @LibraryOperator(libraries = [ORACLE])
    val CHR: SqlFunction = SqlFunction(
        "CHR",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.CHAR,
        null,
        OperandTypes.INTEGER,
        SqlFunctionCategory.STRING
    )

    @LibraryOperator(libraries = [ORACLE])
    val TANH: SqlFunction = SqlFunction(
        "TANH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC
    )

    @LibraryOperator(libraries = [ORACLE])
    val COSH: SqlFunction = SqlFunction(
        "COSH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC
    )

    @LibraryOperator(libraries = [ORACLE])
    val SINH: SqlFunction = SqlFunction(
        "SINH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC
    )

    @LibraryOperator(libraries = [MYSQL, POSTGRESQL])
    val MD5: SqlFunction = SqlFunction(
        "MD5", SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARCHAR)
            .andThen(SqlTypeTransforms.TO_NULLABLE),
        null, OperandTypes.or(OperandTypes.STRING, OperandTypes.BINARY),
        SqlFunctionCategory.STRING
    )

    @LibraryOperator(libraries = [MYSQL, POSTGRESQL])
    val SHA1: SqlFunction = SqlFunction(
        "SHA1", SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARCHAR)
            .andThen(SqlTypeTransforms.TO_NULLABLE),
        null, OperandTypes.or(OperandTypes.STRING, OperandTypes.BINARY),
        SqlFunctionCategory.STRING
    )

    /** Infix "::" cast operator used by PostgreSQL, for example
     * `'100'::INTEGER`.  */
    @LibraryOperator(libraries = [POSTGRESQL])
    val INFIX_CAST: SqlOperator = SqlCastOperator()
}
