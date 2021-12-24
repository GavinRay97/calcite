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
package org.apache.calcite.sql.validate

import org.apache.calcite.sql.`fun`.SqlLibrary

/**
 * Enumeration of valid SQL compatibility modes.
 *
 *
 * For most purposes, one of the built-in compatibility modes in enum
 * [SqlConformanceEnum] will suffice.
 *
 *
 * If you wish to implement this interface to build your own conformance,
 * we strongly recommend that you extend [SqlAbstractConformance],
 * or use a [SqlDelegatingConformance],
 * so that you won't be broken by future changes.
 *
 * @see SqlConformanceEnum
 *
 * @see SqlAbstractConformance
 *
 * @see SqlDelegatingConformance
 */
interface SqlConformance {
    /**
     * Whether this dialect supports features from a wide variety of
     * dialects. This is enabled for the Babel parser, disabled otherwise.
     */
    val isLiberal: Boolean

    /**
     * Whether this dialect allows character literals as column aliases.
     *
     *
     * For example,
     *
     * <blockquote><pre>
     * SELECT empno, sal + comm AS 'remuneration'
     * FROM Emp</pre></blockquote>
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.BABEL],
     * [SqlConformanceEnum.BIG_QUERY],
     * [SqlConformanceEnum.LENIENT],
     * [SqlConformanceEnum.MYSQL_5],
     * [SqlConformanceEnum.SQL_SERVER_2008];
     * false otherwise.
     */
    fun allowCharLiteralAlias(): Boolean

    /**
     * Whether to allow aliases from the `SELECT` clause to be used as
     * column names in the `GROUP BY` clause.
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.BABEL],
     * [SqlConformanceEnum.BIG_QUERY],
     * [SqlConformanceEnum.LENIENT],
     * [SqlConformanceEnum.MYSQL_5];
     * false otherwise.
     */
    val isGroupByAlias: Boolean

    /**
     * Whether `GROUP BY 2` is interpreted to mean 'group by the 2nd column
     * in the select list'.
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.BABEL],
     * [SqlConformanceEnum.BIG_QUERY],
     * [SqlConformanceEnum.LENIENT],
     * [SqlConformanceEnum.MYSQL_5],
     * [SqlConformanceEnum.PRESTO];
     * false otherwise.
     */
    val isGroupByOrdinal: Boolean

    /**
     * Whether to allow aliases from the `SELECT` clause to be used as
     * column names in the `HAVING` clause.
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.BABEL],
     * [SqlConformanceEnum.BIG_QUERY],
     * [SqlConformanceEnum.LENIENT],
     * [SqlConformanceEnum.MYSQL_5];
     * false otherwise.
     */
    val isHavingAlias: Boolean

    /**
     * Whether '`ORDER BY 2`' is interpreted to mean 'sort by the 2nd
     * column in the select list'.
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.DEFAULT],
     * [SqlConformanceEnum.BABEL],
     * [SqlConformanceEnum.LENIENT],
     * [SqlConformanceEnum.MYSQL_5],
     * [SqlConformanceEnum.ORACLE_10],
     * [SqlConformanceEnum.ORACLE_12],
     * [SqlConformanceEnum.PRAGMATIC_99],
     * [SqlConformanceEnum.PRAGMATIC_2003],
     * [SqlConformanceEnum.PRESTO],
     * [SqlConformanceEnum.SQL_SERVER_2008],
     * [SqlConformanceEnum.STRICT_92];
     * false otherwise.
     */
    val isSortByOrdinal: Boolean

    /**
     * Whether '`ORDER BY x`' is interpreted to mean 'sort by the select
     * list item whose alias is x' even if there is a column called x.
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.DEFAULT],
     * [SqlConformanceEnum.BABEL],
     * [SqlConformanceEnum.LENIENT],
     * [SqlConformanceEnum.MYSQL_5],
     * [SqlConformanceEnum.ORACLE_10],
     * [SqlConformanceEnum.ORACLE_12],
     * [SqlConformanceEnum.SQL_SERVER_2008],
     * [SqlConformanceEnum.STRICT_92];
     * false otherwise.
     */
    val isSortByAlias: Boolean

    /**
     * Whether "empno" is invalid in "select empno as x from emp order by empno"
     * because the alias "x" obscures it.
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.STRICT_92];
     * false otherwise.
     */
    val isSortByAliasObscures: Boolean

    /**
     * Whether `FROM` clause is required in a `SELECT` statement.
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.ORACLE_10],
     * [SqlConformanceEnum.ORACLE_12],
     * [SqlConformanceEnum.STRICT_92],
     * [SqlConformanceEnum.STRICT_99],
     * [SqlConformanceEnum.STRICT_2003];
     * false otherwise.
     */
    val isFromRequired: Boolean

    /**
     * Whether to split a quoted table name. If true, `` `x.y.z` `` is parsed as
     * if the user had written `` `x`.`y`.`z` ``.
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.BIG_QUERY];
     * false otherwise.
     */
    fun splitQuotedTableName(): Boolean

    /**
     * Whether to allow hyphens in an unquoted table name.
     *
     *
     * If true, `SELECT * FROM foo-bar.baz-buzz` is valid, and is parsed
     * as if the user had written `SELECT * FROM `foo-bar`.`baz-buzz``.
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.BIG_QUERY];
     * false otherwise.
     */
    fun allowHyphenInUnquotedTableName(): Boolean

    /**
     * Whether the bang-equal token != is allowed as an alternative to &lt;&gt; in
     * the parser.
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.BABEL],
     * [SqlConformanceEnum.LENIENT],
     * [SqlConformanceEnum.MYSQL_5],
     * [SqlConformanceEnum.ORACLE_10],
     * [SqlConformanceEnum.ORACLE_12],
     * [SqlConformanceEnum.PRESTO];
     * false otherwise.
     */
    val isBangEqualAllowed: Boolean

    /**
     * Whether the "%" operator is allowed by the parser as an alternative to the
     * `mod` function.
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.BABEL],
     * [SqlConformanceEnum.LENIENT],
     * [SqlConformanceEnum.MYSQL_5],
     * [SqlConformanceEnum.PRESTO];
     * false otherwise.
     */
    val isPercentRemainderAllowed: Boolean

    /**
     * Whether `MINUS` is allowed as an alternative to `EXCEPT` in
     * the parser.
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.BABEL],
     * [SqlConformanceEnum.LENIENT],
     * [SqlConformanceEnum.ORACLE_10],
     * [SqlConformanceEnum.ORACLE_12];
     * false otherwise.
     *
     *
     * Note: MySQL does not support `MINUS` or `EXCEPT` (as of
     * version 5.5).
     */
    val isMinusAllowed: Boolean

    /**
     * Whether `CROSS APPLY` and `OUTER APPLY` operators are allowed
     * in the parser.
     *
     *
     * `APPLY` invokes a table-valued function for each row returned
     * by a table expression. It is syntactic sugar:
     *
     *  * `SELECT * FROM emp CROSS APPLY TABLE(promote(empno)`<br></br>
     * is equivalent to<br></br>
     * `SELECT * FROM emp CROSS JOIN LATERAL TABLE(promote(empno)`
     *
     *  * `SELECT * FROM emp OUTER APPLY TABLE(promote(empno)`<br></br>
     * is equivalent to<br></br>
     * `SELECT * FROM emp LEFT JOIN LATERAL TABLE(promote(empno)` ON true
     *
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.BABEL],
     * [SqlConformanceEnum.LENIENT],
     * [SqlConformanceEnum.ORACLE_12],
     * [SqlConformanceEnum.SQL_SERVER_2008];
     * false otherwise.
     */
    val isApplyAllowed: Boolean

    /**
     * Whether to allow `INSERT` (or `UPSERT`) with no column list
     * but fewer values than the target table.
     *
     *
     * The N values provided are assumed to match the first N columns of the
     * table, and for each of the remaining columns, the default value of the
     * column is used. It is an error if any of these columns has no default
     * value.
     *
     *
     * The default value of a column is specified by the `DEFAULT`
     * clause in the `CREATE TABLE` statement, or is `NULL` if the
     * column is not declared `NOT NULL`.
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.BABEL],
     * [SqlConformanceEnum.LENIENT],
     * [SqlConformanceEnum.PRAGMATIC_99],
     * [SqlConformanceEnum.PRAGMATIC_2003];
     * false otherwise.
     */
    val isInsertSubsetColumnsAllowed: Boolean

    /**
     * Whether directly alias array items in UNNEST.
     *
     *
     * E.g. in UNNEST(a_array, b_array) AS T(a, b),
     * a and b will be aliases of elements in a_array and b_array
     * respectively.
     *
     *
     * Without this flag set, T will be the alias
     * of the element in a_array and a, b will be the top level
     * fields of T if T is a STRUCT type.
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.PRESTO];
     * false otherwise.
     */
    fun allowAliasUnnestItems(): Boolean

    /**
     * Whether to allow parentheses to be specified in calls to niladic functions
     * and procedures (that is, functions and procedures with no parameters).
     *
     *
     * For example, `CURRENT_DATE` is a niladic system function. In
     * standard SQL it must be invoked without parentheses:
     *
     * <blockquote>`VALUES CURRENT_DATE`</blockquote>
     *
     *
     * If `allowNiladicParentheses`, the following syntax is also valid:
     *
     * <blockquote>`VALUES CURRENT_DATE()`</blockquote>
     *
     *
     * Of the popular databases, MySQL, Apache Phoenix and VoltDB allow this
     * behavior;
     * Apache Hive, HSQLDB, IBM DB2, Microsoft SQL Server, Oracle, PostgreSQL do
     * not.
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.BABEL],
     * [SqlConformanceEnum.LENIENT],
     * [SqlConformanceEnum.MYSQL_5];
     * false otherwise.
     */
    fun allowNiladicParentheses(): Boolean

    /**
     * Whether to allow SQL syntax "`ROW(expr1, expr2, expr3)`".
     *
     * The equivalent syntax in standard SQL is
     * "`(expr1, expr2, expr3)`".
     *
     *
     * Standard SQL does not allow this because the type is not
     * well-defined. However, PostgreSQL allows this behavior.
     *
     *
     * Standard SQL allows row expressions in other contexts, for instance
     * inside `VALUES` clause.
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.DEFAULT],
     * [SqlConformanceEnum.LENIENT],
     * [SqlConformanceEnum.PRESTO];
     * false otherwise.
     */
    fun allowExplicitRowValueConstructor(): Boolean

    /**
     * Whether to allow mixing table columns with extended columns in
     * `INSERT` (or `UPSERT`).
     *
     *
     * For example, suppose that the declaration of table `T` has columns
     * `A` and `B`, and you want to insert data of column
     * `C INTEGER` not present in the table declaration as an extended
     * column. You can specify the columns in an `INSERT` statement as
     * follows:
     *
     * <blockquote>
     * `INSERT INTO T (A, B, C INTEGER) VALUES (1, 2, 3)`
    </blockquote> *
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.BABEL],
     * [SqlConformanceEnum.LENIENT];
     * false otherwise.
     */
    fun allowExtend(): Boolean

    /**
     * Whether to allow the SQL syntax "`LIMIT start, count`".
     *
     *
     * The equivalent syntax in standard SQL is
     * "`OFFSET start ROW FETCH FIRST count ROWS ONLY`",
     * and in PostgreSQL "`LIMIT count OFFSET start`".
     *
     *
     * MySQL and CUBRID allow this behavior.
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.BABEL],
     * [SqlConformanceEnum.LENIENT],
     * [SqlConformanceEnum.MYSQL_5];
     * false otherwise.
     */
    val isLimitStartCountAllowed: Boolean

    /**
     * Whether to allow geo-spatial extensions, including the GEOMETRY type.
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.BABEL],
     * [SqlConformanceEnum.LENIENT],
     * [SqlConformanceEnum.MYSQL_5],
     * [SqlConformanceEnum.PRESTO],
     * [SqlConformanceEnum.SQL_SERVER_2008];
     * false otherwise.
     */
    fun allowGeometry(): Boolean

    /**
     * Whether the least restrictive type of a number of CHAR types of different
     * lengths should be a VARCHAR type. And similarly BINARY to VARBINARY.
     *
     *
     * For example, consider the query
     *
     * <blockquote><pre>SELECT 'abcde' UNION SELECT 'xyz'</pre></blockquote>
     *
     *
     * The input columns have types `CHAR(5)` and `CHAR(3)`, and
     * we need a result type that is large enough for both:
     *
     *  * Under strict SQL:2003 behavior, its column has type `CHAR(5)`,
     * and the value in the second row will have trailing spaces.
     *  * With lenient behavior, its column has type `VARCHAR(5)`, and the
     * values have no trailing spaces.
     *
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.PRAGMATIC_99],
     * [SqlConformanceEnum.PRAGMATIC_2003],
     * [SqlConformanceEnum.MYSQL_5],
     * [SqlConformanceEnum.ORACLE_10],
     * [SqlConformanceEnum.ORACLE_12],
     * [SqlConformanceEnum.PRESTO],
     * [SqlConformanceEnum.SQL_SERVER_2008];
     * false otherwise.
     */
    fun shouldConvertRaggedUnionTypesToVarying(): Boolean

    /**
     * Whether TRIM should support more than one trim character.
     *
     *
     * For example, consider the query
     *
     * <blockquote><pre>SELECT TRIM('eh' FROM 'hehe__hehe')</pre></blockquote>
     *
     *
     * Under strict behavior, if the length of trim character is not 1,
     * TRIM throws an exception, and the query fails.
     * However many implementations (in databases such as MySQL and SQL Server)
     * trim all the characters, resulting in a return value of '__'.
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.BABEL],
     * [SqlConformanceEnum.LENIENT],
     * [SqlConformanceEnum.MYSQL_5],
     * [SqlConformanceEnum.SQL_SERVER_2008];
     * false otherwise.
     */
    fun allowExtendedTrim(): Boolean

    /**
     * Whether interval literals should allow plural time units
     * such as "YEARS" and "DAYS" in interval literals.
     *
     *
     * Under strict behavior, `INTERVAL '2' DAY` is valid
     * and `INTERVAL '2' DAYS` is invalid;
     * PostgreSQL allows both; Oracle only allows singular time units.
     *
     *
     * Among the built-in conformance levels, true in
     * [SqlConformanceEnum.BABEL],
     * [SqlConformanceEnum.LENIENT];
     * false otherwise.
     */
    fun allowPluralTimeUnits(): Boolean

    /**
     * Whether to allow a qualified common column in a query that has a
     * NATURAL join or a join with a USING clause.
     *
     *
     * For example, in the query
     *
     * <blockquote><pre>SELECT emp.deptno
     * FROM emp
     * JOIN dept USING (deptno)</pre></blockquote>
     *
     *
     * `deptno` is the common column. A qualified common column
     * such as `emp.deptno` is not allowed in Oracle, but is allowed
     * in PostgreSQL.
     *
     *
     * Among the built-in conformance levels, false in
     * [SqlConformanceEnum.ORACLE_10],
     * [SqlConformanceEnum.ORACLE_12],
     * [SqlConformanceEnum.PRESTO],
     * [SqlConformanceEnum.STRICT_92],
     * [SqlConformanceEnum.STRICT_99],
     * [SqlConformanceEnum.STRICT_2003];
     * true otherwise.
     */
    fun allowQualifyingCommonColumn(): Boolean

    /**
     * Controls the behavior of operators that are part of Standard SQL but
     * nevertheless have different behavior in different databases.
     *
     *
     * Consider the `SUBSTRING` operator. In ISO standard SQL, negative
     * start indexes are converted to 1; in Google BigQuery, negative start
     * indexes are treated as offsets from the end of the string. For example,
     * `SUBSTRING('abcde' FROM -3 FOR 2)` returns `'ab'` in standard
     * SQL and 'cd' in BigQuery.
     *
     *
     * If you specify `conformance=BIG_QUERY` in your connection
     * parameters, `SUBSTRING` will give the BigQuery behavior. Similarly
     * MySQL and Oracle.
     *
     *
     * Among the built-in conformance levels:
     *
     *  * [SqlConformanceEnum.BIG_QUERY] returns
     * [SqlLibrary.BIG_QUERY];
     *  * [SqlConformanceEnum.MYSQL_5] returns [SqlLibrary.MYSQL];
     *  * [SqlConformanceEnum.ORACLE_10] and
     * [SqlConformanceEnum.ORACLE_12] return [SqlLibrary.ORACLE];
     *  * otherwise returns [SqlLibrary.STANDARD].
     *
     */
    fun semantics(): SqlLibrary

    companion object {
        /** Short-cut for [SqlConformanceEnum.DEFAULT].  */
        @SuppressWarnings("unused")
        @Deprecated
        val DEFAULT: // to be removed before 2.0
                SqlConformanceEnum = SqlConformanceEnum.DEFAULT

        /** Short-cut for [SqlConformanceEnum.STRICT_92].  */
        @SuppressWarnings("unused")
        @Deprecated
        val STRICT_92: // to be removed before 2.0
                SqlConformanceEnum = SqlConformanceEnum.STRICT_92

        /** Short-cut for [SqlConformanceEnum.STRICT_99].  */
        @SuppressWarnings("unused")
        @Deprecated
        val STRICT_99: // to be removed before 2.0
                SqlConformanceEnum = SqlConformanceEnum.STRICT_99

        /** Short-cut for [SqlConformanceEnum.PRAGMATIC_99].  */
        @SuppressWarnings("unused")
        @Deprecated
        val PRAGMATIC_99: // to be removed before 2.0
                SqlConformanceEnum = SqlConformanceEnum.PRAGMATIC_99

        /** Short-cut for [SqlConformanceEnum.ORACLE_10].  */
        @SuppressWarnings("unused")
        @Deprecated
        val ORACLE_10: // to be removed before 2.0
                SqlConformanceEnum = SqlConformanceEnum.ORACLE_10

        /** Short-cut for [SqlConformanceEnum.STRICT_2003].  */
        @SuppressWarnings("unused")
        @Deprecated
        val STRICT_2003: // to be removed before 2.0
                SqlConformanceEnum = SqlConformanceEnum.STRICT_2003

        /** Short-cut for [SqlConformanceEnum.PRAGMATIC_2003].  */
        @SuppressWarnings("unused")
        @Deprecated
        val PRAGMATIC_2003: // to be removed before 2.0
                SqlConformanceEnum = SqlConformanceEnum.PRAGMATIC_2003
    }
}
