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
package org.apache.calcite.sql

import com.google.common.collect.Sets

/**
 * Enumerates the possible types of [SqlNode].
 *
 *
 * The values are immutable, canonical constants, so you can use Kinds to
 * find particular types of expressions quickly. To identity a call to a common
 * operator such as '=', use [org.apache.calcite.sql.SqlNode.isA]:
 *
 * <blockquote>
 * exp.[isA][org.apache.calcite.sql.SqlNode.isA]([.EQUALS])
</blockquote> *
 *
 *
 * Only commonly-used nodes have their own type; other nodes are of type
 * [.OTHER]. Some of the values, such as [.SET_QUERY], represent
 * aggregates.
 *
 *
 * To quickly choose between a number of options, use a switch statement:
 *
 * <blockquote>
 * <pre>switch (exp.getKind()) {
 * case [.EQUALS]:
 * ...;
 * case [.NOT_EQUALS]:
 * ...;
 * default:
 * throw new AssertionError("unexpected");
 * }</pre>
</blockquote> *
 *
 *
 * Note that we do not even have to check that a `SqlNode` is a
 * [SqlCall].
 *
 *
 * To identify a category of expressions, use `SqlNode.isA` with
 * an aggregate SqlKind. The following expression will return `true`
 * for calls to '=' and '&gt;=', but `false` for the constant '5', or
 * a call to '+':
 *
 * <blockquote>
 * <pre>exp.isA([SqlKind.COMPARISON][.COMPARISON])</pre>
</blockquote> *
 *
 *
 * RexNode also has a `getKind` method; `SqlKind` values are
 * preserved during translation from `SqlNode` to `RexNode`, where
 * applicable.
 *
 *
 * There is no water-tight definition of "common", but that's OK. There will
 * always be operators that don't have their own kind, and for these we use the
 * `SqlOperator`. But for really the common ones, e.g. the many places
 * where we are looking for `AND`, `OR` and `EQUALS`, the enum
 * helps.
 *
 *
 * (If we were using Scala, [SqlOperator] would be a case
 * class, and we wouldn't need `SqlKind`. But we're not.)
 */
enum class SqlKind {
    //~ Static fields/initializers ---------------------------------------------
    // the basics
    /**
     * Expression not covered by any other [SqlKind] value.
     *
     * @see .OTHER_FUNCTION
     */
    OTHER,

    /**
     * SELECT statement or sub-query.
     */
    SELECT,

    /**
     * Sql Hint statement.
     */
    HINT,

    /**
     * Table reference.
     */
    TABLE_REF,

    /**
     * JOIN operator or compound FROM clause.
     *
     *
     * A FROM clause with more than one table is represented as if it were a
     * join. For example, "FROM x, y, z" is represented as
     * "JOIN(x, JOIN(x, y))".
     */
    JOIN,

    /** An identifier.  */
    IDENTIFIER,

    /** A literal.  */
    LITERAL,

    /** Interval qualifier.  */
    INTERVAL_QUALIFIER,

    /**
     * Function that is not a special function.
     *
     * @see .FUNCTION
     */
    OTHER_FUNCTION,

    /** POSITION function.  */
    POSITION,

    /** EXPLAIN statement.  */
    EXPLAIN,

    /** DESCRIBE SCHEMA statement.  */
    DESCRIBE_SCHEMA,

    /** DESCRIBE TABLE statement.  */
    DESCRIBE_TABLE,

    /** INSERT statement.  */
    INSERT,

    /** DELETE statement.  */
    DELETE,

    /** UPDATE statement.  */
    UPDATE,

    /** "`ALTER scope SET option = value`" statement.  */
    SET_OPTION,

    /** A dynamic parameter.  */
    DYNAMIC_PARAM,

    /**
     * ORDER BY clause.
     *
     * @see .DESCENDING
     *
     * @see .NULLS_FIRST
     *
     * @see .NULLS_LAST
     */
    ORDER_BY,

    /** WITH clause.  */
    WITH,

    /** Item in WITH clause.  */
    WITH_ITEM,

    /** Item expression.  */
    ITEM,

    /** `UNION` relational operator.  */
    UNION,

    /** `EXCEPT` relational operator (known as `MINUS` in some SQL
     * dialects).  */
    EXCEPT,

    /** `INTERSECT` relational operator.  */
    INTERSECT,

    /** `AS` operator.  */
    AS,

    /** Argument assignment operator, `=>`.  */
    ARGUMENT_ASSIGNMENT,

    /** `DEFAULT` operator.  */
    DEFAULT,

    /** `OVER` operator.  */
    OVER,

    /** `RESPECT NULLS` operator.  */
    RESPECT_NULLS("RESPECT NULLS"),

    /** `IGNORE NULLS` operator.  */
    IGNORE_NULLS("IGNORE NULLS"),

    /** `FILTER` operator.  */
    FILTER,

    /** `WITHIN GROUP` operator.  */
    WITHIN_GROUP,

    /** `WITHIN DISTINCT` operator.  */
    WITHIN_DISTINCT,

    /** Window specification.  */
    WINDOW,

    /** MERGE statement.  */
    MERGE,

    /** TABLESAMPLE relational operator.  */
    TABLESAMPLE,

    /** PIVOT clause.  */
    PIVOT,

    /** UNPIVOT clause.  */
    UNPIVOT,

    /** MATCH_RECOGNIZE clause.  */
    MATCH_RECOGNIZE,

    /** SNAPSHOT operator.  */
    SNAPSHOT,  // binary operators

    /** Arithmetic multiplication operator, "*".  */
    TIMES,

    /** Arithmetic division operator, "/".  */
    DIVIDE,

    /** Arithmetic remainder operator, "MOD" (and "%" in some dialects).  */
    MOD,

    /**
     * Arithmetic plus operator, "+".
     *
     * @see .PLUS_PREFIX
     */
    PLUS,

    /**
     * Arithmetic minus operator, "-".
     *
     * @see .MINUS_PREFIX
     */
    MINUS,

    /**
     * Alternation operator in a pattern expression within a
     * `MATCH_RECOGNIZE` clause.
     */
    PATTERN_ALTER,

    /**
     * Concatenation operator in a pattern expression within a
     * `MATCH_RECOGNIZE` clause.
     */
    PATTERN_CONCAT,  // comparison operators

    /** `IN` operator.  */
    IN,

    /**
     * `NOT IN` operator.
     *
     *
     * Only occurs in SqlNode trees. Is expanded to NOT(IN ...) before
     * entering RelNode land.
     */
    NOT_IN("NOT IN"),

    /** Variant of `IN` for the Druid adapter.  */
    DRUID_IN,

    /** Variant of `NOT_IN` for the Druid adapter.  */
    DRUID_NOT_IN,

    /** Less-than operator, "&lt;".  */
    LESS_THAN("<"),

    /** Greater-than operator, "&gt;".  */
    GREATER_THAN(">"),

    /** Less-than-or-equal operator, "&lt;=".  */
    LESS_THAN_OR_EQUAL("<="),

    /** Greater-than-or-equal operator, "&gt;=".  */
    GREATER_THAN_OR_EQUAL(">="),

    /** Equals operator, "=".  */
    EQUALS("="),

    /**
     * Not-equals operator, "&#33;=" or "&lt;&gt;".
     * The latter is standard, and preferred.
     */
    NOT_EQUALS("<>"),

    /** `IS DISTINCT FROM` operator.  */
    IS_DISTINCT_FROM,

    /** `IS NOT DISTINCT FROM` operator.  */
    IS_NOT_DISTINCT_FROM,

    /** `SEARCH` operator. (Analogous to scalar `IN`, used only in
     * RexNode, not SqlNode.)  */
    SEARCH,

    /** Logical "OR" operator.  */
    OR,

    /** Logical "AND" operator.  */
    AND,  // other infix

    /** Dot.  */
    DOT,

    /** `OVERLAPS` operator for periods.  */
    OVERLAPS,

    /** `CONTAINS` operator for periods.  */
    CONTAINS,

    /** `PRECEDES` operator for periods.  */
    PRECEDES,

    /** `IMMEDIATELY PRECEDES` operator for periods.  */
    IMMEDIATELY_PRECEDES("IMMEDIATELY PRECEDES"),

    /** `SUCCEEDS` operator for periods.  */
    SUCCEEDS,

    /** `IMMEDIATELY SUCCEEDS` operator for periods.  */
    IMMEDIATELY_SUCCEEDS("IMMEDIATELY SUCCEEDS"),

    /** `EQUALS` operator for periods.  */
    PERIOD_EQUALS("EQUALS"),

    /** `LIKE` operator.  */
    LIKE,

    /** `RLIKE` operator.  */
    RLIKE,

    /** `SIMILAR` operator.  */
    SIMILAR,

    /** `~` operator (for POSIX-style regular expressions).  */
    POSIX_REGEX_CASE_SENSITIVE,

    /** `~*` operator (for case-insensitive POSIX-style regular
     * expressions).  */
    POSIX_REGEX_CASE_INSENSITIVE,

    /** `BETWEEN` operator.  */
    BETWEEN,

    /** Variant of `BETWEEN` for the Druid adapter.  */
    DRUID_BETWEEN,

    /** `CASE` expression.  */
    CASE,

    /** `INTERVAL` expression.  */
    INTERVAL,

    /** `SEPARATOR` expression.  */
    SEPARATOR,

    /** `NULLIF` operator.  */
    NULLIF,

    /** `COALESCE` operator.  */
    COALESCE,

    /** `DECODE` function (Oracle).  */
    DECODE,

    /** `NVL` function (Oracle).  */
    NVL,

    /** `GREATEST` function (Oracle).  */
    GREATEST,

    /** The two-argument `CONCAT` function (Oracle).  */
    CONCAT2,

    /** The "IF" function (BigQuery, Hive, Spark).  */
    IF,

    /** `LEAST` function (Oracle).  */
    LEAST,

    /** `TIMESTAMP_ADD` function (ODBC, SQL Server, MySQL).  */
    TIMESTAMP_ADD,

    /** `TIMESTAMP_DIFF` function (ODBC, SQL Server, MySQL).  */
    TIMESTAMP_DIFF,  // prefix operators

    /** Logical `NOT` operator.  */
    NOT,

    /**
     * Unary plus operator, as in "+1".
     *
     * @see .PLUS
     */
    PLUS_PREFIX,

    /**
     * Unary minus operator, as in "-1".
     *
     * @see .MINUS
     */
    MINUS_PREFIX,

    /** `EXISTS` operator.  */
    EXISTS,

    /** `SOME` quantification operator (also called `ANY`).  */
    SOME,

    /** `ALL` quantification operator.  */
    ALL,

    /** `VALUES` relational operator.  */
    VALUES,

    /**
     * Explicit table, e.g. `select * from (TABLE t)` or `TABLE
     * t`. See also [.COLLECTION_TABLE].
     */
    EXPLICIT_TABLE,

    /**
     * Scalar query; that is, a sub-query used in an expression context, and
     * returning one row and one column.
     */
    SCALAR_QUERY,

    /** Procedure call.  */
    PROCEDURE_CALL,

    /** New specification.  */
    NEW_SPECIFICATION,  // special functions in MATCH_RECOGNIZE

    /** `FINAL` operator in `MATCH_RECOGNIZE`.  */
    FINAL,

    /** `FINAL` operator in `MATCH_RECOGNIZE`.  */
    RUNNING,

    /** `PREV` operator in `MATCH_RECOGNIZE`.  */
    PREV,

    /** `NEXT` operator in `MATCH_RECOGNIZE`.  */
    NEXT,

    /** `FIRST` operator in `MATCH_RECOGNIZE`.  */
    FIRST,

    /** `LAST` operator in `MATCH_RECOGNIZE`.  */
    LAST,

    /** `CLASSIFIER` operator in `MATCH_RECOGNIZE`.  */
    CLASSIFIER,

    /** `MATCH_NUMBER` operator in `MATCH_RECOGNIZE`.  */
    MATCH_NUMBER,

    /** `SKIP TO FIRST` qualifier of restarting point in a
     * `MATCH_RECOGNIZE` clause.  */
    SKIP_TO_FIRST,

    /** `SKIP TO LAST` qualifier of restarting point in a
     * `MATCH_RECOGNIZE` clause.  */
    SKIP_TO_LAST,  // postfix operators

    /** `DESC` operator in `ORDER BY`. A parse tree, not a true
     * expression.  */
    DESCENDING,

    /** `NULLS FIRST` clause in `ORDER BY`. A parse tree, not a true
     * expression.  */
    NULLS_FIRST,

    /** `NULLS LAST` clause in `ORDER BY`. A parse tree, not a true
     * expression.  */
    NULLS_LAST,

    /** `IS TRUE` operator.  */
    IS_TRUE,

    /** `IS FALSE` operator.  */
    IS_FALSE,

    /** `IS NOT TRUE` operator.  */
    IS_NOT_TRUE,

    /** `IS NOT FALSE` operator.  */
    IS_NOT_FALSE,

    /** `IS UNKNOWN` operator.  */
    IS_UNKNOWN,

    /** `IS NULL` operator.  */
    IS_NULL,

    /** `IS NOT NULL` operator.  */
    IS_NOT_NULL,

    /** `PRECEDING` qualifier of an interval end-point in a window
     * specification.  */
    PRECEDING,

    /** `FOLLOWING` qualifier of an interval end-point in a window
     * specification.  */
    FOLLOWING,

    /**
     * The field access operator, ".".
     *
     *
     * (Only used at the RexNode level; at
     * SqlNode level, a field-access is part of an identifier.)
     */
    FIELD_ACCESS,

    /**
     * Reference to an input field.
     *
     *
     * (Only used at the RexNode level.)
     */
    INPUT_REF,

    /**
     * Reference to an input field, with a qualified name and an identifier.
     *
     *
     * (Only used at the RexNode level.)
     */
    TABLE_INPUT_REF,

    /**
     * Reference to an input field, with pattern var as modifier.
     *
     *
     * (Only used at the RexNode level.)
     */
    PATTERN_INPUT_REF,

    /**
     * Reference to a sub-expression computed within the current relational
     * operator.
     *
     *
     * (Only used at the RexNode level.)
     */
    LOCAL_REF,

    /**
     * Reference to correlation variable.
     *
     *
     * (Only used at the RexNode level.)
     */
    CORREL_VARIABLE,

    /**
     * the repetition quantifier of a pattern factor in a match_recognize clause.
     */
    PATTERN_QUANTIFIER,  // functions

    /**
     * The row-constructor function. May be explicit or implicit:
     * `VALUES 1, ROW (2)`.
     */
    ROW,

    /**
     * The non-standard constructor used to pass a
     * COLUMN_LIST parameter to a user-defined transform.
     */
    COLUMN_LIST,

    /**
     * The "CAST" operator, and also the PostgreSQL-style infix cast operator
     * "::".
     */
    CAST,

    /**
     * The "NEXT VALUE OF sequence" operator.
     */
    NEXT_VALUE,

    /**
     * The "CURRENT VALUE OF sequence" operator.
     */
    CURRENT_VALUE,

    /** `FLOOR` function.  */
    FLOOR,

    /** `CEIL` function.  */
    CEIL,

    /** `TRIM` function.  */
    TRIM,

    /** `LTRIM` function (Oracle).  */
    LTRIM,

    /** `RTRIM` function (Oracle).  */
    RTRIM,

    /** `EXTRACT` function.  */
    EXTRACT,

    /** `ARRAY_CONCAT` function (BigQuery semantics).  */
    ARRAY_CONCAT,

    /** `ARRAY_REVERSE` function (BigQuery semantics).  */
    ARRAY_REVERSE,

    /** `REVERSE` function (SQL Server, MySQL).  */
    REVERSE,

    /** `SUBSTR` function (BigQuery semantics).  */
    SUBSTR_BIG_QUERY,

    /** `SUBSTR` function (MySQL semantics).  */
    SUBSTR_MYSQL,

    /** `SUBSTR` function (Oracle semantics).  */
    SUBSTR_ORACLE,

    /** `SUBSTR` function (PostgreSQL semantics).  */
    SUBSTR_POSTGRESQL,

    /** Call to a function using JDBC function syntax.  */
    JDBC_FN,

    /** `MULTISET` value constructor.  */
    MULTISET_VALUE_CONSTRUCTOR,

    /** `MULTISET` query constructor.  */
    MULTISET_QUERY_CONSTRUCTOR,

    /** `JSON` value expression.  */
    JSON_VALUE_EXPRESSION,

    /** `JSON_ARRAYAGG` aggregate function.  */
    JSON_ARRAYAGG,

    /** `JSON_OBJECTAGG` aggregate function.  */
    JSON_OBJECTAGG,

    /** `UNNEST` operator.  */
    UNNEST,

    /**
     * The "LATERAL" qualifier to relations in the FROM clause.
     */
    LATERAL,

    /**
     * Table operator which converts user-defined transform into a relation, for
     * example, `select * from TABLE(udx(x, y, z))`. See also the
     * [.EXPLICIT_TABLE] prefix operator.
     */
    COLLECTION_TABLE,

    /**
     * Array Value Constructor, e.g. `Array[1, 2, 3]`.
     */
    ARRAY_VALUE_CONSTRUCTOR,

    /**
     * Array Query Constructor, e.g. `Array(select deptno from dept)`.
     */
    ARRAY_QUERY_CONSTRUCTOR,

    /** MAP value constructor, e.g. `MAP ['washington', 1, 'obama', 44]`.  */
    MAP_VALUE_CONSTRUCTOR,

    /** MAP query constructor,
     * e.g. `MAP (SELECT empno, deptno FROM emp)`.  */
    MAP_QUERY_CONSTRUCTOR,

    /** `CURSOR` constructor, for example, `SELECT * FROM
     * TABLE(udx(CURSOR(SELECT ...), x, y, z))`.  */
    CURSOR,  // internal operators (evaluated in validator) 200-299

    /**
     * Literal chain operator (for composite string literals).
     * An internal operator that does not appear in SQL syntax.
     */
    LITERAL_CHAIN,

    /**
     * Escape operator (always part of LIKE or SIMILAR TO expression).
     * An internal operator that does not appear in SQL syntax.
     */
    ESCAPE,

    /**
     * The internal REINTERPRET operator (meaning a reinterpret cast).
     * An internal operator that does not appear in SQL syntax.
     */
    REINTERPRET,

    /** The internal `EXTEND` operator that qualifies a table name in the
     * `FROM` clause.  */
    EXTEND,

    /** The internal `CUBE` operator that occurs within a `GROUP BY`
     * clause.  */
    CUBE,

    /** The internal `ROLLUP` operator that occurs within a `GROUP BY`
     * clause.  */
    ROLLUP,

    /** The internal `GROUPING SETS` operator that occurs within a
     * `GROUP BY` clause.  */
    GROUPING_SETS,

    /** The `GROUPING(e, ...)` function.  */
    GROUPING,  // CHECKSTYLE: IGNORE 1

    @Deprecated
    @Deprecated("Use {@link #GROUPING}. ") // to be removed before 2.0
    GROUPING_ID,

    /** The `GROUP_ID()` function.  */
    GROUP_ID,

    /** The internal "permute" function in a MATCH_RECOGNIZE clause.  */
    PATTERN_PERMUTE,

    /** The special patterns to exclude enclosing pattern from output in a
     * MATCH_RECOGNIZE clause.  */
    PATTERN_EXCLUDED,  // Aggregate functions

    /** The `COUNT` aggregate function.  */
    COUNT,

    /** The `SUM` aggregate function.  */
    SUM,

    /** The `SUM0` aggregate function.  */
    SUM0,

    /** The `MIN` aggregate function.  */
    MIN,

    /** The `MAX` aggregate function.  */
    MAX,

    /** The `LEAD` aggregate function.  */
    LEAD,

    /** The `LAG` aggregate function.  */
    LAG,

    /** The `FIRST_VALUE` aggregate function.  */
    FIRST_VALUE,

    /** The `LAST_VALUE` aggregate function.  */
    LAST_VALUE,

    /** The `ANY_VALUE` aggregate function.  */
    ANY_VALUE,

    /** The `COVAR_POP` aggregate function.  */
    COVAR_POP,

    /** The `COVAR_SAMP` aggregate function.  */
    COVAR_SAMP,

    /** The `REGR_COUNT` aggregate function.  */
    REGR_COUNT,

    /** The `REGR_SXX` aggregate function.  */
    REGR_SXX,

    /** The `REGR_SYY` aggregate function.  */
    REGR_SYY,

    /** The `AVG` aggregate function.  */
    AVG,

    /** The `STDDEV_POP` aggregate function.  */
    STDDEV_POP,

    /** The `STDDEV_SAMP` aggregate function.  */
    STDDEV_SAMP,

    /** The `VAR_POP` aggregate function.  */
    VAR_POP,

    /** The `VAR_SAMP` aggregate function.  */
    VAR_SAMP,

    /** The `NTILE` aggregate function.  */
    NTILE,

    /** The `NTH_VALUE` aggregate function.  */
    NTH_VALUE,

    /** The `LISTAGG` aggregate function.  */
    LISTAGG,

    /** The `STRING_AGG` aggregate function.  */
    STRING_AGG,

    /** The `COUNTIF` aggregate function.  */
    COUNTIF,

    /** The `ARRAY_AGG` aggregate function.  */
    ARRAY_AGG,

    /** The `ARRAY_CONCAT_AGG` aggregate function.  */
    ARRAY_CONCAT_AGG,

    /** The `GROUP_CONCAT` aggregate function.  */
    GROUP_CONCAT,

    /** The `COLLECT` aggregate function.  */
    COLLECT,

    /** The `MODE` aggregate function.  */
    MODE,

    /** The `PERCENTILE_CONT` aggregate function.  */
    PERCENTILE_CONT,

    /** The `PERCENTILE_DISC` aggregate function.  */
    PERCENTILE_DISC,

    /** The `FUSION` aggregate function.  */
    FUSION,

    /** The `INTERSECTION` aggregate function.  */
    INTERSECTION,

    /** The `SINGLE_VALUE` aggregate function.  */
    SINGLE_VALUE,

    /** The `BIT_AND` aggregate function.  */
    BIT_AND,

    /** The `BIT_OR` aggregate function.  */
    BIT_OR,

    /** The `BIT_XOR` aggregate function.  */
    BIT_XOR,

    /** The `ROW_NUMBER` window function.  */
    ROW_NUMBER,

    /** The `RANK` window function.  */
    RANK,

    /** The `PERCENT_RANK` window function.  */
    PERCENT_RANK,

    /** The `DENSE_RANK` window function.  */
    DENSE_RANK,

    /** The `ROW_NUMBER` window function.  */
    CUME_DIST,

    /** The `DESCRIPTOR(column_name, ...)`.  */
    DESCRIPTOR,

    /** The `TUMBLE` group function.  */
    TUMBLE,  // Group functions

    /** The `TUMBLE_START` auxiliary function of
     * the [.TUMBLE] group function.  */
    // TODO: deprecate TUMBLE_START.
    TUMBLE_START,

    /** The `TUMBLE_END` auxiliary function of
     * the [.TUMBLE] group function.  */
    // TODO: deprecate TUMBLE_END.
    TUMBLE_END,

    /** The `HOP` group function.  */
    HOP,

    /** The `HOP_START` auxiliary function of
     * the [.HOP] group function.  */
    HOP_START,

    /** The `HOP_END` auxiliary function of
     * the [.HOP] group function.  */
    HOP_END,

    /** The `SESSION` group function.  */
    SESSION,

    /** The `SESSION_START` auxiliary function of
     * the [.SESSION] group function.  */
    SESSION_START,

    /** The `SESSION_END` auxiliary function of
     * the [.SESSION] group function.  */
    SESSION_END,

    /** Column declaration.  */
    COLUMN_DECL,

    /** Attribute definition.  */
    ATTRIBUTE_DEF,

    /** `CHECK` constraint.  */
    CHECK,

    /** `UNIQUE` constraint.  */
    UNIQUE,

    /** `PRIMARY KEY` constraint.  */
    PRIMARY_KEY,

    /** `FOREIGN KEY` constraint.  */
    FOREIGN_KEY,  // Spatial functions. They are registered as "user-defined functions" but it
    // is convenient to have a "kind" so that we can quickly match them in planner
    // rules.
    /** The `ST_DWithin` geo-spatial function.  */
    ST_DWITHIN,

    /** The `ST_Point` function.  */
    ST_POINT,

    /** The `ST_Point` function that makes a 3D point.  */
    ST_POINT3,

    /** The `ST_MakeLine` function that makes a line.  */
    ST_MAKE_LINE,

    /** The `ST_Contains` function that tests whether one geometry contains
     * another.  */
    ST_CONTAINS,

    /** The `Hilbert` function that converts (x, y) to a position on a
     * Hilbert space-filling curve.  */
    HILBERT,  // DDL and session control statements follow. The list is not exhaustive: feel
    // free to add more.
    /** `COMMIT` session control statement.  */
    COMMIT,

    /** `ROLLBACK` session control statement.  */
    ROLLBACK,

    /** `ALTER SESSION` DDL statement.  */
    ALTER_SESSION,

    /** `CREATE SCHEMA` DDL statement.  */
    CREATE_SCHEMA,

    /** `CREATE FOREIGN SCHEMA` DDL statement.  */
    CREATE_FOREIGN_SCHEMA,

    /** `DROP SCHEMA` DDL statement.  */
    DROP_SCHEMA,

    /** `CREATE TABLE` DDL statement.  */
    CREATE_TABLE,

    /** `ALTER TABLE` DDL statement.  */
    ALTER_TABLE,

    /** `DROP TABLE` DDL statement.  */
    DROP_TABLE,

    /** `CREATE VIEW` DDL statement.  */
    CREATE_VIEW,

    /** `ALTER VIEW` DDL statement.  */
    ALTER_VIEW,

    /** `DROP VIEW` DDL statement.  */
    DROP_VIEW,

    /** `CREATE MATERIALIZED VIEW` DDL statement.  */
    CREATE_MATERIALIZED_VIEW,

    /** `ALTER MATERIALIZED VIEW` DDL statement.  */
    ALTER_MATERIALIZED_VIEW,

    /** `DROP MATERIALIZED VIEW` DDL statement.  */
    DROP_MATERIALIZED_VIEW,

    /** `CREATE SEQUENCE` DDL statement.  */
    CREATE_SEQUENCE,

    /** `ALTER SEQUENCE` DDL statement.  */
    ALTER_SEQUENCE,

    /** `DROP SEQUENCE` DDL statement.  */
    DROP_SEQUENCE,

    /** `CREATE INDEX` DDL statement.  */
    CREATE_INDEX,

    /** `ALTER INDEX` DDL statement.  */
    ALTER_INDEX,

    /** `DROP INDEX` DDL statement.  */
    DROP_INDEX,

    /** `CREATE TYPE` DDL statement.  */
    CREATE_TYPE,

    /** `DROP TYPE` DDL statement.  */
    DROP_TYPE,

    /** `CREATE FUNCTION` DDL statement.  */
    CREATE_FUNCTION,

    /** `DROP FUNCTION` DDL statement.  */
    DROP_FUNCTION,

    /** DDL statement not handled above.
     *
     *
     * **Note to other projects**: If you are extending Calcite's SQL parser
     * and have your own object types you no doubt want to define CREATE and DROP
     * commands for them. Use OTHER_DDL in the short term, but we are happy to add
     * new enum values for your object types. Just ask!
     */
    OTHER_DDL;

    /** Lower-case name.  */
    val lowerName: String = name().toLowerCase(Locale.ROOT)
    val sql: String

    constructor() {
        sql = name()
    }

    constructor(sql: String) {
        this.sql = sql
    }

    /** Returns the kind that corresponds to this operator but in the opposite
     * direction. Or returns this, if this kind is not reversible.
     *
     *
     * For example, `GREATER_THAN.reverse()` returns [.LESS_THAN].
     */
    fun reverse(): SqlKind {
        return when (this) {
            GREATER_THAN -> LESS_THAN
            GREATER_THAN_OR_EQUAL -> LESS_THAN_OR_EQUAL
            LESS_THAN -> GREATER_THAN
            LESS_THAN_OR_EQUAL -> GREATER_THAN_OR_EQUAL
            else -> this
        }
    }

    /** Returns the kind that you get if you apply NOT to this kind.
     *
     *
     * For example, `IS_NOT_NULL.negate()` returns [.IS_NULL].
     *
     *
     * For [.IS_TRUE], [.IS_FALSE], [.IS_NOT_TRUE],
     * [.IS_NOT_FALSE], nullable inputs need to be treated carefully.
     *
     *
     * `NOT(IS_TRUE(null))` = `NOT(false)` = `true`,
     * while `IS_FALSE(null)` = `false`,
     * so `NOT(IS_TRUE(X))` should be `IS_NOT_TRUE(X)`.
     * On the other hand,
     * `IS_TRUE(NOT(null))` = `IS_TRUE(null)` = `false`.
     *
     *
     * This is why negate() != negateNullSafe() for these operators.
     */
    fun negate(): SqlKind {
        return when (this) {
            IS_TRUE -> IS_NOT_TRUE
            IS_FALSE -> IS_NOT_FALSE
            IS_NULL -> IS_NOT_NULL
            IS_NOT_TRUE -> IS_TRUE
            IS_NOT_FALSE -> IS_FALSE
            IS_NOT_NULL -> IS_NULL
            IS_DISTINCT_FROM -> IS_NOT_DISTINCT_FROM
            IS_NOT_DISTINCT_FROM -> IS_DISTINCT_FROM
            else -> this
        }
    }

    /** Returns the kind that you get if you negate this kind.
     * To conform to null semantics, null value should not be compared.
     *
     *
     * For [.IS_TRUE], [.IS_FALSE], [.IS_NOT_TRUE] and
     * [.IS_NOT_FALSE], nullable inputs need to be treated carefully:
     *
     *
     *  * NOT(IS_TRUE(null)) = NOT(false) = true
     *  * IS_TRUE(NOT(null)) = IS_TRUE(null) = false
     *  * IS_FALSE(null) = false
     *  * IS_NOT_TRUE(null) = true
     *
     */
    fun negateNullSafe(): SqlKind {
        return when (this) {
            EQUALS -> NOT_EQUALS
            NOT_EQUALS -> EQUALS
            LESS_THAN -> GREATER_THAN_OR_EQUAL
            GREATER_THAN -> LESS_THAN_OR_EQUAL
            LESS_THAN_OR_EQUAL -> GREATER_THAN
            GREATER_THAN_OR_EQUAL -> LESS_THAN
            IN -> NOT_IN
            NOT_IN -> IN
            DRUID_IN -> DRUID_NOT_IN
            DRUID_NOT_IN -> DRUID_IN
            IS_TRUE -> IS_FALSE
            IS_FALSE -> IS_TRUE
            IS_NOT_TRUE -> IS_NOT_FALSE
            IS_NOT_FALSE -> IS_NOT_TRUE
            IS_NOT_NULL, IS_NULL -> this
            else -> negate()
        }
    }

    /**
     * Returns whether this `SqlKind` belongs to a given category.
     *
     *
     * A category is a collection of kinds, not necessarily disjoint. For
     * example, QUERY is { SELECT, UNION, INTERSECT, EXCEPT, VALUES, ORDER_BY,
     * EXPLICIT_TABLE }.
     *
     * @param category Category
     * @return Whether this kind belongs to the given category
     */
    fun belongsTo(category: Collection<SqlKind?>): Boolean {
        return category.contains(this)
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        // Most of the static fields are categories, aggregating several kinds into
        // a set.
        /**
         * Category consisting of set-query node types.
         *
         *
         * Consists of:
         * [.EXCEPT],
         * [.INTERSECT],
         * [.UNION].
         */
        val SET_QUERY: EnumSet<SqlKind> = EnumSet.of(UNION, INTERSECT, EXCEPT)

        /**
         * Category consisting of all built-in aggregate functions.
         */
        val AGGREGATE: EnumSet<SqlKind> = EnumSet.of(
            COUNT,
            SUM,
            SUM0,
            MIN,
            MAX,
            LEAD,
            LAG,
            FIRST_VALUE,
            LAST_VALUE,
            COVAR_POP,
            COVAR_SAMP,
            REGR_COUNT,
            REGR_SXX,
            REGR_SYY,
            AVG,
            STDDEV_POP,
            STDDEV_SAMP,
            VAR_POP,
            VAR_SAMP,
            NTILE,
            COLLECT,
            MODE,
            FUSION,
            SINGLE_VALUE,
            ROW_NUMBER,
            RANK,
            PERCENT_RANK,
            DENSE_RANK,
            CUME_DIST,
            JSON_ARRAYAGG,
            JSON_OBJECTAGG,
            BIT_AND,
            BIT_OR,
            BIT_XOR,
            LISTAGG,
            STRING_AGG,
            ARRAY_AGG,
            ARRAY_CONCAT_AGG,
            GROUP_CONCAT,
            COUNTIF,
            PERCENTILE_CONT,
            PERCENTILE_DISC,
            INTERSECTION,
            ANY_VALUE
        )

        /**
         * Category consisting of all DML operators.
         *
         *
         * Consists of:
         * [.INSERT],
         * [.UPDATE],
         * [.DELETE],
         * [.MERGE],
         * [.PROCEDURE_CALL].
         *
         *
         * NOTE jvs 1-June-2006: For now we treat procedure calls as DML;
         * this makes it easy for JDBC clients to call execute or
         * executeUpdate and not have to process dummy cursor results.  If
         * in the future we support procedures which return results sets,
         * we'll need to refine this.
         */
        val DML: EnumSet<SqlKind> = EnumSet.of(INSERT, DELETE, UPDATE, MERGE, PROCEDURE_CALL)

        /**
         * Category consisting of all DDL operators.
         */
        val DDL: EnumSet<SqlKind> = EnumSet.of(
            COMMIT,
            ROLLBACK,
            ALTER_SESSION,
            CREATE_SCHEMA,
            CREATE_FOREIGN_SCHEMA,
            DROP_SCHEMA,
            CREATE_TABLE,
            ALTER_TABLE,
            DROP_TABLE,
            CREATE_FUNCTION,
            DROP_FUNCTION,
            CREATE_VIEW,
            ALTER_VIEW,
            DROP_VIEW,
            CREATE_MATERIALIZED_VIEW,
            ALTER_MATERIALIZED_VIEW,
            DROP_MATERIALIZED_VIEW,
            CREATE_SEQUENCE,
            ALTER_SEQUENCE,
            DROP_SEQUENCE,
            CREATE_INDEX,
            ALTER_INDEX,
            DROP_INDEX,
            CREATE_TYPE,
            DROP_TYPE,
            SET_OPTION,
            OTHER_DDL
        )

        /**
         * Category consisting of query node types.
         *
         *
         * Consists of:
         * [.SELECT],
         * [.EXCEPT],
         * [.INTERSECT],
         * [.UNION],
         * [.VALUES],
         * [.ORDER_BY],
         * [.EXPLICIT_TABLE].
         */
        val QUERY: EnumSet<SqlKind> =
            EnumSet.of(SELECT, UNION, INTERSECT, EXCEPT, VALUES, WITH, ORDER_BY, EXPLICIT_TABLE)

        /**
         * Category consisting of all expression operators.
         *
         *
         * A node is an expression if it is NOT one of the following:
         * [.AS],
         * [.ARGUMENT_ASSIGNMENT],
         * [.DEFAULT],
         * [.DESCENDING],
         * [.SELECT],
         * [.JOIN],
         * [.OTHER_FUNCTION],
         * [.CAST],
         * [.TRIM],
         * [.LITERAL_CHAIN],
         * [.JDBC_FN],
         * [.PRECEDING],
         * [.FOLLOWING],
         * [.ORDER_BY],
         * [.COLLECTION_TABLE],
         * [.TABLESAMPLE],
         * [.UNNEST]
         * or an aggregate function, DML or DDL.
         */
        val EXPRESSION: Set<SqlKind> = EnumSet.complementOf(
            concat(
                EnumSet.of(
                    AS,
                    ARGUMENT_ASSIGNMENT,
                    DEFAULT,
                    RUNNING,
                    FINAL,
                    LAST,
                    FIRST,
                    PREV,
                    NEXT,
                    FILTER,
                    WITHIN_GROUP,
                    IGNORE_NULLS,
                    RESPECT_NULLS,
                    SEPARATOR,
                    DESCENDING,
                    CUBE,
                    ROLLUP,
                    GROUPING_SETS,
                    EXTEND,
                    LATERAL,
                    SELECT,
                    JOIN,
                    OTHER_FUNCTION,
                    POSITION,
                    CAST,
                    TRIM,
                    FLOOR,
                    CEIL,
                    TIMESTAMP_ADD,
                    TIMESTAMP_DIFF,
                    EXTRACT,
                    INTERVAL,
                    LITERAL_CHAIN,
                    JDBC_FN,
                    PRECEDING,
                    FOLLOWING,
                    ORDER_BY,
                    NULLS_FIRST,
                    NULLS_LAST,
                    COLLECTION_TABLE,
                    TABLESAMPLE,
                    VALUES,
                    WITH,
                    WITH_ITEM,
                    ITEM,
                    SKIP_TO_FIRST,
                    SKIP_TO_LAST,
                    JSON_VALUE_EXPRESSION,
                    UNNEST
                ),
                AGGREGATE, DML, DDL
            )
        )

        /**
         * Category of all SQL statement types.
         *
         *
         * Consists of all types in [.QUERY], [.DML] and [.DDL].
         */
        val TOP_LEVEL: EnumSet<SqlKind> = concat<Enum<E>>(QUERY, DML, DDL)

        /**
         * Category consisting of regular and special functions.
         *
         *
         * Consists of regular functions [.OTHER_FUNCTION] and special
         * functions [.ROW], [.TRIM], [.CAST], [.REVERSE], [.JDBC_FN].
         */
        val FUNCTION: Set<SqlKind> =
            EnumSet.of(OTHER_FUNCTION, ROW, TRIM, LTRIM, RTRIM, CAST, REVERSE, JDBC_FN, POSITION)

        /**
         * Category of SqlAvgAggFunction.
         *
         *
         * Consists of [.AVG], [.STDDEV_POP], [.STDDEV_SAMP],
         * [.VAR_POP], [.VAR_SAMP].
         */
        val AVG_AGG_FUNCTIONS: Set<SqlKind> = EnumSet.of(AVG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP)

        /**
         * Category of SqlCovarAggFunction.
         *
         *
         * Consists of [.COVAR_POP], [.COVAR_SAMP], [.REGR_SXX],
         * [.REGR_SYY].
         */
        val COVAR_AVG_AGG_FUNCTIONS: Set<SqlKind> = EnumSet.of(COVAR_POP, COVAR_SAMP, REGR_COUNT, REGR_SXX, REGR_SYY)

        /**
         * Category of comparison operators.
         *
         *
         * Consists of:
         * [.IN],
         * [.EQUALS],
         * [.NOT_EQUALS],
         * [.LESS_THAN],
         * [.GREATER_THAN],
         * [.LESS_THAN_OR_EQUAL],
         * [.GREATER_THAN_OR_EQUAL].
         */
        val COMPARISON: Set<SqlKind> =
            EnumSet.of(IN, EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL)

        /**
         * Category of binary arithmetic.
         *
         *
         * Consists of:
         * [.PLUS]
         * [.MINUS]
         * [.TIMES]
         * [.DIVIDE]
         * [.MOD].
         */
        val BINARY_ARITHMETIC: Set<SqlKind> = EnumSet.of(PLUS, MINUS, TIMES, DIVIDE, MOD)

        /**
         * Category of binary equality.
         *
         *
         * Consists of:
         * [.EQUALS]
         * [.NOT_EQUALS]
         */
        val BINARY_EQUALITY: Set<SqlKind> = EnumSet.of(EQUALS, NOT_EQUALS)

        /**
         * Category of binary comparison.
         *
         *
         * Consists of:
         * [.EQUALS]
         * [.NOT_EQUALS]
         * [.GREATER_THAN]
         * [.GREATER_THAN_OR_EQUAL]
         * [.LESS_THAN]
         * [.LESS_THAN_OR_EQUAL]
         * [.IS_DISTINCT_FROM]
         * [.IS_NOT_DISTINCT_FROM]
         */
        val BINARY_COMPARISON: Set<SqlKind> = EnumSet.of(
            EQUALS,
            NOT_EQUALS,
            GREATER_THAN,
            GREATER_THAN_OR_EQUAL,
            LESS_THAN,
            LESS_THAN_OR_EQUAL,
            IS_DISTINCT_FROM,
            IS_NOT_DISTINCT_FROM
        )

        /**
         * Category of operators that do not depend on the argument order.
         *
         *
         * For instance: [.AND], [.OR], [.EQUALS], [.LEAST]
         *
         * Note: [.PLUS] does depend on the argument oder if argument types are different
         */
        @API(since = "1.22", status = API.Status.EXPERIMENTAL)
        val SYMMETRICAL: Set<SqlKind> =
            EnumSet.of(AND, OR, EQUALS, NOT_EQUALS, IS_DISTINCT_FROM, IS_NOT_DISTINCT_FROM, GREATEST, LEAST)

        /**
         * Category of operators that do not depend on the argument order if argument types are equal.
         *
         *
         * For instance: [.PLUS], [.TIMES]
         */
        @API(since = "1.22", status = API.Status.EXPERIMENTAL)
        val SYMMETRICAL_SAME_ARG_TYPE: Set<SqlKind> = EnumSet.of(PLUS, TIMES)

        /**
         * Simple binary operators are those operators which expects operands from the same Domain.
         *
         *
         * Example: simple comparisons (`=`, `<`).
         *
         *
         * Note: it does not contain `IN` because that is defined on D x D^n.
         */
        @API(since = "1.24", status = API.Status.EXPERIMENTAL)
        val SIMPLE_BINARY_OPS: Set<SqlKind>? = null

        init {
            val kinds: EnumSet<SqlKind> = EnumSet.copyOf(BINARY_ARITHMETIC)
            org.apache.calcite.sql.kinds.remove(MOD)
            org.apache.calcite.sql.kinds.addAll(BINARY_COMPARISON)
            SIMPLE_BINARY_OPS = Sets.immutableEnumSet(org.apache.calcite.sql.kinds)
        }

        @SafeVarargs
        private fun <E : Enum<E>?> concat(
            set0: EnumSet<E>,
            vararg sets: EnumSet<E>
        ): EnumSet<E> {
            val set: EnumSet<E> = set0.clone()
            for (s in sets) {
                set.addAll(s)
            }
            return set
        }
    }
}
