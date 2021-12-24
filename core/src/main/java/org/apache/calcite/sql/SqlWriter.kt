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

import org.apache.calcite.sql.`fun`.SqlStdOperatorTable

/**
 * A `SqlWriter` is the target to construct a SQL statement from a
 * parse tree. It deals with dialect differences; for example, Oracle quotes
 * identifiers as `"scott"`, while SQL Server quotes them as `
 * [scott]`.
 */
interface SqlWriter {
    //~ Enums ------------------------------------------------------------------
    /**
     * Style of formatting sub-queries.
     */
    enum class SubQueryStyle {
        /**
         * Julian's style of sub-query nesting. Like this:
         *
         * <blockquote><pre>SELECT *
         * FROM (
         * SELECT *
         * FROM t
         * )
         * WHERE condition</pre></blockquote>
         */
        HYDE,

        /**
         * Damian's style of sub-query nesting. Like this:
         *
         * <blockquote><pre>SELECT *
         * FROM
         * (   SELECT *
         * FROM t
         * )
         * WHERE condition</pre></blockquote>
         */
        BLACK
    }

    /**
     * Enumerates the types of frame.
     */
    enum class FrameTypeEnum
    /**
     * Creates a list type.
     */ @JvmOverloads constructor(private val needsIndent: Boolean = true) : FrameType {
        /**
         * SELECT query (or UPDATE or DELETE). The items in the list are the
         * clauses: FROM, WHERE, etc.
         */
        SELECT,

        /**
         * Simple list.
         */
        SIMPLE,

        /**
         * Comma-separated list surrounded by parentheses.
         * The parentheses are present even if the list is empty.
         */
        PARENTHESES,

        /**
         * The SELECT clause of a SELECT statement.
         */
        SELECT_LIST,

        /**
         * The WINDOW clause of a SELECT statement.
         */
        WINDOW_DECL_LIST,

        /**
         * The SET clause of an UPDATE statement.
         */
        UPDATE_SET_LIST,

        /**
         * Function declaration.
         */
        FUN_DECL,

        /**
         * Function call or datatype declaration.
         *
         *
         * Examples:
         *
         *  * `SUBSTRING('foobar' FROM 1 + 2 TO 4)`
         *  * `DECIMAL(10, 5)`
         *
         */
        FUN_CALL,

        /**
         * Window specification.
         *
         *
         * Examples:
         *
         *  * `SUM(x) OVER (ORDER BY hireDate ROWS 3 PRECEDING)`
         *  * `WINDOW w1 AS (ORDER BY hireDate), w2 AS (w1 PARTITION BY gender
         * RANGE BETWEEN INTERVAL '1' YEAR PRECEDING AND '2' MONTH
         * PRECEDING)`
         *
         */
        WINDOW,

        /**
         * ORDER BY clause of a SELECT statement. The "list" has only two items:
         * the query and the order by clause, with ORDER BY as the separator.
         */
        ORDER_BY,

        /**
         * ORDER BY list.
         *
         *
         * Example:
         *
         *  * `ORDER BY x, y DESC, z`
         *
         */
        ORDER_BY_LIST,

        /**
         * WITH clause of a SELECT statement. The "list" has only two items:
         * the WITH clause and the query, with AS as the separator.
         */
        WITH,

        /**
         * OFFSET clause.
         *
         *
         * Example:
         *
         *  * `OFFSET 10 ROWS`
         *
         */
        OFFSET,

        /**
         * FETCH clause.
         *
         *
         * Example:
         *
         *  * `FETCH FIRST 3 ROWS ONLY`
         *
         */
        FETCH,

        /**
         * GROUP BY list.
         *
         *
         * Example:
         *
         *  * `GROUP BY x, FLOOR(y)`
         *
         */
        GROUP_BY_LIST,

        /**
         * Sub-query list. Encloses a SELECT, UNION, EXCEPT, INTERSECT query
         * with optional ORDER BY.
         *
         *
         * Example:
         *
         *  * `GROUP BY x, FLOOR(y)`
         *
         */
        SUB_QUERY(true),

        /**
         * Set operation.
         *
         *
         * Example:
         *
         *  * `SELECT * FROM a UNION SELECT * FROM b`
         *
         */
        SETOP,

        /**
         * VALUES clause.
         *
         *
         * Example:
         *
         * <blockquote><pre>VALUES (1, 'a'),
         * (2, 'b')</pre></blockquote>
         */
        VALUES,

        /**
         * FROM clause (containing various kinds of JOIN).
         */
        FROM_LIST,

        /**
         * Pair-wise join.
         */
        JOIN(false),

        /**
         * WHERE clause.
         */
        WHERE_LIST,

        /**
         * Compound identifier.
         *
         *
         * Example:
         *
         *  * `"A"."B"."C"`
         *
         */
        IDENTIFIER(false),

        /**
         * Alias ("AS"). No indent.
         */
        AS(false),

        /**
         * CASE expression.
         */
        CASE,

        /**
         * Same behavior as user-defined frame type.
         */
        OTHER;

        /**
         * Creates a list type.
         */
        @Override
        override fun needsIndent(): Boolean {
            return needsIndent
        }

        @get:Override
        override val name: String?
            get() = field()

        companion object {
            /**
             * Creates a frame type.
             *
             * @param name Name
             * @return frame type
             */
            fun create(name: String): FrameType {
                return object : FrameType {
                    @get:Override
                    override val name: String?
                        get() = name

                    @Override
                    override fun needsIndent(): Boolean {
                        return true
                    }
                }
            }
        }
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Resets this writer so that it can format another expression. Does not
     * affect formatting preferences (see [.resetSettings]
     */
    fun reset()

    /**
     * Resets all properties to their default values.
     */
    fun resetSettings()

    /**
     * Returns the dialect of SQL.
     *
     * @return SQL dialect
     */
    val dialect: SqlDialect

    /**
     * Returns the contents of this writer as a 'certified kocher' SQL string.
     *
     * @return SQL string
     */
    fun toSqlString(): SqlString?

    /**
     * Prints a literal, exactly as provided. Does not attempt to indent or
     * convert to upper or lower case. Does not add quotation marks. Adds
     * preceding whitespace if necessary.
     */
    @Pure
    fun literal(s: String?)

    /**
     * Prints a sequence of keywords. Must not start or end with space, but may
     * contain a space. For example, `keyword("SELECT")`, `
     * keyword("CHARACTER SET")`.
     */
    @Pure
    fun keyword(s: String?)

    /**
     * Prints a string, preceded by whitespace if necessary.
     */
    @Pure
    fun print(s: String?)

    /**
     * Prints an integer.
     *
     * @param x Integer
     */
    @Pure
    fun print(x: Int)

    /**
     * Prints an identifier, quoting as necessary.
     *
     * @param name   The identifier name
     * @param quoted Whether this identifier was quoted in the original sql statement,
     * this may not be the only factor to decide whether this identifier
     * should be quoted
     */
    fun identifier(name: String?, quoted: Boolean)

    /**
     * Prints a dynamic parameter (e.g. `?` for default JDBC)
     */
    fun dynamicParam(index: Int)

    /**
     * Prints the OFFSET/FETCH clause.
     */
    fun fetchOffset(@Nullable fetch: SqlNode?, @Nullable offset: SqlNode?)

    /**
     * Prints the TOP(n) clause.
     *
     * @see .fetchOffset
     */
    fun topN(@Nullable fetch: SqlNode?, @Nullable offset: SqlNode?)

    /**
     * Prints a new line, and indents.
     */
    fun newlineAndIndent()

    /**
     * Returns whether this writer should quote all identifiers, even those
     * that do not contain mixed-case identifiers or punctuation.
     *
     * @return whether to quote all identifiers
     */
    val isQuoteAllIdentifiers: Boolean

    /**
     * Returns whether this writer should start each clause (e.g. GROUP BY) on
     * a new line.
     *
     * @return whether to start each clause on a new line
     */
    val isClauseStartsLine: Boolean

    /**
     * Returns whether the items in the SELECT clause should each be on a
     * separate line.
     *
     * @return whether to put each SELECT clause item on a new line
     */
    val isSelectListItemsOnSeparateLines: Boolean

    /**
     * Returns whether to output all keywords (e.g. SELECT, GROUP BY) in lower
     * case.
     *
     * @return whether to output SQL keywords in lower case
     */
    val isKeywordsLowerCase: Boolean

    /**
     * Starts a list which is a call to a function.
     *
     * @see .endFunCall
     */
    @Pure
    fun startFunCall(funName: String?): Frame?

    /**
     * Ends a list which is a call to a function.
     *
     * @param frame Frame
     * @see .startFunCall
     */
    @Pure
    fun endFunCall(frame: Frame?)

    /**
     * Starts a list.
     */
    @Pure
    fun startList(open: String?, close: String?): Frame

    /**
     * Starts a list with no opening string.
     *
     * @param frameType Type of list. For example, a SELECT list will be
     * governed according to SELECT-list formatting preferences.
     */
    @Pure
    fun startList(frameType: FrameTypeEnum?): Frame

    /**
     * Starts a list.
     *
     * @param frameType Type of list. For example, a SELECT list will be
     * governed according to SELECT-list formatting preferences.
     * @param open      String to start the list; typically "(" or the empty
     * string.
     * @param close     String to close the list
     */
    @Pure
    fun startList(frameType: FrameType?, open: String?, close: String?): Frame

    /**
     * Ends a list.
     *
     * @param frame The frame which was created by [.startList].
     */
    @Pure
    fun endList(@Nullable frame: Frame?)

    /**
     * Writes a list.
     */
    @Pure
    fun list(frameType: FrameTypeEnum?, action: Consumer<SqlWriter?>?): SqlWriter?

    /**
     * Writes a list separated by a binary operator
     * ([AND][SqlStdOperatorTable.AND],
     * [OR][SqlStdOperatorTable.OR], or
     * [COMMA][.COMMA]).
     */
    @Pure
    fun list(
        frameType: FrameTypeEnum?, sepOp: SqlBinaryOperator?,
        list: SqlNodeList?
    ): SqlWriter?

    /**
     * Writes a list separator, unless the separator is "," and this is the
     * first occurrence in the list.
     *
     * @param sep List separator, typically ",".
     */
    @Pure
    fun sep(sep: String?)

    /**
     * Writes a list separator.
     *
     * @param sep        List separator, typically ","
     * @param printFirst Whether to print the first occurrence of the separator
     */
    @Pure
    fun sep(sep: String?, printFirst: Boolean)

    /**
     * Sets whether whitespace is needed before the next token.
     */
    @Pure
    fun setNeedWhitespace(needWhitespace: Boolean)

    /**
     * Returns the offset for each level of indentation. Default 4.
     */
    val indentation: Int

    /**
     * Returns whether to enclose all expressions in parentheses, even if the
     * operator has high enough precedence that the parentheses are not
     * required.
     *
     *
     * For example, the parentheses are required in the expression `(a +
     * b) * c` because the '*' operator has higher precedence than the '+'
     * operator, and so without the parentheses, the expression would be
     * equivalent to `a + (b * c)`. The fully-parenthesized
     * expression, `((a + b) * c)` is unambiguous even if you don't
     * know the precedence of every operator.
     */
    val isAlwaysUseParentheses: Boolean

    /**
     * Returns whether we are currently in a query context (SELECT, INSERT,
     * UNION, INTERSECT, EXCEPT, and the ORDER BY operator).
     */
    fun inQuery(): Boolean
    //~ Inner Interfaces -------------------------------------------------------
    /**
     * A Frame is a piece of generated text which shares a common indentation
     * level.
     *
     *
     * Every frame has a beginning, a series of clauses and separators, and
     * an end. A typical frame is a comma-separated list. It begins with a "(",
     * consists of expressions separated by ",", and ends with a ")".
     *
     *
     * A select statement is also a kind of frame. The beginning and end are
     * are empty strings, but it consists of a sequence of clauses. "SELECT",
     * "FROM", "WHERE" are separators.
     *
     *
     * A frame is current between a call to one of the
     * [SqlWriter.startList] methods and the call to
     * [SqlWriter.endList]. If other code starts a frame in the mean
     * time, the sub-frame is put onto a stack.
     */
    interface Frame

    /** Frame type.  */
    interface FrameType {
        /**
         * Returns the name of this frame type.
         *
         * @return name
         */
        val name: String?

        /**
         * Returns whether this frame type should cause the code be further
         * indented.
         *
         * @return whether to further indent code within a frame of this type
         */
        fun needsIndent(): Boolean
    }

    companion object {
        /** Comma operator.
         *
         *
         * Defined in `SqlWriter` because it is only used while converting
         * [SqlNode] to SQL;
         * see [SqlWriter.list].
         *
         *
         * The precedence of the comma operator is low but not zero. For
         * instance, this ensures parentheses in
         * `select x, (select * from foo order by z), y from t`.  */
        val COMMA: SqlBinaryOperator = SqlBinaryOperator(",", SqlKind.OTHER, 2, false, null, null, null)
    }
}
