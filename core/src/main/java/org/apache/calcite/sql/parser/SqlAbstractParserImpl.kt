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
package org.apache.calcite.sql.parser

import org.apache.calcite.avatica.util.Casing

/**
 * Abstract base for parsers generated from CommonParser.jj.
 */
abstract class SqlAbstractParserImpl {
    //~ Enums ------------------------------------------------------------------
    /**
     * Type-safe enum for context of acceptable expressions.
     */
    protected enum class ExprContext {
        /**
         * Accept any kind of expression in this context.
         */
        ACCEPT_ALL,

        /**
         * Accept any kind of expression in this context, with the exception of
         * CURSOR constructors.
         */
        ACCEPT_NONCURSOR,

        /**
         * Accept only query expressions in this context.
         */
        ACCEPT_QUERY,

        /**
         * Accept only non-query expressions in this context.
         */
        ACCEPT_NON_QUERY,

        /**
         * Accept only parenthesized queries or non-query expressions in this
         * context.
         */
        ACCEPT_SUB_QUERY,

        /**
         * Accept only CURSOR constructors, parenthesized queries, or non-query
         * expressions in this context.
         */
        ACCEPT_CURSOR;

        companion object {
            @Deprecated // to be removed before 2.0
            val ACCEPT_SUBQUERY = ACCEPT_SUB_QUERY

            @Deprecated // to be removed before 2.0
            val ACCEPT_NONQUERY = ACCEPT_NON_QUERY
        }
    }

    //~ Instance fields --------------------------------------------------------
    protected var nDynamicParams = 0
    /**
     * Returns the SQL text.
     */
    /**
     * Sets the SQL text that is being parsed.
     */
    @get:Nullable
    @Nullable
    var originalSql: String? = null
    val warnings: List<CalciteContextException> = ArrayList()

    /**
     * Creates a call.
     *
     * @param funName           Name of function
     * @param pos               Position in source code
     * @param funcType          Type of function
     * @param functionQualifier Qualifier
     * @param operands          Operands to call
     * @return Call
     */
    protected fun createCall(
        funName: SqlIdentifier?,
        pos: SqlParserPos?,
        funcType: SqlFunctionCategory?,
        functionQualifier: SqlLiteral?,
        operands: Iterable<SqlNode?>?
    ): SqlCall {
        return createCall(
            funName, pos, funcType, functionQualifier,
            Iterables.toArray(operands, SqlNode::class.java)
        )
    }

    /**
     * Creates a call.
     *
     * @param funName           Name of function
     * @param pos               Position in source code
     * @param funcType          Type of function
     * @param functionQualifier Qualifier
     * @param operands          Operands to call
     * @return Call
     */
    protected fun createCall(
        funName: SqlIdentifier?,
        pos: SqlParserPos?,
        funcType: SqlFunctionCategory?,
        functionQualifier: SqlLiteral?,
        operands: Array<SqlNode?>?
    ): SqlCall {
        // Create a placeholder function.  Later, during
        // validation, it will be resolved into a real function reference.
        val `fun`: SqlOperator = SqlUnresolvedFunction(
            funName, null, null, null, null,
            funcType
        )
        return `fun`.createCall(functionQualifier, pos, operands)
    }

    /**
     * Returns metadata about this parser: keywords, etc.
     */
    abstract val metadata: Metadata

    /**
     * Removes or transforms misleading information from a parse exception or
     * error, and converts to [SqlParseException].
     *
     * @param ex dirty excn
     * @return clean excn
     */
    abstract fun normalizeException(@Nullable ex: Throwable?): SqlParseException

    @get:Throws(Exception::class)
    protected abstract val pos: org.apache.calcite.sql.parser.SqlParserPos

    /**
     * Reinitializes parser with new input.
     *
     * @param reader provides new input
     */
    // CHECKSTYLE: IGNORE 1
    abstract fun ReInit(reader: Reader?)

    /**
     * Parses a SQL expression ending with EOF and constructs a
     * parse tree.
     *
     * @return constructed parse tree.
     */
    @Throws(Exception::class)
    abstract fun parseSqlExpressionEof(): SqlNode

    /**
     * Parses a SQL statement ending with EOF and constructs a
     * parse tree.
     *
     * @return constructed parse tree.
     */
    @Throws(Exception::class)
    abstract fun parseSqlStmtEof(): SqlNode

    /**
     * Parses a list of SQL statements separated by semicolon and constructs a
     * parse tree. The semicolon is required between statements, but is
     * optional at the end.
     *
     * @return constructed list of SQL statements.
     */
    @Throws(Exception::class)
    abstract fun parseSqlStmtList(): SqlNodeList

    /**
     * Sets the tab stop size.
     *
     * @param tabSize Tab stop size
     */
    abstract fun setTabSize(tabSize: Int)

    /**
     * Sets the casing policy for quoted identifiers.
     *
     * @param quotedCasing Casing to set.
     */
    abstract fun setQuotedCasing(quotedCasing: Casing?)

    /**
     * Sets the casing policy for unquoted identifiers.
     *
     * @param unquotedCasing Casing to set.
     */
    abstract fun setUnquotedCasing(unquotedCasing: Casing?)

    /**
     * Sets the maximum length for sql identifier.
     */
    abstract fun setIdentifierMaxLength(identifierMaxLength: Int)

    /**
     * Sets the SQL language conformance level.
     */
    abstract fun setConformance(conformance: SqlConformance?)

    /**
     * Change parser state.
     *
     * @param state New state
     */
    abstract fun switchTo(state: LexicalState?)
    //~ Inner Interfaces -------------------------------------------------------
    /** Valid starting states of the parser.
     *
     *
     * (There are other states that the parser enters during parsing, such as
     * being inside a multi-line comment.)
     *
     *
     * The starting states generally control the syntax of quoted
     * identifiers.  */
    enum class LexicalState {
        /** Starting state where quoted identifiers use brackets, like Microsoft SQL
         * Server.  */
        DEFAULT,

        /** Starting state where quoted identifiers use double-quotes, like
         * Oracle and PostgreSQL.  */
        DQID,

        /** Starting state where quoted identifiers use back-ticks, like MySQL.  */
        BTID,

        /** Starting state where quoted identifiers use back-ticks,
         * unquoted identifiers that are part of table names may contain hyphens,
         * and character literals may be enclosed in single- or double-quotes,
         * like BigQuery.  */
        BQID;

        companion object {
            /** Returns the corresponding parser state with the given configuration
             * (in particular, quoting style).  */
            fun forConfig(config: SqlParser.Config): LexicalState {
                return when (config.quoting()) {
                    BRACKET -> DEFAULT
                    DOUBLE_QUOTE -> DQID
                    BACK_TICK_BACKSLASH -> BQID
                    BACK_TICK -> {
                        if (config.conformance().allowHyphenInUnquotedTableName()
                            && config.charLiteralStyles().equals(
                                EnumSet.of(
                                    CharLiteralStyle.BQ_SINGLE,
                                    CharLiteralStyle.BQ_DOUBLE
                                )
                            )
                        ) {
                            return BQID
                        }
                        if (!config.conformance().allowHyphenInUnquotedTableName()
                            && config.charLiteralStyles().equals(
                                EnumSet.of(CharLiteralStyle.STANDARD)
                            )
                        ) {
                            return BTID
                        }
                        throw AssertionError(config)
                    }
                    else -> throw AssertionError(config)
                }
            }
        }
    }

    /**
     * Metadata about the parser. For example:
     *
     *
     *  * "KEY" is a keyword: it is meaningful in certain contexts, such as
     * "CREATE FOREIGN KEY", but can be used as an identifier, as in `
     * "CREATE TABLE t (key INTEGER)"`.
     *  * "SELECT" is a reserved word. It can not be used as an identifier.
     *  * "CURRENT_USER" is the name of a context variable. It cannot be used
     * as an identifier.
     *  * "ABS" is the name of a reserved function. It cannot be used as an
     * identifier.
     *  * "DOMAIN" is a reserved word as specified by the SQL:92 standard.
     *
     */
    interface Metadata {
        /**
         * Returns true if token is a keyword but not a reserved word. For
         * example, "KEY".
         */
        fun isNonReservedKeyword(token: String?): Boolean

        /**
         * Returns whether token is the name of a context variable such as
         * "CURRENT_USER".
         */
        fun isContextVariableName(token: String?): Boolean

        /**
         * Returns whether token is a reserved function name such as
         * "CURRENT_USER".
         */
        fun isReservedFunctionName(token: String?): Boolean

        /**
         * Returns whether token is a keyword. (That is, a non-reserved keyword,
         * a context variable, or a reserved function name.)
         */
        fun isKeyword(token: String?): Boolean

        /**
         * Returns whether token is a reserved word.
         */
        fun isReservedWord(token: String?): Boolean

        /**
         * Returns whether token is a reserved word as specified by the SQL:92
         * standard.
         */
        fun isSql92ReservedWord(token: String?): Boolean

        /**
         * Returns comma-separated list of JDBC keywords.
         */
        val jdbcKeywords: String?

        /**
         * Returns a list of all tokens in alphabetical order.
         */
        val tokens: List<String?>?
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Default implementation of the [Metadata] interface.
     */
    class MetadataImpl(sqlParser: SqlAbstractParserImpl) : Metadata {
        private val reservedFunctionNames: Set<String> = HashSet()
        private val contextVariableNames: Set<String> = HashSet()
        private val nonReservedKeyWordSet: Set<String> = HashSet()

        /**
         * Set of all tokens.
         */
        private val tokenSet: NavigableSet<String> = TreeSet()

        /**
         * Immutable list of all tokens, in alphabetical order.
         */
        @get:Override
        override val tokens: List<String>
        private val reservedWords: Set<String> = HashSet()

        @get:Override
        override val jdbcKeywords: String

        /**
         * Creates a MetadataImpl.
         *
         * @param sqlParser Parser
         */
        init {
            initList(sqlParser, reservedFunctionNames, "ReservedFunctionName")
            initList(sqlParser, contextVariableNames, "ContextVariable")
            initList(sqlParser, nonReservedKeyWordSet, "NonReservedKeyWord")
            tokens = ImmutableList.copyOf(tokenSet)
            jdbcKeywords = constructSql92ReservedWordList()
            val reservedWordSet: Set<String> = TreeSet()
            reservedWordSet.addAll(tokenSet)
            reservedWordSet.removeAll(nonReservedKeyWordSet)
            reservedWords.addAll(reservedWordSet)
        }

        /**
         * Initializes lists of keywords.
         */
        private fun initList(
            parserImpl: SqlAbstractParserImpl,
            keywords: Set<String>,
            name: String
        ) {
            parserImpl.ReInit(StringReader("1"))
            try {
                val o: Object = virtualCall(parserImpl, name)
                throw AssertionError("expected call to fail, got $o")
            } catch (parseException: SqlParseException) {
                // First time through, build the list of all tokens.
                val tokenImages: Array<String> = parseException.getTokenImages()
                if (tokenSet.isEmpty()) {
                    for (token in tokenImages) {
                        val tokenVal: String = SqlParserUtil.getTokenVal(token)
                        if (tokenVal != null) {
                            tokenSet.add(tokenVal)
                        }
                    }
                }

                // Add the tokens which would have been expected in this
                // syntactic context to the list we're building.
                val expectedTokenSequences: Array<IntArray> = parseException.getExpectedTokenSequences()
                for (tokens in expectedTokenSequences) {
                    assert(tokens.size == 1)
                    val tokenId = tokens[0]
                    val token = tokenImages[tokenId]
                    val tokenVal: String = SqlParserUtil.getTokenVal(token)
                    if (tokenVal != null) {
                        keywords.add(tokenVal)
                    }
                }
            } catch (e: Throwable) {
                throw RuntimeException("While building token lists", e)
            }
        }

        /**
         * Uses reflection to invoke a method on this parser. The method must be
         * public and have no parameters.
         *
         * @param parserImpl Parser
         * @param name       Name of method. For example "ReservedFunctionName".
         * @return Result of calling method
         */
        @Nullable
        @Throws(Throwable::class)
        private fun virtualCall(
            parserImpl: SqlAbstractParserImpl,
            name: String
        ): Object {
            val clazz: Class<*> = parserImpl.getClass()
            return try {
                val method: Method = clazz.getMethod(name)
                method.invoke(parserImpl)
            } catch (e: InvocationTargetException) {
                val cause: Throwable = e.getCause()
                throw parserImpl.normalizeException(cause)
            }
        }

        /**
         * Builds a comma-separated list of JDBC reserved words.
         */
        private fun constructSql92ReservedWordList(): String {
            val sb = StringBuilder()
            val jdbcReservedSet: TreeSet<String> = TreeSet()
            jdbcReservedSet.addAll(tokenSet)
            jdbcReservedSet.removeAll(SQL_92_RESERVED_WORD_SET)
            jdbcReservedSet.removeAll(nonReservedKeyWordSet)
            var j = 0
            for (jdbcReserved in jdbcReservedSet) {
                if (j++ > 0) {
                    sb.append(",")
                }
                sb.append(jdbcReserved)
            }
            return sb.toString()
        }

        @Override
        override fun isSql92ReservedWord(token: String?): Boolean {
            return SQL_92_RESERVED_WORD_SET.contains(token)
        }

        @Override
        override fun isKeyword(token: String): Boolean {
            return (isNonReservedKeyword(token)
                    || isReservedFunctionName(token)
                    || isContextVariableName(token)
                    || isReservedWord(token))
        }

        @Override
        override fun isNonReservedKeyword(token: String): Boolean {
            return nonReservedKeyWordSet.contains(token)
        }

        @Override
        override fun isReservedFunctionName(token: String): Boolean {
            return reservedFunctionNames.contains(token)
        }

        @Override
        override fun isContextVariableName(token: String): Boolean {
            return contextVariableNames.contains(token)
        }

        @Override
        override fun isReservedWord(token: String): Boolean {
            return reservedWords.contains(token)
        }
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        private val SQL_92_RESERVED_WORD_SET: ImmutableSet<String> = ImmutableSet.of(
            "ABSOLUTE",
            "ACTION",
            "ADD",
            "ALL",
            "ALLOCATE",
            "ALTER",
            "AND",
            "ANY",
            "ARE",
            "AS",
            "ASC",
            "ASSERTION",
            "AT",
            "AUTHORIZATION",
            "AVG",
            "BEGIN",
            "BETWEEN",
            "BIT",
            "BIT_LENGTH",
            "BOTH",
            "BY",
            "CALL",
            "CASCADE",
            "CASCADED",
            "CASE",
            "CAST",
            "CATALOG",
            "CHAR",
            "CHARACTER",
            "CHARACTER_LENGTH",
            "CHAR_LENGTH",
            "CHECK",
            "CLOSE",
            "COALESCE",
            "COLLATE",
            "COLLATION",
            "COLUMN",
            "COMMIT",
            "CONDITION",
            "CONNECT",
            "CONNECTION",
            "CONSTRAINT",
            "CONSTRAINTS",
            "CONTAINS",
            "CONTINUE",
            "CONVERT",
            "CORRESPONDING",
            "COUNT",
            "CREATE",
            "CROSS",
            "CURRENT",
            "CURRENT_DATE",
            "CURRENT_PATH",
            "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
            "CURRENT_USER",
            "CURSOR",
            "DATE",
            "DAY",
            "DEALLOCATE",
            "DEC",
            "DECIMAL",
            "DECLARE",
            "DEFAULT",
            "DEFERRABLE",
            "DEFERRED",
            "DELETE",
            "DESC",
            "DESCRIBE",
            "DESCRIPTOR",
            "DETERMINISTIC",
            "DIAGNOSTICS",
            "DISCONNECT",
            "DISTINCT",
            "DOMAIN",
            "DOUBLE",
            "DROP",
            "ELSE",
            "END",
            "ESCAPE",
            "EXCEPT",
            "EXCEPTION",
            "EXEC",
            "EXECUTE",
            "EXISTS",
            "EXTERNAL",
            "EXTRACT",
            "FALSE",
            "FETCH",
            "FIRST",
            "FLOAT",
            "FOR",
            "FOREIGN",
            "FOUND",
            "FROM",
            "FULL",
            "FUNCTION",
            "GET",
            "GLOBAL",
            "GO",
            "GOTO",
            "GRANT",
            "GROUP",
            "HAVING",
            "HOUR",
            "IDENTITY",
            "IMMEDIATE",
            "IN",
            "INADD",
            "INDICATOR",
            "INITIALLY",
            "INNER",
            "INOUT",
            "INPUT",
            "INSENSITIVE",
            "INSERT",
            "INT",
            "INTEGER",
            "INTERSECT",
            "INTERVAL",
            "INTO",
            "IS",
            "ISOLATION",
            "JOIN",
            "KEY",
            "LANGUAGE",
            "LAST",
            "LEADING",
            "LEFT",
            "LEVEL",
            "LIKE",
            "LOCAL",
            "LOWER",
            "MATCH",
            "MAX",
            "MIN",
            "MINUTE",
            "MODULE",
            "MONTH",
            "NAMES",
            "NATIONAL",
            "NATURAL",
            "NCHAR",
            "NEXT",
            "NO",
            "NOT",
            "NULL",
            "NULLIF",
            "NUMERIC",
            "OCTET_LENGTH",
            "OF",
            "ON",
            "ONLY",
            "OPEN",
            "OPTION",
            "OR",
            "ORDER",
            "OUT",
            "OUTADD",
            "OUTER",
            "OUTPUT",
            "OVERLAPS",
            "PAD",
            "PARAMETER",
            "PARTIAL",
            "PATH",
            "POSITION",
            "PRECISION",
            "PREPARE",
            "PRESERVE",
            "PRIMARY",
            "PRIOR",
            "PRIVILEGES",
            "PROCEDURE",
            "PUBLIC",
            "READ",
            "REAL",
            "REFERENCES",
            "RELATIVE",
            "RESTRICT",
            "RETURN",
            "RETURNS",
            "REVOKE",
            "RIGHT",
            "ROLLBACK",
            "ROUTINE",
            "ROWS",
            "SCHEMA",
            "SCROLL",
            "SECOND",
            "SECTION",
            "SELECT",
            "SESSION",
            "SESSION_USER",
            "SET",
            "SIZE",
            "SMALLINT",
            "SOME",
            "SPACE",
            "SPECIFIC",
            "SQL",
            "SQLCODE",
            "SQLERROR",
            "SQLEXCEPTION",
            "SQLSTATE",
            "SQLWARNING",
            "SUBSTRING",
            "SUM",
            "SYSTEM_USER",
            "TABLE",
            "TEMPORARY",
            "THEN",
            "TIME",
            "TIMESTAMP",
            "TIMEZONE_HOUR",
            "TIMEZONE_MINUTE",
            "TO",
            "TRAILING",
            "TRANSACTION",
            "TRANSLATE",
            "TRANSLATION",
            "TRIM",
            "TRUE",
            "UNION",
            "UNIQUE",
            "UNKNOWN",
            "UPDATE",
            "UPPER",
            "USAGE",
            "USER",
            "USING",
            "VALUE",
            "VALUES",
            "VARCHAR",
            "VARYING",
            "VIEW",
            "WHEN",
            "WHENEVER",
            "WHERE",
            "WITH",
            "WORK",
            "WRITE",
            "YEAR",
            "ZONE"
        )
        //~ Methods ----------------------------------------------------------------
        /**
         * Returns immutable set of all reserved words defined by SQL-92.
         *
         * @see Glossary.SQL92 SQL-92 Section 5.2
         */
        val sql92ReservedWords: Set<String>
            get() = SQL_92_RESERVED_WORD_SET
    }
}
