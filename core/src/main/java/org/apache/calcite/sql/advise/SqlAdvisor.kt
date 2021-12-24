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
package org.apache.calcite.sql.advise

import org.apache.calcite.avatica.util.Casing

/**
 * An assistant which offers hints and corrections to a partially-formed SQL
 * statement. It is used in the SQL editor user-interface.
 */
class SqlAdvisor(
    validator: SqlValidatorWithHints,
    parserConfig: SqlParser.Config
) {
    //~ Instance fields --------------------------------------------------------
    // Flags indicating precision/scale combinations
    private val validator: SqlValidatorWithHints
    private val parserConfig: SqlParser.Config

    // Cache for getPreferredCasing
    @Nullable
    private var prevWord: String? = null

    @Nullable
    private var prevPreferredCasing: Casing? = null

    // Reserved words cache
    @Nullable
    private var reservedWordsSet: Set<String>? = null

    @Nullable
    private var reservedWordsList: List<String>? = null
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a SqlAdvisor with a validator instance.
     *
     * @param validator Validator
     */
    @Deprecated // to be removed before 2.0
    @Deprecated("use {@link #SqlAdvisor(SqlValidatorWithHints, SqlParser.Config)}")
    constructor(
        validator: SqlValidatorWithHints
    ) : this(validator, SqlParser.Config.DEFAULT) {
    }

    /**
     * Creates a SqlAdvisor with a validator instance and given parser
     * configuration.
     *
     * @param validator Validator
     * @param parserConfig parser config
     */
    init {
        this.validator = validator
        this.parserConfig = parserConfig
    }

    //~ Methods ----------------------------------------------------------------
    private fun quoteStart(): Char {
        return parserConfig.quoting().string.charAt(0)
    }

    private fun quoteEnd(): Char {
        val quote = quoteStart()
        return if (quote == '[') ']' else quote
    }

    /**
     * Gets completion hints for a partially completed or syntactically incorrect
     * sql statement with cursor pointing to the position where completion hints
     * are requested.
     *
     *
     * Writes into `replaced[0]` the string that is being
     * replaced. Includes the cursor and the preceding identifier. For example,
     * if `sql` is "select abc^de from t", sets `
     * replaced[0]` to "abc". If the cursor is in the middle of
     * whitespace, the replaced string is empty. The replaced string is never
     * null.
     *
     * @param sql      A partial or syntactically incorrect sql statement for
     * which to retrieve completion hints
     * @param cursor   to indicate the 0-based cursor position in the query at
     * @param replaced String which is being replaced (output)
     * @return completion hints
     */
    fun getCompletionHints(
        sql: String,
        cursor: Int,
        replaced: Array<String?>
    ): List<SqlMoniker> {
        // search backward starting from current position to find a "word"
        var sql = sql
        var wordStart = cursor
        var quoted = false
        while (wordStart > 0
            && Character.isJavaIdentifierPart(sql.charAt(wordStart - 1))
        ) {
            --wordStart
        }
        if (wordStart > 0
            && sql.charAt(wordStart - 1) === quoteStart()
        ) {
            quoted = true
            --wordStart
        }
        if (wordStart < 0) {
            return Collections.emptyList()
        }

        // Search forwards to the end of the word we should remove. Eat up
        // trailing double-quote, if any
        var wordEnd = cursor
        while (wordEnd < sql.length()
            && Character.isJavaIdentifierPart(sql.charAt(wordEnd))
        ) {
            ++wordEnd
        }
        if (quoted
            && wordEnd < sql.length()
            && sql.charAt(wordEnd) === quoteEnd()
        ) {
            ++wordEnd
        }

        // remove the partially composed identifier from the
        // sql statement - otherwise we get a parser exception
        replaced[0] = sql.substring(wordStart, cursor)
        var word = replaced[0]
        if (wordStart < wordEnd) {
            sql = (sql.substring(0, wordStart)
                    + sql.substring(wordEnd))
        }
        val completionHints: List<SqlMoniker> = getCompletionHints0(sql, wordStart)
        if (quoted) {
            word = word.substring(1)
        }
        if (word.isEmpty()) {
            return ImmutableList.copyOf(completionHints)
        }

        // If cursor was part of the way through a word, only include hints
        // which start with that word in the result.
        val result: ImmutableList.Builder<SqlMoniker> = Builder()
        val preferredCasing: Casing = getPreferredCasing(word)
        val ignoreCase = preferredCasing !== Casing.UNCHANGED
        for (hint in completionHints) {
            val names: List<String> = hint.getFullyQualifiedNames()
            // For now we treat only simple cases where the added name is the last
            // See [CALCITE-2439] Smart complete for SqlAdvisor
            val cname: String = Util.last(names)
            if (cname.regionMatches(ignoreCase, 0, word, 0, word!!.length())) {
                result.add(hint)
            }
        }
        return result.build()
    }

    fun getCompletionHints0(sql: String, cursor: Int): List<SqlMoniker> {
        val simpleSql = simplifySql(sql, cursor)
        val idx: Int = simpleSql.indexOf(HINT_TOKEN)
        if (idx < 0) {
            return Collections.emptyList()
        }
        val pos = SqlParserPos(1, idx + 1)
        return getCompletionHints(simpleSql, pos)
    }

    /**
     * Returns casing which is preferred for replacement.
     * For instance, `en => ename, EN => ENAME`.
     * When input has mixed case, `Casing.UNCHANGED` is returned.
     * @param word input word
     * @return preferred casing when replacing input word
     */
    private fun getPreferredCasing(word: String?): Casing {
        if (word === prevWord) {
            return castNonNull(prevPreferredCasing)
        }
        var hasLower = false
        var hasUpper = false
        var i = 0
        while (i < word!!.length() && !(hasLower && hasUpper)) {
            val codePoint: Int = word.codePointAt(i)
            hasLower = hasLower or Character.isLowerCase(codePoint)
            hasUpper = hasUpper or Character.isUpperCase(codePoint)
            i += Character.charCount(codePoint)
        }
        val preferredCasing: Casing
        preferredCasing = if (hasUpper && !hasLower) {
            Casing.TO_UPPER
        } else if (!hasUpper && hasLower) {
            Casing.TO_LOWER
        } else {
            Casing.UNCHANGED
        }
        prevWord = word
        prevPreferredCasing = preferredCasing
        return preferredCasing
    }

    fun getReplacement(hint: SqlMoniker, word: String?): String {
        val preferredCasing: Casing = getPreferredCasing(word)
        val quoted = !word.isEmpty() && word.charAt(0) === quoteStart()
        return getReplacement(hint, quoted, preferredCasing)
    }

    fun getReplacement(hint: SqlMoniker, quoted: Boolean, preferredCasing: Casing): String {
        var quoted = quoted
        val name: String = Util.last(hint.getFullyQualifiedNames())
        val isKeyword = hint.getType() === SqlMonikerType.KEYWORD
        // If replacement has mixed case, we need to quote it (or not depending
        // on quotedCasing/unquotedCasing
        quoted = quoted and !isKeyword
        if (!quoted && !isKeyword && reservedAndKeyWordsSet!!.contains(name)) {
            quoted = true
        }
        val sb = StringBuilder(name.length() + if (quoted) 2 else 0)
        if (!isKeyword && !Util.isValidJavaIdentifier(name)) {
            // needs quotes ==> quoted
            quoted = true
        }
        var idToAppend = name
        if (!quoted) {
            // id ==preferredCasing==> preferredId ==unquotedCasing==> recasedId
            // if recasedId matches id, then use preferredId
            val preferredId = applyCasing(name, preferredCasing)
            if (isKeyword || matchesUnquoted(name, preferredId)) {
                idToAppend = preferredId
            } else {
                // Check if we can use unquoted identifier as is: for instance, unquotedCasing==UNCHANGED
                quoted = !matchesUnquoted(name, idToAppend)
            }
        }
        if (quoted) {
            sb.append(quoteStart())
        }
        sb.append(idToAppend)
        if (quoted) {
            sb.append(quoteEnd())
        }
        return sb.toString()
    }

    private fun matchesUnquoted(name: String, idToAppend: String): Boolean {
        val recasedId = applyCasing(idToAppend, parserConfig.unquotedCasing())
        return recasedId.regionMatches(!parserConfig.caseSensitive(), 0, name, 0, name.length())
    }

    /**
     * Gets completion hints for a syntactically correct SQL statement with dummy
     * [SqlIdentifier].
     *
     * @param sql A syntactically correct sql statement for which to retrieve
     * completion hints
     * @param pos to indicate the line and column position in the query at which
     * completion hints need to be retrieved. For example, "select
     * a.ename, b.deptno from sales.emp a join sales.dept b "on
     * a.deptno=b.deptno where empno=1"; setting pos to 'Line 1, Column
     * 17' returns all the possible column names that can be selected
     * from sales.dept table setting pos to 'Line 1, Column 31' returns
     * all the possible table names in 'sales' schema
     * @return an array of hints ([SqlMoniker]) that can fill in at the
     * indicated position
     */
    fun getCompletionHints(sql: String, pos: SqlParserPos): List<SqlMoniker> {
        // First try the statement they gave us. If this fails, just return
        // the tokens which were expected at the failure point.
        var sql = sql
        val hintList: List<SqlMoniker> = ArrayList()
        val sqlNode: SqlNode = tryParse(sql, hintList) ?: return hintList

        // Now construct a statement which is bound to fail. (Character 7 BEL
        // is not legal in any SQL statement.)
        val x: Int = pos.getColumnNum() - 1
        sql = (sql.substring(0, x)
                + " \u0007"
                + sql.substring(x))
        tryParse(sql, hintList)
        val star: SqlMoniker = SqlMonikerImpl(ImmutableList.of("*"), SqlMonikerType.KEYWORD)
        val hintToken = if (parserConfig.unquotedCasing() === Casing.TO_UPPER) UPPER_HINT_TOKEN else HINT_TOKEN
        if (hintList.contains(star) && !isSelectListItem(sqlNode, pos, hintToken)) {
            hintList.remove(star)
        }

        // Add the identifiers which are expected at the point of interest.
        try {
            validator.validate(sqlNode)
        } catch (e: Exception) {
            // mask any exception that is thrown during the validation, i.e.
            // try to continue even if the sql is invalid. we are doing a best
            // effort here to try to come up with the requested completion
            // hints
            Util.swallow(e, LOGGER)
        }
        val validatorHints: List<SqlMoniker> = validator.lookupHints(sqlNode, pos)
        hintList.addAll(validatorHints)
        return hintList
    }

    /**
     * Tries to parse a SQL statement.
     *
     *
     * If succeeds, returns the parse tree node; if fails, populates the list
     * of hints and returns null.
     *
     * @param sql      SQL statement
     * @param hintList List of hints suggesting allowable tokens at the point of
     * failure
     * @return Parse tree if succeeded, null if parse failed
     */
    @Nullable
    private fun tryParse(sql: String, hintList: List<SqlMoniker>): SqlNode? {
        return try {
            parseQuery(sql)
        } catch (e: SqlParseException) {
            for (tokenName in e.getExpectedTokenNames()) {
                // Only add tokens which are keywords, like '"BY"'; ignore
                // symbols such as '<Identifier>'.
                if (tokenName.startsWith("\"")
                    && tokenName.endsWith("\"")
                ) {
                    hintList.add(
                        SqlMonikerImpl(
                            tokenName.substring(1, tokenName.length() - 1),
                            SqlMonikerType.KEYWORD
                        )
                    )
                }
            }
            null
        } catch (e: CalciteException) {
            Util.swallow(e, null)
            null
        }
    }

    /**
     * Gets the fully qualified name for a [SqlIdentifier] at a given
     * position of a sql statement.
     *
     * @param sql    A syntactically correct sql statement for which to retrieve a
     * fully qualified SQL identifier name
     * @param cursor to indicate the 0-based cursor position in the query that
     * represents a SQL identifier for which its fully qualified
     * name is to be returned.
     * @return a [SqlMoniker] that contains the fully qualified name of
     * the specified SQL identifier, returns null if none is found or the SQL
     * statement is invalid.
     */
    @Nullable
    fun getQualifiedName(sql: String?, cursor: Int): SqlMoniker? {
        val sqlNode: SqlNode
        try {
            sqlNode = parseQuery(sql)
            validator.validate(sqlNode)
        } catch (e: Exception) {
            return null
        }
        val pos = SqlParserPos(1, cursor + 1)
        return try {
            validator.lookupQualifiedName(sqlNode, pos)
        } catch (e: CalciteContextException) {
            null
        } catch (e: AssertionError) {
            null
        }
    }

    /**
     * Attempts to complete and validate a given partially completed sql
     * statement, and returns whether it is valid.
     *
     * @param sql A partial or syntactically incorrect sql statement to validate
     * @return whether SQL statement is valid
     */
    fun isValid(sql: String): Boolean {
        val simpleParser = SqlSimpleParser(HINT_TOKEN, parserConfig)
        val simpleSql: String = simpleParser.simplifySql(sql)
        val sqlNode: SqlNode
        sqlNode = try {
            parseQuery(simpleSql)
        } catch (e: Exception) {
            // if the sql can't be parsed we wont' be able to validate it
            return false
        }
        try {
            validator.validate(sqlNode)
        } catch (e: Exception) {
            return false
        }
        return true
    }

    /**
     * Attempts to parse and validate a SQL statement. Throws the first
     * exception encountered. The error message of this exception is to be
     * displayed on the UI
     *
     * @param sql A user-input sql statement to be validated
     * @return a List of ValidateErrorInfo (null if sql is valid)
     */
    @Nullable
    fun validate(sql: String): List<ValidateErrorInfo>? {
        val sqlNode: SqlNode?
        val errorList: List<ValidateErrorInfo> = ArrayList()
        sqlNode = collectParserError(sql, errorList)
        if (!errorList.isEmpty()) {
            return errorList
        } else if (sqlNode == null) {
            throw IllegalStateException(
                "collectParserError returned null (sql is not valid)"
                        + ", however, the resulting errorList is empty. sql=" + sql
            )
        }
        try {
            validator.validate(sqlNode)
        } catch (e: CalciteContextException) {
            val errInfo = ValidateErrorInfo(e)

            // validator only returns 1 exception now
            errorList.add(errInfo)
            return errorList
        } catch (e: Exception) {
            val errInfo = ValidateErrorInfo(
                1,
                1,
                1,
                sql.length(),
                e.getMessage()
            )

            // parser only returns 1 exception now
            errorList.add(errInfo)
            return errorList
        }
        return null
    }

    /**
     * Turns a partially completed or syntactically incorrect sql statement into
     * a simplified, valid one that can be passed into
     * [.getCompletionHints].
     *
     * @param sql    A partial or syntactically incorrect SQL statement
     * @param cursor Indicates the position in the query at which
     * completion hints need to be retrieved
     * @return a completed, valid (and possibly simplified SQL statement
     */
    fun simplifySql(sql: String, cursor: Int): String {
        val parser = SqlSimpleParser(HINT_TOKEN, parserConfig)
        return parser.simplifySql(sql, cursor)
    }

    /**
     * Returns an array of SQL reserved and keywords.
     *
     * @return an of SQL reserved and keywords
     */
    @get:EnsuresNonNull(["reservedWordsSet", "reservedWordsList"])
    val reservedAndKeyWords: List<String>?
        get() {
            ensureReservedAndKeyWords()
            return reservedWordsList
        }

    @get:EnsuresNonNull(["reservedWordsSet", "reservedWordsList"])
    private val reservedAndKeyWordsSet: Set<String>?
        private get() {
            ensureReservedAndKeyWords()
            return reservedWordsSet
        }

    @EnsuresNonNull(["reservedWordsSet", "reservedWordsList"])
    private fun ensureReservedAndKeyWords() {
        if (reservedWordsSet != null && reservedWordsList != null) {
            return
        }
        val c: Collection<String> = SqlAbstractParserImpl.getSql92ReservedWords()
        val l: List<String> = Arrays.asList(
            parserMetadata.getJdbcKeywords().split(",")
        )
        val al: List<String> = ArrayList()
        al.addAll(c)
        al.addAll(l)
        reservedWordsList = al
        reservedWordsSet = TreeSet(String.CASE_INSENSITIVE_ORDER)
        reservedWordsSet.addAll(reservedWordsList)
    }

    /**
     * Returns the underlying Parser metadata.
     *
     *
     * To use a different parser (recognizing a different dialect of SQL),
     * derived class should override.
     *
     * @return metadata
     */
    protected val parserMetadata: SqlAbstractParserImpl.Metadata
        protected get() {
            val parser: SqlParser = SqlParser.create("", parserConfig)
            return parser.getMetadata()
        }

    /**
     * Wrapper function to parse a SQL query (SELECT or VALUES, but not INSERT,
     * UPDATE, DELETE, CREATE, DROP etc.), throwing a [SqlParseException]
     * if the statement is not syntactically valid.
     *
     * @param sql SQL statement
     * @return parse tree
     * @throws SqlParseException if not syntactically valid
     */
    @Throws(SqlParseException::class)
    protected fun parseQuery(sql: String?): SqlNode {
        val parser: SqlParser = SqlParser.create(sql, parserConfig)
        return parser.parseStmt()
    }

    /**
     * Attempts to parse a SQL statement and adds to the errorList if any syntax
     * error is found. This implementation uses [SqlParser]. Subclass can
     * re-implement this with a different parser implementation
     *
     * @param sql       A user-input sql statement to be parsed
     * @param errorList A [List] of error to be added to
     * @return [SqlNode] that is root of the parse tree, null if the sql
     * is not valid
     */
    @Nullable
    protected fun collectParserError(
        sql: String?,
        errorList: List<ValidateErrorInfo?>
    ): SqlNode? {
        return try {
            parseQuery(sql)
        } catch (e: SqlParseException) {
            val errInfo = ValidateErrorInfo(
                e.getPos(),
                e.getMessage()
            )

            // parser only returns 1 exception now
            errorList.add(errInfo)
            null
        }
    }
    //~ Inner Classes ----------------------------------------------------------
    /** Text and position info of a validator or parser exception.  */
    class ValidateErrorInfo {
        /** Returns 1-based starting line number.  */
        var startLineNum: Int
            private set
        /** Returns 1-based starting column number.  */
        var startColumnNum: Int
            private set
        /** Returns 1-based end line number.  */
        var endLineNum: Int
            private set
        /** Returns 1-based end column number.  */
        var endColumnNum: Int
            private set

        /** Returns the error message.  */
        @get:Nullable
        @Nullable
        var message: String
            private set

        /**
         * Creates a new ValidateErrorInfo with the position coordinates and an
         * error string.
         *
         * @param startLineNum   Start line number
         * @param startColumnNum Start column number
         * @param endLineNum     End line number
         * @param endColumnNum   End column number
         * @param errorMsg       Error message
         */
        constructor(
            startLineNum: Int,
            startColumnNum: Int,
            endLineNum: Int,
            endColumnNum: Int,
            @Nullable errorMsg: String
        ) {
            this.startLineNum = startLineNum
            this.startColumnNum = startColumnNum
            this.endLineNum = endLineNum
            this.endColumnNum = endColumnNum
            message = errorMsg
        }

        /**
         * Creates a new ValidateErrorInfo with an CalciteContextException.
         *
         * @param e Exception
         */
        constructor(
            e: CalciteContextException
        ) {
            startLineNum = e.getPosLine()
            startColumnNum = e.getPosColumn()
            endLineNum = e.getEndPosLine()
            endColumnNum = e.getEndPosColumn()
            val cause: Throwable = e.getCause()
            message = (cause ?: e).getMessage()
        }

        /**
         * Creates a new ValidateErrorInfo with a SqlParserPos and an error
         * string.
         *
         * @param pos      Error position
         * @param errorMsg Error message
         */
        constructor(
            pos: SqlParserPos,
            @Nullable errorMsg: String
        ) {
            startLineNum = pos.getLineNum()
            startColumnNum = pos.getColumnNum()
            endLineNum = pos.getEndLineNum()
            endColumnNum = pos.getEndColumnNum()
            message = errorMsg
        }
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        val LOGGER: Logger = CalciteTrace.PARSER_LOGGER
        private const val HINT_TOKEN = "_suggest_"
        private val UPPER_HINT_TOKEN: String = HINT_TOKEN.toUpperCase(Locale.ROOT)
        private fun applyCasing(value: String, casing: Casing): String {
            return SqlParserUtil.toCase(value, casing)
        }

        private fun isSelectListItem(
            root: SqlNode?,
            pos: SqlParserPos, hintToken: String
        ): Boolean {
            var nodes: List<SqlNode> = SqlUtil.getAncestry(root,
                { input ->
                    (input is SqlIdentifier
                            && (input as SqlIdentifier).names.contains(hintToken))
                }
            ) { input ->
                Objects.requireNonNull(input, "input").getParserPosition()
                    .startsAt(pos)
            }
            assert(nodes[0] === root)
            nodes = Lists.reverse(nodes)
            return (nodes.size() > 2 && nodes[2] is SqlSelect
                    && nodes[1] === (nodes[2] as SqlSelect).getSelectList())
        }
    }
}
