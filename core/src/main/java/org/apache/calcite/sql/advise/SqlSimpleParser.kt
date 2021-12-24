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

import org.apache.calcite.avatica.util.Quoting

/**
 * A simple parser that takes an incomplete and turn it into a syntactically
 * correct statement. It is used in the SQL editor user-interface.
 */
class SqlSimpleParser(//~ Instance fields --------------------------------------------------------
    private val hintToken: String,
    parserConfig: SqlParser.Config
) {
    //~ Enums ------------------------------------------------------------------
    /** Token.  */
    enum class TokenType {
        // keywords
        SELECT, FROM, JOIN, ON, USING, WHERE, GROUP, HAVING, ORDER, BY, UNION, INTERSECT, EXCEPT, MINUS,

        /** Left parenthesis.  */
        LPAREN {
            @Override
            override fun sql(): String {
                return "("
            }
        },

        /** Right parenthesis.  */
        RPAREN {
            @Override
            override fun sql(): String {
                return ")"
            }
        },

        /** Identifier, or indeed any miscellaneous sequence of characters.  */
        ID,

        /**
         * double-quoted identifier, e.g. "FOO""BAR"
         */
        DQID,

        /**
         * single-quoted string literal, e.g. 'foobar'
         */
        SQID, COMMENT, COMMA {
            @Override
            override fun sql(): String {
                return ","
            }
        },

        /**
         * A token created by reducing an entire sub-query.
         */
        QUERY;

        open fun sql(): String? {
            return name()
        }
    }

    private val parserConfig: SqlParser.Config
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a SqlSimpleParser.
     *
     * @param hintToken Hint token
     */
    @Deprecated // to be removed before 2.0
    @Deprecated("Use {@link #SqlSimpleParser(String, SqlParser.Config)}")
    constructor(hintToken: String) : this(hintToken, SqlParser.Config.DEFAULT) {
    }

    /**
     * Creates a SqlSimpleParser.
     *
     * @param hintToken Hint token
     * @param parserConfig parser configuration
     */
    init {
        this.parserConfig = parserConfig
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Turns a partially completed or syntactically incorrect sql statement into
     * a simplified, valid one that can be passed into getCompletionHints().
     *
     * @param sql    A partial or syntactically incorrect sql statement
     * @param cursor to indicate column position in the query at which
     * completion hints need to be retrieved.
     * @return a completed, valid (and possibly simplified SQL statement
     */
    fun simplifySql(sql: String, cursor: Int): String {
        // introduce the hint token into the sql at the cursor pos
        var sql = sql
        if (cursor >= sql.length()) {
            sql += " $hintToken "
        } else {
            val left: String = sql.substring(0, cursor)
            val right: String = sql.substring(cursor)
            sql = "$left $hintToken $right"
        }
        return simplifySql(sql)
    }

    /**
     * Turns a partially completed or syntactically incorrect SQL statement into a
     * simplified, valid one that can be validated.
     *
     * @param sql A partial or syntactically incorrect sql statement
     * @return a completed, valid (and possibly simplified) SQL statement
     */
    fun simplifySql(sql: String): String {
        val tokenizer = Tokenizer(sql, hintToken, parserConfig.quoting())
        val list: List<Token> = ArrayList()
        while (true) {
            val token = tokenizer.nextToken() ?: break
            if (token.type === TokenType.COMMENT) {
                // ignore comments
                continue
            }
            list.add(token)
        }

        // Gather consecutive sub-sequences of tokens into sub-queries.
        val outList: List<Token> = ArrayList()
        consumeQuery(list.listIterator(), outList)

        // Simplify.
        Query.simplifyList(outList, hintToken)

        // Convert to string.
        val buf = StringBuilder()
        var k = -1
        for (token in outList) {
            if (++k > 0) {
                buf.append(' ')
            }
            token.unparse(buf)
        }
        return buf.toString()
    }
    //~ Inner Classes ----------------------------------------------------------
    /** Tokenizer.  */
    class Tokenizer(val sql: String, private val hintToken: String, quoting: Quoting) {
        private val openQuote: Char
        private var pos: Int
        var start = 0

        @Deprecated // to be removed before 2.0
        constructor(sql: String, hintToken: String) : this(sql, hintToken, Quoting.DOUBLE_QUOTE) {
        }

        init {
            openQuote = quoting.string.charAt(0)
            pos = 0
        }

        private fun parseQuotedIdentifier(): Token {
            // Parse double-quoted identifier.
            start = pos
            ++pos
            val closeQuote = if (openQuote == '[') ']' else openQuote
            while (pos < sql.length()) {
                val c: Char = sql.charAt(pos)
                ++pos
                if (c == closeQuote) {
                    if (pos < sql.length() && sql.charAt(pos) === closeQuote) {
                        // Double close means escaped closing quote is a part of identifer
                        ++pos
                        continue
                    }
                    break
                }
            }
            val match: String = sql.substring(start, pos)
            return if (match.startsWith("$openQuote $hintToken ")) {
                Token(
                    TokenType.ID,
                    hintToken
                )
            } else Token(
                TokenType.DQID,
                match
            )
        }

        @Nullable
        fun nextToken(): Token? {
            while (pos < sql.length()) {
                var c: Char = sql.charAt(pos)
                val match: String
                return when (c) {
                    ',' -> {
                        ++pos
                        Token(TokenType.COMMA)
                    }
                    '(' -> {
                        ++pos
                        Token(TokenType.LPAREN)
                    }
                    ')' -> {
                        ++pos
                        Token(TokenType.RPAREN)
                    }
                    '\'' -> {

                        // Parse single-quoted identifier.
                        start = pos
                        ++pos
                        while (pos < sql.length()) {
                            c = sql.charAt(pos)
                            ++pos
                            if (c == '\'') {
                                if (pos < sql.length()) {
                                    val c1: Char = sql.charAt(pos)
                                    if (c1 == '\'') {
                                        // encountered consecutive
                                        // single-quotes; still in identifier
                                        ++pos
                                    } else {
                                        break
                                    }
                                } else {
                                    break
                                }
                            }
                        }
                        match = sql.substring(start, pos)
                        Token(TokenType.SQID, match)
                    }
                    '/' -> {

                        // possible start of '/*' or '//' comment
                        if (pos + 1 < sql.length()) {
                            val c1: Char = sql.charAt(pos + 1)
                            if (c1 == '*') {
                                var end: Int = sql.indexOf("*/", pos + 2)
                                if (end < 0) {
                                    end = sql.length()
                                } else {
                                    end += "*/".length()
                                }
                                pos = end
                                return Token(TokenType.COMMENT)
                            }
                            if (c1 == '/') {
                                pos = indexOfLineEnd(
                                    sql, pos + 2
                                )
                                return Token(TokenType.COMMENT)
                            }
                        }
                        // possible start of '--' comment
                        if (c == '-' && pos + 1 < sql.length() && sql.charAt(pos + 1) === '-') {
                            pos = indexOfLineEnd(sql, pos + 2)
                            return Token(TokenType.COMMENT)
                        }
                        if (c == openQuote) {
                            return parseQuotedIdentifier()
                        }
                        if (Character.isWhitespace(c)) {
                            ++pos
                            break
                        } else {
                            // Probably a letter or digit. Start an identifier.
                            // Other characters, e.g. *, ! are also included
                            // in identifiers.
                            val start = pos
                            ++pos
                            loop@ while (pos < sql.length()) {
                                c = sql.charAt(pos)
                                when (c) {
                                    '(', ')', '/', ',', '\'' -> break@loop
                                    '-' -> {
                                        // possible start of '--' comment
                                        if (c == '-' && pos + 1 < sql.length() && sql.charAt(pos + 1) === '-') {
                                            break@loop
                                        }
                                        if (Character.isWhitespace(c) || c == openQuote) {
                                            break@loop
                                        } else {
                                            ++pos
                                        }
                                    }
                                    else -> if (Character.isWhitespace(c) || c == openQuote) {
                                        break@loop
                                    } else {
                                        ++pos
                                    }
                                }
                            }
                            val name: String = sql.substring(start, pos)
                            val tokenType = TOKEN_TYPES[name.toUpperCase(Locale.ROOT)]
                            if (tokenType == null) {
                                IdToken(TokenType.ID, name)
                            } else {
                                // keyword, e.g. SELECT, FROM, WHERE
                                Token(tokenType)
                            }
                        }
                    }
                    '-' -> {
                        if (c == '-' && pos + 1 < sql.length() && sql.charAt(pos + 1) === '-') {
                            pos = indexOfLineEnd(sql, pos + 2)
                            return Token(TokenType.COMMENT)
                        }
                        if (c == openQuote) {
                            return parseQuotedIdentifier()
                        }
                        if (Character.isWhitespace(c)) {
                            ++pos
                            break
                        } else {
                            val start = pos
                            ++pos
                            loop@ while (pos < sql.length()) {
                                c = sql.charAt(pos)
                                when (c) {
                                    '(', ')', '/', ',', '\'' -> break@loop
                                    '-' -> {
                                        if (c == '-' && pos + 1 < sql.length() && sql.charAt(pos + 1) === '-') {
                                            break@loop
                                        }
                                        if (Character.isWhitespace(c) || c == openQuote) {
                                            break@loop
                                        } else {
                                            ++pos
                                        }
                                    }
                                    else -> if (Character.isWhitespace(c) || c == openQuote) {
                                        break@loop
                                    } else {
                                        ++pos
                                    }
                                }
                            }
                            val name: String = sql.substring(start, pos)
                            val tokenType = TOKEN_TYPES[name.toUpperCase(Locale.ROOT)]
                            if (tokenType == null) {
                                IdToken(TokenType.ID, name)
                            } else {
                                Token(tokenType)
                            }
                        }
                    }
                    else -> {
                        if (c == openQuote) {
                            return parseQuotedIdentifier()
                        }
                        if (Character.isWhitespace(c)) {
                            ++pos
                            break
                        } else {
                            val start = pos
                            ++pos
                            loop@ while (pos < sql.length()) {
                                c = sql.charAt(pos)
                                when (c) {
                                    '(', ')', '/', ',', '\'' -> break@loop
                                    '-' -> {
                                        if (c == '-' && pos + 1 < sql.length() && sql.charAt(pos + 1) === '-') {
                                            break@loop
                                        }
                                        if (Character.isWhitespace(c) || c == openQuote) {
                                            break@loop
                                        } else {
                                            ++pos
                                        }
                                    }
                                    else -> if (Character.isWhitespace(c) || c == openQuote) {
                                        break@loop
                                    } else {
                                        ++pos
                                    }
                                }
                            }
                            val name: String = sql.substring(start, pos)
                            val tokenType = TOKEN_TYPES[name.toUpperCase(Locale.ROOT)]
                            if (tokenType == null) {
                                IdToken(TokenType.ID, name)
                            } else {
                                Token(tokenType)
                            }
                        }
                    }
                }
            }
            return null
        }

        companion object {
            private val TOKEN_TYPES: Map<String, TokenType> = HashMap()

            init {
                for (type in TokenType.values()) {
                    TOKEN_TYPES.put(org.apache.calcite.sql.advise.type.name(), org.apache.calcite.sql.advise.type)
                }
            }

            private fun indexOfLineEnd(sql: String, i: Int): Int {
                var i = i
                val length: Int = sql.length()
                while (i < length) {
                    val c: Char = sql.charAt(i)
                    when (c) {
                        '\r', '\n' -> return i
                        else -> ++i
                    }
                }
                return i
            }
        }
    }

    /** Token.  */
    class Token internal constructor(val type: TokenType, @field:Nullable @param:Nullable val s: String?) {
        internal constructor(tokenType: TokenType) : this(tokenType, null) {}

        @Override
        override fun toString(): String {
            return if (s == null) type.toString() else "$type($s)"
        }

        fun unparse(buf: StringBuilder) {
            if (s == null) {
                buf.append(type.sql())
            } else {
                buf.append(s)
            }
        }
    }

    /** Token representing an identifier.  */
    class IdToken(type: TokenType, s: String?) : Token(type, s) {
        init {
            assert(type === TokenType.DQID || type === TokenType.ID)
        }
    }

    /** Token representing a query.  */
    internal class Query(tokenList: List<Token?>?) : Token(TokenType.QUERY) {
        private val tokenList: List<Token>

        init {
            this.tokenList = ArrayList(tokenList)
        }

        @Override
        override fun unparse(buf: StringBuilder) {
            var k = -1
            for (token in tokenList) {
                if (++k > 0) {
                    buf.append(' ')
                }
                token.unparse(buf)
            }
        }

        fun simplify(@Nullable hintToken: String?): Query {
            var clause = TokenType.SELECT
            var foundInClause: TokenType? = null
            var foundInSubQuery: Query? = null
            var majorClause: TokenType? = null
            if (hintToken != null) {
                for (token in tokenList) {
                    when (token.type) {
                        TokenType.ID -> if (hintToken.equals(token.s)) {
                            foundInClause = clause
                        }
                        TokenType.SELECT, TokenType.FROM, TokenType.WHERE, TokenType.GROUP, TokenType.HAVING, TokenType.ORDER -> {
                            majorClause = token.type
                            clause = token.type
                        }
                        TokenType.JOIN, TokenType.USING, TokenType.ON -> clause = token.type
                        TokenType.COMMA -> if (majorClause === TokenType.FROM) {
                            // comma inside from clause
                            clause = TokenType.FROM
                        }
                        TokenType.QUERY -> if ((token as Query).contains(hintToken)) {
                            foundInClause = clause
                            foundInSubQuery = token as Query
                        }
                        else -> {}
                    }
                }
            } else {
                foundInClause = TokenType.QUERY
            }
            if (foundInClause != null) {
                when (foundInClause) {
                    TokenType.SELECT -> {
                        purgeSelectListExcept(hintToken)
                        purgeWhere()
                        purgeOrderBy()
                    }
                    TokenType.FROM, TokenType.JOIN -> {

                        // See comments against ON/USING.
                        purgeSelect()
                        purgeFromExcept(hintToken)
                        purgeWhere()
                        purgeGroupByHaving()
                        purgeOrderBy()
                    }
                    TokenType.ON, TokenType.USING -> {

                        // We need to treat expressions in FROM and JOIN
                        // differently than ON and USING. Consider
                        //     FROM t1 JOIN t2 ON b1 JOIN t3 USING (c2)
                        // t1, t2, t3 occur in the FROM clause, and do not depend
                        // on anything; b1 and c2 occur in ON scope, and depend
                        // on the FROM clause
                        purgeSelect()
                        purgeWhere()
                        purgeOrderBy()
                    }
                    TokenType.WHERE -> {
                        purgeSelect()
                        purgeGroupByHaving()
                        purgeOrderBy()
                    }
                    TokenType.GROUP, TokenType.HAVING -> {
                        purgeSelect()
                        purgeWhere()
                        purgeOrderBy()
                    }
                    TokenType.ORDER -> purgeWhere()
                    TokenType.QUERY -> {

                        // Indicates that the expression to be simplified is
                        // outside this sub-query. Preserve a simplified SELECT
                        // clause.
                        // It might be a good idea to purge select expressions, however
                        // purgeSelectExprsKeepAliases might end up with <<0 as "*">> which is not valid.
                        // purgeSelectExprsKeepAliases();
                        purgeWhere()
                        purgeGroupByHaving()
                    }
                    else -> {}
                }
            }

            // Simplify sub-queries.
            for (token in tokenList) {
                when (token.type) {
                    TokenType.QUERY -> {
                        val query = token as Query
                        query.simplify(
                            if (query === foundInSubQuery) hintToken else null
                        )
                    }
                    else -> {}
                }
            }
            return this
        }

        private fun purgeSelectListExcept(@Nullable hintToken: String?) {
            val sublist = findClause(TokenType.SELECT)
            var parenCount = 0
            var itemStart = 1
            var itemEnd = -1
            var found = false
            for (i in 0 until sublist.size()) {
                val token = sublist[i]
                when (token.type) {
                    TokenType.LPAREN -> ++parenCount
                    TokenType.RPAREN -> --parenCount
                    TokenType.COMMA -> if (parenCount == 0) {
                        if (found) {
                            itemEnd = i
                            break
                        }
                        itemStart = i + 1
                    }
                    TokenType.ID -> if (requireNonNull(hintToken, "hintToken").equals(token.s)) {
                        found = true
                    }
                    else -> {}
                }
            }
            if (found) {
                if (itemEnd < 0) {
                    itemEnd = sublist.size()
                }
                val selectItem: List<Token> = ArrayList(
                    sublist.subList(itemStart, itemEnd)
                )
                val select = sublist[0]
                sublist.clear()
                sublist.add(select)
                sublist.addAll(selectItem)
            }
        }

        private fun purgeSelect() {
            val sublist = findClause(TokenType.SELECT)
            val select = sublist[0]
            sublist.clear()
            sublist.add(select)
            sublist.add(Token(TokenType.ID, "*"))
        }

        @SuppressWarnings("unused")
        private fun purgeSelectExprsKeepAliases() {
            val sublist = findClause(TokenType.SELECT)
            val newSelectClause: List<Token> = ArrayList()
            newSelectClause.add(sublist[0])
            var itemStart = 1
            for (i in 1 until sublist.size()) {
                val token = sublist[i]
                if (i + 1 == sublist.size()
                    || sublist[i + 1].type === TokenType.COMMA
                ) {
                    if (token.type === TokenType.ID) {
                        // This might produce <<0 as "a.x+b.y">>, or <<0 as "*">>, or even <<0 as "a.*">>
                        newSelectClause.add(Token(TokenType.ID, "0"))
                        newSelectClause.add(Token(TokenType.ID, "AS"))
                        newSelectClause.add(token)
                    } else {
                        newSelectClause.addAll(
                            sublist.subList(itemStart, i + 1)
                        )
                    }
                    itemStart = i + 2
                    if (i + 1 < sublist.size()) {
                        newSelectClause.add(Token(TokenType.COMMA))
                    }
                }
            }
            sublist.clear()
            sublist.addAll(newSelectClause)
        }

        private fun purgeFromExcept(@Nullable hintToken: String?) {
            val sublist = findClause(TokenType.FROM)
            var itemStart = -1
            var itemEnd = -1
            var joinCount = 0
            var found = false
            for (i in 0 until sublist.size()) {
                val token = sublist[i]
                when (token.type) {
                    TokenType.QUERY -> if ((token as Query).contains(requireNonNull(hintToken, "hintToken"))) {
                        found = true
                    }
                    TokenType.JOIN -> {
                        ++joinCount
                        if (found) {
                            itemEnd = i
                            break
                        }
                        itemStart = i + 1
                    }
                    TokenType.FROM, TokenType.ON, TokenType.COMMA -> {
                        if (found) {
                            itemEnd = i
                            break
                        }
                        itemStart = i + 1
                    }
                    TokenType.ID -> if (requireNonNull(hintToken, "hintToken").equals(token.s)) {
                        found = true
                    }
                    else -> {}
                }
            }

            // Don't simplify a FROM clause containing a JOIN: we lose help
            // with syntax.
            if (found && joinCount == 0) {
                if (itemEnd == -1) {
                    itemEnd = sublist.size()
                }
                val fromItem: List<Token> = ArrayList(
                    sublist.subList(itemStart, itemEnd)
                )
                val from = sublist[0]
                sublist.clear()
                sublist.add(from)
                sublist.addAll(fromItem)
            }
            if (sublist[sublist.size() - 1].type === TokenType.ON) {
                sublist.add(Token(TokenType.ID, "TRUE"))
            }
        }

        private fun purgeWhere() {
            val sublist = findClauseOrNull(TokenType.WHERE)
            sublist?.clear()
        }

        private fun purgeGroupByHaving() {
            var sublist: List<Token?>? = findClauseOrNull(TokenType.GROUP)
            sublist?.clear()
            sublist = findClauseOrNull(TokenType.HAVING)
            sublist?.clear()
        }

        private fun purgeOrderBy() {
            val sublist = findClauseOrNull(TokenType.ORDER)
            sublist?.clear()
        }

        private fun findClause(keyword: TokenType): List<Token> {
            return requireNonNull(
                findClauseOrNull(keyword)
            ) { "clause does not exist: $keyword" }
        }

        @Nullable
        private fun findClauseOrNull(keyword: TokenType): List<Token>? {
            var start = -1
            var k = -1
            val clauses: EnumSet<TokenType> = EnumSet.of(
                TokenType.SELECT,
                TokenType.FROM,
                TokenType.WHERE,
                TokenType.GROUP,
                TokenType.HAVING,
                TokenType.ORDER
            )
            for (token in tokenList) {
                ++k
                if (token.type === keyword) {
                    start = k
                } else if (start >= 0
                    && clauses.contains(token.type)
                ) {
                    return tokenList.subList(start, k)
                }
            }
            return if (start >= 0) {
                tokenList.subList(start, k + 1)
            } else null
        }

        private operator fun contains(hintToken: String): Boolean {
            for (token in tokenList) {
                when (token.type) {
                    TokenType.ID -> if (hintToken.equals(token.s)) {
                        return true
                    }
                    TokenType.QUERY -> if ((token as Query).contains(hintToken)) {
                        return true
                    }
                    else -> {}
                }
            }
            return false
        }

        companion object {
            fun simplifyList(list: List<Token?>, hintToken: String) {
                // Simplify
                //   SELECT * FROM t UNION ALL SELECT * FROM u WHERE ^
                // to
                //   SELECT * FROM u WHERE ^
                for (token in list) {
                    if (token is Query) {
                        val query = token as Query
                        if (query.contains(hintToken)) {
                            list.clear()
                            list.add(query.simplify(hintToken))
                            break
                        }
                    }
                }
            }
        }
    }

    companion object {
        private fun consumeQuery(iter: ListIterator<Token>, outList: List<Token>) {
            while (iter.hasNext()) {
                consumeSelect(iter, outList)
                if (iter.hasNext()) {
                    var token = iter.next()
                    when (token.type) {
                        TokenType.UNION, TokenType.INTERSECT, TokenType.EXCEPT, TokenType.MINUS -> {
                            outList.add(token)
                            if (iter.hasNext()) {
                                token = iter.next()
                                if (token.type === TokenType.ID
                                    && "ALL".equalsIgnoreCase(token.s)
                                ) {
                                    outList.add(token)
                                } else {
                                    iter.previous()
                                }
                            }
                        }
                        else -> {
                            // Unknown token detected => end of query detected
                            iter.previous()
                            return
                        }
                    }
                }
            }
        }

        private fun consumeSelect(iter: ListIterator<Token>, outList: List<Token>) {
            var isQuery = false
            val start: Int = outList.size()
            val subQueryList: List<Token> = ArrayList()
            loop@ while (iter.hasNext()) {
                val token = iter.next()
                subQueryList.add(token)
                when (token.type) {
                    TokenType.LPAREN -> consumeQuery(iter, subQueryList)
                    TokenType.RPAREN -> {
                        if (isQuery) {
                            subQueryList.remove(subQueryList.size() - 1)
                        }
                        break@loop
                    }
                    TokenType.SELECT -> isQuery = true
                    TokenType.UNION, TokenType.INTERSECT, TokenType.EXCEPT, TokenType.MINUS -> {
                        subQueryList.remove(subQueryList.size() - 1)
                        iter.previous()
                        break@loop
                    }
                    else -> {}
                }
            }

            // Fell off end of list. Pretend we saw the required right-paren.
            if (isQuery) {
                outList.subList(start, outList.size()).clear()
                outList.add(Query(subQueryList))
                if (outList.size() >= 2
                    && outList[outList.size() - 2].type === TokenType.LPAREN
                ) {
                    outList.add(Token(TokenType.RPAREN))
                }
            } else {
                // not a query - just a parenthesized expr
                outList.addAll(subQueryList)
            }
        }
    }
}
