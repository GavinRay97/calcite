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
 * Utility methods relating to parsing SQL.
 */
object SqlParserUtil {
    //~ Static fields/initializers ---------------------------------------------
    val LOGGER: Logger = CalciteTrace.getParserTracer()
    //~ Methods ----------------------------------------------------------------
    /** Returns the character-set prefix of a SQL string literal; returns null if
     * there is none.  */
    @Nullable
    fun getCharacterSet(s: String): String? {
        if (s.charAt(0) === '\'') {
            return null
        }
        if (Character.toUpperCase(s.charAt(0)) === 'N') {
            return CalciteSystemProperty.DEFAULT_NATIONAL_CHARSET.value()
        }
        val i: Int = s.indexOf("'")
        return s.substring(1, i) // skip prefixed '_'
    }

    /**
     * Converts the contents of an sql quoted string literal into the
     * corresponding Java string representation (removing leading and trailing
     * quotes and unescaping internal doubled quotes).
     */
    fun parseString(s: String): String {
        var s = s
        val i: Int = s.indexOf("'") // start of body
        if (i > 0) {
            s = s.substring(i)
        }
        return strip(s, "'", "'", "''", Casing.UNCHANGED)
    }

    fun parseDecimal(s: String?): BigDecimal {
        return BigDecimal(s)
    }

    fun parseInteger(s: String?): BigDecimal {
        return BigDecimal(s)
    }
    // CHECKSTYLE: IGNORE 1

    @Deprecated // to be removed before 2.0
    @Deprecated("this method is not localized for Farrago standards ")
    fun parseDate(s: String?): java.sql.Date {
        return java.sql.Date.valueOf(s)
    }
    // CHECKSTYLE: IGNORE 1

    @Deprecated // to be removed before 2.0
    @Deprecated("Does not parse SQL:99 milliseconds ")
    fun parseTime(s: String?): Time {
        return java.sql.Time.valueOf(s)
    }
    // CHECKSTYLE: IGNORE 1

    @Deprecated // to be removed before 2.0
    @Deprecated("this method is not localized for Farrago standards ")
    fun parseTimestamp(s: String?): java.sql.Timestamp {
        return java.sql.Timestamp.valueOf(s)
    }

    fun parseDateLiteral(s: String, pos: SqlParserPos?): SqlDateLiteral {
        val dateStr = parseString(s)
        val cal: Calendar = DateTimeUtils.parseDateFormat(
            dateStr, Format.get().date,
            DateTimeUtils.UTC_ZONE
        )
            ?: throw SqlUtil.newContextException(
                pos,
                RESOURCE.illegalLiteral(
                    "DATE", s,
                    RESOURCE.badFormat(DateTimeUtils.DATE_FORMAT_STRING).str()
                )
            )
        val d: DateString = DateString.fromCalendarFields(cal)
        return SqlLiteral.createDate(d, pos)
    }

    fun parseTimeLiteral(s: String, pos: SqlParserPos?): SqlTimeLiteral {
        val dateStr = parseString(s)
        val pt: DateTimeUtils.PrecisionTime = DateTimeUtils.parsePrecisionDateTimeLiteral(
            dateStr,
            Format.get().time, DateTimeUtils.UTC_ZONE, -1
        )
            ?: throw SqlUtil.newContextException(
                pos,
                RESOURCE.illegalLiteral(
                    "TIME", s,
                    RESOURCE.badFormat(DateTimeUtils.TIME_FORMAT_STRING).str()
                )
            )
        val t: TimeString = TimeString.fromCalendarFields(pt.getCalendar())
            .withFraction(pt.getFraction())
        return SqlLiteral.createTime(t, pt.getPrecision(), pos)
    }

    fun parseTimestampLiteral(
        s: String,
        pos: SqlParserPos?
    ): SqlTimestampLiteral {
        val dateStr = parseString(s)
        val format = Format.get()
        var pt: DateTimeUtils.PrecisionTime? = null
        // Allow timestamp literals with and without time fields (as does
        // PostgreSQL); TODO: require time fields except in Babel's lenient mode
        val dateFormats: Array<DateFormat> = arrayOf<DateFormat>(format.timestamp, format.date)
        for (dateFormat in dateFormats) {
            pt = DateTimeUtils.parsePrecisionDateTimeLiteral(
                dateStr,
                dateFormat, DateTimeUtils.UTC_ZONE, -1
            )
            if (pt != null) {
                break
            }
        }
        if (pt == null) {
            throw SqlUtil.newContextException(
                pos,
                RESOURCE.illegalLiteral(
                    "TIMESTAMP", s,
                    RESOURCE.badFormat(DateTimeUtils.TIMESTAMP_FORMAT_STRING).str()
                )
            )
        }
        val ts: TimestampString = TimestampString.fromCalendarFields(pt.getCalendar())
            .withFraction(pt.getFraction())
        return SqlLiteral.createTimestamp(ts, pt.getPrecision(), pos)
    }

    fun parseIntervalLiteral(
        pos: SqlParserPos,
        sign: Int, s: String, intervalQualifier: SqlIntervalQualifier
    ): SqlIntervalLiteral {
        val intervalStr = parseString(s)
        if (intervalStr.equals("")) {
            throw SqlUtil.newContextException(
                pos,
                RESOURCE.illegalIntervalLiteral(
                    s + " "
                            + intervalQualifier.toString(), pos.toString()
                )
            )
        }
        return SqlLiteral.createInterval(sign, intervalStr, intervalQualifier, pos)
    }

    /**
     * Checks if the date/time format is valid, throws if not.
     *
     * @param pattern [SimpleDateFormat]  pattern
     */
    fun checkDateFormat(pattern: String?) {
        val df = SimpleDateFormat(pattern, Locale.ROOT)
        Util.discard(df)
    }

    /**
     * Converts the interval value into a millisecond representation.
     *
     * @param interval Interval
     * @return a long value that represents millisecond equivalent of the
     * interval value.
     */
    fun intervalToMillis(
        interval: SqlIntervalLiteral.IntervalValue
    ): Long {
        return intervalToMillis(
            interval.getIntervalLiteral(),
            interval.getIntervalQualifier()
        )
    }

    fun intervalToMillis(
        literal: String,
        intervalQualifier: SqlIntervalQualifier
    ): Long {
        Preconditions.checkArgument(
            !intervalQualifier.isYearMonth(),
            "interval must be day time"
        )
        val ret: IntArray
        try {
            ret = intervalQualifier.evaluateIntervalLiteral(
                literal,
                intervalQualifier.getParserPosition(), RelDataTypeSystem.DEFAULT
            )
            assert(ret != null)
        } catch (e: CalciteContextException) {
            throw RuntimeException(
                "while parsing day-to-second interval "
                        + literal, e
            )
        }
        var l: Long = 0
        val conv = LongArray(5)
        conv[4] = 1 // millisecond
        conv[3] = conv[4] * 1000 // second
        conv[2] = conv[3] * 60 // minute
        conv[1] = conv[2] * 60 // hour
        conv[0] = conv[1] * 24 // day
        for (i in 1 until ret.size) {
            l += conv[i - 1] * ret[i]
        }
        return ret[0] * l
    }

    /**
     * Converts the interval value into a months representation.
     *
     * @param interval Interval
     * @return a long value that represents months equivalent of the interval
     * value.
     */
    fun intervalToMonths(
        interval: SqlIntervalLiteral.IntervalValue
    ): Long {
        return intervalToMonths(
            interval.getIntervalLiteral(),
            interval.getIntervalQualifier()
        )
    }

    fun intervalToMonths(
        literal: String,
        intervalQualifier: SqlIntervalQualifier
    ): Long {
        Preconditions.checkArgument(
            intervalQualifier.isYearMonth(),
            "interval must be year month"
        )
        val ret: IntArray
        try {
            ret = intervalQualifier.evaluateIntervalLiteral(
                literal,
                intervalQualifier.getParserPosition(), RelDataTypeSystem.DEFAULT
            )
            assert(ret != null)
        } catch (e: CalciteContextException) {
            throw RuntimeException(
                "Error while parsing year-to-month interval "
                        + literal, e
            )
        }
        var l: Long = 0
        val conv = LongArray(2)
        conv[1] = 1 // months
        conv[0] = conv[1] * 12 // years
        for (i in 1 until ret.size) {
            l += conv[i - 1] * ret[i]
        }
        return ret[0] * l
    }

    /**
     * Parses a positive int. All characters have to be digits.
     *
     * @see Integer.parseInt
     * @throws java.lang.NumberFormatException if invalid number or leading '-'
     */
    fun parsePositiveInt(value: String): Int {
        var value = value
        value = value.trim()
        if (value.charAt(0) === '-') {
            throw NumberFormatException(value)
        }
        return Integer.parseInt(value)
    }

    /**
     * Parses a Binary string. SQL:99 defines a binary string as a hexstring
     * with EVEN nbr of hex digits.
     */
    @Deprecated // to be removed before 2.0
    fun parseBinaryString(s: String): ByteArray {
        var s = s
        s = s.replace(" ", "")
        s = s.replace("\n", "")
        s = s.replace("\t", "")
        s = s.replace("\r", "")
        s = s.replace("\u000c", "")
        s = s.replace("'", "")
        if (s.length() === 0) {
            return ByteArray(0)
        }
        assert(
            s.length() and 1 === 0 // must be even nbr of hex digits
        )
        val lengthToBe: Int = s.length() / 2
        s = "ff$s"
        val bigInt = BigInteger(s, 16)
        val ret = ByteArray(lengthToBe)
        System.arraycopy(
            bigInt.toByteArray(),
            2,
            ret,
            0,
            ret.size
        )
        return ret
    }

    /**
     * Converts a quoted identifier, unquoted identifier, or quoted string to a
     * string of its contents.
     *
     *
     * First, if `startQuote` is provided, `endQuote` and
     * `escape` must also be provided, and this method removes quotes.
     *
     *
     * Finally, converts the string to the provided casing.
     */
    fun strip(
        s: String, @Nullable startQuote: String?,
        @Nullable endQuote: String?, @Nullable escape: String?, casing: Casing?
    ): String {
        return if (startQuote != null) {
            stripQuotes(
                s, Objects.requireNonNull(startQuote, "startQuote"),
                Objects.requireNonNull(endQuote, "endQuote"), Objects.requireNonNull(escape, "escape"),
                casing
            )
        } else {
            toCase(s, casing)
        }
    }

    /**
     * Unquotes a quoted string, using different quotes for beginning and end.
     */
    fun stripQuotes(
        s: String, startQuote: String, endQuote: String,
        escape: String?, casing: Casing?
    ): String {
        var s = s
        assert(startQuote.length() === 1)
        assert(endQuote.length() === 1)
        assert(s.startsWith(startQuote) && s.endsWith(endQuote)) { s }
        s = s.substring(1, s.length() - 1).replace(escape, endQuote)
        return toCase(s, casing)
    }

    /**
     * Converts an identifier to a particular casing.
     */
    fun toCase(s: String, casing: Casing?): String {
        return when (casing) {
            TO_UPPER -> s.toUpperCase(Locale.ROOT)
            TO_LOWER -> s.toLowerCase(Locale.ROOT)
            else -> s
        }
    }

    /**
     * Trims a string for given characters from left and right. E.g.
     * `trim("aBaac123AabC","abBcC")` returns `"123A"`.
     */
    fun trim(
        s: String,
        chars: String
    ): String {
        if (s.length() === 0) {
            return ""
        }
        var start: Int
        start = 0
        while (start < s.length()) {
            val c: Char = s.charAt(start)
            if (chars.indexOf(c) < 0) {
                break
            }
            start++
        }
        var stop: Int
        stop = s.length()
        while (stop > start) {
            val c: Char = s.charAt(stop - 1)
            if (chars.indexOf(c) < 0) {
                break
            }
            stop--
        }
        return if (start >= stop) {
            ""
        } else s.substring(start, stop)
    }

    @Deprecated // to be removed before 2.0
    fun findPos(sql: String): StringAndPos {
        return StringAndPos.of(sql)
    }

    /**
     * Returns the (1-based) line and column corresponding to a particular
     * (0-based) offset in a string.
     *
     *
     * Converse of [.lineColToIndex].
     */
    fun indexToLineCol(sql: String, i: Int): IntArray {
        var line = 0
        var j = 0
        while (true) {
            val prevj = j
            j = nextLine(sql, j)
            if (j < 0 || j > i) {
                return intArrayOf(line + 1, i - prevj + 1)
            }
            ++line
        }
    }

    fun nextLine(sql: String, j: Int): Int {
        val rn: Int = sql.indexOf("\r\n", j)
        val r: Int = sql.indexOf("\r", j)
        val n: Int = sql.indexOf("\n", j)
        return if (r < 0 && n < 0) {
            assert(rn < 0)
            -1
        } else if (rn >= 0 && rn < n && rn <= r) {
            rn + 2 // looking at "\r\n"
        } else if (r >= 0 && r < n) {
            r + 1 // looking at "\r"
        } else {
            n + 1 // looking at "\n"
        }
    }

    /**
     * Finds the position (0-based) in a string which corresponds to a given
     * line and column (1-based).
     *
     *
     * Converse of [.indexToLineCol].
     */
    fun lineColToIndex(sql: String, line: Int, column: Int): Int {
        var line = line
        var column = column
        --line
        --column
        var i = 0
        while (line-- > 0) {
            i = nextLine(sql, i)
        }
        return i + column
    }

    /**
     * Converts a string to a string with one or two carets in it. For example,
     * `addCarets("values (foo)", 1, 9, 1, 12)` yields "values
     * (^foo^)".
     */
    fun addCarets(
        sql: String,
        line: Int,
        col: Int,
        endLine: Int,
        endCol: Int
    ): String {
        var sqlWithCarets: String
        var cut = lineColToIndex(sql, line, col)
        sqlWithCarets = (sql.substring(0, cut) + "^"
                + sql.substring(cut))
        if (col != endCol || line != endLine) {
            cut = lineColToIndex(sqlWithCarets, endLine, endCol)
            if (line == endLine) {
                ++cut // for caret
            }
            if (cut < sqlWithCarets.length()) {
                sqlWithCarets = (sqlWithCarets.substring(0, cut)
                        + "^" + sqlWithCarets.substring(cut))
            } else {
                sqlWithCarets += "^"
            }
        }
        return sqlWithCarets
    }

    @Nullable
    fun getTokenVal(token: String): String? {
        // We don't care about the token which are not string
        if (!token.startsWith("\"")) {
            return null
        }

        // Remove the quote from the token
        val startIndex: Int = token.indexOf("\"")
        val endIndex: Int = token.lastIndexOf("\"")
        val tokenVal: String = token.substring(startIndex + 1, endIndex)
        val c: Char = tokenVal.charAt(0)
        return if (Character.isLetter(c)) {
            tokenVal
        } else null
    }

    /**
     * Extracts the values from a collation name.
     *
     *
     * Collation names are on the form *charset$locale$strength*.
     *
     * @param in The collation name
     * @return A [ParsedCollation]
     */
    fun parseCollation(`in`: String?): ParsedCollation {
        val st = StringTokenizer(`in`, "$")
        val charsetStr: String = st.nextToken()
        val localeStr: String = st.nextToken()
        val strength: String
        strength = if (st.countTokens() > 0) {
            st.nextToken()
        } else {
            CalciteSystemProperty.DEFAULT_COLLATION_STRENGTH.value()
        }
        val charset: Charset = SqlUtil.getCharset(charsetStr)
        val localeParts: Array<String> = localeStr.split("_")
        val locale: Locale
        if (1 == localeParts.size) {
            locale = Locale(localeParts[0])
        } else if (2 == localeParts.size) {
            locale = Locale(localeParts[0], localeParts[1])
        } else if (3 == localeParts.size) {
            locale = Locale(localeParts[0], localeParts[1], localeParts[2])
        } else {
            throw RESOURCE.illegalLocaleFormat(localeStr).ex()
        }
        return ParsedCollation(charset, locale, strength)
    }

    @Deprecated // to be removed before 2.0
    fun toStringArray(list: List<String?>): Array<String> {
        return list.toArray(arrayOfNulls<String>(0))
    }

    fun toNodeArray(list: List<SqlNode?>): Array<SqlNode> {
        return list.toArray(arrayOfNulls<SqlNode>(0))
    }

    fun toNodeArray(list: SqlNodeList): Array<SqlNode> {
        return list.toArray(arrayOfNulls<SqlNode>(0))
    }

    /** Converts "ROW (1, 2)" to "(1, 2)"
     * and "3" to "(3)".  */
    fun stripRow(n: SqlNode): SqlNodeList {
        val list: List<SqlNode>
        list = when (n.getKind()) {
            ROW -> (n as SqlCall).getOperandList()
            else -> ImmutableList.of(n)
        }
        return SqlNodeList(list, n.getParserPosition())
    }

    @Deprecated // to be removed before 2.0
    fun rightTrim(
        s: String,
        c: Char
    ): String {
        var stop: Int
        stop = s.length()
        while (stop > 0) {
            if (s.charAt(stop - 1) !== c) {
                break
            }
            stop--
        }
        return if (stop > 0) {
            s.substring(0, stop)
        } else ""
    }

    /**
     * Replaces a range of elements in a list with a single element. For
     * example, if list contains `{A, B, C, D, E}` then `
     * replaceSublist(list, X, 1, 4)` returns `{A, X, E}`.
     */
    fun <T> replaceSublist(
        list: List<T>,
        start: Int,
        end: Int,
        o: T
    ) {
        requireNonNull(list, "list")
        Preconditions.checkArgument(start < end)
        for (i in end - 1 downTo start + 1) {
            list.remove(i)
        }
        list.set(start, o)
    }

    /**
     * Converts a list of {expression, operator, expression, ...} into a tree,
     * taking operator precedence and associativity into account.
     */
    @Nullable
    fun toTree(list: List<Object>): SqlNode {
        if (list.size() === 1
            && list[0] is SqlNode
        ) {
            // Short-cut for the simple common case
            return list[0] as SqlNode
        }
        LOGGER.trace("Attempting to reduce {}", list)
        val tokenSequence = OldTokenSequenceImpl(list)
        val node: SqlNode = toTreeEx(tokenSequence, 0, 0, SqlKind.OTHER)
        LOGGER.debug("Reduced {}", node)
        return node
    }

    /**
     * Converts a list of {expression, operator, expression, ...} into a tree,
     * taking operator precedence and associativity into account.
     *
     * @param list        List of operands and operators. This list is modified as
     * expressions are reduced.
     * @param start       Position of first operand in the list. Anything to the
     * left of this (besides the immediately preceding operand)
     * is ignored. Generally use value 1.
     * @param minPrec     Minimum precedence to consider. If the method encounters
     * an operator of lower precedence, it doesn't reduce any
     * further.
     * @param stopperKind If not [SqlKind.OTHER], stop reading the list if
     * we encounter a token of this kind.
     * @return the root node of the tree which the list condenses into
     */
    fun toTreeEx(
        list: SqlSpecialOperator.TokenSequence,
        start: Int, minPrec: Int, stopperKind: SqlKind
    ): SqlNode {
        val parser: PrecedenceClimbingParser = list.parser(
            start
        ) { token ->
            if (token is PrecedenceClimbingParser.Op) {
                val tokenOp: PrecedenceClimbingParser.Op = token as PrecedenceClimbingParser.Op
                val op: SqlOperator = (tokenOp.o() as ToTreeListItem).op
                return@parser (stopperKind !== SqlKind.OTHER
                        && op.kind === stopperKind
                        || minPrec > 0
                        && op.getLeftPrec() < minPrec)
            } else {
                return@parser false
            }
        }
        val beforeSize: Int = parser.all().size()
        parser.partialParse()
        val afterSize: Int = parser.all().size()
        val node: SqlNode = convert(parser.all().get(0))
        list.replaceSublist(start, start + beforeSize - afterSize + 1, node)
        return node
    }

    private fun convert(token: PrecedenceClimbingParser.Token): SqlNode {
        return when (token.type) {
            ATOM -> requireNonNull(token.o as SqlNode)
            CALL -> {
                val call: Call = token as Call
                val list: List<SqlNode> = ArrayList()
                for (arg in call.args) {
                    list.add(convert(arg))
                }
                val item = call.op.o() as ToTreeListItem
                if (list.size() === 1) {
                    val firstItem: SqlNode = list[0]
                    if (item.op === SqlStdOperatorTable.UNARY_MINUS
                        && firstItem is SqlNumericLiteral
                    ) {
                        return SqlLiteral.createNegative(
                            firstItem as SqlNumericLiteral,
                            item.pos.plusAll(list)
                        )
                    }
                    if (item.op === SqlStdOperatorTable.UNARY_PLUS
                        && firstItem is SqlNumericLiteral
                    ) {
                        return firstItem
                    }
                }
                item.op.createCall(item.pos.plusAll(list), list)
            }
            else -> throw AssertionError(token)
        }
    }

    /**
     * Checks a UESCAPE string for validity, and returns the escape character if
     * no exception is thrown.
     *
     * @param s UESCAPE string to check
     * @return validated escape character
     */
    fun checkUnicodeEscapeChar(s: String): Char {
        if (s.length() !== 1) {
            throw RESOURCE.unicodeEscapeCharLength(s).ex()
        }
        val c: Char = s.charAt(0)
        if (Character.isDigit(c)
            || Character.isWhitespace(c)
            || c == '+'
            || c == '"'
            || c >= 'a' && c <= 'f'
            || c >= 'A' && c <= 'F'
        ) {
            throw RESOURCE.unicodeEscapeCharIllegal(s).ex()
        }
        return c
    }

    /**
     * Returns whether the reported ParseException tokenImage
     * allows SQL identifier.
     *
     * @param tokenImage The allowed tokens from the ParseException
     * @param expectedTokenSequences Expected token sequences
     *
     * @return true if SQL identifier is allowed
     */
    fun allowsIdentifier(tokenImage: Array<String>, expectedTokenSequences: Array<IntArray>): Boolean {
        // Compares from tailing tokens first because the <IDENTIFIER>
        // was very probably at the tail.
        for (i in expectedTokenSequences.indices.reversed()) {
            val expectedTokenSequence = expectedTokenSequences[i]
            for (j in expectedTokenSequence.indices.reversed()) {
                if (tokenImage[expectedTokenSequence[j]].equals("<IDENTIFIER>")) {
                    return true
                }
            }
        }
        return false
    }
    //~ Inner Classes ----------------------------------------------------------
    /** The components of a collation definition, per the SQL standard.  */
    class ParsedCollation(
        charset: Charset,
        locale: Locale,
        strength: String
    ) {
        private val charset: Charset
        private val locale: Locale
        val strength: String

        init {
            this.charset = charset
            this.locale = locale
            this.strength = strength
        }

        fun getCharset(): Charset {
            return charset
        }

        fun getLocale(): Locale {
            return locale
        }
    }

    /**
     * Class that holds a [SqlOperator] and a [SqlParserPos]. Used
     * by [SqlSpecialOperator.reduceExpr] and the parser to associate a
     * parsed operator with a parser position.
     */
    class ToTreeListItem(
        op: SqlOperator,
        pos: SqlParserPos
    ) {
        val op: SqlOperator
        val pos: SqlParserPos

        init {
            this.op = op
            this.pos = pos
        }

        @Override
        override fun toString(): String {
            return op.toString()
        }

        val operator: SqlOperator
            get() = op

        fun getPos(): SqlParserPos {
            return pos
        }
    }

    /** Implementation of
     * [org.apache.calcite.sql.SqlSpecialOperator.TokenSequence]
     * based on an existing parser.  */
    private class TokenSequenceImpl(parser: PrecedenceClimbingParser) : SqlSpecialOperator.TokenSequence {
        val list: List<PrecedenceClimbingParser.Token>
        val parser: PrecedenceClimbingParser

        init {
            this.parser = parser
            list = parser.all()
        }

        @Override
        fun parser(
            start: Int,
            predicate: Predicate<PrecedenceClimbingParser.Token?>?
        ): PrecedenceClimbingParser {
            return parser.copy(start, predicate)
        }

        @Override
        fun size(): Int {
            return list.size()
        }

        @Override
        fun op(i: Int): SqlOperator {
            val o = requireNonNull(
                list[i].o
            ) { "list.get($i).o is null in $list" } as ToTreeListItem
            return o.operator
        }

        @Override
        fun pos(i: Int): SqlParserPos {
            return pos(list[i])
        }

        @Override
        fun isOp(i: Int): Boolean {
            return list[i].o is ToTreeListItem
        }

        @Override
        fun node(i: Int): SqlNode {
            return convert(list[i])
        }

        @Override
        fun replaceSublist(start: Int, end: Int, e: SqlNode?) {
            replaceSublist(list, start, end, parser.atom(e))
        }

        companion object {
            private fun pos(token: PrecedenceClimbingParser.Token): SqlParserPos {
                return when (token.type) {
                    ATOM -> requireNonNull(token.o as SqlNode, "token.o").getParserPosition()
                    CALL -> {
                        val call: Call = token as Call
                        var pos: SqlParserPos = (call.op.o() as ToTreeListItem).pos
                        for (arg in call.args) {
                            pos = pos.plus(pos(arg))
                        }
                        pos
                    }
                    else -> requireNonNull(token.o as ToTreeListItem, "token.o").getPos()
                }
            }
        }
    }

    /** Implementation of
     * [org.apache.calcite.sql.SqlSpecialOperator.TokenSequence].  */
    private class OldTokenSequenceImpl(list: List<Object>) : SqlSpecialOperator.TokenSequence {
        val list: List<Object>

        init {
            this.list = list
        }

        @Override
        fun parser(
            start: Int,
            predicate: Predicate<PrecedenceClimbingParser.Token?>?
        ): PrecedenceClimbingParser {
            val builder: PrecedenceClimbingParser.Builder = Builder()
            for (o in Util.skip(list, start)) {
                if (o is ToTreeListItem) {
                    val item = o as ToTreeListItem
                    val op: SqlOperator = item.operator
                    if (op is SqlPrefixOperator) {
                        builder.prefix(item, op.getLeftPrec())
                    } else if (op is SqlPostfixOperator) {
                        builder.postfix(item, op.getRightPrec())
                    } else if (op is SqlBinaryOperator) {
                        builder.infix(
                            item, op.getLeftPrec(),
                            op.getLeftPrec() < op.getRightPrec()
                        )
                    } else if (op is SqlSpecialOperator) {
                        builder.special(
                            item, op.getLeftPrec(), op.getRightPrec()
                        ) { parser, op2 ->
                            val tokens: List<PrecedenceClimbingParser.Token> = parser.all()
                            val op1: SqlSpecialOperator =
                                requireNonNull(op2.o as ToTreeListItem, "op2.o").op as SqlSpecialOperator
                            val r: SqlSpecialOperator.ReduceResult = op1.reduceExpr(
                                tokens.indexOf(op2),
                                TokenSequenceImpl(parser)
                            )
                            Result(
                                tokens[r.startOrdinal],
                                tokens[r.endOrdinal - 1],
                                parser.atom(r.node)
                            )
                        }
                    } else {
                        throw AssertionError()
                    }
                } else {
                    builder.atom(requireNonNull(o, "o"))
                }
            }
            return builder.build()
        }

        @Override
        fun size(): Int {
            return list.size()
        }

        @Override
        fun op(i: Int): SqlOperator {
            val item = requireNonNull(
                list[i]
            ) { "list.get($i)" } as ToTreeListItem
            return item.op
        }

        @Override
        fun pos(i: Int): SqlParserPos {
            val o: Object = list[i]
            return if (o is ToTreeListItem) (o as ToTreeListItem).pos else requireNonNull(o as SqlNode) { "item $i is null in $list" }
                .getParserPosition()
        }

        @Override
        fun isOp(i: Int): Boolean {
            return list[i] is ToTreeListItem
        }

        @Override
        fun node(i: Int): SqlNode {
            return requireNonNull(list[i] as SqlNode)
        }

        @Override
        fun replaceSublist(start: Int, end: Int, e: SqlNode) {
            replaceSublist<Any>(list, start, end, e)
        }
    }

    /** Pre-initialized [DateFormat] objects, to be used within the current
     * thread, because `DateFormat` is not thread-safe.  */
    private class Format {
        val timestamp: DateFormat = SimpleDateFormat(
            DateTimeUtils.TIMESTAMP_FORMAT_STRING,
            Locale.ROOT
        )
        val time: DateFormat = SimpleDateFormat(DateTimeUtils.TIME_FORMAT_STRING, Locale.ROOT)
        val date: DateFormat = SimpleDateFormat(DateTimeUtils.DATE_FORMAT_STRING, Locale.ROOT)

        companion object {
            private val PER_THREAD: ThreadLocal<Format> = ThreadLocal.withInitial { Format() }
            fun get(): Format {
                return requireNonNull(PER_THREAD.get(), "PER_THREAD.get()")
            }
        }
    }
}
