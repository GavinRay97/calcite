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
package org.apache.calcite.sql.pretty

import org.apache.calcite.avatica.util.Spaces

/**
 * Pretty printer for SQL statements.
 *
 *
 * There are several options to control the format.
 *
 * <table>
 * <caption>Formatting options</caption>
 * <tr>
 * <th>Option</th>
 * <th>Description</th>
 * <th>Default</th>
</tr> *
 *
 * <tr>
 * <td>[SqlWriterConfig.clauseStartsLine] ClauseStartsLine}</td>
 * <td>Whether a clause (`FROM`, `WHERE`, `GROUP BY`,
 * `HAVING`, `WINDOW`, `ORDER BY`) starts a new line.
 * `SELECT` is always at the start of a line.</td>
 * <td>true</td>
</tr> *
 *
 * <tr>
 * <td>[ClauseEndsLine][SqlWriterConfig.clauseEndsLine]</td>
 * <td>Whether a clause (`SELECT`, `FROM`, `WHERE`,
 * `GROUP BY`, `HAVING`, `WINDOW`, `ORDER BY`) is
 * followed by a new line.</td>
 * <td>false</td>
</tr> *
 *
 * <tr>
 * <td>[CaseClausesOnNewLines][SqlWriterConfig.caseClausesOnNewLines]</td>
 * <td>Whether the WHEN, THEN and ELSE clauses of a CASE expression appear at
 * the start of a new line.</td>
 * <td>false</td>
</tr> *
 *
 * <tr>
 * <td>[Indentation][SqlWriterConfig.indentation]</td>
 * <td>Number of spaces to indent</td>
 * <td>4</td>
</tr> *
 *
 * <tr>
 * <td>[KeywordsLowerCase][SqlWriterConfig.keywordsLowerCase]</td>
 * <td>Whether to print keywords (SELECT, AS, etc.) in lower-case.</td>
 * <td>false</td>
</tr> *
 *
 * <tr>
 * <td>[AlwaysUseParentheses][SqlWriterConfig.alwaysUseParentheses]</td>
 * <td>
 *
 *Whether to enclose all expressions in parentheses, even if the
 * operator has high enough precedence that the parentheses are not required.
 *
 *
 * For example, the parentheses are required in the expression
 * `(a + b) * c` because the '*' operator has higher precedence than the
 * '+' operator, and so without the parentheses, the expression would be
 * equivalent to `a + (b * c)`. The fully-parenthesized expression,
 * `((a + b) * c)` is unambiguous even if you don't know the precedence
 * of every operator.</td>
 * <td>false</td>
</tr> *
 *
 * <tr>
 * <td>[QuoteAllIdentifiers][SqlWriterConfig.quoteAllIdentifiers]</td>
 * <td>Whether to quote all identifiers, even those which would be correct
 * according to the rules of the [SqlDialect] if quotation marks were
 * omitted.</td>
 * <td>true</td>
</tr> *
 *
 * <tr>
 * <td>[SubQueryStyle][SqlWriterConfig.subQueryStyle]</td>
 * <td>Style for formatting sub-queries. Values are:
 * [Hyde][org.apache.calcite.sql.SqlWriter.SubQueryStyle.HYDE],
 * [Black][org.apache.calcite.sql.SqlWriter.SubQueryStyle.BLACK].</td>
 *
 * <td>[Hyde][org.apache.calcite.sql.SqlWriter.SubQueryStyle.HYDE]</td>
</tr> *
 *
 * <tr>
 * <td>[LineLength][SqlWriterConfig.lineLength]</td>
 * <td>The desired maximum length for lines (to look nice in editors,
 * printouts, etc.).</td>
 * <td>-1 (no maximum)</td>
</tr> *
 *
 * <tr>
 * <td>[FoldLength][SqlWriterConfig.foldLength]</td>
 * <td>The line length at which lines are folded or chopped down
 * (see `LineFolding`). Only has an effect if clauses are marked
 * [CHOP][SqlWriterConfig.LineFolding.CHOP] or
 * [FOLD][SqlWriterConfig.LineFolding.FOLD].</td>
 * <td>80</td>
</tr> *
 *
 * <tr>
 * <td>[LineFolding][SqlWriterConfig.lineFolding]</td>
 * <td>How long lines are to be handled. Options are lines are
 * WIDE (do not wrap),
 * FOLD (wrap if long),
 * CHOP (chop down if long),
 * and TALL (wrap always).</td>
 * <td>WIDE</td>
</tr> *
 *
 * <tr>
 * <td>[SelectFolding][SqlWriterConfig.selectFolding]</td>
 * <td>How the `SELECT` clause is to be folded.</td>
 * <td>`LineFolding`</td>
</tr> *
 *
 * <tr>
 * <td>[FromFolding][SqlWriterConfig.fromFolding]</td>
 * <td>How the `FROM` clause and nested `JOIN` clauses are to be
 * folded.</td>
 * <td>`LineFolding`</td>
</tr> *
 *
 * <tr>
 * <td>[WhereFolding][SqlWriterConfig.whereFolding]</td>
 * <td>How the `WHERE` clause is to be folded.</td>
 * <td>`LineFolding`</td>
</tr> *
 *
 * <tr>
 * <td>[GroupByFolding][SqlWriterConfig.groupByFolding]</td>
 * <td>How the `GROUP BY` clause is to be folded.</td>
 * <td>`LineFolding`</td>
</tr> *
 *
 * <tr>
 * <td>[HavingFolding][SqlWriterConfig.havingFolding]</td>
 * <td>How the `HAVING` clause is to be folded.</td>
 * <td>`LineFolding`</td>
</tr> *
 *
 * <tr>
 * <td>[OrderByFolding][SqlWriterConfig.orderByFolding]</td>
 * <td>How the `ORDER BY` clause is to be folded.</td>
 * <td>`LineFolding`</td>
</tr> *
 *
 * <tr>
 * <td>[WindowFolding][SqlWriterConfig.windowFolding]</td>
 * <td>How the `WINDOW` clause is to be folded.</td>
 * <td>`LineFolding`</td>
</tr> *
 *
 * <tr>
 * <td>[OverFolding][SqlWriterConfig.overFolding]</td>
 * <td>How window declarations in the `WINDOW` clause
 * and in the `OVER` clause of aggregate functions are to be folded.</td>
 * <td>`LineFolding`</td>
</tr> *
 *
 * <tr>
 * <td>[ValuesFolding][SqlWriterConfig.valuesFolding]</td>
 * <td>How lists of values in the `VALUES` clause are to be folded.</td>
 * <td>`LineFolding`</td>
</tr> *
 *
 * <tr>
 * <td>[UpdateSetFolding][SqlWriterConfig.updateSetFolding]</td>
 * <td>How assignments in the `SET` clause of an `UPDATE` statement
 * are to be folded.</td>
 * <td>`LineFolding`</td>
</tr> *
 *
</table> *
 *
 *
 * The following options exist for backwards compatibility. They are
 * used if [LineFolding][SqlWriterConfig.lineFolding] and clause-specific
 * options such as [SelectFolding][SqlWriterConfig.selectFolding] are not
 * specified:
 *
 *
 *
 *  * [SelectListItemsOnSeparateLines][SqlWriterConfig.selectListItemsOnSeparateLines]
 * replaced by [SelectFolding][SqlWriterConfig.selectFolding],
 * [GroupByFolding][SqlWriterConfig.groupByFolding], and
 * [OrderByFolding][SqlWriterConfig.orderByFolding];
 *
 *  * [UpdateSetListNewline][SqlWriterConfig.updateSetListNewline]
 * replaced by [UpdateSetFolding][SqlWriterConfig.updateSetFolding];
 *
 *  * [WindowDeclListNewline][SqlWriterConfig.windowDeclListNewline]
 * replaced by [WindowFolding][SqlWriterConfig.windowFolding];
 *
 *  * [WindowNewline][SqlWriterConfig.windowNewline]
 * replaced by [OverFolding][SqlWriterConfig.overFolding];
 *
 *  * [ValuesListNewline][SqlWriterConfig.valuesListNewline]
 * replaced by [ValuesFolding][SqlWriterConfig.valuesFolding].
 *
 *
 */
class SqlPrettyWriter @SuppressWarnings("method.invocation.invalid") private constructor(
    config: SqlWriterConfig,
    buf: StringBuilder, @SuppressWarnings("unused") ignore: Boolean
) : SqlWriter {
    //~ Instance fields --------------------------------------------------------
    private val dialect: SqlDialect
    private val buf: StringBuilder
    private val listStack: Deque<FrameImpl> = ArrayDeque()
    private var dynamicParameters: @Nullable ImmutableList.Builder<Integer>? = null

    @Nullable
    protected var frame: FrameImpl? = null
    private var needWhitespace = false

    @Nullable
    protected var nextWhitespace: String? = null
    private var config: SqlWriterConfig? = null

    /**
     * Returns an object which encapsulates each property as a get/set method.
     */
    @Nullable
    private var bean: Bean? = null
        private get() {
            if (field == null) {
                field = Bean(this)
            }
            return field
        }
    private var currentIndent = 0
    private var lineStart = 0

    //~ Constructors -----------------------------------------------------------
    init {
        this.buf = requireNonNull(buf, "buf")
        dialect = requireNonNull(config.dialect())
        this.config = requireNonNull(config, "config")
        lineStart = 0
        reset()
    }

    /** Creates a writer with the given configuration
     * and a given buffer to write to.  */
    constructor(
        config: SqlWriterConfig?,
        buf: StringBuilder?
    ) : this(config, requireNonNull(buf, "buf"), false) {
    }

    /** Creates a writer with the given configuration and dialect,
     * and a given print writer (or a private print writer if it is null).  */
    constructor(
        dialect: SqlDialect?,
        config: SqlWriterConfig,
        buf: StringBuilder?
    ) : this(config.withDialect(requireNonNull(dialect, "dialect")), buf) {
    }

    /** Creates a writer with the given configuration
     * and a private print writer.  */
    @Deprecated
    constructor(dialect: SqlDialect?, config: SqlWriterConfig) : this(
        config.withDialect(
            requireNonNull(
                dialect,
                "dialect"
            )
        )
    ) {
    }

    @Deprecated
    constructor(
        dialect: SqlDialect?,
        alwaysUseParentheses: Boolean,
        pw: PrintWriter?
    ) : this(
        config().withDialect(requireNonNull(dialect, "dialect"))
            .withAlwaysUseParentheses(alwaysUseParentheses)
    ) {
        // NOTE that 'pw' is ignored; there is no place for it in the new API
    }

    @Deprecated
    constructor(
        dialect: SqlDialect?,
        alwaysUseParentheses: Boolean
    ) : this(
        config().withDialect(requireNonNull(dialect, "dialect"))
            .withAlwaysUseParentheses(alwaysUseParentheses)
    ) {
    }

    /** Creates a writer with a given dialect, the default configuration
     * and a private print writer.  */
    @Deprecated
    constructor(dialect: SqlDialect?) : this(config().withDialect(requireNonNull(dialect, "dialect"))) {
    }
    /** Creates a writer with the given configuration,
     * and a private builder.  */
    /** Creates a writer with the default configuration.
     *
     * @see .config
     */
    @JvmOverloads
    constructor(config: SqlWriterConfig? = config()) : this(config, StringBuilder(), true) {
    }

    //~ Methods ----------------------------------------------------------------
    @Deprecated
    fun setCaseClausesOnNewLines(caseClausesOnNewLines: Boolean) {
        config = config.withCaseClausesOnNewLines(caseClausesOnNewLines)
    }

    @Deprecated
    fun setSubQueryStyle(subQueryStyle: SubQueryStyle?) {
        config = config.withSubQueryStyle(subQueryStyle)
    }

    @Deprecated
    fun setWindowNewline(windowNewline: Boolean) {
        config = config.withWindowNewline(windowNewline)
    }

    @Deprecated
    fun setWindowDeclListNewline(windowDeclListNewline: Boolean) {
        config = config.withWindowDeclListNewline(windowDeclListNewline)
    }

    @get:Override
    @get:Deprecated
    @set:Deprecated
    var indentation: Int
        get() = config.indentation()
        set(indentation) {
            config = config.withIndentation(indentation)
        }

    @get:Override
    @get:Deprecated
    @set:Deprecated
    var isAlwaysUseParentheses: Boolean
        get() = config.alwaysUseParentheses()
        set(alwaysUseParentheses) {
            config = config.withAlwaysUseParentheses(alwaysUseParentheses)
        }

    @Override
    fun inQuery(): Boolean {
        return (frame == null
                || frame!!.frameType === FrameTypeEnum.ORDER_BY
                || frame!!.frameType === FrameTypeEnum.WITH
                || frame!!.frameType === FrameTypeEnum.SETOP)
    }

    @get:Override
    @get:Deprecated
    @set:Deprecated
    var isQuoteAllIdentifiers: Boolean
        get() = config.quoteAllIdentifiers()
        set(quoteAllIdentifiers) {
            config = config.withQuoteAllIdentifiers(quoteAllIdentifiers)
        }

    @get:Override
    @get:Deprecated
    @set:Deprecated
    var isClauseStartsLine: Boolean
        get() = config.clauseStartsLine()
        set(clauseStartsLine) {
            config = config.withClauseStartsLine(clauseStartsLine)
        }

    @get:Override
    @get:Deprecated
    @set:Deprecated
    var isSelectListItemsOnSeparateLines: Boolean
        get() = config.selectListItemsOnSeparateLines()
        set(b) {
            config = config.withSelectListItemsOnSeparateLines(b)
        }

    @get:Deprecated
    @set:Deprecated
    var isWhereListItemsOnSeparateLines: Boolean
        get() = config.whereListItemsOnSeparateLines()
        set(whereListItemsOnSeparateLines) {
            config = config.withWhereListItemsOnSeparateLines(whereListItemsOnSeparateLines)
        }

    @get:Deprecated
    @set:Deprecated
    var isSelectListExtraIndentFlag: Boolean
        get() = config.selectListExtraIndentFlag()
        set(selectListExtraIndentFlag) {
            config = config.withSelectListExtraIndentFlag(selectListExtraIndentFlag)
        }

    @get:Override
    @get:Deprecated
    @set:Deprecated
    var isKeywordsLowerCase: Boolean
        get() = config.keywordsLowerCase()
        set(keywordsLowerCase) {
            config = config.withKeywordsLowerCase(keywordsLowerCase)
        }

    @get:Deprecated
    @set:Deprecated
    var lineLength: Int
        get() = config.lineLength()
        set(lineLength) {
            config = config.withLineLength(lineLength)
        }

    @Override
    fun resetSettings() {
        reset()
        config = config()
    }

    @Override
    fun reset() {
        buf.setLength(0)
        lineStart = 0
        dynamicParameters = null
        setNeedWhitespace(false)
        nextWhitespace = " "
    }

    /**
     * Prints the property settings of this pretty-writer to a writer.
     *
     * @param pw           Writer
     * @param omitDefaults Whether to omit properties whose value is the same as
     * the default
     */
    fun describe(pw: PrintWriter, omitDefaults: Boolean) {
        val properties = bean!!
        val propertyNames: Array<String> = properties.getPropertyNames()
        var count = 0
        for (key in propertyNames) {
            val value: Object = properties[key]
            val defaultValue: Object = DEFAULT_BEAN[key]
            if (Objects.equals(value, defaultValue)) {
                continue
            }
            if (count++ > 0) {
                pw.print(",")
            }
            pw.print("$key=$value")
        }
    }

    /**
     * Sets settings from a properties object.
     */
    fun setSettings(properties: Properties) {
        resetSettings()
        val bean = bean!!
        val propertyNames: Array<String> = bean.getPropertyNames()
        for (propertyName in propertyNames) {
            val value: String = properties.getProperty(propertyName)
            if (value != null) {
                bean[propertyName] = value
            }
        }
    }

    @Override
    fun newlineAndIndent() {
        newlineAndIndent(currentIndent)
    }

    fun newlineAndIndent(indent: Int) {
        buf.append(NL)
        lineStart = buf.length()
        indent(indent)
        setNeedWhitespace(false) // no further whitespace necessary
    }

    fun indent(indent: Int) {
        if (indent < 0) {
            throw IllegalArgumentException("negative indent $indent")
        }
        Spaces.append(buf, indent)
    }

    /**
     * Creates a list frame.
     *
     *
     * Derived classes should override this method to specify the indentation
     * of the list.
     *
     * @param frameType What type of list
     * @param keyword   The keyword to be printed at the start of the list
     * @param open      The string to print at the start of the list
     * @param close     The string to print at the end of the list
     * @return A frame
     */
    protected fun createListFrame(
        frameType: FrameType?,
        @Nullable keyword: String?,
        open: String?,
        close: String
    ): FrameImpl {
        var open = open
        var close = close
        val frameTypeEnum: FrameTypeEnum =
            if (frameType is FrameTypeEnum) frameType as FrameTypeEnum? else FrameTypeEnum.OTHER
        val indentation: Int = config.indentation()
        var newlineAfterOpen = false
        var newlineBeforeSep = false
        var newlineAfterSep = false
        var newlineBeforeClose = false
        var left = column()
        var sepIndent = indentation
        var extraIndent = 0
        val fold: SqlWriterConfig.LineFolding = fold(frameTypeEnum)
        val newline = fold === SqlWriterConfig.LineFolding.TALL
        when (frameTypeEnum) {
            SELECT -> {
                extraIndent = indentation
                newlineAfterOpen = false
                newlineBeforeSep = config.clauseStartsLine() // newline before FROM, WHERE etc.
                newlineAfterSep = false
                sepIndent = 0 // all clauses appear below SELECT
            }
            SETOP -> {
                extraIndent = 0
                newlineAfterOpen = false
                newlineBeforeSep = config.clauseStartsLine() // newline before UNION, EXCEPT
                newlineAfterSep = config.clauseStartsLine() // newline after UNION, EXCEPT
                sepIndent = 0 // all clauses appear below SELECT
            }
            SELECT_LIST, FROM_LIST, JOIN, GROUP_BY_LIST, ORDER_BY_LIST, WINDOW_DECL_LIST, VALUES -> {
                if (config.selectListExtraIndentFlag()) {
                    extraIndent = indentation
                }
                left = if (frame == null) 0 else frame!!.left
                newlineAfterOpen = (config.clauseEndsLine()
                        && (fold === SqlWriterConfig.LineFolding.TALL
                        || fold === SqlWriterConfig.LineFolding.STEP))
                newlineBeforeSep = false
                newlineAfterSep = newline
                if (config.leadingComma() && newline) {
                    newlineBeforeSep = true
                    newlineAfterSep = false
                    sepIndent = -", ".length()
                }
            }
            WHERE_LIST, WINDOW -> {
                extraIndent = indentation
                newlineAfterOpen = newline && config.clauseEndsLine()
                newlineBeforeSep = newline
                sepIndent = 0
                newlineAfterSep = false
            }
            ORDER_BY, OFFSET, FETCH -> {
                newlineAfterOpen = false
                newlineBeforeSep = true
                sepIndent = 0
                newlineAfterSep = false
            }
            UPDATE_SET_LIST -> {
                extraIndent = indentation
                newlineAfterOpen = newline
                newlineBeforeSep = false
                sepIndent = 0
                newlineAfterSep = newline
            }
            CASE -> {
                newlineAfterOpen = newline
                newlineBeforeSep = newline
                newlineBeforeClose = newline
                sepIndent = 0
            }
            else -> {}
        }
        val chopColumn: Int
        var lineFolding: SqlWriterConfig.LineFolding = config.lineFolding()
        if (lineFolding == null) {
            lineFolding = SqlWriterConfig.LineFolding.WIDE
            chopColumn = -1
        } else {
            if (config.foldLength() > 0
                && (lineFolding === SqlWriterConfig.LineFolding.CHOP || lineFolding === SqlWriterConfig.LineFolding.FOLD || lineFolding === SqlWriterConfig.LineFolding.STEP)
            ) {
                chopColumn = left + config.foldLength()
            } else {
                chopColumn = -1
            }
        }
        return when (frameTypeEnum) {
            UPDATE_SET_LIST, WINDOW_DECL_LIST, VALUES, SELECT, SETOP, SELECT_LIST, WHERE_LIST, ORDER_BY_LIST, GROUP_BY_LIST, WINDOW, ORDER_BY, OFFSET, FETCH -> FrameImpl(
                frameType, keyword, open, close,
                left, extraIndent, chopColumn, lineFolding, newlineAfterOpen,
                newlineBeforeSep, sepIndent, newlineAfterSep, false, false
            )
            SUB_QUERY -> when (config.subQueryStyle()) {
                BLACK -> {
                    // Generate, e.g.:
                    //
                    // WHERE foo = bar IN
                    // (   SELECT ...
                    open = Spaces.padRight("(", indentation - 1)
                    object : FrameImpl(
                        frameType, keyword, open, close,
                        left, 0, chopColumn, lineFolding, false, true, indentation,
                        false, false, false
                    ) {
                        protected fun _before() {
                            newlineAndIndent()
                        }
                    }
                }
                HYDE ->         // Generate, e.g.:
                    //
                    // WHERE foo IN (
                    //     SELECT ...
                    object : FrameImpl(
                        frameType, keyword, open, close, left, 0,
                        chopColumn, lineFolding, false, true, 0, false, false, false
                    ) {
                        protected fun _before() {
                            nextWhitespace = NL
                        }
                    }
                else -> throw Util.unexpected(config.subQueryStyle())
            }
            FUN_CALL -> {
                setNeedWhitespace(false)
                FrameImpl(
                    frameType, keyword, open, close,
                    left, indentation, chopColumn, lineFolding, false, false,
                    indentation, false, false, false
                )
            }
            PARENTHESES -> {
                open = "("
                close = ")"
                FrameImpl(
                    frameType, keyword, open, close,
                    left, indentation, chopColumn, lineFolding, false, false,
                    indentation, false, false, false
                )
            }
            IDENTIFIER, SIMPLE -> FrameImpl(
                frameType, keyword, open, close,
                left, indentation, chopColumn, lineFolding, false, false,
                indentation, false, false, false
            )
            FROM_LIST, JOIN -> {
                newlineBeforeSep = newline
                sepIndent = 0 // all clauses appear below SELECT
                newlineAfterSep = false
                val newlineBeforeComma = config.leadingComma() && newline
                val newlineAfterComma = !config.leadingComma() && newline
                object : FrameImpl(
                    frameType, keyword, open, close, left,
                    indentation, chopColumn, lineFolding, newlineAfterOpen,
                    newlineBeforeSep, sepIndent, newlineAfterSep, false, false
                ) {
                    @Override
                    protected override fun sep(printFirst: Boolean, sep: String) {
                        val newlineBeforeSep: Boolean
                        val newlineAfterSep: Boolean
                        if (sep.equals(",")) {
                            newlineBeforeSep = newlineBeforeComma
                            newlineAfterSep = newlineAfterComma
                        } else {
                            newlineBeforeSep = this.newlineBeforeSep
                            newlineAfterSep = this.newlineAfterSep
                        }
                        if (itemCount == 0) {
                            if (newlineAfterOpen) {
                                newlineAndIndent(currentIndent)
                            }
                        } else {
                            if (newlineBeforeSep) {
                                newlineAndIndent(currentIndent + sepIndent)
                            }
                        }
                        if (itemCount > 0 || printFirst) {
                            keyword(sep)
                            nextWhitespace = if (newlineAfterSep) NL else " "
                        }
                        ++itemCount
                    }
                }
            }
            OTHER -> FrameImpl(
                frameType, keyword, open, close, left, indentation,
                chopColumn, lineFolding, newlineAfterOpen, newlineBeforeSep,
                sepIndent, newlineAfterSep, newlineBeforeClose, false
            )
            else -> FrameImpl(
                frameType, keyword, open, close, left, indentation,
                chopColumn, lineFolding, newlineAfterOpen, newlineBeforeSep,
                sepIndent, newlineAfterSep, newlineBeforeClose, false
            )
        }
    }

    private fun fold(frameType: FrameTypeEnum?): SqlWriterConfig.LineFolding {
        return when (frameType) {
            SELECT_LIST -> f3(
                config.selectFolding(), config.lineFolding(),
                config.selectListItemsOnSeparateLines()
            )
            GROUP_BY_LIST -> f3(
                config.groupByFolding(), config.lineFolding(),
                config.selectListItemsOnSeparateLines()
            )
            ORDER_BY_LIST -> f3(
                config.orderByFolding(), config.lineFolding(),
                config.selectListItemsOnSeparateLines()
            )
            UPDATE_SET_LIST -> f3(
                config.updateSetFolding(), config.lineFolding(),
                config.updateSetListNewline()
            )
            WHERE_LIST -> f3(
                config.whereFolding(), config.lineFolding(),
                config.whereListItemsOnSeparateLines()
            )
            WINDOW_DECL_LIST -> f3(
                config.windowFolding(), config.lineFolding(),
                config.clauseStartsLine() && config.windowDeclListNewline()
            )
            WINDOW -> f3(
                config.overFolding(), config.lineFolding(),
                config.windowNewline()
            )
            VALUES -> f3(
                config.valuesFolding(), config.lineFolding(),
                config.valuesListNewline()
            )
            FROM_LIST, JOIN -> f3(
                config.fromFolding(), config.lineFolding(),
                config.caseClausesOnNewLines()
            )
            CASE -> f3(null, null, config.caseClausesOnNewLines())
            else -> SqlWriterConfig.LineFolding.WIDE
        }
    }

    /**
     * Starts a list.
     *
     * @param frameType Type of list. For example, a SELECT list will be
     * governed according to SELECT-list formatting preferences.
     * @param open      String to print at the start of the list; typically "(" or
     * the empty string.
     * @param close     String to print at the end of the list.
     */
    protected fun startList(
        frameType: FrameType?,
        @Nullable keyword: String?,
        open: String?,
        close: String
    ): Frame? {
        assert(frameType != null)
        var frame = frame
        if (frame != null) {
            if (frame.itemCount++ == 0 && frame.newlineAfterOpen) {
                newlineAndIndent()
            } else if (frameType === FrameTypeEnum.SUB_QUERY
                && config.subQueryStyle() === SubQueryStyle.BLACK
            ) {
                newlineAndIndent(currentIndent - frame.extraIndent)
            }

            // REVIEW jvs 9-June-2006:  This is part of the fix for FRG-149
            // (extra frame for identifier was leading to extra indentation,
            // causing select list to come out raggedy with identifiers
            // deeper than literals); are there other frame types
            // for which extra indent should be suppressed?
            currentIndent += frame.extraIndent(frameType)
            assert(!listStack.contains(frame))
            listStack.push(frame)
        }
        frame = createListFrame(frameType, keyword, open, close)
        this.frame = frame
        frame.before()
        return frame
    }

    @Override
    fun endList(@Nullable frame: Frame?) {
        val endedFrame = frame as FrameImpl?
        Preconditions.checkArgument(
            frame === this.frame,
            "Frame does not match current frame"
        )
        if (endedFrame == null) {
            throw RuntimeException("No list started")
        }
        if (endedFrame.open!!.equals("(")) {
            if (!endedFrame.close.equals(")")) {
                throw RuntimeException("Expected ')'")
            }
        }
        if (endedFrame.newlineBeforeClose) {
            newlineAndIndent()
        }
        keyword(endedFrame.close)
        if (endedFrame.newlineAfterClose) {
            newlineAndIndent()
        }

        // Pop the frame, and move to the previous indentation level.
        if (listStack.isEmpty()) {
            this.frame = null
            assert(currentIndent == 0) { currentIndent }
        } else {
            this.frame = listStack.pop()
            currentIndent -= this.frame!!.extraIndent(endedFrame.frameType)
        }
    }

    fun format(node: SqlNode): String {
        assert(frame == null)
        node.unparse(this, 0, 0)
        assert(frame == null)
        return toString()
    }

    @Override
    override fun toString(): String {
        return buf.toString()
    }

    @Override
    fun toSqlString(): SqlString {
        val dynamicParameters: ImmutableList<Integer>? =
            if (dynamicParameters == null) null else dynamicParameters.build()
        return SqlString(dialect, toString(), dynamicParameters)
    }

    @Override
    fun getDialect(): SqlDialect {
        return dialect
    }

    @Override
    fun literal(s: String) {
        print(s)
        setNeedWhitespace(true)
    }

    @Override
    fun keyword(s: String) {
        maybeWhitespace(s)
        buf.append(
            if (isKeywordsLowerCase) s.toLowerCase(Locale.ROOT) else s.toUpperCase(Locale.ROOT)
        )
        if (!s.equals("")) {
            setNeedWhitespace(needWhitespaceAfter(s))
        }
    }

    private fun maybeWhitespace(s: String) {
        if (tooLong(s) || needWhitespace && needWhitespaceBefore(s)) {
            whiteSpace()
        }
    }

    protected fun whiteSpace() {
        if (needWhitespace) {
            if (NL.equals(nextWhitespace)) {
                newlineAndIndent()
            } else {
                buf.append(nextWhitespace)
            }
            nextWhitespace = " "
            setNeedWhitespace(false)
        }
    }

    /** Returns the number of characters appended since the last newline.  */
    private fun column(): Int {
        return buf.length() - lineStart
    }

    protected fun tooLong(s: String): Boolean {
        val lineLength: Int = config.lineLength()
        val result = (lineLength > 0 && column() > currentIndent
                && column() + s.length() >= lineLength)
        if (result) {
            nextWhitespace = NL
        }
        LOGGER.trace("Token is '{}'; result is {}", s, result)
        return result
    }

    @Override
    fun print(s: String) {
        maybeWhitespace(s)
        buf.append(s)
    }

    @Override
    fun print(x: Int) {
        maybeWhitespace("0")
        buf.append(x)
    }

    @Override
    fun identifier(name: String, quoted: Boolean) {
        // If configured globally or the original identifier is quoted,
        // then quotes the identifier.
        maybeWhitespace(name)
        if (isQuoteAllIdentifiers || quoted) {
            dialect.quoteIdentifier(buf, name)
        } else {
            buf.append(name)
        }
        setNeedWhitespace(true)
    }

    @Override
    fun dynamicParam(index: Int) {
        if (dynamicParameters == null) {
            dynamicParameters = ImmutableList.builder()
        }
        dynamicParameters.add(index)
        print("?")
        setNeedWhitespace(true)
    }

    @Override
    fun fetchOffset(@Nullable fetch: SqlNode?, @Nullable offset: SqlNode?) {
        if (fetch == null && offset == null) {
            return
        }
        dialect.unparseOffsetFetch(this, offset, fetch)
    }

    @Override
    fun topN(@Nullable fetch: SqlNode?, @Nullable offset: SqlNode?) {
        if (fetch == null && offset == null) {
            return
        }
        dialect.unparseTopN(this, offset, fetch)
    }

    @Override
    fun startFunCall(funName: String): Frame? {
        keyword(funName)
        setNeedWhitespace(false)
        return startList(FrameTypeEnum.FUN_CALL, "(", ")")
    }

    @Override
    fun endFunCall(frame: Frame?) {
        endList(this.frame)
    }

    @Override
    fun startList(open: String?, close: String): Frame? {
        return startList(FrameTypeEnum.SIMPLE, null, open, close)
    }

    @Override
    fun startList(frameType: FrameTypeEnum?): Frame? {
        assert(frameType != null)
        return startList(frameType, null, "", "")
    }

    @Override
    fun startList(frameType: FrameType?, open: String?, close: String): Frame? {
        assert(frameType != null)
        return startList(frameType, null, open, close)
    }

    @Override
    fun list(frameType: FrameTypeEnum?, action: Consumer<SqlWriter?>): SqlWriter {
        val selectListFrame: SqlWriter.Frame? = startList(SqlWriter.FrameTypeEnum.SELECT_LIST)
        val w: SqlWriter = this
        action.accept(w)
        endList(selectListFrame)
        return this
    }

    @Override
    fun list(
        frameType: FrameTypeEnum?, sepOp: SqlBinaryOperator,
        list: SqlNodeList
    ): SqlWriter {
        val frame: SqlWriter.Frame? = startList(frameType)
        (frame as FrameImpl?)!!.list(list, sepOp)
        endList(frame)
        return this
    }

    @Override
    fun sep(sep: String) {
        sep(sep, !(sep.equals(",") || sep.equals(".")))
    }

    @Override
    fun sep(sep: String, printFirst: Boolean) {
        if (frame == null) {
            throw RuntimeException("No list started")
        }
        if (sep.startsWith(" ") || sep.endsWith(" ")) {
            throw RuntimeException("Separator must not contain whitespace")
        }
        frame!!.sep(printFirst, sep)
    }

    @Override
    fun setNeedWhitespace(needWhitespace: Boolean) {
        this.needWhitespace = needWhitespace
    }

    fun setFormatOptions(@Nullable options: SqlFormatOptions?) {
        if (options == null) {
            return
        }
        isAlwaysUseParentheses = options.isAlwaysUseParentheses()
        setCaseClausesOnNewLines(options.isCaseClausesOnNewLines())
        isClauseStartsLine = options.isClauseStartsLine()
        isKeywordsLowerCase = options.isKeywordsLowercase()
        isQuoteAllIdentifiers = options.isQuoteAllIdentifiers()
        isSelectListItemsOnSeparateLines = options.isSelectListItemsOnSeparateLines()
        isWhereListItemsOnSeparateLines = options.isWhereListItemsOnSeparateLines()
        setWindowNewline(options.isWindowDeclarationStartsLine())
        setWindowDeclListNewline(options.isWindowListItemsOnSeparateLines())
        indentation = options.getIndentation()
        lineLength = options.getLineLength()
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Implementation of [org.apache.calcite.sql.SqlWriter.Frame].
     */
    protected inner class FrameImpl internal constructor(
        frameType: FrameType?, @Nullable keyword: String?, open: String?, close: String,
        left: Int, extraIndent: Int, chopLimit: Int,
        lineFolding: SqlWriterConfig.LineFolding?, newlineAfterOpen: Boolean,
        newlineBeforeSep: Boolean, sepIndent: Int, newlineAfterSep: Boolean,
        newlineBeforeClose: Boolean, newlineAfterClose: Boolean
    ) : Frame {
        val frameType: FrameType?

        @Nullable
        val keyword: String?
        val open: String?
        val close: String
        val left: Int

        /**
         * Indent of sub-frame with respect to this one.
         */
        val extraIndent: Int

        /**
         * Indent of separators with respect to this frame's indent. Typically
         * zero.
         */
        val sepIndent: Int

        /**
         * Number of items which have been printed in this list so far.
         */
        var itemCount = 0

        /**
         * Whether to print a newline before each separator.
         */
        val newlineBeforeSep: Boolean

        /**
         * Whether to print a newline after each separator.
         */
        val newlineAfterSep: Boolean
        val newlineBeforeClose: Boolean
        val newlineAfterClose: Boolean
        val newlineAfterOpen: Boolean

        /** Character count after which we should move an item to the
         * next line. Or [Integer.MAX_VALUE] if we are not chopping.  */
        private val chopLimit: Int

        /** How lines are to be folded.  */
        private val lineFolding: SqlWriterConfig.LineFolding?

        init {
            this.frameType = frameType
            this.keyword = keyword
            this.open = open
            this.close = close
            this.left = left
            this.extraIndent = extraIndent
            this.chopLimit = chopLimit
            this.lineFolding = lineFolding
            this.newlineAfterOpen = newlineAfterOpen
            this.newlineBeforeSep = newlineBeforeSep
            this.newlineAfterSep = newlineAfterSep
            this.newlineBeforeClose = newlineBeforeClose
            this.newlineAfterClose = newlineAfterClose
            this.sepIndent = sepIndent
            assert(
                chopLimit >= 0
                        == (lineFolding === SqlWriterConfig.LineFolding.CHOP || lineFolding === SqlWriterConfig.LineFolding.FOLD || lineFolding === SqlWriterConfig.LineFolding.STEP)
            )
        }

        fun before() {
            if (open != null && !open.equals("")) {
                keyword(open)
            }
        }

        protected fun after() {}
        fun sep(printFirst: Boolean, sep: String) {
            if (itemCount == 0) {
                if (newlineAfterOpen) {
                    newlineAndIndent(currentIndent)
                }
            } else {
                if (newlineBeforeSep) {
                    newlineAndIndent(currentIndent + sepIndent)
                }
            }
            if (itemCount > 0 || printFirst) {
                keyword(sep)
                nextWhitespace = if (newlineAfterSep) NL else " "
            }
            ++itemCount
        }

        /** Returns the extra indent required for a given type of sub-frame.  */
        fun extraIndent(subFrameType: FrameType?): Int {
            if (frameType === FrameTypeEnum.ORDER_BY
                && subFrameType === FrameTypeEnum.ORDER_BY_LIST
            ) {
                return config.indentation()
            }
            return if (subFrameType.needsIndent()) {
                extraIndent
            } else 0
        }

        fun list(list: SqlNodeList, sepOp: SqlBinaryOperator) {
            val save: Save
            when (lineFolding) {
                CHOP, FOLD -> {
                    save = Save()
                    if (!list2(list, sepOp)) {
                        save.restore()
                        val newlineAfterOpen: Boolean = config.clauseEndsLine()
                        val lineFolding: SqlWriterConfig.LineFolding
                        val chopLimit: Int
                        if (this.lineFolding === SqlWriterConfig.LineFolding.CHOP) {
                            lineFolding = SqlWriterConfig.LineFolding.TALL
                            chopLimit = -1
                        } else {
                            lineFolding = SqlWriterConfig.LineFolding.FOLD
                            chopLimit = this.chopLimit
                        }
                        val newline = lineFolding === SqlWriterConfig.LineFolding.TALL
                        val newlineBeforeSep: Boolean
                        val newlineAfterSep: Boolean
                        val sepIndent: Int
                        if (config.leadingComma() && newline) {
                            newlineBeforeSep = true
                            newlineAfterSep = false
                            sepIndent = -", ".length()
                        } else if (newline) {
                            newlineBeforeSep = false
                            newlineAfterSep = true
                            sepIndent = this.sepIndent
                        } else {
                            newlineBeforeSep = false
                            newlineAfterSep = false
                            sepIndent = this.sepIndent
                        }
                        val frame2: FrameImpl = FrameImpl(
                            frameType, keyword, open, close, left, extraIndent,
                            chopLimit, lineFolding, newlineAfterOpen, newlineBeforeSep,
                            sepIndent, newlineAfterSep, newlineBeforeClose,
                            newlineAfterClose
                        )
                        frame2.list2(list, sepOp)
                    }
                }
                else -> list2(list, sepOp)
            }
        }

        /** Tries to write a list. If the line runs too long, returns false,
         * indicating to retry.  */
        private fun list2(list: SqlNodeList, sepOp: SqlBinaryOperator): Boolean {
            // The precedence pulling on the LHS of a node is the
            // right-precedence of the separator operator. Similarly RHS.
            //
            // At the start and end of the list precedence should be 0, but non-zero
            // precedence is useful, because it forces parentheses around
            // sub-queries and empty lists, e.g. "SELECT a, (SELECT * FROM t), b",
            // "GROUP BY ()".
            val lprec: Int = sepOp.getRightPrec()
            val rprec: Int = sepOp.getLeftPrec()
            if (chopLimit < 0) {
                for (i in 0 until list.size()) {
                    val node: SqlNode = list.get(i)
                    sep(false, sepOp.getName())
                    node.unparse(this@SqlPrettyWriter, lprec, rprec)
                }
            } else if (newlineBeforeSep) {
                for (i in 0 until list.size()) {
                    val node: SqlNode = list.get(i)
                    sep(false, sepOp.getName())
                    val prevSize: Save = Save()
                    node.unparse(this@SqlPrettyWriter, lprec, rprec)
                    if (column() > chopLimit) {
                        if (lineFolding === SqlWriterConfig.LineFolding.CHOP
                            || lineFolding === SqlWriterConfig.LineFolding.TALL
                        ) {
                            return false
                        }
                        prevSize.restore()
                        newlineAndIndent()
                        node.unparse(this@SqlPrettyWriter, lprec, rprec)
                    }
                }
            } else {
                for (i in 0 until list.size()) {
                    val node: SqlNode = list.get(i)
                    if (i == 0) {
                        sep(false, sepOp.getName())
                    }
                    val save: Save = Save()
                    node.unparse(this@SqlPrettyWriter, lprec, rprec)
                    if (i + 1 < list.size()) {
                        sep(false, sepOp.getName())
                    }
                    if (column() > chopLimit) {
                        when (lineFolding) {
                            CHOP -> return false
                            FOLD -> if (newlineAfterOpen != config.clauseEndsLine()) {
                                return false
                            }
                            else -> {}
                        }
                        save.restore()
                        newlineAndIndent()
                        node.unparse(this@SqlPrettyWriter, lprec, rprec)
                        if (i + 1 < list.size()) {
                            sep(false, sepOp.getName())
                        }
                    }
                }
            }
            return true
        }

        /** Remembers the state of the current frame and writer.
         *
         *
         * You can call [.restore] to restore to that state, or just
         * continue. It is useful if you wish to re-try with different options
         * (for example, with lines wrapped).  */
        internal inner class Save {
            val bufLength: Int

            init {
                bufLength = buf.length()
            }

            fun restore() {
                buf.setLength(bufLength)
            }
        }
    }

    /**
     * Helper class which exposes the get/set methods of an object as
     * properties.
     */
    private class Bean internal constructor(private val o: SqlPrettyWriter) {
        private val getterMethods: Map<String, Method> = HashMap()
        private val setterMethods: Map<String, Method> = HashMap()

        init {

            // Figure out the getter/setter methods for each attribute.
            for (method in o.getClass().getMethods()) {
                if (method.getName().startsWith("set")
                    && method.getReturnType() === Void::class.java
                    && method.getParameterTypes().length === 1
                ) {
                    val attributeName = stripPrefix(
                        method.getName(),
                        3
                    )
                    setterMethods.put(attributeName, method)
                }
                if (method.getName().startsWith("get")
                    && method.getReturnType() !== Void::class.java
                    && method.getParameterTypes().length === 0
                ) {
                    val attributeName = stripPrefix(
                        method.getName(),
                        3
                    )
                    getterMethods.put(attributeName, method)
                }
                if (method.getName().startsWith("is")
                    && method.getReturnType() === Boolean::class.java
                    && method.getParameterTypes().length === 0
                ) {
                    val attributeName = stripPrefix(
                        method.getName(),
                        2
                    )
                    getterMethods.put(attributeName, method)
                }
            }
        }

        operator fun set(name: String, value: String?) {
            val method: Method = requireNonNull(
                setterMethods[name]
            ) { "setter method $name not found" }
            try {
                method.invoke(o, value)
            } catch (e: IllegalAccessException) {
                throw Util.throwAsRuntime(Util.causeOrSelf(e))
            } catch (e: InvocationTargetException) {
                throw Util.throwAsRuntime(Util.causeOrSelf(e))
            }
        }

        @Nullable
        operator fun get(name: String): Object {
            val method: Method = requireNonNull(
                getterMethods[name]
            ) { "getter method $name not found" }
            return try {
                method.invoke(o)
            } catch (e: IllegalAccessException) {
                throw Util.throwAsRuntime(Util.causeOrSelf(e))
            } catch (e: InvocationTargetException) {
                throw Util.throwAsRuntime(Util.causeOrSelf(e))
            }
        }

        val propertyNames: Array<String>
            get() {
                val names: Set<String> = HashSet()
                names.addAll(getterMethods.keySet())
                names.addAll(setterMethods.keySet())
                return names.toArray(arrayOfNulls<String>(0))
            }

        companion object {
            private fun stripPrefix(name: String, offset: Int): String {
                return (name.substring(offset, offset + 1).toLowerCase(Locale.ROOT)
                        + name.substring(offset + 1))
            }
        }
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        protected val LOGGER: CalciteLogger = CalciteLogger(
            LoggerFactory.getLogger("org.apache.calcite.sql.pretty.SqlPrettyWriter")
        )

        /**
         * Default SqlWriterConfig, reduce the overhead of "ImmutableBeans.create"
         */
        private val CONFIG: SqlWriterConfig = SqlWriterConfig.of()
            .withDialect(CalciteSqlDialect.DEFAULT)

        /**
         * Bean holding the default property values.
         */
        private val DEFAULT_BEAN: Bean = SqlPrettyWriter(
            config()
                .withDialect(AnsiSqlDialect.DEFAULT)
        ).getBean()
        protected val NL: String = System.getProperty("line.separator")

        /** Creates a [SqlWriterConfig] with Calcite's SQL dialect.  */
        fun config(): SqlWriterConfig {
            return CONFIG
        }

        private fun f3(
            folding0: @Nullable SqlWriterConfig.LineFolding?,
            folding1: @Nullable SqlWriterConfig.LineFolding?, opt: Boolean
        ): SqlWriterConfig.LineFolding {
            return if (folding0 != null) folding0 else if (folding1 != null) folding1 else if (opt) SqlWriterConfig.LineFolding.TALL else SqlWriterConfig.LineFolding.WIDE
        }

        private fun needWhitespaceBefore(s: String): Boolean {
            return !(s.equals(",")
                    || s.equals(".")
                    || s.equals(")")
                    || s.equals("[")
                    || s.equals("]")
                    || s.equals(""))
        }

        private fun needWhitespaceAfter(s: String): Boolean {
            return !(s.equals("(")
                    || s.equals("[")
                    || s.equals("."))
        }
    }
}
