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

import org.apache.calcite.sql.pretty.SqlPrettyWriter

/** Configuration for [SqlWriter] and [SqlPrettyWriter].  */
@Value.Immutable
interface SqlWriterConfig {
    /** Returns the dialect.  */
    @Nullable
    fun dialect(): SqlDialect?

    /** Sets [.dialect].  */
    fun withDialect(@Nullable dialect: SqlDialect?): SqlWriterConfig?

    /** Returns whether to print keywords (SELECT, AS, etc.) in lower-case.
     * Default is false: keywords are printed in upper-case.  */
    @Value.Default
    fun keywordsLowerCase(): Boolean {
        return false
    }

    /** Sets [.keywordsLowerCase].  */
    fun withKeywordsLowerCase(keywordsLowerCase: Boolean): SqlWriterConfig?

    /** Returns whether to quote all identifiers, even those which would be
     * correct according to the rules of the [SqlDialect] if quotation
     * marks were omitted. Default is true.  */
    @Value.Default
    fun quoteAllIdentifiers(): Boolean {
        return true
    }

    /** Sets [.quoteAllIdentifiers].  */
    fun withQuoteAllIdentifiers(quoteAllIdentifiers: Boolean): SqlWriterConfig?

    /** Returns the number of spaces indentation. Default is 4.  */
    @Value.Default
    fun indentation(): Int {
        return 4
    }

    /** Sets [.indentation].  */
    fun withIndentation(indentation: Int): SqlWriterConfig?

    /** Returns whether a clause (FROM, WHERE, GROUP BY, HAVING, WINDOW,
     * ORDER BY) starts a new line. Default is true. SELECT is always at the
     * start of a line.  */
    @Value.Default
    fun clauseStartsLine(): Boolean {
        return true
    }

    /** Sets [.clauseStartsLine].  */
    fun withClauseStartsLine(clauseStartsLine: Boolean): SqlWriterConfig?

    /** Returns whether a clause (FROM, WHERE, GROUP BY, HAVING, WINDOW,
     * ORDER BY) is followed by a new line. Default is false.  */
    @Value.Default
    fun clauseEndsLine(): Boolean {
        return false
    }

    /** Sets [.clauseEndsLine].  */
    fun withClauseEndsLine(clauseEndsLine: Boolean): SqlWriterConfig?

    /** Returns whether each item in a SELECT list, GROUP BY list, or ORDER BY
     * list is on its own line.
     *
     *
     * Default is false;
     * this property is superseded by [.selectFolding],
     * [.groupByFolding], [.orderByFolding].  */
    @Value.Default
    fun selectListItemsOnSeparateLines(): Boolean {
        return false
    }

    /** Sets [.selectListItemsOnSeparateLines].  */
    fun withSelectListItemsOnSeparateLines(
        selectListItemsOnSeparateLines: Boolean
    ): SqlWriterConfig?

    /** Returns the line-folding policy for lists in the SELECT, GROUP BY and
     * ORDER clauses, for items in the SET clause of UPDATE, and for items in
     * VALUES.
     *
     * @see .foldLength
     */
    @Nullable
    fun lineFolding(): LineFolding?

    /** Sets [.lineFolding].  */
    fun withLineFolding(@Nullable lineFolding: LineFolding?): SqlWriterConfig?

    /** Returns the line-folding policy for the SELECT clause.
     * If not set, the value of [.lineFolding] is used.  */
    @Nullable
    fun selectFolding(): LineFolding?

    /** Sets [.selectFolding].  */
    fun withSelectFolding(@Nullable lineFolding: LineFolding?): SqlWriterConfig?

    /** Returns the line-folding policy for the FROM clause (and JOIN).
     * If not set, the value of [.lineFolding] is used.  */
    @Value.Default
    fun fromFolding(): LineFolding? {
        return LineFolding.TALL
    }

    /** Sets [.fromFolding].  */
    fun withFromFolding(lineFolding: LineFolding?): SqlWriterConfig?

    /** Returns the line-folding policy for the WHERE clause.
     * If not set, the value of [.lineFolding] is used.  */
    @Nullable
    fun whereFolding(): LineFolding?

    /** Sets [.whereFolding].  */
    fun withWhereFolding(@Nullable lineFolding: LineFolding?): SqlWriterConfig?

    /** Returns the line-folding policy for the GROUP BY clause.
     * If not set, the value of [.lineFolding] is used.  */
    @Nullable
    fun groupByFolding(): LineFolding?

    /** Sets [.groupByFolding].  */
    fun withGroupByFolding(@Nullable lineFolding: LineFolding?): SqlWriterConfig?

    /** Returns the line-folding policy for the HAVING clause.
     * If not set, the value of [.lineFolding] is used.  */
    @Nullable
    fun havingFolding(): LineFolding?

    /** Sets [.havingFolding].  */
    fun withHavingFolding(@Nullable lineFolding: LineFolding?): SqlWriterConfig?

    /** Returns the line-folding policy for the WINDOW clause.
     * If not set, the value of [.lineFolding] is used.  */
    @Nullable
    fun windowFolding(): LineFolding?

    /** Sets [.windowFolding].  */
    fun withWindowFolding(@Nullable lineFolding: LineFolding?): SqlWriterConfig?

    /** Returns the line-folding policy for the MATCH_RECOGNIZE clause.
     * If not set, the value of [.lineFolding] is used.  */
    @Nullable
    fun matchFolding(): LineFolding?

    /** Sets [.matchFolding].  */
    fun withMatchFolding(@Nullable lineFolding: LineFolding?): SqlWriterConfig?

    /** Returns the line-folding policy for the ORDER BY clause.
     * If not set, the value of [.lineFolding] is used.  */
    @Nullable
    fun orderByFolding(): LineFolding?

    /** Sets [.orderByFolding].  */
    fun withOrderByFolding(@Nullable lineFolding: LineFolding?): SqlWriterConfig?

    /** Returns the line-folding policy for the OVER clause or a window
     * declaration. If not set, the value of [.lineFolding] is used.  */
    @Nullable
    fun overFolding(): LineFolding?

    /** Sets [.overFolding].  */
    fun withOverFolding(@Nullable lineFolding: LineFolding?): SqlWriterConfig?

    /** Returns the line-folding policy for the VALUES expression.
     * If not set, the value of [.lineFolding] is used.  */
    @Nullable
    fun valuesFolding(): LineFolding?

    /** Sets [.valuesFolding].  */
    fun withValuesFolding(@Nullable lineFolding: LineFolding?): SqlWriterConfig?

    /** Returns the line-folding policy for the SET clause of an UPDATE statement.
     * If not set, the value of [.lineFolding] is used.  */
    @Nullable
    fun updateSetFolding(): LineFolding?

    /** Sets [.updateSetFolding].  */
    fun withUpdateSetFolding(@Nullable lineFolding: LineFolding?): SqlWriterConfig?

    /**
     * Returns whether to use a fix for SELECT list indentations.
     *
     *
     *  * If set to "false":
     *
     * <blockquote><pre>
     * SELECT
     * A as A,
     * B as B,
     * C as C,
     * D
    </pre></blockquote> *
     *
     *  * If set to "true" (the default):
     *
     * <blockquote><pre>
     * SELECT
     * A as A,
     * B as B,
     * C as C,
     * D
    </pre></blockquote> *
     *
     */
    @Value.Default
    fun selectListExtraIndentFlag(): Boolean {
        return true
    }

    /** Sets [.selectListExtraIndentFlag].  */
    fun withSelectListExtraIndentFlag(selectListExtraIndentFlag: Boolean): SqlWriterConfig?

    /** Returns whether each declaration in a WINDOW clause should be on its own
     * line.
     *
     *
     * Default is true;
     * this property is superseded by [.windowFolding].  */
    @Value.Default
    fun windowDeclListNewline(): Boolean {
        return true
    }

    /** Sets [.windowDeclListNewline].  */
    fun withWindowDeclListNewline(windowDeclListNewline: Boolean): SqlWriterConfig?

    /** Returns whether each row in a VALUES clause should be on its own
     * line.
     *
     *
     * Default is true;
     * this property is superseded by [.valuesFolding].  */
    @Value.Default
    fun valuesListNewline(): Boolean {
        return true
    }

    /** Sets [.valuesListNewline].  */
    fun withValuesListNewline(valuesListNewline: Boolean): SqlWriterConfig?

    /** Returns whether each assignment in the SET clause of an UPDATE or MERGE
     * statement should be on its own line.
     *
     *
     * Default is true;
     * this property is superseded by [.updateSetFolding].  */
    @Value.Default
    fun updateSetListNewline(): Boolean {
        return true
    }

    /** Sets [.updateSetListNewline].  */
    fun withUpdateSetListNewline(updateSetListNewline: Boolean): SqlWriterConfig?

    /** Returns whether a WINDOW clause should start its own line.  */
    @Value.Default
    fun windowNewline(): Boolean {
        return false
    }

    /** Sets [.windowNewline].  */
    fun withWindowNewline(windowNewline: Boolean): SqlWriterConfig?

    /** Returns whether commas in SELECT, GROUP BY and ORDER clauses should
     * appear at the start of the line. Default is false.  */
    @Value.Default
    fun leadingComma(): Boolean {
        return false
    }

    /** Sets [.leadingComma].  */
    fun withLeadingComma(leadingComma: Boolean): SqlWriterConfig?

    /** Returns the sub-query style.
     * Default is [SqlWriter.SubQueryStyle.HYDE].  */
    @Value.Default
    fun subQueryStyle(): SqlWriter.SubQueryStyle? {
        return SqlWriter.SubQueryStyle.HYDE
    }

    /** Sets [.subQueryStyle].  */
    fun withSubQueryStyle(subQueryStyle: SqlWriter.SubQueryStyle?): SqlWriterConfig?

    /** Returns whether to print a newline before each AND or OR (whichever is
     * higher level) in WHERE clauses.
     *
     *
     * NOTE: Ignored when alwaysUseParentheses is set to true.  */
    @Value.Default
    fun whereListItemsOnSeparateLines(): Boolean {
        return false
    }

    /** Sets [.whereListItemsOnSeparateLines].  */
    fun withWhereListItemsOnSeparateLines(
        whereListItemsOnSeparateLines: Boolean
    ): SqlWriterConfig?

    /** Returns whether expressions should always be included in parentheses.
     * Default is false.  */
    @Value.Default
    fun alwaysUseParentheses(): Boolean {
        return false
    }

    /** Sets [.alwaysUseParentheses].  */
    fun withAlwaysUseParentheses(alwaysUseParentheses: Boolean): SqlWriterConfig?

    /** Returns the maximum line length. Default is zero, which means there is
     * no maximum.  */
    @Value.Default
    fun lineLength(): Int {
        return 0
    }

    /** Sets [.lineLength].  */
    fun withLineLength(lineLength: Int): SqlWriterConfig?

    /** Returns the line length at which items are chopped or folded (for clauses
     * that have chosen [LineFolding.CHOP] or [LineFolding.FOLD]).
     * Default is 80.  */
    @Value.Default
    fun foldLength(): Int {
        return 80
    }

    /** Sets [.foldLength].  */
    fun withFoldLength(lineLength: Int): SqlWriterConfig?

    /** Returns whether the WHEN, THEN and ELSE clauses of a CASE expression
     * appear at the start of a new line. The default is false.  */
    @Value.Default
    fun caseClausesOnNewLines(): Boolean {
        return false
    }

    /** Sets [.caseClausesOnNewLines].  */
    fun withCaseClausesOnNewLines(caseClausesOnNewLines: Boolean): SqlWriterConfig?

    /** Policy for how to do deal with long lines.
     *
     *
     * The following examples all have
     * [ClauseEndsLine=true][.clauseEndsLine],
     * [Indentation=4][.indentation], and
     * [FoldLength=25][.foldLength] (so that the long `SELECT`
     * clause folds but the shorter `GROUP BY` clause does not).
     *
     *
     * Note that [ClauseEndsLine][.clauseEndsLine] is observed in
     * STEP and TALL modes, and in CHOP mode when a line is long.
     *
     * <table border=1>
     * <caption>SQL formatted with each Folding value</caption>
     * <tr>
     * <th>Folding</th>
     * <th>Example</th>
    </tr> *
     *
     * <tr>
     * <td>WIDE</td>
     * <td><pre>
     * SELECT abc, def, ghi, jkl, mno, pqr
     * FROM t
     * GROUP BY abc, def</pre></td>
    </tr> *
     *
     * <tr>
     * <td>STEP</td>
     * <td><pre>
     * SELECT
     * abc, def, ghi, jkl, mno, pqr
     * FROM t
     * GROUP BY
     * abc, def</pre></td>
    </tr> *
     *
     * <tr>
     * <td>FOLD</td>
     * <td><pre>
     * SELECT abc, def, ghi,
     * jkl, mno, pqr
     * FROM t
     * GROUP BY abc, def</pre></td>
    </tr> *
     *
     * <tr>
     * <td>CHOP</td>
     * <td><pre>
     * SELECT
     * abc,
     * def,
     * ghi,
     * jkl,
     * mno,
     * pqr
     * FROM t
     * GROUP BY abc, def</pre></td>
    </tr> *
     *
     * <tr>
     * <td>TALL</td>
     * <td><pre>
     * SELECT
     * abc,
     * def,
     * ghi,
     * jkl,
     * mno,
     * pqr
     * FROM t
     * GROUP BY
     * abc,
     * def</pre></td>
    </tr> *
    </table> *
     */
    enum class LineFolding {
        /** Do not wrap. Items are on the same line, regardless of length.  */
        WIDE,

        /** As [.WIDE] but start a new line if [.clauseEndsLine].  */
        STEP,

        /** Wrap if long. Items are on the same line, but if the line's length
         * exceeds [.foldLength], move items to the next line.  */
        FOLD,

        /** Chop down if long. Items are on the same line, but if the line grows
         * longer than [.foldLength], put all items on separate lines.  */
        CHOP,

        /** Wrap always. Items are on separate lines.  */
        TALL
    }

    companion object {
        /**
         * Create a default SqlWriterConfig object.
         * @return The config.
         */
        fun of(): SqlWriterConfig? {
            return ImmutableSqlWriterConfig.of()
        }
    }
}
