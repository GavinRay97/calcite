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

import kotlin.jvm.JvmOverloads

/**
 * Data structure to hold options for
 * [SqlPrettyWriter.setFormatOptions].
 */
class SqlFormatOptions
/**
 * Constructs a set of default SQL format options.
 */
    () {
    var isAlwaysUseParentheses = false
    var isCaseClausesOnNewLines = false
    var isClauseStartsLine = true
    var isKeywordsLowercase = false
    var isQuoteAllIdentifiers = true
    var isSelectListItemsOnSeparateLines = false
    var isWhereListItemsOnSeparateLines = false
    var isWindowDeclarationStartsLine = true
    var isWindowListItemsOnSeparateLines = true
    var indentation = 4
    var lineLength = 0

    /**
     * Constructs a complete set of SQL format options.
     *
     * @param alwaysUseParentheses           Always use parentheses
     * @param caseClausesOnNewLines          Case clauses on new lines
     * @param clauseStartsLine               Clause starts line
     * @param keywordsLowercase              Keywords in lower case
     * @param quoteAllIdentifiers            Quote all identifiers
     * @param selectListItemsOnSeparateLines Select items on separate lines
     * @param whereListItemsOnSeparateLines  Where items on separate lines
     * @param windowDeclarationStartsLine    Window declaration starts line
     * @param windowListItemsOnSeparateLines Window list items on separate lines
     * @param indentation                    Indentation
     * @param lineLength                     Line length
     */
    constructor(
        alwaysUseParentheses: Boolean,
        caseClausesOnNewLines: Boolean,
        clauseStartsLine: Boolean,
        keywordsLowercase: Boolean,
        quoteAllIdentifiers: Boolean,
        selectListItemsOnSeparateLines: Boolean,
        whereListItemsOnSeparateLines: Boolean,
        windowDeclarationStartsLine: Boolean,
        windowListItemsOnSeparateLines: Boolean,
        indentation: Int,
        lineLength: Int
    ) : this() {
        isAlwaysUseParentheses = alwaysUseParentheses
        isCaseClausesOnNewLines = caseClausesOnNewLines
        isClauseStartsLine = clauseStartsLine
        isKeywordsLowercase = keywordsLowercase
        isQuoteAllIdentifiers = quoteAllIdentifiers
        isSelectListItemsOnSeparateLines = selectListItemsOnSeparateLines
        isWhereListItemsOnSeparateLines = whereListItemsOnSeparateLines
        isWindowDeclarationStartsLine = windowDeclarationStartsLine
        isWindowListItemsOnSeparateLines = windowListItemsOnSeparateLines
        this.indentation = indentation
        this.lineLength = lineLength
    }
}
