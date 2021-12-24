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

import org.apache.calcite.sql.parser.Span
import kotlin.Throws
import java.sql.Time
import PrecedenceClimbingParser.Call
import kotlin.jvm.Synchronized

/**
 * Contains a string, the offset of a token within the string, and a parser
 * position containing the beginning and end line number.
 */
class StringAndPos private constructor(val sql: String, val cursor: Int, @Nullable pos: SqlParserPos?) {
    @Nullable
    val pos: SqlParserPos?

    init {
        this.pos = pos
    }

    fun addCarets(): String {
        return if (pos == null) sql else SqlParserUtil.addCarets(
            sql, pos.getLineNum(), pos.getColumnNum(),
            pos.getEndLineNum(), pos.getEndColumnNum() + 1
        )
    }

    companion object {
        /**
         * Looks for one or two carets in a SQL string, and if present, converts
         * them into a parser position.
         *
         *
         * Examples:
         *
         *
         *  * of("xxx^yyy") yields {"xxxyyy", position 3, line 1 column 4}
         *  * of("xxxyyy") yields {"xxxyyy", null}
         *  * of("xxx^yy^y") yields {"xxxyyy", position 3, line 4 column 4
         * through line 1 column 6}
         *
         */
        fun of(sql: String): StringAndPos {
            val firstCaret: Int = sql.indexOf('^')
            if (firstCaret < 0) {
                return StringAndPos(sql, -1, null)
            }
            var secondCaret: Int = sql.indexOf('^', firstCaret + 1)
            return if (secondCaret == firstCaret + 1) {
                // If SQL contains "^^", it does not contain error positions; convert each
                // "^^" to a single "^".
                StringAndPos(sql.replace("^^", "^"), -1, null)
            } else if (secondCaret < 0) {
                val sqlSansCaret: String = (sql.substring(0, firstCaret)
                        + sql.substring(firstCaret + 1))
                val start: IntArray = SqlParserUtil.indexToLineCol(sql, firstCaret)
                val pos = SqlParserPos(start[0], start[1])
                StringAndPos(sqlSansCaret, firstCaret, pos)
            } else {
                val sqlSansCaret: String = (sql.substring(0, firstCaret)
                        + sql.substring(firstCaret + 1, secondCaret)
                        + sql.substring(secondCaret + 1))
                val start: IntArray = SqlParserUtil.indexToLineCol(sql, firstCaret)

                // subtract 1 because the col position needs to be inclusive
                --secondCaret
                val end: IntArray = SqlParserUtil.indexToLineCol(sql, secondCaret)

                // if second caret is on same line as first, decrement its column,
                // because first caret pushed the string out
                if (start[0] == end[0]) {
                    --end[1]
                }
                val pos = SqlParserPos(start[0], start[1], end[0], end[1])
                StringAndPos(sqlSansCaret, firstCaret, pos)
            }
        }
    }
}
