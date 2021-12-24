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

import org.apache.calcite.sql.SqlNode

/**
 * SqlParserPos represents the position of a parsed token within SQL statement
 * text.
 */
class SqlParserPos(
    /** Returns 1-based starting line number.  */
    //~ Instance fields --------------------------------------------------------
    val lineNum: Int,
    /** Returns 1-based starting column number.  */
    val columnNum: Int,
    /** Returns 1-based end line number (same as starting line number if the
     * ParserPos is a point, not a range).  */
    val endLineNum: Int,
    /** Returns 1-based end column number (same as starting column number if the
     * ParserPos is a point, not a range).  */
    val endColumnNum: Int
) : Serializable {

    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a new parser position.
     */
    constructor(
        lineNumber: Int,
        columnNumber: Int
    ) : this(lineNumber, columnNumber, lineNumber, columnNumber) {
    }

    /**
     * Creates a new parser range.
     */
    init {
        assert(
            lineNum < endLineNum
                    || lineNum == endLineNum
                    && columnNum <= endColumnNum
        )
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun hashCode(): Int {
        return Objects.hash(lineNum, columnNum, endLineNum, endColumnNum)
    }

    @Override
    override fun equals(@Nullable o: Object): Boolean {
        return (o === this
                || (o is SqlParserPos
                && lineNum == (o as SqlParserPos).lineNum && columnNum == (o as SqlParserPos).columnNum && endLineNum == (o as SqlParserPos).endLineNum && endColumnNum == (o as SqlParserPos).endColumnNum))
    }

    /** Returns a `SqlParserPos` the same as this but quoted.  */
    fun withQuoting(quoted: Boolean): SqlParserPos {
        return if (isQuoted == quoted) {
            this
        } else if (quoted) {
            QuotedParserPos(
                lineNum, columnNum, endLineNum,
                endColumnNum
            )
        } else {
            SqlParserPos(
                lineNum, columnNum, endLineNum,
                endColumnNum
            )
        }
    }

    /** Returns whether this SqlParserPos is quoted.  */
    val isQuoted: Boolean
        get() = false

    @Override
    override fun toString(): String {
        return RESOURCE.parserContext(lineNum, columnNum).str()
    }

    /**
     * Combines this parser position with another to create a
     * position that spans from the first point in the first to the last point
     * in the other.
     */
    operator fun plus(pos: SqlParserPos): SqlParserPos {
        return SqlParserPos(
            lineNum,
            columnNum,
            pos.endLineNum,
            pos.endColumnNum
        )
    }

    /**
     * Combines this parser position with an array of positions to create a
     * position that spans from the first point in the first to the last point
     * in the other.
     */
    fun plusAll(@Nullable nodes: Array<SqlNode?>): SqlParserPos {
        val b = PosBuilder(this)
        for (node in nodes) {
            if (node != null) {
                b.add(node.getParserPosition())
            }
        }
        return b.build(this)
    }

    /**
     * Combines this parser position with a list of positions.
     */
    fun plusAll(nodes: Collection<SqlNode?>): SqlParserPos {
        val b = PosBuilder(this)
        for (node in nodes) {
            if (node != null) {
                b.add(node.getParserPosition())
            }
        }
        return b.build(this)
    }

    fun overlaps(pos: SqlParserPos): Boolean {
        return (startsBefore(pos) && endsAfter(pos)
                || pos.startsBefore(this) && pos.endsAfter(this))
    }

    private fun startsBefore(pos: SqlParserPos): Boolean {
        return (lineNum < pos.lineNum
                || lineNum == pos.lineNum
                && columnNum <= pos.columnNum)
    }

    private fun endsAfter(pos: SqlParserPos): Boolean {
        return (endLineNum > pos.endLineNum
                || endLineNum == pos.endLineNum
                && endColumnNum >= pos.endColumnNum)
    }

    fun startsAt(pos: SqlParserPos): Boolean {
        return (lineNum == pos.lineNum
                && columnNum == pos.columnNum)
    }

    /** Parser position for an identifier segment that is quoted.  */
    private class QuotedParserPos internal constructor(
        startLineNumber: Int, startColumnNumber: Int,
        endLineNumber: Int, endColumnNumber: Int
    ) : SqlParserPos(
        startLineNumber, startColumnNumber, endLineNumber,
        endColumnNumber
    ) {
        @Override
        override fun isQuoted(): Boolean {
            return true
        }
    }

    /** Builds a parser position.  */
    private class PosBuilder internal constructor(
        private var line: Int,
        private var column: Int,
        private var endLine: Int,
        private var endColumn: Int
    ) {
        internal constructor(p: SqlParserPos) : this(p.lineNum, p.columnNum, p.endLineNum, p.endColumnNum) {}

        fun add(pos: SqlParserPos) {
            if (pos.equals(ZERO)) {
                return
            }
            var testLine = pos.lineNum
            var testColumn = pos.columnNum
            if (testLine < line || testLine == line && testColumn < column) {
                line = testLine
                column = testColumn
            }
            testLine = pos.endLineNum
            testColumn = pos.endColumnNum
            if (testLine > endLine || testLine == endLine && testColumn > endColumn) {
                endLine = testLine
                endColumn = testColumn
            }
        }

        fun build(p: SqlParserPos): SqlParserPos {
            return if (p.lineNum == line && p.columnNum == column && p.endLineNum == endLine && p.endColumnNum == endColumn) p else build()
        }

        fun build(): SqlParserPos {
            return SqlParserPos(line, column, endLine, endColumn)
        }
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        /**
         * SqlParserPos representing line one, character one. Use this if the node
         * doesn't correspond to a position in piece of SQL text.
         */
        val ZERO = SqlParserPos(0, 0)

        /** Same as [.ZERO] but always quoted.  */
        val QUOTED_ZERO: SqlParserPos = QuotedParserPos(0, 0, 0, 0)
        private const val serialVersionUID = 1L

        /**
         * Combines the parser positions of an array of nodes to create a position
         * which spans from the beginning of the first to the end of the last.
         */
        fun sum(nodes: Array<SqlNode>): SqlParserPos {
            if (nodes.size == 0) {
                throw AssertionError()
            }
            val pos0: SqlParserPos = nodes[0].getParserPosition()
            if (nodes.size == 1) {
                return pos0
            }
            val b = PosBuilder(pos0)
            for (i in 1 until nodes.size) {
                b.add(nodes[i].getParserPosition())
            }
            return b.build(pos0)
        }

        /**
         * Combines the parser positions of a list of nodes to create a position
         * which spans from the beginning of the first to the end of the last.
         */
        fun sum(nodes: List<SqlNode?>): SqlParserPos {
            if (nodes.size() === 0) {
                throw AssertionError()
            }
            val pos0: SqlParserPos = nodes[0].getParserPosition()
            if (nodes.size() === 1) {
                return pos0
            }
            val b = PosBuilder(pos0)
            for (i in 1 until nodes.size()) {
                b.add(nodes[i].getParserPosition())
            }
            return b.build(pos0)
        }

        /** Returns a position spanning the earliest position to the latest.
         * Does not assume that the positions are sorted.
         * Throws if the list is empty.  */
        fun sum(poses: Iterable<SqlParserPos>?): SqlParserPos {
            val list = if (poses is List) poses else Lists.newArrayList(poses)
            if (list.size() === 0) {
                throw AssertionError()
            }
            val pos0 = list[0]
            if (list.size() === 1) {
                return pos0
            }
            val b = PosBuilder(pos0)
            for (i in 1 until list.size()) {
                b.add(list[i])
            }
            return b.build(pos0)
        }
    }
}
