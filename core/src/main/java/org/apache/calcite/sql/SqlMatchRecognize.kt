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

import org.apache.calcite.sql.parser.SqlParserPos

/**
 * SqlNode for MATCH_RECOGNIZE clause.
 */
class SqlMatchRecognize(
    pos: SqlParserPos?, tableRef: SqlNode?, pattern: SqlNode?,
    strictStart: SqlLiteral, strictEnd: SqlLiteral, patternDefList: SqlNodeList,
    measureList: SqlNodeList?, @Nullable after: SqlNode, subsetList: SqlNodeList?,
    @Nullable rowsPerMatch: SqlLiteral?, partitionList: SqlNodeList?,
    orderList: SqlNodeList?, @Nullable interval: SqlLiteral
) : SqlCall(pos) {
    //~ Instance fields -------------------------------------------
    private var tableRef: SqlNode
    private var pattern: SqlNode
    private var strictStart: SqlLiteral
    private var strictEnd: SqlLiteral
    private var patternDefList: SqlNodeList
    private var measureList: SqlNodeList?

    @Nullable
    private var after: SqlNode
    private var subsetList: SqlNodeList?

    @Nullable
    private var rowsPerMatch: SqlLiteral?
    private var partitionList: SqlNodeList?
    private var orderList: SqlNodeList?

    @Nullable
    private var interval: SqlLiteral

    /** Creates a SqlMatchRecognize.  */
    init {
        this.tableRef = Objects.requireNonNull(tableRef, "tableRef")
        this.pattern = Objects.requireNonNull(pattern, "pattern")
        this.strictStart = strictStart
        this.strictEnd = strictEnd
        this.patternDefList = Objects.requireNonNull(patternDefList, "patternDefList")
        Preconditions.checkArgument(patternDefList.size() > 0)
        this.measureList = Objects.requireNonNull(measureList, "measureList")
        this.after = after
        this.subsetList = subsetList
        Preconditions.checkArgument(
            rowsPerMatch == null
                    || rowsPerMatch.value is RowsPerMatchOption
        )
        this.rowsPerMatch = rowsPerMatch
        this.partitionList = Objects.requireNonNull(partitionList, "partitionList")
        this.orderList = Objects.requireNonNull(orderList, "orderList")
        this.interval = interval
    }

    // ~ Methods
    @get:Override
    val operator: SqlOperator
        get() = SqlMatchRecognizeOperator.INSTANCE

    @get:Override
    val kind: org.apache.calcite.sql.SqlKind
        get() = SqlKind.MATCH_RECOGNIZE

    @get:Override
    @get:SuppressWarnings("nullness")
    val operandList: List<org.apache.calcite.sql.SqlNode>
        get() = ImmutableNullableList.of(
            tableRef, pattern, strictStart, strictEnd,
            patternDefList, measureList, after, subsetList, rowsPerMatch, partitionList, orderList,
            interval
        )

    @Override
    fun unparse(
        writer: SqlWriter?, leftPrec: Int,
        rightPrec: Int
    ) {
        operator.unparse(writer, this, 0, 0)
    }

    @Override
    fun validate(validator: SqlValidator, scope: SqlValidatorScope?) {
        validator.validateMatchRecognize(this)
    }

    @SuppressWarnings("assignment.type.incompatible")
    @Override
    fun setOperand(i: Int, @Nullable operand: SqlNode) {
        when (i) {
            OPERAND_TABLE_REF -> tableRef = Objects.requireNonNull(operand, "operand")
            OPERAND_PATTERN -> pattern = operand
            OPERAND_STRICT_START -> strictStart = operand as SqlLiteral
            OPERAND_STRICT_END -> strictEnd = operand as SqlLiteral
            OPERAND_PATTERN_DEFINES -> {
                patternDefList = Objects.requireNonNull(operand as SqlNodeList)
                Preconditions.checkArgument(patternDefList.size() > 0)
            }
            OPERAND_MEASURES -> measureList = Objects.requireNonNull(operand as SqlNodeList)
            OPERAND_AFTER -> after = operand
            OPERAND_SUBSET -> subsetList = operand as SqlNodeList
            OPERAND_ROWS_PER_MATCH -> {
                rowsPerMatch = operand as SqlLiteral
                Preconditions.checkArgument(
                    rowsPerMatch == null
                            || rowsPerMatch.value is RowsPerMatchOption
                )
            }
            OPERAND_PARTITION_BY -> partitionList = operand as SqlNodeList
            OPERAND_ORDER_BY -> orderList = operand as SqlNodeList
            OPERAND_INTERVAL -> interval = operand as SqlLiteral
            else -> throw AssertionError(i)
        }
    }

    fun getTableRef(): SqlNode {
        return tableRef
    }

    fun getPattern(): SqlNode {
        return pattern
    }

    fun getStrictStart(): SqlLiteral {
        return strictStart
    }

    fun getStrictEnd(): SqlLiteral {
        return strictEnd
    }

    fun getPatternDefList(): SqlNodeList {
        return patternDefList
    }

    fun getMeasureList(): SqlNodeList? {
        return measureList
    }

    @Nullable
    fun getAfter(): SqlNode {
        return after
    }

    fun getSubsetList(): SqlNodeList? {
        return subsetList
    }

    @Nullable
    fun getRowsPerMatch(): SqlLiteral? {
        return rowsPerMatch
    }

    fun getPartitionList(): SqlNodeList? {
        return partitionList
    }

    fun getOrderList(): SqlNodeList? {
        return orderList
    }

    @Nullable
    fun getInterval(): SqlLiteral {
        return interval
    }

    /**
     * Options for `ROWS PER MATCH`.
     */
    enum class RowsPerMatchOption(sql: String) {
        ONE_ROW("ONE ROW PER MATCH"), ALL_ROWS("ALL ROWS PER MATCH");

        private val sql: String

        init {
            this.sql = sql
        }

        @Override
        override fun toString(): String {
            return sql
        }

        fun symbol(pos: SqlParserPos?): SqlLiteral {
            return SqlLiteral.createSymbol(this, pos)
        }
    }

    /**
     * Options for `AFTER MATCH` clause.
     */
    enum class AfterOption(sql: String) : Symbolizable {
        SKIP_TO_NEXT_ROW("SKIP TO NEXT ROW"), SKIP_PAST_LAST_ROW("SKIP PAST LAST ROW");

        private val sql: String

        init {
            this.sql = sql
        }

        @Override
        override fun toString(): String {
            return sql
        }
    }

    /**
     * An operator describing a MATCH_RECOGNIZE specification.
     */
    class SqlMatchRecognizeOperator private constructor() :
        SqlOperator("MATCH_RECOGNIZE", SqlKind.MATCH_RECOGNIZE, 2, true, null, null, null) {
        @get:Override
        val syntax: SqlSyntax
            get() = SqlSyntax.SPECIAL

        @SuppressWarnings("argument.type.incompatible")
        @Override
        fun createCall(
            @Nullable functionQualifier: SqlLiteral?,
            pos: SqlParserPos?,
            @Nullable vararg operands: SqlNode
        ): SqlCall {
            assert(functionQualifier == null)
            assert(operands.size == 12)
            return SqlMatchRecognize(
                pos, operands[0], operands[1],
                operands[2] as SqlLiteral, operands[3] as SqlLiteral,
                operands[4] as SqlNodeList, operands[5] as SqlNodeList, operands[6],
                operands[7] as SqlNodeList, operands[8] as SqlLiteral,
                operands[9] as SqlNodeList, operands[10] as SqlNodeList, operands[11] as SqlLiteral
            )
        }

        @Override
        fun <R> acceptCall(
            visitor: SqlVisitor<R>?,
            call: SqlCall,
            onlyExpressions: Boolean,
            argHandler: SqlBasicVisitor.ArgHandler<R>
        ) {
            if (onlyExpressions) {
                val operands: List<SqlNode> = call.getOperandList()
                for (i in 0 until operands.size()) {
                    val operand: SqlNode = operands[i] ?: continue
                    argHandler.visitChild(visitor, call, i, operand)
                }
            } else {
                super.acceptCall(visitor, call, onlyExpressions, argHandler)
            }
        }

        @Override
        fun validateCall(
            call: SqlCall?,
            validator: SqlValidator,
            scope: SqlValidatorScope?,
            operandScope: SqlValidatorScope?
        ) {
            validator.validateMatchRecognize(call)
        }

        @Override
        fun unparse(
            writer: SqlWriter,
            call: SqlCall,
            leftPrec: Int,
            rightPrec: Int
        ) {
            val pattern = call as SqlMatchRecognize
            pattern.tableRef.unparse(writer, 0, 0)
            val mrFrame: SqlWriter.Frame = writer.startFunCall("MATCH_RECOGNIZE")
            if (pattern.partitionList != null && pattern.partitionList.size() > 0) {
                writer.newlineAndIndent()
                writer.sep("PARTITION BY")
                val partitionFrame: SqlWriter.Frame = writer.startList("", "")
                pattern.partitionList.unparse(writer, 0, 0)
                writer.endList(partitionFrame)
            }
            if (pattern.orderList != null && pattern.orderList.size() > 0) {
                writer.newlineAndIndent()
                writer.sep("ORDER BY")
                writer.list(
                    SqlWriter.FrameTypeEnum.ORDER_BY_LIST, SqlWriter.COMMA,
                    pattern.orderList
                )
            }
            if (pattern.measureList != null && pattern.measureList.size() > 0) {
                writer.newlineAndIndent()
                writer.sep("MEASURES")
                val measureFrame: SqlWriter.Frame = writer.startList("", "")
                pattern.measureList.unparse(writer, 0, 0)
                writer.endList(measureFrame)
            }
            val rowsPerMatch: SqlLiteral? = pattern.rowsPerMatch
            if (rowsPerMatch != null) {
                writer.newlineAndIndent()
                rowsPerMatch.unparse(writer, 0, 0)
            }
            val after: SqlNode = pattern.after
            if (after != null) {
                writer.newlineAndIndent()
                writer.sep("AFTER MATCH")
                after.unparse(writer, 0, 0)
            }
            writer.newlineAndIndent()
            writer.sep("PATTERN")
            val patternFrame: SqlWriter.Frame = writer.startList("(", ")")
            if (pattern.strictStart.booleanValue()) {
                writer.sep("^")
            }
            pattern.pattern.unparse(writer, 0, 0)
            if (pattern.strictEnd.booleanValue()) {
                writer.sep("$")
            }
            writer.endList(patternFrame)
            val interval: SqlLiteral = pattern.interval
            if (interval != null) {
                writer.sep("WITHIN")
                interval.unparse(writer, 0, 0)
            }
            if (pattern.subsetList != null && pattern.subsetList.size() > 0) {
                writer.newlineAndIndent()
                writer.sep("SUBSET")
                val subsetFrame: SqlWriter.Frame = writer.startList("", "")
                pattern.subsetList.unparse(writer, 0, 0)
                writer.endList(subsetFrame)
            }
            writer.newlineAndIndent()
            writer.sep("DEFINE")
            val patternDefFrame: SqlWriter.Frame = writer.startList("", "")
            val newDefineList = SqlNodeList(SqlParserPos.ZERO)
            for (node in pattern.getPatternDefList()) {
                val call2: SqlCall = node as SqlCall
                // swap the position of alias position in AS operator
                newDefineList.add(
                    call2.getOperator().createCall(
                        SqlParserPos.ZERO, call2.operand(1),
                        call2.operand(0)
                    )
                )
            }
            newDefineList.unparse(writer, 0, 0)
            writer.endList(patternDefFrame)
            writer.endList(mrFrame)
        }

        companion object {
            val INSTANCE = SqlMatchRecognizeOperator()
        }
    }

    companion object {
        const val OPERAND_TABLE_REF = 0
        const val OPERAND_PATTERN = 1
        const val OPERAND_STRICT_START = 2
        const val OPERAND_STRICT_END = 3
        const val OPERAND_PATTERN_DEFINES = 4
        const val OPERAND_MEASURES = 5
        const val OPERAND_AFTER = 6
        const val OPERAND_SUBSET = 7
        const val OPERAND_ROWS_PER_MATCH = 8
        const val OPERAND_PARTITION_BY = 9
        const val OPERAND_ORDER_BY = 10
        const val OPERAND_INTERVAL = 11
        val SKIP_TO_FIRST: SqlPrefixOperator = SqlPrefixOperator(
            "SKIP TO FIRST", SqlKind.SKIP_TO_FIRST, 20, null,
            null, null
        )
        val SKIP_TO_LAST: SqlPrefixOperator = SqlPrefixOperator(
            "SKIP TO LAST", SqlKind.SKIP_TO_LAST, 20, null,
            null, null
        )
    }
}
