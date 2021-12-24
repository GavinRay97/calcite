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
package org.apache.calcite.rex

import org.apache.calcite.sql.SqlBasicCall
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlDataTypeSpec
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlNodeList
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.`fun`.SqlCaseOperator
import org.apache.calcite.sql.`fun`.SqlLibraryOperators
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeUtil
import java.util.ArrayList
import java.util.List

/**
 * Standard implementation of [RexSqlConvertletTable].
 */
class RexSqlStandardConvertletTable @SuppressWarnings("method.invocation.invalid") constructor() :
    RexSqlReflectiveConvertletTable() {
    //~ Constructors -----------------------------------------------------------
    init {

        // Register convertlets
        registerEquivOp(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL)
        registerEquivOp(SqlStdOperatorTable.GREATER_THAN)
        registerEquivOp(SqlStdOperatorTable.LESS_THAN_OR_EQUAL)
        registerEquivOp(SqlStdOperatorTable.LESS_THAN)
        registerEquivOp(SqlStdOperatorTable.EQUALS)
        registerEquivOp(SqlStdOperatorTable.NOT_EQUALS)
        registerEquivOp(SqlStdOperatorTable.AND)
        registerEquivOp(SqlStdOperatorTable.OR)
        registerEquivOp(SqlStdOperatorTable.NOT_IN)
        registerEquivOp(SqlStdOperatorTable.IN)
        registerEquivOp(SqlStdOperatorTable.LIKE)
        registerEquivOp(SqlStdOperatorTable.NOT_LIKE)
        registerEquivOp(SqlStdOperatorTable.SIMILAR_TO)
        registerEquivOp(SqlStdOperatorTable.NOT_SIMILAR_TO)
        registerEquivOp(SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE)
        registerEquivOp(SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE)
        registerEquivOp(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_SENSITIVE)
        registerEquivOp(SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_INSENSITIVE)
        registerEquivOp(SqlStdOperatorTable.PLUS)
        registerEquivOp(SqlStdOperatorTable.MINUS)
        registerEquivOp(SqlStdOperatorTable.MULTIPLY)
        registerEquivOp(SqlStdOperatorTable.DIVIDE)
        registerEquivOp(SqlStdOperatorTable.NOT)
        registerEquivOp(SqlStdOperatorTable.IS_NOT_NULL)
        registerEquivOp(SqlStdOperatorTable.IS_NULL)
        registerEquivOp(SqlStdOperatorTable.IS_NOT_TRUE)
        registerEquivOp(SqlStdOperatorTable.IS_TRUE)
        registerEquivOp(SqlStdOperatorTable.IS_NOT_FALSE)
        registerEquivOp(SqlStdOperatorTable.IS_FALSE)
        registerEquivOp(SqlStdOperatorTable.IS_NOT_UNKNOWN)
        registerEquivOp(SqlStdOperatorTable.IS_UNKNOWN)
        registerEquivOp(SqlStdOperatorTable.UNARY_MINUS)
        registerEquivOp(SqlStdOperatorTable.UNARY_PLUS)
        registerCaseOp(SqlStdOperatorTable.CASE)
        registerEquivOp(SqlStdOperatorTable.CONCAT)
        registerEquivOp(SqlStdOperatorTable.BETWEEN)
        registerEquivOp(SqlStdOperatorTable.SYMMETRIC_BETWEEN)
        registerEquivOp(SqlStdOperatorTable.NOT_BETWEEN)
        registerEquivOp(SqlStdOperatorTable.SYMMETRIC_NOT_BETWEEN)
        registerEquivOp(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM)
        registerEquivOp(SqlStdOperatorTable.IS_DISTINCT_FROM)
        registerEquivOp(SqlStdOperatorTable.MINUS_DATE)
        registerEquivOp(SqlStdOperatorTable.EXTRACT)
        registerEquivOp(SqlStdOperatorTable.SUBSTRING)
        registerEquivOp(SqlStdOperatorTable.CONVERT)
        registerEquivOp(SqlStdOperatorTable.TRANSLATE)
        registerEquivOp(SqlStdOperatorTable.OVERLAY)
        registerEquivOp(SqlStdOperatorTable.TRIM)
        registerEquivOp(SqlLibraryOperators.TRANSLATE3)
        registerEquivOp(SqlStdOperatorTable.POSITION)
        registerEquivOp(SqlStdOperatorTable.CHAR_LENGTH)
        registerEquivOp(SqlStdOperatorTable.CHARACTER_LENGTH)
        registerEquivOp(SqlStdOperatorTable.UPPER)
        registerEquivOp(SqlStdOperatorTable.LOWER)
        registerEquivOp(SqlStdOperatorTable.INITCAP)
        registerEquivOp(SqlStdOperatorTable.POWER)
        registerEquivOp(SqlStdOperatorTable.SQRT)
        registerEquivOp(SqlStdOperatorTable.MOD)
        registerEquivOp(SqlStdOperatorTable.LN)
        registerEquivOp(SqlStdOperatorTable.LOG10)
        registerEquivOp(SqlStdOperatorTable.ABS)
        registerEquivOp(SqlStdOperatorTable.EXP)
        registerEquivOp(SqlStdOperatorTable.FLOOR)
        registerEquivOp(SqlStdOperatorTable.CEIL)
        registerEquivOp(SqlStdOperatorTable.NULLIF)
        registerEquivOp(SqlStdOperatorTable.COALESCE)
        registerTypeAppendOp(SqlStdOperatorTable.CAST)
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Converts a call to an operator into a [SqlCall] to the same
     * operator.
     *
     *
     * Called automatically via reflection.
     *
     * @param converter Converter
     * @param call      Call
     * @return Sql call
     */
    @Nullable
    fun convertCall(
        converter: RexToSqlNodeConverter,
        call: RexCall
    ): SqlNode? {
        if (get(call) == null) {
            return null
        }
        val op: SqlOperator = call.getOperator()
        val operands: List<RexNode> = call.getOperands()
        @Nullable val exprs: List<Any> = convertExpressionList(converter, operands)
            ?: return null
        return SqlBasicCall(
            op,
            exprs,
            SqlParserPos.ZERO
        )
    }

    /**
     * Creates and registers a convertlet for an operator in which
     * the SQL and Rex representations are structurally equivalent.
     *
     * @param op operator instance
     */
    protected fun registerEquivOp(op: SqlOperator?) {
        registerOp(op, EquivConvertlet(op))
    }

    /**
     * Creates and registers a convertlet for an operator in which
     * the SQL representation needs the result type appended
     * as an extra argument (e.g. CAST).
     *
     * @param op operator instance
     */
    private fun registerTypeAppendOp(op: SqlOperator) {
        registerOp(
            op
        ) { converter, call ->
            @Nullable val operandList: List<Any> = convertExpressionList(converter, call.operands)
                ?: return@registerOp null
            val typeSpec: SqlDataTypeSpec = SqlTypeUtil.convertTypeToSpec(call.getType())
            operandList.add(typeSpec)
            SqlBasicCall(op, operandList, SqlParserPos.ZERO)
        }
    }

    /**
     * Creates and registers a convertlet for the CASE operator,
     * which takes different forms for SQL vs Rex.
     *
     * @param op instance of CASE operator
     */
    private fun registerCaseOp(op: SqlOperator) {
        registerOp(
            op
        ) { converter, call ->
            assert(op is SqlCaseOperator)
            @Nullable val operands: List<Any> = convertExpressionList(converter, call.operands)
                ?: return@registerOp null
            val whenList = SqlNodeList(SqlParserPos.ZERO)
            val thenList = SqlNodeList(SqlParserPos.ZERO)
            var i = 0
            while (i < operands.size() - 1) {
                whenList.add(operands[i])
                ++i
                thenList.add(operands[i])
                ++i
            }
            val elseExpr: SqlNode = operands[i]
            op.createCall(null, SqlParserPos.ZERO, null, whenList, thenList, elseExpr)
        }
    }

    /** Convertlet that converts a [SqlCall] to a [RexCall] of the
     * same operator.  */
    private class EquivConvertlet internal constructor(op: SqlOperator?) : RexSqlConvertlet {
        private val op: SqlOperator?

        init {
            this.op = op
        }

        @Override
        @Nullable
        fun convertCall(converter: RexToSqlNodeConverter, call: RexCall): SqlNode? {
            @Nullable val operands: List<Any> = convertExpressionList(converter, call.operands)
                ?: return null
            return SqlBasicCall(op, operands, SqlParserPos.ZERO)
        }
    }

    companion object {
        @Nullable
        private fun convertExpressionList(
            converter: RexToSqlNodeConverter,
            nodes: List<RexNode>
        ): List<SqlNode>? {
            val exprs: List<SqlNode> = ArrayList()
            for (node in nodes) {
                @Nullable val converted: SqlNode = converter.convertNode(node) ?: return null
                exprs.add(converted)
            }
            return exprs
        }
    }
}
