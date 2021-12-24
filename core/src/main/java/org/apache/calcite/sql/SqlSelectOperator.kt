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

import org.apache.calcite.sql.`fun`.SqlStdOperatorTable

/**
 * An operator describing a query. (Not a query itself.)
 *
 *
 * Operands are:
 *
 *
 *  * 0: distinct ([SqlLiteral])
 *  * 1: selectClause ([SqlNodeList])
 *  * 2: fromClause ([SqlCall] to "join" operator)
 *  * 3: whereClause ([SqlNode])
 *  * 4: havingClause ([SqlNode])
 *  * 5: groupClause ([SqlNode])
 *  * 6: windowClause ([SqlNodeList])
 *  * 7: orderClause ([SqlNode])
 *
 */
class SqlSelectOperator  //~ Constructors -----------------------------------------------------------
private constructor() : SqlOperator("SELECT", SqlKind.SELECT, 2, true, ReturnTypes.SCOPE, null, null) {
    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val syntax: org.apache.calcite.sql.SqlSyntax
        get() = SqlSyntax.SPECIAL

    @Override
    override fun createCall(
        @Nullable functionQualifier: SqlLiteral?,
        pos: SqlParserPos?,
        @Nullable vararg operands: SqlNode?
    ): SqlCall {
        assert(functionQualifier == null)
        return SqlSelect(
            pos,
            operands[0] as SqlNodeList?,
            requireNonNull(operands[1] as SqlNodeList?, "selectList"),
            operands[2],
            operands[3],
            operands[4] as SqlNodeList?,
            operands[5],
            operands[6] as SqlNodeList?,
            operands[7] as SqlNodeList?,
            operands[8],
            operands[9],
            operands[10] as SqlNodeList?
        )
    }

    /**
     * Creates a call to the `SELECT` operator.
     *
     */
    @Deprecated // to be removed before 2.0
    @Deprecated("Use {@link #createCall(SqlLiteral, SqlParserPos, SqlNode...)}.")
    fun createCall(
        keywordList: SqlNodeList?,
        selectList: SqlNodeList?,
        fromClause: SqlNode?,
        whereClause: SqlNode?,
        groupBy: SqlNodeList?,
        having: SqlNode?,
        windowDecls: SqlNodeList?,
        orderBy: SqlNodeList?,
        offset: SqlNode?,
        fetch: SqlNode?,
        hints: SqlNodeList?,
        pos: SqlParserPos?
    ): SqlSelect {
        return SqlSelect(
            pos,
            keywordList,
            selectList,
            fromClause,
            whereClause,
            groupBy,
            having,
            windowDecls,
            orderBy,
            offset,
            fetch,
            hints
        )
    }

    @Override
    override fun <R> acceptCall(
        visitor: SqlVisitor<R>?,
        call: SqlCall,
        onlyExpressions: Boolean,
        argHandler: SqlBasicVisitor.ArgHandler<R>
    ) {
        if (!onlyExpressions) {
            // None of the arguments to the SELECT operator are expressions.
            super.acceptCall(visitor, call, onlyExpressions, argHandler)
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    override fun unparse(
        writer: SqlWriter,
        call: SqlCall,
        leftPrec: Int,
        rightPrec: Int
    ) {
        val select: SqlSelect = call
        val selectFrame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT)
        writer.sep("SELECT")
        if (select.hasHints()) {
            writer.sep("/*+")
            castNonNull(select.hints).unparse(writer, 0, 0)
            writer.print("*/")
            writer.newlineAndIndent()
        }
        for (i in 0 until select.keywordList.size()) {
            val keyword: SqlNode = select.keywordList.get(i)
            keyword.unparse(writer, 0, 0)
        }
        writer.topN(select.fetch, select.offset)
        val selectClause: SqlNodeList = select.selectList
        writer.list(
            SqlWriter.FrameTypeEnum.SELECT_LIST, SqlWriter.COMMA,
            selectClause
        )
        if (select.from != null) {
            // Calcite SQL requires FROM but MySQL does not.
            writer.sep("FROM")

            // for FROM clause, use precedence just below join operator to make
            // sure that an un-joined nested select will be properly
            // parenthesized
            val fromFrame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.FROM_LIST)
            select.from.unparse(
                writer,
                SqlJoin.OPERATOR.getLeftPrec() - 1,
                SqlJoin.OPERATOR.getRightPrec() - 1
            )
            writer.endList(fromFrame)
        }
        val where: SqlNode = select.where
        if (where != null) {
            writer.sep("WHERE")
            if (!writer.isAlwaysUseParentheses()) {
                var node: SqlNode = where

                // decide whether to split on ORs or ANDs
                var whereSep: SqlBinaryOperator = SqlStdOperatorTable.AND
                if (node is SqlCall
                    && node.getKind() === SqlKind.OR
                ) {
                    whereSep = SqlStdOperatorTable.OR
                }

                // unroll whereClause
                val list: List<SqlNode> = ArrayList(0)
                while (node.getKind() === whereSep.kind) {
                    assert(node is SqlCall)
                    val call1: SqlCall = node as SqlCall
                    list.add(0, call1.operand(1))
                    node = call1.operand(0)
                }
                list.add(0, node)

                // unparse in a WHERE_LIST frame
                writer.list(
                    SqlWriter.FrameTypeEnum.WHERE_LIST, whereSep,
                    SqlNodeList(list, where.getParserPosition())
                )
            } else {
                where.unparse(writer, 0, 0)
            }
        }
        if (select.groupBy != null) {
            writer.sep("GROUP BY")
            val groupBy: SqlNodeList = if (select.groupBy.size() === 0) SqlNodeList.SINGLETON_EMPTY else select.groupBy
            writer.list(
                SqlWriter.FrameTypeEnum.GROUP_BY_LIST, SqlWriter.COMMA,
                groupBy
            )
        }
        if (select.having != null) {
            writer.sep("HAVING")
            select.having.unparse(writer, 0, 0)
        }
        if (select.windowDecls.size() > 0) {
            writer.sep("WINDOW")
            writer.list(
                SqlWriter.FrameTypeEnum.WINDOW_DECL_LIST, SqlWriter.COMMA,
                select.windowDecls
            )
        }
        if (select.orderBy != null && select.orderBy.size() > 0) {
            writer.sep("ORDER BY")
            writer.list(
                SqlWriter.FrameTypeEnum.ORDER_BY_LIST, SqlWriter.COMMA,
                select.orderBy
            )
        }
        writer.fetchOffset(select.fetch, select.offset)
        writer.endList(selectFrame)
    }

    @Override
    override fun argumentMustBeScalar(ordinal: Int): Boolean {
        return ordinal == SqlSelect.WHERE_OPERAND
    }

    companion object {
        val INSTANCE = SqlSelectOperator()
    }
}
