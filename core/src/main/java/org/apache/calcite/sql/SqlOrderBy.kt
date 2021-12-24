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
 * Parse tree node that represents an `ORDER BY` on a query other than a
 * `SELECT` (e.g. `VALUES` or `UNION`).
 *
 *
 * It is a purely syntactic operator, and is eliminated by
 * [org.apache.calcite.sql.validate.SqlValidatorImpl.performUnconditionalRewrites]
 * and replaced with the ORDER_OPERAND of SqlSelect.
 */
class SqlOrderBy(
    pos: SqlParserPos?, query: SqlNode, orderList: SqlNodeList,
    @Nullable offset: SqlNode?, @Nullable fetch: SqlNode?
) : SqlCall(pos) {
    val query: SqlNode
    val orderList: SqlNodeList

    @Nullable
    val offset: SqlNode?

    @Nullable
    val fetch: SqlNode?

    //~ Constructors -----------------------------------------------------------
    init {
        this.query = query
        this.orderList = orderList
        this.offset = offset
        this.fetch = fetch
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val kind: SqlKind
        get() = SqlKind.ORDER_BY

    @get:Override
    val operator: org.apache.calcite.sql.SqlOperator
        get() = OPERATOR

    @get:Override
    @get:SuppressWarnings("nullness")
    val operandList: List<Any>
        get() = ImmutableNullableList.of(query, orderList, offset, fetch)

    /** Definition of `ORDER BY` operator.  */
    private class Operator : SqlSpecialOperator("ORDER BY", SqlKind.ORDER_BY, 0) {
        @get:Override
        override val syntax: org.apache.calcite.sql.SqlSyntax
            get() = SqlSyntax.POSTFIX

        @Override
        override fun unparse(
            writer: SqlWriter,
            call: SqlCall,
            leftPrec: Int,
            rightPrec: Int
        ) {
            val orderBy = call as SqlOrderBy
            val frame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.ORDER_BY)
            orderBy.query.unparse(writer, getLeftPrec(), getRightPrec())
            if (orderBy.orderList !== SqlNodeList.EMPTY) {
                writer.sep(getName())
                writer.list(
                    SqlWriter.FrameTypeEnum.ORDER_BY_LIST, SqlWriter.COMMA,
                    orderBy.orderList
                )
            }
            if (orderBy.offset != null || orderBy.fetch != null) {
                writer.fetchOffset(orderBy.fetch, orderBy.offset)
            }
            writer.endList(frame)
        }
    }

    companion object {
        val OPERATOR: SqlSpecialOperator = object : Operator() {
            @SuppressWarnings("argument.type.incompatible")
            @Override
            override fun createCall(
                @Nullable functionQualifier: SqlLiteral?,
                pos: SqlParserPos?, @Nullable vararg operands: SqlNode
            ): SqlCall {
                return SqlOrderBy(
                    pos, operands[0], operands[1] as SqlNodeList,
                    operands[2], operands[3]
                )
            }
        }
    }
}
