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
 * An item in a WITH clause of a query.
 * It has a name, an optional column list, and a query.
 */
class SqlWithItem(
    pos: SqlParserPos?, name: SqlIdentifier,
    @Nullable columnList: SqlNodeList?, query: SqlNode
) : SqlCall(pos) {
    var name: SqlIdentifier

    @Nullable
    var columnList // may be null
            : SqlNodeList?
    var query: SqlNode

    init {
        this.name = name
        this.columnList = columnList
        this.query = query
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val kind: SqlKind
        get() = SqlKind.WITH_ITEM

    @get:Override
    @get:SuppressWarnings("nullness")
    val operandList: List<Any>
        get() = ImmutableNullableList.of(name, columnList, query)

    @SuppressWarnings("assignment.type.incompatible")
    @Override
    fun setOperand(i: Int, @Nullable operand: SqlNode) {
        when (i) {
            0 -> name = operand as SqlIdentifier
            1 -> columnList = operand as SqlNodeList
            2 -> query = operand
            else -> throw AssertionError(i)
        }
    }

    @get:Override
    val operator: SqlOperator
        get() = SqlWithItemOperator.INSTANCE

    /**
     * SqlWithItemOperator is used to represent an item in a WITH clause of a
     * query. It has a name, an optional column list, and a query.
     */
    private class SqlWithItemOperator internal constructor() : SqlSpecialOperator("WITH_ITEM", SqlKind.WITH_ITEM, 0) {
        //~ Methods ----------------------------------------------------------------
        @Override
        fun unparse(
            writer: SqlWriter,
            call: SqlCall,
            leftPrec: Int,
            rightPrec: Int
        ) {
            val withItem = call as SqlWithItem
            withItem.name.unparse(writer, getLeftPrec(), getRightPrec())
            if (withItem.columnList != null) {
                withItem.columnList.unparse(writer, getLeftPrec(), getRightPrec())
            }
            writer.keyword("AS")
            withItem.query.unparse(writer, 10, 10)
        }

        @SuppressWarnings("argument.type.incompatible")
        @Override
        fun createCall(
            @Nullable functionQualifier: SqlLiteral?,
            pos: SqlParserPos?, @Nullable vararg operands: SqlNode
        ): SqlCall {
            assert(functionQualifier == null)
            assert(operands.size == 3)
            return SqlWithItem(
                pos, operands[0] as SqlIdentifier,
                operands[1] as SqlNodeList, operands[2]
            )
        }

        companion object {
            val INSTANCE = SqlWithItemOperator()
        }
    }
}
