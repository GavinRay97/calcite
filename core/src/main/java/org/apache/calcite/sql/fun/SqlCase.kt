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
package org.apache.calcite.sql.`fun`

import org.apache.calcite.sql.SqlCall

/**
 * A `SqlCase` is a node of a parse tree which represents a case
 * statement. It warrants its own node type just because we have a lot of
 * methods to put somewhere.
 */
class SqlCase(
    pos: SqlParserPos?, @Nullable value: SqlNode?, whenList: SqlNodeList,
    thenList: SqlNodeList, @Nullable elseExpr: SqlNode?
) : SqlCall(pos) {
    @Nullable
    var value: SqlNode?
    var whenList: SqlNodeList
    var thenList: SqlNodeList

    @Nullable
    var elseExpr: SqlNode?
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a SqlCase expression.
     *
     * @param pos Parser position
     * @param value The value (null for boolean case)
     * @param whenList List of all WHEN expressions
     * @param thenList List of all THEN expressions
     * @param elseExpr The implicit or explicit ELSE expression
     */
    init {
        this.value = value
        this.whenList = whenList
        this.thenList = thenList
        this.elseExpr = elseExpr
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val kind: SqlKind
        get() = SqlKind.CASE

    @get:Override
    val operator: SqlOperator
        get() = SqlStdOperatorTable.CASE

    @get:Override
    @get:SuppressWarnings("nullness")
    val operandList: List<Any>
        get() = UnmodifiableArrayList.of(value, whenList, thenList, elseExpr)

    @SuppressWarnings("assignment.type.incompatible")
    @Override
    fun setOperand(i: Int, @Nullable operand: SqlNode) {
        when (i) {
            0 -> value = operand
            1 -> whenList = operand as SqlNodeList
            2 -> thenList = operand as SqlNodeList
            3 -> elseExpr = operand
            else -> throw AssertionError(i)
        }
    }

    @get:Nullable
    val valueOperand: SqlNode?
        get() = value
    val whenOperands: SqlNodeList
        get() = whenList
    val thenOperands: SqlNodeList
        get() = thenList

    @get:Nullable
    val elseOperand: SqlNode?
        get() = elseExpr

    companion object {
        /**
         * Creates a call to the switched form of the CASE operator. For example:
         *
         * <blockquote>`CASE value<br></br>
         * WHEN whenList[0] THEN thenList[0]<br></br>
         * WHEN whenList[1] THEN thenList[1]<br></br>
         * ...<br></br>
         * ELSE elseClause<br></br>
         * END`</blockquote>
         */
        fun createSwitched(
            pos: SqlParserPos?, @Nullable value: SqlNode?,
            whenList: SqlNodeList, thenList: SqlNodeList, @Nullable elseClause: SqlNode?
        ): SqlCase {
            var elseClause: SqlNode? = elseClause
            if (null != value) {
                for (i in 0 until whenList.size()) {
                    val e: SqlNode = whenList.get(i)
                    val call: SqlCall
                    call = if (e is SqlNodeList) {
                        SqlStdOperatorTable.IN.createCall(pos, value, e)
                    } else {
                        SqlStdOperatorTable.EQUALS.createCall(pos, value, e)
                    }
                    whenList.set(i, call)
                }
            }
            if (null == elseClause) {
                elseClause = SqlLiteral.createNull(pos)
            }
            return SqlCase(pos, null, whenList, thenList, elseClause)
        }
    }
}
