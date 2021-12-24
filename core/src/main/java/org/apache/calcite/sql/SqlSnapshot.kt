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
 * Parse tree node for "`FOR SYSTEM_TIME AS OF`" temporal clause.
 */
class SqlSnapshot(pos: SqlParserPos?, tableRef: SqlNode?, period: SqlNode?) : SqlCall(pos) {
    //~ Instance fields -------------------------------------------
    private var tableRef: SqlNode
    private var period: SqlNode

    /** Creates a SqlSnapshot.  */
    init {
        this.tableRef = Objects.requireNonNull(tableRef, "tableRef")
        this.period = Objects.requireNonNull(period, "period")
    }

    // ~ Methods
    @get:Override
    val operator: org.apache.calcite.sql.SqlOperator
        get() = SqlSnapshotOperator.INSTANCE

    @get:Override
    val operandList: List<Any>
        get() = ImmutableNullableList.of(tableRef, period)

    fun getTableRef(): SqlNode {
        return tableRef
    }

    fun getPeriod(): SqlNode {
        return period
    }

    @Override
    fun setOperand(i: Int, @Nullable operand: SqlNode?) {
        when (i) {
            OPERAND_TABLE_REF -> tableRef = Objects.requireNonNull(operand, "operand")
            OPERAND_PERIOD -> period = Objects.requireNonNull(operand, "operand")
            else -> throw AssertionError(i)
        }
    }

    @Override
    fun unparse(writer: SqlWriter?, leftPrec: Int, rightPrec: Int) {
        operator.unparse(writer, this, 0, 0)
    }

    /**
     * An operator describing a FOR SYSTEM_TIME specification.
     */
    class SqlSnapshotOperator private constructor() :
        SqlOperator("SNAPSHOT", SqlKind.SNAPSHOT, 2, true, null, null, null) {
        @get:Override
        override val syntax: org.apache.calcite.sql.SqlSyntax
            get() = SqlSyntax.SPECIAL

        @SuppressWarnings("argument.type.incompatible")
        @Override
        override fun createCall(
            @Nullable functionQualifier: SqlLiteral?,
            pos: SqlParserPos?,
            @Nullable vararg operands: SqlNode?
        ): SqlCall {
            assert(functionQualifier == null)
            assert(operands.size == 2)
            return SqlSnapshot(pos, operands[0], operands[1])
        }

        @Override
        override fun <R> acceptCall(
            visitor: SqlVisitor<R>?,
            call: SqlCall,
            onlyExpressions: Boolean,
            argHandler: SqlBasicVisitor.ArgHandler<R>
        ) {
            if (onlyExpressions) {
                val operands: List<SqlNode> = call.getOperandList()
                // skip the first operand
                for (i in 1 until operands.size()) {
                    argHandler.visitChild(visitor, call, i, operands[i])
                }
            } else {
                super.acceptCall(visitor, call, false, argHandler)
            }
        }

        @Override
        override fun unparse(
            writer: SqlWriter,
            call: SqlCall,
            leftPrec: Int,
            rightPrec: Int
        ) {
            val snapshot = call as SqlSnapshot
            val tableRef: SqlNode = snapshot.tableRef
            if (tableRef is SqlBasicCall
                && (tableRef as SqlBasicCall).getOperator() is SqlAsOperator
            ) {
                val basicCall: SqlBasicCall = tableRef as SqlBasicCall
                basicCall.operand(0).unparse(writer, 0, 0)
                writer.setNeedWhitespace(true)
                writeForSystemTimeAsOf(writer, snapshot)
                writer.keyword("AS")
                basicCall.operand(1).unparse(writer, 0, 0)
            } else {
                tableRef.unparse(writer, 0, 0)
                writeForSystemTimeAsOf(writer, snapshot)
            }
        }

        companion object {
            val INSTANCE = SqlSnapshotOperator()
            private fun writeForSystemTimeAsOf(writer: SqlWriter, snapshot: SqlSnapshot) {
                writer.keyword("FOR SYSTEM_TIME AS OF")
                snapshot.period.unparse(writer, 0, 0)
            }
        }
    }

    companion object {
        private const val OPERAND_TABLE_REF = 0
        private const val OPERAND_PERIOD = 1
    }
}
