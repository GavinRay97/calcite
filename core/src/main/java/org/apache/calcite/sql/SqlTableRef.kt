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
 * A `SqlTableRef` is a node of a parse tree which represents
 * a table reference.
 *
 *
 * It can be attached with a sql hint statement, see [SqlHint] for details.
 */
class SqlTableRef(pos: SqlParserPos?, tableName: SqlIdentifier, hints: SqlNodeList?) : SqlCall(pos) {
    //~ Instance fields --------------------------------------------------------
    private val tableName: SqlIdentifier
    private val hints: SqlNodeList?

    //~ Constructors -----------------------------------------------------------
    init {
        this.tableName = tableName
        this.hints = hints
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val operator: SqlOperator
        get() = OPERATOR

    @get:Override
    val operandList: List<Any>
        get() = ImmutableList.of(tableName, hints)

    @Override
    fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        tableName.unparse(writer, leftPrec, rightPrec)
        if (hints != null && hints.size() > 0) {
            writer.getDialect().unparseTableScanHints(writer, hints, leftPrec, rightPrec)
        }
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        private val OPERATOR: SqlOperator = object : SqlSpecialOperator("TABLE_REF", SqlKind.TABLE_REF) {
            @Override
            fun createCall(
                @Nullable functionQualifier: SqlLiteral?,
                pos: SqlParserPos?, @Nullable vararg operands: SqlNode?
            ): SqlCall {
                return SqlTableRef(
                    pos,
                    requireNonNull(operands[0], "tableName") as SqlIdentifier,
                    requireNonNull(operands[1], "hints") as SqlNodeList?
                )
            }
        }
    }
}
