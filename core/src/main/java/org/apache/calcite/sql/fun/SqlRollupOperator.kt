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
 * Operator that appears in a `GROUP BY` clause: `CUBE`,
 * `ROLLUP`, `GROUPING SETS`.
 */
internal class SqlRollupOperator(name: String?, kind: SqlKind?) : SqlInternalOperator(name, kind, 4) {
    @Override
    fun unparse(
        writer: SqlWriter, call: SqlCall, leftPrec: Int,
        rightPrec: Int
    ) {
        when (kind) {
            ROLLUP -> if (!writer.getDialect().supportsAggregateFunction(kind)
                && writer.getDialect().supportsGroupByWithRollup()
            ) {
                // MySQL version 5: generate "GROUP BY x, y WITH ROLLUP".
                // MySQL version 8 and higher is SQL-compliant,
                // so generate "GROUP BY ROLLUP(x, y)"
                unparseKeyword(writer, call, "WITH ROLLUP")
                return
            }
            CUBE -> if (!writer.getDialect().supportsAggregateFunction(kind)
                && writer.getDialect().supportsGroupByWithCube()
            ) {
                // Spark SQL: generate "GROUP BY x, y WITH CUBE".
                unparseKeyword(writer, call, "WITH CUBE")
                return
            }
            else -> {}
        }
        unparseCube(writer, call)
    }

    companion object {
        private fun unparseKeyword(writer: SqlWriter, call: SqlCall, keyword: String) {
            val groupFrame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.GROUP_BY_LIST)
            for (operand in call.getOperandList()) {
                writer.sep(",")
                operand.unparse(writer, 2, 3)
            }
            writer.endList(groupFrame)
            writer.keyword(keyword)
        }

        private fun unparseCube(writer: SqlWriter, call: SqlCall) {
            writer.keyword(call.getOperator().getName())
            val frame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")")
            for (operand in call.getOperandList()) {
                writer.sep(",")
                if (operand.getKind() === SqlKind.ROW) {
                    val frame2: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.SIMPLE, "(", ")")
                    for (operand2 in (operand as SqlCall).getOperandList()) {
                        writer.sep(",")
                        operand2.unparse(writer, 0, 0)
                    }
                    writer.endList(frame2)
                } else if (operand is SqlNodeList
                    && (operand as SqlNodeList).size() === 0
                ) {
                    writer.keyword("()")
                } else {
                    operand.unparse(writer, 0, 0)
                }
            }
            writer.endList(frame)
        }
    }
}
