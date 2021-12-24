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

import org.apache.calcite.linq4j.Ord

/** `EXTEND` operator.
 *
 *
 * Adds columns to a table's schema, as in
 * `SELECT ... FROM emp EXTEND (horoscope VARCHAR(100))`.
 *
 *
 * Not standard SQL. Added to Calcite to support Phoenix, but can be used to
 * achieve schema-on-query against other adapters.
 */
internal class SqlExtendOperator : SqlInternalOperator("EXTEND", SqlKind.EXTEND, MDX_PRECEDENCE) {
    @Override
    fun unparse(
        writer: SqlWriter, call: SqlCall, leftPrec: Int,
        rightPrec: Int
    ) {
        val operator: SqlOperator = call.getOperator()
        assert(call.operandCount() === 2)
        val frame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.SIMPLE)
        call.operand(0).unparse(writer, leftPrec, operator.getLeftPrec())
        writer.setNeedWhitespace(true)
        writer.sep(operator.getName())
        val list: SqlNodeList = call.operand(1)
        val frame2: SqlWriter.Frame = writer.startList("(", ")")
        for (node2 in Ord.zip(list)) {
            if (node2.i > 0 && node2.i % 2 === 0) {
                writer.sep(",")
            }
            node2.e.unparse(writer, 2, 3)
        }
        writer.endList(frame2)
        writer.endList(frame)
    }
}
