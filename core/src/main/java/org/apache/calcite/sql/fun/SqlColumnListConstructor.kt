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
 * SqlColumnListConstructor defines the non-standard constructor used to pass a
 * COLUMN_LIST parameter to a UDX.
 */
class SqlColumnListConstructor  //~ Constructors -----------------------------------------------------------
    : SqlSpecialOperator(
    "COLUMN_LIST",
    SqlKind.COLUMN_LIST, MDX_PRECEDENCE,
    false,
    ReturnTypes.COLUMN_LIST,
    null,
    OperandTypes.ANY
) {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun unparse(
        writer: SqlWriter,
        call: SqlCall,
        leftPrec: Int,
        rightPrec: Int
    ) {
        writer.keyword("ROW")
        val frame: SqlWriter.Frame = writer.startList("(", ")")
        for (operand in call.getOperandList()) {
            writer.sep(",")
            operand.unparse(writer, leftPrec, rightPrec)
        }
        writer.endList(frame)
    }
}
