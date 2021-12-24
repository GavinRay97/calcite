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
 * The `NULLIF` function.
 */
class SqlNullifFunction  //~ Constructors -----------------------------------------------------------
    : SqlFunction(
    "NULLIF",
    SqlKind.NULLIF,
    ReturnTypes.ARG0_FORCE_NULLABLE,
    null,
    OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED,
    SqlFunctionCategory.SYSTEM
) {
    //~ Methods ----------------------------------------------------------------
    // override SqlOperator
    @Override
    fun rewriteCall(validator: SqlValidator?, call: SqlCall): SqlNode {
        val operands: List<SqlNode> = call.getOperandList()
        val pos: SqlParserPos = call.getParserPosition()
        checkOperandCount(
            validator,
            getOperandTypeChecker(),
            call
        )
        assert(operands.size() === 2)
        val whenList = SqlNodeList(pos)
        val thenList = SqlNodeList(pos)
        whenList.add(operands[1])
        thenList.add(SqlLiteral.createNull(SqlParserPos.ZERO))
        return SqlCase.createSwitched(
            pos, operands[0], whenList, thenList,
            SqlNode.clone(operands[0])
        )
    }
}
