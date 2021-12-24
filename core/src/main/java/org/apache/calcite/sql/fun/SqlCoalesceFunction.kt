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
 * The `COALESCE` function.
 */
class SqlCoalesceFunction  //~ Constructors -----------------------------------------------------------
    : SqlFunction(
    "COALESCE",
    SqlKind.COALESCE,
    ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.LEAST_NULLABLE),
    null,
    OperandTypes.SAME_VARIADIC,
    SqlFunctionCategory.SYSTEM
) {
    //~ Methods ----------------------------------------------------------------
    // override SqlOperator
    @Override
    fun rewriteCall(validator: SqlValidator?, call: SqlCall): SqlNode {
        validateQuantifier(validator, call) // check DISTINCT/ALL
        val operands: List<SqlNode> = call.getOperandList()
        if (operands.size() === 1) {
            // No CASE needed
            return operands[0]
        }
        val pos: SqlParserPos = call.getParserPosition()
        val whenList = SqlNodeList(pos)
        val thenList = SqlNodeList(pos)

        // todo: optimize when know operand is not null.
        for (operand in Util.skipLast(operands)) {
            whenList.add(
                SqlStdOperatorTable.IS_NOT_NULL.createCall(pos, operand)
            )
            thenList.add(SqlNode.clone(operand))
        }
        val elseExpr: SqlNode = Util.last(operands)
        assert(call.getFunctionQuantifier() == null)
        return SqlCase.createSwitched(pos, null, whenList, thenList, elseExpr)
    }
}
