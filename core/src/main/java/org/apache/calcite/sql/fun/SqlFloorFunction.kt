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
 * Definition of the "FLOOR" and "CEIL" built-in SQL functions.
 */
class SqlFloorFunction(kind: SqlKind) : SqlMonotonicUnaryFunction(
    kind.name(), kind, ReturnTypes.ARG0_OR_EXACT_NO_SCALE, null,
    OperandTypes.or(
        OperandTypes.NUMERIC_OR_INTERVAL,
        OperandTypes.sequence(
            """
        '$kind(<DATE> TO <TIME_UNIT>)'
        '$kind(<TIME> TO <TIME_UNIT>)'
        '$kind(<TIMESTAMP> TO <TIME_UNIT>)'
        """.trimIndent(),
            OperandTypes.DATETIME,
            OperandTypes.ANY
        )
    ),
    SqlFunctionCategory.NUMERIC
) {
    //~ Constructors -----------------------------------------------------------
    init {
        Preconditions.checkArgument(kind === SqlKind.FLOOR || kind === SqlKind.CEIL)
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun getMonotonicity(call: SqlOperatorBinding): SqlMonotonicity {
        // Monotonic iff its first argument is, but not strict.
        return call.getOperandMonotonicity(0).unstrict()
    }

    @Override
    fun unparse(
        writer: SqlWriter, call: SqlCall, leftPrec: Int,
        rightPrec: Int
    ) {
        val frame: SqlWriter.Frame = writer.startFunCall(getName())
        if (call.operandCount() === 2) {
            call.operand(0).unparse(writer, 0, 100)
            writer.sep("TO")
            call.operand(1).unparse(writer, 100, 0)
        } else {
            call.operand(0).unparse(writer, 0, 0)
        }
        writer.endFunCall(frame)
    }

    companion object {
        /**
         * Copies a [SqlCall], replacing the time unit operand with the given
         * literal.
         *
         * @param call Call
         * @param literal Literal to replace time unit with
         * @param pos Parser position
         * @return Modified call
         */
        fun replaceTimeUnitOperand(call: SqlCall, literal: String?, pos: SqlParserPos?): SqlCall {
            val literalNode: SqlLiteral = SqlLiteral.createCharString(literal, null, pos)
            return call.getOperator().createCall(
                call.getFunctionQuantifier(), pos,
                call.getOperandList().get(0), literalNode
            )
        }

        /**
         * Most dialects that natively support datetime floor will use this.
         * In those cases the call will look like TRUNC(datetime, 'year').
         *
         * @param writer SqlWriter
         * @param call SqlCall
         * @param funName Name of the sql function to call
         * @param datetimeFirst Specify the order of the datetime &amp; timeUnit
         * arguments
         */
        fun unparseDatetimeFunction(
            writer: SqlWriter?, call: SqlCall,
            funName: String?, datetimeFirst: Boolean
        ) {
            val func = SqlFunction(
                funName, SqlKind.OTHER_FUNCTION,
                ReturnTypes.ARG0_NULLABLE_VARYING, null, null,
                SqlFunctionCategory.STRING
            )
            val call1: SqlCall
            call1 = if (datetimeFirst) {
                call
            } else {
                // switch order of operands
                val op1: SqlNode = call.operand(0)
                val op2: SqlNode = call.operand(1)
                call.getOperator().createCall(call.getParserPosition(), op2, op1)
            }
            SqlUtil.unparseFunctionSyntax(func, writer, call1, false)
        }
    }
}
