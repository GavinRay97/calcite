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

import org.apache.calcite.rel.type.RelDataType

/**
 * SqlOverlapsOperator represents the SQL:1999 standard `OVERLAPS`
 * function. Determines whether two anchored time intervals overlap.
 */
class SqlOverlapsOperator  //~ Constructors -----------------------------------------------------------
internal constructor(kind: SqlKind) : SqlBinaryOperator(
    kind.sql, kind, 30, true, ReturnTypes.BOOLEAN_NULLABLE,
    InferTypes.FIRST_KNOWN,
    OperandTypes.sequence(
        "'<PERIOD> " + kind.sql.toString() + " <PERIOD>'",
        OperandTypes.PERIOD, OperandTypes.PERIOD
    )
) {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun unparse(
        writer: SqlWriter, call: SqlCall, leftPrec: Int,
        rightPrec: Int
    ) {
        val frame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.SIMPLE)
        arg(writer, call, leftPrec, rightPrec, 0)
        writer.sep(getName())
        arg(writer, call, leftPrec, rightPrec, 1)
        writer.endList(frame)
    }

    fun arg(writer: SqlWriter, call: SqlCall, leftPrec: Int, rightPrec: Int, i: Int) {
        if (SqlUtil.isCallTo(call.operand(i), SqlStdOperatorTable.ROW)) {
            val row: SqlCall = call.operand(i)
            writer.keyword("PERIOD")
            writer.sep("(", true)
            row.operand(0).unparse(writer, leftPrec, rightPrec)
            writer.sep(",", true)
            row.operand(1).unparse(writer, leftPrec, rightPrec)
            writer.sep(")", true)
        } else {
            call.operand(i).unparse(writer, leftPrec, rightPrec)
        }
    }

    @get:Override
    val operandCountRange: SqlOperandCountRange
        get() = SqlOperandCountRanges.of(2)

    @Override
    fun getAllowedSignatures(opName: String?): String {
        val d = "DATETIME"
        val i = "INTERVAL"
        val typeNames = arrayOf(
            d, d,
            d, i,
            i, d,
            i, i
        )
        val ret = StringBuilder()
        var y = 0
        while (y < typeNames.size) {
            if (y > 0) {
                ret.append(NL)
            }
            ret.append(
                SqlUtil.getAliasedSignature(
                    this, opName,
                    ImmutableList.of(d, typeNames[y], d, typeNames[y + 1])
                )
            )
            y += 2
        }
        return ret.toString()
    }

    @Override
    fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        if (!OperandTypes.PERIOD.checkSingleOperandType(
                callBinding,
                callBinding.operand(0), 0, throwOnFailure
            )
        ) {
            return false
        }
        val rightChecker: SqlSingleOperandTypeChecker
        rightChecker = when (kind) {
            CONTAINS -> OperandTypes.PERIOD_OR_DATETIME
            else -> OperandTypes.PERIOD
        }
        if (!rightChecker.checkSingleOperandType(
                callBinding,
                callBinding.operand(1), 0, throwOnFailure
            )
        ) {
            return false
        }
        val t0: RelDataType = callBinding.getOperandType(0)
        val t1: RelDataType = callBinding.getOperandType(1)
        if (!SqlTypeUtil.isDatetime(t1)) {
            val t00: RelDataType = t0.getFieldList().get(0).getType()
            val t10: RelDataType = t1.getFieldList().get(0).getType()
            if (!SqlTypeUtil.sameNamedType(t00, t10)) {
                if (throwOnFailure) {
                    throw callBinding.newValidationSignatureError()
                }
                return false
            }
        }
        return true
    }
}
