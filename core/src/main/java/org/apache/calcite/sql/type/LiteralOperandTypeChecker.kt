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
package org.apache.calcite.sql.type

import org.apache.calcite.sql.SqlCallBinding

/**
 * Parameter type-checking strategy type must be a literal (whether null is
 * allowed is determined by the constructor). `CAST(NULL as ...)` is
 * considered to be a NULL literal but not `CAST(CAST(NULL as ...) AS
 * ...)`
 */
class LiteralOperandTypeChecker     //~ Constructors -----------------------------------------------------------
    (  //~ Instance fields --------------------------------------------------------
    private val allowNull: Boolean
) : SqlSingleOperandTypeChecker {
    //~ Methods ----------------------------------------------------------------
    @Override
    override fun isOptional(i: Int): Boolean {
        return false
    }

    @Override
    override fun checkSingleOperandType(
        callBinding: SqlCallBinding,
        node: SqlNode?,
        iFormalOperand: Int,
        throwOnFailure: Boolean
    ): Boolean {
        Util.discard(iFormalOperand)
        if (SqlUtil.isNullLiteral(node, true)) {
            if (allowNull) {
                return true
            }
            if (throwOnFailure) {
                throw callBinding.newError(
                    RESOURCE.argumentMustNotBeNull(
                        callBinding.getOperator().getName()
                    )
                )
            }
            return false
        }
        if (!SqlUtil.isLiteral(node) && !SqlUtil.isLiteralChain(node)) {
            if (throwOnFailure) {
                throw callBinding.newError(
                    RESOURCE.argumentMustBeLiteral(
                        callBinding.getOperator().getName()
                    )
                )
            }
            return false
        }
        return true
    }

    @Override
    override fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        return checkSingleOperandType(
            callBinding,
            callBinding.operand(0),
            0,
            throwOnFailure
        )
    }

    @get:Override
    override val operandCountRange: SqlOperandCountRange
        get() = SqlOperandCountRanges.of(1)

    @Override
    override fun getAllowedSignatures(op: SqlOperator?, opName: String?): String {
        return "<LITERAL>"
    }

    @get:Override
    override val consistency: org.apache.calcite.sql.type.SqlOperandTypeChecker.Consistency?
        get() = Consistency.NONE
}
