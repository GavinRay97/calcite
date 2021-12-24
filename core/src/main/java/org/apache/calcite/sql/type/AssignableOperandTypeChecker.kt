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

import org.apache.calcite.linq4j.Ord

/**
 * AssignableOperandTypeChecker implements [SqlOperandTypeChecker] by
 * verifying that the type of each argument is assignable to a predefined set of
 * parameter types (under the SQL definition of "assignable").
 */
class AssignableOperandTypeChecker(
    paramTypes: List<RelDataType?>?,
    @Nullable paramNames: List<String?>?
) : SqlOperandTypeChecker {
    //~ Instance fields --------------------------------------------------------
    private val paramTypes: List<RelDataType>

    @Nullable
    private val paramNames: ImmutableList<String>?
    //~ Constructors -----------------------------------------------------------
    /**
     * Instantiates this strategy with a specific set of parameter types.
     *
     * @param paramTypes parameter types for operands; index in this array
     * corresponds to operand number
     * @param paramNames parameter names, or null
     */
    init {
        this.paramTypes = ImmutableList.copyOf(paramTypes)
        this.paramNames = if (paramNames == null) null else ImmutableList.copyOf(paramNames)
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun isOptional(i: Int): Boolean {
        return false
    }

    @get:Override
    override val operandCountRange: SqlOperandCountRange
        get() = SqlOperandCountRanges.of(paramTypes.size())

    @Override
    override fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        // Do not use callBinding.operands(). We have not resolved to a function
        // yet, therefore we do not know the ordered parameter names.
        val operands: List<SqlNode> = callBinding.getCall().getOperandList()
        for (pair in Pair.zip(paramTypes, operands)) {
            val argType: RelDataType = SqlTypeUtil.deriveType(callBinding, pair.right)
            if (!SqlTypeUtil.canAssignFrom(pair.left, argType)) {
                // TODO: add in unresolved function type cast.
                return if (throwOnFailure) {
                    throw callBinding.newValidationSignatureError()
                } else {
                    false
                }
            }
        }
        return true
    }

    @Override
    override fun getAllowedSignatures(op: SqlOperator?, opName: String?): String {
        val sb = StringBuilder()
        sb.append(opName)
        sb.append("(")
        for (paramType in Ord.zip(paramTypes)) {
            if (paramType.i > 0) {
                sb.append(", ")
            }
            if (paramNames != null) {
                sb.append(paramNames.get(paramType.i))
                    .append(" => ")
            }
            sb.append("<")
            sb.append(paramType.e.getFamily())
            sb.append(">")
        }
        sb.append(")")
        return sb.toString()
    }

    @get:Override
    override val consistency: org.apache.calcite.sql.type.SqlOperandTypeChecker.Consistency?
        get() = Consistency.NONE
}
