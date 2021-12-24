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
 * SqlCastFunction. Note that the std functions are really singleton objects,
 * because they always get fetched via the StdOperatorTable. So you can't store
 * any local info in the class and hence the return type data is maintained in
 * operand[1] through the validation phase.
 *
 *
 * Can be used for both [SqlCall] and
 * [org.apache.calcite.rex.RexCall].
 * Note that the `SqlCall` has two operands (expression and type),
 * while the `RexCall` has one operand (expression) and the type is
 * obtained from [org.apache.calcite.rex.RexNode.getType].
 *
 * @see SqlCastOperator
 */
class SqlCastFunction  //~ Constructors -----------------------------------------------------------
    : SqlFunction(
    "CAST",
    SqlKind.CAST,
    null,
    InferTypes.FIRST_KNOWN,
    null,
    SqlFunctionCategory.SYSTEM
) {
    //~ Instance fields --------------------------------------------------------
    /** Map of all casts that do not preserve monotonicity.  */
    private val nonMonotonicCasts: SetMultimap<SqlTypeFamily, SqlTypeFamily> =
        ImmutableSetMultimap.< SqlTypeFamily, SqlTypeFamily>builder<SqlTypeFamily?, SqlTypeFamily?>()
    .put(SqlTypeFamily.EXACT_NUMERIC, SqlTypeFamily.CHARACTER)
    .put(SqlTypeFamily.NUMERIC, SqlTypeFamily.CHARACTER)
    .put(SqlTypeFamily.APPROXIMATE_NUMERIC, SqlTypeFamily.CHARACTER)
    .put(SqlTypeFamily.DATETIME_INTERVAL, SqlTypeFamily.CHARACTER)
    .put(SqlTypeFamily.CHARACTER, SqlTypeFamily.EXACT_NUMERIC)
    .put(SqlTypeFamily.CHARACTER, SqlTypeFamily.NUMERIC)
    .put(SqlTypeFamily.CHARACTER, SqlTypeFamily.APPROXIMATE_NUMERIC)
    .put(SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME_INTERVAL)
    .put(SqlTypeFamily.DATETIME, SqlTypeFamily.TIME)
    .put(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.TIME)
    .put(SqlTypeFamily.TIME, SqlTypeFamily.DATETIME)
    .put(SqlTypeFamily.TIME, SqlTypeFamily.TIMESTAMP)
    .build()
    //~ Methods ----------------------------------------------------------------
    @Override
    fun inferReturnType(
        opBinding: SqlOperatorBinding
    ): RelDataType {
        assert(opBinding.getOperandCount() === 2)
        var ret: RelDataType = opBinding.getOperandType(1)
        val firstType: RelDataType = opBinding.getOperandType(0)
        ret = opBinding.getTypeFactory().createTypeWithNullability(
            ret,
            firstType.isNullable()
        )
        if (opBinding is SqlCallBinding) {
            val callBinding: SqlCallBinding = opBinding as SqlCallBinding
            val operand0: SqlNode = callBinding.operand(0)

            // dynamic parameters and null constants need their types assigned
            // to them using the type they are casted to.
            if (SqlUtil.isNullLiteral(operand0, false)
                || operand0 is SqlDynamicParam
            ) {
                val validator: SqlValidatorImpl = callBinding.getValidator() as SqlValidatorImpl
                validator.setValidatedNodeType(operand0, ret)
            }
        }
        return ret
    }

    @Override
    fun getSignatureTemplate(operandsCount: Int): String {
        assert(operandsCount == 2)
        return "{0}({1} AS {2})"
    }

    @get:Override
    val operandCountRange: SqlOperandCountRange
        get() = SqlOperandCountRanges.of(2)

    /**
     * Makes sure that the number and types of arguments are allowable.
     * Operators (such as "ROW" and "AS") which do not check their arguments can
     * override this method.
     */
    @Override
    fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        val left: SqlNode = callBinding.operand(0)
        val right: SqlNode = callBinding.operand(1)
        if (SqlUtil.isNullLiteral(left, false)
            || left is SqlDynamicParam
        ) {
            return true
        }
        val validatedNodeType: RelDataType = callBinding.getValidator().getValidatedNodeType(left)
        val returnType: RelDataType = SqlTypeUtil.deriveType(callBinding, right)
        if (!SqlTypeUtil.canCastFrom(returnType, validatedNodeType, true)) {
            if (throwOnFailure) {
                throw callBinding.newError(
                    RESOURCE.cannotCastValue(
                        validatedNodeType.toString(),
                        returnType.toString()
                    )
                )
            }
            return false
        }
        if (SqlTypeUtil.areCharacterSetsMismatched(
                validatedNodeType,
                returnType
            )
        ) {
            if (throwOnFailure) {
                // Include full type string to indicate character
                // set mismatch.
                throw callBinding.newError(
                    RESOURCE.cannotCastValue(
                        validatedNodeType.getFullTypeString(),
                        returnType.getFullTypeString()
                    )
                )
            }
            return false
        }
        return true
    }

    @get:Override
    val syntax: SqlSyntax
        get() = SqlSyntax.SPECIAL

    @Override
    fun unparse(
        writer: SqlWriter,
        call: SqlCall,
        leftPrec: Int,
        rightPrec: Int
    ) {
        assert(call.operandCount() === 2)
        val frame: SqlWriter.Frame = writer.startFunCall(getName())
        call.operand(0).unparse(writer, 0, 0)
        writer.sep("AS")
        if (call.operand(1) is SqlIntervalQualifier) {
            writer.sep("INTERVAL")
        }
        call.operand(1).unparse(writer, 0, 0)
        writer.endFunCall(frame)
    }

    @Override
    fun getMonotonicity(call: SqlOperatorBinding): SqlMonotonicity {
        val castFromType: RelDataType = call.getOperandType(0)
        val castFromFamily: RelDataTypeFamily = castFromType.getFamily()
        val castFromCollator: Collator? =
            if (castFromType.getCollation() == null) null else castFromType.getCollation().getCollator()
        val castToType: RelDataType = call.getOperandType(1)
        val castToFamily: RelDataTypeFamily = castToType.getFamily()
        val castToCollator: Collator? =
            if (castToType.getCollation() == null) null else castToType.getCollation().getCollator()
        return if (!Objects.equals(castFromCollator, castToCollator)) {
            // Cast between types compared with different collators: not monotonic.
            SqlMonotonicity.NOT_MONOTONIC
        } else if (castFromFamily is SqlTypeFamily
            && castToFamily is SqlTypeFamily
            && nonMonotonicCasts.containsEntry(castFromFamily, castToFamily)
        ) {
            SqlMonotonicity.NOT_MONOTONIC
        } else {
            call.getOperandMonotonicity(0)
        }
    }
}
