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
package org.apache.calcite.sql

import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.type.SqlOperandTypeChecker
import org.apache.calcite.sql.type.SqlOperandTypeInference
import org.apache.calcite.sql.type.SqlReturnTypeInference
import org.apache.calcite.sql.type.SqlTypeUtil
import org.apache.calcite.sql.validate.SqlMonotonicity
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql.validate.SqlValidatorScope
import org.apache.calcite.util.Litmus
import org.apache.calcite.util.Util
import java.math.BigDecimal
import java.nio.charset.Charset
import org.apache.calcite.util.Static.RESOURCE
import java.util.Objects.requireNonNull

/**
 * `SqlBinaryOperator` is a binary operator.
 */
class SqlBinaryOperator  //~ Constructors -----------------------------------------------------------
/**
 * Creates a SqlBinaryOperator.
 *
 * @param name                 Name of operator
 * @param kind                 Kind
 * @param prec                 Precedence
 * @param leftAssoc            Left-associativity
 * @param returnTypeInference  Strategy to infer return type
 * @param operandTypeInference Strategy to infer operand types
 * @param operandTypeChecker   Validator for operand types
 */
    (
    name: String?,
    kind: SqlKind?,
    prec: Int,
    leftAssoc: Boolean,
    @Nullable returnTypeInference: SqlReturnTypeInference?,
    @Nullable operandTypeInference: SqlOperandTypeInference?,
    @Nullable operandTypeChecker: SqlOperandTypeChecker?
) : SqlOperator(
    name,
    kind,
    leftPrec(prec, leftAssoc),
    rightPrec(prec, leftAssoc),
    returnTypeInference,
    operandTypeInference,
    operandTypeChecker
) {
    //~ Methods ----------------------------------------------------------------
    @get:Override
    val syntax: SqlSyntax
        get() = SqlSyntax.BINARY

    @Override
    @Nullable
    fun getSignatureTemplate(operandsCount: Int): String {
        Util.discard(operandsCount)

        // op0 opname op1
        return "{1} {0} {2}"
    }

    /**
     * {@inheritDoc}
     *
     *
     * Returns true for most operators but false for the '.' operator;
     * consider
     *
     * <blockquote>
     * <pre>x.y + 5 * 6</pre>
    </blockquote> *
     */
    @Override
    fun needsSpace(): Boolean {
        return !getName().equals(".")
    }

    @Override
    @Nullable
    fun reverse(): SqlOperator? {
        return when (kind) {
            EQUALS, NOT_EQUALS, IS_DISTINCT_FROM, IS_NOT_DISTINCT_FROM, OR, AND, PLUS, TIMES -> this
            GREATER_THAN -> SqlStdOperatorTable.LESS_THAN
            GREATER_THAN_OR_EQUAL -> SqlStdOperatorTable.LESS_THAN_OR_EQUAL
            LESS_THAN -> SqlStdOperatorTable.GREATER_THAN
            LESS_THAN_OR_EQUAL -> SqlStdOperatorTable.GREATER_THAN_OR_EQUAL
            else -> null
        }
    }

    @Override
    protected fun adjustType(
        validator: SqlValidator,
        call: SqlCall,
        type: RelDataType
    ): RelDataType {
        return convertType(validator, call, type)
    }

    private fun convertType(validator: SqlValidator, call: SqlCall, type: RelDataType): RelDataType {
        var type: RelDataType = type
        val operandType0: RelDataType = validator.getValidatedNodeType(call.operand(0))
        val operandType1: RelDataType = validator.getValidatedNodeType(call.operand(1))
        if (SqlTypeUtil.inCharFamily(operandType0)
            && SqlTypeUtil.inCharFamily(operandType1)
        ) {
            val cs0: Charset = operandType0.getCharset()
            val cs1: Charset = operandType1.getCharset()
            assert(null != cs0 && null != cs1) { "An implicit or explicit charset should have been set" }
            if (!cs0.equals(cs1)) {
                throw validator.newValidationError(
                    call,
                    RESOURCE.incompatibleCharset(getName(), cs0.name(), cs1.name())
                )
            }
            val collation0: SqlCollation = operandType0.getCollation()
            val collation1: SqlCollation = operandType1.getCollation()
            assert(null != collation0 && null != collation1) { "An implicit or explicit collation should have been set" }

            // Validation will occur inside getCoercibilityDyadicOperator...
            val resultCol: SqlCollation = SqlCollation.getCoercibilityDyadicOperator(
                collation0,
                collation1
            )
            if (SqlTypeUtil.inCharFamily(type)) {
                type = validator.getTypeFactory()
                    .createTypeWithCharsetAndCollation(
                        type,
                        type.getCharset(),
                        requireNonNull(resultCol, "resultCol")
                    )
            }
        }
        return type
    }

    @Override
    fun deriveType(
        validator: SqlValidator,
        scope: SqlValidatorScope?,
        call: SqlCall
    ): RelDataType {
        val type: RelDataType = super.deriveType(validator, scope, call)
        return convertType(validator, call, type)
    }

    @Override
    fun getMonotonicity(call: SqlOperatorBinding): SqlMonotonicity {
        if (getName().equals("/")) {
            if (call.isOperandNull(0, true)
                || call.isOperandNull(1, true)
            ) {
                // null result => CONSTANT monotonicity
                return SqlMonotonicity.CONSTANT
            }
            val mono0: SqlMonotonicity = call.getOperandMonotonicity(0)
            val mono1: SqlMonotonicity = call.getOperandMonotonicity(1)
            if (mono1 === SqlMonotonicity.CONSTANT) {
                if (call.isOperandLiteral(1, false)) {
                    val value: BigDecimal =
                        call.getOperandLiteralValue(1, BigDecimal::class.java) ?: return SqlMonotonicity.CONSTANT
                    return when (value.signum()) {
                        -1 ->
                            // mono / -ve constant --> reverse mono, unstrict
                            mono0.reverse().unstrict()
                        0 ->
                            // mono / zero --> constant (infinity!)
                            SqlMonotonicity.CONSTANT
                        else ->
                            // mono / +ve constant * mono1 --> mono, unstrict
                            mono0.unstrict()
                    }
                }
            }
        }
        return super.getMonotonicity(call)
    }

    @Override
    fun validRexOperands(count: Int, litmus: Litmus): Boolean {
        return if (count != 2) {
            // Special exception for AND and OR.
            if ((this === SqlStdOperatorTable.AND
                        || this === SqlStdOperatorTable.OR)
                && count > 2
            ) {
                true
            } else litmus.fail("wrong operand count {} for {}", count, this)
        } else litmus.succeed()
    }
}
