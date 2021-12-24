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

/**
 * A unary operator.
 */
class SqlPrefixOperator  //~ Constructors -----------------------------------------------------------
    (
    name: String,
    kind: SqlKind?,
    prec: Int,
    @Nullable returnTypeInference: SqlReturnTypeInference?,
    @Nullable operandTypeInference: SqlOperandTypeInference?,
    @Nullable operandTypeChecker: SqlOperandTypeChecker?
) : SqlOperator(
    name,
    kind,
    leftPrec(prec, true),
    rightPrec(prec, true),
    returnTypeInference,
    operandTypeInference,
    operandTypeChecker
) {
    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val syntax: org.apache.calcite.sql.SqlSyntax
        get() = SqlSyntax.PREFIX

    @Override
    @Nullable
    override fun getSignatureTemplate(operandsCount: Int): String {
        Util.discard(operandsCount)
        return "{0}{1}"
    }

    @Override
    protected override fun adjustType(
        validator: SqlValidator,
        call: SqlCall,
        type: RelDataType?
    ): RelDataType? {
        var type: RelDataType? = type
        if (SqlTypeUtil.inCharFamily(type)) {
            // Determine coercibility and resulting collation name of
            // unary operator if needed.
            val operandType: RelDataType = validator.getValidatedNodeType(call.operand(0))
                ?: throw AssertionError("operand's type should have been derived")
            if (SqlTypeUtil.inCharFamily(operandType)) {
                val collation: SqlCollation = getCollation(operandType)
                type = validator.getTypeFactory()
                    .createTypeWithCharsetAndCollation(
                        type,
                        getCharset(type),
                        collation
                    )
            }
        }
        return type
    }

    @Override
    fun getMonotonicity(call: SqlOperatorBinding): SqlMonotonicity {
        return if (getName().equals("-")) {
            call.getOperandMonotonicity(0).reverse()
        } else super.getMonotonicity(call)
    }

    @Override
    override fun validRexOperands(count: Int, litmus: Litmus): Boolean {
        return if (count != 1) {
            litmus.fail("wrong operand count {} for {}", count, this)
        } else litmus.succeed()
    }
}
