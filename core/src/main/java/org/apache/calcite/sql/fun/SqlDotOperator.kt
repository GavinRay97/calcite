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
 * The dot operator `.`, used to access a field of a
 * record. For example, `a.b`.
 */
class SqlDotOperator internal constructor() : SqlSpecialOperator("DOT", SqlKind.DOT, 100, true, null, null, null) {
    @Override
    fun reduceExpr(ordinal: Int, list: TokenSequence): ReduceResult {
        val left: SqlNode = list.node(ordinal - 1)
        val right: SqlNode = list.node(ordinal + 1)
        return ReduceResult(
            ordinal - 1,
            ordinal + 2,
            createCall(
                SqlParserPos.sum(
                    Arrays.asList(
                        requireNonNull(left, "left").getParserPosition(),
                        requireNonNull(right, "right").getParserPosition(),
                        list.pos(ordinal)
                    )
                ),
                left,
                right
            )
        )
    }

    @Override
    fun unparse(
        writer: SqlWriter, call: SqlCall, leftPrec: Int,
        rightPrec: Int
    ) {
        val frame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.IDENTIFIER)
        call.operand(0).unparse(writer, leftPrec, 0)
        writer.sep(".")
        call.operand(1).unparse(writer, 0, 0)
        writer.endList(frame)
    }

    @get:Override
    val operandCountRange: SqlOperandCountRange
        get() = SqlOperandCountRanges.of(2)

    @Override
    fun <R> acceptCall(
        visitor: SqlVisitor<R>?, call: SqlCall,
        onlyExpressions: Boolean, argHandler: SqlBasicVisitor.ArgHandler<R>
    ) {
        if (onlyExpressions) {
            // Do not visit operands[1] -- it is not an expression.
            argHandler.visitChild(visitor, call, 0, call.operand(0))
        } else {
            super.acceptCall(visitor, call, onlyExpressions, argHandler)
        }
    }

    @Override
    fun deriveType(
        validator: SqlValidator,
        scope: SqlValidatorScope?, call: SqlCall
    ): RelDataType {
        val operand: SqlNode = call.getOperandList().get(0)
        val nodeType: RelDataType = validator.deriveType(scope, operand)
        assert(nodeType != null)
        if (!nodeType.isStruct()) {
            throw SqlUtil.newContextException(
                operand.getParserPosition(),
                Static.RESOURCE.incompatibleTypes()
            )
        }
        val fieldId: SqlNode = call.operand(1)
        val fieldName: String = fieldId.toString()
        val field: RelDataTypeField = nodeType.getField(fieldName, false, false)
            ?: throw SqlUtil.newContextException(
                fieldId.getParserPosition(),
                Static.RESOURCE.unknownField(fieldName)
            )
        var type: RelDataType = field.getType()
        if (nodeType.isNullable()) {
            type = validator.getTypeFactory().createTypeWithNullability(type, true)
        }

        // Validate and determine coercibility and resulting collation
        // name of binary operator if needed.
        type = adjustType(validator, call, type)
        SqlValidatorUtil.checkCharsetAndCollateConsistentIfCharType(type)
        return type
    }

    @Override
    fun validateCall(
        call: SqlCall,
        validator: SqlValidator?,
        scope: SqlValidatorScope?,
        operandScope: SqlValidatorScope?
    ) {
        assert(call.getOperator() === this)
        // Do not visit call.getOperandList().get(1) here.
        // call.getOperandList().get(1) will be validated when deriveType() is called.
        call.getOperandList().get(0).validateExpr(validator, operandScope)
    }

    @Override
    fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        val left: SqlNode = callBinding.operand(0)
        val right: SqlNode = callBinding.operand(1)
        val type: RelDataType = SqlTypeUtil.deriveType(callBinding, left)
        if (type.getSqlTypeName() !== SqlTypeName.ROW) {
            return false
        } else if (requireNonNull(
                type.getSqlIdentifier()
            ) { "type.getSqlIdentifier() is null for $type" }.isStar()
        ) {
            return false
        }
        val operandType: RelDataType = callBinding.getOperandType(0)
        val checker: SqlSingleOperandTypeChecker = getChecker(operandType)
        // Actually operand0 always comes from parsing the SqlIdentifier, so there
        // is no need to make implicit type coercion.
        return checker.checkSingleOperandType(
            callBinding, right, 0,
            throwOnFailure
        )
    }

    @Override
    fun validRexOperands(count: Int, litmus: Litmus): Boolean {
        return litmus.fail("DOT is valid only for SqlCall not for RexCall")
    }

    @Override
    fun getAllowedSignatures(name: String?): String {
        return "<A>.<B>"
    }

    @Override
    fun inferReturnType(opBinding: SqlOperatorBinding): RelDataType {
        val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
        val recordType: RelDataType = opBinding.getOperandType(0)
        return when (recordType.getSqlTypeName()) {
            ROW -> {
                val fieldName: String = getOperandLiteralValueOrThrow(opBinding, 1, String::class.java)
                val type: RelDataType = requireNonNull(
                    recordType.getField(fieldName, false, false)
                ) { "field $fieldName is not found in $recordType" }
                    .getType()
                if (recordType.isNullable()) {
                    typeFactory.createTypeWithNullability(type, true)
                } else {
                    type
                }
            }
            else -> throw AssertionError()
        }
    }

    companion object {
        private fun getChecker(operandType: RelDataType): SqlSingleOperandTypeChecker {
            return when (operandType.getSqlTypeName()) {
                ROW -> OperandTypes.family(SqlTypeFamily.STRING)
                else -> throw AssertionError(operandType.getSqlTypeName())
            }
        }
    }
}
