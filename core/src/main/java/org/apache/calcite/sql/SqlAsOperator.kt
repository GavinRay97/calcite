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
import org.apache.calcite.sql.type.InferTypes
import org.apache.calcite.sql.type.OperandTypes
import org.apache.calcite.sql.type.ReturnTypes
import org.apache.calcite.sql.type.SqlOperandTypeChecker
import org.apache.calcite.sql.type.SqlOperandTypeInference
import org.apache.calcite.sql.type.SqlReturnTypeInference
import org.apache.calcite.sql.util.SqlBasicVisitor
import org.apache.calcite.sql.util.SqlVisitor
import org.apache.calcite.sql.validate.SqlMonotonicity
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql.validate.SqlValidatorScope
import org.apache.calcite.util.Util
import java.util.List
import org.apache.calcite.util.Static.RESOURCE

/**
 * The `AS` operator associates an expression with an alias.
 */
class SqlAsOperator protected constructor(
    name: String?, kind: SqlKind?, prec: Int,
    leftAssoc: Boolean, returnTypeInference: SqlReturnTypeInference?,
    operandTypeInference: SqlOperandTypeInference?,
    operandTypeChecker: SqlOperandTypeChecker?
) : SqlSpecialOperator(
    name, kind, prec, leftAssoc, returnTypeInference,
    operandTypeInference, operandTypeChecker
) {
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates an AS operator.
     */
    constructor() : this(
        "AS",
        SqlKind.AS,
        20,
        true,
        ReturnTypes.ARG0,
        InferTypes.RETURN_TYPE,
        OperandTypes.ANY_IGNORE
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun unparse(
        writer: SqlWriter,
        call: SqlCall,
        leftPrec: Int,
        rightPrec: Int
    ) {
        assert(call.operandCount() >= 2)
        val frame: SqlWriter.Frame = writer.startList(
            SqlWriter.FrameTypeEnum.AS
        )
        call.operand(0).unparse(writer, leftPrec, getLeftPrec())
        val needsSpace = true
        writer.setNeedWhitespace(needsSpace)
        if (writer.getDialect().allowsAs()) {
            writer.sep("AS")
            writer.setNeedWhitespace(needsSpace)
        }
        call.operand(1).unparse(writer, getRightPrec(), rightPrec)
        if (call.operandCount() > 2) {
            val frame1: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.SIMPLE, "(", ")")
            for (operand in Util.skip(call.getOperandList(), 2)) {
                writer.sep(",", false)
                operand.unparse(writer, 0, 0)
            }
            writer.endList(frame1)
        }
        writer.endList(frame)
    }

    @Override
    fun validateCall(
        call: SqlCall,
        validator: SqlValidator,
        scope: SqlValidatorScope?,
        operandScope: SqlValidatorScope?
    ) {
        // The base method validates all operands. We override because
        // we don't want to validate the identifier.
        val operands: List<SqlNode> = call.getOperandList()
        assert(operands.size() === 2)
        assert(operands[1] is SqlIdentifier)
        operands[0].validateExpr(validator, scope)
        val id: SqlIdentifier = operands[1] as SqlIdentifier
        if (!id.isSimple()) {
            throw validator.newValidationError(
                id,
                RESOURCE.aliasMustBeSimpleIdentifier()
            )
        }
    }

    @Override
    fun <R> acceptCall(
        visitor: SqlVisitor<R>?,
        call: SqlCall,
        onlyExpressions: Boolean,
        argHandler: SqlBasicVisitor.ArgHandler<R>
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
        scope: SqlValidatorScope?,
        call: SqlCall
    ): RelDataType {
        // special case for AS:  never try to derive type for alias
        val nodeType: RelDataType = validator.deriveType(scope, call.operand(0))
        assert(nodeType != null)
        return validateOperands(validator, scope, call)
    }

    @Override
    fun getMonotonicity(call: SqlOperatorBinding): SqlMonotonicity {
        return call.getOperandMonotonicity(0)
    }
}
