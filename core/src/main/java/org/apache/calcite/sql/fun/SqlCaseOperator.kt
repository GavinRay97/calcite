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
 * An operator describing a `CASE`, `NULLIF` or `
 * COALESCE` expression. All of these forms are normalized at parse time
 * to a to a simple `CASE` statement like this:
 *
 * <blockquote><pre>`CASE
 * WHEN <when expression_0> THEN <then expression_0>
 * WHEN <when expression_1> THEN <then expression_1>
 * ...
 * WHEN <when expression_N> THEN <then expression_N>
 * ELSE <else expression>
 * END`</pre></blockquote>
 *
 *
 * The switched form of the `CASE` statement is normalized to the
 * simple form by inserting calls to the `=` operator. For
 * example,
 *
 * <blockquote><pre>`CASE x + y
 * WHEN 1 THEN 'fee'
 * WHEN 2 THEN 'fie'
 * ELSE 'foe'
 * END`</pre></blockquote>
 *
 *
 * becomes
 *
 * <blockquote><pre>`CASE
 * WHEN Equals(x + y, 1) THEN 'fee'
 * WHEN Equals(x + y, 2) THEN 'fie'
 * ELSE 'foe'
 * END`</pre></blockquote>
 *
 *
 * REVIEW jhyde 2004/3/19 Does `Equals` handle NULL semantics
 * correctly?
 *
 *
 * `COALESCE(x, y, z)` becomes
 *
 * <blockquote><pre>`CASE
 * WHEN x IS NOT NULL THEN x
 * WHEN y IS NOT NULL THEN y
 * ELSE z
 * END`</pre></blockquote>
 *
 *
 * `NULLIF(x, -1)` becomes
 *
 * <blockquote><pre>`CASE
 * WHEN x = -1 THEN NULL
 * ELSE x
 * END`</pre></blockquote>
 *
 *
 * Note that some of these normalizations cause expressions to be duplicated.
 * This may make it more difficult to write optimizer rules (because the rules
 * will have to deduce that expressions are equivalent). It also requires that
 * some part of the planning process (probably the generator of the calculator
 * program) does common sub-expression elimination.
 *
 *
 * REVIEW jhyde 2004/3/19. Expanding expressions at parse time has some other
 * drawbacks. It is more difficult to give meaningful validation errors: given
 * `COALESCE(DATE '2004-03-18', 3.5)`, do we issue a type-checking
 * error against a `CASE` operator? Second, I'd like to use the
 * [SqlNode] object model to generate SQL to send to 3rd-party databases,
 * but there's now no way to represent a call to COALESCE or NULLIF. All in all,
 * it would be better to have operators for COALESCE, NULLIF, and both simple
 * and switched forms of CASE, then translate to simple CASE when building the
 * [org.apache.calcite.rex.RexNode] tree.
 *
 *
 * The arguments are physically represented as follows:
 *
 *
 *  * The *when* expressions are stored in a [SqlNodeList]
 * whenList.
 *  * The *then* expressions are stored in a [SqlNodeList]
 * thenList.
 *  * The *else* expression is stored as a regular [SqlNode].
 *
 */
class SqlCaseOperator  //~ Constructors -----------------------------------------------------------
private constructor() : SqlOperator(
    "CASE", SqlKind.CASE, MDX_PRECEDENCE, true, null,
    InferTypes.RETURN_TYPE, null
) {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun validateCall(
        call: SqlCall,
        validator: SqlValidator?,
        scope: SqlValidatorScope?,
        operandScope: SqlValidatorScope?
    ) {
        val sqlCase: SqlCase = call
        val whenOperands: SqlNodeList = sqlCase.getWhenOperands()
        val thenOperands: SqlNodeList = sqlCase.getThenOperands()
        val elseOperand: SqlNode = sqlCase.getElseOperand()
        for (operand in whenOperands) {
            operand.validateExpr(validator, operandScope)
        }
        for (operand in thenOperands) {
            operand.validateExpr(validator, operandScope)
        }
        if (elseOperand != null) {
            elseOperand.validateExpr(validator, operandScope)
        }
    }

    @Override
    fun deriveType(
        validator: SqlValidator?,
        scope: SqlValidatorScope?,
        call: SqlCall?
    ): RelDataType {
        // Do not try to derive the types of the operands. We will do that
        // later, top down.
        return validateOperands(validator, scope, call)
    }

    @Override
    fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        val caseCall: SqlCase = callBinding.getCall()
        val whenList: SqlNodeList = caseCall.getWhenOperands()
        val thenList: SqlNodeList = caseCall.getThenOperands()
        assert(whenList.size() === thenList.size())

        // checking that search conditions are ok...
        for (node in whenList) {
            // should throw validation error if something wrong...
            val type: RelDataType = SqlTypeUtil.deriveType(callBinding, node)
            if (!SqlTypeUtil.inBooleanFamily(type)) {
                if (throwOnFailure) {
                    throw callBinding.newError(RESOURCE.expectedBoolean())
                }
                return false
            }
        }
        var foundNotNull = false
        for (node in thenList) {
            if (!SqlUtil.isNullLiteral(node, false)) {
                foundNotNull = true
            }
        }
        if (!SqlUtil.isNullLiteral(
                caseCall.getElseOperand(),
                false
            )
        ) {
            foundNotNull = true
        }
        if (!foundNotNull) {
            // according to the sql standard we can not have all of the THEN
            // statements and the ELSE returning null
            if (throwOnFailure && !callBinding.isTypeCoercionEnabled()) {
                throw callBinding.newError(RESOURCE.mustNotNullInElse())
            }
            return false
        }
        return true
    }

    @Override
    fun inferReturnType(
        opBinding: SqlOperatorBinding
    ): RelDataType? {
        // REVIEW jvs 4-June-2005:  can't these be unified?
        return if (opBinding !is SqlCallBinding) {
            inferTypeFromOperands(opBinding)
        } else inferTypeFromValidator(
            opBinding as SqlCallBinding
        )
    }

    @get:Override
    val operandCountRange: SqlOperandCountRange
        get() = SqlOperandCountRanges.any()

    @get:Override
    val syntax: SqlSyntax
        get() = SqlSyntax.SPECIAL

    @SuppressWarnings("argument.type.incompatible")
    @Override
    fun createCall(
        @Nullable functionQualifier: SqlLiteral?,
        pos: SqlParserPos?,
        @Nullable vararg operands: SqlNode
    ): SqlCall {
        assert(functionQualifier == null)
        assert(operands.size == 4)
        return SqlCase(
            pos, operands[0], operands[1] as SqlNodeList,
            operands[2] as SqlNodeList, operands[3]
        )
    }

    @Override
    fun unparse(
        writer: SqlWriter, call_: SqlCall, leftPrec: Int,
        rightPrec: Int
    ) {
        val kase: SqlCase = call_
        val frame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.CASE, "CASE", "END")
        assert(kase.whenList.size() === kase.thenList.size())
        if (kase.value != null) {
            kase.value.unparse(writer, 0, 0)
        }
        for (pair in Pair.zip(kase.whenList, kase.thenList)) {
            writer.sep("WHEN")
            pair.left.unparse(writer, 0, 0)
            writer.sep("THEN")
            pair.right.unparse(writer, 0, 0)
        }
        val elseExpr: SqlNode = kase.elseExpr
        if (elseExpr != null) {
            writer.sep("ELSE")
            elseExpr.unparse(writer, 0, 0)
        }
        writer.endList(frame)
    }

    companion object {
        val INSTANCE = SqlCaseOperator()
        private fun inferTypeFromValidator(
            callBinding: SqlCallBinding
        ): RelDataType? {
            val caseCall: SqlCase = callBinding.getCall()
            val thenList: SqlNodeList = caseCall.getThenOperands()
            val nullList: ArrayList<SqlNode> = ArrayList()
            val argTypes: List<RelDataType> = ArrayList()
            val whenOperands: SqlNodeList = caseCall.getWhenOperands()
            val typeFactory: RelDataTypeFactory = callBinding.getTypeFactory()
            for (i in 0 until thenList.size()) {
                val node: SqlNode = thenList.get(i)
                var type: RelDataType = SqlTypeUtil.deriveType(callBinding, node)
                val operand: SqlNode = whenOperands.get(i)
                if (operand.getKind() === SqlKind.IS_NOT_NULL && type.isNullable()) {
                    val call: SqlBasicCall = operand as SqlBasicCall
                    if (call.getOperandList().get(0).equalsDeep(node, Litmus.IGNORE)) {
                        // We're sure that the type is not nullable if the kind is IS NOT NULL.
                        type = typeFactory.createTypeWithNullability(type, false)
                    }
                }
                argTypes.add(type)
                if (SqlUtil.isNullLiteral(node, false)) {
                    nullList.add(node)
                }
            }
            val elseOp: SqlNode = requireNonNull(
                caseCall.getElseOperand()
            ) { "elseOperand for $caseCall" }
            argTypes.add(
                SqlTypeUtil.deriveType(callBinding, elseOp)
            )
            if (SqlUtil.isNullLiteral(elseOp, false)) {
                nullList.add(elseOp)
            }
            var ret: RelDataType = typeFactory.leastRestrictive(argTypes)
            if (null == ret) {
                var coerced = false
                if (callBinding.isTypeCoercionEnabled()) {
                    val typeCoercion: TypeCoercion = callBinding.getValidator().getTypeCoercion()
                    val commonType: RelDataType = typeCoercion.getWiderTypeFor(argTypes, true)
                    // commonType is always with nullability as false, we do not consider the
                    // nullability when deducing the common type. Use the deduced type
                    // (with the correct nullability) in SqlValidator
                    // instead of the commonType as the return type.
                    if (null != commonType) {
                        coerced = typeCoercion.caseWhenCoercion(callBinding)
                        if (coerced) {
                            ret = SqlTypeUtil.deriveType(callBinding)
                        }
                    }
                }
                if (!coerced) {
                    throw callBinding.newValidationError(RESOURCE.illegalMixingOfTypes())
                }
            }
            val validator: SqlValidatorImpl = callBinding.getValidator() as SqlValidatorImpl
            requireNonNull(ret) { "return type for $callBinding" }
            for (node in nullList) {
                validator.setValidatedNodeType(node, ret)
            }
            return ret
        }

        private fun inferTypeFromOperands(opBinding: SqlOperatorBinding): RelDataType {
            val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
            val argTypes: List<RelDataType> = opBinding.collectOperandTypes()
            assert(argTypes.size() % 2 === 1) {
                ("odd number of arguments expected: "
                        + argTypes.size())
            }
            assert(argTypes.size() > 1) {
                ("CASE must have more than 1 argument. Given "
                        + argTypes.size()) + ", " + argTypes
            }
            val thenTypes: List<RelDataType> = ArrayList()
            var j = 1
            while (j < argTypes.size() - 1) {
                var argType: RelDataType = argTypes[j]
                if (opBinding is RexCallBinding) {
                    val rexCallBinding: RexCallBinding = opBinding as RexCallBinding
                    val whenNode: RexNode = rexCallBinding.operands().get(j - 1)
                    val thenNode: RexNode = rexCallBinding.operands().get(j)
                    if (whenNode.getKind() === SqlKind.IS_NOT_NULL && argType.isNullable()) {
                        // Type is not nullable if the kind is IS NOT NULL.
                        val isNotNullCall: RexCall = whenNode as RexCall
                        if (isNotNullCall.getOperands().get(0).equals(thenNode)) {
                            argType = typeFactory.createTypeWithNullability(argType, false)
                        }
                    }
                }
                thenTypes.add(argType)
                j += 2
            }
            thenTypes.add(Iterables.getLast(argTypes))
            return requireNonNull(
                typeFactory.leastRestrictive(thenTypes)
            ) { "Can't find leastRestrictive type for $thenTypes" }
        }
    }
}
