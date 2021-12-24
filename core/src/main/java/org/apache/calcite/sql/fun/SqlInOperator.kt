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
 * Definition of the SQL `IN` operator, which tests for a value's
 * membership in a sub-query or a list of values.
 */
class SqlInOperator protected constructor(name: String?, kind: SqlKind?) : SqlBinaryOperator(
    name, kind,
    32,
    true,
    ReturnTypes.BOOLEAN_NULLABLE,
    InferTypes.FIRST_KNOWN,
    null
) {
    //~ Instance fields --------------------------------------------------------
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a SqlInOperator.
     *
     * @param kind IN or NOT IN
     */
    internal constructor(kind: SqlKind) : this(kind.sql, kind) {
        assert(kind === SqlKind.IN || kind === SqlKind.NOT_IN || kind === SqlKind.DRUID_IN || kind === SqlKind.DRUID_NOT_IN)
    }

    //~ Methods ----------------------------------------------------------------
    // to be removed before 2.0
    @get:Deprecated
    val isNotIn: Boolean
        get() = kind === SqlKind.NOT_IN

    @Override
    operator fun not(): SqlOperator {
        return of(kind.negateNullSafe())
    }

    @Override
    fun validRexOperands(count: Int, litmus: Litmus): Boolean {
        return if (count == 0) {
            litmus.fail("wrong operand count {} for {}", count, this)
        } else litmus.succeed()
    }

    @Override
    fun deriveType(
        validator: SqlValidator,
        scope: SqlValidatorScope?,
        call: SqlCall
    ): RelDataType {
        val operands: List<SqlNode> = call.getOperandList()
        assert(operands.size() === 2)
        val left: SqlNode = operands[0]
        val right: SqlNode = operands[1]
        val typeFactory: RelDataTypeFactory = validator.getTypeFactory()
        var leftType: RelDataType = validator.deriveType(scope, left)
        var rightType: RelDataType

        // Derive type for RHS.
        if (right is SqlNodeList) {
            // Handle the 'IN (expr, ...)' form.
            val rightTypeList: List<RelDataType> = ArrayList()
            val nodeList: SqlNodeList = right as SqlNodeList
            for (i in 0 until nodeList.size()) {
                val node: SqlNode = nodeList.get(i)
                val nodeType: RelDataType = validator.deriveType(scope, node)
                rightTypeList.add(nodeType)
            }
            rightType = typeFactory.leastRestrictive(rightTypeList)

            // First check that the expressions in the IN list are compatible
            // with each other. Same rules as the VALUES operator (per
            // SQL:2003 Part 2 Section 8.4, <in predicate>).
            if (null == rightType && validator.config().typeCoercionEnabled()) {
                // Do implicit type cast if it is allowed to.
                rightType = validator.getTypeCoercion().getWiderTypeFor(rightTypeList, true)
            }
            if (null == rightType) {
                throw validator.newValidationError(
                    right,
                    RESOURCE.incompatibleTypesInList()
                )
            }

            // Record the RHS type for use by SqlToRelConverter.
            (validator as SqlValidatorImpl).setValidatedNodeType(nodeList, rightType)
        } else {
            // Handle the 'IN (query)' form.
            rightType = validator.deriveType(scope, right)
        }
        val callBinding = SqlCallBinding(validator, scope, call)
        // Coerce type first.
        if (callBinding.isTypeCoercionEnabled()) {
            val coerced: Boolean = callBinding.getValidator().getTypeCoercion()
                .inOperationCoercion(callBinding)
            if (coerced) {
                // Update the node data type if we coerced any type.
                leftType = validator.deriveType(scope, call.operand(0))
                rightType = validator.deriveType(scope, call.operand(1))
            }
        }

        // Now check that the left expression is compatible with the
        // type of the list. Same strategy as the '=' operator.
        // Normalize the types on both sides to be row types
        // for the purposes of compatibility-checking.
        val leftRowType: RelDataType = SqlTypeUtil.promoteToRowType(
            typeFactory,
            leftType,
            null
        )
        val rightRowType: RelDataType = SqlTypeUtil.promoteToRowType(
            typeFactory,
            rightType,
            null
        )
        val checker: ComparableOperandTypeChecker =
            OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED as ComparableOperandTypeChecker
        if (!checker.checkOperandTypes(
                ExplicitOperatorBinding(
                    callBinding,
                    ImmutableList.of(leftRowType, rightRowType)
                ), callBinding
            )
        ) {
            throw validator.newValidationError(
                call,
                RESOURCE.incompatibleValueType(SqlStdOperatorTable.IN.getName())
            )
        }

        // Result is a boolean, nullable if there are any nullable types
        // on either side.
        return typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.BOOLEAN), anyNullable(leftRowType.getFieldList())
                    || anyNullable(rightRowType.getFieldList())
        )
    }

    @Override
    fun argumentMustBeScalar(ordinal: Int): Boolean {
        // Argument #0 must be scalar, argument #1 can be a list (1, 2) or
        // a query (select deptno from emp). So, only coerce argument #0 into
        // a scalar sub-query. For example, in
        //  select * from emp
        //  where (select count(*) from dept) in (select deptno from dept)
        // we should coerce the LHS to a scalar.
        return ordinal == 0
    }

    companion object {
        private fun of(kind: SqlKind): SqlBinaryOperator {
            return when (kind) {
                IN -> SqlStdOperatorTable.IN
                NOT_IN -> SqlStdOperatorTable.NOT_IN
                DRUID_IN -> SqlInternalOperators.DRUID_IN
                DRUID_NOT_IN -> SqlInternalOperators.DRUID_NOT_IN
                else -> throw AssertionError("unexpected $kind")
            }
        }

        private fun anyNullable(fieldList: List<RelDataTypeField>): Boolean {
            for (field in fieldList) {
                if (field.getType().isNullable()) {
                    return true
                }
            }
            return false
        }
    }
}
