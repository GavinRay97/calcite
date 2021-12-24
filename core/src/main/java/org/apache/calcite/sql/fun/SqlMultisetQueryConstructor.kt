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
 * Definition of the SQL:2003 standard MULTISET query constructor, `
 * MULTISET (<query>)`.
 *
 * @see SqlMultisetValueConstructor
 */
class SqlMultisetQueryConstructor protected constructor(
    name: String?, kind: SqlKind?,
    typeTransform: SqlTypeTransform
) : SqlSpecialOperator(
    name, kind, MDX_PRECEDENCE, false,
    ReturnTypes.ARG0.andThen(typeTransform), null, OperandTypes.VARIADIC
) {
    val typeTransform: SqlTypeTransform

    //~ Constructors -----------------------------------------------------------
    constructor() : this(
        "MULTISET", SqlKind.MULTISET_QUERY_CONSTRUCTOR,
        SqlTypeTransforms.TO_MULTISET
    ) {
    }

    init {
        this.typeTransform = typeTransform
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        val argTypes: List<RelDataType> = SqlTypeUtil.deriveType(callBinding, callBinding.operands())
        val componentType: RelDataType = callBinding.getTypeFactory().leastRestrictive(argTypes)
        if (null == componentType) {
            if (throwOnFailure) {
                throw callBinding.newValidationError(RESOURCE.needSameTypeParameter())
            }
            return false
        }
        return true
    }

    @Override
    fun deriveType(
        validator: SqlValidator,
        scope: SqlValidatorScope?,
        call: SqlCall
    ): RelDataType {
        val subSelect: SqlSelect = call.operand(0)
        subSelect.validateExpr(validator, scope)
        val ns: SqlValidatorNamespace = requireNonNull(
            validator.getNamespace(subSelect)
        ) { "namespace is missing for $subSelect" }
        val rowType: RelDataType = requireNonNull(ns.getRowType(), "rowType")
        val opBinding = SqlCallBinding(validator, scope, call)
        return typeTransform.transformType(opBinding, rowType)
    }

    @Override
    fun unparse(
        writer: SqlWriter,
        call: SqlCall,
        leftPrec: Int,
        rightPrec: Int
    ) {
        writer.keyword(getName())
        val frame: SqlWriter.Frame = writer.startList("(", ")")
        assert(call.operandCount() === 1)
        call.operand(0).unparse(writer, leftPrec, rightPrec)
        writer.endList(frame)
    }

    @Override
    fun argumentMustBeScalar(ordinal: Int): Boolean {
        return false
    }
}
