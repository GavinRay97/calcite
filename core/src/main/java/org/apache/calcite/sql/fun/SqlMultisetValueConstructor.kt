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
 * Definition of the SQL:2003 standard MULTISET constructor, `MULTISET
 * [<expr>, ...]`.
 *
 *
 * Derived classes construct other kinds of collections.
 *
 * @see SqlMultisetQueryConstructor
 */
class SqlMultisetValueConstructor protected constructor(name: String?, kind: SqlKind?) : SqlSpecialOperator(
    name,
    kind, MDX_PRECEDENCE,
    false,
    ReturnTypes.ARG0,
    InferTypes.FIRST_KNOWN,
    OperandTypes.VARIADIC
) {
    //~ Constructors -----------------------------------------------------------
    constructor() : this("MULTISET", SqlKind.MULTISET_VALUE_CONSTRUCTOR) {}

    //~ Methods ----------------------------------------------------------------
    @Override
    fun inferReturnType(
        opBinding: SqlOperatorBinding
    ): RelDataType {
        val type: RelDataType = getComponentType(
            opBinding.getTypeFactory(),
            opBinding.collectOperandTypes()
        )
        requireNonNull(type, "inferred multiset value")
        return SqlTypeUtil.createMultisetType(
            opBinding.getTypeFactory(),
            type,
            false
        )
    }

    @Nullable
    protected fun getComponentType(
        typeFactory: RelDataTypeFactory,
        argTypes: List<RelDataType?>?
    ): RelDataType {
        return typeFactory.leastRestrictive(argTypes)
    }

    @Override
    fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        val argTypes: List<RelDataType> = SqlTypeUtil.deriveType(callBinding, callBinding.operands())
        if (argTypes.size() === 0) {
            throw callBinding.newValidationError(RESOURCE.requireAtLeastOneArg())
        }
        val componentType: RelDataType = getComponentType(
            callBinding.getTypeFactory(),
            argTypes
        )
        if (null == componentType) {
            if (throwOnFailure) {
                throw callBinding.newValidationError(RESOURCE.needSameTypeParameter())
            }
            return false
        }
        return true
    }

    @Override
    fun unparse(
        writer: SqlWriter,
        call: SqlCall,
        leftPrec: Int,
        rightPrec: Int
    ) {
        writer.keyword(getName()) // "MULTISET" or "ARRAY"
        val frame: SqlWriter.Frame = writer.startList("[", "]")
        for (operand in call.getOperandList()) {
            writer.sep(",")
            operand.unparse(writer, leftPrec, rightPrec)
        }
        writer.endList(frame)
    }
}
