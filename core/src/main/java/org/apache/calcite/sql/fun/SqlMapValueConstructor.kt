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
 * Definition of the MAP constructor,
 * `MAP [<key>, <value>, ...]`.
 *
 *
 * This is an extension to standard SQL.
 */
class SqlMapValueConstructor : SqlMultisetValueConstructor("MAP", SqlKind.MAP_VALUE_CONSTRUCTOR) {
    @Override
    override fun inferReturnType(opBinding: SqlOperatorBinding): RelDataType {
        val type: Pair<RelDataType, RelDataType> = getComponentTypes(
            opBinding.getTypeFactory(), opBinding.collectOperandTypes()
        )
        return SqlTypeUtil.createMapType(
            opBinding.getTypeFactory(),
            requireNonNull(type.left, "inferred key type"),
            requireNonNull(type.right, "inferred value type"),
            false
        )
    }

    @Override
    override fun checkOperandTypes(
        callBinding: SqlCallBinding,
        throwOnFailure: Boolean
    ): Boolean {
        val argTypes: List<RelDataType> = SqlTypeUtil.deriveType(callBinding, callBinding.operands())
        if (argTypes.size() === 0) {
            throw callBinding.newValidationError(RESOURCE.mapRequiresTwoOrMoreArgs())
        }
        if (argTypes.size() % 2 > 0) {
            throw callBinding.newValidationError(RESOURCE.mapRequiresEvenArgCount())
        }
        val componentType: Pair<RelDataType, RelDataType> = getComponentTypes(
            callBinding.getTypeFactory(), argTypes
        )
        if (null == componentType.left || null == componentType.right) {
            if (throwOnFailure) {
                throw callBinding.newValidationError(RESOURCE.needSameTypeParameter())
            }
            return false
        }
        return true
    }

    companion object {
        private fun getComponentTypes(
            typeFactory: RelDataTypeFactory,
            argTypes: List<RelDataType>
        ): Pair<RelDataType, RelDataType> {
            return Pair.of(
                typeFactory.leastRestrictive(Util.quotientList(argTypes, 2, 0)),
                typeFactory.leastRestrictive(Util.quotientList(argTypes, 2, 1))
            )
        }
    }
}
