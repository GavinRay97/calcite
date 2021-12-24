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
package org.apache.calcite.sql.validate

import org.apache.calcite.rel.type.RelDataType

/**
 * Namespace whose contents are defined by the result of a call to a
 * user-defined procedure.
 */
class ProcedureNamespace internal constructor(
    validator: SqlValidatorImpl?,
    scope: SqlValidatorScope,
    call: SqlCall,
    enclosingNode: SqlNode?
) : AbstractNamespace(validator, enclosingNode) {
    //~ Instance fields --------------------------------------------------------
    private val scope: SqlValidatorScope
    private val call: SqlCall

    //~ Constructors -----------------------------------------------------------
    init {
        this.scope = scope
        this.call = call
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun validateImpl(targetRowType: RelDataType?): RelDataType {
        validator.inferUnknownTypes(validator.unknownType, scope, call)
        val type: RelDataType = validator.deriveTypeImpl(scope, call)
        val operator: SqlOperator = call.getOperator()
        val callBinding = SqlCallBinding(validator, scope, call)
        if (operator !is SqlTableFunction) {
            throw IllegalArgumentException(
                "Argument must be a table function: "
                        + operator.getNameAsId()
            )
        }
        val tableFunction: SqlTableFunction = operator as SqlTableFunction
        if (type.getSqlTypeName() !== SqlTypeName.CURSOR) {
            throw IllegalArgumentException(
                "Table function should have CURSOR "
                        + "type, not " + type
            )
        }
        val rowTypeInference: SqlReturnTypeInference = tableFunction.getRowTypeInference()
        return requireNonNull(
            rowTypeInference.inferReturnType(callBinding)
        ) { "got null from inferReturnType for call " + callBinding.getCall() }
    }

    @get:Nullable
    @get:Override
    val node: SqlNode
        get() = call
}
