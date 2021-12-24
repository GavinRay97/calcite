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
 * User-defined table function.
 *
 *
 * Created by the validator, after resolving a function call to a function
 * defined in a Calcite schema.
 */
class SqlUserDefinedTableFunction
/** Creates a user-defined table function.  */
    (
    opName: SqlIdentifier, kind: SqlKind?,
    returnTypeInference: SqlReturnTypeInference?,
    operandTypeInference: SqlOperandTypeInference?,
    @Nullable operandMetadata: SqlOperandMetadata?,
    function: TableFunction
) : SqlUserDefinedFunction(
    opName, kind, returnTypeInference, operandTypeInference,
    operandMetadata, function,
    SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION
), SqlTableFunction {
    @Deprecated // to be removed before 2.0
    constructor(
        opName: SqlIdentifier?,
        returnTypeInference: SqlReturnTypeInference?,
        operandTypeInference: SqlOperandTypeInference?,
        @Nullable operandTypeChecker: SqlOperandTypeChecker?,
        paramTypes: List<RelDataType?>?,  // no longer used
        function: TableFunction?
    ) : this(
        opName, SqlKind.OTHER_FUNCTION, returnTypeInference,
        operandTypeInference,
        if (operandTypeChecker is SqlOperandMetadata) operandTypeChecker as SqlOperandMetadata? else null, function
    ) {
        Util.discard(paramTypes)
    }

    /**
     * Returns function that implements given operator call.
     * @return function that implements given operator call
     */
    @Override
    override fun getFunction(): TableFunction {
        return super.getFunction() as TableFunction
    }

    @get:Override
    val rowTypeInference: SqlReturnTypeInference
        get() = SqlReturnTypeInference { callBinding: SqlOperatorBinding -> inferRowType(callBinding) }

    private fun inferRowType(callBinding: SqlOperatorBinding): RelDataType {
        val arguments: List<Object> = SqlUserDefinedTableMacro.convertArguments(
            callBinding, function,
            getNameAsId(), false
        )
        return getFunction().getRowType(callBinding.getTypeFactory(), arguments)
    }

    /**
     * Returns the row type of the table yielded by this function when
     * applied to given arguments. Only literal arguments are passed,
     * non-literal are replaced with default values (null, 0, false, etc).
     *
     * @param callBinding Operand bound to arguments
     * @return element type of the table (e.g. `Object[].class`)
     */
    fun getElementType(callBinding: SqlOperatorBinding): Type {
        val arguments: List<Object> = SqlUserDefinedTableMacro.convertArguments(
            callBinding, function,
            getNameAsId(), false
        )
        return getFunction().getElementType(arguments)
    }
}
