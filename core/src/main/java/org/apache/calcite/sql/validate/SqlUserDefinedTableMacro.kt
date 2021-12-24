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

import org.apache.calcite.linq4j.Ord

/**
 * User-defined table macro.
 *
 *
 * Created by the validator, after resolving a function call to a function
 * defined in a Calcite schema.
 */
class SqlUserDefinedTableMacro(
    opName: SqlIdentifier, kind: SqlKind?,
    returnTypeInference: SqlReturnTypeInference?,
    operandTypeInference: SqlOperandTypeInference?,
    @Nullable operandMetadata: SqlOperandMetadata?,
    tableMacro: TableMacro
) : SqlFunction(
    Util.last(opName.names), opName, kind,
    returnTypeInference, operandTypeInference, operandMetadata,
    SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION
), SqlTableFunction {
    private val tableMacro: TableMacro

    @Deprecated // to be removed before 2.0
    constructor(
        opName: SqlIdentifier?,
        returnTypeInference: SqlReturnTypeInference?,
        operandTypeInference: SqlOperandTypeInference?,
        @Nullable operandTypeChecker: SqlOperandTypeChecker?, paramTypes: List<RelDataType?>?,
        tableMacro: TableMacro?
    ) : this(
        opName, SqlKind.OTHER_FUNCTION, returnTypeInference,
        operandTypeInference,
        if (operandTypeChecker is SqlOperandMetadata) operandTypeChecker as SqlOperandMetadata? else null, tableMacro
    ) {
        Util.discard(paramTypes) // no longer used
    }

    /** Creates a user-defined table macro.  */
    init {
        this.tableMacro = tableMacro
    }

    @get:Nullable
    @get:Override
    val operandTypeChecker: SqlOperandMetadata
        get() = super.getOperandTypeChecker() as SqlOperandMetadata

    @get:Override
    @get:SuppressWarnings("deprecation")
    val paramNames: List<String>
        get() = Util.transform(tableMacro.getParameters(), FunctionParameter::getName)

    /** Returns the table in this UDF, or null if there is no table.  */
    fun getTable(callBinding: SqlOperatorBinding): TranslatableTable {
        val arguments: List<Object> = convertArguments(callBinding, tableMacro, getNameAsId(), true)
        return tableMacro.apply(arguments)
    }

    @get:Override
    val rowTypeInference: SqlReturnTypeInference
        get() = SqlReturnTypeInference { callBinding: SqlOperatorBinding -> inferRowType(callBinding) }

    private fun inferRowType(callBinding: SqlOperatorBinding): RelDataType {
        val typeFactory: RelDataTypeFactory = callBinding.getTypeFactory()
        val table: TranslatableTable = getTable(callBinding)
        return table.getRowType(typeFactory)
    }

    companion object {
        /**
         * Converts arguments from [org.apache.calcite.sql.SqlNode] to
         * java object format.
         *
         * @param callBinding Operator bound to arguments
         * @param function target function to get parameter types from
         * @param opName name of the operator to use in error message
         * @param failOnNonLiteral true when conversion should fail on non-literal
         * @return converted list of arguments
         */
        fun convertArguments(
            callBinding: SqlOperatorBinding,
            function: Function, opName: SqlIdentifier, failOnNonLiteral: Boolean
        ): List<Object> {
            val typeFactory: RelDataTypeFactory = callBinding.getTypeFactory()
            val arguments: List<Object> = ArrayList(callBinding.getOperandCount())
            Ord.forEach(function.getParameters()) { parameter, i ->
                val type: RelDataType = parameter.getType(typeFactory)
                val value: Object?
                value = if (callBinding.isOperandLiteral(i, true)) {
                    callBinding.getOperandLiteralValue(i, type)
                } else {
                    if (failOnNonLiteral) {
                        throw IllegalArgumentException(
                            "All arguments of call to macro "
                                    + opName + " should be literal. Actual argument #"
                                    + parameter.getOrdinal() + " (" + parameter.getName()
                                    + ") is not literal"
                        )
                    }
                    if (type.isNullable()) {
                        null
                    } else {
                        0L
                    }
                }
                arguments.add(value)
            }
            return arguments
        }
    }
}
