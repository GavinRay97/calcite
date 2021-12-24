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
 * User-defined scalar function.
 *
 *
 * Created by the validator, after resolving a function call to a function
 * defined in a Calcite schema.
 */
class SqlUserDefinedFunction protected constructor(
    opName: SqlIdentifier, kind: SqlKind?,
    returnTypeInference: SqlReturnTypeInference?,
    operandTypeInference: SqlOperandTypeInference?,
    @Nullable operandMetadata: SqlOperandMetadata?,
    function: Function,
    category: SqlFunctionCategory?
) : SqlFunction(
    Util.last(opName.names), opName, kind, returnTypeInference,
    operandTypeInference, operandMetadata, category
) {
    val function: Function

    @Deprecated // to be removed before 2.0
    constructor(
        opName: SqlIdentifier?,
        returnTypeInference: SqlReturnTypeInference?,
        operandTypeInference: SqlOperandTypeInference?,
        @Nullable operandTypeChecker: SqlOperandTypeChecker?,
        paramTypes: List<RelDataType?>?,
        function: Function?
    ) : this(
        opName, SqlKind.OTHER_FUNCTION, returnTypeInference,
        operandTypeInference,
        if (operandTypeChecker is SqlOperandMetadata) operandTypeChecker as SqlOperandMetadata? else null, function
    ) {
        Util.discard(paramTypes) // no longer used
    }

    /** Creates a [SqlUserDefinedFunction].  */
    constructor(
        opName: SqlIdentifier, kind: SqlKind?,
        returnTypeInference: SqlReturnTypeInference?,
        operandTypeInference: SqlOperandTypeInference?,
        @Nullable operandMetadata: SqlOperandMetadata?,
        function: Function
    ) : this(
        opName, kind, returnTypeInference, operandTypeInference,
        operandMetadata, function, SqlFunctionCategory.USER_DEFINED_FUNCTION
    ) {
    }

    /** Constructor used internally and by derived classes.  */
    init {
        this.function = function
    }

    @get:Nullable
    @get:Override
    val operandTypeChecker: SqlOperandMetadata
        get() = super.getOperandTypeChecker() as SqlOperandMetadata

    /**
     * Returns function that implements given operator call.
     * @return function that implements given operator call
     */
    fun getFunction(): Function {
        return function
    }

    @get:Override
    @get:SuppressWarnings("deprecation")
    val paramNames: List<String>
        get() = Util.transform(function.getParameters(), FunctionParameter::getName)
}
