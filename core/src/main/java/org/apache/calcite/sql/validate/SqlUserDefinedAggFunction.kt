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

import org.apache.calcite.rel.type.RelDataTypeFactory

/**
 * User-defined aggregate function.
 *
 *
 * Created by the validator, after resolving a function call to a function
 * defined in a Calcite schema.
 */
class SqlUserDefinedAggFunction(
    opName: SqlIdentifier, kind: SqlKind?,
    returnTypeInference: SqlReturnTypeInference?,
    operandTypeInference: SqlOperandTypeInference?,
    @Nullable operandMetadata: SqlOperandMetadata?, function: AggregateFunction,
    requiresOrder: Boolean, requiresOver: Boolean,
    requiresGroupOrder: Optionality?
) : SqlAggFunction(
    Util.last(opName.names), opName, kind,
    returnTypeInference, operandTypeInference, operandMetadata,
    SqlFunctionCategory.USER_DEFINED_FUNCTION, requiresOrder, requiresOver,
    requiresGroupOrder
) {
    val function: AggregateFunction

    @Deprecated // to be removed before 2.0
    constructor(
        opName: SqlIdentifier?,
        returnTypeInference: SqlReturnTypeInference?,
        operandTypeInference: SqlOperandTypeInference?,
        @Nullable operandTypeChecker: SqlOperandTypeChecker?, function: AggregateFunction?,
        requiresOrder: Boolean, requiresOver: Boolean,
        requiresGroupOrder: Optionality?, typeFactory: RelDataTypeFactory?
    ) : this(
        opName, SqlKind.OTHER_FUNCTION, returnTypeInference,
        operandTypeInference,
        if (operandTypeChecker is SqlOperandMetadata) operandTypeChecker as SqlOperandMetadata? else null, function,
        requiresOrder, requiresOver, requiresGroupOrder
    ) {
        Util.discard(typeFactory) // no longer used
    }

    /** Creates a SqlUserDefinedAggFunction.  */
    init {
        this.function = function
    }

    @get:Nullable
    @get:Override
    val operandTypeChecker: SqlOperandMetadata
        get() = super.getOperandTypeChecker() as SqlOperandMetadata
}
