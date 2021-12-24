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
package org.apache.calcite.sql

import org.apache.calcite.sql.type.OperandTypes

/**
 * SqlSetOperator represents a relational set theory operator (UNION, INTERSECT,
 * MINUS). These are binary operators, but with an extra boolean attribute
 * tacked on for whether to remove duplicates (e.g. UNION ALL does not remove
 * duplicates).
 */
class SqlSetOperator : SqlBinaryOperator {
    //~ Methods ----------------------------------------------------------------
    //~ Instance fields --------------------------------------------------------
    val isAll: Boolean

    //~ Constructors -----------------------------------------------------------
    constructor(
        name: String?,
        kind: SqlKind?,
        prec: Int,
        all: Boolean
    ) : super(
        name,
        kind,
        prec,
        true,
        ReturnTypes.LEAST_RESTRICTIVE,
        null,
        OperandTypes.SET_OP
    ) {
        isAll = all
    }

    constructor(
        name: String?,
        kind: SqlKind?,
        prec: Int,
        all: Boolean,
        returnTypeInference: SqlReturnTypeInference?,
        operandTypeInference: SqlOperandTypeInference?,
        operandTypeChecker: SqlOperandTypeChecker?
    ) : super(
        name,
        kind,
        prec,
        true,
        returnTypeInference,
        operandTypeInference,
        operandTypeChecker
    ) {
        isAll = all
    }

    val isDistinct: Boolean
        get() = !isAll

    @Override
    fun validateCall(
        call: SqlCall?,
        validator: SqlValidator,
        scope: SqlValidatorScope?,
        operandScope: SqlValidatorScope?
    ) {
        validator.validateQuery(call, operandScope, validator.getUnknownType())
    }
}
