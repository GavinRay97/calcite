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

import org.apache.calcite.rel.type.RelDataType

/**
 * Generic operator for nodes with internal syntax.
 *
 *
 * If you do not override [.getSyntax] or
 * [.unparse], they will be unparsed using
 * function syntax, `F(arg1, arg2, ...)`. This may be OK for operators
 * that never appear in SQL, only as structural elements in an abstract syntax
 * tree.
 *
 *
 * You can use this operator, without creating a sub-class, for
 * non-expression nodes. Validate will validate the arguments, but will not
 * attempt to deduce a type.
 */
class SqlInternalOperator(
    name: String?,
    kind: SqlKind?,
    prec: Int,
    isLeftAssoc: Boolean,
    returnTypeInference: SqlReturnTypeInference?,
    @Nullable operandTypeInference: SqlOperandTypeInference?,
    operandTypeChecker: SqlOperandTypeChecker?
) : SqlSpecialOperator(
    name,
    kind,
    prec,
    isLeftAssoc,
    returnTypeInference,
    operandTypeInference,
    operandTypeChecker
) {
    //~ Constructors -----------------------------------------------------------
    constructor(
        name: String?,
        kind: SqlKind?
    ) : this(name, kind, 2) {
    }

    constructor(
        name: String?,
        kind: SqlKind?,
        prec: Int
    ) : this(name, kind, prec, true, ReturnTypes.ARG0, null, OperandTypes.VARIADIC) {
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val syntax: SqlSyntax
        get() = SqlSyntax.INTERNAL

    @Override
    fun deriveType(
        validator: SqlValidator?,
        scope: SqlValidatorScope?, call: SqlCall?
    ): RelDataType {
        return validateOperands(validator, scope, call)
    }
}
