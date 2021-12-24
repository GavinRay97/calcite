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

import org.apache.calcite.sql.type.SqlOperandTypeChecker

/**
 * SqlFunctionalOperator is a base class for special operators which use
 * functional syntax.
 */
class SqlFunctionalOperator  //~ Constructors -----------------------------------------------------------
    (
    name: String?,
    kind: SqlKind?,
    pred: Int,
    isLeftAssoc: Boolean,
    @Nullable returnTypeInference: SqlReturnTypeInference?,
    @Nullable operandTypeInference: SqlOperandTypeInference?,
    @Nullable operandTypeChecker: SqlOperandTypeChecker?
) : SqlSpecialOperator(
    name,
    kind,
    pred,
    isLeftAssoc,
    returnTypeInference,
    operandTypeInference,
    operandTypeChecker
) {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun unparse(
        writer: SqlWriter?,
        call: SqlCall?,
        leftPrec: Int,
        rightPrec: Int
    ) {
        SqlUtil.unparseFunctionSyntax(this, writer, call, false)
    }
}
