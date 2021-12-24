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

import org.apache.calcite.sql.SqlAsOperator

/**
 * Operator that assigns an argument to a function call to a particular named
 * parameter.
 *
 *
 * Not an expression; just a holder to represent syntax until the validator
 * has chance to resolve arguments.
 *
 *
 * Sub-class of [SqlAsOperator] ("AS") out of convenience; to be
 * consistent with AS, we reverse the arguments.
 */
internal class SqlArgumentAssignmentOperator : SqlAsOperator(
    "=>", SqlKind.ARGUMENT_ASSIGNMENT, 20, true, ReturnTypes.ARG0,
    InferTypes.RETURN_TYPE, OperandTypes.ANY_IGNORE
) {
    @Override
    fun unparse(
        writer: SqlWriter, call: SqlCall, leftPrec: Int,
        rightPrec: Int
    ) {
        // Arguments are held in reverse order to be consistent with base class (AS).
        call.operand(1).unparse(writer, leftPrec, getLeftPrec())
        writer.keyword(getName())
        call.operand(0).unparse(writer, getRightPrec(), rightPrec)
    }

    @Override
    fun argumentMustBeScalar(ordinal: Int): Boolean {
        return false
    }
}
