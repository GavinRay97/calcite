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
 * Infix cast operator, "::", as used in PostgreSQL.
 *
 *
 * This operator is not enabled in the default operator table; it is
 * registered for various dialects via [SqlLibraryOperators.INFIX_CAST].
 *
 *
 * The [.kind] is [SqlKind.CAST], the same as the built-in
 * `CAST` function, [SqlCastFunction]. Be sure to use `kind`
 * rather than `instanceof` if you would like your code to apply to both
 * operators.
 *
 *
 * Unlike `SqlCastFunction`, it can only be used for
 * [SqlCall][org.apache.calcite.sql.SqlCall];
 * [RexCall][org.apache.calcite.rex.RexCall] must use
 * `SqlCastFunction`.
 */
internal class SqlCastOperator : SqlBinaryOperator("::", SqlKind.CAST, 94, true, null, InferTypes.FIRST_KNOWN, null) {
    @Override
    fun inferReturnType(
        opBinding: SqlOperatorBinding?
    ): RelDataType {
        return SqlStdOperatorTable.CAST.inferReturnType(opBinding)
    }

    @Override
    fun checkOperandTypes(
        callBinding: SqlCallBinding?,
        throwOnFailure: Boolean
    ): Boolean {
        return SqlStdOperatorTable.CAST.checkOperandTypes(
            callBinding,
            throwOnFailure
        )
    }

    @get:Override
    val operandCountRange: SqlOperandCountRange
        get() = SqlStdOperatorTable.CAST.getOperandCountRange()

    @Override
    fun getMonotonicity(
        call: SqlOperatorBinding?
    ): SqlMonotonicity {
        return SqlStdOperatorTable.CAST.getMonotonicity(call)
    }
}
