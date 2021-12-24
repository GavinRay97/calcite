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

/** Operator that returns the current or next value of a sequence.  */
class SqlSequenceValueOperator internal constructor(kind: SqlKind) : SqlSpecialOperator(kind.name(), kind, 100) {
    /** Creates a SqlSequenceValueOperator.  */
    init {
        assert(kind === SqlKind.NEXT_VALUE || kind === SqlKind.CURRENT_VALUE)
    }

    @get:Override
    val isDeterministic: Boolean
        get() = false

    @Override
    fun unparse(
        writer: SqlWriter, call: SqlCall, leftPrec: Int,
        rightPrec: Int
    ) {
        writer.sep(if (kind === SqlKind.NEXT_VALUE) "NEXT VALUE FOR" else "CURRENT VALUE FOR")
        call.getOperandList().get(0).unparse(writer, 0, 0)
    }

    @Override
    fun deriveType(
        validator: SqlValidator,
        scope: SqlValidatorScope?, call: SqlCall?
    ): RelDataType {
        val typeFactory: RelDataTypeFactory = validator.getTypeFactory()
        return typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.BIGINT), false
        )
    }

    @Override
    fun validateCall(
        call: SqlCall, validator: SqlValidator,
        scope: SqlValidatorScope?, operandScope: SqlValidatorScope?
    ) {
        val operands: List<SqlNode> = call.getOperandList()
        assert(operands.size() === 1)
        assert(operands[0] is SqlIdentifier)
        val id: SqlIdentifier = operands[0] as SqlIdentifier
        validator.validateSequenceValue(scope, id)
    }
}
