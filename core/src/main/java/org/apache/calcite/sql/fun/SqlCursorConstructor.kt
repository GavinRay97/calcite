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
 * SqlCursorConstructor defines the non-standard CURSOR(&lt;query&gt;)
 * constructor.
 */
class SqlCursorConstructor  //~ Constructors -----------------------------------------------------------
    : SqlSpecialOperator(
    "CURSOR",
    SqlKind.CURSOR, MDX_PRECEDENCE,
    false,
    ReturnTypes.CURSOR,
    null,
    OperandTypes.ANY
) {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun deriveType(
        validator: SqlValidator,
        scope: SqlValidatorScope?,
        call: SqlCall
    ): RelDataType {
        val subSelect: SqlSelect = call.operand(0)
        validator.declareCursor(subSelect, scope)
        subSelect.validateExpr(validator, scope)
        return super.deriveType(validator, scope, call)
    }

    @Override
    fun unparse(
        writer: SqlWriter,
        call: SqlCall,
        leftPrec: Int,
        rightPrec: Int
    ) {
        writer.keyword("CURSOR")
        val frame: SqlWriter.Frame = writer.startList("(", ")")
        assert(call.operandCount() === 1)
        call.operand(0).unparse(writer, leftPrec, rightPrec)
        writer.endList(frame)
    }

    @Override
    fun argumentMustBeScalar(ordinal: Int): Boolean {
        return false
    }
}
