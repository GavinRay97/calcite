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

import org.apache.calcite.sql.SqlCall

/**
 * An internal operator that throws an exception.
 *
 *
 * The exception is thrown with a (localized) error message which is the only
 * input parameter to the operator.
 *
 *
 * The return type is defined as a `BOOLEAN` to facilitate the use
 * of it in constructs such as the following:
 *
 * <blockquote>`CASE<br></br>
 * WHEN <conditionn> THEN true<br></br>
 * ELSE throw("what's wrong with you man?")<br></br>
 * END`</blockquote>
 */
class SqlThrowOperator  //~ Constructors -----------------------------------------------------------
    : SqlSpecialOperator(
    "\$throw",
    SqlKind.OTHER,
    2,
    true,
    ReturnTypes.BOOLEAN,
    null,
    OperandTypes.CHARACTER
) {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun unparse(
        writer: SqlWriter,
        call: SqlCall,
        leftPrec: Int,
        rightPrec: Int
    ) {
        val frame: SqlWriter.Frame = writer.startFunCall(getName())
        call.operand(0).unparse(writer, 0, 0)
        writer.endFunCall(frame)
    }
}
