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

import org.apache.calcite.sql.SqlFunction

/**
 * Base class for functions such as "USER", "CURRENT_ROLE", and "CURRENT_PATH".
 */
class SqlBaseContextVariable  //~ Constructors -----------------------------------------------------------
/** Creates a SqlBaseContextVariable.  */
protected constructor(
    name: String?,
    returnType: SqlReturnTypeInference?, category: SqlFunctionCategory?
) : SqlFunction(
    name, SqlKind.OTHER_FUNCTION, returnType, null, OperandTypes.NILADIC,
    category
) {
    //~ Methods ----------------------------------------------------------------
    @get:Override
    val syntax: SqlSyntax
        get() = SqlSyntax.FUNCTION_ID

    // All of the string constants are monotonic.
    @Override
    fun getMonotonicity(call: SqlOperatorBinding?): SqlMonotonicity {
        return SqlMonotonicity.CONSTANT
    }

    // Plans referencing context variables should never be cached
    @get:Override
    val isDynamicFunction: Boolean
        get() = true
}
