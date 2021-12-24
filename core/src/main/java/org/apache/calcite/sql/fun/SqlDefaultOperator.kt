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
 * Operator that indicates that an argument to a function call is to take its
 * default value.
 *
 *
 * Not an expression; just a holder to represent syntax until the validator
 * has chance to resolve arguments.
 */
internal class SqlDefaultOperator : SqlSpecialOperator(
    "DEFAULT", SqlKind.DEFAULT, 100, true,
    ReturnTypes.explicit(SqlTypeName.ANY), InferTypes.RETURN_TYPE,
    OperandTypes.NILADIC
) {
    @Override
    fun unparse(
        writer: SqlWriter, call: SqlCall?, leftPrec: Int,
        rightPrec: Int
    ) {
        writer.keyword(getName())
    }
}
