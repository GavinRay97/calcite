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

import org.apache.calcite.sql.parser.SqlParserPos
import java.util.Objects

/** Base class for CREATE, DROP and other DDL statements.  */
abstract class SqlDdl protected constructor(operator: SqlOperator?, pos: SqlParserPos?) : SqlCall(pos) {
    private override val operator: SqlOperator

    /** Creates a SqlDdl.  */
    init {
        this.operator = Objects.requireNonNull(operator, "operator")
    }

    @Override
    fun getOperator(): SqlOperator {
        return operator
    }

    companion object {
        /** Use this operator only if you don't have a better one.  */
        protected val DDL_OPERATOR: SqlOperator = SqlSpecialOperator("DDL", SqlKind.OTHER_DDL)
    }
}
