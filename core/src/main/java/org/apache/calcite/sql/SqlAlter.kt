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

/**
 * Base class for an ALTER statements parse tree nodes. The portion of the
 * statement covered by this class is "ALTER &lt;SCOPE&gt;. Subclasses handle
 * whatever comes after the scope.
 */
abstract class SqlAlter protected constructor(
    pos: SqlParserPos?, @field:Nullable
    /** Scope of the operation. Values "SYSTEM" and "SESSION" are typical.  */
    @get:Nullable
    @param:Nullable var scope: String?
) : SqlCall(pos) {

    protected constructor(pos: SqlParserPos?) : this(pos, null) {}

    @Override
    override fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        val scope = scope
        if (scope != null) {
            writer.keyword("ALTER")
            writer.keyword(scope)
        }
        unparseAlterOperation(writer, leftPrec, rightPrec)
    }

    protected abstract fun unparseAlterOperation(writer: SqlWriter?, leftPrec: Int, rightPrec: Int)
}
