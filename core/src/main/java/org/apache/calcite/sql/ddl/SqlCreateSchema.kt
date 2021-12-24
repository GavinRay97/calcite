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
package org.apache.calcite.sql.ddl

import org.apache.calcite.sql.SqlCreate
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.util.ImmutableNullableList
import java.util.List
import java.util.Objects

/**
 * Parse tree for `CREATE SCHEMA` statement.
 */
class SqlCreateSchema internal constructor(
    pos: SqlParserPos?, replace: Boolean, ifNotExists: Boolean,
    name: SqlIdentifier?
) : SqlCreate(OPERATOR, pos, replace, ifNotExists) {
    val name: SqlIdentifier

    /** Creates a SqlCreateSchema.  */
    init {
        this.name = Objects.requireNonNull(name, "name")
    }

    @get:Override
    val operandList: List<Any>
        get() = ImmutableNullableList.of(name)

    @Override
    fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        if (getReplace()) {
            writer.keyword("CREATE OR REPLACE")
        } else {
            writer.keyword("CREATE")
        }
        writer.keyword("SCHEMA")
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS")
        }
        name.unparse(writer, leftPrec, rightPrec)
    }

    companion object {
        private val OPERATOR: SqlOperator = SqlSpecialOperator("CREATE SCHEMA", SqlKind.CREATE_SCHEMA)
    }
}
