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

import org.apache.calcite.sql.SqlDrop
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.parser.SqlParserPos
import com.google.common.collect.ImmutableList
import java.util.List

/**
 * Parse tree for `DROP SCHEMA` statement.
 */
class SqlDropSchema internal constructor(
    pos: SqlParserPos?, private val foreign: Boolean, ifExists: Boolean,
    name: SqlIdentifier
) : SqlDrop(OPERATOR, pos, ifExists) {
    val name: SqlIdentifier

    /** Creates a SqlDropSchema.  */
    init {
        this.name = name
    }

    @get:Override
    val operandList: List<Any>
        get() = ImmutableList.of(
            SqlLiteral.createBoolean(foreign, SqlParserPos.ZERO), name
        )

    @Override
    fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        writer.keyword("DROP")
        if (foreign) {
            writer.keyword("FOREIGN")
        }
        writer.keyword("SCHEMA")
        if (ifExists) {
            writer.keyword("IF EXISTS")
        }
        name.unparse(writer, leftPrec, rightPrec)
    }

    companion object {
        private val OPERATOR: SqlOperator = SqlSpecialOperator("DROP SCHEMA", SqlKind.DROP_SCHEMA)
    }
}
