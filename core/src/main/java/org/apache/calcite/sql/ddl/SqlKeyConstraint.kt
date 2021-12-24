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

import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlNodeList
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.util.ImmutableNullableList
import java.util.List

/**
 * Parse tree for `UNIQUE`, `PRIMARY KEY` constraints.
 *
 *
 * And `FOREIGN KEY`, when we support it.
 */
class SqlKeyConstraint internal constructor(
    pos: SqlParserPos?, @Nullable name: SqlIdentifier?,
    columnList: SqlNodeList
) : SqlCall(pos) {
    @Nullable
    private val name: SqlIdentifier?
    private val columnList: SqlNodeList

    /** Creates a SqlKeyConstraint.  */
    init {
        this.name = name
        this.columnList = columnList
    }

    @get:Override
    val operator: SqlOperator
        get() = UNIQUE

    @get:Override
    @get:SuppressWarnings("nullness")
    val operandList: List<Any>
        get() = ImmutableNullableList.of(name, columnList)

    @Override
    fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        if (name != null) {
            writer.keyword("CONSTRAINT")
            name.unparse(writer, 0, 0)
        }
        writer.keyword(operator.getName()) // "UNIQUE" or "PRIMARY KEY"
        columnList.unparse(writer, 1, 1)
    }

    companion object {
        private val UNIQUE: SqlSpecialOperator = SqlSpecialOperator("UNIQUE", SqlKind.UNIQUE)
        protected val PRIMARY: SqlSpecialOperator = SqlSpecialOperator("PRIMARY KEY", SqlKind.PRIMARY_KEY)

        /** Creates a UNIQUE constraint.  */
        fun unique(
            pos: SqlParserPos?, name: SqlIdentifier?,
            columnList: SqlNodeList
        ): SqlKeyConstraint {
            return SqlKeyConstraint(pos, name, columnList)
        }

        /** Creates a PRIMARY KEY constraint.  */
        fun primary(
            pos: SqlParserPos?, name: SqlIdentifier?,
            columnList: SqlNodeList
        ): SqlKeyConstraint {
            return object : SqlKeyConstraint(pos, name, columnList) {
                @get:Override
                override val operator: SqlOperator
                    get() = PRIMARY
            }
        }
    }
}
