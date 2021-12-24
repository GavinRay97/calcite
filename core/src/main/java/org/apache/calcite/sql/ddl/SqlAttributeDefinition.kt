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
import org.apache.calcite.sql.SqlCollation
import org.apache.calcite.sql.SqlDataTypeSpec
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.parser.SqlParserPos
import com.google.common.collect.ImmutableList
import java.util.List

/**
 * Parse tree for SqlAttributeDefinition,
 * which is part of a [SqlCreateType].
 */
class SqlAttributeDefinition internal constructor(
    pos: SqlParserPos?, name: SqlIdentifier,
    dataType: SqlDataTypeSpec, @Nullable expression: SqlNode?, @Nullable collation: SqlCollation?
) : SqlCall(pos) {
    val name: SqlIdentifier
    val dataType: SqlDataTypeSpec

    @Nullable
    val expression: SqlNode?

    @Nullable
    val collation: SqlCollation?

    /** Creates a SqlAttributeDefinition; use [SqlDdlNodes.attribute].  */
    init {
        this.name = name
        this.dataType = dataType
        this.expression = expression
        this.collation = collation
    }

    @get:Override
    val operator: SqlOperator
        get() = OPERATOR

    @get:Override
    val operandList: List<Any>
        get() = ImmutableList.of(name, dataType)

    @Override
    fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        name.unparse(writer, 0, 0)
        dataType.unparse(writer, 0, 0)
        if (collation != null) {
            writer.keyword("COLLATE")
            collation.unparse(writer)
        }
        if (Boolean.FALSE.equals(dataType.getNullable())) {
            writer.keyword("NOT NULL")
        }
        val expression: SqlNode? = expression
        if (expression != null) {
            writer.keyword("DEFAULT")
            SqlColumnDeclaration.exp(writer, expression)
        }
    }

    companion object {
        private val OPERATOR: SqlSpecialOperator = SqlSpecialOperator("ATTRIBUTE_DEF", SqlKind.ATTRIBUTE_DEF)
    }
}
