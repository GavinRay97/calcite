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
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlNodeList
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.util.Pair
import org.apache.calcite.util.Util
import com.google.common.base.Preconditions
import java.util.Arrays
import java.util.List
import java.util.Objects

/**
 * Parse tree for `CREATE FUNCTION` statement.
 */
class SqlCreateFunction(
    pos: SqlParserPos?, replace: Boolean,
    ifNotExists: Boolean, name: SqlIdentifier?,
    className: SqlNode, usingList: SqlNodeList
) : SqlCreate(OPERATOR, pos, replace, ifNotExists) {
    private val name: SqlIdentifier
    private val className: SqlNode
    private val usingList: SqlNodeList

    /** Creates a SqlCreateFunction.  */
    init {
        this.name = Objects.requireNonNull(name, "name")
        this.className = className
        this.usingList = Objects.requireNonNull(usingList, "usingList")
        Preconditions.checkArgument(usingList.size() % 2 === 0)
    }

    @Override
    fun unparse(
        writer: SqlWriter, leftPrec: Int,
        rightPrec: Int
    ) {
        writer.keyword(if (getReplace()) "CREATE OR REPLACE" else "CREATE")
        writer.keyword("FUNCTION")
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS")
        }
        name.unparse(writer, 0, 0)
        writer.keyword("AS")
        className.unparse(writer, 0, 0)
        if (usingList.size() > 0) {
            writer.keyword("USING")
            val frame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.SIMPLE)
            for (using in pairs()) {
                writer.sep(",")
                using.left.unparse(writer, 0, 0) // FILE, URL or ARCHIVE
                using.right.unparse(writer, 0, 0) // e.g. 'file:foo/bar.jar'
            }
            writer.endList(frame)
        }
    }

    @SuppressWarnings("unchecked")
    private fun pairs(): List<Pair<SqlLiteral, SqlLiteral>> {
        return Util.pairs(usingList as List)
    }

    @get:Override
    val operator: SqlOperator
        get() = OPERATOR

    @get:Override
    val operandList: List<Any>
        get() = Arrays.asList(name, className, usingList)

    companion object {
        private val OPERATOR: SqlSpecialOperator = SqlSpecialOperator("CREATE FUNCTION", SqlKind.CREATE_FUNCTION)
    }
}
