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
import org.apache.calcite.sql.SqlNodeList
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.SqlSpecialOperator
import org.apache.calcite.sql.SqlWriter
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.util.ImmutableNullableList
import org.apache.calcite.util.Pair
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import java.util.AbstractList
import java.util.List
import java.util.Objects
import org.apache.calcite.linq4j.Nullness.castNonNull

/**
 * Parse tree for `CREATE FOREIGN SCHEMA` statement.
 */
class SqlCreateForeignSchema internal constructor(
    pos: SqlParserPos?, replace: Boolean, ifNotExists: Boolean,
    name: SqlIdentifier?, @Nullable type: SqlNode?, @Nullable library: SqlNode?,
    @Nullable optionList: SqlNodeList?
) : SqlCreate(OPERATOR, pos, replace, ifNotExists) {
    val name: SqlIdentifier

    @Nullable
    val type: SqlNode?

    @Nullable
    val library: SqlNode?

    @Nullable
    private val optionList: SqlNodeList?

    /** Creates a SqlCreateForeignSchema.  */
    init {
        this.name = Objects.requireNonNull(name, "name")
        this.type = type
        this.library = library
        Preconditions.checkArgument(
            type == null != (library == null),
            "of type and library, exactly one must be specified"
        )
        this.optionList = optionList // may be null
    }

    @get:Override
    @get:SuppressWarnings("nullness")
    val operandList: List<Any>
        get() = ImmutableNullableList.of(name, type, library, optionList)

    @Override
    fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        if (getReplace()) {
            writer.keyword("CREATE OR REPLACE")
        } else {
            writer.keyword("CREATE")
        }
        writer.keyword("FOREIGN SCHEMA")
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS")
        }
        name.unparse(writer, leftPrec, rightPrec)
        if (library != null) {
            writer.keyword("LIBRARY")
            library.unparse(writer, 0, 0)
        }
        if (type != null) {
            writer.keyword("TYPE")
            type.unparse(writer, 0, 0)
        }
        if (optionList != null) {
            writer.keyword("OPTIONS")
            val frame: SqlWriter.Frame = writer.startList("(", ")")
            var i = 0
            for (c in options()) {
                if (i++ > 0) {
                    writer.sep(",")
                }
                c.left.unparse(writer, 0, 0)
                c.right.unparse(writer, 0, 0)
            }
            writer.endList(frame)
        }
    }

    /** Returns options as a list of (name, value) pairs.  */
    fun options(): List<Pair<SqlIdentifier, SqlNode>> {
        return options(optionList)
    }

    companion object {
        private val OPERATOR: SqlOperator = SqlSpecialOperator(
            "CREATE FOREIGN SCHEMA",
            SqlKind.CREATE_FOREIGN_SCHEMA
        )

        private fun options(
            @Nullable optionList: SqlNodeList?
        ): List<Pair<SqlIdentifier, SqlNode>> {
            return if (optionList == null) {
                ImmutableList.of()
            } else object : AbstractList<Pair<SqlIdentifier?, SqlNode?>?>() {
                @Override
                operator fun get(index: Int): Pair<SqlIdentifier, SqlNode> {
                    return Pair.of(
                        castNonNull(optionList.get(index * 2)) as SqlIdentifier?,
                        castNonNull(optionList.get(index * 2 + 1))
                    )
                }

                @Override
                fun size(): Int {
                    return optionList.size() / 2
                }
            }
        }
    }
}
