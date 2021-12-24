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
package org.apache.calcite.tools

import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.parser.SqlParseException
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.parser.SqlParserUtil
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.sql.util.SqlShuttle
import com.google.common.base.Preconditions
import com.google.common.collect.ImmutableList
import com.google.common.collect.Lists
import org.immutables.value.Value
import java.sql.PreparedStatement
import java.util.ArrayList
import java.util.List
import java.util.Objects
import java.util.function.Function

/**
 * Utility that extracts constants from a SQL query.
 *
 *
 * Simple use:
 *
 * <blockquote>`
 * final String sql =<br></br>
 * "select 'x' from emp where deptno < 10";<br></br>
 * final Hoist.Hoisted hoisted =<br></br>
 * Hoist.create(Hoist.config()).hoist();<br></br>
 * print(hoisted); // "select ?0 from emp where deptno < ?1"
`</blockquote> *
 *
 *
 * Calling [Hoisted.toString] generates a string that is similar to
 * SQL where a user has manually converted all constants to bind variables, and
 * which could then be executed using [PreparedStatement.execute].
 * That is not a goal of this utility, but see
 * [[CALCITE-963]
 * Hoist literals](https://issues.apache.org/jira/browse/CALCITE-963).
 *
 *
 * For more advanced formatting, use [Hoisted.substitute].
 *
 *
 * Adjust [Config] to use a different parser or parsing options.
 */
@Value.Enclosing
class Hoist private constructor(config: Config) {
    private val config: Config

    init {
        this.config = Objects.requireNonNull(config, "config")
    }

    /** Hoists literals in a given SQL string, returning a [Hoisted].  */
    fun hoist(sql: String): Hoisted {
        val variables: List<Variable> = ArrayList()
        val parser: SqlParser = SqlParser.create(sql, config.parserConfig())
        val node: SqlNode
        node = try {
            parser.parseQuery()
        } catch (e: SqlParseException) {
            throw RuntimeException(e)
        }
        node.accept(object : SqlShuttle() {
            @Override
            @Nullable
            fun visit(literal: SqlLiteral): SqlNode {
                variables.add(Variable(sql, variables.size(), literal))
                return super.visit(literal)
            }
        })
        return Hoisted(sql, variables)
    }

    /** Configuration.  */
    @Value.Immutable(singleton = false)
    interface Config {
        /** Returns the configuration for the SQL parser.  */
        fun parserConfig(): SqlParser.Config?

        /** Sets [.parserConfig].  */
        fun withParserConfig(parserConfig: SqlParser.Config?): Config?
    }

    /** Variable.  */
    class Variable(originalSql: String, ordinal: Int, node: SqlNode) {
        /** Original SQL of whole statement.  */
        val originalSql: String

        /** Zero-based ordinal in statement.  */
        val ordinal: Int

        /** Parse tree node (typically a literal).  */
        val node: SqlNode

        /** Zero-based position within the SQL text of start of node.  */
        val start: Int

        /** Zero-based position within the SQL text after end of node.  */
        val end: Int

        init {
            this.originalSql = Objects.requireNonNull(originalSql, "originalSql")
            this.ordinal = ordinal
            this.node = Objects.requireNonNull(node, "node")
            val pos: SqlParserPos = node.getParserPosition()
            start = SqlParserUtil.lineColToIndex(
                originalSql,
                pos.getLineNum(), pos.getColumnNum()
            )
            end = SqlParserUtil.lineColToIndex(
                originalSql,
                pos.getEndLineNum(), pos.getEndColumnNum()
            ) + 1
            Preconditions.checkArgument(ordinal >= 0)
            Preconditions.checkArgument(start >= 0)
            Preconditions.checkArgument(start <= end)
            Preconditions.checkArgument(end <= originalSql.length())
        }

        /** Returns SQL text of the region of the statement covered by this
         * Variable.  */
        fun sql(): String {
            return originalSql.substring(start, end)
        }
    }

    /** Result of hoisting.  */
    class Hoisted internal constructor(val originalSql: String, variables: List<Variable?>?) {
        val variables: List<Variable>

        init {
            this.variables = ImmutableList.copyOf(variables)
        }

        @Override
        override fun toString(): String {
            return substitute(Function<Variable, String> { v: Variable -> ordinalString(v) })
        }

        /** Returns the SQL string with variables replaced according to the
         * given substitution function.  */
        fun substitute(fn: Function<Variable?, String?>): String {
            val b = StringBuilder(originalSql)
            for (variable in Lists.reverse(variables)) {
                val s: String = fn.apply(variable)
                b.replace(variable.start, variable.end, s)
            }
            return b.toString()
        }
    }

    companion object {
        /** Creates a Config.  */
        fun config(): Config {
            return ImmutableHoist.Config.builder()
                .withParserConfig(SqlParser.config())
                .build()
        }

        /** Creates a Hoist.  */
        fun create(config: Config): Hoist {
            return Hoist(config)
        }

        /** Converts a [Variable] to a string "?N",
         * where N is the [Variable.ordinal].  */
        fun ordinalString(v: Variable): String {
            return "?" + v.ordinal
        }

        /** Converts a [Variable] to a string "?N",
         * where N is the [Variable.ordinal],
         * if the fragment is a character literal. Other fragments are unchanged.  */
        fun ordinalStringIfChar(v: Variable): String {
            return if (v.node is SqlLiteral
                && (v.node as SqlLiteral).getTypeName() === SqlTypeName.CHAR
            ) {
                "?" + v.ordinal
            } else {
                v.sql()
            }
        }
    }
}
