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
package org.apache.calcite.sql2rel

import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.parser.SqlParseException
import org.apache.calcite.sql.parser.SqlParser

/**
 * Provides context for [InitializerExpressionFactory] methods.
 */
interface InitializerContext {
    val rexBuilder: RexBuilder?

    /**
     * Parse a column computation expression for a table. Usually this expression is declared
     * in the create table statement, i.e.
     * <pre>
     * create table t(
     * a int not null,
     * b varchar(5) as (my_udf(a)) virtual,
     * c int not null as (a + 1)
     * );
    </pre> *
     *
     *
     * You can use the string format expression "my_udf(a)" and "a + 1"
     * as the initializer expression of column b and c.
     *
     *
     * Calcite doesn't really need this now because the DDL nodes
     * can be executed directly from `SqlNode`s, but we still provide the way
     * to initialize from a SQL-like string, because a string can be used to persist easily and
     * the column expressions are important part of the table metadata.
     *
     * @param config parse config
     * @param expr   the SQL-style column expression
     * @return a `SqlNode` instance
     */
    fun parseExpression(config: SqlParser.Config?, expr: String): SqlNode? {
        val parser: SqlParser = SqlParser.create(expr, config)
        return try {
            parser.parseExpression()
        } catch (e: SqlParseException) {
            throw RuntimeException("Failed to parse expression $expr", e)
        }
    }

    /**
     * Validate the expression with a base table row type. The expression may reference the fields
     * of the row type defines.
     *
     * @param rowType the table row type
     * @param expr    the expression
     * @return a validated `SqlNode`, usually it transforms
     * from a `SqlUnresolvedFunction` to a resolved one
     */
    fun validateExpression(rowType: RelDataType?, expr: SqlNode?): SqlNode?

    /**
     * Converts a `SqlNode` to `RexNode`.
     *
     *
     * Caution that the `SqlNode` must be validated,
     * you can use [.validateExpression] to validate if the `SqlNode`
     * is un-validated.
     *
     * @param expr the expression of sql node to convert
     * @return a converted `RexNode` instance
     */
    fun convertExpression(expr: SqlNode?): RexNode?
}
