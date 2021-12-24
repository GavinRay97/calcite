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

import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexRangeRef
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlSelect
import org.apache.calcite.sql.validate.SqlValidator

/**
 * Contains the context necessary for a [SqlRexConvertlet] to convert a
 * [SqlNode] expression into a [RexNode].
 */
interface SqlRexContext {
    //~ Methods ----------------------------------------------------------------
    /**
     * Converts an expression from [SqlNode] to [RexNode] format.
     *
     * @param expr Expression to translate
     * @return Converted expression
     */
    fun convertExpression(expr: SqlNode?): RexNode?

    /**
     * If the operator call occurs in an aggregate query, returns the number of
     * columns in the GROUP BY clause. For example, for "SELECT count(*) FROM emp
     * GROUP BY deptno, gender", returns 2.
     * If the operator call occurs in window aggregate query, then returns 1 if
     * the window is guaranteed to be non-empty, or 0 if the window might be
     * empty.
     *
     *
     * Returns 0 if the query is implicitly "GROUP BY ()" because of an
     * aggregate expression. For example, "SELECT sum(sal) FROM emp".
     *
     *
     * Returns -1 if the query is not an aggregate query.
     * @return 0 if the query is implicitly GROUP BY (), -1 if the query is not
     * and aggregate query
     * @see org.apache.calcite.sql.SqlOperatorBinding.getGroupCount
     */
    val groupCount: Int

    /**
     * Returns the [RexBuilder] to use to create [RexNode] objects.
     */
    val rexBuilder: RexBuilder

    /**
     * Returns the expression used to access a given IN or EXISTS
     * [sub-query][SqlSelect].
     *
     * @param call IN or EXISTS expression
     * @return Expression used to access current row of sub-query
     */
    fun getSubQueryExpr(call: SqlCall?): RexRangeRef?

    /**
     * Returns the type factory.
     */
    val typeFactory: RelDataTypeFactory

    /**
     * Returns the factory which supplies default values for INSERT, UPDATE, and
     * NEW.
     */
    val initializerExpressionFactory: InitializerExpressionFactory?

    /**
     * Returns the validator.
     */
    val validator: SqlValidator

    /**
     * Converts a literal.
     */
    fun convertLiteral(literal: SqlLiteral?): RexNode?
}
