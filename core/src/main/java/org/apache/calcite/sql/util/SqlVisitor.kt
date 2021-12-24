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
package org.apache.calcite.sql.util

import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlDataTypeSpec
import org.apache.calcite.sql.SqlDynamicParam
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlNodeList
import org.apache.calcite.sql.SqlOperator

/**
 * Visitor class, follows the
 * [visitor pattern][org.apache.calcite.util.Glossary.VISITOR_PATTERN].
 *
 *
 * The type parameter `R` is the return type of each `
 * visit()` method. If the methods do not need to return a value, use
 * [Void].
 *
 * @see SqlBasicVisitor
 *
 * @see SqlNode.accept
 * @see SqlOperator.acceptCall
 *
 *
 * @param <R> Return type
</R> */
interface SqlVisitor<R> {
    //~ Methods ----------------------------------------------------------------
    /**
     * Visits a literal.
     *
     * @param literal Literal
     * @see SqlLiteral.accept
     */
    fun visit(literal: SqlLiteral?): R

    /**
     * Visits a call to a [SqlOperator].
     *
     * @param call Call
     * @see SqlCall.accept
     */
    fun visit(call: SqlCall?): R

    /**
     * Visits a list of [SqlNode] objects.
     *
     * @param nodeList list of nodes
     * @see SqlNodeList.accept
     */
    fun visit(nodeList: SqlNodeList?): R

    /**
     * Visits an identifier.
     *
     * @param id identifier
     * @see SqlIdentifier.accept
     */
    fun visit(id: SqlIdentifier?): R

    /**
     * Visits a datatype specification.
     *
     * @param type datatype specification
     * @see SqlDataTypeSpec.accept
     */
    fun visit(type: SqlDataTypeSpec?): R

    /**
     * Visits a dynamic parameter.
     *
     * @param param Dynamic parameter
     * @see SqlDynamicParam.accept
     */
    fun visit(param: SqlDynamicParam?): R

    /**
     * Visits an interval qualifier.
     *
     * @param intervalQualifier Interval qualifier
     * @see SqlIntervalQualifier.accept
     */
    fun visit(intervalQualifier: SqlIntervalQualifier?): R
}
