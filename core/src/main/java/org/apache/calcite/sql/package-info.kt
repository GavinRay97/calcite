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
/**
 * Provides a SQL parser and object model.
 *
 *
 * This package, and the dependent `org.apache.calcite.sql.parser`
 * package, are independent of the other Calcite packages, so may be used
 * standalone.
 *
 * <h2>Parser</h2>
 *
 *
 * [org.apache.calcite.sql.parser.SqlParser] parses a SQL string to a
 * parse tree. It only performs the most basic syntactic validation.
 *
 * <h2>Object model</h2>
 *
 *
 * Every node in the parse tree is a [org.apache.calcite.sql.SqlNode].
 * Sub-types are:
 *
 *
 *  * [org.apache.calcite.sql.SqlLiteral] represents a boolean,
 * numeric, string, or date constant, or the value `NULL`.
 *
 *
 *  * [org.apache.calcite.sql.SqlIdentifier] represents an
 * identifier, such as ` EMPNO` or `emp.deptno`.
 *
 *
 *  * [org.apache.calcite.sql.SqlCall] is a call to an operator or
 * function.  By means of special operators, we can use this construct
 * to represent virtually every non-leaf node in the tree. For example,
 * a `select` statement is a call to the 'select'
 * operator.
 *
 *  * [org.apache.calcite.sql.SqlNodeList] is a list of nodes.
 *
 *
 *
 *
 * A [org.apache.calcite.sql.SqlOperator] describes the behavior of a
 * node in the tree, such as how to un-parse a
 * [org.apache.calcite.sql.SqlCall] into a SQL string.  It is
 * important to note that operators are metadata, not data: there is only
 * one `SqlOperator` instance representing the '=' operator, even
 * though there may be many calls to it.
 *
 *
 * `SqlOperator` has several derived classes which make it easy to
 * define new operators: [org.apache.calcite.sql.SqlFunction],
 * [org.apache.calcite.sql.SqlBinaryOperator],
 * [org.apache.calcite.sql.SqlPrefixOperator],
 * [org.apache.calcite.sql.SqlPostfixOperator].
 * And there are singleton classes for special syntactic constructs
 * [org.apache.calcite.sql.SqlSelectOperator]
 * and [org.apache.calcite.sql.SqlJoin.SqlJoinOperator]. (These
 * special operators even have their own sub-types of
 * [org.apache.calcite.sql.SqlCall]:
 * [org.apache.calcite.sql.SqlSelect] and
 * [org.apache.calcite.sql.SqlJoin].)
 *
 *
 * A [org.apache.calcite.sql.SqlOperatorTable] is a collection of
 * operators. By supplying your own operator table, you can customize the
 * dialect of SQL without modifying the parser.
 *
 * <h2>Validation</h2>
 *
 *
 * [org.apache.calcite.sql.validate.SqlValidator] checks that
 * a tree of [org.apache.calcite.sql.SqlNode]s is
 * semantically valid. You supply a
 * [org.apache.calcite.sql.SqlOperatorTable] to describe the available
 * functions and operators, and a
 * [org.apache.calcite.sql.validate.SqlValidatorCatalogReader] for
 * access to the database's catalog.
 *
 * <h2>Generating SQL</h2>
 *
 *
 * A [org.apache.calcite.sql.SqlWriter] converts a tree of
 * [org.apache.calcite.sql.SqlNode]s into a SQL string. A
 * [org.apache.calcite.sql.SqlDialect] defines how this happens.
 */
package org.apache.calcite.sql

import org.checkerframework.framework.qual.DefaultQualifier
import org.checkerframework.framework.qual.TypeUseLocation
