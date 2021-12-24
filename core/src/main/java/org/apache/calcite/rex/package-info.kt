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
 * Provides a language for representing row-expressions.
 *
 * <h2>Life-cycle</h2>
 *
 *
 * A [org.apache.calcite.sql2rel.SqlToRelConverter] converts a SQL
 * parse tree consisting of [org.apache.calcite.sql.SqlNode] objects into
 * a relational expression ([org.apache.calcite.rel.RelNode]). Several
 * kinds of nodes in this tree have row expressions
 * ([org.apache.calcite.rex.RexNode]).
 *
 *
 * After the relational expression has been optimized, a
 * [org.apache.calcite.adapter.enumerable.JavaRelImplementor] converts it
 * into to a plan. If the plan is a Java parse tree, row-expressions are
 * translated into equivalent Java expressions.
 *
 * <h2>Expressions</h2>
 *
 *
 *
 * Every row-expression has a type. (Compare with
 * [org.apache.calcite.sql.SqlNode], which is created before validation,
 * and therefore types may not be available.)
 *
 *
 * Every node in the parse tree is a [org.apache.calcite.rex.RexNode].
 * Sub-types are:
 *
 *  * [org.apache.calcite.rex.RexLiteral] represents a boolean,
 * numeric, string, or date constant, or the value `NULL`.
 *
 *  * [org.apache.calcite.rex.RexVariable] represents a leaf of the
 * tree. It has sub-types:
 *
 *  * [org.apache.calcite.rex.RexCorrelVariable] is a
 * correlating variable for nested-loop joins
 *
 *  * [org.apache.calcite.rex.RexInputRef] refers to a field
 * of an input relational expression
 *
 *  * [org.apache.calcite.rex.RexCall] is a call to an
 * operator or function.  By means of special operators, we can
 * use this construct to represent virtually every non-leaf node
 * in the tree.
 *
 *  * [org.apache.calcite.rex.RexRangeRef] refers to a
 * collection of contiguous fields from an input relational
 * expression. It usually exists only during translation.
 *
 *
 *
 *
 *
 *
 * Expressions are generally
 * created using a [org.apache.calcite.rex.RexBuilder] factory.
 *
 * <h2>Related packages</h2>
 *
 *  * [org.apache.calcite.sql] SQL object model
 *
 *  * [org.apache.calcite.plan] Core classes, including
 * [org.apache.calcite.rel.type.RelDataType] and
 * [org.apache.calcite.rel.type.RelDataTypeFactory].
 *
 *
 */
package org.apache.calcite.rex

import org.checkerframework.framework.qual.DefaultQualifier
import org.checkerframework.framework.qual.TypeUseLocation
