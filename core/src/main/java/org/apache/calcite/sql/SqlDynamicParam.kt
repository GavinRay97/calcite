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
package org.apache.calcite.sql

import org.apache.calcite.sql.parser.SqlParserPos

/**
 * A `SqlDynamicParam` represents a dynamic parameter marker in an
 * SQL statement. The textual order in which dynamic parameters appear within an
 * SQL statement is the only property which distinguishes them, so this 0-based
 * index is recorded as soon as the parameter is encountered.
 */
class SqlDynamicParam     //~ Constructors -----------------------------------------------------------
    (
    //~ Instance fields --------------------------------------------------------
    val index: Int,
    pos: SqlParserPos?
) : SqlNode(pos) {

    //~ Methods ----------------------------------------------------------------
    @Override
    fun clone(pos: SqlParserPos?): SqlNode {
        return SqlDynamicParam(index, pos)
    }

    @get:Override
    val kind: SqlKind
        get() = SqlKind.DYNAMIC_PARAM

    @Override
    fun unparse(
        writer: SqlWriter,
        leftPrec: Int,
        rightPrec: Int
    ) {
        writer.dynamicParam(index)
    }

    @Override
    fun validate(validator: SqlValidator, scope: SqlValidatorScope?) {
        validator.validateDynamicParam(this)
    }

    @Override
    fun getMonotonicity(@Nullable scope: SqlValidatorScope?): SqlMonotonicity {
        return SqlMonotonicity.CONSTANT
    }

    @Override
    fun <R> accept(visitor: SqlVisitor<R>): R {
        return visitor.visit(this)
    }

    @Override
    fun equalsDeep(@Nullable node: SqlNode, litmus: Litmus): Boolean {
        if (node !is SqlDynamicParam) {
            return litmus.fail("{} != {}", this, node)
        }
        val that = node as SqlDynamicParam
        return if (index != that.index) {
            litmus.fail("{} != {}", this, node)
        } else litmus.succeed()
    }
}
