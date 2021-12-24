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
 * The WITH clause of a query. It wraps a SELECT, UNION, or INTERSECT.
 */
class SqlWith(pos: SqlParserPos?, withList: SqlNodeList, body: SqlNode) : SqlCall(pos) {
    var withList: SqlNodeList
    var body: SqlNode

    //~ Constructors -----------------------------------------------------------
    init {
        this.withList = withList
        this.body = body
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val kind: SqlKind
        get() = SqlKind.WITH

    @get:Override
    val operator: SqlOperator
        get() = SqlWithOperator.INSTANCE

    @get:Override
    val operandList: List<Any>
        get() = ImmutableList.of(withList, body)

    @SuppressWarnings("assignment.type.incompatible")
    @Override
    fun setOperand(i: Int, @Nullable operand: SqlNode) {
        when (i) {
            0 -> withList = operand as SqlNodeList
            1 -> body = operand
            else -> throw AssertionError(i)
        }
    }

    @Override
    fun validate(
        validator: SqlValidator,
        scope: SqlValidatorScope?
    ) {
        validator.validateWith(this, scope)
    }

    /**
     * SqlWithOperator is used to represent a WITH clause of a query. It wraps
     * a SELECT, UNION, or INTERSECT.
     */
    private class SqlWithOperator private constructor() : SqlSpecialOperator("WITH", SqlKind.WITH, 2) {
        //~ Methods ----------------------------------------------------------------
        @Override
        fun unparse(
            writer: SqlWriter,
            call: SqlCall,
            leftPrec: Int,
            rightPrec: Int
        ) {
            val with = call as SqlWith
            val frame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.WITH, "WITH", "")
            val frame1: SqlWriter.Frame = writer.startList("", "")
            for (node in with.withList) {
                writer.sep(",")
                node.unparse(writer, 0, 0)
            }
            writer.endList(frame1)
            val frame2: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.SIMPLE)
            with.body.unparse(writer, 100, 100)
            writer.endList(frame2)
            writer.endList(frame)
        }

        @SuppressWarnings("argument.type.incompatible")
        @Override
        fun createCall(
            @Nullable functionQualifier: SqlLiteral?,
            pos: SqlParserPos?, @Nullable vararg operands: SqlNode
        ): SqlCall {
            return SqlWith(pos, operands[0] as SqlNodeList, operands[1])
        }

        @Override
        fun validateCall(
            call: SqlCall?,
            validator: SqlValidator,
            scope: SqlValidatorScope?,
            operandScope: SqlValidatorScope?
        ) {
            validator.validateWith(call as SqlWith?, scope)
        }

        companion object {
            val INSTANCE = SqlWithOperator()
        }
    }
}
