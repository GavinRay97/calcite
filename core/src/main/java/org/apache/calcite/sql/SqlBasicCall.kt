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
import org.apache.calcite.util.ImmutableNullableList
import java.util.List
import java.util.Objects
import org.apache.calcite.linq4j.Nullness.castNonNull

/**
 * Implementation of [SqlCall] that keeps its operands in an array.
 */
class SqlBasicCall(
    operator: SqlOperator?,
    operandList: List<SqlNode?>?,
    pos: SqlParserPos?,
    @Nullable functionQualifier: SqlLiteral?
) : SqlCall(pos) {
    private override var operator: SqlOperator? = null
    private override var operandList: List<SqlNode>? = null

    @Nullable
    private override val functionQuantifier: SqlLiteral?

    @Deprecated // to be removed before 2.0
    constructor(
        operator: SqlOperator?,
        @Nullable operands: Array<SqlNode?>?,
        pos: SqlParserPos?
    ) : this(operator, ImmutableNullableList.copyOf(operands), pos, null) {
    }

    /** Creates a SqlBasicCall.
     *
     *
     * It is not expanded; call [withExpanded(true)][.withExpanded]
     * to expand.  */
    constructor(
        operator: SqlOperator?,
        operandList: List<SqlNode?>?,
        pos: SqlParserPos?
    ) : this(operator, operandList, pos, null) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        operator: SqlOperator?,
        @Nullable operands: Array<SqlNode?>?,
        pos: SqlParserPos?,
        @Nullable functionQualifier: SqlLiteral?
    ) : this(
        operator, ImmutableNullableList.copyOf(operands), pos,
        functionQualifier
    ) {
    }

    /** Creates a SqlBasicCall with an optional function qualifier.
     *
     *
     * It is not expanded; call [withExpanded(true)][.withExpanded]
     * to expand.  */
    init {
        this.operator = Objects.requireNonNull(operator, "operator")
        this.operandList = ImmutableNullableList.copyOf(operandList)
        functionQuantifier = functionQualifier
    }

    @get:Override
    override val kind: SqlKind
        get() = operator.getKind()

    /** Sets whether this call is expanded.
     *
     * @see .isExpanded
     */
    fun withExpanded(expanded: Boolean): SqlCall {
        return if (!expanded) this else ExpandedBasicCall(
            operator, operandList, pos,
            functionQuantifier
        )
    }

    @Override
    override fun setOperand(i: Int, @Nullable operand: SqlNode) {
        operandList = set<Any>(operandList, i, operand)
    }

    /** Sets the operator (or function) that is being called.
     *
     *
     * This method is used by the validator to set a more refined version of
     * the same operator (for instance, a version where overloading has been
     * resolved); use with care.  */
    fun setOperator(operator: SqlOperator?) {
        this.operator = Objects.requireNonNull(operator, "operator")
    }

    @Override
    fun getOperator(): SqlOperator? {
        return operator
    }

    @SuppressWarnings("nullness")
    @Override
    fun getOperandList(): List<SqlNode>? {
        return operandList
    }

    @SuppressWarnings("unchecked")
    @Override
    override fun <S : SqlNode?> operand(i: Int): S {
        return castNonNull(operandList!![i]) as S
    }

    @Override
    override fun operandCount(): Int {
        return operandList!!.size()
    }

    @Override
    @Nullable
    fun getFunctionQuantifier(): SqlLiteral? {
        return functionQuantifier
    }

    @Override
    override fun clone(pos: SqlParserPos?): SqlNode {
        return getOperator().createCall(getFunctionQuantifier(), pos, operandList)
    }

    /** Sub-class of [org.apache.calcite.sql.SqlBasicCall]
     * for which [.isExpanded] returns true.  */
    private class ExpandedBasicCall internal constructor(
        operator: SqlOperator?,
        operandList: List<SqlNode?>?, pos: SqlParserPos?,
        @Nullable functionQualifier: SqlLiteral?
    ) : SqlBasicCall(operator, operandList, pos, functionQualifier) {
        @Override
        fun isExpanded(): Boolean {
            return true
        }

        @Override
        override fun withExpanded(expanded: Boolean): SqlCall {
            return if (expanded) this else SqlBasicCall(
                getOperator(), getOperandList(), pos,
                getFunctionQuantifier()
            )
        }
    }

    companion object {
        /** Sets the `i`th element of `list` to value `e`, creating
         * an immutable copy of the list.  */
        private operator fun <E> set(list: List<E>?, i: Int, @Nullable e: E): List<E> {
            if (i == 0 && list!!.size() === 1) {
                // short-cut case where the contents of the previous list can be ignored
                return ImmutableNullableList.of(e)
            }
            @Nullable val objects = list.toArray() as Array<E>
            objects[i] = e
            return ImmutableNullableList.copyOf(objects)
        }
    }
}
