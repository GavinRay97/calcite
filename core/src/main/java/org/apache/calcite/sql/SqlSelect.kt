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
 * A `SqlSelect` is a node of a parse tree which represents a select
 * statement. It warrants its own node type just because we have a lot of
 * methods to put somewhere.
 */
class SqlSelect(
    pos: SqlParserPos?,
    @Nullable keywordList: SqlNodeList?,
    selectList: SqlNodeList?,
    @Nullable from: SqlNode?,
    @Nullable where: SqlNode?,
    @Nullable groupBy: SqlNodeList?,
    @Nullable having: SqlNode?,
    @Nullable windowDecls: SqlNodeList?,
    @Nullable orderBy: SqlNodeList?,
    @Nullable offset: SqlNode?,
    @Nullable fetch: SqlNode?,
    @Nullable hints: SqlNodeList?
) : SqlCall(pos) {
    var keywordList: SqlNodeList
    var selectList: SqlNodeList

    @Nullable
    var from: SqlNode?

    @Nullable
    var where: SqlNode?

    @Nullable
    var groupBy: SqlNodeList?

    @Nullable
    var having: SqlNode?
    var windowDecls: SqlNodeList

    @Nullable
    var orderBy: SqlNodeList?

    @Nullable
    var offset: SqlNode?

    @Nullable
    var fetch: SqlNode?

    @Nullable
    var hints: SqlNodeList?

    //~ Constructors -----------------------------------------------------------
    init {
        this.keywordList = requireNonNull(if (keywordList != null) keywordList else SqlNodeList(pos))
        this.selectList = requireNonNull(selectList, "selectList")
        this.from = from
        this.where = where
        this.groupBy = groupBy
        this.having = having
        this.windowDecls = requireNonNull(if (windowDecls != null) windowDecls else SqlNodeList(pos))
        this.orderBy = orderBy
        this.offset = offset
        this.fetch = fetch
        this.hints = hints
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val operator: org.apache.calcite.sql.SqlOperator
        get() = SqlSelectOperator.INSTANCE

    @get:Override
    val kind: SqlKind
        get() = SqlKind.SELECT

    @get:Override
    @get:SuppressWarnings("nullness")
    val operandList: List<Any>
        get() = ImmutableNullableList.of(
            keywordList, selectList, from, where,
            groupBy, having, windowDecls, orderBy, offset, fetch, hints
        )

    @Override
    fun setOperand(i: Int, @Nullable operand: SqlNode?) {
        when (i) {
            0 -> keywordList = requireNonNull(operand as SqlNodeList?)
            1 -> selectList = requireNonNull(operand as SqlNodeList?)
            2 -> from = operand
            3 -> where = operand
            4 -> groupBy = operand as SqlNodeList?
            5 -> having = operand
            6 -> windowDecls = requireNonNull(operand as SqlNodeList?)
            7 -> orderBy = operand as SqlNodeList?
            8 -> offset = operand
            9 -> fetch = operand
            else -> throw AssertionError(i)
        }
    }

    val isDistinct: Boolean
        get() = getModifierNode(SqlSelectKeyword.DISTINCT) != null

    @Nullable
    fun getModifierNode(modifier: SqlSelectKeyword): SqlNode? {
        for (keyword in keywordList) {
            val keyword2: SqlSelectKeyword = (keyword as SqlLiteral).symbolValue(SqlSelectKeyword::class.java)
            if (keyword2 === modifier) {
                return keyword
            }
        }
        return null
    }

    @Pure
    @Nullable
    fun getFrom(): SqlNode? {
        return from
    }

    fun setFrom(@Nullable from: SqlNode?) {
        this.from = from
    }

    @get:Nullable
    @get:Pure
    val group: SqlNodeList?
        get() = groupBy

    fun setGroupBy(@Nullable groupBy: SqlNodeList?) {
        this.groupBy = groupBy
    }

    @Pure
    @Nullable
    fun getHaving(): SqlNode? {
        return having
    }

    fun setHaving(@Nullable having: SqlNode?) {
        this.having = having
    }

    @Pure
    fun getSelectList(): SqlNodeList {
        return selectList
    }

    fun setSelectList(selectList: SqlNodeList) {
        this.selectList = selectList
    }

    @Pure
    @Nullable
    fun getWhere(): SqlNode? {
        return where
    }

    fun setWhere(@Nullable whereClause: SqlNode?) {
        where = whereClause
    }

    val windowList: SqlNodeList
        get() = windowDecls

    @get:Nullable
    @get:Pure
    val orderList: SqlNodeList?
        get() = orderBy

    fun setOrderBy(@Nullable orderBy: SqlNodeList?) {
        this.orderBy = orderBy
    }

    @Pure
    @Nullable
    fun getOffset(): SqlNode? {
        return offset
    }

    fun setOffset(@Nullable offset: SqlNode?) {
        this.offset = offset
    }

    @Pure
    @Nullable
    fun getFetch(): SqlNode? {
        return fetch
    }

    fun setFetch(@Nullable fetch: SqlNode?) {
        this.fetch = fetch
    }

    fun setHints(@Nullable hints: SqlNodeList?) {
        this.hints = hints
    }

    @Pure
    @Nullable
    fun getHints(): SqlNodeList? {
        return hints
    }

    @EnsuresNonNullIf(expression = "hints", result = true)
    fun hasHints(): Boolean {
        // The hints may be passed as null explicitly.
        return hints != null && hints.size() > 0
    }

    @Override
    fun validate(validator: SqlValidator, scope: SqlValidatorScope?) {
        validator.validateQuery(this, scope, validator.getUnknownType())
    }

    // Override SqlCall, to introduce a sub-query frame.
    @Override
    fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        if (!writer.inQuery()) {
            // If this SELECT is the topmost item in a sub-query, introduce a new
            // frame. (The topmost item in the sub-query might be a UNION or
            // ORDER. In this case, we don't need a wrapper frame.)
            val frame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.SUB_QUERY, "(", ")")
            writer.getDialect().unparseCall(writer, this, 0, 0)
            writer.endList(frame)
        } else {
            writer.getDialect().unparseCall(writer, this, leftPrec, rightPrec)
        }
    }

    fun hasOrderBy(): Boolean {
        return orderBy != null && orderBy.size() !== 0
    }

    fun hasWhere(): Boolean {
        return where != null
    }

    fun isKeywordPresent(targetKeyWord: SqlSelectKeyword): Boolean {
        return getModifierNode(targetKeyWord) != null
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        // constants representing operand positions
        const val FROM_OPERAND = 2
        const val WHERE_OPERAND = 3
        const val HAVING_OPERAND = 5
    }
}
