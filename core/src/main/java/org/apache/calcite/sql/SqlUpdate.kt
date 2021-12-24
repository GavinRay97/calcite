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
 * A `SqlUpdate` is a node of a parse tree which represents an UPDATE
 * statement.
 */
class SqlUpdate(
    pos: SqlParserPos?,
    targetTable: SqlNode,
    targetColumnList: SqlNodeList,
    sourceExpressionList: SqlNodeList,
    @Nullable condition: SqlNode,
    @Nullable sourceSelect: SqlSelect,
    @Nullable alias: SqlIdentifier
) : SqlCall(pos) {
    var targetTable: SqlNode
    var targetColumnList: SqlNodeList
    var sourceExpressionList: SqlNodeList

    @Nullable
    var condition: SqlNode

    @Nullable
    var sourceSelect: SqlSelect

    @Nullable
    var alias: SqlIdentifier

    //~ Constructors -----------------------------------------------------------
    init {
        this.targetTable = targetTable
        this.targetColumnList = targetColumnList
        this.sourceExpressionList = sourceExpressionList
        this.condition = condition
        this.sourceSelect = sourceSelect
        assert(sourceExpressionList.size() === targetColumnList.size())
        this.alias = alias
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val kind: SqlKind
        get() = SqlKind.UPDATE

    @get:Override
    val operator: SqlOperator
        get() = OPERATOR

    @get:Override
    @get:SuppressWarnings("nullness")
    val operandList: List<Any>
        get() = ImmutableNullableList.of(
            targetTable, targetColumnList,
            sourceExpressionList, condition, alias
        )

    @SuppressWarnings("assignment.type.incompatible")
    @Override
    fun setOperand(i: Int, @Nullable operand: SqlNode) {
        when (i) {
            0 -> {
                assert(operand is SqlIdentifier)
                targetTable = operand
            }
            1 -> targetColumnList = operand as SqlNodeList
            2 -> sourceExpressionList = operand as SqlNodeList
            3 -> condition = operand
            4 -> sourceExpressionList = operand as SqlNodeList
            5 -> alias = operand as SqlIdentifier
            else -> throw AssertionError(i)
        }
    }

    /** Returns the identifier for the target table of this UPDATE.  */
    fun getTargetTable(): SqlNode {
        return targetTable
    }

    /** Returns the alias for the target table of this UPDATE.  */
    @Pure
    @Nullable
    fun getAlias(): SqlIdentifier {
        return alias
    }

    fun setAlias(alias: SqlIdentifier) {
        this.alias = alias
    }

    /** Returns the list of target column names.  */
    fun getTargetColumnList(): SqlNodeList {
        return targetColumnList
    }

    /** Returns the list of source expressions.  */
    fun getSourceExpressionList(): SqlNodeList {
        return sourceExpressionList
    }

    /**
     * Gets the filter condition for rows to be updated.
     *
     * @return the condition expression for the data to be updated, or null for
     * all rows in the table
     */
    @Nullable
    fun getCondition(): SqlNode {
        return condition
    }

    /**
     * Gets the source SELECT expression for the data to be updated. Returns
     * null before the statement has been expanded by
     * [SqlValidatorImpl.performUnconditionalRewrites].
     *
     * @return the source SELECT for the data to be updated
     */
    @Nullable
    fun getSourceSelect(): SqlSelect {
        return sourceSelect
    }

    fun setSourceSelect(sourceSelect: SqlSelect) {
        this.sourceSelect = sourceSelect
    }

    @Override
    fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        val frame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "UPDATE", "")
        val opLeft: Int = operator.getLeftPrec()
        val opRight: Int = operator.getRightPrec()
        targetTable.unparse(writer, opLeft, opRight)
        val alias: SqlIdentifier = alias
        if (alias != null) {
            writer.keyword("AS")
            alias.unparse(writer, opLeft, opRight)
        }
        val setFrame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.UPDATE_SET_LIST, "SET", "")
        for (pair in Pair.zip(getTargetColumnList(), getSourceExpressionList())) {
            writer.sep(",")
            val id: SqlIdentifier = pair.left as SqlIdentifier
            id.unparse(writer, opLeft, opRight)
            writer.keyword("=")
            val sourceExp: SqlNode = pair.right
            sourceExp.unparse(writer, opLeft, opRight)
        }
        writer.endList(setFrame)
        val condition: SqlNode = condition
        if (condition != null) {
            writer.sep("WHERE")
            condition.unparse(writer, opLeft, opRight)
        }
        writer.endList(frame)
    }

    @Override
    fun validate(validator: SqlValidator, scope: SqlValidatorScope?) {
        validator.validateUpdate(this)
    }

    companion object {
        val OPERATOR: SqlSpecialOperator = SqlSpecialOperator("UPDATE", SqlKind.UPDATE)
    }
}
