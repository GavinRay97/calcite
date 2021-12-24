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
 * A `SqlMerge` is a node of a parse tree which represents a MERGE
 * statement.
 */
class SqlMerge(
    pos: SqlParserPos?,
    targetTable: SqlNode,
    condition: SqlNode,
    source: SqlNode,
    @Nullable updateCall: SqlUpdate,
    @Nullable insertCall: SqlInsert,
    @Nullable sourceSelect: SqlSelect,
    @Nullable alias: SqlIdentifier
) : SqlCall(pos) {
    var targetTable: SqlNode
    var condition: SqlNode
    var source: SqlNode

    @Nullable
    var updateCall: SqlUpdate

    @Nullable
    var insertCall: SqlInsert

    @Nullable
    var sourceSelect: SqlSelect

    @Nullable
    var alias: SqlIdentifier

    //~ Constructors -----------------------------------------------------------
    init {
        this.targetTable = targetTable
        this.condition = condition
        this.source = source
        this.updateCall = updateCall
        this.insertCall = insertCall
        this.sourceSelect = sourceSelect
        this.alias = alias
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val operator: SqlOperator
        get() = OPERATOR

    @get:Override
    val kind: org.apache.calcite.sql.SqlKind
        get() = SqlKind.MERGE

    @get:Override
    @get:SuppressWarnings("nullness")
    val operandList: List<org.apache.calcite.sql.SqlNode>
        get() = ImmutableNullableList.of(
            targetTable, condition, source, updateCall,
            insertCall, sourceSelect, alias
        )

    @SuppressWarnings("assignment.type.incompatible")
    @Override
    fun setOperand(i: Int, @Nullable operand: SqlNode) {
        when (i) {
            0 -> {
                assert(operand is SqlIdentifier)
                targetTable = operand
            }
            1 -> condition = operand
            2 -> source = operand
            3 -> updateCall = operand as SqlUpdate
            4 -> insertCall = operand as SqlInsert
            5 -> sourceSelect = operand as SqlSelect
            6 -> alias = operand as SqlIdentifier
            else -> throw AssertionError(i)
        }
    }

    /** Return the identifier for the target table of this MERGE.  */
    fun getTargetTable(): SqlNode {
        return targetTable
    }

    /** Returns the alias for the target table of this MERGE.  */
    @Pure
    @Nullable
    fun getAlias(): SqlIdentifier {
        return alias
    }

    /** Returns the source query of this MERGE.  */
    var sourceTableRef: org.apache.calcite.sql.SqlNode
        get() = source
        set(tableRef) {
            source = tableRef
        }

    /** Returns the UPDATE statement for this MERGE.  */
    @Nullable
    fun getUpdateCall(): SqlUpdate {
        return updateCall
    }

    /** Returns the INSERT statement for this MERGE.  */
    @Nullable
    fun getInsertCall(): SqlInsert {
        return insertCall
    }

    /** Returns the condition expression to determine whether to UPDATE or
     * INSERT.  */
    fun getCondition(): SqlNode {
        return condition
    }

    /**
     * Gets the source SELECT expression for the data to be updated/inserted.
     * Returns null before the statement has been expanded by
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
        val frame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "MERGE INTO", "")
        val opLeft: Int = operator.getLeftPrec()
        val opRight: Int = operator.getRightPrec()
        targetTable.unparse(writer, opLeft, opRight)
        val alias: SqlIdentifier = alias
        if (alias != null) {
            writer.keyword("AS")
            alias.unparse(writer, opLeft, opRight)
        }
        writer.newlineAndIndent()
        writer.keyword("USING")
        source.unparse(writer, opLeft, opRight)
        writer.newlineAndIndent()
        writer.keyword("ON")
        condition.unparse(writer, opLeft, opRight)
        val updateCall: SqlUpdate = updateCall
        if (updateCall != null) {
            writer.newlineAndIndent()
            writer.keyword("WHEN MATCHED THEN UPDATE")
            val setFrame: SqlWriter.Frame = writer.startList(
                SqlWriter.FrameTypeEnum.UPDATE_SET_LIST,
                "SET",
                ""
            )
            for (pair in Pair.zip(
                updateCall.targetColumnList, updateCall.sourceExpressionList
            )) {
                writer.sep(",")
                pair.left.unparse(writer, opLeft, opRight)
                writer.keyword("=")
                val sourceExp: SqlNode = pair.right
                sourceExp.unparse(writer, opLeft, opRight)
            }
            writer.endList(setFrame)
        }
        val insertCall: SqlInsert = insertCall
        if (insertCall != null) {
            writer.newlineAndIndent()
            writer.keyword("WHEN NOT MATCHED THEN INSERT")
            val targetColumnList: SqlNodeList = insertCall.getTargetColumnList()
            if (targetColumnList != null) {
                targetColumnList.unparse(writer, opLeft, opRight)
            }
            insertCall.getSource().unparse(writer, opLeft, opRight)
            writer.endList(frame)
        }
    }

    @Override
    fun validate(validator: SqlValidator, scope: SqlValidatorScope?) {
        validator.validateMerge(this)
    }

    companion object {
        val OPERATOR: SqlSpecialOperator = SqlSpecialOperator("MERGE", SqlKind.MERGE)
    }
}
