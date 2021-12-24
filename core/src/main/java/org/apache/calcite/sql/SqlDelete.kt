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
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql.validate.SqlValidatorImpl
import org.apache.calcite.sql.validate.SqlValidatorScope
import org.apache.calcite.util.ImmutableNullableList
import java.util.List

/**
 * A `SqlDelete` is a node of a parse tree which represents a DELETE
 * statement.
 */
class SqlDelete(
    pos: SqlParserPos?,
    targetTable: SqlNode,
    @Nullable condition: SqlNode,
    @Nullable sourceSelect: SqlSelect,
    @Nullable alias: SqlIdentifier
) : SqlCall(pos) {
    var targetTable: SqlNode

    @Nullable
    var condition: SqlNode

    @Nullable
    var sourceSelect: SqlSelect

    @Nullable
    var alias: SqlIdentifier

    //~ Constructors -----------------------------------------------------------
    init {
        this.targetTable = targetTable
        this.condition = condition
        this.sourceSelect = sourceSelect
        this.alias = alias
    }

    //~ Methods ----------------------------------------------------------------
    override val kind: SqlKind
        @Override get() = SqlKind.DELETE
    override val operator: SqlOperator
        @Override get() = OPERATOR
    override val operandList: List<Any>
        @SuppressWarnings("nullness") @Override get() = ImmutableNullableList.of(targetTable, condition, alias)

    @SuppressWarnings("assignment.type.incompatible")
    @Override
    override fun setOperand(i: Int, @Nullable operand: SqlNode) {
        when (i) {
            0 -> targetTable = operand
            1 -> condition = operand
            2 -> sourceSelect = operand as SqlSelect
            3 -> alias = operand as SqlIdentifier
            else -> throw AssertionError(i)
        }
    }

    /**
     * Returns the identifier for the target table of the deletion.
     */
    fun getTargetTable(): SqlNode {
        return targetTable
    }

    /**
     * Returns the alias for the target table of the deletion.
     */
    @Nullable
    fun getAlias(): SqlIdentifier {
        return alias
    }

    /**
     * Gets the filter condition for rows to be deleted.
     *
     * @return the condition expression for the data to be deleted, or null for
     * all rows in the table
     */
    @Nullable
    fun getCondition(): SqlNode {
        return condition
    }

    /**
     * Gets the source SELECT expression for the data to be deleted. This
     * returns null before the condition has been expanded by
     * [SqlValidatorImpl.performUnconditionalRewrites].
     *
     * @return the source SELECT for the data to be inserted
     */
    @Nullable
    fun getSourceSelect(): SqlSelect {
        return sourceSelect
    }

    @Override
    override fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        val frame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "DELETE FROM", "")
        val opLeft: Int = operator.getLeftPrec()
        val opRight: Int = operator.getRightPrec()
        targetTable.unparse(writer, opLeft, opRight)
        val alias: SqlIdentifier = alias
        if (alias != null) {
            writer.keyword("AS")
            alias.unparse(writer, opLeft, opRight)
        }
        val condition: SqlNode = condition
        if (condition != null) {
            writer.sep("WHERE")
            condition.unparse(writer, opLeft, opRight)
        }
        writer.endList(frame)
    }

    @Override
    override fun validate(validator: SqlValidator, scope: SqlValidatorScope?) {
        validator.validateDelete(this)
    }

    fun setSourceSelect(sourceSelect: SqlSelect) {
        this.sourceSelect = sourceSelect
    }

    companion object {
        val OPERATOR: SqlSpecialOperator = SqlSpecialOperator("DELETE", SqlKind.DELETE)
    }
}
