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
 * A `SqlInsert` is a node of a parse tree which represents an INSERT
 * statement.
 */
class SqlInsert(
    pos: SqlParserPos?,
    keywords: SqlNodeList?,
    targetTable: SqlNode,
    source: SqlNode,
    @Nullable columnList: SqlNodeList?
) : SqlCall(pos) {
    var keywords: SqlNodeList?
    var targetTable: SqlNode
    var source: SqlNode

    @Nullable
    var columnList: SqlNodeList?

    //~ Constructors -----------------------------------------------------------
    init {
        this.keywords = keywords
        this.targetTable = targetTable
        this.source = source
        this.columnList = columnList
        assert(keywords != null)
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val kind: org.apache.calcite.sql.SqlKind
        get() = SqlKind.INSERT

    @get:Override
    val operator: SqlOperator
        get() = OPERATOR

    @get:Override
    @get:SuppressWarnings("nullness")
    val operandList: List<org.apache.calcite.sql.SqlNode>
        get() = ImmutableNullableList.of(keywords, targetTable, source, columnList)

    /** Returns whether this is an UPSERT statement.
     *
     *
     * In SQL, this is represented using the `UPSERT` keyword rather than
     * `INSERT`; in the abstract syntax tree, an UPSERT is indicated by the
     * presence of a [SqlInsertKeyword.UPSERT] keyword.  */
    val isUpsert: Boolean
        get() = getModifierNode(SqlInsertKeyword.UPSERT) != null

    @SuppressWarnings("assignment.type.incompatible")
    @Override
    fun setOperand(i: Int, @Nullable operand: SqlNode) {
        when (i) {
            0 -> keywords = operand as SqlNodeList
            1 -> {
                assert(operand is SqlIdentifier)
                targetTable = operand
            }
            2 -> source = operand
            3 -> columnList = operand as SqlNodeList
            else -> throw AssertionError(i)
        }
    }

    /**
     * Return the identifier for the target table of the insertion.
     */
    fun getTargetTable(): SqlNode {
        return targetTable
    }

    /**
     * Returns the source expression for the data to be inserted.
     */
    fun getSource(): SqlNode {
        return source
    }

    fun setSource(source: SqlSelect) {
        this.source = source
    }

    /**
     * Returns the list of target column names, or null for all columns in the
     * target table.
     */
    @get:Nullable
    @get:Pure
    val targetColumnList: org.apache.calcite.sql.SqlNodeList?
        get() = columnList

    @Nullable
    fun getModifierNode(modifier: SqlInsertKeyword): SqlNode? {
        for (keyword in keywords) {
            val keyword2: SqlInsertKeyword = (keyword as SqlLiteral).symbolValue(SqlInsertKeyword::class.java)!!
            if (keyword2 === modifier) {
                return keyword
            }
        }
        return null
    }

    @Override
    fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        writer.startList(SqlWriter.FrameTypeEnum.SELECT)
        writer.sep(if (isUpsert) "UPSERT INTO" else "INSERT INTO")
        val opLeft: Int = operator.getLeftPrec()
        val opRight: Int = operator.getRightPrec()
        targetTable.unparse(writer, opLeft, opRight)
        if (columnList != null) {
            columnList.unparse(writer, opLeft, opRight)
        }
        writer.newlineAndIndent()
        source.unparse(writer, 0, 0)
    }

    @Override
    fun validate(validator: SqlValidator, scope: SqlValidatorScope?) {
        validator.validateInsert(this)
    }

    companion object {
        val OPERATOR: SqlSpecialOperator = object : SqlSpecialOperator("INSERT", SqlKind.INSERT) {
            @SuppressWarnings("argument.type.incompatible")
            @Override
            fun createCall(
                @Nullable functionQualifier: SqlLiteral?,
                pos: SqlParserPos?,
                @Nullable vararg operands: SqlNode
            ): SqlCall {
                return SqlInsert(
                    pos,
                    operands[0] as SqlNodeList,
                    operands[1],
                    operands[2],
                    operands[3] as SqlNodeList
                )
            }
        }
    }
}
