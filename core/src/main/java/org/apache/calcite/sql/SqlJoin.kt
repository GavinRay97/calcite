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
 * Parse tree node representing a `JOIN` clause.
 */
class SqlJoin(
    pos: SqlParserPos?, left: SqlNode, natural: SqlLiteral,
    joinType: SqlLiteral, right: SqlNode, conditionType: SqlLiteral,
    @Nullable condition: SqlNode
) : SqlCall(pos) {
    var left: SqlNode

    /**
     * Operand says whether this is a natural join. Must be constant TRUE or
     * FALSE.
     */
    var natural: SqlLiteral

    /**
     * Value must be a [SqlLiteral], one of the integer codes for
     * [JoinType].
     */
    var joinType: SqlLiteral
    var right: SqlNode

    /**
     * Value must be a [SqlLiteral], one of the integer codes for
     * [JoinConditionType].
     */
    var conditionType: SqlLiteral

    @Nullable
    var condition: SqlNode

    //~ Constructors -----------------------------------------------------------
    init {
        this.left = left
        this.natural = requireNonNull(natural, "natural")
        this.joinType = requireNonNull(joinType, "joinType")
        this.right = right
        this.conditionType = requireNonNull(conditionType, "conditionType")
        this.condition = condition
        Preconditions.checkArgument(natural.getTypeName() === SqlTypeName.BOOLEAN)
        conditionType.getValueAs(JoinConditionType::class.java)
        joinType.getValueAs(JoinType::class.java)
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val operator: SqlOperator
        get() = OPERATOR

    @get:Override
    val kind: org.apache.calcite.sql.SqlKind
        get() = SqlKind.JOIN

    @get:Override
    @get:SuppressWarnings("nullness")
    val operandList: List<org.apache.calcite.sql.SqlNode>
        get() = ImmutableNullableList.of(
            left, natural, joinType, right,
            conditionType, condition
        )

    @SuppressWarnings("assignment.type.incompatible")
    @Override
    fun setOperand(i: Int, @Nullable operand: SqlNode) {
        when (i) {
            0 -> left = operand
            1 -> natural = operand as SqlLiteral
            2 -> joinType = operand as SqlLiteral
            3 -> right = operand
            4 -> conditionType = operand as SqlLiteral
            5 -> condition = operand
            else -> throw AssertionError(i)
        }
    }

    @Nullable
    fun getCondition(): SqlNode {
        return condition
    }

    /** Returns a [JoinConditionType], never null.  */
    fun getConditionType(): JoinConditionType {
        return conditionType.getValueAs(JoinConditionType::class.java)
    }

    val conditionTypeNode: org.apache.calcite.sql.SqlLiteral
        get() = conditionType

    /** Returns a [JoinType], never null.  */
    fun getJoinType(): JoinType {
        return joinType.getValueAs(JoinType::class.java)
    }

    val joinTypeNode: org.apache.calcite.sql.SqlLiteral
        get() = joinType

    fun getLeft(): SqlNode {
        return left
    }

    fun setLeft(left: SqlNode) {
        this.left = left
    }

    fun isNatural(): Boolean {
        return natural.booleanValue()
    }

    val isNaturalNode: org.apache.calcite.sql.SqlLiteral
        get() = natural

    fun getRight(): SqlNode {
        return right
    }

    fun setRight(right: SqlNode) {
        this.right = right
    }

    /**
     * `SqlJoinOperator` describes the syntax of the SQL `
     * JOIN` operator. Since there is only one such operator, this class is
     * almost certainly a singleton.
     */
    class SqlJoinOperator  //~ Constructors -----------------------------------------------------------
        : SqlOperator("JOIN", SqlKind.JOIN, 16, true, null, null, null) {
        //~ Methods ----------------------------------------------------------------
        @get:Override
        val syntax: SqlSyntax
            get() = SqlSyntax.SPECIAL

        @SuppressWarnings("argument.type.incompatible")
        @Override
        fun createCall(
            @Nullable functionQualifier: SqlLiteral?,
            pos: SqlParserPos?,
            @Nullable vararg operands: SqlNode
        ): SqlCall {
            assert(functionQualifier == null)
            return SqlJoin(
                pos, operands[0], operands[1] as SqlLiteral,
                operands[2] as SqlLiteral, operands[3], operands[4] as SqlLiteral,
                operands[5]
            )
        }

        @Override
        fun unparse(
            writer: SqlWriter,
            call: SqlCall,
            leftPrec: Int,
            rightPrec: Int
        ) {
            val join = call as SqlJoin
            join.left.unparse(
                writer,
                leftPrec,
                getLeftPrec()
            )
            when (join.getJoinType()) {
                COMMA -> writer.sep(",", true)
                CROSS -> writer.sep(if (join.isNatural()) "NATURAL CROSS JOIN" else "CROSS JOIN")
                FULL -> writer.sep(if (join.isNatural()) "NATURAL FULL JOIN" else "FULL JOIN")
                INNER -> writer.sep(if (join.isNatural()) "NATURAL INNER JOIN" else "INNER JOIN")
                LEFT -> writer.sep(if (join.isNatural()) "NATURAL LEFT JOIN" else "LEFT JOIN")
                LEFT_SEMI_JOIN -> writer.sep(if (join.isNatural()) "NATURAL LEFT SEMI JOIN" else "LEFT SEMI JOIN")
                RIGHT -> writer.sep(if (join.isNatural()) "NATURAL RIGHT JOIN" else "RIGHT JOIN")
                else -> throw Util.unexpected(join.getJoinType())
            }
            join.right.unparse(writer, getRightPrec(), rightPrec)
            val joinCondition: SqlNode = join.condition
            if (joinCondition != null) {
                when (join.getConditionType()) {
                    USING -> {
                        // No need for an extra pair of parens -- the condition is a
                        // list. The result is something like "USING (deptno, gender)".
                        writer.keyword("USING")
                        assert(joinCondition is SqlNodeList) { "joinCondition should be SqlNodeList, got $joinCondition" }
                        val frame: SqlWriter.Frame = writer.startList(FRAME_TYPE, "(", ")")
                        joinCondition.unparse(writer, 0, 0)
                        writer.endList(frame)
                    }
                    ON -> {
                        writer.keyword("ON")
                        joinCondition.unparse(writer, leftPrec, rightPrec)
                    }
                    else -> throw Util.unexpected(join.getConditionType())
                }
            }
        }

        companion object {
            private val FRAME_TYPE: SqlWriter.FrameType = SqlWriter.FrameTypeEnum.create("USING")
        }
    }

    companion object {
        val OPERATOR = SqlJoinOperator()
    }
}
