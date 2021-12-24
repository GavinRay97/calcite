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

import org.apache.calcite.sql.`fun`.SqlStdOperatorTable

/**
 * Parse tree node that represents a PIVOT applied to a table reference
 * (or sub-query).
 *
 *
 * Syntax:
 * <blockquote>`SELECT *
 * FROM query PIVOT (agg, ... FOR axis, ... IN (in, ...)) AS alias`
</blockquote> *
 */
class SqlPivot(
    pos: SqlParserPos?, query: SqlNode?, aggList: SqlNodeList?,
    axisList: SqlNodeList?, inList: SqlNodeList?
) : SqlCall(pos) {
    var query: SqlNode
    val aggList: SqlNodeList
    val axisList: SqlNodeList
    val inList: SqlNodeList

    //~ Constructors -----------------------------------------------------------
    init {
        this.query = Objects.requireNonNull(query, "query")
        this.aggList = Objects.requireNonNull(aggList, "aggList")
        this.axisList = Objects.requireNonNull(axisList, "axisList")
        this.inList = Objects.requireNonNull(inList, "inList")
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val operator: org.apache.calcite.sql.SqlOperator
        get() = OPERATOR

    @get:Override
    val operandList: List<Any>
        get() = ImmutableNullableList.of(query, aggList, axisList, inList)

    @SuppressWarnings("nullness")
    @Override
    fun setOperand(i: Int, @Nullable operand: SqlNode) {
        // Only 'query' is mutable. (It is required for validation.)
        when (i) {
            0 -> query = operand
            else -> super.setOperand(i, operand)
        }
    }

    @Override
    fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        query.unparse(writer, leftPrec, 0)
        writer.keyword("PIVOT")
        val frame: SqlWriter.Frame = writer.startList("(", ")")
        aggList.unparse(writer, 0, 0)
        writer.sep("FOR")
        // force parentheses if there is more than one axis
        val leftPrec1 = if (axisList.size() > 1) 1 else 0
        axisList.unparse(writer, leftPrec1, 0)
        writer.sep("IN")
        writer.list(
            SqlWriter.FrameTypeEnum.PARENTHESES, SqlWriter.COMMA,
            stripList(inList)
        )
        writer.endList(frame)
    }

    /** Returns the aggregate list as (alias, call) pairs.
     * If there is no 'AS', alias is null.  */
    fun forEachAgg(consumer: BiConsumer<String?, SqlNode?>) {
        for (agg in aggList) {
            val call: SqlNode = SqlUtil.stripAs(agg)
            val alias: String = SqlValidatorUtil.getAlias(agg, -1)
            consumer.accept(alias, call)
        }
    }

    /** Returns the value list as (alias, node list) pairs.  */
    fun forEachNameValues(consumer: BiConsumer<String?, SqlNodeList?>) {
        for (node in inList) {
            var alias: String
            if (node.getKind() === SqlKind.AS) {
                val operands: List<SqlNode> = (node as SqlCall).getOperandList()
                alias = (operands[1] as SqlIdentifier).getSimple()
                node = operands[0]
            } else {
                alias = pivotAlias(node)
            }
            consumer.accept(alias, toNodes(node))
        }
    }

    /** Returns the set of columns that are referenced as an argument to an
     * aggregate function or in a column in the `FOR` clause. All columns
     * that are not used will become "GROUP BY" columns.  */
    fun usedColumnNames(): Set<String> {
        val columnNames: Set<String> = HashSet()
        val nameCollector: SqlVisitor<Void> = object : SqlBasicVisitor<Void?>() {
            @Override
            fun visit(id: SqlIdentifier): Void {
                columnNames.add(Util.last(id.names))
                return super.visit(id)
            }
        }
        for (agg in aggList) {
            val call: SqlCall = SqlUtil.stripAs(agg) as SqlCall
            call.accept(nameCollector)
        }
        for (axis in axisList) {
            axis.accept(nameCollector)
        }
        return columnNames
    }

    /** Pivot operator.  */
    class Operator(kind: SqlKind) : SqlSpecialOperator(kind.name(), kind)
    companion object {
        val OPERATOR = Operator(SqlKind.PIVOT)
        fun stripList(list: SqlNodeList): SqlNodeList {
            return list.stream().map { e: SqlNode -> strip(e) }
                .collect(SqlNode.toList(list.pos))
        }

        /** Converts a single-element SqlNodeList to its constituent node.
         * For example, "(1)" becomes "1";
         * "(2) as a" becomes "2 as a";
         * "(3, 4)" remains "(3, 4)";
         * "(5, 6) as b" remains "(5, 6) as b".  */
        private fun strip(e: SqlNode): SqlNode {
            return when (e.getKind()) {
                AS -> {
                    val call: SqlCall = e as SqlCall
                    val operands: List<SqlNode> = call.getOperandList()
                    SqlStdOperatorTable.AS.createCall(
                        e.pos,
                        strip(operands[0]), operands[1]
                    )
                }
                else -> {
                    if (e is SqlNodeList && (e as SqlNodeList).size() === 1) {
                        (e as SqlNodeList).get(0)
                    } else e
                }
            }
        }

        fun pivotAlias(node: SqlNode): String {
            return if (node is SqlNodeList) {
                (node as SqlNodeList).stream()
                    .map { node: SqlNode -> pivotAlias(node) }.collect(Collectors.joining("_"))
            } else node.toString()
        }

        /** Converts a SqlNodeList to a list, and other nodes to a singleton list.  */
        fun toNodes(node: SqlNode): SqlNodeList {
            return if (node is SqlNodeList) {
                node as SqlNodeList
            } else {
                SqlNodeList(ImmutableList.of(node), node.getParserPosition())
            }
        }
    }
}
