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
 * Parse tree node that represents UNPIVOT applied to a table reference
 * (or sub-query).
 *
 *
 * Syntax:
 * <blockquote><pre>`SELECT *
 * FROM query
 * UNPIVOT [ { INCLUDE | EXCLUDE } NULLS ] (
 * columns FOR columns IN ( columns [ AS values ], ...))
 *
 * where:
 *
 * columns: column
 * | '(' column, ... ')'
 * values:  value
 * | '(' value, ... ')'
`</pre></blockquote> *
 */
class SqlUnpivot(
    pos: SqlParserPos?, query: SqlNode?, includeNulls: Boolean,
    measureList: SqlNodeList?, axisList: SqlNodeList?, inList: SqlNodeList?
) : SqlCall(pos) {
    var query: SqlNode
    val includeNulls: Boolean
    val measureList: SqlNodeList
    val axisList: SqlNodeList
    val inList: SqlNodeList

    //~ Constructors -----------------------------------------------------------
    init {
        this.query = Objects.requireNonNull(query, "query")
        this.includeNulls = includeNulls
        this.measureList = Objects.requireNonNull(measureList, "measureList")
        this.axisList = Objects.requireNonNull(axisList, "axisList")
        this.inList = Objects.requireNonNull(inList, "inList")
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val operator: SqlOperator
        get() = OPERATOR

    @get:Override
    val operandList: List<Any>
        get() = ImmutableNullableList.of(query, measureList, axisList, inList)

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
        writer.keyword("UNPIVOT")
        writer.keyword(if (includeNulls) "INCLUDE NULLS" else "EXCLUDE NULLS")
        val frame: SqlWriter.Frame = writer.startList("(", ")")
        // force parentheses if there is more than one foo
        val leftPrec1 = if (measureList.size() > 1) 1 else 0
        measureList.unparse(writer, leftPrec1, 0)
        writer.sep("FOR")
        // force parentheses if there is more than one axis
        val leftPrec2 = if (axisList.size() > 1) 1 else 0
        axisList.unparse(writer, leftPrec2, 0)
        writer.sep("IN")
        writer.list(
            SqlWriter.FrameTypeEnum.PARENTHESES, SqlWriter.COMMA,
            SqlPivot.stripList(inList)
        )
        writer.endList(frame)
    }

    /** Returns the measure list as SqlIdentifiers.  */
    @SuppressWarnings(["unchecked", "rawtypes"])
    fun forEachMeasure(consumer: Consumer<SqlIdentifier?>?) {
        (measureList as List<SqlIdentifier?>).forEach(consumer)
    }

    /** Returns contents of the IN clause `(nodeList, valueList)` pairs.
     * `valueList` is null if the entry has no `AS` clause.  */
    fun forEachNameValues(
        consumer: BiConsumer<SqlNodeList?, SqlNodeList?>
    ) {
        for (node in inList) {
            when (node.getKind()) {
                AS -> {
                    val call: SqlCall = node as SqlCall
                    assert(call.getOperandList().size() === 2)
                    val nodeList: SqlNodeList = call.operand(0)
                    val valueList: SqlNodeList = call.operand(1)
                    consumer.accept(nodeList, valueList)
                }
                else -> {
                    val nodeList2: SqlNodeList = node as SqlNodeList
                    consumer.accept(nodeList2, null)
                }
            }
        }
    }

    /** Returns the set of columns that are referenced in the `FOR`
     * clause. All columns that are not used will be part of the returned row.  */
    fun usedColumnNames(): Set<String> {
        val columnNames: Set<String> = HashSet()
        val nameCollector: SqlVisitor<Void> = object : SqlBasicVisitor<Void?>() {
            @Override
            fun visit(id: SqlIdentifier): Void {
                columnNames.add(Util.last(id.names))
                return super.visit(id)
            }
        }
        forEachNameValues(BiConsumer<SqlNodeList, SqlNodeList> { aliasList, valueList -> aliasList.accept(nameCollector) })
        return columnNames
    }

    /** Unpivot operator.  */
    class Operator(kind: SqlKind) : SqlSpecialOperator(kind.name(), kind)
    companion object {
        val OPERATOR = Operator(SqlKind.UNPIVOT)

        /** Computes an alias. In the query fragment
         * <blockquote>
         * `UNPIVOT ... FOR ... IN ((c1, c2) AS 'c1_c2', (c3, c4))`
        </blockquote> *
         * note that `(c3, c4)` has no `AS`. The computed alias is
         * 'C3_C4'.  */
        fun aliasValue(aliasList: SqlNodeList): String {
            val b = StringBuilder()
            aliasList.forEach { alias ->
                if (b.length() > 0) {
                    b.append('_')
                }
                b.append(Util.last((alias as SqlIdentifier).names))
            }
            return b.toString()
        }
    }
}
