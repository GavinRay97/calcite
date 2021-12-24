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
package org.apache.calcite.interpreter

import org.apache.calcite.rel.core.Join

/**
 * Interpreter node that implements a
 * [org.apache.calcite.rel.core.Join].
 */
class JoinNode(compiler: Compiler, rel: Join) : Node {
    private val leftSource: Source
    private val rightSource: Source
    private val sink: Sink
    private val rel: Join
    private val condition: Scalar
    private val context: Context

    init {
        leftSource = compiler.source(rel, 0)
        rightSource = compiler.source(rel, 1)
        sink = compiler.sink(rel)
        condition = compiler.compile(
            ImmutableList.of(rel.getCondition()),
            compiler.combinedRowType(rel.getInputs())
        )
        this.rel = rel
        context = compiler.createContext()
    }

    @Override
    override fun close() {
        leftSource.close()
        rightSource.close()
    }

    @Override
    @Throws(InterruptedException::class)
    override fun run() {
        val fieldCount: Int = (rel.getLeft().getRowType().getFieldCount()
                + rel.getRight().getRowType().getFieldCount())
        context.values = arrayOfNulls<Object>(fieldCount)

        // source for the outer relation of nested loop
        var outerSource: Source = leftSource
        // source for the inner relation of nested loop
        var innerSource: Source = rightSource
        if (rel.getJoinType() === JoinRelType.RIGHT) {
            outerSource = rightSource
            innerSource = leftSource
        }

        // row from outer source
        var outerRow: Row? = null
        // rows from inner source
        var innerRows: List<Row>? = null
        val matchRowSet: Set<Row> = HashSet()
        while (outerSource.receive().also { outerRow = it } != null) {
            if (innerRows == null) {
                innerRows = ArrayList<Row>()
                var innerRow: Row? = null
                while (innerSource.receive().also { innerRow = it } != null) {
                    innerRows.add(innerRow)
                }
            }
            matchRowSet.addAll(doJoin(outerRow, innerRows, rel.getJoinType()))
        }
        if (rel.getJoinType() === JoinRelType.FULL) {
            // send un-match rows for full join on right source
            val empty: List<Row> = ArrayList()
            // TODO: CALCITE-4308, JointNode in Interpreter might fail with NPE for FULL join
            for (row in requireNonNull(innerRows, "innerRows")) {
                if (matchRowSet.contains(row)) {
                    continue
                }
                doSend(row, empty, JoinRelType.RIGHT)
            }
        }
    }

    /**
     * Execution of the join action, returns the matched rows for the outer source row.
     */
    @Throws(InterruptedException::class)
    private fun doJoin(
        outerRow: Row, innerRows: List<Row>?,
        joinRelType: JoinRelType
    ): List<Row> {
        val outerRowOnLeft = joinRelType !== JoinRelType.RIGHT
        copyToContext(outerRow, outerRowOnLeft)
        val matchInnerRows: List<Row> = ArrayList()
        for (innerRow in innerRows) {
            copyToContext(innerRow, !outerRowOnLeft)
            val execute = condition.execute(context) as Boolean
            if (execute != null && execute) {
                matchInnerRows.add(innerRow)
            }
        }
        doSend(outerRow, matchInnerRows, joinRelType)
        return matchInnerRows
    }

    /**
     * If there exists matched rows with the outer row, sends the corresponding joined result,
     * otherwise, checks if need to use null value for column.
     */
    @Throws(InterruptedException::class)
    private fun doSend(
        outerRow: Row, matchInnerRows: List<Row>,
        joinRelType: JoinRelType
    ) {
        if (!matchInnerRows.isEmpty()) {
            when (joinRelType) {
                INNER, LEFT, RIGHT, FULL -> {
                    val outerRowOnLeft = joinRelType !== JoinRelType.RIGHT
                    copyToContext(outerRow, outerRowOnLeft)
                    requireNonNull(context.values, "context.values")
                    for (row in matchInnerRows) {
                        copyToContext(row, !outerRowOnLeft)
                        sink.send(Row.asCopy(context.values))
                    }
                }
                SEMI -> sink.send(Row.asCopy(outerRow.getValues()))
                else -> {}
            }
        } else {
            when (joinRelType) {
                LEFT, RIGHT, FULL -> {
                    requireNonNull(context.values, "context.values")
                    val nullColumnNum: Int = context.values.length - outerRow.size()
                    // for full join, use left source as outer source,
                    // and send un-match rows in left source fist,
                    // the un-match rows in right source will be process later.
                    copyToContext(outerRow, joinRelType.generatesNullsOnRight())
                    val nullColumnStart = if (joinRelType.generatesNullsOnRight()) outerRow.size() else 0
                    System.arraycopy(
                        arrayOfNulls<Object>(nullColumnNum), 0,
                        context.values, nullColumnStart, nullColumnNum
                    )
                    sink.send(Row.asCopy(context.values))
                }
                ANTI -> sink.send(Row.asCopy(outerRow.getValues()))
                else -> {}
            }
        }
    }

    /**
     * Copies the value of row into context values.
     */
    private fun copyToContext(row: Row, toLeftSide: Boolean) {
        @Nullable val values: Array<Object> = row.getValues()
        requireNonNull(context.values, "context.values")
        if (toLeftSide) {
            System.arraycopy(values, 0, context.values, 0, values.size)
        } else {
            System.arraycopy(
                values, 0, context.values,
                context.values.length - values.size, values.size
            )
        }
    }
}
