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

import org.apache.calcite.rel.core.Values

/**
 * Interpreter node that implements a
 * [org.apache.calcite.rel.core.Values].
 */
class ValuesNode(compiler: Compiler, rel: Values) : Node {
    private val sink: Sink
    private val fieldCount: Int
    private val rows: ImmutableList<Row>

    init {
        sink = compiler.sink(rel)
        fieldCount = rel.getRowType().getFieldCount()
        rows = createRows(compiler, fieldCount, rel.getTuples())
    }

    @Override
    @Throws(InterruptedException::class)
    fun run() {
        for (row in rows) {
            sink.send(row)
        }
        sink.end()
    }

    companion object {
        private fun createRows(
            compiler: Compiler,
            fieldCount: Int,
            tuples: ImmutableList<ImmutableList<RexLiteral>>
        ): ImmutableList<Row> {
            val nodes: List<RexNode> = ArrayList()
            for (tuple in tuples) {
                nodes.addAll(tuple)
            }
            val scalar: Scalar = compiler.compile(nodes, null)
            val values: Array<Object?> = arrayOfNulls<Object>(nodes.size())
            val context: Context = compiler.createContext()
            scalar.execute(context, values)
            val rows: ImmutableList.Builder<Row> = ImmutableList.builder()
            val subValues: Array<Object?> = arrayOfNulls<Object>(fieldCount)
            var r = 0
            val n: Int = tuples.size()
            while (r < n) {
                System.arraycopy(values, r * fieldCount, subValues, 0, fieldCount)
                rows.add(Row.asCopy(subValues))
                ++r
            }
            return rows.build()
        }
    }
}
