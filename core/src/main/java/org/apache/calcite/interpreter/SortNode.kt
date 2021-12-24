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

import org.apache.calcite.rel.RelFieldCollation

/**
 * Interpreter node that implements a
 * [org.apache.calcite.rel.core.Sort].
 */
class SortNode(compiler: Compiler?, rel: Sort?) : AbstractSingleNode<Sort?>(compiler, rel) {
    @Override
    @Throws(InterruptedException::class)
    fun run() {
        val offset = if (rel.offset == null) 0 else getValueAsInt(rel.offset)
        val fetch = if (rel.fetch == null) -1 else getValueAsInt(rel.fetch)
        // In pure limit mode. No sort required.
        var row: Row?
        loop@ if (rel.getCollation().getFieldCollations().isEmpty()) {
            for (i in 0 until offset) {
                row = source.receive()
                if (row == null) {
                    break@loop
                }
            }
            if (fetch >= 0) {
                var i = 0
                while (i < fetch && source.receive().also { row = it } != null) {
                    sink.send(row)
                    i++
                }
            } else {
                while (source.receive().also { row = it } != null) {
                    sink.send(row)
                }
            }
        } else {
            // Build a sorted collection.
            val list: List<Row> = ArrayList()
            while (source.receive().also { row = it } != null) {
                list.add(row)
            }
            list.sort(comparator())
            val end = if (fetch < 0 || offset + fetch > list.size()) list.size() else offset + fetch
            for (i in offset until end) {
                sink.send(list[i])
            }
        }
        sink.end()
    }

    private fun comparator(): Comparator<Row> {
        return if (rel.getCollation().getFieldCollations().size() === 1) {
            comparator(
                rel.getCollation().getFieldCollations().get(0)
            )
        } else Ordering.compound(
            Util.transform(
                rel.getCollation().getFieldCollations(),
                SortNode::comparator
            )
        )
    }

    companion object {
        private fun getValueAsInt(node: RexNode): Int {
            return requireNonNull(
                (node as RexLiteral).getValueAs(Integer::class.java)
            ) { "getValueAs(Integer.class) for $node" }
        }

        private fun comparator(fieldCollation: RelFieldCollation): Comparator<Row> {
            val nullComparison: Int = fieldCollation.nullDirection.nullComparison
            val x: Int = fieldCollation.getFieldIndex()
            return when (fieldCollation.direction) {
                ASCENDING -> Comparator<Row> { o1, o2 ->
                    RelFieldCollation.compare(o1.getValues().get(x), o2.getValues().get(x), nullComparison)
                }
                else -> Comparator<Row> { o1, o2 ->
                    RelFieldCollation.compare(o2.getValues().get(x), o1.getValues().get(x), -nullComparison)
                }
            }
        }
    }
}
