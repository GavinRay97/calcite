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

import org.apache.calcite.rel.core.SetOp

/**
 * Interpreter node that implements a
 * [org.apache.calcite.rel.core.SetOp],
 * including [org.apache.calcite.rel.core.Minus],
 * [org.apache.calcite.rel.core.Union] and
 * [org.apache.calcite.rel.core.Intersect].
 */
class SetOpNode(compiler: Compiler, setOp: SetOp) : Node {
    private val leftSource: Source
    private val rightSource: Source
    private val sink: Sink
    private val setOp: SetOp

    init {
        leftSource = compiler.source(setOp, 0)
        rightSource = compiler.source(setOp, 1)
        sink = compiler.sink(setOp)
        this.setOp = setOp
    }

    @Override
    fun close() {
        leftSource.close()
        rightSource.close()
    }

    @Override
    @Throws(InterruptedException::class)
    fun run() {
        val leftRows: Collection<Row>
        val rightRows: Collection<Row>
        if (setOp.all) {
            leftRows = HashMultiset.create()
            rightRows = HashMultiset.create()
        } else {
            leftRows = HashSet()
            rightRows = HashSet()
        }
        var row: Row?
        while (leftSource.receive().also { row = it } != null) {
            leftRows.add(row)
        }
        while (rightSource.receive().also { row = it } != null) {
            rightRows.add(row)
        }
        when (setOp.kind) {
            INTERSECT -> for (leftRow in leftRows) {
                if (rightRows.remove(leftRow)) {
                    sink.send(leftRow)
                }
            }
            EXCEPT -> for (leftRow in leftRows) {
                if (!rightRows.remove(leftRow)) {
                    sink.send(leftRow)
                }
            }
            UNION -> {
                leftRows.addAll(rightRows)
                for (r in leftRows) {
                    sink.send(r)
                }
            }
            else -> {}
        }
    }
}
