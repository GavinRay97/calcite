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

import org.apache.calcite.rel.core.Union

/**
 * Interpreter node that implements a
 * [org.apache.calcite.rel.core.Union].
 *
 */
@Deprecated // to be removed before 2.0
@Deprecated("Use {@link org.apache.calcite.interpreter.SetOpNode}")
class UnionNode(compiler: Compiler, rel: Union) : Node {
    private val sources: ImmutableList<Source>
    private val sink: Sink
    private val rel: Union

    init {
        val builder: ImmutableList.Builder<Source> = ImmutableList.builder()
        for (i in 0 until rel.getInputs().size()) {
            builder.add(compiler.source(rel, i))
        }
        sources = builder.build()
        sink = compiler.sink(rel)
        this.rel = rel
    }

    @Override
    @Throws(InterruptedException::class)
    fun run() {
        val rows: Set<Row>? = if (rel.all) null else HashSet()
        for (source in sources) {
            var row: Row?
            while (source.receive().also { row = it } != null) {
                if (rows == null || rows.add(row)) {
                    sink.send(row)
                }
            }
        }
    }
}
