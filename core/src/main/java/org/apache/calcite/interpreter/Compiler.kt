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

import org.apache.calcite.DataContext

/**
 * Context while converting a tree of [RelNode] to a program
 * that can be run by an [Interpreter].
 */
interface Compiler {
    /** Compiles an expression to an executable form.  */
    fun compile(nodes: List<RexNode?>?, @Nullable inputRowType: RelDataType?): Scalar
    fun combinedRowType(inputs: List<RelNode?>?): RelDataType?
    fun source(rel: RelNode?, ordinal: Int): Source?

    /**
     * Creates a Sink for a relational expression to write into.
     *
     *
     * This method is generally called from the constructor of a [Node].
     * But a constructor could instead call
     * [.enumerable].
     *
     * @param rel Relational expression
     * @return Sink
     */
    fun sink(rel: RelNode?): Sink?

    /** Tells the interpreter that a given relational expression wishes to
     * give its output as an enumerable.
     *
     *
     * This is as opposed to the norm, where a relational expression calls
     * [.sink], then its [Node.run] method writes into that
     * sink.
     *
     * @param rel Relational expression
     * @param rowEnumerable Contents of relational expression
     */
    fun enumerable(rel: RelNode?, rowEnumerable: Enumerable<Row?>?)
    val dataContext: DataContext?
    fun createContext(): Context
}
