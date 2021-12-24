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
package org.apache.calcite.rel

import org.apache.calcite.sql.SqlExplainLevel

/**
 * Callback for an expression to dump itself to.
 *
 *
 * It is used for generating EXPLAIN PLAN output, and also for serializing
 * a tree of relational expressions to JSON.
 */
interface RelWriter {
    /**
     * Prints an explanation of a node, with a list of (term, value) pairs.
     *
     *
     * The term-value pairs are generally gathered by calling
     * [org.apache.calcite.rel.RelNode.explain].
     * Each sub-class of [org.apache.calcite.rel.RelNode]
     * calls [.input]
     * and [.item] to declare term-value pairs.
     *
     * @param rel       Relational expression
     * @param valueList List of term-value pairs
     */
    fun explain(rel: RelNode?, valueList: List<Pair<String?, Object?>?>?)

    /** Returns detail level at which plan should be generated.  */
    val detailLevel: SqlExplainLevel?

    /**
     * Adds an input to the explanation of the current node.
     *
     * @param term  Term for input, e.g. "left" or "input #1".
     * @param input Input relational expression
     */
    fun input(term: String?, input: RelNode?): RelWriter {
        return item(term, input)
    }

    /**
     * Adds an attribute to the explanation of the current node.
     *
     * @param term  Term for attribute, e.g. "joinType"
     * @param value Attribute value
     */
    fun item(term: String?, @Nullable value: Object?): RelWriter

    /**
     * Adds an input to the explanation of the current node, if a condition
     * holds.
     */
    fun itemIf(term: String?, @Nullable value: Object?, condition: Boolean): RelWriter? {
        return if (condition) item(term, value) else this
    }

    /**
     * Writes the completed explanation.
     */
    fun done(node: RelNode?): RelWriter?

    /**
     * Returns whether the writer prefers nested values. Traditional explain
     * writers prefer flattened values.
     */
    fun nest(): Boolean {
        return false
    }
}
