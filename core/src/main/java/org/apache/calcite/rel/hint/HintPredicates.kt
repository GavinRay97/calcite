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
package org.apache.calcite.rel.hint

import org.apache.calcite.rel.hint.NodeTypeHintPredicate.NodeType

/**
 * A collection of hint predicates.
 */
object HintPredicates {
    /** A hint predicate that indicates a hint can only be used to
     * the whole query(no specific nodes).  */
    val SET_VAR: HintPredicate = NodeTypeHintPredicate(NodeTypeHintPredicate.NodeType.SET_VAR)

    /** A hint predicate that indicates a hint can only be used to
     * [org.apache.calcite.rel.core.Join] nodes.  */
    val JOIN: HintPredicate = NodeTypeHintPredicate(NodeTypeHintPredicate.NodeType.JOIN)

    /** A hint predicate that indicates a hint can only be used to
     * [org.apache.calcite.rel.core.TableScan] nodes.  */
    val TABLE_SCAN: HintPredicate = NodeTypeHintPredicate(NodeTypeHintPredicate.NodeType.TABLE_SCAN)

    /** A hint predicate that indicates a hint can only be used to
     * [org.apache.calcite.rel.core.Project] nodes.  */
    val PROJECT: HintPredicate = NodeTypeHintPredicate(NodeTypeHintPredicate.NodeType.PROJECT)

    /** A hint predicate that indicates a hint can only be used to
     * [org.apache.calcite.rel.core.Aggregate] nodes.  */
    val AGGREGATE: HintPredicate = NodeTypeHintPredicate(NodeTypeHintPredicate.NodeType.AGGREGATE)

    /** A hint predicate that indicates a hint can only be used to
     * [org.apache.calcite.rel.core.Calc] nodes.  */
    val CALC: HintPredicate = NodeTypeHintPredicate(NodeTypeHintPredicate.NodeType.CALC)

    /**
     * Returns a composed hint predicate that represents a short-circuiting logical
     * AND of an array of hint predicates `hintPredicates`.  When evaluating the composed
     * predicate, if a predicate is `false`, then all the left
     * predicates are not evaluated.
     *
     *
     * The predicates are evaluated in sequence.
     */
    fun and(vararg hintPredicates: HintPredicate?): HintPredicate {
        return CompositeHintPredicate(CompositeHintPredicate.Composition.AND, hintPredicates)
    }

    /**
     * Returns a composed hint predicate that represents a short-circuiting logical
     * OR of an array of hint predicates `hintPredicates`.  When evaluating the composed
     * predicate, if a predicate is `true`, then all the left
     * predicates are not evaluated.
     *
     *
     * The predicates are evaluated in sequence.
     */
    fun or(vararg hintPredicates: HintPredicate?): HintPredicate {
        return CompositeHintPredicate(CompositeHintPredicate.Composition.OR, hintPredicates)
    }
}
