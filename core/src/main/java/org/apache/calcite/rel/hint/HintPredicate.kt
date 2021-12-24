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

import org.apache.calcite.rel.RelNode

/**
 * A `HintPredicate` indicates whether a [org.apache.calcite.rel.RelNode]
 * can apply the specified hint.
 *
 *
 * Every supported hint should register a `HintPredicate`
 * into the [HintStrategyTable]. For example, [HintPredicates.JOIN] implies
 * that this hint would be propagated and applied to the [org.apache.calcite.rel.core.Join]
 * relational expressions.
 *
 *
 * Usually use [NodeTypeHintPredicate] is enough for most of the [RelHint]s.
 * Some of the hints can only be matched to the relational expression with special
 * match conditions(not only the relational expression type).
 * i.e. "hash_join(r, st)", this hint can only be applied to JOIN expression that
 * has "r" and "st" as the input table names. To implement this, you can make a custom
 * `HintPredicate` instance.
 *
 *
 * A `HintPredicate` can be used independently or cascaded with other strategies
 * with method [HintPredicates.and].
 *
 *
 * In [HintStrategyTable] the predicate is used for
 * hints registration.
 *
 * @see HintStrategyTable
 */
interface HintPredicate {
    /**
     * Decides if the given `hint` can be applied to
     * the relational expression `rel`.
     *
     * @param hint The hint
     * @param rel  The relational expression
     * @return True if the `hint` can be applied to the `rel`
     */
    fun apply(hint: RelHint?, rel: RelNode?): Boolean
}
