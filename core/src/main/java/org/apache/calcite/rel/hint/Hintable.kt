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

import org.apache.calcite.linq4j.function.Experimental

/**
 * [Hintable] is a kind of [RelNode] that can attach [RelHint]s.
 *
 *
 * This interface is experimental, [RelNode]s that implement it
 * have a constructor parameter named "hints" used to construct relational expression
 * with given hints.
 *
 *
 * Current design is not that elegant and mature, because we have to
 * copy the hints whenever these relational expressions are copied or used to
 * derive new relational expressions.
 * Even though we have implemented the mechanism to propagate the hints, for large queries,
 * there would be many cases where the hints are not copied to the right RelNode,
 * and the effort/memory is wasted if we are copying the hint to a RelNode
 * but the hint is not used.
 */
@Experimental
interface Hintable {
    /**
     * Attaches list of hints to this relational expression.
     *
     *
     * This method is only for internal use during sql-to-rel conversion.
     *
     *
     * Sub-class should return a new copy of the relational expression.
     *
     *
     * The default implementation merges the given hints with existing ones,
     * put them in one list and eliminate the duplicates; then
     * returns a new copy of this relational expression with the merged hints.
     *
     * @param hintList The hints to attach to this relational expression
     * @return Relational expression with the hints `hintList` attached
     */
    fun attachHints(hintList: List<RelHint?>?): RelNode? {
        Objects.requireNonNull(hintList, "hintList")
        val hints: Set<RelHint> = LinkedHashSet(hints)
        hints.addAll(hintList)
        return withHints(ArrayList(hints))
    }

    /**
     * Returns a new relational expression with the specified hints `hintList`.
     *
     *
     * This method should be overridden by every logical node that supports hint.
     * It is only for internal use during decorrelation.
     *
     *
     * Sub-class should return a new copy of the relational expression.
     *
     *
     * The default implementation returns the relational expression directly
     * only because not every kind of relational expression supports hints.
     *
     * @return Relational expression with set up hints
     */
    fun withHints(hintList: List<RelHint?>?): RelNode? {
        return this as RelNode
    }

    /**
     * Returns the hints of this relational expressions as an immutable list.
     */
    val hints: ImmutableList<RelHint?>?
}
