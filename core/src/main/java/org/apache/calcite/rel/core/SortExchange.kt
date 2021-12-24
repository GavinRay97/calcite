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
package org.apache.calcite.rel.core

import org.apache.calcite.plan.RelOptCluster

/**
 * Relational expression that performs [Exchange] and [Sort]
 * simultaneously.
 *
 *
 * Whereas a Sort produces output with a particular
 * [org.apache.calcite.rel.RelCollation] and an Exchange produces output
 * with a particular [org.apache.calcite.rel.RelDistribution], the output
 * of a SortExchange has both the required collation and distribution.
 *
 *
 * Several implementations of SortExchange are possible; the purpose of this
 * base class allows rules to be written that apply to all of those
 * implementations.
 */
abstract class SortExchange protected constructor(
    cluster: RelOptCluster?, traitSet: RelTraitSet,
    input: RelNode?, distribution: RelDistribution, collation: RelCollation
) : Exchange(cluster, traitSet, input, distribution) {
    protected val collation: RelCollation
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a SortExchange.
     *
     * @param cluster   Cluster this relational expression belongs to
     * @param traitSet  Trait set
     * @param input     Input relational expression
     * @param distribution Distribution specification
     */
    init {
        this.collation = Objects.requireNonNull(collation, "collation")
        assert(traitSet.containsIfApplicable(collation)) { "traits=$traitSet, collation=$collation" }
    }

    /**
     * Creates a SortExchange by parsing serialized output.
     */
    protected constructor(input: RelInput) : this(
        input.getCluster(),
        input.getTraitSet().plus(input.getCollation())
            .plus(input.getDistribution()),
        input.getInput(),
        RelDistributionTraitDef.INSTANCE.canonize(input.getDistribution()),
        RelCollationTraitDef.INSTANCE.canonize(input.getCollation())
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun copy(
        traitSet: RelTraitSet?,
        newInput: RelNode?, newDistribution: RelDistribution?
    ): SortExchange {
        return copy(traitSet, newInput, newDistribution, collation)
    }

    abstract fun copy(
        traitSet: RelTraitSet?, newInput: RelNode?,
        newDistribution: RelDistribution?, newCollation: RelCollation?
    ): SortExchange

    /**
     * Returns the array of [org.apache.calcite.rel.RelFieldCollation]s
     * asked for by the sort specification, from most significant to least
     * significant.
     *
     *
     * See also
     * [org.apache.calcite.rel.metadata.RelMetadataQuery.collations],
     * which lists all known collations. For example,
     * `ORDER BY time_id` might also be sorted by
     * `the_year, the_month` because of a known monotonicity
     * constraint among the columns. `getCollation` would return
     * `[time_id]` and `collations` would return
     * `[ [time_id], [the_year, the_month] ]`.
     */
    fun getCollation(): RelCollation {
        return collation
    }

    @Override
    override fun explainTerms(pw: RelWriter?): RelWriter {
        return super.explainTerms(pw)
            .item("collation", collation)
    }
}
