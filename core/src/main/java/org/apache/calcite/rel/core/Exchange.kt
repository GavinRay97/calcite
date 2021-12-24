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
 * Relational expression that imposes a particular distribution on its input
 * without otherwise changing its content.
 *
 * @see org.apache.calcite.rel.core.SortExchange
 */
abstract class Exchange protected constructor(
    cluster: RelOptCluster?, traitSet: RelTraitSet, input: RelNode?,
    distribution: RelDistribution
) : SingleRel(cluster, traitSet, input) {
    //~ Instance fields --------------------------------------------------------
    val distribution: RelDistribution
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates an Exchange.
     *
     * @param cluster   Cluster this relational expression belongs to
     * @param traitSet  Trait set
     * @param input     Input relational expression
     * @param distribution Distribution specification
     */
    init {
        this.distribution = Objects.requireNonNull(distribution, "distribution")
        assert(traitSet.containsIfApplicable(distribution)) { "traits=$traitSet, distribution=$distribution" }
        assert(distribution !== RelDistributions.ANY)
    }

    /**
     * Creates an Exchange by parsing serialized output.
     */
    protected constructor(input: RelInput) : this(
        input.getCluster(), input.getTraitSet().plus(input.getDistribution()), input.getInput(),
        RelDistributionTraitDef.INSTANCE.canonize(input.getDistribution())
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun copy(
        traitSet: RelTraitSet?,
        inputs: List<RelNode?>?
    ): Exchange {
        return copy(traitSet, sole(inputs), distribution)
    }

    abstract fun copy(
        traitSet: RelTraitSet?, newInput: RelNode?,
        newDistribution: RelDistribution?
    ): Exchange

    /** Returns the distribution of the rows returned by this Exchange.  */
    fun getDistribution(): RelDistribution {
        return distribution
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner,
        mq: RelMetadataQuery
    ): RelOptCost {
        // Higher cost if rows are wider discourages pushing a project through an
        // exchange.
        val rowCount: Double = mq.getRowCount(this)
        val bytesPerRow: Double = getRowType().getFieldCount() * 4
        return planner.getCostFactory().makeCost(
            Util.nLogN(rowCount) * bytesPerRow, rowCount, 0
        )
    }

    @Override
    fun explainTerms(pw: RelWriter?): RelWriter {
        return super.explainTerms(pw)
            .item("distribution", distribution)
    }
}
