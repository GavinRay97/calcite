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
package org.apache.calcite.adapter.enumerable

import org.apache.calcite.plan.RelOptCluster

/** Implementation of [org.apache.calcite.rel.core.Filter] in
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention].  */
class EnumerableFilter(
    cluster: RelOptCluster?,
    traitSet: RelTraitSet?,
    child: RelNode?,
    condition: RexNode?
) : Filter(cluster, traitSet, child, condition), EnumerableRel {
    /** Creates an EnumerableFilter.
     *
     *
     * Use [.create] unless you know what you're doing.  */
    init {
        assert(getConvention() is EnumerableConvention)
    }

    @Override
    fun copy(
        traitSet: RelTraitSet?, input: RelNode?,
        condition: RexNode?
    ): EnumerableFilter {
        return EnumerableFilter(getCluster(), traitSet, input, condition)
    }

    @Override
    override fun implement(implementor: EnumerableRelImplementor?, pref: Prefer?): Result {
        // EnumerableCalc is always better
        throw UnsupportedOperationException()
    }

    @Override
    @Nullable
    override fun passThroughTraits(
        required: RelTraitSet
    ): Pair<RelTraitSet, List<RelTraitSet>>? {
        val collation: RelCollation = required.getCollation()
        if (collation == null || collation === RelCollations.EMPTY) {
            return null
        }
        val traits: RelTraitSet = traitSet.replace(collation)
        return Pair.of(traits, ImmutableList.of(traits))
    }

    @Override
    @Nullable
    override fun deriveTraits(
        childTraits: RelTraitSet, childId: Int
    ): Pair<RelTraitSet, List<RelTraitSet>>? {
        val collation: RelCollation = childTraits.getCollation()
        if (collation == null || collation === RelCollations.EMPTY) {
            return null
        }
        val traits: RelTraitSet = traitSet.replace(collation)
        return Pair.of(traits, ImmutableList.of(traits))
    }

    companion object {
        /** Creates an EnumerableFilter.  */
        fun create(
            input: RelNode,
            condition: RexNode?
        ): EnumerableFilter {
            val cluster: RelOptCluster = input.getCluster()
            val mq: RelMetadataQuery = cluster.getMetadataQuery()
            val traitSet: RelTraitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE)
                .replaceIfs(
                    RelCollationTraitDef.INSTANCE
                ) { RelMdCollation.filter(mq, input) }
                .replaceIf(
                    RelDistributionTraitDef.INSTANCE
                ) { RelMdDistribution.filter(mq, input) }
            return EnumerableFilter(cluster, traitSet, input, condition)
        }
    }
}
