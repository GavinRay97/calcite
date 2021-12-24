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

/** Implementation of [org.apache.calcite.rel.core.Project] in
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention].  */
class EnumerableProject(
    cluster: RelOptCluster?,
    traitSet: RelTraitSet?,
    input: RelNode?,
    projects: List<RexNode?>?,
    rowType: RelDataType?
) : Project(cluster, traitSet, ImmutableList.of(), input, projects, rowType), EnumerableRel {
    /**
     * Creates an EnumerableProject.
     *
     *
     * Use [.create] unless you know what you're doing.
     *
     * @param cluster  Cluster this relational expression belongs to
     * @param traitSet Traits of this relational expression
     * @param input    Input relational expression
     * @param projects List of expressions for the input columns
     * @param rowType  Output row type
     */
    init {
        assert(getConvention() is EnumerableConvention)
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster?, traitSet: RelTraitSet?,
        input: RelNode?, projects: List<RexNode?>?, rowType: RelDataType?,
        flags: Int
    ) : this(cluster, traitSet, input, projects, rowType) {
        Util.discard(flags)
    }

    @Override
    fun copy(
        traitSet: RelTraitSet?, input: RelNode?,
        projects: List<RexNode?>?, rowType: RelDataType?
    ): EnumerableProject {
        return EnumerableProject(
            getCluster(), traitSet, input,
            projects, rowType
        )
    }

    @Override
    override fun implement(implementor: EnumerableRelImplementor?, pref: Prefer?): Result {
        // EnumerableCalcRel is always better
        throw UnsupportedOperationException()
    }

    @Override
    @Nullable
    override fun passThroughTraits(
        required: RelTraitSet?
    ): Pair<RelTraitSet, List<RelTraitSet>> {
        return EnumerableTraitsUtils.passThroughTraitsForProject(
            required, exps,
            input.getRowType(), input.getCluster().getTypeFactory(), traitSet
        )
    }

    @Override
    @Nullable
    override fun deriveTraits(
        childTraits: RelTraitSet?, childId: Int
    ): Pair<RelTraitSet, List<RelTraitSet>> {
        return EnumerableTraitsUtils.deriveTraitsForProject(
            childTraits, childId, exps,
            input.getRowType(), input.getCluster().getTypeFactory(), traitSet
        )
    }

    companion object {
        /** Creates an EnumerableProject, specifying row type rather than field
         * names.  */
        fun create(
            input: RelNode,
            projects: List<RexNode?>?, rowType: RelDataType?
        ): EnumerableProject {
            val cluster: RelOptCluster = input.getCluster()
            val mq: RelMetadataQuery = cluster.getMetadataQuery()
            val traitSet: RelTraitSet = cluster.traitSet().replace(EnumerableConvention.INSTANCE)
                .replaceIfs(
                    RelCollationTraitDef.INSTANCE
                ) { RelMdCollation.project(mq, input, projects) }
            return EnumerableProject(cluster, traitSet, input, projects, rowType)
        }
    }
}
