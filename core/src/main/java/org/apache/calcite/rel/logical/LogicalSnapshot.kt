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
package org.apache.calcite.rel.logical

import org.apache.calcite.plan.Convention

/**
 * Sub-class of [org.apache.calcite.rel.core.Snapshot]
 * not targeted at any particular engine or calling convention.
 */
class LogicalSnapshot  //~ Constructors -----------------------------------------------------------
/**
 * Creates a LogicalSnapshot.
 *
 *
 * Use [.create] unless you know what you're doing.
 *
 * @param cluster   Cluster that this relational expression belongs to
 * @param traitSet  The traits of this relational expression
 * @param input     Input relational expression
 * @param period    Timestamp expression which as the table was at the given
 * time in the past
 */
    (
    cluster: RelOptCluster?, traitSet: RelTraitSet?,
    input: RelNode?, period: RexNode?
) : Snapshot(cluster, traitSet, input, period) {
    @Override
    fun copy(
        traitSet: RelTraitSet?, input: RelNode?,
        period: RexNode?
    ): Snapshot {
        return LogicalSnapshot(getCluster(), traitSet, input, period)
    }

    companion object {
        /** Creates a LogicalSnapshot.  */
        fun create(input: RelNode, period: RexNode?): LogicalSnapshot {
            val cluster: RelOptCluster = input.getCluster()
            val mq: RelMetadataQuery = cluster.getMetadataQuery()
            val traitSet: RelTraitSet = cluster.traitSet()
                .replace(Convention.NONE)
                .replaceIfs(
                    RelCollationTraitDef.INSTANCE
                ) { RelMdCollation.snapshot(mq, input) }
                .replaceIf(
                    RelDistributionTraitDef.INSTANCE
                ) { RelMdDistribution.snapshot(mq, input) }
            return LogicalSnapshot(cluster, traitSet, input, period)
        }
    }
}
