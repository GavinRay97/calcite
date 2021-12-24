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
 * Sub-class of [org.apache.calcite.rel.core.SortExchange] not
 * targeted at any particular engine or calling convention.
 */
class LogicalSortExchange : SortExchange {
    private constructor(
        cluster: RelOptCluster, traitSet: RelTraitSet,
        input: RelNode, distribution: RelDistribution, collation: RelCollation
    ) : super(cluster, traitSet, input, distribution, collation) {
    }

    /**
     * Creates a LogicalSortExchange by parsing serialized output.
     */
    constructor(input: RelInput?) : super(input) {}

    //~ Methods ----------------------------------------------------------------
    @Override
    fun copy(
        traitSet: RelTraitSet, newInput: RelNode,
        newDistribution: RelDistribution, newCollation: RelCollation
    ): SortExchange {
        return LogicalSortExchange(
            this.getCluster(), traitSet, newInput,
            newDistribution, newCollation
        )
    }

    companion object {
        /**
         * Creates a LogicalSortExchange.
         *
         * @param input     Input relational expression
         * @param distribution Distribution specification
         * @param collation array of sort specifications
         */
        fun create(
            input: RelNode,
            distribution: RelDistribution,
            collation: RelCollation
        ): LogicalSortExchange {
            var distribution: RelDistribution = distribution
            var collation: RelCollation = collation
            val cluster: RelOptCluster = input.getCluster()
            collation = RelCollationTraitDef.INSTANCE.canonize(collation)
            distribution = RelDistributionTraitDef.INSTANCE.canonize(distribution)
            val traitSet: RelTraitSet =
                input.getTraitSet().replace(Convention.NONE).replace(distribution).replace(collation)
            return LogicalSortExchange(
                cluster, traitSet, input, distribution,
                collation
            )
        }
    }
}
