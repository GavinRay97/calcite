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
 * Sub-class of [Exchange] not
 * targeted at any particular engine or calling convention.
 */
class LogicalExchange : Exchange {
    private constructor(
        cluster: RelOptCluster, traitSet: RelTraitSet,
        input: RelNode, distribution: RelDistribution
    ) : super(cluster, traitSet, input, distribution) {
        assert(traitSet.containsIfApplicable(Convention.NONE))
    }

    /**
     * Creates a LogicalExchange by parsing serialized output.
     */
    constructor(input: RelInput?) : super(input) {}

    //~ Methods ----------------------------------------------------------------
    @Override
    fun copy(
        traitSet: RelTraitSet, newInput: RelNode,
        newDistribution: RelDistribution
    ): Exchange {
        return LogicalExchange(
            getCluster(), traitSet, newInput,
            newDistribution
        )
    }

    @Override
    fun accept(shuttle: RelShuttle): RelNode {
        return shuttle.visit(this)
    }

    companion object {
        /**
         * Creates a LogicalExchange.
         *
         * @param input     Input relational expression
         * @param distribution Distribution specification
         */
        fun create(
            input: RelNode,
            distribution: RelDistribution
        ): LogicalExchange {
            var distribution: RelDistribution = distribution
            val cluster: RelOptCluster = input.getCluster()
            distribution = RelDistributionTraitDef.INSTANCE.canonize(distribution)
            val traitSet: RelTraitSet = input.getTraitSet().replace(Convention.NONE).replace(distribution)
            return LogicalExchange(cluster, traitSet, input, distribution)
        }
    }
}
