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
package org.apache.calcite.rel

import org.apache.calcite.plan.RelOptPlanner

/**
 * Definition of the distribution trait.
 *
 *
 * Distribution is a physical property (i.e. a trait) because it can be
 * changed without loss of information. The converter to do this is the
 * [Exchange] operator.
 */
class RelDistributionTraitDef private constructor() : RelTraitDef<RelDistribution?>() {
    val traitClass: Class<RelDistribution>
        @Override get() = RelDistribution::class.java
    val simpleName: String
        @Override get() = "dist"
    val default: org.apache.calcite.rel.RelDistribution
        @Override get() = RelDistributions.ANY

    @Override
    @Nullable
    fun convert(
        planner: RelOptPlanner, rel: RelNode,
        toDistribution: RelDistribution, allowInfiniteCostConverters: Boolean
    ): RelNode {
        if (toDistribution === RelDistributions.ANY) {
            return rel
        }

        // Create a logical sort, then ask the planner to convert its remaining
        // traits (e.g. convert it to an EnumerableSortRel if rel is enumerable
        // convention)
        val exchange: Exchange = LogicalExchange.create(rel, toDistribution)
        var newRel: RelNode = planner.register(exchange, rel)
        val newTraitSet: RelTraitSet = rel.getTraitSet().replace(toDistribution)
        if (!newRel.getTraitSet().equals(newTraitSet)) {
            newRel = planner.changeTraits(newRel, newTraitSet)
        }
        return newRel
    }

    @Override
    fun canConvert(
        planner: RelOptPlanner?, fromTrait: RelDistribution?,
        toTrait: RelDistribution?
    ): Boolean {
        return true
    }

    companion object {
        val INSTANCE = RelDistributionTraitDef()
    }
}
