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
package org.apache.calcite.rel.metadata

import org.apache.calcite.plan.RelOptCost

/**
 * Default implementations of the
 * [BuiltInMetadata.LowerBoundCost]
 * metadata provider for the standard algebra.
 */
class RelMdLowerBoundCost  //~ Constructors -----------------------------------------------------------
protected constructor() : MetadataHandler<LowerBoundCost?> {
    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        get() = BuiltInMetadata.LowerBoundCost.DEF

    @Nullable
    fun getLowerBoundCost(
        subset: RelSubset,
        mq: RelMetadataQuery?, planner: VolcanoPlanner
    ): RelOptCost? {
        return if (planner.isLogical(subset)) {
            // currently only support physical, will improve in the future
            null
        } else subset.getWinnerCost()
    }

    @Nullable
    fun getLowerBoundCost(
        node: RelNode,
        mq: RelMetadataQuery, planner: VolcanoPlanner
    ): RelOptCost? {
        if (planner.isLogical(node)) {
            // currently only support physical, will improve in the future
            return null
        }
        var selfCost: RelOptCost? = mq.getNonCumulativeCost(node)
        if (selfCost != null && selfCost.isInfinite()) {
            selfCost = null
        }
        for (input in node.getInputs()) {
            val lb: RelOptCost = mq.getLowerBoundCost(input, planner)
            if (lb != null) {
                selfCost = if (selfCost == null) lb else selfCost.plus(lb)
            }
        }
        return selfCost
    }

    companion object {
        val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
            RelMdLowerBoundCost(), LowerBoundCost.Handler::class.java
        )
    }
}
