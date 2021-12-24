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

import org.apache.calcite.linq4j.function.Experimental

/**
 * Sub-class of [org.apache.calcite.rel.core.RepeatUnion]
 * not targeted at any particular engine or calling convention.
 *
 *
 * NOTE: The current API is experimental and subject to change without
 * notice.
 */
@Experimental
class LogicalRepeatUnion  //~ Constructors -----------------------------------------------------------
private constructor(
    cluster: RelOptCluster, traitSet: RelTraitSet,
    seed: RelNode?, iterative: RelNode?, all: Boolean, iterationLimit: Int
) : RepeatUnion(cluster, traitSet, seed, iterative, all, iterationLimit) {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun copy(
        traitSet: RelTraitSet,
        inputs: List<RelNode?>
    ): LogicalRepeatUnion {
        assert(traitSet.containsIfApplicable(Convention.NONE))
        assert(inputs.size() === 2)
        return LogicalRepeatUnion(
            getCluster(), traitSet,
            inputs[0], inputs[1], all, iterationLimit
        )
    }

    companion object {
        /** Creates a LogicalRepeatUnion.  */
        fun create(
            seed: RelNode, iterative: RelNode?,
            all: Boolean
        ): LogicalRepeatUnion {
            return create(seed, iterative, all, -1)
        }

        /** Creates a LogicalRepeatUnion.  */
        fun create(
            seed: RelNode, iterative: RelNode?,
            all: Boolean, iterationLimit: Int
        ): LogicalRepeatUnion {
            val cluster: RelOptCluster = seed.getCluster()
            val traitSet: RelTraitSet = cluster.traitSetOf(Convention.NONE)
            return LogicalRepeatUnion(cluster, traitSet, seed, iterative, all, iterationLimit)
        }
    }
}
