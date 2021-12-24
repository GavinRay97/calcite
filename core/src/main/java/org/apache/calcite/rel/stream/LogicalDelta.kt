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
package org.apache.calcite.rel.stream

import org.apache.calcite.plan.Convention
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelInput
import org.apache.calcite.rel.RelNode
import java.util.List

/**
 * Sub-class of [org.apache.calcite.rel.stream.Delta]
 * not targeted at any particular engine or calling convention.
 */
class LogicalDelta : Delta {
    /**
     * Creates a LogicalDelta.
     *
     *
     * Use [.create] unless you know what you're doing.
     *
     * @param cluster   Cluster that this relational expression belongs to
     * @param input     Input relational expression
     */
    constructor(
        cluster: RelOptCluster?, traits: RelTraitSet?,
        input: RelNode?
    ) : super(cluster, traits, input) {
    }

    /** Creates a LogicalDelta by parsing serialized output.  */
    constructor(input: RelInput) : super(input) {}

    @Override
    fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>?): RelNode {
        return LogicalDelta(getCluster(), traitSet, sole(inputs))
    }

    companion object {
        /** Creates a LogicalDelta.  */
        fun create(input: RelNode): LogicalDelta {
            val traitSet: RelTraitSet = input.getTraitSet().replace(Convention.NONE)
            return LogicalDelta(input.getCluster(), traitSet, input)
        }
    }
}
