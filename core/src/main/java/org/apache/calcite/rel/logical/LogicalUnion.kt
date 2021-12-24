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
 * Sub-class of [org.apache.calcite.rel.core.Union]
 * not targeted at any particular engine or calling convention.
 */
class LogicalUnion : Union {
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a LogicalUnion.
     *
     *
     * Use [.create] unless you know what you're doing.
     */
    constructor(
        cluster: RelOptCluster?,
        traitSet: RelTraitSet?,
        inputs: List<RelNode?>?,
        all: Boolean
    ) : super(cluster, traitSet, inputs, all) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster, inputs: List<RelNode?>?,
        all: Boolean
    ) : this(cluster, cluster.traitSetOf(Convention.NONE), inputs, all) {
    }

    /**
     * Creates a LogicalUnion by parsing serialized output.
     */
    constructor(input: RelInput?) : super(input) {}

    //~ Methods ----------------------------------------------------------------
    @Override
    fun copy(
        traitSet: RelTraitSet, inputs: List<RelNode?>?, all: Boolean
    ): LogicalUnion {
        assert(traitSet.containsIfApplicable(Convention.NONE))
        return LogicalUnion(getCluster(), traitSet, inputs, all)
    }

    @Override
    fun accept(shuttle: RelShuttle): RelNode {
        return shuttle.visit(this)
    }

    companion object {
        /** Creates a LogicalUnion.  */
        fun create(inputs: List<RelNode>, all: Boolean): LogicalUnion {
            val cluster: RelOptCluster = inputs[0].getCluster()
            val traitSet: RelTraitSet = cluster.traitSetOf(Convention.NONE)
            return LogicalUnion(cluster, traitSet, inputs, all)
        }
    }
}
