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
package org.apache.calcite.plan.hep

import org.apache.calcite.plan.RelOptCost
import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.AbstractRelNode
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.metadata.DelegatingMetadataRel
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.type.RelDataType
import java.util.List

/**
 * HepRelVertex wraps a real [RelNode] as a vertex in a DAG representing
 * the entire query expression.
 */
class HepRelVertex internal constructor(rel: RelNode?) : AbstractRelNode(
    rel.getCluster(),
    rel.getTraitSet()
), DelegatingMetadataRel {
    //~ Instance fields --------------------------------------------------------
    /**
     * Wrapped rel currently chosen for implementation of expression.
     */
    private var currentRel: RelNode?

    //~ Constructors -----------------------------------------------------------
    init {
        currentRel = rel
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun explain(pw: RelWriter?) {
        currentRel.explain(pw)
    }

    @Override
    fun copy(traitSet: RelTraitSet, inputs: List<RelNode?>): RelNode {
        assert(traitSet.equals(traitSet))
        assert(inputs.equals(this.getInputs()))
        return this
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner,
        mq: RelMetadataQuery?
    ): RelOptCost {
        // HepRelMetadataProvider is supposed to intercept this
        // and redirect to the real rels. But sometimes it doesn't.
        return planner.getCostFactory().makeTinyCost()
    }

    @Override
    fun estimateRowCount(mq: RelMetadataQuery): Double {
        return mq.getRowCount(currentRel)
    }

    @Override
    protected fun deriveRowType(): RelDataType {
        return currentRel.getRowType()
    }

    /**
     * Replaces the implementation for this expression with a new one.
     *
     * @param newRel new expression
     */
    fun replaceRel(newRel: RelNode?) {
        currentRel = newRel
    }

    /**
     * Returns current implementation chosen for this vertex.
     */
    fun getCurrentRel(): RelNode? {
        return currentRel
    }

    /**
     * Returns [RelNode] for metadata.
     */
    @get:Override
    val metadataDelegateRel: RelNode?
        get() = currentRel

    @Override
    fun deepEquals(@Nullable obj: Object): Boolean {
        return (this === obj
                || (obj is HepRelVertex
                && currentRel === (obj as HepRelVertex).currentRel))
    }

    @Override
    fun deepHashCode(): Int {
        return currentRel.getId()
    }

    @get:Override
    val digest: String
        get() = "HepRelVertex($currentRel)"
}
