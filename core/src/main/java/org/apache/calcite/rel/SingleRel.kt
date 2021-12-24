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

import org.apache.calcite.plan.RelOptCluster

/**
 * Abstract base class for relational expressions with a single input.
 *
 *
 * It is not required that single-input relational expressions use this
 * class as a base class. However, default implementations of methods make life
 * easier.
 */
abstract class SingleRel protected constructor(
    cluster: RelOptCluster?,
    traits: RelTraitSet?,
    input: RelNode
) : AbstractRelNode(cluster, traits) {
    //~ Instance fields --------------------------------------------------------
    protected var input: RelNode
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a `SingleRel`.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param input   Input relational expression
     */
    init {
        this.input = input
    }

    //~ Methods ----------------------------------------------------------------
    fun getInput(): RelNode {
        return input
    }

    override val inputs: List<org.apache.calcite.rel.RelNode?>?
        @Override get() = ImmutableList.of(input)

    @Override
    override fun estimateRowCount(mq: RelMetadataQuery): Double {
        // Not necessarily correct, but a better default than AbstractRelNode's 1.0
        return mq.getRowCount(input)
    }

    @Override
    override fun childrenAccept(visitor: RelVisitor) {
        visitor.visit(input, 0, this)
    }

    @Override
    override fun explainTerms(pw: RelWriter): RelWriter {
        return super.explainTerms(pw)
            .input("input", getInput())
    }

    @Override
    fun replaceInput(
        ordinalInParent: Int,
        rel: RelNode
    ) {
        assert(ordinalInParent == 0)
        input = rel
        recomputeDigest()
    }

    @Override
    protected override fun deriveRowType(): RelDataType {
        return input.getRowType()
    }
}
