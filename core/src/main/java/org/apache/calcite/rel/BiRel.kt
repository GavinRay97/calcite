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
 * Abstract base class for relational expressions with a two inputs.
 *
 *
 * It is not required that two-input relational expressions use this
 * class as a base class. However, default implementations of methods make life
 * easier.
 */
abstract class BiRel protected constructor(
    cluster: RelOptCluster?, traitSet: RelTraitSet?, left: RelNode,
    right: RelNode
) : AbstractRelNode(cluster, traitSet) {
    protected var left: RelNode
    protected var right: RelNode

    init {
        this.left = left
        this.right = right
    }

    @Override
    override fun childrenAccept(visitor: RelVisitor) {
        visitor.visit(left, 0, this)
        visitor.visit(right, 1, this)
    }

    override val inputs: List<org.apache.calcite.rel.RelNode?>?
        @Override get() = FlatLists.of(left, right)

    fun getLeft(): RelNode {
        return left
    }

    fun getRight(): RelNode {
        return right
    }

    @Override
    fun replaceInput(
        ordinalInParent: Int,
        p: RelNode
    ) {
        when (ordinalInParent) {
            0 -> left = p
            1 -> right = p
            else -> throw IndexOutOfBoundsException("Input $ordinalInParent")
        }
        recomputeDigest()
    }

    @Override
    override fun explainTerms(pw: RelWriter): RelWriter {
        return super.explainTerms(pw)
            .input("left", left)
            .input("right", right)
    }
}
