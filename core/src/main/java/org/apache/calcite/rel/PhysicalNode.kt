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

import org.apache.calcite.plan.Convention

/**
 * Physical node in a planner that is capable of doing
 * physical trait propagation and derivation.
 *
 *
 * How to use?
 *
 *
 *  1. Enable top-down optimization by setting
 * [org.apache.calcite.plan.volcano.VolcanoPlanner.setTopDownOpt].
 *
 *
 *  1. Let your convention's rel interface extends [PhysicalNode],
 * see [org.apache.calcite.adapter.enumerable.EnumerableRel] as
 * an example.
 *
 *  1. Each physical operator overrides any one of the two methods:
 * [PhysicalNode.passThrough] or
 * [PhysicalNode.passThroughTraits] depending on
 * your needs.
 *
 *  1. Choose derive mode for each physical operator by overriding
 * [PhysicalNode.getDeriveMode].
 *
 *  1. If the derive mode is [DeriveMode.OMAKASE], override
 * method [PhysicalNode.derive] in the physical operator,
 * otherwise, override [PhysicalNode.derive]
 * or [PhysicalNode.deriveTraits].
 *
 *  1. Mark your enforcer operator by overriding [RelNode.isEnforcer],
 * see [Sort.isEnforcer] as an example. This is important,
 * because it can help `VolcanoPlanner` avoid unnecessary
 * trait propagation and derivation, therefore improve optimization
 * efficiency.
 *
 *  1. Implement [Convention.enforce]
 * in your convention, which generates appropriate physical enforcer.
 * See [org.apache.calcite.adapter.enumerable.EnumerableConvention]
 * as example. Simply return `null` if you don't want physical
 * trait enforcement.
 *
 */
interface PhysicalNode : RelNode {
    /**
     * Pass required traitset from parent node to child nodes,
     * returns new node after traits is passed down.
     */
    @Nullable
    fun passThrough(required: RelTraitSet?): RelNode? {
        val p: Pair<RelTraitSet, List<RelTraitSet>> = passThroughTraits(required) ?: return null
        val size: Int = getInputs().size()
        assert(size == p.right.size())
        val list: List<RelNode> = ArrayList(size)
        for (i in 0 until size) {
            val n: RelNode = RelOptRule.convert(getInput(i), p.right.get(i))
            list.add(n)
        }
        return copy(p.left, list)
    }

    /**
     * Pass required traitset from parent node to child nodes,
     * returns a pair of traits after traits is passed down.
     *
     *
     * Pair.left: the new traitset
     *
     * Pair.right: the list of required traitsets for child nodes
     */
    @Nullable
    fun passThroughTraits(
        required: RelTraitSet?
    ): Pair<RelTraitSet?, List<RelTraitSet?>?>? {
        throw RuntimeException(
            getClass().getName()
                    + "#passThroughTraits() is not implemented."
        )
    }

    /**
     * Derive traitset from child node, returns new node after
     * traits derivation.
     */
    @Nullable
    fun derive(childTraits: RelTraitSet?, childId: Int): RelNode? {
        val p: Pair<RelTraitSet, List<RelTraitSet>> = deriveTraits(childTraits, childId) ?: return null
        val size: Int = getInputs().size()
        assert(size == p.right.size())
        val list: List<RelNode> = ArrayList(size)
        for (i in 0 until size) {
            var node: RelNode = getInput(i)
            node = RelOptRule.convert(node, p.right.get(i))
            list.add(node)
        }
        return copy(p.left, list)
    }

    /**
     * Derive traitset from child node, returns a pair of traits after
     * traits derivation.
     *
     *
     * Pair.left: the new traitset
     *
     * Pair.right: the list of required traitsets for child nodes
     */
    @Nullable
    fun deriveTraits(
        childTraits: RelTraitSet?, childId: Int
    ): Pair<RelTraitSet?, List<RelTraitSet?>?>? {
        throw RuntimeException(
            getClass().getName()
                    + "#deriveTraits() is not implemented."
        )
    }

    /**
     * Given a list of child traitsets,
     * inputTraits.size() == getInput().size(),
     * returns node list after traits derivation. This method is called
     * ONLY when the derive mode is OMAKASE.
     */
    fun derive(inputTraits: List<List<RelTraitSet?>?>?): List<RelNode?>? {
        throw RuntimeException(
            getClass().getName()
                    + "#derive() is not implemented."
        )
    }

    /**
     * Returns mode of derivation.
     */
    val deriveMode: DeriveMode?
        get() = DeriveMode.LEFT_FIRST
}
