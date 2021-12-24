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
package org.apache.calcite.materialize

import org.apache.calcite.util.Litmus

/** Root node in a [Lattice]. It has no parent.  */
class LatticeRootNode @SuppressWarnings("method.invocation.invalid") internal constructor(
    space: LatticeSpace,
    mutableNode: MutableNode?
) : LatticeNode(space, null, mutableNode) {
    /** Descendants, in prefix order. This root node is at position 0.  */
    val descendants: ImmutableList<LatticeNode>
    val paths: ImmutableList<Path>

    init {
        val b: ImmutableList.Builder<LatticeNode> = ImmutableList.builder()
        flattenTo(b)
        descendants = b.build()
        paths = createPaths(space)
    }

    private fun createPaths(space: LatticeSpace): ImmutableList<Path> {
        val steps: List<Step> = ArrayList()
        val paths: List<Path> = ArrayList()
        createPathsRecurse(space, steps, paths)
        assert(steps.isEmpty())
        return ImmutableList.copyOf(paths)
    }

    @Override
    fun use(usedNodes: List<LatticeNode?>) {
        if (!usedNodes.contains(this)) {
            usedNodes.add(this)
        }
    }

    /** Validates that nodes form a tree; each node except the first references
     * a predecessor.  */
    fun isValid(litmus: Litmus): Boolean {
        for (i in 0 until descendants.size()) {
            val node: LatticeNode = descendants.get(i)
            if (i == 0) {
                if (node !== this) {
                    return litmus.fail("node 0 should be root")
                }
            } else {
                if (node !is LatticeChildNode) {
                    return litmus.fail("node after 0 should be child")
                }
                if (!descendants.subList(0, i).contains(node.parent)) {
                    return litmus.fail("parent not in preceding list")
                }
            }
        }
        return litmus.succeed()
    }

    /** Whether this node's graph is a super-set of (or equal to) another node's
     * graph.  */
    operator fun contains(node: LatticeRootNode): Boolean {
        return paths.containsAll(node.paths)
    }
}
