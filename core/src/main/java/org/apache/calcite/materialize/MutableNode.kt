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

import org.apache.calcite.util.mapping.IntPair

/** Mutable version of [LatticeNode], used while a graph is being
 * built.  */
internal class MutableNode @SuppressWarnings("argument.type.incompatible") constructor(
    table: LatticeTable?,
    @Nullable parent: MutableNode?,
    @Nullable step: Step?
) {
    val table: LatticeTable

    @Nullable
    val parent: MutableNode?

    @Nullable
    val step: Step?
    var startCol = 0
    var endCol = 0

    @Nullable
    var alias: String? = null
    val children: List<MutableNode> = ArrayList()

    /** Creates a root node.  */
    constructor(table: LatticeTable?) : this(table, null, null) {}

    /** Creates a non-root node.  */
    init {
        this.table = Objects.requireNonNull(table, "table")
        this.parent = parent
        this.step = step
        if (parent != null) {
            parent.children.add(this)
            Collections.sort(parent.children, ORDERING)
        }
    }

    /** Populates a flattened list of mutable nodes.  */
    fun flatten(flatNodes: List<MutableNode?>) {
        flatNodes.add(this)
        for (child in children) {
            child.flatten(flatNodes)
        }
    }

    /** Returns whether this node is cylic, in an undirected sense; that is,
     * whether the same descendant can be reached by more than one route.  */
    val isCyclic: Boolean
        get() {
            val descendants: Set<MutableNode> = HashSet()
            return isCyclicRecurse(descendants)
        }

    private fun isCyclicRecurse(descendants: Set<MutableNode>): Boolean {
        if (!descendants.add(this)) {
            return true
        }
        for (child in children) {
            if (child.isCyclicRecurse(descendants)) {
                return true
            }
        }
        return false
    }

    fun addPath(path: Path, @Nullable alias: String?) {
        var n = this
        for (step1 in path.steps) {
            var n2 = n.findChild(step1)
            if (n2 == null) {
                n2 = MutableNode(step1.target(), n, step1)
                if (alias != null) {
                    n2.alias = alias
                }
            }
            n = n2
        }
    }

    @Nullable
    private fun findChild(step: Step): MutableNode? {
        for (child in children) {
            if (Objects.equals(child.table, step.target())
                && Objects.equals(child.step, step)
            ) {
                return child
            }
        }
        return null
    }

    companion object {
        /** Comparator for sorting children within a parent.  */
        val ORDERING: Ordering<MutableNode> = Ordering.from(
            object : Comparator<MutableNode?>() {
                @Override
                fun compare(o1: MutableNode, o2: MutableNode): Int {
                    var c: Int = Ordering.< String > natural < String ? > ().lexicographical().compare(
                        o1.table.t.getQualifiedName(), o2.table.t.getQualifiedName()
                    )
                    if (c == 0 && o1.step != null && o2.step != null) {
                        // The nodes have the same table. Now compare them based on the
                        // columns they use as foreign key.
                        c = Ordering.< Integer > natural < Integer ? > ().lexicographical().compare(
                            IntPair.left(o1.step.keys), IntPair.left(o2.step.keys)
                        )
                    }
                    return c
                }
            })
    }
}
