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

import org.apache.calcite.plan.RelOptTable

/** Source relation of a lattice.
 *
 *
 * Relations form a tree; all relations except the root relation
 * (the fact table) have precisely one parent and an equi-join
 * condition on one or more pairs of columns linking to it.  */
abstract class LatticeNode internal constructor(
    space: LatticeSpace,
    @Nullable parent: LatticeNode?,
    mutableNode: MutableNode?
) {
    val table: LatticeTable
    val startCol: Int
    val endCol: Int

    @Nullable
    val alias: String
    private var children: ImmutableList<LatticeChildNode>? = null
    val digest: String

    /** Creates a LatticeNode.
     *
     *
     * The `parent` and `mutableNode` arguments are used only
     * during construction.  */
    init {
        table = requireNonNull(mutableNode.table)
        startCol = mutableNode.startCol
        endCol = mutableNode.endCol
        alias = mutableNode.alias
        Preconditions.checkArgument(startCol >= 0)
        Preconditions.checkArgument(endCol > startCol)
        val sb: StringBuilder = StringBuilder()
            .append(space.simpleName(table))
        if (parent != null) {
            sb.append(':')
            var i = 0
            for (p in requireNonNull(mutableNode.step, "mutableNode.step").keys) {
                if (i++ > 0) {
                    sb.append(",")
                }
                sb.append(space.fieldName(parent.table, p.source))
            }
        }
        if (mutableNode.children.isEmpty()) {
            children = ImmutableList.of()
        } else {
            sb.append(" (")
            val b: ImmutableList.Builder<LatticeChildNode> = ImmutableList.builder()
            var i = 0
            for (mutableChild in mutableNode.children) {
                if (i++ > 0) {
                    sb.append(' ')
                }
                @SuppressWarnings(["argument.type.incompatible", "assignment.type.incompatible"]) @Initialized val node =
                    LatticeChildNode(space, this, mutableChild)
                sb.append(node.digest)
                b.add(node)
            }
            children = b.build()
            sb.append(")")
        }
        digest = sb.toString()
    }

    @Override
    override fun toString(): String {
        return digest
    }

    fun relOptTable(): RelOptTable {
        return table.t
    }

    abstract fun use(usedNodes: List<LatticeNode?>?)
    fun flattenTo(builder: ImmutableList.Builder<LatticeNode?>) {
        builder.add(this)
        for (child in children) {
            child.flattenTo(builder)
        }
    }

    fun createPathsRecurse(
        space: LatticeSpace, steps: List<Step?>,
        paths: List<Path?>
    ) {
        paths.add(space.addPath(steps))
        for (child in children) {
            steps.add(space.addEdge(table, child.table, child.link))
            child.createPathsRecurse(space, steps, paths)
            steps.remove(steps.size() - 1)
        }
    }
}
