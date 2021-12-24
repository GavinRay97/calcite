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
package org.apache.calcite.rel.mutable

import org.apache.calcite.avatica.util.Spaces

/** Mutable equivalent of [RelNode].
 *
 *
 * Each node has mutable state, and keeps track of its parent and position
 * within parent.
 * It doesn't make sense to canonize `MutableRels`,
 * otherwise one node could end up with multiple parents.
 * It follows that `#hashCode` and `#equals` are less efficient
 * than their `RelNode` counterparts.
 * But, you don't need to copy a `MutableRel` in order to change it.
 * For this reason, you should use `MutableRel` for short-lived
 * operations, and transcribe back to `RelNode` when you are done.
 */
abstract class MutableRel protected constructor(
    cluster: RelOptCluster?,
    rowType: RelDataType?, type: MutableRelType?
) {
    val cluster: RelOptCluster
    val rowType: RelDataType
    val type: MutableRelType

    @get:Nullable
    @Nullable
    var parent: MutableRel? = null
        protected set
    var ordinalInParent = 0

    init {
        this.cluster = Objects.requireNonNull(cluster, "cluster")
        this.rowType = Objects.requireNonNull(rowType, "rowType")
        this.type = Objects.requireNonNull(type, "type")
    }

    abstract fun setInput(ordinalInParent: Int, input: MutableRel?)
    abstract val inputs: List<MutableRel>
    @Override
    abstract fun clone(): MutableRel
    abstract fun childrenAccept(visitor: MutableRelVisitor?)

    /** Replaces this `MutableRel` in its parent with another node at the
     * same position.
     *
     *
     * Before the method, `child` must be an orphan (have null parent)
     * and after this method, this `MutableRel` is an orphan.
     *
     * @return The parent
     */
    @Nullable
    fun replaceInParent(child: MutableRel): MutableRel? {
        val parent = parent
        if (this !== child) {
            if (parent != null) {
                parent.setInput(ordinalInParent, child)
                this.parent = null
                ordinalInParent = 0
            }
        }
        return parent
    }

    abstract fun digest(buf: StringBuilder?): StringBuilder?
    fun deep(): String {
        return MutableRelDumper().apply(this)
    }

    @Override
    override fun toString(): String {
        return deep()
    }

    /**
     * Implementation of MutableVisitor that dumps the details
     * of a MutableRel tree.
     */
    private class MutableRelDumper : MutableRelVisitor() {
        private val buf: StringBuilder = StringBuilder()
        private var level = 0

        @Override
        override fun visit(@Nullable node: MutableRel?) {
            Spaces.append(buf, level * 2)
            if (node == null) {
                buf.append("null")
            } else {
                node.digest(buf)
                buf.append("\n")
                ++level
                super.visit(node)
                --level
            }
        }

        fun apply(rel: MutableRel?): String {
            go(rel)
            return buf.toString()
        }
    }

    companion object {
        /** Equivalence that compares objects by their [Object.toString]
         * method.  */
        protected val STRING_EQUIVALENCE: Equivalence<Object> = object : Equivalence<Object?>() {
            @Override
            protected fun doEquivalent(o: Object, o2: Object): Boolean {
                return o.toString().equals(o2.toString())
            }

            @Override
            protected fun doHash(o: Object): Int {
                return o.toString().hashCode()
            }
        }

        /** Equivalence that compares [Lists]s by the
         * [Object.toString] of their elements.  */
        @SuppressWarnings("unchecked")
        protected val PAIRWISE_STRING_EQUIVALENCE: Equivalence<List<*>> = STRING_EQUIVALENCE.pairwise() as Equivalence
    }
}
