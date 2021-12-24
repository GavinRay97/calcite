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
package org.apache.calcite.plan

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.RelVisitor
import org.apache.calcite.util.Util

/**
 * RelTraitPropagationVisitor traverses a RelNode and its *unregistered*
 * children, making sure that each has a full complement of traits. When a
 * RelNode is found to be missing one or more traits, they are copied from a
 * RelTraitSet given during construction.
 *
 */
@Deprecated
@Deprecated(
    """As of 1.19, if you need to perform certain assertions regarding a RelNode tree and
  the contained traits you are encouraged to implement your own RelVisitor or
  {@link org.apache.calcite.rel.RelShuttle} directly. The reasons for deprecating this class are
  the following:
  <ul>
    <li>The contract (Javadoc and naming) and the behavior of the class are inconsistent.</li>
    <li>The class is no longer used by any other components of the framework.</li>
    <li>The class was used only for debugging purposes.</li>
  </ul>"""
)
class RelTraitPropagationVisitor(
    planner: RelOptPlanner,
    baseTraits: RelTraitSet
) : RelVisitor() {
    //~ Instance fields --------------------------------------------------------
    private val baseTraits: RelTraitSet
    private val planner: RelOptPlanner

    //~ Constructors -----------------------------------------------------------
    init {
        this.planner = planner
        this.baseTraits = baseTraits
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun visit(rel: RelNode, ordinal: Int, @Nullable parent: RelNode?) {
        // REVIEW: SWZ: 1/31/06: We assume that any special RelNodes, such
        // as the VolcanoPlanner's RelSubset always have a full complement
        // of traits and that they either appear as registered or do nothing
        // when childrenAccept is called on them.
        if (planner.isRegistered(rel)) {
            return
        }
        val relTraits: RelTraitSet = rel.getTraitSet()
        for (i in 0 until baseTraits.size()) {
            if (i >= relTraits.size()) {
                // Copy traits that the new rel doesn't know about.
                Util.discard(
                    RelOptUtil.addTrait(
                        rel,
                        baseTraits.getTrait(i)
                    )
                )
                throw AssertionError()
            } else {
                // Verify that the traits are from the same RelTraitDef
                assert(
                    relTraits.getTrait(i).getTraitDef()
                            === baseTraits.getTrait(i).getTraitDef()
                )
            }
        }
        rel.childrenAccept(this)
    }
}
