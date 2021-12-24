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

import org.apache.calcite.plan.RelHintsPropagator
import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelOptRuleOperand
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import java.util.ArrayList
import java.util.List
import java.util.Map

/**
 * HepRuleCall implements [RelOptRuleCall] for a [HepPlanner]. It
 * remembers transformation results so that the planner can choose which one (if
 * any) should replace the original expression.
 */
class HepRuleCall internal constructor(
    planner: RelOptPlanner?,
    operand: RelOptRuleOperand?,
    rels: Array<RelNode?>?,
    nodeChildren: Map<RelNode, List<RelNode?>?>?,
    @Nullable parents: List<RelNode?>?
) : RelOptRuleCall(planner, operand, rels, nodeChildren, parents) {
    //~ Instance fields --------------------------------------------------------
    private val results: List<RelNode>

    //~ Constructors -----------------------------------------------------------
    init {
        results = ArrayList()
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun transformTo(
        rel: RelNode?, equiv: Map<RelNode?, RelNode?>?,
        handler: RelHintsPropagator
    ) {
        var rel: RelNode? = rel
        val rel0: RelNode = rels.get(0)
        RelOptUtil.verifyTypeEquivalence(rel0, rel, rel0)
        rel = handler.propagate(rel0, rel)
        results.add(rel)
        rel0.getCluster().invalidateMetadataQuery()
    }

    fun getResults(): List<RelNode> {
        return results
    }
}
