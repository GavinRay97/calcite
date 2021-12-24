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
package org.apache.calcite.plan.volcano

import org.apache.calcite.plan.RelOptRuleOperand
import org.apache.calcite.rel.RelNode
import org.apache.calcite.util.Litmus
import java.util.List
import java.util.Map

/**
 * A match of a rule to a particular set of target relational expressions,
 * frozen in time.
 */
internal class VolcanoRuleMatch @SuppressWarnings("method.invocation.invalid") constructor(
    volcanoPlanner: VolcanoPlanner, operand0: RelOptRuleOperand?,
    rels: Array<RelNode>, nodeInputs: Map<RelNode?, List<RelNode?>?>?
) : VolcanoRuleCall(volcanoPlanner, operand0, rels.clone(), nodeInputs) {
    //~ Instance fields --------------------------------------------------------
    private var digest: String
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a `VolcanoRuleMatch`.
     *
     * @param operand0 Primary operand
     * @param rels     List of targets; copied by the constructor, so the client
     * can modify it later
     * @param nodeInputs Map from relational expressions to their inputs
     */
    init {
        assert(allNotNull(rels, Litmus.THROW))
        digest = computeDigest()
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun toString(): String {
        return digest
    }

    /**
     * Computes a string describing this rule match. Two rule matches are
     * equivalent if and only if their digests are the same.
     *
     * @return description of this rule match
     */
    private fun computeDigest(): String {
        val buf = StringBuilder("rule [" + getRule().toString() + "] rels [")
        for (i in 0 until rels.length) {
            if (i > 0) {
                buf.append(',')
            }
            buf.append('#').append(rels.get(i).getId())
        }
        buf.append(']')
        return buf.toString()
    }

    /**
     * Recomputes the digest of this VolcanoRuleMatch.
     */
    @Deprecated // to be removed before 2.0
    fun recomputeDigest() {
        digest = computeDigest()
    }

    companion object {
        /** Returns whether all elements of a given array are not-null;
         * fails if any are null.  */
        private fun <E> allNotNull(es: Array<E>, litmus: Litmus): Boolean {
            for (e in es) {
                if (e == null) {
                    return litmus.fail("was null", es as Object)
                }
            }
            return litmus.succeed()
        }
    }
}
