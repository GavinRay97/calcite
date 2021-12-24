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
package org.apache.calcite.rel.core

import org.apache.calcite.linq4j.function.Experimental

/**
 * Relational expression that computes a repeat union (recursive union in SQL
 * terminology).
 *
 *
 * This operation is executed as follows:
 *
 *
 *  * Evaluate the left input (i.e., seed relational expression) once.  For
 * UNION (but not UNION ALL), discard duplicated rows.
 *
 *  * Evaluate the right input (i.e., iterative relational expression) over and
 * over until it produces no more results (or until an optional maximum number
 * of iterations is reached).  For UNION (but not UNION ALL), discard
 * duplicated results.
 *
 *
 *
 * NOTE: The current API is experimental and subject to change without
 * notice.
 */
@Experimental
abstract class RepeatUnion  //~ Constructors -----------------------------------------------------------
protected constructor(
    cluster: RelOptCluster?, traitSet: RelTraitSet?,
    seed: RelNode?, iterative: RelNode?,
    /**
     * Whether duplicates are considered.
     */
    val all: Boolean,
    /**
     * Maximum number of times to repeat the iterative relational expression;
     * negative value means no limit, 0 means only seed will be evaluated.
     */
    val iterationLimit: Int
) : BiRel(cluster, traitSet, seed, iterative) {
    @Override
    fun estimateRowCount(mq: RelMetadataQuery): Double {
        // TODO implement a more accurate row count?
        val seedRowCount: Double = mq.getRowCount(seedRel)
        return if (iterationLimit == 0) {
            seedRowCount
        } else seedRowCount
                + mq.getRowCount(iterativeRel) * if (iterationLimit < 0) 10 else iterationLimit
    }

    @Override
    fun explainTerms(pw: RelWriter): RelWriter {
        super.explainTerms(pw)
        if (iterationLimit >= 0) {
            pw.item("iterationLimit", iterationLimit)
        }
        return pw.item("all", all)
    }

    val seedRel: RelNode
        get() = left
    val iterativeRel: RelNode
        get() = right

    @Override
    protected fun deriveRowType(): RelDataType {
        val inputRowTypes: List<RelDataType> = Util.transform(getInputs(), RelNode::getRowType)
        return getCluster().getTypeFactory().leastRestrictive(inputRowTypes)
            ?: throw IllegalArgumentException(
                "Cannot compute compatible row type "
                        + "for arguments: "
                        + Util.sepList(inputRowTypes, ", ")
            )
    }
}
