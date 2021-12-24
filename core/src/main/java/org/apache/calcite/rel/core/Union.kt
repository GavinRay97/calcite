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

import org.apache.calcite.plan.RelOptCluster

/**
 * Relational expression that returns the union of the rows of its inputs,
 * optionally eliminating duplicates.
 *
 *
 * Corresponds to SQL `UNION` and `UNION ALL`.
 */
abstract class Union : SetOp {
    //~ Constructors -----------------------------------------------------------
    protected constructor(
        cluster: RelOptCluster?,
        traits: RelTraitSet?,
        inputs: List<RelNode?>?,
        all: Boolean
    ) : super(cluster, traits, inputs, SqlKind.UNION, all) {
    }

    /**
     * Creates a Union by parsing serialized output.
     */
    protected constructor(input: RelInput) : super(input) {}

    //~ Methods ----------------------------------------------------------------
    @Override
    fun estimateRowCount(mq: RelMetadataQuery?): Double {
        var dRows: Double = RelMdUtil.getUnionAllRowCount(mq, this)
        if (!all) {
            dRows *= 0.5
        }
        return dRows
    }

    companion object {
        @Deprecated // to be removed before 2.0
        fun estimateRowCount(rel: RelNode): Double {
            val mq: RelMetadataQuery = rel.getCluster().getMetadataQuery()
            return RelMdUtil.getUnionAllRowCount(mq, rel as Union)
        }
    }
}
