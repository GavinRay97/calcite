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
 * Relational expression that returns the intersection of the rows of its
 * inputs.
 *
 *
 * If "all" is true, performs then multiset intersection; otherwise,
 * performs set set intersection (implying no duplicates in the results).
 */
abstract class Intersect : SetOp {
    /**
     * Creates an Intersect.
     */
    protected constructor(
        cluster: RelOptCluster?,
        traits: RelTraitSet?,
        inputs: List<RelNode?>?,
        all: Boolean
    ) : super(cluster, traits, inputs, SqlKind.INTERSECT, all) {
    }

    /**
     * Creates an Intersect by parsing serialized output.
     */
    protected constructor(input: RelInput) : super(input) {}

    @Override
    fun estimateRowCount(mq: RelMetadataQuery): Double {
        // REVIEW jvs 30-May-2005:  I just pulled this out of a hat.
        var dRows = Double.MAX_VALUE
        for (input in inputs) {
            val rowCount: Double = mq.getRowCount(input)
                ?: // Assume this input does not reduce row count
                continue
            dRows = Math.min(dRows, rowCount)
        }
        dRows *= 0.25
        return dRows
    }
}
