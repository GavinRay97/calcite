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
 * Relational expression that returns the rows of its first input minus any
 * matching rows from its other inputs.
 *
 *
 * Corresponds to the SQL `EXCEPT` operator.
 *
 *
 * If "all" is true, then multiset subtraction is
 * performed; otherwise, set subtraction is performed (implying no duplicates in
 * the results).
 */
abstract class Minus : SetOp {
    protected constructor(
        cluster: RelOptCluster?, traits: RelTraitSet?, inputs: List<RelNode?>?,
        all: Boolean
    ) : super(cluster, traits, inputs, SqlKind.EXCEPT, all) {
    }

    /**
     * Creates a Minus by parsing serialized output.
     */
    protected constructor(input: RelInput) : super(input) {}

    @Override
    fun estimateRowCount(mq: RelMetadataQuery?): Double {
        return RelMdUtil.getMinusRowCount(mq, this)
    }
}
