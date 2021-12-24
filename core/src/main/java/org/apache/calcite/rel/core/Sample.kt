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

import org.apache.calcite.plan.Convention

/**
 * Relational expression that returns a sample of the rows from its input.
 *
 *
 * In SQL, a sample is expressed using the `TABLESAMPLE BERNOULLI` or
 * `SYSTEM` keyword applied to a table, view or sub-query.
 */
class Sample(
    cluster: RelOptCluster, child: RelNode?,
    params: RelOptSamplingParameters
) : SingleRel(cluster, cluster.traitSetOf(Convention.NONE), child) {
    //~ Instance fields --------------------------------------------------------
    private val params: RelOptSamplingParameters

    //~ Constructors -----------------------------------------------------------
    init {
        this.params = params
    }

    /**
     * Creates a Sample by parsing serialized output.
     */
    constructor(input: RelInput) : this(input.getCluster(), input.getInput(), getSamplingParameters(input)) {}

    @Override
    fun copy(traitSet: RelTraitSet, inputs: List<RelNode?>?): RelNode {
        assert(traitSet.containsIfApplicable(Convention.NONE))
        return Sample(getCluster(), sole(inputs), params)
    }

    /**
     * Retrieve the sampling parameters for this Sample.
     */
    val samplingParameters: RelOptSamplingParameters
        get() = params

    @Override
    fun explainTerms(pw: RelWriter?): RelWriter {
        return super.explainTerms(pw)
            .item("mode", if (params.isBernoulli()) "bernoulli" else "system")
            .item("rate", params.getSamplingPercentage())
            .item(
                "repeatableSeed",
                if (params.isRepeatable()) params.getRepeatableSeed() else "-"
            )
    }

    companion object {
        //~ Methods ----------------------------------------------------------------
        private fun getSamplingParameters(
            input: RelInput
        ): RelOptSamplingParameters {
            val mode: String = input.getString("mode")
            val percentage: Float = input.getFloat("rate")
            val repeatableSeed: Object = input.get("repeatableSeed")
            val repeatable = repeatableSeed is Number
            return RelOptSamplingParameters(
                "bernoulli".equals(mode), percentage, repeatable,
                if (repeatable && repeatableSeed != null) (repeatableSeed as Number).intValue() else 0
            )
        }
    }
}
