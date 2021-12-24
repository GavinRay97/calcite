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

import org.apache.calcite.plan.RelOptSamplingParameters

/** Mutable equivalent of [org.apache.calcite.rel.core.Sample].  */
class MutableSample private constructor(input: MutableRel, params: RelOptSamplingParameters) :
    MutableSingleRel(MutableRelType.SAMPLE, input.rowType, input) {
    val params: RelOptSamplingParameters

    init {
        this.params = params
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (obj === this
                || (obj is MutableSample
                && params.equals((obj as MutableSample).params)
                && input!!.equals((obj as MutableSample).input)))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(input, params)
    }

    @Override
    override fun digest(buf: StringBuilder): StringBuilder {
        return buf.append("Sample(mode: ")
            .append(if (params.isBernoulli()) "bernoulli" else "system")
            .append("rate")
            .append(params.getSamplingPercentage())
            .append("repeatableSeed")
            .append(if (params.isRepeatable()) params.getRepeatableSeed() else "-")
            .append(")")
    }

    @Override
    override fun clone(): MutableRel {
        return of(input!!.clone(), params)
    }

    companion object {
        /**
         * Creates a MutableSample.
         *
         * @param input   Input relational expression
         * @param params  parameters necessary to produce a sample of a relation
         */
        fun of(
            input: MutableRel, params: RelOptSamplingParameters
        ): MutableSample {
            return MutableSample(input, params)
        }
    }
}
