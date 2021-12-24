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
package org.apache.calcite.rel.rules.materialize

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.hep.HepProgram
import org.apache.calcite.rel.core.Join
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.tools.RelBuilderFactory
import org.immutables.value.Value

/** Rule that matches Project on Join.  */
@Value.Enclosing
class MaterializedViewProjectJoinRule private constructor(config: Config) :
    MaterializedViewJoinRule<MaterializedViewProjectJoinRule.Config?>(config) {
    @Deprecated // to be removed before 2.0
    constructor(
        relBuilderFactory: RelBuilderFactory?,
        generateUnionRewriting: Boolean, unionRewritingPullProgram: HepProgram?,
        fastBailOut: Boolean
    ) : this(
        Config.DEFAULT
            .withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withGenerateUnionRewriting(generateUnionRewriting)
            .withUnionRewritingPullProgram(unionRewritingPullProgram)
            .withFastBailOut(fastBailOut)
            .`as`(Config::class.java)
    ) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val project: Project = call.rel(0)
        val join: Join = call.rel(1)
        perform(call, project, join)
    }

    /** Rule configuration.  */
    @Value.Immutable(singleton = false)
    interface Config : MaterializedViewRule.Config {
        @Override
        fun toRule(): MaterializedViewProjectJoinRule {
            return MaterializedViewProjectJoinRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableMaterializedViewProjectJoinRule.Config.builder()
                .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
                .withOperandSupplier { b0 ->
                    b0.operand(Project::class.java).oneInput { b1 -> b1.operand(Join::class.java).anyInputs() }
                }
                .withDescription("MaterializedViewJoinRule(Project-Join)")
                .withGenerateUnionRewriting(true)
                .withUnionRewritingPullProgram(null)
                .withFastBailOut(true)
                .build()
        }
    }
}
