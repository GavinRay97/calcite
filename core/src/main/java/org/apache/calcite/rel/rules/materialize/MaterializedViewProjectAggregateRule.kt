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

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.hep.HepProgram
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.tools.RelBuilderFactory
import org.immutables.value.Value

/** Rule that matches Project on Aggregate.
 *
 * @see MaterializedViewRules.PROJECT_AGGREGATE
 */
@Value.Enclosing
class MaterializedViewProjectAggregateRule private constructor(config: Config) :
    MaterializedViewAggregateRule<MaterializedViewProjectAggregateRule.Config?>(config) {
    @Deprecated // to be removed before 2.0
    constructor(
        relBuilderFactory: RelBuilderFactory?,
        generateUnionRewriting: Boolean, unionRewritingPullProgram: HepProgram?
    ) : this(
        Config.create(relBuilderFactory)
            .withGenerateUnionRewriting(generateUnionRewriting)
            .withUnionRewritingPullProgram(unionRewritingPullProgram)
            .`as`(Config::class.java)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        relBuilderFactory: RelBuilderFactory?,
        generateUnionRewriting: Boolean, unionRewritingPullProgram: HepProgram?,
        filterProjectTransposeRule: RelOptRule?,
        filterAggregateTransposeRule: RelOptRule?,
        aggregateProjectPullUpConstantsRule: RelOptRule?,
        projectMergeRule: RelOptRule?
    ) : this(
        Config.create(relBuilderFactory)
            .withGenerateUnionRewriting(generateUnionRewriting)
            .withUnionRewritingPullProgram(unionRewritingPullProgram)
            .`as`(Config::class.java)
            .withFilterProjectTransposeRule(filterProjectTransposeRule)
            .withFilterAggregateTransposeRule(filterAggregateTransposeRule)
            .withAggregateProjectPullUpConstantsRule(
                aggregateProjectPullUpConstantsRule
            )
            .withProjectMergeRule(projectMergeRule)
            .`as`(Config::class.java)
    ) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val project: Project = call.rel(0)
        val aggregate: Aggregate = call.rel(1)
        perform(call, project, aggregate)
    }

    /** Rule configuration.  */
    @Value.Immutable(singleton = false)
    interface Config : MaterializedViewAggregateRule.Config {
        @Override
        fun toRule(): MaterializedViewProjectAggregateRule {
            return MaterializedViewProjectAggregateRule(this)
        }

        companion object {
            fun create(relBuilderFactory: RelBuilderFactory?): Config {
                return ImmutableMaterializedViewProjectAggregateRule.Config.builder()
                    .withRelBuilderFactory(relBuilderFactory)
                    .withGenerateUnionRewriting(true)
                    .withUnionRewritingPullProgram(null)
                    .withFastBailOut(false)
                    .withOperandSupplier { b0 ->
                        b0.operand(Project::class.java).oneInput { b1 -> b1.operand(Aggregate::class.java).anyInputs() }
                    }
                    .withDescription("MaterializedViewAggregateRule(Project-Aggregate)")
                    .build()
            }

            val DEFAULT = create(RelFactories.LOGICAL_BUILDER)
        }
    }
}
