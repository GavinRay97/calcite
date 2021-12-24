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
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.tools.RelBuilderFactory
import org.immutables.value.Value

/** Rule that matches Join.  */
@Value.Enclosing
class MaterializedViewOnlyJoinRule internal constructor(config: Config) :
    MaterializedViewJoinRule<MaterializedViewRule.Config?>(config) {
    @Deprecated // to be removed before 2.0
    constructor(
        relBuilderFactory: RelBuilderFactory?,
        generateUnionRewriting: Boolean, unionRewritingPullProgram: HepProgram?,
        fastBailOut: Boolean
    ) : this(
        Config.DEFAULT
            .withGenerateUnionRewriting(generateUnionRewriting)
            .withUnionRewritingPullProgram(unionRewritingPullProgram)
            .withFastBailOut(fastBailOut)
            .withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val join: Join = call.rel(0)
        perform(call, null, join)
    }

    /** Rule configuration.  */
    @Value.Immutable(singleton = false)
    interface Config : MaterializedViewRule.Config {
        @Override
        fun toRule(): MaterializedViewOnlyJoinRule {
            return MaterializedViewOnlyJoinRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableMaterializedViewOnlyJoinRule.Config.builder()
                .withOperandSupplier { b -> b.operand(Join::class.java).anyInputs() }
                .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
                .withDescription("MaterializedViewJoinRule(Join)")
                .withGenerateUnionRewriting(true)
                .withUnionRewritingPullProgram(null)
                .withFastBailOut(true)
                .build()
        }
    }
}
