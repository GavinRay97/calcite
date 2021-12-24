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
package org.apache.calcite.rel.rules

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.core.Project
import org.apache.calcite.schema.impl.StarTable
import org.immutables.value.Value

/** Variant of [AggregateStarTableRule] that accepts a [Project]
 * between the [Aggregate] and its [StarTable.StarTableScan]
 * input.  */
@Value.Enclosing
class AggregateProjectStarTableRule
/** Creates an AggregateProjectStarTableRule.  */
protected constructor(config: Config?) : AggregateStarTableRule(config) {
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val aggregate: Aggregate = call.rel(0)
        val project: Project = call.rel(1)
        val scan: StarTable.StarTableScan = call.rel(2)
        val rel: RelNode = AggregateProjectMergeRule.apply(call, aggregate, project)
        val aggregate2: Aggregate
        val project2: Project?
        if (rel is Aggregate) {
            project2 = null
            aggregate2 = rel as Aggregate
        } else if (rel is Project) {
            project2 = rel as Project
            aggregate2 = project2.getInput() as Aggregate
        } else {
            return
        }
        apply(call, project2, aggregate2, scan)
    }

    /** Rule configuration.  */
    @Value.Immutable
    @SuppressWarnings("immutables:subtype")
    interface Config : AggregateStarTableRule.Config {
        @Override
        fun toRule(): AggregateProjectStarTableRule? {
            return AggregateProjectStarTableRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            aggregateClass: Class<out Aggregate?>?,
            projectClass: Class<out Project?>?,
            scanClass: Class<StarTable.StarTableScan?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(aggregateClass)
                    .predicate(Aggregate::isSimple)
                    .oneInput { b1 ->
                        b1.operand(projectClass)
                            .oneInput { b2 -> b2.operand(scanClass).noInputs() }
                    }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableAggregateProjectStarTableRule.Config.of()
                .withOperandFor(
                    Aggregate::class.java, Project::class.java,
                    StarTable.StarTableScan::class.java
                )
        }
    }
}
