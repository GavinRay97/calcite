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

/**
 * Planner rule that translates a distinct
 * [org.apache.calcite.rel.core.Union]
 * (`all` = `false`)
 * into an [org.apache.calcite.rel.core.Aggregate]
 * on top of a non-distinct [org.apache.calcite.rel.core.Union]
 * (`all` = `true`).
 *
 * @see CoreRules.UNION_TO_DISTINCT
 */
@Value.Enclosing
class UnionToDistinctRule
/** Creates a UnionToDistinctRule.  */
protected constructor(config: Config?) : RelRule<UnionToDistinctRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        unionClass: Class<out Union?>?,
        relBuilderFactory: RelBuilderFactory?
    ) : this(
        Config.DEFAULT.withOperandFor(unionClass)
            .withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        unionClazz: Class<out Union?>?,
        setOpFactory: RelFactories.SetOpFactory?
    ) : this(
        Config.DEFAULT.withOperandFor(unionClazz)
            .withRelBuilderFactory(RelBuilder.proto(setOpFactory))
            .`as`(Config::class.java)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val union: Union = call.rel(0)
        val relBuilder: RelBuilder = call.builder()
        relBuilder.pushAll(union.getInputs())
        relBuilder.union(true, union.getInputs().size())
        relBuilder.distinct()
        call.transformTo(relBuilder.build())
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): UnionToDistinctRule? {
            return UnionToDistinctRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(unionClass: Class<out Union?>?): Config {
            return withOperandSupplier { b ->
                b.operand(unionClass)
                    .predicate { union -> !union.all }.anyInputs()
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableUnionToDistinctRule.Config.of()
                .withOperandFor(LogicalUnion::class.java)
        }
    }
}
