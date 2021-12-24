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
 * `UnionEliminatorRule` checks to see if its possible to optimize a
 * Union call by eliminating the Union operator altogether in the case the call
 * consists of only one input.
 *
 * @see CoreRules.UNION_REMOVE
 */
@Value.Enclosing
class UnionEliminatorRule : RelRule<UnionEliminatorRule.Config?>, SubstitutionRule {
    /** Creates a UnionEliminatorRule.  */
    protected constructor(config: Config?) : super(config) {}

    @Deprecated // to be removed before 2.0
    constructor(
        unionClass: Class<out Union?>?,
        relBuilderFactory: RelBuilderFactory?
    ) : super(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(unionClass)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun matches(call: RelOptRuleCall): Boolean {
        val union: Union = call.rel(0)
        return union.all && union.getInputs().size() === 1
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val union: Union = call.rel(0)
        call.transformTo(union.getInputs().get(0))
    }

    @Override
    override fun autoPruneOld(): Boolean {
        return true
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): UnionEliminatorRule? {
            return UnionEliminatorRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(unionClass: Class<out Union?>?): Config? {
            return withOperandSupplier { b -> b.operand(unionClass).anyInputs() }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableUnionEliminatorRule.Config.of()
                .withOperandFor(LogicalUnion::class.java)
        }
    }
}
