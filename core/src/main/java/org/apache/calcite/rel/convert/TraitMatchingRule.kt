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
package org.apache.calcite.rel.convert

import org.apache.calcite.plan.Convention
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelOptRuleOperand
import org.apache.calcite.plan.RelOptRuleOperandChildPolicy
import org.apache.calcite.plan.RelRule
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.tools.RelBuilderFactory
import org.immutables.value.Value

/**
 * TraitMatchingRule adapts a converter rule, restricting it to fire only when
 * its input already matches the expected output trait. This can be used with
 * [org.apache.calcite.plan.hep.HepPlanner] in cases where alternate
 * implementations are available and it is desirable to minimize converters.
 */
@Value.Enclosing
class TraitMatchingRule  //~ Constructors -----------------------------------------------------------
/** Creates a TraitMatchingRule.  */
protected constructor(config: Config?) : RelRule<TraitMatchingRule.Config?>(config) {
    @Deprecated // to be removed before 2.0
    constructor(converterRule: ConverterRule) : this(config(converterRule, RelFactories.LOGICAL_BUILDER)) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        converterRule: ConverterRule,
        relBuilderFactory: RelBuilderFactory?
    ) : this(config(converterRule, relBuilderFactory)) {
    }

    //~ Methods ----------------------------------------------------------------
    @get:Nullable
    @get:Override
    val outConvention: Convention
        get() = config.converterRule().getOutConvention()

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val input: RelNode = call.rel(1)
        val converterRule: ConverterRule = config.converterRule()
        if (input.getTraitSet().contains(converterRule.getOutTrait())) {
            converterRule.onMatch(call)
        }
    }

    /** Rule configuration.  */
    @Value.Immutable(singleton = false)
    interface Config : RelRule.Config {
        @Override
        fun toRule(): TraitMatchingRule? {
            return TraitMatchingRule(this)
        }

        /** Returns the rule to be restricted; rule must take a single
         * operand expecting a single input.  */
        fun converterRule(): ConverterRule

        /** Sets [.converterRule].  */
        fun withConverterRule(converterRule: ConverterRule?): Config?
    }

    companion object {
        /**
         * Creates a configuration for a TraitMatchingRule.
         *
         * @param converterRule     Rule to be restricted; rule must take a single
         * operand expecting a single input
         * @param relBuilderFactory Builder for relational expressions
         */
        fun config(
            converterRule: ConverterRule,
            relBuilderFactory: RelBuilderFactory?
        ): Config {
            val operand: RelOptRuleOperand = converterRule.getOperand()
            assert(operand.childPolicy === RelOptRuleOperandChildPolicy.ANY)
            return ImmutableTraitMatchingRule.Config.builder().withRelBuilderFactory(relBuilderFactory)
                .withDescription("TraitMatchingRule: $converterRule")
                .withOperandSupplier { b0 ->
                    b0.operand(operand.getMatchedClass()).oneInput { b1 -> b1.operand(RelNode::class.java).anyInputs() }
                }
                .withConverterRule(converterRule)
                .build()
        }
    }
}
