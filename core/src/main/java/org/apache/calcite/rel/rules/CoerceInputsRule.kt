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

import org.apache.calcite.plan.Convention

/**
 * CoerceInputsRule pre-casts inputs to a particular type. This can be used to
 * assist operator implementations which impose requirements on their input
 * types.
 *
 * @see CoreRules.COERCE_INPUTS
 */
@Value.Enclosing
class CoerceInputsRule  //~ Constructors -----------------------------------------------------------
/** Creates a CoerceInputsRule.  */
protected constructor(config: Config?) : RelRule<CoerceInputsRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        consumerRelClass: Class<out RelNode?>,
        coerceNames: Boolean
    ) : this(
        Config.DEFAULT
            .withCoerceNames(coerceNames)
            .withOperandFor(consumerRelClass)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        consumerRelClass: Class<out RelNode?>?,
        coerceNames: Boolean, relBuilderFactory: RelBuilderFactory?
    ) : this(
        Config.DEFAULT
            .withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withCoerceNames(coerceNames)
            .withConsumerRelClass(consumerRelClass)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @get:Nullable
    @get:Override
    val outConvention: Convention
        get() = Convention.NONE

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val consumerRel: RelNode = call.rel(0)
        if (consumerRel.getClass() !== config.consumerRelClass()) {
            // require exact match on type
            return
        }
        val inputs: List<RelNode> = consumerRel.getInputs()
        val newInputs: List<RelNode> = ArrayList(inputs)
        var coerce = false
        for (i in 0 until inputs.size()) {
            val expectedType: RelDataType = consumerRel.getExpectedInputRowType(i)
            val input: RelNode = inputs[i]
            val newInput: RelNode = RelOptUtil.createCastRel(
                input,
                expectedType,
                config.isCoerceNames()
            )
            if (newInput !== input) {
                newInputs.set(i, newInput)
                coerce = true
            }
            assert(
                RelOptUtil.areRowTypesEqual(
                    newInputs[i].getRowType(),
                    expectedType,
                    config.isCoerceNames()
                )
            )
        }
        if (!coerce) {
            return
        }
        val newConsumerRel: RelNode = consumerRel.copy(
            consumerRel.getTraitSet(),
            newInputs
        )
        call.transformTo(newConsumerRel)
    }

    /** Rule configuration.  */
    @Value.Immutable(singleton = false)
    interface Config : RelRule.Config {
        @Override
        fun toRule(): CoerceInputsRule {
            return CoerceInputsRule(this)
        }

        /** Whether to coerce names.  */
        @get:Value.Default
        val isCoerceNames: Boolean
            get() = false

        /** Sets [.isCoerceNames].  */
        fun withCoerceNames(coerceNames: Boolean): Config

        /** Class of [RelNode] to coerce to.  */
        fun consumerRelClass(): Class<out RelNode?>?

        /** Sets [.consumerRelClass].  */
        fun withConsumerRelClass(relClass: Class<out RelNode?>?): Config

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(consumerRelClass: Class<out RelNode?>): Config? {
            return withConsumerRelClass(consumerRelClass)
                .withOperandSupplier { b -> b.operand(consumerRelClass).anyInputs() }
                .withDescription("CoerceInputsRule:" + consumerRelClass.getName())
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableCoerceInputsRule.Config.builder()
                .withConsumerRelClass(RelNode::class.java)
                .build()
                .withCoerceNames(false)
                .withOperandFor(RelNode::class.java)
        }
    }
}
