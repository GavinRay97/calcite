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

import org.apache.calcite.plan.Contexts

/**
 * Planner rule that pushes a [org.apache.calcite.rel.core.Filter]
 * past a [org.apache.calcite.rel.core.SetOp].
 *
 * @see CoreRules.FILTER_SET_OP_TRANSPOSE
 */
@Value.Enclosing
class FilterSetOpTransposeRule
/** Creates a FilterSetOpTransposeRule.  */
protected constructor(config: Config?) : RelRule<FilterSetOpTransposeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    @Deprecated // to  be removed before 2.0
    constructor(filterFactory: FilterFactory?) : this(
        Config.DEFAULT
            .withRelBuilderFactory(RelBuilder.proto(Contexts.of(filterFactory)))
            .`as`(Config::class.java)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val filterRel: Filter = call.rel(0)
        val setOp: SetOp = call.rel(1)
        val condition: RexNode = filterRel.getCondition()

        // create filters on top of each setop child, modifying the filter
        // condition to reference each setop child
        val rexBuilder: RexBuilder = filterRel.getCluster().getRexBuilder()
        val relBuilder: RelBuilder = call.builder()
        val origFields: List<RelDataTypeField> = setOp.getRowType().getFieldList()
        val adjustments = IntArray(origFields.size())
        val newSetOpInputs: List<RelNode> = ArrayList()
        for (input in setOp.getInputs()) {
            val newCondition: RexNode = condition.accept(
                RexInputConverter(
                    rexBuilder,
                    origFields,
                    input.getRowType().getFieldList(),
                    adjustments
                )
            )
            newSetOpInputs.add(relBuilder.push(input).filter(newCondition).build())
        }

        // create a new setop whose children are the filters created above
        val newSetOp: SetOp = setOp.copy(setOp.getTraitSet(), newSetOpInputs)
        call.transformTo(newSetOp)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): FilterSetOpTransposeRule? {
            return FilterSetOpTransposeRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableFilterSetOpTransposeRule.Config.of()
                .withOperandSupplier { b0 ->
                    b0.operand(Filter::class.java).oneInput { b1 -> b1.operand(SetOp::class.java).anyInputs() }
                }
        }
    }
}
