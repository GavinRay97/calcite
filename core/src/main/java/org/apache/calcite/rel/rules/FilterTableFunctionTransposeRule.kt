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

import org.apache.calcite.plan.RelOptCluster

/**
 * Planner rule that pushes
 * a [org.apache.calcite.rel.logical.LogicalFilter]
 * past a [org.apache.calcite.rel.logical.LogicalTableFunctionScan].
 *
 * @see CoreRules.FILTER_TABLE_FUNCTION_TRANSPOSE
 */
@Value.Enclosing
class FilterTableFunctionTransposeRule
/** Creates a FilterTableFunctionTransposeRule.  */
protected constructor(config: Config?) : RelRule<FilterTableFunctionTransposeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val filter: LogicalFilter = call.rel(0)
        val funcRel: LogicalTableFunctionScan = call.rel(1)
        val columnMappings: Set<RelColumnMapping> = funcRel.getColumnMappings()
        if (columnMappings == null || columnMappings.isEmpty()) {
            // No column mapping information, so no push-down
            // possible.
            return
        }
        val funcInputs: List<RelNode> = funcRel.getInputs()
        if (funcInputs.size() !== 1) {
            // TODO:  support more than one relational input; requires
            // offsetting field indices, similar to join
            return
        }
        // TODO:  support mappings other than 1-to-1
        if (funcRel.getRowType().getFieldCount()
            !== funcInputs[0].getRowType().getFieldCount()
        ) {
            return
        }
        for (mapping in columnMappings) {
            if (mapping.iInputColumn !== mapping.iOutputColumn) {
                return
            }
            if (mapping.derived) {
                return
            }
        }
        val newFuncInputs: List<RelNode> = ArrayList()
        val cluster: RelOptCluster = funcRel.getCluster()
        val condition: RexNode = filter.getCondition()

        // create filters on top of each func input, modifying the filter
        // condition to reference the child instead
        val rexBuilder: RexBuilder = filter.getCluster().getRexBuilder()
        val origFields: List<RelDataTypeField> = funcRel.getRowType().getFieldList()
        // TODO:  these need to be non-zero once we
        // support arbitrary mappings
        val adjustments = IntArray(origFields.size())
        for (funcInput in funcInputs) {
            val newCondition: RexNode = condition.accept(
                RexInputConverter(
                    rexBuilder,
                    origFields,
                    funcInput.getRowType().getFieldList(),
                    adjustments
                )
            )
            newFuncInputs.add(
                LogicalFilter.create(funcInput, newCondition)
            )
        }

        // create a new UDX whose children are the filters created above
        val newFuncRel: LogicalTableFunctionScan = LogicalTableFunctionScan.create(
            cluster, newFuncInputs,
            funcRel.getCall(), funcRel.getElementType(), funcRel.getRowType(),
            columnMappings
        )
        call.transformTo(newFuncRel)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): FilterTableFunctionTransposeRule? {
            return FilterTableFunctionTransposeRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableFilterTableFunctionTransposeRule.Config.of()
                .withOperandSupplier { b0 ->
                    b0.operand(LogicalFilter::class.java).oneInput { b1 ->
                        b1.operand(
                            LogicalTableFunctionScan::class.java
                        ).anyInputs()
                    }
                }
        }
    }
}
