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
 * Planner rule that converts a
 * [org.apache.calcite.rel.logical.LogicalTableScan] to the result
 * of calling [RelOptTable.toRel].
 *
 */
@Deprecated // to be removed before 2.0
@Value.Enclosing
@SuppressWarnings("deprecation")
@Deprecated(
    """{@code org.apache.calcite.rel.core.RelFactories.TableScanFactoryImpl}
  has called {@link RelOptTable#toRel(RelOptTable.ToRelContext)}."""
)
class TableScanRule  //~ Constructors -----------------------------------------------------------
/** Creates a TableScanRule.  */
protected constructor(config: RelRule.Config?) : RelRule<RelRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val oldRel: LogicalTableScan = call.rel(0)
        val newRel: RelNode = oldRel.getTable().toRel(
            ViewExpanders.simpleContext(oldRel.getCluster())
        )
        call.transformTo(newRel)
    }

    /** Rule configuration.  */
    @Value.Immutable
    @SuppressWarnings("deprecation")
    interface Config : RelRule.Config {
        @Override
        fun toRule(): TableScanRule {
            return TableScanRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableTableScanRule.Config.of()
                .withOperandSupplier { b -> b.operand(LogicalTableScan::class.java).noInputs() }
        }
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        val INSTANCE = Config.DEFAULT.toRule()
    }
}
