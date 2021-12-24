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

import org.apache.calcite.adapter.enumerable.EnumerableInterpreter

/**
 * Planner rule that converts
 * a [org.apache.calcite.rel.core.Filter]
 * on a [org.apache.calcite.rel.core.TableScan]
 * of a [org.apache.calcite.schema.FilterableTable]
 * or a [org.apache.calcite.schema.ProjectableFilterableTable]
 * to a [org.apache.calcite.interpreter.Bindables.BindableTableScan].
 *
 *
 * The [CoreRules.FILTER_INTERPRETER_SCAN] variant allows an
 * intervening
 * [org.apache.calcite.adapter.enumerable.EnumerableInterpreter].
 *
 * @see org.apache.calcite.rel.rules.ProjectTableScanRule
 *
 * @see CoreRules.FILTER_SCAN
 *
 * @see CoreRules.FILTER_INTERPRETER_SCAN
 */
@Value.Enclosing
class FilterTableScanRule
/** Creates a FilterTableScanRule.  */
protected constructor(config: Config?) : RelRule<FilterTableScanRule.Config?>(config) {
    @Deprecated // to be removed before 2.0
    protected constructor(operand: RelOptRuleOperand?, description: String?) : this(
        ImmutableFilterTableScanRule.Config.of().`as`(
            Config::class.java
        )
    ) {
        throw UnsupportedOperationException()
    }

    @Deprecated // to be removed before 2.0
    protected constructor(
        operand: RelOptRuleOperand?,
        relBuilderFactory: RelBuilderFactory?, description: String?
    ) : this(
        ImmutableFilterTableScanRule.Config.of().`as`(
            Config::class.java
        )
    ) {
        throw UnsupportedOperationException()
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        if (call.rels.length === 2) {
            // the ordinary variant
            val filter: Filter = call.rel(0)
            val scan: TableScan = call.rel(1)
            apply(call, filter, scan)
        } else if (call.rels.length === 3) {
            // the variant with intervening EnumerableInterpreter
            val filter: Filter = call.rel(0)
            val scan: TableScan = call.rel(2)
            apply(call, filter, scan)
        } else {
            throw AssertionError()
        }
    }

    protected fun apply(call: RelOptRuleCall, filter: Filter, scan: TableScan) {
        val projects: ImmutableIntList
        val filters: ImmutableList.Builder<RexNode> = ImmutableList.builder()
        projects = if (scan is Bindables.BindableTableScan) {
            val bindableScan: Bindables.BindableTableScan = scan as Bindables.BindableTableScan
            filters.addAll(bindableScan.filters)
            bindableScan.projects
        } else {
            scan.identity()
        }
        val mapping: Mapping = Mappings.target(
            projects,
            scan.getTable().getRowType().getFieldCount()
        )
        filters.add(
            RexUtil.apply(mapping.inverse(), filter.getCondition())
        )
        call.transformTo(
            Bindables.BindableTableScan.create(
                scan.getCluster(), scan.getTable(),
                filters.build(), projects
            )
        )
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): FilterTableScanRule? {
            return FilterTableScanRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableFilterTableScanRule.Config.of()
                .withOperandSupplier { b0 ->
                    b0.operand(Filter::class.java).oneInput { b1 ->
                        b1.operand(TableScan::class.java)
                            .predicate { scan: TableScan -> test(scan) }.noInputs()
                    }
                }
            val INTERPRETER: Config = ImmutableFilterTableScanRule.Config.of()
                .withOperandSupplier { b0 ->
                    b0.operand(Filter::class.java).oneInput { b1 ->
                        b1.operand(
                            EnumerableInterpreter::class.java
                        ).oneInput { b2 ->
                            b2.operand(TableScan::class.java)
                                .predicate { scan: TableScan -> test(scan) }.noInputs()
                        }
                    }
                }
                .withDescription("FilterTableScanRule:interpreter")
        }
    }

    companion object {
        @SuppressWarnings("Guava")
        @Deprecated // to be removed before 2.0
        val PREDICATE: com.google.common.base.Predicate<TableScan> =
            com.google.common.base.Predicate<TableScan> { scan: TableScan -> test(scan) }

        //~ Methods ----------------------------------------------------------------
        fun test(scan: TableScan): Boolean {
            // We can only push filters into a FilterableTable or
            // ProjectableFilterableTable.
            val table: RelOptTable = scan.getTable()
            return (table.unwrap(FilterableTable::class.java) != null
                    || table.unwrap(ProjectableFilterableTable::class.java) != null)
        }
    }
}
