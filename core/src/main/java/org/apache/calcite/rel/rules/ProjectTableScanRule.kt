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
import org.apache.calcite.interpreter.Bindables
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelOptRuleOperand
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.RelRule
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexUtil
import org.apache.calcite.rex.RexVisitorImpl
import org.apache.calcite.schema.ProjectableFilterableTable
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.util.mapping.Mapping
import org.apache.calcite.util.mapping.Mappings
import com.google.common.collect.ImmutableList
import org.immutables.value.Value
import java.util.ArrayList
import java.util.List
import java.util.stream.Collectors

/**
 * Planner rule that converts a [Project]
 * on a [org.apache.calcite.rel.core.TableScan]
 * of a [org.apache.calcite.schema.ProjectableFilterableTable]
 * to a [org.apache.calcite.interpreter.Bindables.BindableTableScan].
 *
 *
 * The [CoreRules.PROJECT_INTERPRETER_TABLE_SCAN] variant allows an
 * intervening
 * [org.apache.calcite.adapter.enumerable.EnumerableInterpreter].
 *
 * @see FilterTableScanRule
 */
@Value.Enclosing
class ProjectTableScanRule
/** Creates a ProjectTableScanRule.  */
protected constructor(config: Config?) : RelRule<ProjectTableScanRule.Config?>(config) {
    @Deprecated // to be removed before 2.0
    constructor(
        operand: RelOptRuleOperand?,
        relBuilderFactory: RelBuilderFactory?, description: String?
    ) : this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .withDescription(description)
        .withOperandSupplier { b -> b.exactly(operand) }
        .`as`(Config::class.java)) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        if (call.rels.length === 2) {
            // the ordinary variant
            val project: Project = call.rel(0)
            val scan: TableScan = call.rel(1)
            apply(call, project, scan)
        } else if (call.rels.length === 3) {
            // the variant with intervening EnumerableInterpreter
            val project: Project = call.rel(0)
            val scan: TableScan = call.rel(2)
            apply(call, project, scan)
        } else {
            throw AssertionError()
        }
    }

    protected fun apply(call: RelOptRuleCall, project: Project, scan: TableScan) {
        val table: RelOptTable = scan.getTable()
        assert(table.unwrap(ProjectableFilterableTable::class.java) != null)
        val selectedColumns: List<Integer> = ArrayList()
        val visitor: RexVisitorImpl<Void> = object : RexVisitorImpl<Void?>(true) {
            @Override
            fun visitInputRef(inputRef: RexInputRef): Void? {
                if (!selectedColumns.contains(inputRef.getIndex())) {
                    selectedColumns.add(inputRef.getIndex())
                }
                return null
            }
        }
        visitor.visitEach(project.getProjects())
        val filtersPushDown: List<RexNode>
        val projectsPushDown: List<Integer>
        if (scan is Bindables.BindableTableScan) {
            val bindableScan: Bindables.BindableTableScan = scan as Bindables.BindableTableScan
            filtersPushDown = bindableScan.filters
            projectsPushDown = selectedColumns.stream()
                .map(bindableScan.projects::get)
                .collect(Collectors.toList())
        } else {
            filtersPushDown = ImmutableList.of()
            projectsPushDown = selectedColumns
        }
        val newScan: Bindables.BindableTableScan = Bindables.BindableTableScan.create(
            scan.getCluster(), scan.getTable(), filtersPushDown, projectsPushDown
        )
        val mapping: Mapping = Mappings.target(selectedColumns, scan.getRowType().getFieldCount())
        val newProjectRexNodes: List<RexNode> = RexUtil.apply(mapping, project.getProjects())
        if (RexUtil.isIdentity(newProjectRexNodes, newScan.getRowType())) {
            call.transformTo(newScan)
        } else {
            call.transformTo(
                call.builder()
                    .push(newScan)
                    .project(newProjectRexNodes)
                    .build()
            )
        }
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): ProjectTableScanRule? {
            return ProjectTableScanRule(this)
        }

        companion object {
            /** Config that matches Project on TableScan.  */
            val DEFAULT: Config = ImmutableProjectTableScanRule.Config.of()
                .withOperandSupplier { b0 ->
                    b0.operand(Project::class.java).oneInput { b1 ->
                        b1.operand(TableScan::class.java)
                            .predicate { scan: TableScan -> test(scan) }
                            .noInputs()
                    }
                }

            /** Config that matches Project on EnumerableInterpreter on TableScan.  */
            val INTERPRETER: Config = DEFAULT
                .withOperandSupplier { b0 ->
                    b0.operand(Project::class.java).oneInput { b1 ->
                        b1.operand(
                            EnumerableInterpreter::class.java
                        ).oneInput { b2 ->
                            b2.operand(TableScan::class.java)
                                .predicate { scan: TableScan -> test(scan) }
                                .noInputs()
                        }
                    }
                }
                .withDescription("ProjectTableScanRule:interpreter")
                .`as`(Config::class.java)
        }
    }

    companion object {
        @SuppressWarnings("Guava")
        @Deprecated // to be removed before 2.0
        val PREDICATE: com.google.common.base.Predicate<TableScan> =
            com.google.common.base.Predicate<TableScan> { scan: TableScan -> test(scan) }

        //~ Methods ----------------------------------------------------------------
        protected fun test(scan: TableScan): Boolean {
            // We can only push projects into a ProjectableFilterableTable.
            val table: RelOptTable = scan.getTable()
            return table.unwrap(ProjectableFilterableTable::class.java) != null
        }
    }
}
