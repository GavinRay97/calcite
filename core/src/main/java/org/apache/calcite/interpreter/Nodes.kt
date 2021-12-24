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
package org.apache.calcite.interpreter

import org.apache.calcite.plan.RelOptCluster

/**
 * Helper methods for [Node] and implementations for core relational
 * expressions.
 */
class Nodes {
    /** Extension to
     * [Interpreter.CompilerImpl]
     * that knows how to handle the core logical
     * [org.apache.calcite.rel.RelNode]s.  */
    class CoreCompiler internal constructor(@UnknownInitialization interpreter: Interpreter?, cluster: RelOptCluster?) :
        Interpreter.CompilerImpl(interpreter, cluster) {
        fun visit(agg: Aggregate?) {
            node = AggregateNode(this, agg)
        }

        fun visit(filter: Filter?) {
            node = FilterNode(this, filter)
        }

        fun visit(project: Project) {
            node = ProjectNode(this, project)
        }

        fun visit(value: Values?) {
            node = ValuesNode(this, value)
        }

        fun visit(scan: TableScan?) {
            val filters: ImmutableList<RexNode> = ImmutableList.of()
            node = TableScanNode.create(this, scan, filters, null)
        }

        fun visit(scan: Bindables.BindableTableScan) {
            node = TableScanNode.create(this, scan, scan.filters, scan.projects)
        }

        fun visit(functionScan: TableFunctionScan?) {
            node = TableFunctionScanNode.create(this, functionScan)
        }

        fun visit(sort: Sort?) {
            node = SortNode(this, sort)
        }

        fun visit(setOp: SetOp?) {
            node = SetOpNode(this, setOp)
        }

        fun visit(join: Join) {
            node = JoinNode(this, join)
        }

        fun visit(window: Window?) {
            node = WindowNode(this, window)
        }

        fun visit(match: Match?) {
            node = MatchNode(this, match)
        }

        fun visit(collect: Collect?) {
            node = CollectNode(this, collect)
        }

        fun visit(uncollect: Uncollect?) {
            node = UncollectNode(this, uncollect)
        }
    }
}
