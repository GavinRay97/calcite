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
package org.apache.calcite.rel

import org.apache.calcite.linq4j.Ord

/**
 * Basic implementation of [RelShuttle] that calls
 * [RelNode.accept] on each child, and
 * [RelNode.copy] if
 * any children change.
 */
class RelShuttleImpl : RelShuttle {
    protected val stack: Deque<RelNode> = ArrayDeque()

    /**
     * Visits a particular child of a parent.
     */
    protected fun visitChild(parent: RelNode, i: Int, child: RelNode): RelNode {
        stack.push(parent)
        return try {
            val child2: RelNode = child.accept(this)
            if (child2 !== child) {
                val newInputs: List<RelNode> = ArrayList(parent.getInputs())
                newInputs.set(i, child2)
                return parent.copy(parent.getTraitSet(), newInputs)
            }
            parent
        } finally {
            stack.pop()
        }
    }

    protected fun visitChildren(rel: RelNode): RelNode {
        var rel: RelNode = rel
        for (input in Ord.zip(rel.getInputs())) {
            rel = visitChild(rel, input.i, input.e)
        }
        return rel
    }

    @Override
    override fun visit(aggregate: LogicalAggregate): RelNode {
        return visitChild(aggregate, 0, aggregate.getInput())
    }

    @Override
    override fun visit(match: LogicalMatch): RelNode {
        return visitChild(match, 0, match.getInput())
    }

    @Override
    override fun visit(scan: TableScan): RelNode {
        return scan
    }

    @Override
    override fun visit(scan: TableFunctionScan): RelNode {
        return visitChildren(scan)
    }

    @Override
    override fun visit(values: LogicalValues): RelNode {
        return values
    }

    @Override
    override fun visit(filter: LogicalFilter): RelNode {
        return visitChild(filter, 0, filter.getInput())
    }

    @Override
    override fun visit(calc: LogicalCalc): RelNode {
        return visitChildren(calc)
    }

    @Override
    override fun visit(project: LogicalProject): RelNode {
        return visitChild(project, 0, project.getInput())
    }

    @Override
    override fun visit(join: LogicalJoin): RelNode {
        return visitChildren(join)
    }

    @Override
    override fun visit(correlate: LogicalCorrelate): RelNode {
        return visitChildren(correlate)
    }

    @Override
    override fun visit(union: LogicalUnion): RelNode {
        return visitChildren(union)
    }

    @Override
    override fun visit(intersect: LogicalIntersect): RelNode {
        return visitChildren(intersect)
    }

    @Override
    override fun visit(minus: LogicalMinus): RelNode {
        return visitChildren(minus)
    }

    @Override
    override fun visit(sort: LogicalSort): RelNode {
        return visitChildren(sort)
    }

    @Override
    override fun visit(exchange: LogicalExchange): RelNode {
        return visitChildren(exchange)
    }

    @Override
    override fun visit(modify: LogicalTableModify): RelNode {
        return visitChildren(modify)
    }

    @Override
    override fun visit(other: RelNode): RelNode {
        return visitChildren(other)
    }
}
