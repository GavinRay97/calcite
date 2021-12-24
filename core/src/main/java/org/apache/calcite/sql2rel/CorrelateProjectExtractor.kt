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
package org.apache.calcite.sql2rel

import org.apache.calcite.rel.RelHomogeneousShuttle
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.CorrelationId
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.logical.LogicalCorrelate
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexCorrelVariable
import org.apache.calcite.rex.RexFieldAccess
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexLocalRef
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexOver
import org.apache.calcite.rex.RexPatternFieldRef
import org.apache.calcite.rex.RexShuttle
import org.apache.calcite.rex.RexSubQuery
import org.apache.calcite.rex.RexTableInputRef
import org.apache.calcite.rex.RexVisitorImpl
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.util.ImmutableBitSet
import org.apiguardian.api.API
import java.util.ArrayList
import java.util.HashMap
import java.util.LinkedHashSet
import java.util.List
import java.util.Map
import java.util.Set

/**
 * A visitor for relational expressions that extracts a [org.apache.calcite.rel.core.Project], with a "simple"
 * computation over the correlated variables, from the right side of a correlation
 * ([org.apache.calcite.rel.core.Correlate]) and places it on the left side.
 *
 *
 * Plan before
 * <pre>
 * LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{7}])
 * LogicalTableScan(table=[[scott, EMP]])
 * LogicalFilter(condition=[=($0, +(10, $cor0.DEPTNO))])
 * LogicalTableScan(table=[[scott, DEPT]])
</pre> *
 *
 *
 * Plan after
 * <pre>
 * LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3],... DNAME=[$10], LOC=[$11])
 * LogicalCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{8}])
 * LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], ... COMM=[$6], DEPTNO=[$7], $f8=[+(10, $7)])
 * LogicalTableScan(table=[[scott, EMP]])
 * LogicalFilter(condition=[=($0, $cor0.$f8)])
 * LogicalTableScan(table=[[scott, DEPT]])
</pre> *
 *
 *
 * Essentially this transformation moves the computation over a correlated expression from the
 * inner loop to the outer loop. It materializes the computation on the left side and flattens
 * expressions on correlated variables on the right side.
 */
@API(since = "1.27", status = API.Status.EXPERIMENTAL)
class CorrelateProjectExtractor(factory: RelBuilderFactory) : RelHomogeneousShuttle() {
    private val builderFactory: RelBuilderFactory

    init {
        builderFactory = factory
    }

    @Override
    fun visit(correlate: LogicalCorrelate): RelNode {
        val left: RelNode = correlate.getLeft().accept(this)
        var right: RelNode = correlate.getRight().accept(this)
        val oldLeft: Int = left.getRowType().getFieldCount()
        // Find the correlated expressions from the right side that can be moved to the left
        val callsWithCorrelationInRight: Set<RexNode> =
            findCorrelationDependentCalls(correlate.getCorrelationId(), right)
        val isTrivialCorrelation: Boolean =
            callsWithCorrelationInRight.stream().allMatch { exp -> exp is RexFieldAccess }
        // Early exit condition
        if (isTrivialCorrelation) {
            return if (correlate.getLeft().equals(left) && correlate.getRight().equals(right)) {
                correlate
            } else {
                correlate.copy(
                    correlate.getTraitSet(),
                    left,
                    right,
                    correlate.getCorrelationId(),
                    correlate.getRequiredColumns(),
                    correlate.getJoinType()
                )
            }
        }
        val builder: RelBuilder = builderFactory.create(correlate.getCluster(), null)
        // Transform the correlated expression from the right side to an expression over the left side
        builder.push(left)
        val callsWithCorrelationOverLeft: List<RexNode> = ArrayList()
        for (callInRight in callsWithCorrelationInRight) {
            callsWithCorrelationOverLeft.add(replaceCorrelationsWithInputRef(callInRight, builder))
        }
        builder.projectPlus(callsWithCorrelationOverLeft)

        // Construct the mapping to transform the expressions in the right side based on the new
        // projection in the left side.
        val transformMapping: Map<RexNode, RexNode> = HashMap()
        for (callInRight in callsWithCorrelationInRight) {
            val xb: RexBuilder = builder.getRexBuilder()
            val v: RexNode = xb.makeCorrel(builder.peek().getRowType(), correlate.getCorrelationId())
            val flatCorrelationInRight: RexNode = xb.makeFieldAccess(v, oldLeft + transformMapping.size())
            transformMapping.put(callInRight, flatCorrelationInRight)
        }

        // Select the required fields/columns from the left side of the correlation. Based on the code
        // above all these fields should be at the end of the left relational expression.
        val requiredFields: List<RexNode> = builder.fields(
            ImmutableBitSet.range(oldLeft, oldLeft + callsWithCorrelationOverLeft.size()).asList()
        )
        val newLeft: Int = builder.fields().size()
        // Transform the expressions in the right side using the mapping constructed earlier.
        right = replaceExpressionsUsingMap(right, transformMapping)
        builder.push(right)
        builder.correlate(correlate.getJoinType(), correlate.getCorrelationId(), requiredFields)
        // Remove the additional fields that were added for the needs of the correlation to keep the old
        // and new plan equivalent.
        val retainFields: List<Integer>
        retainFields = when (correlate.getJoinType()) {
            SEMI, ANTI -> ImmutableBitSet.range(0, oldLeft).asList()
            LEFT, INNER -> ImmutableBitSet.builder()
                .set(0, oldLeft)
                .set(newLeft, newLeft + right.getRowType().getFieldCount())
                .build()
                .asList()
            else -> throw AssertionError(correlate.getJoinType())
        }
        builder.project(builder.fields(retainFields))
        return builder.build()
    }

    /**
     * A collector of simply correlated row expressions.
     *
     * The shuttle traverses the tree and collects all calls and field accesses that are classified
     * as simply correlated expressions. Multiple nodes in a call hierarchy may satisfy the criteria
     * of a simple correlation so we peek the expressions closest to the root.
     *
     * @see SimpleCorrelationDetector
     */
    private class SimpleCorrelationCollector internal constructor(corrId: CorrelationId) : RexShuttle() {
        private val correlationId: CorrelationId

        // Clients are iterating over the collection thus it is better to use LinkedHashSet to keep
        // plans stable among executions.
        val correlations: Set<RexNode> = LinkedHashSet()

        init {
            correlationId = corrId
        }

        @Override
        fun visitCall(call: RexCall): RexNode {
            return if (isSimpleCorrelatedExpression(
                    call,
                    correlationId
                )
            ) {
                correlations.add(call)
                call
            } else {
                super.visitCall(call)
            }
        }

        @Override
        fun visitFieldAccess(fieldAccess: RexFieldAccess): RexNode {
            return if (isSimpleCorrelatedExpression(
                    fieldAccess,
                    correlationId
                )
            ) {
                correlations.add(fieldAccess)
                fieldAccess
            } else {
                super.visitFieldAccess(fieldAccess)
            }
        }
    }

    /**
     * A visitor classifying row expressions as simply correlated if they satisfy the conditions
     * below.
     *
     *  * all correlated variables have the specified correlation id
     *  * all leafs are either correlated variables, dynamic parameters, or literals
     *  * intermediate nodes are either calls or field access expressions
     *
     *
     * Examples:
     * <pre>`+(10, $cor0.DEPTNO) -> TRUE
     * /(100,+(10, $cor0.DEPTNO)) -> TRUE
     * CAST(+(10, $cor0.DEPTNO)):INTEGER NOT NULL -> TRUE
     * +($0, $cor0.DEPTNO) -> FALSE
    `</pre> *
     *
     */
    private class SimpleCorrelationDetector(corrId: CorrelationId) : RexVisitorImpl<Boolean?>(true) {
        private val corrId: CorrelationId

        init {
            this.corrId = corrId
        }

        @Override
        fun visitOver(over: RexOver?): Boolean {
            return Boolean.FALSE
        }

        @Override
        fun visitSubQuery(subQuery: RexSubQuery?): Boolean {
            return Boolean.FALSE
        }

        @Override
        fun visitCall(call: RexCall): Boolean {
            var hasSimpleCorrelation: Boolean? = null
            for (op in call.operands) {
                val b: Boolean = op.accept(this)
                if (b != null) {
                    hasSimpleCorrelation = if (hasSimpleCorrelation == null) b else hasSimpleCorrelation && b
                }
            }
            return hasSimpleCorrelation ?: Boolean.FALSE
        }

        @Override
        @Nullable
        fun visitFieldAccess(fieldAccess: RexFieldAccess): Boolean {
            return fieldAccess.getReferenceExpr().accept(this)
        }

        @Override
        fun visitInputRef(inputRef: RexInputRef?): Boolean {
            return Boolean.FALSE
        }

        @Override
        fun visitCorrelVariable(correlVariable: RexCorrelVariable): Boolean {
            return correlVariable.id.equals(corrId)
        }

        @Override
        fun visitTableInputRef(ref: RexTableInputRef?): Boolean {
            return Boolean.FALSE
        }

        @Override
        fun visitLocalRef(localRef: RexLocalRef?): Boolean {
            return Boolean.FALSE
        }

        @Override
        fun visitPatternFieldRef(fieldRef: RexPatternFieldRef?): Boolean {
            return Boolean.FALSE
        }
    }

    /**
     * A visitor traversing row expressions and replacing calls with other expressions according
     * to the specified mapping.
     */
    private class CallReplacer internal constructor(mapping: Map<RexNode, RexNode>) : RexShuttle() {
        private val mapping: Map<RexNode, RexNode>

        init {
            this.mapping = mapping
        }

        @Override
        fun visitCall(oldCall: RexCall): RexNode {
            val newCall: RexNode? = mapping[oldCall]
            return if (newCall != null) {
                newCall
            } else {
                super.visitCall(oldCall)
            }
        }
    }

    companion object {
        /**
         * Traverses a plan and finds all simply correlated row expressions with the specified id.
         */
        private fun findCorrelationDependentCalls(corrId: CorrelationId, plan: RelNode): Set<RexNode> {
            val finder = SimpleCorrelationCollector(corrId)
            plan.accept(object : RelHomogeneousShuttle() {
                @Override
                fun visit(other: RelNode): RelNode {
                    if (other is Project || other is Filter) {
                        other.accept(finder)
                    }
                    return super.visit(other)
                }
            })
            return finder.correlations
        }

        /**
         * Replaces all row expressions in the plan using the provided mapping.
         *
         * @param plan the relational expression on which we want to perform the replacements.
         * @param mapping a mapping defining how to replace row expressions in the plan
         * @return a new relational expression where all expressions present in the mapping are replaced.
         */
        private fun replaceExpressionsUsingMap(plan: RelNode, mapping: Map<RexNode, RexNode>): RelNode {
            val replacer = CallReplacer(mapping)
            return plan.accept(object : RelHomogeneousShuttle() {
                @Override
                fun visit(other: RelNode?): RelNode {
                    val mNode: RelNode = super.visitChildren(other)
                    return mNode.accept(replacer)
                }
            })
        }

        /**
         * Returns whether the specified node is a simply correlated expression.
         */
        private fun isSimpleCorrelatedExpression(node: RexNode, id: CorrelationId): Boolean {
            val r: Boolean = node.accept(SimpleCorrelationDetector(id))
            return r ?: Boolean.FALSE
        }

        private fun replaceCorrelationsWithInputRef(exp: RexNode, b: RelBuilder): RexNode {
            return exp.accept(object : RexShuttle() {
                @Override
                fun visitFieldAccess(fieldAccess: RexFieldAccess): RexNode {
                    return if (fieldAccess.getReferenceExpr() is RexCorrelVariable) {
                        b.field(fieldAccess.getField().getIndex())
                    } else super.visitFieldAccess(fieldAccess)
                }
            })
        }
    }
}
