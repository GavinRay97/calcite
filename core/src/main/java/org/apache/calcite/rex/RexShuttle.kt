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
package org.apache.calcite.rex

import com.google.common.collect.ImmutableList
import org.checkerframework.checker.nullness.qual.PolyNull
import java.util.ArrayList
import java.util.List

/**
 * Passes over a row-expression, calling a handler method for each node,
 * appropriate to the type of the node.
 *
 *
 * Like [RexVisitor], this is an instance of the
 * [Visitor Pattern][org.apache.calcite.util.Glossary.VISITOR_PATTERN]. Use
 * ` RexShuttle` if you would like your methods to return a
 * value.
 */
class RexShuttle : RexVisitor<RexNode?> {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun visitOver(over: RexOver): RexNode {
        val update = booleanArrayOf(false)
        val clonedOperands: List<RexNode> = visitList(over.operands, update)
        val window: RexWindow = visitWindow(over.getWindow())
        return if (update[0] || window !== over.getWindow()) {
            // REVIEW jvs 8-Mar-2005:  This doesn't take into account
            // the fact that a rewrite may have changed the result type.
            // To do that, we would need to take a RexBuilder and
            // watch out for special operators like CAST and NEW where
            // the type is embedded in the original call.
            RexOver(
                over.getType(),
                over.getAggOperator(),
                clonedOperands,
                window,
                over.isDistinct(),
                over.ignoreNulls()
            )
        } else {
            over
        }
    }

    fun visitWindow(window: RexWindow): RexWindow {
        val update = booleanArrayOf(false)
        val clonedOrderKeys: List<RexFieldCollation> = visitFieldCollations(window.orderKeys, update)
        val clonedPartitionKeys: List<RexNode> = visitList(window.partitionKeys, update)
        val lowerBound: RexWindowBound = window.getLowerBound().accept(this)
        val upperBound: RexWindowBound = window.getUpperBound().accept(this)
        if (lowerBound == null || upperBound == null || (!update[0]
                    && lowerBound === window.getLowerBound() && upperBound === window.getUpperBound())
        ) {
            return window
        }
        var rows: Boolean = window.isRows()
        if (lowerBound.isUnbounded() && lowerBound.isPreceding()
            && upperBound.isUnbounded() && upperBound.isFollowing()
        ) {
            // RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            //   is equivalent to
            // ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            //   but we prefer "RANGE"
            rows = false
        }
        return RexWindow(
            clonedPartitionKeys,
            clonedOrderKeys,
            lowerBound,
            upperBound,
            rows
        )
    }

    @Override
    fun visitSubQuery(subQuery: RexSubQuery): RexNode {
        val update = booleanArrayOf(false)
        val clonedOperands: List<RexNode> = visitList(subQuery.operands, update)
        return if (update[0]) {
            subQuery.clone(subQuery.getType(), clonedOperands)
        } else {
            subQuery
        }
    }

    @Override
    fun visitTableInputRef(ref: RexTableInputRef): RexNode {
        return ref
    }

    @Override
    fun visitPatternFieldRef(fieldRef: RexPatternFieldRef): RexNode {
        return fieldRef
    }

    @Override
    fun visitCall(call: RexCall): RexNode {
        val update = booleanArrayOf(false)
        val clonedOperands: List<RexNode> = visitList(call.operands, update)
        return if (update[0]) {
            // REVIEW jvs 8-Mar-2005:  This doesn't take into account
            // the fact that a rewrite may have changed the result type.
            // To do that, we would need to take a RexBuilder and
            // watch out for special operators like CAST and NEW where
            // the type is embedded in the original call.
            call.clone(call.getType(), clonedOperands)
        } else {
            call
        }
    }

    /**
     * Visits each of an array of expressions and returns an array of the
     * results.
     *
     * @param exprs  Array of expressions
     * @param update If not null, sets this to true if any of the expressions
     * was modified
     * @return Array of visited expressions
     */
    protected fun visitArray(exprs: Array<RexNode>, update: @Nullable BooleanArray?): Array<RexNode?> {
        val clonedOperands: Array<RexNode?> = arrayOfNulls<RexNode>(exprs.size)
        for (i in exprs.indices) {
            val operand: RexNode = exprs[i]
            val clonedOperand: RexNode = operand.accept(this)
            if (clonedOperand !== operand && update != null) {
                update[0] = true
            }
            clonedOperands[i] = clonedOperand
        }
        return clonedOperands
    }

    /**
     * Visits each of a list of expressions and returns a list of the
     * results.
     *
     * @param exprs  List of expressions
     * @param update If not null, sets this to true if any of the expressions
     * was modified
     * @return Array of visited expressions
     */
    protected fun visitList(
        exprs: List<RexNode?>, update: @Nullable BooleanArray?
    ): List<RexNode> {
        val clonedOperands: ImmutableList.Builder<RexNode> = ImmutableList.builder()
        for (operand in exprs) {
            val clonedOperand: RexNode = operand!!.accept(this)
            if (clonedOperand !== operand && update != null) {
                update[0] = true
            }
            clonedOperands.add(clonedOperand)
        }
        return clonedOperands.build()
    }

    /**
     * Visits each of a list of field collations and returns a list of the
     * results.
     *
     * @param collations List of field collations
     * @param update     If not null, sets this to true if any of the expressions
     * was modified
     * @return Array of visited field collations
     */
    protected fun visitFieldCollations(
        collations: List<RexFieldCollation?>, update: @Nullable BooleanArray?
    ): List<RexFieldCollation> {
        val clonedOperands: ImmutableList.Builder<RexFieldCollation> = ImmutableList.builder()
        for (collation in collations) {
            val clonedOperand: RexNode = collation.left.accept(this)
            if (clonedOperand !== collation.left && update != null) {
                update[0] = true
                collation = RexFieldCollation(clonedOperand, collation.right)
            }
            clonedOperands.add(collation)
        }
        return clonedOperands.build()
    }

    @Override
    fun visitCorrelVariable(variable: RexCorrelVariable): RexNode {
        return variable
    }

    @Override
    fun visitFieldAccess(fieldAccess: RexFieldAccess): RexNode {
        val before: RexNode = fieldAccess.getReferenceExpr()
        val after: RexNode = before.accept(this)
        return if (before === after) {
            fieldAccess
        } else {
            RexFieldAccess(
                after,
                fieldAccess.getField()
            )
        }
    }

    @Override
    fun visitInputRef(inputRef: RexInputRef): RexNode {
        return inputRef
    }

    @Override
    fun visitLocalRef(localRef: RexLocalRef): RexNode {
        return localRef
    }

    @Override
    fun visitLiteral(literal: RexLiteral): RexNode {
        return literal
    }

    @Override
    fun visitDynamicParam(dynamicParam: RexDynamicParam): RexNode {
        return dynamicParam
    }

    @Override
    fun visitRangeRef(rangeRef: RexRangeRef): RexNode {
        return rangeRef
    }

    /**
     * Applies this shuttle to each expression in a list.
     *
     * @return whether any of the expressions changed
     */
    fun <T : RexNode?> mutate(exprList: List<T>): Boolean {
        var changeCount = 0
        for (i in 0 until exprList.size()) {
            val expr = exprList[i]
            val expr2 = apply(expr) as T? // Avoid NPE if expr is null
            if (expr !== expr2) {
                ++changeCount
                exprList.set(i, expr2)
            }
        }
        return changeCount > 0
    }

    /**
     * Applies this shuttle to each expression in a list and returns the
     * resulting list. Does not modify the initial list.
     *
     *
     * Returns null if and only if `exprList` is null.
     */
    fun <T : RexNode?> apply(@PolyNull exprList: List<T>?): @PolyNull List<T>? {
        if (exprList == null) {
            return exprList
        }
        val list2: List<T> = ArrayList(exprList)
        return if (mutate(list2)) {
            list2
        } else {
            exprList
        }
    }

    /**
     * Applies this shuttle to an expression, or returns null if the expression
     * is null.
     */
    @PolyNull
    fun apply(@PolyNull expr: RexNode?): RexNode? {
        return if (expr == null) expr else expr.accept(this)
    }
}
