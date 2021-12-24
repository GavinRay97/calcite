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

import java.util.List

/**
 * Default implementation of [RexVisitor], which visits each node but does
 * nothing while it's there.
 *
 * @param <R> Return type from each `visitXxx` method.
</R> */
class RexVisitorImpl<R>  //~ Constructors -----------------------------------------------------------
protected constructor(  //~ Instance fields --------------------------------------------------------
    protected val deep: Boolean
) : RexVisitor<R> {
    //~ Methods ----------------------------------------------------------------
    @Override
    override fun visitInputRef(inputRef: RexInputRef?): R? {
        return null
    }

    @Override
    override fun visitLocalRef(localRef: RexLocalRef?): R? {
        return null
    }

    @Override
    override fun visitLiteral(literal: RexLiteral?): R? {
        return null
    }

    @Override
    override fun visitOver(over: RexOver): R? {
        val r = visitCall(over)
        if (!deep) {
            return null
        }
        val window: RexWindow = over.getWindow()
        for (orderKey in window.orderKeys) {
            orderKey.left.accept(this)
        }
        visitEach(window.partitionKeys)
        window.getLowerBound()!!.accept(this)
        window.getUpperBound()!!.accept(this)
        return r
    }

    @Override
    override fun visitCorrelVariable(correlVariable: RexCorrelVariable?): R? {
        return null
    }

    @Override
    override fun visitCall(call: RexCall): R? {
        if (!deep) {
            return null
        }
        var r: R? = null
        for (operand in call.operands) {
            r = operand.accept(this)
        }
        return r
    }

    @Override
    override fun visitDynamicParam(dynamicParam: RexDynamicParam?): R? {
        return null
    }

    @Override
    override fun visitRangeRef(rangeRef: RexRangeRef?): R? {
        return null
    }

    @Override
    override fun visitFieldAccess(fieldAccess: RexFieldAccess): R? {
        if (!deep) {
            return null
        }
        val expr: RexNode = fieldAccess.getReferenceExpr()
        return expr.accept(this)
    }

    @Override
    override fun visitSubQuery(subQuery: RexSubQuery): R? {
        if (!deep) {
            return null
        }
        var r: R? = null
        for (operand in subQuery.operands) {
            r = operand.accept(this)
        }
        return r
    }

    @Override
    override fun visitTableInputRef(ref: RexTableInputRef?): R? {
        return null
    }

    @Override
    override fun visitPatternFieldRef(fieldRef: RexPatternFieldRef?): R? {
        return null
    }

    companion object {
        /**
         *
         * Visits an array of expressions, returning the logical 'and' of their
         * results.
         *
         *
         * If any of them returns false, returns false immediately; if they all
         * return true, returns true.
         *
         * @see .visitArrayOr
         *
         * @see RexShuttle.visitArray
         */
        fun visitArrayAnd(
            visitor: RexVisitor<Boolean?>?,
            exprs: List<RexNode?>
        ): Boolean {
            for (expr in exprs) {
                val b: Boolean = expr.accept(visitor)
                if (!b) {
                    return false
                }
            }
            return true
        }

        /**
         *
         * Visits an array of expressions, returning the logical 'or' of their
         * results.
         *
         *
         * If any of them returns true, returns true immediately; if they all
         * return false, returns false.
         *
         * @see .visitArrayAnd
         *
         * @see RexShuttle.visitArray
         */
        fun visitArrayOr(
            visitor: RexVisitor<Boolean?>?,
            exprs: List<RexNode?>
        ): Boolean {
            for (expr in exprs) {
                val b: Boolean = expr.accept(visitor)
                if (b) {
                    return true
                }
            }
            return false
        }
    }
}
