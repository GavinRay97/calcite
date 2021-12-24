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

/**
 * Default implementation of [RexBiVisitor], which visits each node but
 * does nothing while it's there.
 *
 * @param <R> Return type from each `visitXxx` method
 * @param <P> Payload type
</P></R> */
class RexBiVisitorImpl<R, P>  //~ Constructors -----------------------------------------------------------
protected constructor(  //~ Instance fields --------------------------------------------------------
    protected val deep: Boolean
) : RexBiVisitor<R, P> {
    //~ Methods ----------------------------------------------------------------
    @Override
    override fun visitInputRef(inputRef: RexInputRef?, arg: P): R? {
        return null
    }

    @Override
    override fun visitLocalRef(localRef: RexLocalRef?, arg: P): R? {
        return null
    }

    @Override
    override fun visitLiteral(literal: RexLiteral?, arg: P): R? {
        return null
    }

    @Override
    override fun visitOver(over: RexOver, arg: P): R? {
        val r = visitCall(over, arg)
        if (!deep) {
            return null
        }
        val window: RexWindow = over.getWindow()
        for (orderKey in window.orderKeys) {
            orderKey.left.accept(this, arg)
        }
        for (partitionKey in window.partitionKeys) {
            partitionKey.accept(this, arg)
        }
        window.getLowerBound().accept(this, arg)
        window.getUpperBound().accept(this, arg)
        return r
    }

    @Override
    override fun visitCorrelVariable(correlVariable: RexCorrelVariable?, arg: P): R? {
        return null
    }

    @Override
    fun visitCall(call: RexCall, arg: P): R? {
        if (!deep) {
            return null
        }
        var r: R? = null
        for (operand in call.operands) {
            r = operand.accept(this, arg)
        }
        return r
    }

    @Override
    override fun visitDynamicParam(dynamicParam: RexDynamicParam?, arg: P): R? {
        return null
    }

    @Override
    override fun visitRangeRef(rangeRef: RexRangeRef?, arg: P): R? {
        return null
    }

    @Override
    fun visitFieldAccess(fieldAccess: RexFieldAccess, arg: P): R? {
        if (!deep) {
            return null
        }
        val expr: RexNode = fieldAccess.getReferenceExpr()
        return expr.accept(this, arg)
    }

    @Override
    override fun visitSubQuery(subQuery: RexSubQuery, arg: P): R? {
        if (!deep) {
            return null
        }
        var r: R? = null
        for (operand in subQuery.operands) {
            r = operand.accept(this, arg)
        }
        return r
    }

    @Override
    override fun visitTableInputRef(ref: RexTableInputRef?, arg: P): R? {
        return null
    }

    @Override
    override fun visitPatternFieldRef(fieldRef: RexPatternFieldRef?, arg: P): R? {
        return null
    }
}
