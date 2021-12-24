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
 * Default implementation of a [RexBiVisitor] whose payload and return
 * type are the same.
 *
 * @param <R> Return type from each `visitXxx` method
</R> */
class RexUnaryBiVisitor<R>
/** Creates a RexUnaryBiVisitor.  */
protected constructor(deep: Boolean) : RexBiVisitorImpl<R, R>(deep) {
    /** Called as the last action of, and providing the result for,
     * each `visitXxx` method; derived classes may override.  */
    protected fun end(e: RexNode?, arg: R): R {
        return arg
    }

    @Override
    fun visitInputRef(inputRef: RexInputRef?, arg: R): R {
        return end(inputRef, arg)
    }

    @Override
    fun visitLocalRef(localRef: RexLocalRef?, arg: R): R {
        return end(localRef, arg)
    }

    @Override
    fun visitTableInputRef(ref: RexTableInputRef?, arg: R): R {
        return end(ref, arg)
    }

    @Override
    fun visitPatternFieldRef(fieldRef: RexPatternFieldRef?, arg: R): R {
        return end(fieldRef, arg)
    }

    @Override
    fun visitLiteral(literal: RexLiteral?, arg: R): R {
        return end(literal, arg)
    }

    @Override
    fun visitDynamicParam(dynamicParam: RexDynamicParam?, arg: R): R {
        return end(dynamicParam, arg)
    }

    @Override
    fun visitRangeRef(rangeRef: RexRangeRef?, arg: R): R {
        return end(rangeRef, arg)
    }

    @Override
    fun visitCorrelVariable(correlVariable: RexCorrelVariable?, arg: R): R {
        return end(correlVariable, arg)
    }

    @Override
    fun visitOver(over: RexOver?, arg: R): R {
        super.visitOver(over, arg)
        return end(over, arg)
    }

    @Override
    fun visitCall(call: RexCall?, arg: R): R {
        super.visitCall(call, arg)
        return end(call, arg)
    }

    @Override
    fun visitFieldAccess(fieldAccess: RexFieldAccess?, arg: R): R {
        super.visitFieldAccess(fieldAccess, arg)
        return end(fieldAccess, arg)
    }

    @Override
    fun visitSubQuery(subQuery: RexSubQuery?, arg: R): R {
        super.visitSubQuery(subQuery, arg)
        return end(subQuery, arg)
    }
}
