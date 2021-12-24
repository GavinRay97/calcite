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
import java.util.ArrayList
import java.util.List

/**
 * Visitor pattern for traversing a tree of [RexNode] objects
 * and passing a payload to each.
 *
 * @see RexVisitor
 *
 *
 * @param <R> Return type
 * @param <P> Payload type
</P></R> */
interface RexBiVisitor<R, P> {
    //~ Methods ----------------------------------------------------------------
    fun visitInputRef(inputRef: RexInputRef?, arg: P): R
    fun visitLocalRef(localRef: RexLocalRef?, arg: P): R
    fun visitLiteral(literal: RexLiteral?, arg: P): R
    fun visitCall(call: RexCall?, arg: P): R
    fun visitOver(over: RexOver?, arg: P): R
    fun visitCorrelVariable(correlVariable: RexCorrelVariable?, arg: P): R
    fun visitDynamicParam(dynamicParam: RexDynamicParam?, arg: P): R
    fun visitRangeRef(rangeRef: RexRangeRef?, arg: P): R
    fun visitFieldAccess(fieldAccess: RexFieldAccess?, arg: P): R
    fun visitSubQuery(subQuery: RexSubQuery?, arg: P): R
    fun visitTableInputRef(ref: RexTableInputRef?, arg: P): R
    fun visitPatternFieldRef(ref: RexPatternFieldRef?, arg: P): R

    /** Visits a list and writes the results to another list.  */
    fun visitList(
        exprs: Iterable<RexNode?>, arg: P,
        out: List<R>
    ) {
        for (expr in exprs) {
            out.add(expr.accept(this, arg))
        }
    }

    /** Visits a list and returns a list of the results.
     * The resulting list is immutable and does not contain nulls.  */
    fun visitList(exprs: Iterable<RexNode?>, arg: P): List<R>? {
        val out: List<R> = ArrayList()
        visitList(exprs, arg, out)
        return ImmutableList.copyOf(out)
    }

    /** Visits a list of expressions.  */
    fun visitEach(exprs: Iterable<RexNode?>, arg: P) {
        for (expr in exprs) {
            expr.accept(this, arg)
        }
    }

    /** Visits a list of expressions, passing the 0-based index of the expression
     * in the list.
     *
     *
     * Assumes that the payload type `P` is `Integer`.  */
    fun visitEachIndexed(exprs: Iterable<RexNode?>) {
        var i = 0
        for (expr in exprs) {
            expr.accept(this, i++ as Integer as P)
        }
    }
}
