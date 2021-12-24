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
 * Visitor pattern for traversing a tree of [RexNode] objects.
 *
 * @see org.apache.calcite.util.Glossary.VISITOR_PATTERN
 *
 * @see RexShuttle
 *
 * @see RexVisitorImpl
 *
 *
 * @param <R> Return type
</R> */
interface RexVisitor<R> {
    //~ Methods ----------------------------------------------------------------
    fun visitInputRef(inputRef: RexInputRef?): R
    fun visitLocalRef(localRef: RexLocalRef?): R
    fun visitLiteral(literal: RexLiteral?): R
    fun visitCall(call: RexCall?): R
    fun visitOver(over: RexOver?): R
    fun visitCorrelVariable(correlVariable: RexCorrelVariable?): R
    fun visitDynamicParam(dynamicParam: RexDynamicParam?): R
    fun visitRangeRef(rangeRef: RexRangeRef?): R
    fun visitFieldAccess(fieldAccess: RexFieldAccess?): R
    fun visitSubQuery(subQuery: RexSubQuery?): R
    fun visitTableInputRef(fieldRef: RexTableInputRef?): R
    fun visitPatternFieldRef(fieldRef: RexPatternFieldRef?): R

    /** Visits a list and writes the results to another list.  */
    fun visitList(exprs: Iterable<RexNode?>, out: List<R>) {
        for (expr in exprs) {
            out.add(expr.accept(this))
        }
    }

    /** Visits a list and returns a list of the results.
     * The resulting list is immutable and does not contain nulls.  */
    fun visitList(exprs: Iterable<RexNode?>): List<R>? {
        val out: List<R> = ArrayList()
        visitList(exprs, out)
        return ImmutableList.copyOf(out)
    }

    /** Visits a list of expressions.  */
    fun visitEach(exprs: Iterable<RexNode?>) {
        for (expr in exprs) {
            expr.accept(this)
        }
    }
}
