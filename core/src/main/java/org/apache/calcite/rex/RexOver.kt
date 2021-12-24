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

import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.SqlWindow
import org.apache.calcite.util.ControlFlowException
import org.apache.calcite.util.Util
import com.google.common.base.Preconditions
import java.util.List
import java.util.Objects

/**
 * Call to an aggregate function over a window.
 */
class RexOver internal constructor(
    type: RelDataType?,
    op: SqlAggFunction,
    operands: List<RexNode?>?,
    window: RexWindow?,
    distinct: Boolean,
    ignoreNulls: Boolean
) : RexCall(type, op, operands) {
    //~ Instance fields --------------------------------------------------------
    private val window: RexWindow
    val isDistinct: Boolean
    private val ignoreNulls: Boolean
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a RexOver.
     *
     *
     * For example, "SUM(DISTINCT x) OVER (ROWS 3 PRECEDING)" is represented
     * as:
     *
     *
     *  * type = Integer,
     *  * op = [org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM],
     *  * operands = { [RexFieldAccess]("x") }
     *  * window = [SqlWindow](ROWS 3 PRECEDING)
     *
     *
     * @param type     Result type
     * @param op       Aggregate operator
     * @param operands Operands list
     * @param window   Window specification
     * @param distinct Aggregate operator is applied on distinct elements
     */
    init {
        Preconditions.checkArgument(op.isAggregator())
        this.window = Objects.requireNonNull(window, "window")
        isDistinct = distinct
        this.ignoreNulls = ignoreNulls
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Returns the aggregate operator for this expression.
     */
    val aggOperator: SqlAggFunction
        get() = getOperator() as SqlAggFunction

    fun getWindow(): RexWindow {
        return window
    }

    fun ignoreNulls(): Boolean {
        return ignoreNulls
    }

    @Override
    protected fun computeDigest(withType: Boolean): String {
        val sb = StringBuilder(op.getName())
        sb.append("(")
        if (isDistinct) {
            sb.append("DISTINCT ")
        }
        appendOperands(sb)
        sb.append(")")
        if (ignoreNulls) {
            sb.append(" IGNORE NULLS")
        }
        if (withType) {
            sb.append(":")
            sb.append(type.getFullTypeString())
        }
        sb.append(" OVER (")
        window.appendDigest(sb, op.allowsFraming())
            .append(")")
        return sb.toString()
    }

    @Override
    fun <R> accept(visitor: RexVisitor<R>): R {
        return visitor.visitOver(this)
    }

    @Override
    fun <R, P> accept(visitor: RexBiVisitor<R, P>, arg: P): R {
        return visitor.visitOver(this, arg)
    }

    @Override
    fun nodeCount(): Int {
        return super.nodeCount() + window.nodeCount
    }

    @Override
    fun clone(type: RelDataType?, operands: List<RexNode?>?): RexCall {
        throw UnsupportedOperationException()
    }
    //~ Inner Classes ----------------------------------------------------------
    /** Exception thrown when an OVER is found.  */
    private object OverFound : ControlFlowException() {
        val INSTANCE: OverFound = OverFound()
    }

    /**
     * Visitor which detects a [RexOver] inside a [RexNode]
     * expression.
     *
     *
     * It is re-entrant (two threads can use an instance at the same time)
     * and it can be re-used for multiple visits.
     */
    private class Finder internal constructor() : RexVisitorImpl<Void?>(true) {
        @Override
        fun visitOver(over: RexOver?): Void {
            throw OverFound.INSTANCE
        }
    }

    @Override
    override fun equals(@Nullable o: Object?): Boolean {
        if (this === o) {
            return true
        }
        if (o == null || getClass() !== o.getClass()) {
            return false
        }
        if (!super.equals(o)) {
            return false
        }
        val rexOver = o as RexOver
        return (isDistinct == rexOver.isDistinct && ignoreNulls == rexOver.ignoreNulls && window.equals(rexOver.window)
                && op.allowsFraming() === rexOver.op.allowsFraming())
    }

    @Override
    override fun hashCode(): Int {
        if (hash === 0) {
            hash = Objects.hash(
                super.hashCode(), window,
                isDistinct, ignoreNulls, op.allowsFraming()
            )
        }
        return hash
    }

    companion object {
        private val FINDER = Finder()

        /**
         * Returns whether an expression contains an OVER clause.
         */
        fun containsOver(expr: RexNode): Boolean {
            return try {
                expr.accept(FINDER)
                false
            } catch (e: OverFound) {
                Util.swallow(e, null)
                true
            }
        }

        /**
         * Returns whether a program contains an OVER clause.
         */
        fun containsOver(program: RexProgram): Boolean {
            return try {
                RexUtil.apply(FINDER, program.getExprList(), null)
                false
            } catch (e: OverFound) {
                Util.swallow(e, null)
                true
            }
        }

        /**
         * Returns whether an expression list contains an OVER clause.
         */
        fun containsOver(
            exprs: List<RexNode?>?,
            @Nullable condition: RexNode?
        ): Boolean {
            return try {
                RexUtil.apply(FINDER, exprs, condition)
                false
            } catch (e: OverFound) {
                Util.swallow(e, null)
                true
            }
        }
    }
}
