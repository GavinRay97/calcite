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
package org.apache.calcite.sql.util

import org.apache.calcite.sql.SqlCall

/**
 * Basic implementation of [SqlVisitor] which does nothing at each node.
 *
 *
 * This class is useful as a base class for classes which implement the
 * [SqlVisitor] interface. The derived class can override whichever
 * methods it chooses.
 *
 * @param <R> Return type
</R> */
class SqlBasicVisitor<R> : SqlVisitor<R> {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun visit(literal: SqlLiteral?): R? {
        return null
    }

    @Override
    fun visit(call: SqlCall): R {
        return call.getOperator().acceptCall(this, call)
    }

    @Override
    fun visit(nodeList: SqlNodeList): R? {
        var result: R? = null
        for (i in 0 until nodeList.size()) {
            val node: SqlNode = nodeList.get(i)
            result = node.accept(this)
        }
        return result
    }

    @Override
    fun visit(id: SqlIdentifier?): R? {
        return null
    }

    @Override
    fun visit(type: SqlDataTypeSpec?): R? {
        return null
    }

    @Override
    fun visit(param: SqlDynamicParam?): R? {
        return null
    }

    @Override
    fun visit(intervalQualifier: SqlIntervalQualifier?): R? {
        return null
    }
    //~ Inner Interfaces -------------------------------------------------------
    /** Argument handler.
     *
     * @param <R> result type
    </R> */
    interface ArgHandler<R> {
        /** Returns the result of visiting all children of a call to an operator,
         * then the call itself.
         *
         *
         * Typically the result will be the result of the last child visited, or
         * (if R is [Boolean]) whether all children were visited
         * successfully.  */
        fun result(): R

        /** Visits a particular operand of a call, using a given visitor.  */
        fun visitChild(
            visitor: SqlVisitor<R>?,
            expr: SqlNode?,
            i: Int,
            @Nullable operand: SqlNode?
        ): R
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Default implementation of [ArgHandler] which merely calls
     * [SqlNode.accept] on each operand.
     *
     * @param <R> result type
    </R> */
    class ArgHandlerImpl<R> : ArgHandler<R> {
        @Override
        override fun result(): R? {
            return null
        }

        @Override
        override fun visitChild(
            visitor: SqlVisitor<R>?,
            expr: SqlNode?,
            i: Int,
            @Nullable operand: SqlNode?
        ): R? {
            return if (operand == null) {
                null
            } else operand.accept(visitor)
        }

        companion object {
            private val INSTANCE: ArgHandler<*> = ArgHandlerImpl<Any>()
            @SuppressWarnings("unchecked")
            fun <R> instance(): ArgHandler<R> {
                return INSTANCE as ArgHandler<R>
            }
        }
    }
}
