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
import org.apache.calcite.sql.SqlDataTypeSpec
import org.apache.calcite.sql.SqlDynamicParam
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlIntervalQualifier
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlNodeList
import java.util.ArrayList
import java.util.List

/**
 * Basic implementation of [SqlVisitor] which returns each leaf node
 * unchanged.
 *
 *
 * This class is useful as a base class for classes which implement the
 * [SqlVisitor] interface and have [SqlNode] as the return type. The
 * derived class can override whichever methods it chooses.
 */
class SqlShuttle : SqlBasicVisitor<SqlNode?>() {
    //~ Methods ----------------------------------------------------------------
    @Override
    @Nullable
    fun visit(literal: SqlLiteral): SqlNode {
        return literal
    }

    @Override
    @Nullable
    fun visit(id: SqlIdentifier): SqlNode {
        return id
    }

    @Override
    @Nullable
    fun visit(type: SqlDataTypeSpec): SqlNode {
        return type
    }

    @Override
    @Nullable
    fun visit(param: SqlDynamicParam): SqlNode {
        return param
    }

    @Override
    @Nullable
    fun visit(intervalQualifier: SqlIntervalQualifier): SqlNode {
        return intervalQualifier
    }

    @Override
    @Nullable
    fun visit(call: SqlCall): SqlNode {
        // Handler creates a new copy of 'call' only if one or more operands
        // change.
        val argHandler: CallCopyingArgHandler = CallCopyingArgHandler(call, false)
        call.getOperator().acceptCall(this, call, false, argHandler)
        return argHandler.result()
    }

    @Override
    @Nullable
    fun visit(nodeList: SqlNodeList): SqlNode {
        var update = false
        val newList: List<SqlNode> = ArrayList(nodeList.size())
        for (operand in nodeList) {
            var clonedOperand: SqlNode?
            if (operand == null) {
                clonedOperand = null
            } else {
                clonedOperand = operand.accept(this)
                if (clonedOperand !== operand) {
                    update = true
                }
            }
            newList.add(clonedOperand)
        }
        return if (update) {
            SqlNodeList.of(nodeList.getParserPosition(), newList)
        } else {
            nodeList
        }
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Implementation of
     * [org.apache.calcite.sql.util.SqlBasicVisitor.ArgHandler]
     * that deep-copies [SqlCall]s and their operands.
     */
    protected inner class CallCopyingArgHandler(call: SqlCall, alwaysCopy: Boolean) : ArgHandler<SqlNode?> {
        var update: Boolean

        @Nullable
        var clonedOperands: Array<SqlNode>
        private val call: SqlCall
        private val alwaysCopy: Boolean

        init {
            this.call = call
            update = false
            clonedOperands = call.getOperandList().toArray(arrayOfNulls<SqlNode>(0))
            this.alwaysCopy = alwaysCopy
        }

        @Override
        fun result(): SqlNode {
            return if (update || alwaysCopy) {
                call.getOperator().createCall(
                    call.getFunctionQuantifier(),
                    call.getParserPosition(),
                    clonedOperands
                )
            } else {
                call
            }
        }

        @Override
        @Nullable
        fun visitChild(
            visitor: SqlVisitor<SqlNode?>?,
            expr: SqlNode?,
            i: Int,
            @Nullable operand: SqlNode?
        ): SqlNode? {
            if (operand == null) {
                return null
            }
            val newOperand: SqlNode = operand.accept(this@SqlShuttle)
            if (newOperand !== operand) {
                update = true
            }
            clonedOperands[i] = newOperand
            return newOperand
        }
    }
}
