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

import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.type.SqlTypeName
import com.google.common.collect.ImmutableSet
import java.util.List
import java.util.Set

/**
 * Utility class for various methods related to multisets.
 */
object RexMultisetUtil {
    //~ Static fields/initializers ---------------------------------------------
    /**
     * A set defining all implementable multiset calls.
     */
    private val MULTISET_OPERATORS: Set<SqlOperator> = ImmutableSet.of(
        SqlStdOperatorTable.CARDINALITY,
        SqlStdOperatorTable.CAST,
        SqlStdOperatorTable.ELEMENT,
        SqlStdOperatorTable.ELEMENT_SLICE,
        SqlStdOperatorTable.MULTISET_EXCEPT_DISTINCT,
        SqlStdOperatorTable.MULTISET_EXCEPT,
        SqlStdOperatorTable.MULTISET_INTERSECT_DISTINCT,
        SqlStdOperatorTable.MULTISET_INTERSECT,
        SqlStdOperatorTable.MULTISET_UNION_DISTINCT,
        SqlStdOperatorTable.MULTISET_UNION,
        SqlStdOperatorTable.IS_A_SET,
        SqlStdOperatorTable.IS_NOT_A_SET,
        SqlStdOperatorTable.MEMBER_OF,
        SqlStdOperatorTable.NOT_SUBMULTISET_OF,
        SqlStdOperatorTable.SUBMULTISET_OF
    )

    /**
     * Returns true if any expression in a program contains a mixing between
     * multiset and non-multiset calls.
     */
    fun containsMixing(program: RexProgram): Boolean {
        val counter = RexCallMultisetOperatorCounter()
        for (expr in program.getExprList()) {
            counter.reset()
            expr.accept(counter)
            if (counter.totalCount != counter.multisetCount
                && 0 != counter.multisetCount
            ) {
                return true
            }
        }
        return false
    }

    /**
     * Returns true if a node contains a mixing between multiset and
     * non-multiset calls.
     */
    fun containsMixing(node: RexNode): Boolean {
        val counter = RexCallMultisetOperatorCounter()
        node.accept(counter)
        return (counter.totalCount != counter.multisetCount
                && 0 != counter.multisetCount)
    }

    /**
     * Returns true if node contains a multiset operator, otherwise false. Use
     * it with deep=false when checking if a RexCall is a multiset call.
     *
     * @param node Expression
     * @param deep If true, returns whether expression contains a multiset. If
     * false, returns whether expression *is* a multiset.
     */
    fun containsMultiset(node: RexNode, deep: Boolean): Boolean {
        return null != findFirstMultiset(node, deep)
    }

    /**
     * Returns whether a list of expressions contains a multiset.
     */
    fun containsMultiset(nodes: List<RexNode>?, deep: Boolean): Boolean {
        for (node in nodes) {
            if (containsMultiset(node, deep)) {
                return true
            }
        }
        return false
    }

    /**
     * Returns whether a program contains a multiset.
     */
    fun containsMultiset(program: RexProgram): Boolean {
        return containsMultiset(program.getExprList(), true)
    }

    /**
     * Returns true if `call` is a call to `CAST` and the to/from
     * cast types are of multiset types.
     */
    fun isMultisetCast(call: RexCall): Boolean {
        return when (call.getKind()) {
            CAST -> call.getType().getSqlTypeName() === SqlTypeName.MULTISET
            else -> false
        }
    }

    /**
     * Returns a reference to the first found multiset call or null if none was
     * found.
     */
    @Nullable
    fun findFirstMultiset(node: RexNode, deep: Boolean): RexCall? {
        if (node is RexFieldAccess) {
            return findFirstMultiset(
                (node as RexFieldAccess).getReferenceExpr(),
                deep
            )
        }
        if (node !is RexCall) {
            return null
        }
        val call: RexCall = node as RexCall
        var firstOne: RexCall? = null
        for (op in MULTISET_OPERATORS) {
            firstOne = RexUtil.findOperatorCall(op, call)
            if (null != firstOne) {
                if (firstOne.getOperator().equals(SqlStdOperatorTable.CAST)
                    && !isMultisetCast(firstOne)
                ) {
                    firstOne = null
                    continue
                }
                break
            }
        }
        return if (!deep && firstOne !== call) {
            null
        } else firstOne
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * A RexShuttle that traverse all RexNode and counts total number of
     * RexCalls traversed and number of multiset calls traversed.
     *
     *
     * totalCount  multisetCount always holds true.
     */
    private class RexCallMultisetOperatorCounter internal constructor() : RexVisitorImpl<Void?>(true) {
        var totalCount = 0
        var multisetCount = 0
        fun reset() {
            totalCount = 0
            multisetCount = 0
        }

        @Override
        fun visitCall(call: RexCall): Void {
            ++totalCount
            if (MULTISET_OPERATORS.contains(call.getOperator())) {
                if (!call.getOperator().equals(SqlStdOperatorTable.CAST)
                    || isMultisetCast(call)
                ) {
                    ++multisetCount
                }
            }
            return super.visitCall(call)
        }
    }
}
