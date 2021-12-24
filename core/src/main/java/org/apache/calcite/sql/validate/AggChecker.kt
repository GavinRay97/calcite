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
package org.apache.calcite.sql.validate

import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlNodeList
import org.apache.calcite.sql.SqlSelect
import org.apache.calcite.sql.SqlWindow
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.util.SqlBasicVisitor
import org.apache.calcite.util.Litmus
import java.util.ArrayDeque
import java.util.Deque
import java.util.List
import org.apache.calcite.sql.validate.SqlNonNullableAccessors.getSelectList
import org.apache.calcite.util.Static.RESOURCE
import java.util.Objects.requireNonNull

/**
 * Visitor which throws an exception if any component of the expression is not a
 * group expression.
 */
internal class AggChecker(
    validator: SqlValidatorImpl,
    scope: AggregatingScope?,
    extraExprs: List<SqlNode>,
    groupExprs: List<SqlNode>,
    distinct: Boolean
) : SqlBasicVisitor<Void?>() {
    //~ Instance fields --------------------------------------------------------
    private val scopes: Deque<SqlValidatorScope> = ArrayDeque()
    private val extraExprs: List<SqlNode>
    private val groupExprs: List<SqlNode>
    private val distinct: Boolean
    private val validator: SqlValidatorImpl
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates an AggChecker.
     *
     * @param validator  Validator
     * @param scope      Scope
     * @param groupExprs Expressions in GROUP BY (or SELECT DISTINCT) clause,
     * that are therefore available
     * @param distinct   Whether aggregation checking is because of a SELECT
     * DISTINCT clause
     */
    init {
        this.validator = validator
        this.extraExprs = extraExprs
        this.groupExprs = groupExprs
        this.distinct = distinct
        scopes.push(scope)
    }

    //~ Methods ----------------------------------------------------------------
    fun isGroupExpr(expr: SqlNode?): Boolean {
        for (groupExpr in groupExprs) {
            if (groupExpr.equalsDeep(expr, Litmus.IGNORE)) {
                return true
            }
        }
        for (extraExpr in extraExprs) {
            if (extraExpr.equalsDeep(expr, Litmus.IGNORE)) {
                return true
            }
        }
        return false
    }

    @Override
    fun visit(id: SqlIdentifier): Void? {
        if (isGroupExpr(id) || id.isStar()) {
            // Star may validly occur in "SELECT COUNT(*) OVER w"
            return null
        }

        // Is it a call to a parentheses-free function?
        val call: SqlCall = validator.makeNullaryCall(id)
        if (call != null) {
            return call.accept(this)
        }

        // Didn't find the identifier in the group-by list as is, now find
        // it fully-qualified.
        // TODO: It would be better if we always compared fully-qualified
        // to fully-qualified.
        val fqId: SqlQualified = scopes.getFirst().fullyQualify(id)
        if (isGroupExpr(fqId.identifier)) {
            return null
        }
        val originalExpr: SqlNode = validator.getOriginal(id)
        val exprString: String = originalExpr.toString()
        throw validator.newValidationError(
            originalExpr,
            if (distinct) RESOURCE.notSelectDistinctExpr(exprString) else RESOURCE.notGroupExpr(exprString)
        )
    }

    @Override
    fun visit(call: SqlCall): Void? {
        val scope: SqlValidatorScope = scopes.peek()
        if (call.getOperator().isAggregator()) {
            if (distinct) {
                if (scope is AggregatingSelectScope) {
                    val selectList: SqlNodeList = getSelectList(scope.getNode() as SqlSelect)

                    // Check if this aggregation function is just an element in the select
                    for (sqlNode in selectList) {
                        if (sqlNode.getKind() === SqlKind.AS) {
                            sqlNode = (sqlNode as SqlCall).operand(0)
                        }
                        if (validator.expand(sqlNode, scope)
                                .equalsDeep(call, Litmus.IGNORE)
                        ) {
                            return null
                        }
                    }
                }

                // Cannot use agg fun in ORDER BY clause if have SELECT DISTINCT.
                val originalExpr: SqlNode = validator.getOriginal(call)
                val exprString: String = originalExpr.toString()
                throw validator.newValidationError(
                    call,
                    RESOURCE.notSelectDistinctExpr(exprString)
                )
            }

            // For example, 'sum(sal)' in 'SELECT sum(sal) FROM emp GROUP
            // BY deptno'
            return null
        }
        when (call.getKind()) {
            FILTER, WITHIN_GROUP, RESPECT_NULLS, IGNORE_NULLS, WITHIN_DISTINCT -> {
                call.operand(0).accept(this)
                return null
            }
            else -> {}
        }
        // Visit the operand in window function
        if (call.getKind() === SqlKind.OVER) {
            for (operand in call.< SqlCall > operand < SqlCall ? > 0.getOperandList()) {
                operand.accept(this)
            }
            // Check the OVER clause
            val over: SqlNode = call.operand(1)
            if (over is SqlCall) {
                over.accept(this)
            } else if (over is SqlIdentifier) {
                // Check the corresponding SqlWindow in WINDOW clause
                val window: SqlWindow = requireNonNull(scope) { "scope for $call" }
                    .lookupWindow((over as SqlIdentifier).getSimple())
                requireNonNull(window) { "window for $call" }
                window.getPartitionList().accept(this)
                window.getOrderList().accept(this)
            }
        }
        if (isGroupExpr(call)) {
            // This call matches an expression in the GROUP BY clause.
            return null
        }
        val groupCall: SqlCall = SqlStdOperatorTable.convertAuxiliaryToGroupCall(call)
        if (groupCall != null) {
            if (isGroupExpr(groupCall)) {
                // This call is an auxiliary function that matches a group call in the
                // GROUP BY clause.
                //
                // For example TUMBLE_START is an auxiliary of the TUMBLE
                // group function, and
                //   TUMBLE_START(rowtime, INTERVAL '1' HOUR)
                // matches
                //   TUMBLE(rowtime, INTERVAL '1' HOUR')
                return null
            }
            throw validator.newValidationError(
                groupCall,
                RESOURCE.auxiliaryWithoutMatchingGroupCall(
                    call.getOperator().getName(), groupCall.getOperator().getName()
                )
            )
        }
        if (call.isA(SqlKind.QUERY)) {
            // Allow queries for now, even though they may contain
            // references to forbidden columns.
            return null
        }

        // Switch to new scope.
        val newScope: SqlValidatorScope = requireNonNull(scope) { "scope for $call" }
            .getOperandScope(call)
        scopes.push(newScope)

        // Visit the operands (only expressions).
        call.getOperator()
            .acceptCall(this, call, true, ArgHandlerImpl.instance())

        // Restore scope.
        scopes.pop()
        return null
    }
}
