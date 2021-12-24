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

/**
 * The name-resolution scope of a SELECT clause. The objects visible are those
 * in the FROM clause, and objects inherited from the parent scope.
 *
 *
 *
 * This object is both a [SqlValidatorScope] and a
 * [SqlValidatorNamespace]. In the query
 *
 * <blockquote>
 * <pre>SELECT name FROM (
 * SELECT *
 * FROM emp
 * WHERE gender = 'F')</pre></blockquote>
 *
 *
 * we need to use the [SelectScope] as a
 * [SqlValidatorNamespace] when resolving 'name', and
 * as a [SqlValidatorScope] when resolving 'gender'.
 *
 * <h2>Scopes</h2>
 *
 *
 * In the query
 *
 * <blockquote>
 * <pre>
 * SELECT expr1
 * FROM t1,
 * t2,
 * (SELECT expr2 FROM t3) AS q3
 * WHERE c1 IN (SELECT expr3 FROM t4)
 * ORDER BY expr4</pre>
</blockquote> *
 *
 *
 * The scopes available at various points of the query are as follows:
 *
 *
 *  * expr1 can see t1, t2, q3
 *  * expr2 can see t3
 *  * expr3 can see t4, t1, t2
 *  * expr4 can see t1, t2, q3, plus (depending upon the dialect) any aliases
 * defined in the SELECT clause
 *
 *
 * <h2>Namespaces</h2>
 *
 *
 * In the above query, there are 4 namespaces:
 *
 *
 *  * t1
 *  * t2
 *  * (SELECT expr2 FROM t3) AS q3
 *  * (SELECT expr3 FROM t4)
 *
 *
 * @see SelectNamespace
 */
class SelectScope internal constructor(
    parent: SqlValidatorScope?,
    @Nullable winParent: SqlValidatorScope?,
    select: SqlSelect?
) : ListScope(parent) {
    //~ Instance fields --------------------------------------------------------
    private val select: SqlSelect?
    protected val windowNames: List<String> = ArrayList()

    @get:Nullable
    @Nullable
    var expandedSelectList: List<SqlNode>? = null

    /**
     * List of column names which sort this scope. Empty if this scope is not
     * sorted. Null if has not been computed yet.
     */
    @MonotonicNonNull
    private var orderList: SqlNodeList? = null

    /** Scope to use to resolve windows.  */
    @Nullable
    private val windowParent: SqlValidatorScope?
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a scope corresponding to a SELECT clause.
     *
     * @param parent    Parent scope, must not be null
     * @param winParent Scope for window parent, may be null
     * @param select    Select clause
     */
    init {
        this.select = select
        windowParent = winParent
    }

    //~ Methods ----------------------------------------------------------------
    @get:Nullable
    val table: org.apache.calcite.sql.validate.SqlValidatorTable?
        get() = null

    @get:Override
    val node: SqlSelect?
        get() = select

    @Override
    @Nullable
    fun lookupWindow(name: String?): SqlWindow? {
        val windowList: SqlNodeList = select.getWindowList()
        for (i in 0 until windowList.size()) {
            val window: SqlWindow = windowList.get(i) as SqlWindow
            val declId: SqlIdentifier = Objects.requireNonNull(
                window.getDeclName()
            ) { "declName of window $window" }
            assert(declId.isSimple())
            if (declId.names.get(0).equals(name)) {
                return window
            }
        }

        // if not in the select scope, then check window scope
        return if (windowParent != null) {
            windowParent.lookupWindow(name)
        } else {
            null
        }
    }

    @Override
    fun getMonotonicity(expr: SqlNode): SqlMonotonicity {
        var monotonicity: SqlMonotonicity = expr.getMonotonicity(this)
        if (monotonicity !== SqlMonotonicity.NOT_MONOTONIC) {
            return monotonicity
        }

        // TODO: compare fully qualified names
        val orderList: SqlNodeList? = getOrderList()
        if (orderList.size() > 0) {
            var order0: SqlNode = orderList.get(0)
            monotonicity = SqlMonotonicity.INCREASING
            if (order0 is SqlCall
                && ((order0 as SqlCall).getOperator()
                        === SqlStdOperatorTable.DESC)
            ) {
                monotonicity = monotonicity.reverse()
                order0 = (order0 as SqlCall).operand(0)
            }
            if (expr.equalsDeep(order0, Litmus.IGNORE)) {
                return monotonicity
            }
        }
        return SqlMonotonicity.NOT_MONOTONIC
    }

    @Override
    fun getOrderList(): SqlNodeList? {
        if (orderList == null) {
            // Compute on demand first call.
            orderList = SqlNodeList(SqlParserPos.ZERO)
            if (children.size() === 1) {
                val child: SqlValidatorNamespace = children.get(0).namespace
                val monotonicExprs: List<Pair<SqlNode, SqlMonotonicity>> = child.getMonotonicExprs()
                if (monotonicExprs.size() > 0) {
                    orderList.add(monotonicExprs[0].left)
                }
            }
        }
        return orderList
    }

    fun addWindowName(winName: String?) {
        windowNames.add(winName)
    }

    fun existingWindowName(winName: String?): Boolean {
        for (windowName in windowNames) {
            if (windowName.equalsIgnoreCase(winName)) {
                return true
            }
        }

        // if the name wasn't found then check the parent(s)
        var walker: SqlValidatorScope = parent
        while (walker !is EmptyScope) {
            if (walker is SelectScope) {
                val parentScope = walker as SelectScope
                return parentScope.existingWindowName(winName)
            }
            walker = (walker as DelegatingScope).parent
        }
        return false
    }
}
