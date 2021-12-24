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
 * Refinement to [SqlShuttle] which maintains a stack of scopes.
 *
 *
 * Derived class should override [.visitScoped] rather than
 * [SqlVisitor.visit].
 */
abstract class SqlScopedShuttle protected constructor(initialScope: SqlValidatorScope?) : SqlShuttle() {
    //~ Instance fields --------------------------------------------------------
    private val scopes: Deque<SqlValidatorScope> = ArrayDeque()

    //~ Constructors -----------------------------------------------------------
    init {
        scopes.push(initialScope)
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    @Nullable
    fun visit(call: SqlCall?): SqlNode {
        val oldScope: SqlValidatorScope = scope
        val newScope: SqlValidatorScope = oldScope.getOperandScope(call)
        scopes.push(newScope)
        val result: SqlNode = visitScoped(call)
        scopes.pop()
        return result
    }

    /**
     * Visits an operator call. If the call has entered a new scope, the base
     * class will have already modified the scope.
     */
    @Nullable
    protected fun visitScoped(call: SqlCall?): SqlNode {
        return super.visit(call)
    }

    /**
     * Returns the current scope.
     */
    protected val scope: org.apache.calcite.sql.validate.SqlValidatorScope
        protected get() = requireNonNull(scopes.peek(), "scopes.peek()")
}
