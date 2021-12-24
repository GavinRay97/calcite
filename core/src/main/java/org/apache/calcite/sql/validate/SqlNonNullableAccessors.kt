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

import org.apache.calcite.sql.SqlCallBinding

/**
 * This class provides non-nullable accessors for common getters.
 */
object SqlNonNullableAccessors {
    private fun safeToString(obj: Object?): String {
        return try {
            Objects.toString(obj)
        } catch (e: Throwable) {
            "Error in toString: $e"
        }
    }

    @API(since = "1.27", status = API.Status.EXPERIMENTAL)
    fun getSourceSelect(statement: SqlUpdate): SqlSelect {
        return requireNonNull(
            statement.getSourceSelect()
        ) { "sourceSelect of " + safeToString(statement) }
    }

    @API(since = "1.27", status = API.Status.EXPERIMENTAL)
    fun getSourceSelect(statement: SqlDelete): SqlSelect {
        return requireNonNull(
            statement.getSourceSelect()
        ) { "sourceSelect of " + safeToString(statement) }
    }

    @API(since = "1.27", status = API.Status.EXPERIMENTAL)
    fun getSourceSelect(statement: SqlMerge): SqlSelect {
        return requireNonNull(
            statement.getSourceSelect()
        ) { "sourceSelect of " + safeToString(statement) }
    }

    @API(since = "1.27", status = API.Status.EXPERIMENTAL)
    fun getCondition(join: SqlJoin): SqlNode {
        return requireNonNull(
            join.getCondition()
        ) { "getCondition of " + safeToString(join) }
    }

    @API(since = "1.27", status = API.Status.EXPERIMENTAL)
    fun getNode(child: ScopeChild): SqlNode {
        return requireNonNull(
            child.namespace.getNode()
        ) { "child.namespace.getNode() of " + child.name }
    }

    @API(since = "1.27", status = API.Status.EXPERIMENTAL)
    fun getSelectList(innerSelect: SqlSelect?): SqlNodeList {
        return requireNonNull(
            innerSelect.getSelectList()
        ) { "selectList of " + safeToString(innerSelect) }
    }

    @API(since = "1.27", status = API.Status.EXPERIMENTAL)
    fun getTable(ns: SqlValidatorNamespace): SqlValidatorTable {
        return requireNonNull(
            ns.getTable()
        ) { "ns.getTable() for " + safeToString(ns) }
    }

    @API(since = "1.27", status = API.Status.EXPERIMENTAL)
    fun getScope(callBinding: SqlCallBinding): SqlValidatorScope {
        return requireNonNull(
            callBinding.getScope()
        ) { "scope is null for " + safeToString(callBinding) }
    }

    @API(since = "1.27", status = API.Status.EXPERIMENTAL)
    fun getNamespace(callBinding: SqlCallBinding): SqlValidatorNamespace {
        return requireNonNull(
            callBinding.getValidator().getNamespace(callBinding.getCall())
        ) { "scope is null for " + safeToString(callBinding) }
    }

    @API(since = "1.27", status = API.Status.EXPERIMENTAL)
    fun <T : Object?> getOperandLiteralValueOrThrow(
        opBinding: SqlOperatorBinding,
        ordinal: Int, clazz: Class<T>?
    ): T {
        return requireNonNull(
            opBinding.getOperandLiteralValue(ordinal, clazz)
        ) { "expected non-null operand " + ordinal + " in " + safeToString(opBinding) }
    }
}
