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

import org.apache.calcite.rel.type.RelDataType

/**
 * Represents the name-resolution context for expressions in an ORDER BY clause.
 *
 *
 * In some dialects of SQL, the ORDER BY clause can reference column aliases
 * in the SELECT clause. For example, the query
 *
 * <blockquote>`SELECT empno AS x<br></br>
 * FROM emp<br></br>
 * ORDER BY x`</blockquote>
 *
 *
 * is valid.
 */
class OrderByScope internal constructor(
    parent: SqlValidatorScope?,
    orderList: SqlNodeList,
    select: SqlSelect
) : DelegatingScope(parent) {
    //~ Instance fields --------------------------------------------------------
    private val orderList: SqlNodeList
    private val select: SqlSelect

    //~ Constructors -----------------------------------------------------------
    init {
        this.orderList = orderList
        this.select = select
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val node: SqlNode
        get() = orderList

    @Override
    fun findAllColumnNames(result: List<SqlMoniker?>?) {
        val ns: SqlValidatorNamespace = validator.getNamespaceOrThrow(select)
        addColumnNames(ns, result)
    }

    @Override
    fun fullyQualify(identifier: SqlIdentifier): SqlQualified {
        // If it's a simple identifier, look for an alias.
        if (identifier.isSimple()
            && validator.config().sqlConformance().isSortByAlias()
        ) {
            val name: String = identifier.names.get(0)
            val selectNs: SqlValidatorNamespace = validator.getNamespaceOrThrow(select)
            val rowType: RelDataType = selectNs.getRowType()
            val nameMatcher: SqlNameMatcher = validator.catalogReader.nameMatcher()
            val field: RelDataTypeField = nameMatcher.field(rowType, name)
            val aliasCount = aliasCount(nameMatcher, name)
            if (aliasCount > 1) {
                // More than one column has this alias.
                throw validator.newValidationError(
                    identifier,
                    RESOURCE.columnAmbiguous(name)
                )
            }
            if (field != null && !field.isDynamicStar() && aliasCount == 1) {
                // if identifier is resolved to a dynamic star, use super.fullyQualify() for such case.
                return SqlQualified.create(this, 1, selectNs, identifier)
            }
        }
        return super.fullyQualify(identifier)
    }

    /** Returns the number of columns in the SELECT clause that have `name`
     * as their implicit (e.g. `t.name`) or explicit (e.g.
     * `t.c as name`) alias.  */
    private fun aliasCount(nameMatcher: SqlNameMatcher, name: String): Int {
        var n = 0
        for (s in getSelectList(select)) {
            val alias: String = SqlValidatorUtil.getAlias(s, -1)
            if (alias != null && nameMatcher.matches(alias, name)) {
                n++
            }
        }
        return n
    }

    @Override
    @Nullable
    fun resolveColumn(name: String?, ctx: SqlNode?): RelDataType {
        val selectNs: SqlValidatorNamespace = validator.getNamespaceOrThrow(select)
        val rowType: RelDataType = selectNs.getRowType()
        val nameMatcher: SqlNameMatcher = validator.catalogReader.nameMatcher()
        val field: RelDataTypeField = nameMatcher.field(rowType, name)
        if (field != null) {
            return field.getType()
        }
        val selectScope: SqlValidatorScope = validator.getSelectScope(select)
        return selectScope.resolveColumn(name, ctx)
    }

    @Override
    fun validateExpr(expr: SqlNode?) {
        val expanded: SqlNode = validator.expandOrderExpr(select, expr)

        // expression needs to be valid in parent scope too
        parent.validateExpr(expanded)
    }
}
