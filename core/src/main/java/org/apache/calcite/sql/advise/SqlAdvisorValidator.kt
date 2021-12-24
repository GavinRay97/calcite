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
package org.apache.calcite.sql.advise

import org.apache.calcite.rel.type.RelDataType

/**
 * `SqlAdvisorValidator` is used by [SqlAdvisor] to traverse
 * the parse tree of a SQL statement, not for validation purpose but for setting
 * up the scopes and namespaces to facilitate retrieval of SQL statement
 * completion hints.
 */
class SqlAdvisorValidator  //~ Constructors -----------------------------------------------------------
/**
 * Creates a SqlAdvisor validator.
 *
 * @param opTab         Operator table
 * @param catalogReader Catalog reader
 * @param typeFactory   Type factory
 * @param config        Config
 */
    (
    opTab: SqlOperatorTable?,
    catalogReader: SqlValidatorCatalogReader?,
    typeFactory: RelDataTypeFactory?,
    config: Config?
) : SqlValidatorImpl(opTab, catalogReader, typeFactory, config) {
    //~ Instance fields --------------------------------------------------------
    private val activeNamespaces: Set<SqlValidatorNamespace> = HashSet()
    private val emptyStructType: RelDataType = SqlTypeUtil.createEmptyStructType(typeFactory)
    //~ Methods ----------------------------------------------------------------
    /**
     * Registers the identifier and its scope into a map keyed by ParserPosition.
     */
    @Override
    fun validateIdentifier(id: SqlIdentifier, scope: SqlValidatorScope) {
        registerId(id, scope)
        try {
            super.validateIdentifier(id, scope)
        } catch (e: CalciteException) {
            Util.swallow(e, TRACER)
        }
    }

    private fun registerId(id: SqlIdentifier, scope: SqlValidatorScope) {
        for (i in 0 until id.names.size()) {
            val subPos: SqlParserPos = id.getComponentParserPosition(i)
            val subId: SqlIdentifier =
                if (i == id.names.size() - 1) id else SqlIdentifier(id.names.subList(0, i + 1), subPos)
            idPositions.put(subPos.toString(), IdInfo(scope, subId))
        }
    }

    @Override
    fun expand(expr: SqlNode, scope: SqlValidatorScope?): SqlNode {
        // Disable expansion. It doesn't help us come up with better hints.
        return expr
    }

    @Override
    fun expandSelectExpr(
        expr: SqlNode,
        scope: SelectScope?, select: SqlSelect?
    ): SqlNode {
        // Disable expansion. It doesn't help us come up with better hints.
        return expr
    }

    @Override
    fun expandOrderExpr(select: SqlSelect?, orderExpr: SqlNode): SqlNode {
        // Disable expansion. It doesn't help us come up with better hints.
        return orderExpr
    }

    /**
     * Calls the parent class method and mask Farrago exception thrown.
     */
    @Override
    fun deriveType(
        scope: SqlValidatorScope?,
        operand: SqlNode?
    ): RelDataType {
        // REVIEW Do not mask Error (indicates a serious system problem) or
        // UnsupportedOperationException (a bug). I have to mask
        // UnsupportedOperationException because
        // SqlValidatorImpl.getValidatedNodeType throws it for an unrecognized
        // identifier node I have to mask Error as well because
        // AbstractNamespace.getRowType  called in super.deriveType can do a
        // Util.permAssert that throws Error
        return try {
            super.deriveType(scope, operand)
        } catch (e: CalciteException) {
            unknownType
        } catch (e: UnsupportedOperationException) {
            unknownType
        } catch (e: Error) {
            unknownType
        }
    }

    // we do not need to validate from clause for traversing the parse tree
    // because there is no SqlIdentifier in from clause that need to be
    // registered into {@link #idPositions} map
    @Override
    protected fun validateFrom(
        node: SqlNode?,
        targetRowType: RelDataType?,
        scope: SqlValidatorScope?
    ) {
        try {
            super.validateFrom(node, targetRowType, scope)
        } catch (e: CalciteException) {
            Util.swallow(e, TRACER)
        }
    }

    /**
     * Calls the parent class method and masks Farrago exception thrown.
     */
    @Override
    protected fun validateWhereClause(select: SqlSelect?) {
        try {
            super.validateWhereClause(select)
        } catch (e: CalciteException) {
            Util.swallow(e, TRACER)
        }
    }

    /**
     * Calls the parent class method and masks Farrago exception thrown.
     */
    @Override
    protected fun validateHavingClause(select: SqlSelect?) {
        try {
            super.validateHavingClause(select)
        } catch (e: CalciteException) {
            Util.swallow(e, TRACER)
        }
    }

    @Override
    protected fun validateOver(call: SqlCall, scope: SqlValidatorScope?) {
        try {
            val overScope: OverScope = getOverScope(call) as OverScope
            val relation: SqlNode = call.operand(0)
            validateFrom(relation, unknownType, scope)
            val window: SqlNode = call.operand(1)
            var opScope: SqlValidatorScope = scopes.get(relation)
            if (opScope == null) {
                opScope = overScope
            }
            validateWindow(window, opScope, null)
        } catch (e: CalciteException) {
            Util.swallow(e, TRACER)
        }
    }

    @Override
    protected fun validateNamespace(
        namespace: SqlValidatorNamespace,
        targetRowType: RelDataType?
    ) {
        // Only attempt to validate each namespace once. Otherwise if
        // validation fails, we may end up cycling.
        if (activeNamespaces.add(namespace)) {
            super.validateNamespace(namespace, targetRowType)
        } else {
            namespace.setType(emptyStructType)
        }
    }

    @Override
    fun validateModality(
        select: SqlSelect?,
        modality: SqlModality?, fail: Boolean
    ): Boolean {
        return true
    }

    @Override
    protected fun shouldAllowOverRelation(): Boolean {
        return true // no reason not to be lenient
    }
}
