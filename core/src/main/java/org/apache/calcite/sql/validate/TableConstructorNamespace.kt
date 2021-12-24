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
 * Namespace for a table constructor `VALUES (expr, expr, ...)`.
 */
class TableConstructorNamespace internal constructor(
    validator: SqlValidatorImpl?,
    values: SqlCall?,
    scope: SqlValidatorScope,
    enclosingNode: SqlNode?
) : AbstractNamespace(validator, enclosingNode) {
    //~ Instance fields --------------------------------------------------------
    private val values: SqlCall?
    private val scope: SqlValidatorScope
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a TableConstructorNamespace.
     *
     * @param validator     Validator
     * @param values        VALUES parse tree node
     * @param scope         Scope
     * @param enclosingNode Enclosing node
     */
    init {
        this.values = values
        this.scope = scope
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    protected fun validateImpl(targetRowType: RelDataType?): RelDataType {
        // First, validate the VALUES. If VALUES is inside INSERT, infers
        // the type of NULL values based on the types of target columns.
        validator.validateValues(values, targetRowType, scope)
        return validator.getTableConstructorRowType(values, scope)
            ?: throw validator.newValidationError(values, RESOURCE.incompatibleTypes())
    }

    @get:Nullable
    @get:Override
    val node: SqlNode?
        get() = values

    /**
     * Returns the scope.
     *
     * @return scope
     */
    fun getScope(): SqlValidatorScope {
        return scope
    }

    @Override
    fun supportsModality(modality: SqlModality): Boolean {
        return modality === SqlModality.RELATION
    }
}
