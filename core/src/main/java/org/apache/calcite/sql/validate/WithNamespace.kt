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
 * Namespace for `WITH` clause.
 */
class WithNamespace internal constructor(
    validator: SqlValidatorImpl?,
    with: SqlWith?,
    enclosingNode: SqlNode?
) : AbstractNamespace(validator, enclosingNode) {
    //~ Instance fields --------------------------------------------------------
    private val with: SqlWith?
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a TableConstructorNamespace.
     *
     * @param validator     Validator
     * @param with          WITH clause
     * @param enclosingNode Enclosing node
     */
    init {
        this.with = with
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    protected fun validateImpl(targetRowType: RelDataType?): RelDataType {
        for (withItem in with.withList) {
            validator.validateWithItem(withItem as SqlWithItem)
        }
        val scope2: SqlValidatorScope = validator.getWithScope(Util.last(with.withList))
        validator.validateQuery(with.body, scope2, targetRowType)
        val rowType: RelDataType = validator.getValidatedNodeType(with.body)
        validator.setValidatedNodeType(with, rowType)
        return rowType
    }

    @get:Nullable
    @get:Override
    val node: SqlNode?
        get() = with
}
