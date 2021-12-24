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
 * Namespace offered by a sub-query.
 *
 * @see SelectScope
 *
 * @see SetopNamespace
 */
class SelectNamespace(
    validator: SqlValidatorImpl?,
    select: SqlSelect?,
    enclosingNode: SqlNode?
) : AbstractNamespace(validator, enclosingNode) {
    //~ Instance fields --------------------------------------------------------
    private val select: SqlSelect?
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a SelectNamespace.
     *
     * @param validator     Validate
     * @param select        Select node
     * @param enclosingNode Enclosing node
     */
    init {
        this.select = select
    }

    //~ Methods ----------------------------------------------------------------
    // implement SqlValidatorNamespace, overriding return type
    @get:Nullable
    @get:Override
    val node: SqlNode?
        get() = select

    @Override
    fun validateImpl(targetRowType: RelDataType?): RelDataType {
        validator.validateSelect(select, targetRowType)
        return requireNonNull(rowType, "rowType")
    }

    @Override
    fun supportsModality(modality: SqlModality?): Boolean {
        return validator.validateModality(select, modality, false)
    }

    @Override
    fun getMonotonicity(columnName: String?): SqlMonotonicity {
        val rowType: RelDataType = this.getRowTypeSansSystemColumns()
        val field: Int = SqlTypeUtil.findField(rowType, columnName)
        val selectScope: SelectScope = requireNonNull(
            validator.getRawSelectScope(select)
        ) { "rawSelectScope for $select" }
        val selectItem: SqlNode = requireNonNull(
            selectScope.getExpandedSelectList()
        ) { "expandedSelectList for selectScope of $select" }.get(field)
        return validator.getSelectScope(select).getMonotonicity(selectItem)
    }
}
