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

/** Very similar to [AliasNamespace].  */
internal class WithItemNamespace(
    validator: SqlValidatorImpl?, withItem: SqlWithItem,
    enclosingNode: SqlNode?
) : AbstractNamespace(validator, enclosingNode) {
    private val withItem: SqlWithItem

    init {
        this.withItem = withItem
    }

    @Override
    protected fun validateImpl(targetRowType: RelDataType?): RelDataType {
        val childNs: SqlValidatorNamespace = validator.getNamespaceOrThrow(withItem.query)
        val rowType: RelDataType = childNs.getRowTypeSansSystemColumns()
        val columnList: SqlNodeList = withItem.columnList ?: return rowType
        val builder: RelDataTypeFactory.Builder = validator.getTypeFactory().builder()
        Pair.forEach(
            SqlIdentifier.simpleNames(columnList),
            rowType.getFieldList()
        ) { name, field -> builder.add(name, field.getType()) }
        return builder.build()
    }

    @get:Nullable
    @get:Override
    val node: SqlNode
        get() = withItem

    @Override
    fun translate(name: String): String {
        if (withItem.columnList == null) {
            return name
        }
        val underlyingRowType: RelDataType = validator.getValidatedNodeType(withItem.query)
        var i = 0
        for (field in getRowType().getFieldList()) {
            if (field.getName().equals(name)) {
                return underlyingRowType.getFieldList().get(i).getName()
            }
            ++i
        }
        throw AssertionError(
            "unknown field '" + name
                    + "' in rowtype " + underlyingRowType
        )
    }
}
