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
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlNodeList
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.util.Pair
import org.apache.calcite.util.Util
import java.util.List
import org.apache.calcite.util.Static.RESOURCE

/**
 * Namespace for an `AS t(c1, c2, ...)` clause.
 *
 *
 * A namespace is necessary only if there is a column list, in order to
 * re-map column names; a `relation AS t` clause just uses the same
 * namespace as `relation`.
 */
class AliasNamespace protected constructor(
    validator: SqlValidatorImpl?,
    call: SqlCall,
    enclosingNode: SqlNode?
) : AbstractNamespace(validator, enclosingNode) {
    //~ Instance fields --------------------------------------------------------
    protected val call: SqlCall
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates an AliasNamespace.
     *
     * @param validator     Validator
     * @param call          Call to AS operator
     * @param enclosingNode Enclosing node
     */
    init {
        this.call = call
        assert(call.getOperator() === SqlStdOperatorTable.AS)
        assert(call.operandCount() >= 2)
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun supportsModality(modality: SqlModality?): Boolean {
        val operands: List<SqlNode> = call.getOperandList()
        val childNs: SqlValidatorNamespace = validator.getNamespaceOrThrow(operands[0])
        return childNs.supportsModality(modality)
    }

    @Override
    protected fun validateImpl(targetRowType: RelDataType?): RelDataType {
        val operands: List<SqlNode> = call.getOperandList()
        val childNs: SqlValidatorNamespace = validator.getNamespaceOrThrow(operands[0])
        val rowType: RelDataType = childNs.getRowTypeSansSystemColumns()
        val aliasedType: RelDataType
        aliasedType = if (operands.size() === 2) {
            // Alias is 'AS t' (no column list).
            // If the sub-query is UNNEST or VALUES,
            // and the sub-query has one column,
            // then the namespace's sole column is named after the alias.
            if (rowType.getFieldCount() === 1) {
                validator.getTypeFactory().builder()
                    .kind(rowType.getStructKind())
                    .add(
                        (operands[1] as SqlIdentifier).getSimple(),
                        rowType.getFieldList().get(0).getType()
                    )
                    .build()
            } else {
                rowType
            }
        } else {
            // Alias is 'AS t (c0, ..., cN)'
            val columnNames: List<SqlNode> = Util.skip(operands, 2)
            val nameList: List<String> = SqlIdentifier.simpleNames(columnNames)
            val i: Int = Util.firstDuplicate(nameList)
            if (i >= 0) {
                val id: SqlIdentifier = columnNames[i] as SqlIdentifier
                throw validator.newValidationError(
                    id,
                    RESOURCE.aliasListDuplicate(id.getSimple())
                )
            }
            if (columnNames.size() !== rowType.getFieldCount()) {
                // Position error over all column names
                val node: SqlNode =
                    if (operands.size() === 3) operands[2] else SqlNodeList(columnNames, SqlParserPos.sum(columnNames))
                throw validator.newValidationError(
                    node,
                    RESOURCE.aliasListDegree(
                        rowType.getFieldCount(),
                        getString(rowType), columnNames.size()
                    )
                )
            }
            validator.getTypeFactory().builder()
                .addAll(
                    Util.transform(rowType.getFieldList()) { f -> Pair.of(nameList[f.getIndex()], f.getType()) })
                .kind(rowType.getStructKind())
                .build()
        }

        // As per suggestion in CALCITE-4085, JavaType has its special nullability handling.
        return if (rowType is RelDataTypeFactoryImpl.JavaType) {
            aliasedType
        } else {
            validator.getTypeFactory()
                .createTypeWithNullability(aliasedType, rowType.isNullable())
        }
    }

    @get:Nullable
    @get:Override
    val node: SqlNode
        get() = call

    @Override
    fun translate(name: String): String {
        val underlyingRowType: RelDataType = validator.getValidatedNodeType(call.operand(0))
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

    companion object {
        private fun getString(rowType: RelDataType): String {
            val buf = StringBuilder()
            buf.append("(")
            for (field in rowType.getFieldList()) {
                if (field.getIndex() > 0) {
                    buf.append(", ")
                }
                buf.append("'")
                buf.append(field.getName())
                buf.append("'")
            }
            buf.append(")")
            return buf.toString()
        }
    }
}
