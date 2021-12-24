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
package org.apache.calcite.sql.type

import org.apache.calcite.rel.metadata.RelColumnMapping

/**
 * TableFunctionReturnTypeInference implements rules for deriving table function
 * output row types by expanding references to cursor parameters.
 */
class TableFunctionReturnTypeInference     //~ Constructors -----------------------------------------------------------
    (
    unexpandedOutputType: RelProtoDataType?,
    //~ Instance fields --------------------------------------------------------
    private val paramNames: List<String>,
    private val isPassthrough: Boolean
) : ExplicitReturnTypeInference(unexpandedOutputType) {
    @MonotonicNonNull
    private var columnMappings // not re-entrant!
            : Set<RelColumnMapping>? = null

    //~ Methods ----------------------------------------------------------------
    @Nullable
    fun getColumnMappings(): Set<RelColumnMapping>? {
        return columnMappings
    }

    @Override
    override fun inferReturnType(
        opBinding: SqlOperatorBinding
    ): RelDataType {
        columnMappings = HashSet()
        val unexpandedOutputType: RelDataType = protoType.apply(opBinding.getTypeFactory())
        val expandedOutputTypes: List<RelDataType> = ArrayList()
        val expandedFieldNames: List<String> = ArrayList()
        for (field in unexpandedOutputType.getFieldList()) {
            val fieldType: RelDataType = field.getType()
            val fieldName: String = field.getName()
            if (fieldType.getSqlTypeName() !== SqlTypeName.CURSOR) {
                expandedOutputTypes.add(fieldType)
                expandedFieldNames.add(fieldName)
                continue
            }

            // Look up position of cursor parameter with same name as output
            // field, also counting how many cursors appear before it
            // (need this for correspondence with RelNode child position).
            var paramOrdinal = -1
            var iCursor = 0
            for (i in 0 until paramNames.size()) {
                if (paramNames[i].equals(fieldName)) {
                    paramOrdinal = i
                    break
                }
                val cursorType: RelDataType = opBinding.getCursorOperand(i)
                if (cursorType != null) {
                    ++iCursor
                }
            }
            assert(paramOrdinal != -1)

            // Translate to actual argument type.
            var isRowOp = false
            val columnNames: List<String> = ArrayList()
            var cursorType: RelDataType = opBinding.getCursorOperand(paramOrdinal)
            if (cursorType == null) {
                isRowOp = true
                val parentCursorName: String = opBinding.getColumnListParamInfo(
                    paramOrdinal,
                    fieldName,
                    columnNames
                )
                assert(parentCursorName != null)
                paramOrdinal = -1
                iCursor = 0
                for (i in 0 until paramNames.size()) {
                    if (paramNames[i].equals(parentCursorName)) {
                        paramOrdinal = i
                        break
                    }
                    cursorType = opBinding.getCursorOperand(i)
                    if (cursorType != null) {
                        ++iCursor
                    }
                }
                cursorType = opBinding.getCursorOperand(paramOrdinal)
                assert(cursorType != null)
            }

            // And expand. Function output is always nullable... except system
            // fields.
            var iInputColumn: Int
            if (isRowOp) {
                for (columnName in columnNames) {
                    iInputColumn = -1
                    var cursorField: RelDataTypeField? = null
                    val cursorTypeFieldList: List<RelDataTypeField> = cursorType.getFieldList()
                    for (cField in cursorTypeFieldList) {
                        ++iInputColumn
                        if (cField.getName().equals(columnName)) {
                            cursorField = cField
                            break
                        }
                    }
                    addOutputColumn(
                        expandedFieldNames,
                        expandedOutputTypes,
                        iInputColumn,
                        iCursor,
                        opBinding,
                        requireNonNull(
                            cursorField
                        ) { "cursorField is not found in $cursorTypeFieldList" })
                }
            } else {
                iInputColumn = -1
                for (cursorField in cursorType.getFieldList()) {
                    ++iInputColumn
                    addOutputColumn(
                        expandedFieldNames,
                        expandedOutputTypes,
                        iInputColumn,
                        iCursor,
                        opBinding,
                        cursorField
                    )
                }
            }
        }
        return opBinding.getTypeFactory().createStructType(
            expandedOutputTypes,
            expandedFieldNames
        )
    }

    @RequiresNonNull("columnMappings")
    private fun addOutputColumn(
        expandedFieldNames: List<String>,
        expandedOutputTypes: List<RelDataType>,
        iInputColumn: Int,
        iCursor: Int,
        opBinding: SqlOperatorBinding,
        cursorField: RelDataTypeField
    ) {
        columnMappings.add(
            RelColumnMapping(
                expandedFieldNames.size(),
                iCursor,
                iInputColumn,
                !isPassthrough
            )
        )

        // As a special case, system fields are implicitly NOT NULL.
        // A badly behaved UDX can still provide NULL values, so the
        // system must ensure that each generated system field has a
        // reasonable value.
        var nullable = true
        if (opBinding is SqlCallBinding) {
            val sqlCallBinding: SqlCallBinding = opBinding as SqlCallBinding
            if (sqlCallBinding.getValidator().isSystemField(
                    cursorField
                )
            ) {
                nullable = false
            }
        }
        val nullableType: RelDataType = opBinding.getTypeFactory().createTypeWithNullability(
            cursorField.getType(),
            nullable
        )

        // Make sure there are no duplicates in the output column names
        for (fieldName in expandedFieldNames) {
            if (fieldName.equals(cursorField.getName())) {
                throw opBinding.newError(
                    RESOURCE.duplicateColumnName(cursorField.getName())
                )
            }
        }
        expandedOutputTypes.add(nullableType)
        expandedFieldNames.add(cursorField.getName())
    }
}
