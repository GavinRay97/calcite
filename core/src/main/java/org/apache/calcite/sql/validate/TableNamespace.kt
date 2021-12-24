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

import org.apache.calcite.plan.RelOptTable

/** Namespace based on a table from the catalog.  */
internal class TableNamespace private constructor(
    validator: SqlValidatorImpl, table: SqlValidatorTable,
    fields: List<RelDataTypeField>
) : AbstractNamespace(validator, null) {
    private val table: SqlValidatorTable
    val extendedFields: ImmutableList<RelDataTypeField>

    /** Creates a TableNamespace.  */
    init {
        this.table = Objects.requireNonNull(table, "table")
        extendedFields = ImmutableList.copyOf(fields)
    }

    constructor(validator: SqlValidatorImpl, table: SqlValidatorTable) : this(validator, table, ImmutableList.of()) {}

    @Override
    protected fun validateImpl(targetRowType: RelDataType?): RelDataType {
        if (extendedFields.isEmpty()) {
            return table.getRowType()
        }
        val builder: RelDataTypeFactory.Builder = validator.getTypeFactory().builder()
        builder.addAll(table.getRowType().getFieldList())
        builder.addAll(extendedFields)
        return builder.build()
    }

    // This is the only kind of namespace not based on a node in the parse tree.
    @get:Nullable
    @get:Override
    val node: SqlNode?
        get() =// This is the only kind of namespace not based on a node in the parse tree.
            null

    @Override
    fun getTable(): SqlValidatorTable {
        return table
    }

    @Override
    fun getMonotonicity(columnName: String?): SqlMonotonicity {
        val table: SqlValidatorTable = getTable()
        return table.getMonotonicity(columnName)
    }

    /** Creates a TableNamespace based on the same table as this one, but with
     * extended fields.
     *
     *
     * Extended fields are "hidden" or undeclared fields that may nevertheless
     * be present if you ask for them. Phoenix uses them, for instance, to access
     * rarely used fields in the underlying HBase table.  */
    fun extend(extendList: SqlNodeList): TableNamespace {
        val identifierList: List<SqlNode> = Util.quotientList(extendList, 2, 0)
        SqlValidatorUtil.checkIdentifierListForDuplicates(
            identifierList, validator.getValidationErrorFunction()
        )
        val builder: ImmutableList.Builder<RelDataTypeField> = ImmutableList.builder()
        builder.addAll(extendedFields)
        builder.addAll(
            SqlValidatorUtil.getExtendedColumns(
                validator,
                getTable(), extendList
            )
        )
        val extendedFields: List<RelDataTypeField> = builder.build()
        val schemaTable: Table = table.unwrap(Table::class.java)
        if (table is RelOptTable
            && (schemaTable is ExtensibleTable
                    || schemaTable is ModifiableViewTable)
        ) {
            checkExtendedColumnTypes(extendList)
            val relOptTable: RelOptTable = (table as RelOptTable).extend(extendedFields)
            val validatorTable: SqlValidatorTable = Objects.requireNonNull(
                relOptTable.unwrap(SqlValidatorTable::class.java)
            ) { "cant unwrap SqlValidatorTable from $relOptTable" }
            return TableNamespace(validator, validatorTable, ImmutableList.of())
        }
        return TableNamespace(validator, table, extendedFields)
    }

    /**
     * Gets the data-type of all columns in a table. For a view table, includes
     * columns of the underlying table.
     */
    private val baseRowType: RelDataType
        private get() {
            val schemaTable: Table = Objects.requireNonNull(
                table.unwrap(Table::class.java)
            ) { "can't unwrap Table from $table" }
            if (schemaTable is ModifiableViewTable) {
                val underlying: Table = (schemaTable as ModifiableViewTable).unwrap(Table::class.java)
                assert(underlying != null)
                return underlying.getRowType(validator.typeFactory)
            }
            return schemaTable.getRowType(validator.typeFactory)
        }

    /**
     * Ensures that extended columns that have the same name as a base column also
     * have the same data-type.
     */
    private fun checkExtendedColumnTypes(extendList: SqlNodeList) {
        val extendedFields: List<RelDataTypeField> = SqlValidatorUtil.getExtendedColumns(validator, table, extendList)
        val baseFields: List<RelDataTypeField> = baseRowType.getFieldList()
        val nameToIndex: Map<String, Integer> = SqlValidatorUtil.mapNameToIndex(baseFields)
        for (extendedField in extendedFields) {
            val extFieldName: String = extendedField.getName()
            if (nameToIndex.containsKey(extFieldName)) {
                val baseIndex: Integer? = nameToIndex[extFieldName]
                val baseType: RelDataType = baseFields[baseIndex].getType()
                val extType: RelDataType = extendedField.getType()
                if (!extType.equals(baseType)) {
                    // Get the extended column node that failed validation.
                    val extColNode: SqlNode = Iterables.find(
                        extendList
                    ) { sqlNode ->
                        (sqlNode is SqlIdentifier
                                && Util.last((sqlNode as SqlIdentifier).names).equals(
                            extendedField.getName()
                        ))
                    }
                    throw validator.getValidationErrorFunction().apply(
                        extColNode,
                        RESOURCE.typeNotAssignable(
                            baseFields[baseIndex].getName(), baseType.getFullTypeString(),
                            extendedField.getName(), extType.getFullTypeString()
                        )
                    )
                }
            }
        }
    }
}
