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
package org.apache.calcite.schema.impl

import org.apache.calcite.adapter.java.JavaTypeFactory

/** Table function that implements a view. It returns the operator
 * tree of the view's SQL query.  */
class ViewTableMacro(
    schema: CalciteSchema, protected val viewSql: String,
    @Nullable schemaPath: List<String?>?, @Nullable viewPath: List<String?>?,
    @Nullable modifiable: Boolean?
) : TableMacro {
    protected val schema: CalciteSchema

    @Nullable
    private val modifiable: Boolean?

    /** Typically null. If specified, overrides the path of the schema as the
     * context for validating `viewSql`.  */
    @Nullable
    protected val schemaPath: List<String>?

    @Nullable
    protected val viewPath: List<String>?

    /**
     * Creates a ViewTableMacro.
     *
     * @param schema     Root schema
     * @param viewSql    SQL defining the view
     * @param schemaPath Schema path relative to the root schema
     * @param viewPath   View path relative to the schema path
     * @param modifiable Request that a view is modifiable (dependent on analysis
     * of `viewSql`)
     */
    init {
        this.schema = schema
        this.viewPath = if (viewPath == null) null else ImmutableList.copyOf(viewPath)
        this.modifiable = modifiable
        this.schemaPath = if (schemaPath == null) null else ImmutableList.copyOf(schemaPath)
    }

    @get:Override
    val parameters: List<Any>
        get() = Collections.emptyList()

    @Override
    fun apply(arguments: List<Object?>?): TranslatableTable {
        val connection: CalciteConnection = MaterializedViewTable.MATERIALIZATION_CONNECTION
        val parsed: CalcitePrepare.AnalyzeViewResult = Schemas.analyzeView(
            connection, schema, schemaPath, viewSql, viewPath,
            modifiable != null && modifiable
        )
        val schemaPath1 = schemaPath ?: schema.path(null)
        return if ((modifiable == null || modifiable)
            && parsed.modifiable
            && parsed.table != null
        ) {
            modifiableViewTable(parsed, viewSql, schemaPath1, viewPath, schema)
        } else {
            viewTable(parsed, viewSql, schemaPath1, viewPath)
        }
    }

    /** Allows a sub-class to return an extension of [ModifiableViewTable]
     * by overriding this method.  */
    protected fun modifiableViewTable(
        parsed: CalcitePrepare.AnalyzeViewResult,
        viewSql: String, schemaPath: List<String?>?, @Nullable viewPath: List<String?>?,
        schema: CalciteSchema
    ): ModifiableViewTable {
        val typeFactory: JavaTypeFactory = parsed.typeFactory as JavaTypeFactory
        val elementType: Type = typeFactory.getJavaClass(parsed.rowType)
        return ModifiableViewTable(
            elementType,
            RelDataTypeImpl.proto(parsed.rowType), viewSql, schemaPath, viewPath,
            requireNonNull(parsed.table, "parsed.table"),
            Schemas.path(schema.root(), requireNonNull(parsed.tablePath, "parsed.tablePath")),
            requireNonNull(parsed.constraint, "parsed.constraint"),
            requireNonNull(parsed.columnMapping, "parsed.columnMapping")
        )
    }

    /** Allows a sub-class to return an extension of [ViewTable] by
     * overriding this method.  */
    protected fun viewTable(
        parsed: CalcitePrepare.AnalyzeViewResult,
        viewSql: String, schemaPath: List<String?>?, @Nullable viewPath: List<String?>?
    ): ViewTable {
        val typeFactory: JavaTypeFactory = parsed.typeFactory as JavaTypeFactory
        val elementType: Type = typeFactory.getJavaClass(parsed.rowType)
        return ViewTable(
            elementType,
            RelDataTypeImpl.proto(parsed.rowType), viewSql, schemaPath, viewPath
        )
    }
}
