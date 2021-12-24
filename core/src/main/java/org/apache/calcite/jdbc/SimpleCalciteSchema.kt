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
package org.apache.calcite.jdbc

import org.apache.calcite.rel.type.RelProtoDataType

/**
 * A concrete implementation of [org.apache.calcite.jdbc.CalciteSchema]
 * that maintains minimal state.
 */
internal class SimpleCalciteSchema private constructor(
    @Nullable parent: CalciteSchema,
    schema: Schema?,
    name: String,
    @Nullable subSchemaMap: NameMap<CalciteSchema?>?,
    @Nullable tableMap: NameMap<TableEntry?>?,
    @Nullable latticeMap: NameMap<LatticeEntry?>?,
    @Nullable typeMap: NameMap<TypeEntry?>?,
    @Nullable functionMap: NameMultimap<FunctionEntry?>?,
    @Nullable functionNames: NameSet?,
    @Nullable nullaryFunctionMap: NameMap<FunctionEntry?>?,
    @Nullable path: List<List<String?>?>?
) : CalciteSchema(
    parent, schema, name, subSchemaMap, tableMap, latticeMap, typeMap,
    functionMap, functionNames, nullaryFunctionMap, path
) {
    /** Creates a SimpleCalciteSchema.
     *
     *
     * Use [CalciteSchema.createRootSchema]
     * or [.add].  */
    constructor(@Nullable parent: CalciteSchema?, schema: Schema?, name: String) : this(
        parent!!,
        schema,
        name,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    ) {
    }

    @Override
    override fun setCache(cache: Boolean) {
        throw UnsupportedOperationException()
    }

    @Override
    fun add(name: String, schema: Schema?): CalciteSchema {
        val calciteSchema: CalciteSchema = SimpleCalciteSchema(this, schema, name)
        subSchemaMap.put(name, calciteSchema)
        return calciteSchema
    }

    @Override
    @Nullable
    protected fun getImplicitSubSchema(
        schemaName: String,
        caseSensitive: Boolean
    ): CalciteSchema? {
        // Check implicit schemas.
        val schemaName2 = if (caseSensitive) schemaName else caseInsensitiveLookup(
            schema.getSubSchemaNames(), schemaName
        )
        if (schemaName2 == null) {
            return null
        }
        val s: Schema = schema.getSubSchema(schemaName2) ?: return null
        return SimpleCalciteSchema(this, s, schemaName2)
    }

    @Override
    @Nullable
    protected fun getImplicitTable(
        tableName: String,
        caseSensitive: Boolean
    ): TableEntry? {
        // Check implicit tables.
        val tableName2 = if (caseSensitive) tableName else caseInsensitiveLookup(
            schema.getTableNames(), tableName
        )
        if (tableName2 == null) {
            return null
        }
        val table: Table = schema.getTable(tableName2) ?: return null
        return tableEntry(tableName2, table)
    }

    @Override
    @Nullable
    protected fun getImplicitType(name: String, caseSensitive: Boolean): TypeEntry? {
        // Check implicit types.
        val name2 = if (caseSensitive) name else caseInsensitiveLookup(
            schema.getTypeNames(), name
        )
        if (name2 == null) {
            return null
        }
        val type: RelProtoDataType = schema.getType(name2) ?: return null
        return typeEntry(name2, type)
    }

    @Override
    protected override fun addImplicitSubSchemaToBuilder(
        builder: ImmutableSortedMap.Builder<String?, CalciteSchema?>
    ) {
        val explicitSubSchemas: ImmutableSortedMap<String, CalciteSchema> = builder.build()
        for (schemaName in schema.getSubSchemaNames()) {
            if (explicitSubSchemas.containsKey(schemaName)) {
                // explicit subschema wins.
                continue
            }
            val s: Schema = schema.getSubSchema(schemaName)
            if (s != null) {
                val calciteSchema: CalciteSchema = SimpleCalciteSchema(this, s, schemaName)
                builder.put(schemaName, calciteSchema)
            }
        }
    }

    @Override
    protected override fun addImplicitTableToBuilder(builder: ImmutableSortedSet.Builder<String?>) {
        builder.addAll(schema.getTableNames())
    }

    @Override
    protected override fun addImplicitFunctionsToBuilder(
        builder: ImmutableList.Builder<Function?>,
        name: String?, caseSensitive: Boolean
    ) {
        val functions: Collection<Function> = schema.getFunctions(name)
        if (functions != null) {
            builder.addAll(functions)
        }
    }

    @Override
    protected override fun addImplicitFuncNamesToBuilder(
        builder: ImmutableSortedSet.Builder<String?>
    ) {
        builder.addAll(schema.getFunctionNames())
    }

    @Override
    protected override fun addImplicitTypeNamesToBuilder(
        builder: ImmutableSortedSet.Builder<String?>
    ) {
        builder.addAll(schema.getTypeNames())
    }

    @Override
    protected override fun addImplicitTablesBasedOnNullaryFunctionsToBuilder(
        builder: ImmutableSortedMap.Builder<String?, Table?>
    ) {
        val explicitTables: ImmutableSortedMap<String, Table> = builder.build()
        for (s in schema.getFunctionNames()) {
            // explicit table wins.
            if (explicitTables.containsKey(s)) {
                continue
            }
            for (function in schema.getFunctions(s)) {
                if (function is TableMacro
                    && function.getParameters().isEmpty()
                ) {
                    val table: Table = (function as TableMacro).apply(ImmutableList.of())
                    builder.put(s, table)
                }
            }
        }
    }

    @Override
    @Nullable
    protected override fun getImplicitTableBasedOnNullaryFunction(
        tableName: String?,
        caseSensitive: Boolean
    ): TableEntry? {
        val functions: Collection<Function> = schema.getFunctions(tableName)
        if (functions != null) {
            for (function in functions) {
                if (function is TableMacro
                    && function.getParameters().isEmpty()
                ) {
                    val table: Table = (function as TableMacro).apply(ImmutableList.of())
                    return tableEntry(tableName, table)
                }
            }
        }
        return null
    }

    @Override
    protected fun snapshot(
        @Nullable parent: CalciteSchema,
        version: SchemaVersion?
    ): CalciteSchema {
        val snapshot: CalciteSchema = SimpleCalciteSchema(
            parent,
            schema.snapshot(version), name, null, tableMap, latticeMap, typeMap,
            functionMap, functionNames, nullaryFunctionMap, getPath()
        )
        for (subSchema in subSchemaMap.map().values()) {
            val subSchemaSnapshot: CalciteSchema = subSchema.snapshot(snapshot, version)
            snapshot.subSchemaMap.put(subSchema.name, subSchemaSnapshot)
        }
        return snapshot
    }

    @get:Override
    protected override val isCacheEnabled: Boolean
        protected get() = false

    companion object {
        @Nullable
        private fun caseInsensitiveLookup(candidates: Set<String>, name: String): String? {
            // Exact string lookup
            if (candidates.contains(name)) {
                return name
            }
            // Upper case string lookup
            val upperCaseName: String = name.toUpperCase(Locale.ROOT)
            if (candidates.contains(upperCaseName)) {
                return upperCaseName
            }
            // Lower case string lookup
            val lowerCaseName: String = name.toLowerCase(Locale.ROOT)
            if (candidates.contains(lowerCaseName)) {
                return lowerCaseName
            }
            // Fall through: Set iteration
            for (candidate in candidates) {
                if (candidate.equalsIgnoreCase(name)) {
                    return candidate
                }
            }
            return null
        }
    }
}
