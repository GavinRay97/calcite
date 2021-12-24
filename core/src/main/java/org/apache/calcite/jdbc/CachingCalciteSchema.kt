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
 * Concrete implementation of [CalciteSchema] that caches tables,
 * functions and sub-schemas.
 */
class CachingCalciteSchema @SuppressWarnings(["argument.type.incompatible", "return.type.incompatible"]) private constructor(
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
    private val implicitSubSchemaCache: Cached<SubSchemaCache>
    private val implicitTableCache: Cached<NameSet>
    private val implicitFunctionCache: Cached<NameSet>
    private val implicitTypeCache: Cached<NameSet>

    @get:Override
    protected override var isCacheEnabled = true
        private set

    /** Creates a CachingCalciteSchema.  */
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

    init {
        implicitSubSchemaCache = object : AbstractCached<SubSchemaCache?>() {
            @Override
            override fun build(): SubSchemaCache {
                return SubSchemaCache(
                    this@CachingCalciteSchema,
                    schema.getSubSchemaNames()
                )
            }
        }
        implicitTableCache = object : AbstractCached<NameSet?>() {
            @Override
            override fun build(): NameSet {
                return NameSet.immutableCopyOf(
                    schema.getTableNames()
                )
            }
        }
        implicitFunctionCache = object : AbstractCached<NameSet?>() {
            @Override
            override fun build(): NameSet {
                return NameSet.immutableCopyOf(
                    schema.getFunctionNames()
                )
            }
        }
        implicitTypeCache = object : AbstractCached<NameSet?>() {
            @Override
            override fun build(): NameSet {
                return NameSet.immutableCopyOf(
                    schema.getTypeNames()
                )
            }
        }
    }

    @Override
    override fun setCache(cache: Boolean) {
        if (cache == isCacheEnabled) {
            return
        }
        val now: Long = System.currentTimeMillis()
        implicitSubSchemaCache.enable(now, cache)
        implicitTableCache.enable(now, cache)
        implicitFunctionCache.enable(now, cache)
        isCacheEnabled = cache
    }

    @Override
    @Nullable
    protected override fun getImplicitSubSchema(
        schemaName: String?,
        caseSensitive: Boolean
    ): CalciteSchema? {
        val now: Long = System.currentTimeMillis()
        val subSchemaCache = implicitSubSchemaCache[now]
        for (schemaName2 in subSchemaCache.names.range(schemaName, caseSensitive)) {
            return subSchemaCache.cache.getUnchecked(schemaName2)
        }
        return null
    }

    /** Adds a child schema of this schema.  */
    @Override
    fun add(name: String, schema: Schema?): CalciteSchema {
        val calciteSchema: CalciteSchema = CachingCalciteSchema(this, schema, name)
        subSchemaMap.put(name, calciteSchema)
        return calciteSchema
    }

    @Override
    @Nullable
    protected override fun getImplicitTable(
        tableName: String?,
        caseSensitive: Boolean
    ): TableEntry? {
        val now: Long = System.currentTimeMillis()
        val implicitTableNames: NameSet = implicitTableCache[now]
        for (tableName2 in implicitTableNames.range(tableName, caseSensitive)) {
            val table: Table = schema.getTable(tableName2)
            if (table != null) {
                return tableEntry(tableName2, table)
            }
        }
        return null
    }

    @Override
    @Nullable
    protected override fun getImplicitType(name: String?, caseSensitive: Boolean): TypeEntry? {
        val now: Long = System.currentTimeMillis()
        val implicitTypeNames: NameSet = implicitTypeCache[now]
        for (typeName in implicitTypeNames.range(name, caseSensitive)) {
            val type: RelProtoDataType = schema.getType(typeName)
            if (type != null) {
                return typeEntry(name, type)
            }
        }
        return null
    }

    @Override
    protected override fun addImplicitSubSchemaToBuilder(
        builder: ImmutableSortedMap.Builder<String?, CalciteSchema?>
    ) {
        val explicitSubSchemas: ImmutableSortedMap<String, CalciteSchema> = builder.build()
        val now: Long = System.currentTimeMillis()
        val subSchemaCache = implicitSubSchemaCache[now]
        for (name in subSchemaCache.names.iterable()) {
            if (explicitSubSchemas.containsKey(name)) {
                // explicit sub-schema wins.
                continue
            }
            builder.put(name, subSchemaCache.cache.getUnchecked(name))
        }
    }

    @Override
    protected override fun addImplicitTableToBuilder(
        builder: ImmutableSortedSet.Builder<String?>
    ) {
        // Add implicit tables, case-sensitive.
        val now: Long = System.currentTimeMillis()
        val set: NameSet = implicitTableCache[now]
        builder.addAll(set.iterable())
    }

    @Override
    protected override fun addImplicitFunctionsToBuilder(
        builder: ImmutableList.Builder<Function?>,
        name: String?, caseSensitive: Boolean
    ) {
        // Add implicit functions, case-insensitive.
        val now: Long = System.currentTimeMillis()
        val set: NameSet = implicitFunctionCache[now]
        for (name2 in set.range(name, caseSensitive)) {
            val functions: Collection<Function> = schema.getFunctions(name2)
            if (functions != null) {
                builder.addAll(functions)
            }
        }
    }

    @Override
    protected override fun addImplicitFuncNamesToBuilder(
        builder: ImmutableSortedSet.Builder<String?>
    ) {
        // Add implicit functions, case-sensitive.
        val now: Long = System.currentTimeMillis()
        val set: NameSet = implicitFunctionCache[now]
        builder.addAll(set.iterable())
    }

    @Override
    protected override fun addImplicitTypeNamesToBuilder(
        builder: ImmutableSortedSet.Builder<String?>
    ) {
        // Add implicit types, case-sensitive.
        val now: Long = System.currentTimeMillis()
        val set: NameSet = implicitTypeCache[now]
        builder.addAll(set.iterable())
    }

    @Override
    protected override fun addImplicitTablesBasedOnNullaryFunctionsToBuilder(
        builder: ImmutableSortedMap.Builder<String?, Table?>
    ) {
        val explicitTables: ImmutableSortedMap<String, Table> = builder.build()
        val now: Long = System.currentTimeMillis()
        val set: NameSet = implicitFunctionCache[now]
        for (s in set.iterable()) {
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
        val now: Long = System.currentTimeMillis()
        val set: NameSet = implicitFunctionCache[now]
        for (s in set.range(tableName, caseSensitive)) {
            for (function in schema.getFunctions(s)) {
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
        val snapshot: CalciteSchema = CachingCalciteSchema(
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

    @Override
    override fun removeTable(name: String?): Boolean {
        if (isCacheEnabled) {
            val now: Long = System.nanoTime()
            implicitTableCache.enable(now, false)
            implicitTableCache.enable(now, true)
        }
        return super.removeTable(name)
    }

    @Override
    override fun removeFunction(name: String?): Boolean {
        if (isCacheEnabled) {
            val now: Long = System.nanoTime()
            implicitFunctionCache.enable(now, false)
            implicitFunctionCache.enable(now, true)
        }
        return super.removeFunction(name)
    }

    /** Strategy for caching the value of an object and re-creating it if its
     * value is out of date as of a given timestamp.
     *
     * @param <T> Type of cached object
    </T> */
    private interface Cached<T> {
        /** Returns the value; uses cached value if valid.  */
        operator fun get(now: Long): T

        /** Creates a new value.  */
        fun build(): T

        /** Called when CalciteSchema caching is enabled or disabled.  */
        fun enable(now: Long, enabled: Boolean)
    }

    /** Implementation of [CachingCalciteSchema.Cached]
     * that drives from [this.isCacheEnabled].
     *
     * @param <T> element type
    </T> */
    private abstract inner class AbstractCached<T> : Cached<T> {
        @Nullable
        var t: T? = null
        var built = false
        @Override
        override fun get(now: Long): T {
            if (!isCacheEnabled) {
                return build()
            }
            if (!built) {
                t = build()
            }
            built = true
            return castNonNull(t)
        }

        @Override
        override fun enable(now: Long, enabled: Boolean) {
            if (!enabled) {
                t = null
            }
            built = false
        }
    }

    /** Information about the implicit sub-schemas of an [CalciteSchema].  */
    private class SubSchemaCache(
        calciteSchema: CalciteSchema,
        names: Set<String>
    ) {
        /** The names of sub-schemas returned from the [Schema] SPI.  */
        val names: NameSet

        /** Cached [CalciteSchema] wrappers. It is
         * worth caching them because they contain maps of their own sub-objects.  */
        val cache: LoadingCache<String, CalciteSchema>

        init {
            this.names = NameSet.immutableCopyOf(names)
            cache = CacheBuilder.newBuilder().build(
                object : CacheLoader<String?, CalciteSchema?>() {
                    @SuppressWarnings("NullableProblems")
                    @Override
                    fun load(schemaName: String): CalciteSchema {
                        val subSchema: Schema = calciteSchema.schema.getSubSchema(schemaName)
                            ?: throw RuntimeException(
                                "sub-schema " + schemaName
                                        + " not found"
                            )
                        return CachingCalciteSchema(calciteSchema, subSchema, schemaName)
                    }
                })
        }
    }
}
