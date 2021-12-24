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

import org.apache.calcite.adapter.jdbc.JdbcCatalogSchema

/**
 * Schema.
 *
 *
 * Wrapper around user-defined schema used internally.
 */
abstract class CalciteSchema protected constructor(
    @field:Nullable @param:Nullable private val parent: CalciteSchema, schema: Schema?,
    name: String,
    @Nullable subSchemaMap: NameMap<CalciteSchema?>?,
    @Nullable tableMap: NameMap<TableEntry?>?,
    @Nullable latticeMap: NameMap<LatticeEntry?>?,
    @Nullable typeMap: NameMap<TypeEntry?>?,
    @Nullable functionMap: NameMultimap<FunctionEntry?>?,
    @Nullable functionNames: NameSet?,
    @Nullable nullaryFunctionMap: NameMap<FunctionEntry?>?,
    @Nullable path: List<List<String?>?>?
) {
    val schema: Schema?
    val name: String

    /** Tables explicitly defined in this schema. Does not include tables in
     * [.schema].  */
    protected val tableMap: NameMap<TableEntry>? = null
    protected val functionMap: NameMultimap<FunctionEntry>? = null
    protected val typeMap: NameMap<TypeEntry>? = null
    protected val latticeMap: NameMap<LatticeEntry>? = null
    protected val functionNames: NameSet? = null
    protected val nullaryFunctionMap: NameMap<FunctionEntry>? = null
    val subSchemaMap: NameMap<CalciteSchema>? = null

    @Nullable
    private var path: List<List<String?>?>?

    init {
        this.schema = schema
        this.name = name
        if (tableMap == null) {
            this.tableMap = NameMap()
        } else {
            this.tableMap = Objects.requireNonNull(tableMap, "tableMap")
        }
        if (latticeMap == null) {
            this.latticeMap = NameMap()
        } else {
            this.latticeMap = Objects.requireNonNull(latticeMap, "latticeMap")
        }
        if (subSchemaMap == null) {
            this.subSchemaMap = NameMap()
        } else {
            this.subSchemaMap = Objects.requireNonNull(subSchemaMap, "subSchemaMap")
        }
        if (functionMap == null) {
            this.functionMap = NameMultimap()
            this.functionNames = NameSet()
            this.nullaryFunctionMap = NameMap()
        } else {
            // If you specify functionMap, you must also specify functionNames and
            // nullaryFunctionMap.
            this.functionMap = Objects.requireNonNull(functionMap, "functionMap")
            this.functionNames = Objects.requireNonNull(functionNames, "functionNames")
            this.nullaryFunctionMap = Objects.requireNonNull(nullaryFunctionMap, "nullaryFunctionMap")
        }
        if (typeMap == null) {
            this.typeMap = NameMap()
        } else {
            this.typeMap = Objects.requireNonNull(typeMap, "typeMap")
        }
        this.path = path
    }

    /** Returns a sub-schema with a given name that is defined implicitly
     * (that is, by the underlying [Schema] object, not explicitly
     * by a call to [.add]), or null.  */
    @Nullable
    protected abstract fun getImplicitSubSchema(
        schemaName: String?,
        caseSensitive: Boolean
    ): CalciteSchema

    /** Returns a table with a given name that is defined implicitly
     * (that is, by the underlying [Schema] object, not explicitly
     * by a call to [.add]), or null.  */
    @Nullable
    protected abstract fun getImplicitTable(
        tableName: String?,
        caseSensitive: Boolean
    ): TableEntry

    /** Returns a type with a given name that is defined implicitly
     * (that is, by the underlying [Schema] object, not explicitly
     * by a call to [.add]), or null.  */
    @Nullable
    protected abstract fun getImplicitType(
        name: String?,
        caseSensitive: Boolean
    ): TypeEntry

    /** Returns table function with a given name and zero arguments that is
     * defined implicitly (that is, by the underlying [Schema] object,
     * not explicitly by a call to [.add]), or null.  */
    @Nullable
    protected abstract fun getImplicitTableBasedOnNullaryFunction(
        tableName: String?,
        caseSensitive: Boolean
    ): TableEntry

    /** Adds implicit sub-schemas to a builder.  */
    protected abstract fun addImplicitSubSchemaToBuilder(
        builder: ImmutableSortedMap.Builder<String?, CalciteSchema?>?
    )

    /** Adds implicit tables to a builder.  */
    protected abstract fun addImplicitTableToBuilder(
        builder: ImmutableSortedSet.Builder<String?>?
    )

    /** Adds implicit functions to a builder.  */
    protected abstract fun addImplicitFunctionsToBuilder(
        builder: ImmutableList.Builder<Function?>?,
        name: String?, caseSensitive: Boolean
    )

    /** Adds implicit function names to a builder.  */
    protected abstract fun addImplicitFuncNamesToBuilder(
        builder: ImmutableSortedSet.Builder<String?>?
    )

    /** Adds implicit type names to a builder.  */
    protected abstract fun addImplicitTypeNamesToBuilder(
        builder: ImmutableSortedSet.Builder<String?>?
    )

    /** Adds implicit table functions to a builder.  */
    protected abstract fun addImplicitTablesBasedOnNullaryFunctionsToBuilder(
        builder: ImmutableSortedMap.Builder<String?, Table?>?
    )

    /** Returns a snapshot representation of this CalciteSchema.  */
    abstract fun snapshot(
        @Nullable parent: CalciteSchema?, version: SchemaVersion?
    ): CalciteSchema

    protected abstract val isCacheEnabled: Boolean
    abstract fun setCache(cache: Boolean)

    /** Creates a TableEntryImpl with no SQLs.  */
    protected fun tableEntry(name: String?, table: Table?): TableEntryImpl {
        return TableEntryImpl(this, name, table, ImmutableList.of())
    }

    /** Creates a TableEntryImpl with no SQLs.  */
    protected fun typeEntry(name: String?, relProtoDataType: RelProtoDataType?): TypeEntryImpl {
        return TypeEntryImpl(this, name, relProtoDataType)
    }

    /** Defines a table within this schema.  */
    fun add(tableName: String?, table: Table?): TableEntry {
        return add(tableName, table, ImmutableList.of())
    }

    /** Defines a table within this schema.  */
    fun add(
        tableName: String?, table: Table?,
        sqls: ImmutableList<String?>?
    ): TableEntry {
        val entry = TableEntryImpl(this, tableName, table, sqls)
        tableMap.put(tableName, entry)
        return entry
    }

    /** Defines a type within this schema.  */
    fun add(name: String?, type: RelProtoDataType?): TypeEntry {
        val entry: TypeEntry = TypeEntryImpl(this, name, type)
        typeMap.put(name, entry)
        return entry
    }

    private fun add(name: String, function: Function): FunctionEntry {
        val entry = FunctionEntryImpl(this, name, function)
        functionMap.put(name, entry)
        functionNames.add(name)
        if (function.getParameters().isEmpty()) {
            nullaryFunctionMap.put(name, entry)
        }
        return entry
    }

    private fun add(name: String, lattice: Lattice): LatticeEntry {
        if (latticeMap.containsKey(name, false)) {
            throw RuntimeException("Duplicate lattice '$name'")
        }
        val entry = LatticeEntryImpl(this, name, lattice)
        latticeMap.put(name, entry)
        return entry
    }

    fun root(): CalciteSchema {
        run {
            var schema = this
            while (true) {
                if (schema.parent == null) {
                    return schema
                }
                schema = schema.parent
            }
        }
    }

    /** Returns whether this is a root schema.  */
    val isRoot: Boolean
        get() = parent == null

    /** Returns the path of an object in this schema.  */
    fun path(@Nullable name: String?): List<String> {
        val list: List<String> = ArrayList()
        if (name != null) {
            list.add(name)
        }
        var s = this
        while (s != null) {
            if (s.parent != null || !s.name.equals("")) {
                // Omit the root schema's name from the path if it's the empty string,
                // which it usually is.
                list.add(s.name)
            }
            s = s.parent
        }
        return ImmutableList.copyOf(Lists.reverse(list))
    }

    @Nullable
    fun getSubSchema(
        schemaName: String?,
        caseSensitive: Boolean
    ): CalciteSchema {
        // Check explicit schemas.
        for (entry in subSchemaMap.range(schemaName, caseSensitive).entrySet()) {
            return entry.getValue()
        }
        return getImplicitSubSchema(schemaName, caseSensitive)
    }

    /** Adds a child schema of this schema.  */
    abstract fun add(name: String?, schema: Schema?): CalciteSchema?

    /** Returns a table that materializes the given SQL statement.  */
    @Nullable
    fun getTableBySql(sql: String?): TableEntry? {
        for (tableEntry in tableMap.map().values()) {
            if (tableEntry.sqls.contains(sql)) {
                return tableEntry
            }
        }
        return null
    }

    /** Returns a table with the given name. Does not look for views.  */
    @Nullable
    fun getTable(tableName: String?, caseSensitive: Boolean): TableEntry {
        // Check explicit tables.
        for (entry in tableMap.range(tableName, caseSensitive).entrySet()) {
            return entry.getValue()
        }
        return getImplicitTable(tableName, caseSensitive)
    }

    fun plus(): SchemaPlus {
        return SchemaPlusImpl()
    }

    /** Returns the default path resolving functions from this schema.
     *
     *
     * The path consists is a list of lists of strings.
     * Each list of strings represents the path of a schema from the root schema.
     * For example, [[], [foo], [foo, bar, baz]] represents three schemas: the
     * root schema "/" (level 0), "/foo" (level 1) and "/foo/bar/baz" (level 3).
     *
     * @return Path of this schema; never null, may be empty
     */
    fun getPath(): List<List<String?>?> {
        return if (path != null) {
            path
        } else ImmutableList.of(path(null))
        // Return a path consisting of just this schema.
    }

    /** Returns a collection of sub-schemas, both explicit (defined using
     * [.add]) and implicit
     * (defined using [org.apache.calcite.schema.Schema.getSubSchemaNames]
     * and [Schema.getSubSchema]).  */
    fun getSubSchemaMap(): NavigableMap<String, CalciteSchema> {
        // Build a map of implicit sub-schemas first, then explicit sub-schemas.
        // If there are implicit and explicit with the same name, explicit wins.
        val builder: ImmutableSortedMap.Builder<String?, CalciteSchema?> = Builder(NameSet.COMPARATOR)
        builder.putAll(subSchemaMap.map())
        addImplicitSubSchemaToBuilder(builder)
        return builder.build()
    }

    /** Returns a collection of lattices.
     *
     *
     * All are explicit (defined using [.add]).  */
    fun getLatticeMap(): NavigableMap<String, LatticeEntry> {
        return ImmutableSortedMap.copyOf(latticeMap.map())
    }// Add explicit tables, case-sensitive.
    // Add implicit tables, case-sensitive.
    /** Returns the set of all table names. Includes implicit and explicit tables
     * and functions with zero parameters.  */
    val tableNames: NavigableSet<String>
        get() {
            val builder: ImmutableSortedSet.Builder<String?> = Builder(NameSet.COMPARATOR)
            // Add explicit tables, case-sensitive.
            builder.addAll(tableMap.map().keySet())
            // Add implicit tables, case-sensitive.
            addImplicitTableToBuilder(builder)
            return builder.build()
        }// Add explicit types.
    // Add implicit types.
    /** Returns the set of all types names.  */
    val typeNames: NavigableSet<String>
        get() {
            val builder: ImmutableSortedSet.Builder<String?> = Builder(NameSet.COMPARATOR)
            // Add explicit types.
            builder.addAll(typeMap.map().keySet())
            // Add implicit types.
            addImplicitTypeNamesToBuilder(builder)
            return builder.build()
        }

    /** Returns a type, explicit and implicit, with a given
     * name. Never null.  */
    @Nullable
    fun getType(name: String?, caseSensitive: Boolean): TypeEntry {
        for (entry in typeMap.range(name, caseSensitive).entrySet()) {
            return entry.getValue()
        }
        return getImplicitType(name, caseSensitive)
    }

    /** Returns a collection of all functions, explicit and implicit, with a given
     * name. Never null.  */
    fun getFunctions(name: String?, caseSensitive: Boolean): Collection<Function> {
        val builder: ImmutableList.Builder<Function?> = ImmutableList.builder()
        // Add explicit functions.
        for (functionEntry in Pair.right(functionMap.range(name, caseSensitive))) {
            builder.add(functionEntry.function)
        }
        // Add implicit functions.
        addImplicitFunctionsToBuilder(builder, name, caseSensitive)
        return builder.build()
    }

    /** Returns the list of function names in this schema, both implicit and
     * explicit, never null.  */
    fun getFunctionNames(): NavigableSet<String> {
        val builder: ImmutableSortedSet.Builder<String?> = Builder(NameSet.COMPARATOR)
        // Add explicit functions, case-sensitive.
        builder.addAll(functionMap.map().keySet())
        // Add implicit functions, case-sensitive.
        addImplicitFuncNamesToBuilder(builder)
        return builder.build()
    }// add tables derived from implicit functions

    /** Returns tables derived from explicit and implicit functions
     * that take zero parameters.  */
    val tablesBasedOnNullaryFunctions: NavigableMap<String, Table>
        get() {
            val builder: ImmutableSortedMap.Builder<String?, Table?> = Builder(NameSet.COMPARATOR)
            for (entry in nullaryFunctionMap.map().entrySet()) {
                val function: Function = entry.getValue().getFunction()
                if (function is TableMacro) {
                    assert(function.getParameters().isEmpty())
                    val table: Table = (function as TableMacro).apply(ImmutableList.of())
                    builder.put(entry.getKey(), table)
                }
            }
            // add tables derived from implicit functions
            addImplicitTablesBasedOnNullaryFunctionsToBuilder(builder)
            return builder.build()
        }

    /** Returns a tables derived from explicit and implicit functions
     * that take zero parameters.  */
    @Nullable
    fun getTableBasedOnNullaryFunction(
        tableName: String?,
        caseSensitive: Boolean
    ): TableEntry {
        for (entry in nullaryFunctionMap.range(tableName, caseSensitive).entrySet()) {
            val function: Function = entry.getValue().getFunction()
            if (function is TableMacro) {
                assert(function.getParameters().isEmpty())
                val table: Table = (function as TableMacro).apply(ImmutableList.of())
                return tableEntry(tableName, table)
            }
        }
        return getImplicitTableBasedOnNullaryFunction(tableName, caseSensitive)
    }

    /** Creates a snapshot of this CalciteSchema as of the specified time. All
     * explicit objects in this CalciteSchema will be copied into the snapshot
     * CalciteSchema, while the contents of the snapshot of the underlying schema
     * should not change as specified in [Schema.snapshot].
     * Snapshots of explicit sub schemas will be created and copied recursively.
     *
     *
     * Currently, to accommodate the requirement of creating tables on the fly
     * for materializations, the snapshot will still use the same table map and
     * lattice map as in the original CalciteSchema instead of making copies.
     *
     * @param version The current schema version
     *
     * @return the schema snapshot.
     */
    fun createSnapshot(version: SchemaVersion?): CalciteSchema {
        Preconditions.checkArgument(isRoot, "must be root schema")
        return snapshot(null, version)
    }

    @Experimental
    fun removeSubSchema(name: String?): Boolean {
        return subSchemaMap.remove(name) != null
    }

    @Experimental
    fun removeTable(name: String?): Boolean {
        return tableMap.remove(name) != null
    }

    @Experimental
    fun removeFunction(name: String?): Boolean {
        val remove: FunctionEntry = nullaryFunctionMap.remove(name) ?: return false
        functionMap.remove(name, remove)
        return true
    }

    @Experimental
    fun removeType(name: String?): Boolean {
        return typeMap.remove(name) != null
    }

    /**
     * Entry in a schema, such as a table or sub-schema.
     *
     *
     * Each object's name is a property of its membership in a schema;
     * therefore in principle it could belong to several schemas, or
     * even the same schema several times, with different names. In this
     * respect, it is like an inode in a Unix file system.
     *
     *
     * The members of a schema must have unique names.
     */
    abstract class Entry protected constructor(schema: CalciteSchema?, name: String?) {
        val schema: CalciteSchema
        val name: String

        init {
            this.schema = Objects.requireNonNull(schema, "schema")
            this.name = Objects.requireNonNull(name, "name")
        }

        /** Returns this object's path. For example ["hr", "emps"].  */
        fun path(): List<String> {
            return schema.path(name)
        }
    }

    /** Membership of a table in a schema.  */
    abstract class TableEntry protected constructor(
        schema: CalciteSchema?, name: String?,
        sqls: ImmutableList<String?>?
    ) : Entry(schema, name) {
        val sqls: ImmutableList<String>

        init {
            this.sqls = Objects.requireNonNull(sqls, "sqls")
        }

        abstract val table: Table?
    }

    /** Membership of a type in a schema.  */
    abstract class TypeEntry protected constructor(schema: CalciteSchema?, name: String?) : Entry(schema, name) {
        abstract val type: RelProtoDataType?
    }

    /** Membership of a function in a schema.  */
    abstract class FunctionEntry protected constructor(schema: CalciteSchema?, name: String?) : Entry(schema, name) {
        abstract val function: Function?

        /** Whether this represents a materialized view. (At a given point in time,
         * it may or may not be materialized as a table.)  */
        abstract val isMaterialization: Boolean
    }

    /** Membership of a lattice in a schema.  */
    abstract class LatticeEntry protected constructor(schema: CalciteSchema?, name: String?) : Entry(schema, name) {
        abstract val lattice: Lattice
        abstract val starTable: TableEntry?
    }

    /** Implementation of [SchemaPlus] based on a
     * [org.apache.calcite.jdbc.CalciteSchema].  */
    private inner class SchemaPlusImpl : SchemaPlus {
        fun calciteSchema(): CalciteSchema {
            return this@CalciteSchema
        }

        @get:Nullable
        @get:Override
        val parentSchema: SchemaPlus?
            get() = if (parent == null) null else parent.plus()

        @Override
        fun getName(): String {
            return name
        }

        @get:Override
        val isMutable: Boolean
            get() = schema.isMutable()

        @get:Override
        @set:Override
        var isCacheEnabled: Boolean
            get() = this@CalciteSchema.isCacheEnabled
            set(cache) {
                setCache(cache)
            }

        @Override
        fun snapshot(version: SchemaVersion?): Schema {
            throw UnsupportedOperationException()
        }

        @Override
        fun getExpression(@Nullable parentSchema: SchemaPlus?, name: String?): Expression {
            return schema.getExpression(parentSchema, name)
        }

        @Override
        @Nullable
        fun getTable(name: String?): Table? {
            val entry = this@CalciteSchema.getTable(name, true)
            return if (entry == null) null else entry.table
        }

        @get:Override
        val tableNames: NavigableSet<String>
            get() = this@CalciteSchema.tableNames

        @Override
        @Nullable
        fun getType(name: String?): RelProtoDataType? {
            val entry = this@CalciteSchema.getType(name, true)
            return if (entry == null) null else entry.type
        }

        @get:Override
        val typeNames: Set<String>
            get() = this@CalciteSchema.typeNames

        @Override
        fun getFunctions(name: String?): Collection<Function> {
            return this@CalciteSchema.getFunctions(name, true)
        }

        @Override
        fun getFunctionNames(): NavigableSet<String> {
            return this@CalciteSchema.getFunctionNames()
        }

        @Override
        @Nullable
        fun getSubSchema(name: String?): SchemaPlus? {
            val subSchema = this@CalciteSchema.getSubSchema(name, true)
            return if (subSchema == null) null else subSchema.plus()
        }

        @get:Override
        val subSchemaNames: Set<String>
            get() = getSubSchemaMap().keySet()

        @Override
        fun add(name: String?, schema: Schema?): SchemaPlus {
            val calciteSchema: CalciteSchema = this@CalciteSchema.add(name, schema)
            return calciteSchema.plus()
        }

        @Override
        fun <T : Object?> unwrap(clazz: Class<T>): T {
            if (clazz.isInstance(this)) {
                return clazz.cast(this)
            }
            if (clazz.isInstance(this@CalciteSchema)) {
                return clazz.cast(this@CalciteSchema)
            }
            if (clazz.isInstance(schema)) {
                return clazz.cast(schema)
            }
            if (clazz === DataSource::class.java) {
                if (schema is JdbcSchema) {
                    return clazz.cast((schema as JdbcSchema?).getDataSource())
                }
                if (schema is JdbcCatalogSchema) {
                    return clazz.cast((schema as JdbcCatalogSchema?).getDataSource())
                }
            }
            throw ClassCastException("not a $clazz")
        }

        @Override
        fun setPath(path: ImmutableList<ImmutableList<String?>?>?) {
            this@CalciteSchema.path = path
        }

        @Override
        fun add(name: String?, table: Table?) {
            this@CalciteSchema.add(name, table)
        }

        @Override
        fun add(name: String?, function: Function?) {
            this@CalciteSchema.add(name, function)
        }

        @Override
        fun add(name: String?, type: RelProtoDataType?) {
            this@CalciteSchema.add(name, type)
        }

        @Override
        fun add(name: String?, lattice: Lattice?) {
            this@CalciteSchema.add(name, lattice)
        }
    }

    /**
     * Implementation of [CalciteSchema.TableEntry]
     * where all properties are held in fields.
     */
    class TableEntryImpl(
        schema: CalciteSchema?, name: String?, table: Table?,
        sqls: ImmutableList<String?>?
    ) : TableEntry(schema, name, sqls) {
        private override val table: Table

        /** Creates a TableEntryImpl.  */
        init {
            this.table = Objects.requireNonNull(table, "table")
        }

        @Override
        override fun getTable(): Table {
            return table
        }
    }

    /**
     * Implementation of [TypeEntry]
     * where all properties are held in fields.
     */
    class TypeEntryImpl(schema: CalciteSchema?, name: String?, protoDataType: RelProtoDataType?) :
        TypeEntry(schema, name) {
        private val protoDataType: RelProtoDataType?

        /** Creates a TypeEntryImpl.  */
        init {
            this.protoDataType = protoDataType
        }

        @get:Override
        override val type: RelProtoDataType?
            get() = protoDataType
    }

    /**
     * Implementation of [FunctionEntry]
     * where all properties are held in fields.
     */
    class FunctionEntryImpl(
        schema: CalciteSchema?, name: String?,
        function: Function
    ) : FunctionEntry(schema, name) {
        private override val function: Function

        /** Creates a FunctionEntryImpl.  */
        init {
            this.function = function
        }

        @Override
        override fun getFunction(): Function {
            return function
        }

        @get:Override
        override val isMaterialization: Boolean
            get() = function is MaterializedViewTable.MaterializedViewTableMacro
    }

    /**
     * Implementation of [LatticeEntry]
     * where all properties are held in fields.
     */
    class LatticeEntryImpl(
        schema: CalciteSchema, name: String?,
        lattice: Lattice
    ) : LatticeEntry(schema, name) {
        private override val lattice: Lattice

        @get:Override
        override val starTable: TableEntry

        /** Creates a LatticeEntryImpl.  */
        init {
            this.lattice = lattice

            // Star table has same name as lattice and is in same schema.
            val starTable: StarTable = lattice.createStarTable()
            this.starTable = schema.add(name, starTable)
        }

        @Override
        override fun getLattice(): Lattice {
            return lattice
        }
    }

    companion object {
        fun from(plus: SchemaPlus): CalciteSchema {
            return (plus as SchemaPlusImpl).calciteSchema()
        }

        /** Returns a subset of a map whose keys match the given string
         * case-insensitively.
         */
        @Deprecated // to be removed before 2.0
        @Deprecated("use NameMap")
        protected fun <V> find(
            map: NavigableMap<String?, V>?,
            s: String?
        ): NavigableMap<String, V> {
            return NameMap.immutableCopyOf(map).range(s, false)
        }

        /** Returns a subset of a set whose values match the given string
         * case-insensitively.
         */
        @Deprecated // to be removed before 2.0
        @Deprecated("use NameSet")
        protected fun find(set: NavigableSet<String?>?, name: String?): Iterable<String> {
            return NameSet.immutableCopyOf(set).range(name, false)
        }
        /** Creates a root schema.
         *
         * @param addMetadataSchema Whether to add a "metadata" schema containing
         * definitions of tables, columns etc.
         * @param cache If true create [CachingCalciteSchema];
         * if false create [SimpleCalciteSchema]
         * @param name Schema name
         */
        /** Creates a root schema.
         *
         * @param addMetadataSchema Whether to add a "metadata" schema containing
         * definitions of tables, columns etc.
         * @param cache If true create [CachingCalciteSchema];
         * if false create [SimpleCalciteSchema]
         */
        /** Creates a root schema.
         *
         *
         * When `addMetadataSchema` argument is true adds a "metadata"
         * schema containing definitions of tables, columns etc. to root schema.
         * By default, creates a [CachingCalciteSchema].
         */
        @JvmOverloads
        fun createRootSchema(
            addMetadataSchema: Boolean,
            cache: Boolean = true, name: String = ""
        ): CalciteSchema {
            val rootSchema: Schema = RootSchema()
            return createRootSchema(addMetadataSchema, cache, name, rootSchema)
        }

        @Experimental
        fun createRootSchema(
            addMetadataSchema: Boolean,
            cache: Boolean, name: String, schema: Schema?
        ): CalciteSchema {
            val rootSchema: CalciteSchema
            if (cache) {
                rootSchema = CachingCalciteSchema(null, schema, name)
            } else {
                rootSchema = SimpleCalciteSchema(null, schema, name)
            }
            if (addMetadataSchema) {
                rootSchema.add("metadata", MetadataSchema.INSTANCE)
            }
            return rootSchema
        }
    }
}
