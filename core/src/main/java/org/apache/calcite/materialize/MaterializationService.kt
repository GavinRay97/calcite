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
package org.apache.calcite.materialize

import org.apache.calcite.DataContext

/**
 * Manages the collection of materialized tables known to the system,
 * and the process by which they become valid and invalid.
 */
class MaterializationService private constructor() {
    private val actor: MaterializationActor = MaterializationActor()
    private val tableFactory = DefaultTableFactory()

    /** Defines a new materialization. Returns its key.  */
    @Nullable
    fun defineMaterialization(
        schema: CalciteSchema,
        @Nullable tileKey: TileKey?, viewSql: String?, @Nullable viewSchemaPath: List<String?>?,
        @Nullable suggestedTableName: String?, create: Boolean, existing: Boolean
    ): MaterializationKey? {
        return defineMaterialization(
            schema, tileKey, viewSql, viewSchemaPath,
            suggestedTableName, tableFactory, create, existing
        )
    }

    /** Defines a new materialization. Returns its key.  */
    @Nullable
    fun defineMaterialization(
        schema: CalciteSchema,
        @Nullable tileKey: TileKey?, viewSql: String?, @Nullable viewSchemaPath: List<String?>?,
        @Nullable suggestedTableName: String?, tableFactory: TableFactory, create: Boolean,
        existing: Boolean
    ): MaterializationKey? {
        val queryKey: MaterializationActor.QueryKey = QueryKey(viewSql, schema, viewSchemaPath)
        val existingKey: MaterializationKey = actor.keyBySql.get(queryKey)
        if (existingKey != null) {
            return existingKey
        }
        if (!create) {
            return null
        }
        val connection: CalciteConnection = CalciteMetaImpl.connect(schema.root(), null)
        var tableEntry: CalciteSchema.TableEntry?
        // If the user says the materialization exists, first try to find a table
        // with the name and if none can be found, lookup a view in the schema
        if (existing) {
            requireNonNull(suggestedTableName, "suggestedTableName")
            tableEntry = schema.getTable(suggestedTableName, true)
            if (tableEntry == null) {
                tableEntry = schema.getTableBasedOnNullaryFunction(suggestedTableName, true)
            }
        } else {
            tableEntry = null
        }
        if (tableEntry == null) {
            tableEntry = schema.getTableBySql(viewSql)
        }
        var rowType: RelDataType? = null
        if (tableEntry == null) {
            val table: Table = tableFactory.createTable(schema, viewSql, viewSchemaPath)
            val tableName: String = Schemas.uniqueTableName(
                schema,
                Util.first(suggestedTableName, "m")
            )
            tableEntry = schema.add(tableName, table, ImmutableList.of(viewSql))
            Hook.CREATE_MATERIALIZATION.run(tableName)
            rowType = table.getRowType(connection.getTypeFactory())
        }
        if (rowType == null) {
            // If we didn't validate the SQL by populating a table, validate it now.
            val parse: ParseResult = Schemas.parse(connection, schema, viewSchemaPath, viewSql)
            rowType = parse.rowType
        }
        val key = MaterializationKey()
        val materialization: MaterializationActor.Materialization = Materialization(
            key, schema.root(),
            tableEntry, viewSql, rowType, viewSchemaPath
        )
        actor.keyMap.put(materialization.key, materialization)
        actor.keyBySql.put(queryKey, materialization.key)
        if (tileKey != null) {
            actor.keyByTile.put(tileKey, materialization.key)
        }
        return key
    }

    /** Checks whether a materialization is valid, and if so, returns the table
     * where the data are stored.  */
    fun checkValid(key: MaterializationKey?): @Nullable CalciteSchema.TableEntry? {
        val materialization: MaterializationActor.Materialization = actor.keyMap.get(key)
        return if (materialization != null) {
            materialization.materializedTable
        } else null
    }

    /**
     * Defines a tile.
     *
     *
     * Setting the `create` flag to false prevents a materialization
     * from being created if one does not exist. Critically, it is set to false
     * during the recursive SQL that populates a materialization. Otherwise a
     * materialization would try to create itself to populate itself!
     */
    @Nullable
    fun defineTile(
        lattice: Lattice,
        groupSet: ImmutableBitSet, measureList: List<Lattice.Measure?>,
        schema: CalciteSchema, create: Boolean, exact: Boolean
    ): Pair<CalciteSchema.TableEntry, TileKey> {
        return defineTile(
            lattice, groupSet, measureList, schema, create, exact,
            "m$groupSet", tableFactory
        )
    }

    @Nullable
    fun defineTile(
        lattice: Lattice,
        groupSet: ImmutableBitSet?, measureList: List<Lattice.Measure?>,
        schema: CalciteSchema, create: Boolean, exact: Boolean,
        suggestedTableName: String?, tableFactory: TableFactory
    ): Pair<CalciteSchema.TableEntry, TileKey>? {
        var materializationKey: MaterializationKey?
        val tileKey = TileKey(lattice, groupSet, ImmutableList.copyOf(measureList))

        // Step 1. Look for an exact match for the tile.
        materializationKey = actor.keyByTile.get(tileKey)
        if (materializationKey != null) {
            val tableEntry: CalciteSchema.TableEntry? = checkValid(materializationKey)
            if (tableEntry != null) {
                return Pair.of(tableEntry, tileKey)
            }
        }

        // Step 2. Look for a match of the tile with the same dimensionality and an
        // acceptable list of measures.
        val tileKey0 = TileKey(lattice, groupSet, ImmutableList.of())
        for (tileKey1 in actor.tilesByDimensionality.get(tileKey0)) {
            assert(tileKey1.dimensions.equals(groupSet))
            if (allSatisfiable(measureList, tileKey1)) {
                materializationKey = actor.keyByTile.get(tileKey1)
                if (materializationKey != null) {
                    val tableEntry: CalciteSchema.TableEntry? = checkValid(materializationKey)
                    if (tableEntry != null) {
                        return Pair.of(tableEntry, tileKey1)
                    }
                }
            }
        }

        // Step 3. There's nothing at the exact dimensionality. Look for a roll-up
        // from tiles that have a super-set of dimensions and all the measures we
        // need.
        //
        // If there are several roll-ups, choose the one with the fewest rows.
        //
        // TODO: Allow/deny roll-up based on a size factor. If the source is only
        // say 2x larger than the target, don't materialize, but if it is 3x, do.
        //
        // TODO: Use a partially-ordered set data structure, so we are not scanning
        // through all tiles.
        if (!exact) {
            val queue: PriorityQueue<Pair<CalciteSchema.TableEntry, TileKey>> = PriorityQueue(1, C)
            for (entry in actor.keyByTile.entrySet()) {
                val tileKey2: TileKey = entry.getKey()
                if (tileKey2.lattice === lattice && tileKey2.dimensions.contains(groupSet)
                    && !tileKey2.dimensions.equals(groupSet)
                    && allSatisfiable(measureList, tileKey2)
                ) {
                    materializationKey = entry.getValue()
                    val tableEntry: CalciteSchema.TableEntry? = checkValid(materializationKey)
                    if (tableEntry != null) {
                        queue.add(Pair.of(tableEntry, tileKey2))
                    }
                }
            }
            if (!queue.isEmpty()) {
                return queue.peek()
            }
        }

        // What we need is not there. If we can't create, we're done.
        if (!create) {
            return null
        }

        // Step 4. Create the tile we need.
        //
        // If there were any tiles at this dimensionality, regardless of
        // whether they were current, create a wider tile that contains their
        // measures plus the currently requested measures. Then we can obsolete all
        // other tiles.
        val obsolete: List<TileKey> = ArrayList()
        val measureSet: Set<Lattice.Measure> = LinkedHashSet()
        for (tileKey1 in actor.tilesByDimensionality.get(tileKey0)) {
            measureSet.addAll(tileKey1.measures)
            obsolete.add(tileKey1)
        }
        measureSet.addAll(measureList)
        val newTileKey = TileKey(lattice, groupSet, ImmutableList.copyOf(measureSet))
        val sql: String = lattice.sql(groupSet, newTileKey.measures)
        materializationKey = defineMaterialization(
            schema, newTileKey, sql, schema.path(null),
            suggestedTableName, tableFactory, true, false
        )
        if (materializationKey != null) {
            val tableEntry: CalciteSchema.TableEntry? = checkValid(materializationKey)
            if (tableEntry != null) {
                // Obsolete all of the narrower tiles.
                for (tileKey1 in obsolete) {
                    actor.tilesByDimensionality.remove(tileKey0, tileKey1)
                    actor.keyByTile.remove(tileKey1)
                }
                actor.tilesByDimensionality.put(tileKey0, newTileKey)
                actor.keyByTile.put(newTileKey, materializationKey)
                return Pair.of(tableEntry, newTileKey)
            }
        }
        return null
    }

    /** Gathers a list of all materialized tables known within a given root
     * schema. (Each root schema defines a disconnected namespace, with no overlap
     * with the current schema. Especially in a test run, the contents of two
     * root schemas may look similar.)  */
    fun query(rootSchema: CalciteSchema): List<Prepare.Materialization> {
        val list: List<Prepare.Materialization> = ArrayList()
        for (materialization in actor.keyMap.values()) {
            if (materialization.rootSchema.schema === rootSchema.schema
                && materialization.materializedTable != null
            ) {
                list.add(
                    Materialization(materialization.materializedTable,
                        materialization.sql,
                        requireNonNull(
                            materialization.viewSchemaPath
                        ) {
                            ("materialization.viewSchemaPath is null for "
                                    + materialization.materializedTable)
                        })
                )
            }
        }
        return list
    }

    /** De-registers all materialized tables in the system.  */
    fun clear() {
        actor.keyMap.clear()
    }

    fun removeMaterialization(key: MaterializationKey?) {
        actor.keyMap.remove(key)
    }

    /**
     * Creates tables that represent a materialized view.
     */
    interface TableFactory {
        fun createTable(
            schema: CalciteSchema?, viewSql: String?,
            @Nullable viewSchemaPath: List<String?>?
        ): Table
    }

    /**
     * Default implementation of [TableFactory].
     * Creates a table using [CloneSchema].
     */
    class DefaultTableFactory : TableFactory {
        @Override
        override fun createTable(
            schema: CalciteSchema, viewSql: String?,
            @Nullable viewSchemaPath: List<String?>?
        ): Table {
            val connection: CalciteConnection = CalciteMetaImpl.connect(schema.root(), null)
            val map: ImmutableMap<CalciteConnectionProperty, String> = ImmutableMap.of(
                CalciteConnectionProperty.CREATE_MATERIALIZATIONS,
                "false"
            )
            val calciteSignature: CalcitePrepare.CalciteSignature<Object> =
                Schemas.prepare(connection, schema, viewSchemaPath, viewSql, map)
            return CloneSchema.createCloneTable(connection.getTypeFactory(),
                RelDataTypeImpl.proto(castNonNull(calciteSignature.rowType)),
                calciteSignature.getCollationList(),
                Util.transform(calciteSignature.columns) { column -> column.type.rep },
                object : AbstractQueryable<Object?>() {
                    @Override
                    fun enumerator(): Enumerator<Object> {
                        val dataContext: DataContext = DataContexts.of(
                            connection,
                            requireNonNull(calciteSignature.rootSchema, "rootSchema")
                                .plus()
                        )
                        return calciteSignature.enumerable(dataContext).enumerator()
                    }

                    @get:Override
                    val elementType: Type
                        get() = Object::class.java

                    @get:Override
                    val expression: Expression
                        get() {
                            throw UnsupportedOperationException()
                        }

                    @get:Override
                    val provider: QueryProvider
                        get() = connection

                    @Override
                    operator fun iterator(): Iterator<Object> {
                        val dataContext: DataContext = DataContexts.of(
                            connection,
                            requireNonNull(calciteSignature.rootSchema, "rootSchema")
                                .plus()
                        )
                        return calciteSignature.enumerable(dataContext).iterator()
                    }
                })
        }
    }

    companion object {
        private val INSTANCE = MaterializationService()

        /** For testing.  */
        private val THREAD_INSTANCE: ThreadLocal<MaterializationService> =
            ThreadLocal.withInitial { MaterializationService() }
        private val C: Comparator<Pair<CalciteSchema.TableEntry, TileKey>> =
            label@ Comparator<Pair<CalciteSchema.TableEntry, TileKey>> { o0, o1 ->
                // We prefer rolling up from the table with the fewest rows.
                val t0: Table = o0.left.getTable()
                val t1: Table = o1.left.getTable()
                val rowCount0: Double = t0.getStatistic().getRowCount()
                val rowCount1: Double = t1.getStatistic().getRowCount()
                if (rowCount0 != null && rowCount1 != null) {
                    val c: Int = Double.compare(rowCount0, rowCount1)
                    if (c != 0) {
                        return@label c
                    }
                } else if (rowCount0 == null) {
                    // Unknown is worse than known
                    return@label 1
                } else {
                    // rowCount1 == null => Unknown is worse than known
                    return@label -1
                }
                o0.left.name.compareTo(o1.left.name)
            }

        private fun allSatisfiable(
            measureList: List<Lattice.Measure?>,
            tileKey: TileKey
        ): Boolean {
            // A measure can be satisfied if it is contained in the measure list, or,
            // less obviously, if it is composed of grouping columns.
            for (measure in measureList) {
                if (!(tileKey.measures.contains(measure)
                            || tileKey.dimensions.contains(measure.argBitSet()))
                ) {
                    return false
                }
            }
            return true
        }

        /** Used by tests, to ensure that they see their own service.  */
        fun setThreadLocal() {
            THREAD_INSTANCE.set(MaterializationService())
        }

        /** Returns the instance of the materialization service. Usually the global
         * one, but returns a thread-local one during testing (when
         * [.setThreadLocal] has been called by the current thread).  */
        fun instance(): MaterializationService {
            val materializationService: MaterializationService = THREAD_INSTANCE.get()
            return materializationService ?: INSTANCE
        }
    }
}
