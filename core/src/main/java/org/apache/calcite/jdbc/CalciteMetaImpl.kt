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

import org.apache.calcite.DataContext

/**
 * Helper for implementing the `getXxx` methods such as
 * [org.apache.calcite.avatica.AvaticaDatabaseMetaData.getTables].
 */
class CalciteMetaImpl(connection: CalciteConnectionImpl?) : MetaImpl(connection) {
    init {
        this.connProps
            .setAutoCommit(false)
            .setReadOnly(false)
            .setTransactionIsolation(Connection.TRANSACTION_NONE)
        this.connProps.setDirty(false)
    }

    @Override
    fun createStatement(ch: ConnectionHandle?): StatementHandle {
        val h: StatementHandle = super.createStatement(ch)
        val calciteConnection: CalciteConnectionImpl = connection
        calciteConnection.server.addStatement(calciteConnection, h)
        return h
    }

    @Override
    fun closeStatement(h: StatementHandle?) {
        val calciteConnection: CalciteConnectionImpl = connection
        @SuppressWarnings("unused") val stmt: CalciteServerStatement
        stmt = try {
            calciteConnection.server.getStatement(h)
        } catch (e: NoSuchStatementException) {
            // statement is not valid; nothing to do
            return
        }
        // stmt.close(); // TODO: implement
        calciteConnection.server.removeStatement(h)
    }

    private fun <E> createResultSet(
        enumerable: Enumerable<E>,
        clazz: Class, vararg names: String
    ): MetaResultSet {
        requireNonNull(names, "names")
        val columns: List<ColumnMetaData> = ArrayList(names.size)
        val fields: List<Field> = ArrayList(names.size)
        val fieldNames: List<String> = ArrayList(names.size)
        for (name in names) {
            val index: Int = fields.size()
            val fieldName: String = AvaticaUtils.toCamelCase(name)
            val field: Field
            field = try {
                clazz.getField(fieldName)
            } catch (e: NoSuchFieldException) {
                throw RuntimeException(e)
            }
            columns.add(columnMetaData(name, index, field.getType(), false))
            fields.add(field)
            fieldNames.add(fieldName)
        }
        return createResultSet(
            Collections.emptyMap(),
            columns, CursorFactory.record(clazz, fields, fieldNames),
            Frame(0, true, enumerable)
        )
    }

    @Override
    protected fun createResultSet(
        internalParameters: Map<String?, Object?>?, columns: List<ColumnMetaData?>?,
        cursorFactory: CursorFactory?, firstFrame: Frame
    ): MetaResultSet {
        return try {
            val connection: CalciteConnectionImpl = connection
            val statement: AvaticaStatement = connection.createStatement()
            val signature: CalcitePrepare.CalciteSignature<Object> = object : CalciteSignature<Object?>(
                "",
                ImmutableList.of(), internalParameters, null,
                columns, cursorFactory, null, ImmutableList.of(), -1,
                null, Meta.StatementType.SELECT
            ) {
                @Override
                fun enumerable(
                    dataContext: DataContext?
                ): Enumerable<Object> {
                    return Linq4j.asEnumerable(firstFrame.rows)
                }
            }
            MetaResultSet.create(
                connection.id, statement.getId(), true,
                signature, firstFrame
            )
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }
    }

    val connection: org.apache.calcite.jdbc.CalciteConnectionImpl
        get() = field

    @Override
    fun getDatabaseProperties(ch: ConnectionHandle?): Map<DatabaseProperty, Object> {
        val builder: ImmutableMap.Builder<DatabaseProperty, Object> = ImmutableMap.builder()
        for (p in DatabaseProperty.values()) {
            addProperty(builder, p)
        }
        return builder.build()
    }

    @Override
    fun getTables(
        ch: ConnectionHandle?,
        catalog: String?,
        schemaPattern: Pat,
        tableNamePattern: Pat,
        typeList: List<String?>?
    ): MetaResultSet {
        val typeFilter: Predicate1<MetaTable>
        if (typeList == null) {
            typeFilter = Functions.truePredicate1()
        } else {
            typeFilter = Predicate1<MetaTable> { v1 -> typeList.contains(v1.tableType) }
        }
        val schemaMatcher: Predicate1<MetaSchema> = namedMatcher<Named>(schemaPattern)
        return createResultSet(schemas(catalog)
            .where(schemaMatcher)
            .selectMany { schema -> tables(schema, matcher(tableNamePattern)) }
            .where(typeFilter),
            MetaTable::class.java,
            "TABLE_CAT",
            "TABLE_SCHEM",
            "TABLE_NAME",
            "TABLE_TYPE",
            "REMARKS",
            "TYPE_CAT",
            "TYPE_SCHEM",
            "TYPE_NAME",
            "SELF_REFERENCING_COL_NAME",
            "REF_GENERATION")
    }

    @Override
    fun getTypeInfo(ch: ConnectionHandle?): MetaResultSet {
        return createResultSet<Any>(
            allTypeInfo(),
            MetaTypeInfo::class.java,
            "TYPE_NAME",
            "DATA_TYPE",
            "PRECISION",
            "LITERAL_PREFIX",
            "LITERAL_SUFFIX",
            "CREATE_PARAMS",
            "NULLABLE",
            "CASE_SENSITIVE",
            "SEARCHABLE",
            "UNSIGNED_ATTRIBUTE",
            "FIXED_PREC_SCALE",
            "AUTO_INCREMENT",
            "LOCAL_TYPE_NAME",
            "MINIMUM_SCALE",
            "MAXIMUM_SCALE",
            "SQL_DATA_TYPE",
            "SQL_DATETIME_SUB",
            "NUM_PREC_RADIX"
        )
    }

    @Override
    fun getColumns(
        ch: ConnectionHandle?,
        catalog: String?,
        schemaPattern: Pat,
        tableNamePattern: Pat,
        columnNamePattern: Pat
    ): MetaResultSet {
        val tableNameMatcher: Predicate1<String?> = matcher(tableNamePattern)
        val schemaMatcher: Predicate1<MetaSchema> = namedMatcher<Named>(schemaPattern)
        val columnMatcher: Predicate1<MetaColumn> = namedMatcher<Named>(columnNamePattern)
        return createResultSet(schemas(catalog)
            .where(schemaMatcher)
            .selectMany { schema -> tables(schema, tableNameMatcher) }
            .selectMany { table_: MetaTable -> columns(table_) }
            .where(columnMatcher),
            MetaColumn::class.java,
            "TABLE_CAT",
            "TABLE_SCHEM",
            "TABLE_NAME",
            "COLUMN_NAME",
            "DATA_TYPE",
            "TYPE_NAME",
            "COLUMN_SIZE",
            "BUFFER_LENGTH",
            "DECIMAL_DIGITS",
            "NUM_PREC_RADIX",
            "NULLABLE",
            "REMARKS",
            "COLUMN_DEF",
            "SQL_DATA_TYPE",
            "SQL_DATETIME_SUB",
            "CHAR_OCTET_LENGTH",
            "ORDINAL_POSITION",
            "IS_NULLABLE",
            "SCOPE_CATALOG",
            "SCOPE_SCHEMA",
            "SCOPE_TABLE",
            "SOURCE_DATA_TYPE",
            "IS_AUTOINCREMENT",
            "IS_GENERATEDCOLUMN")
    }

    fun catalogs(): Enumerable<MetaCatalog> {
        val catalog: String
        catalog = try {
            connection.getCatalog()
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }
        return Linq4j.asEnumerable(
            ImmutableList.of(MetaCatalog(catalog))
        )
    }

    fun tableTypes(): Enumerable<MetaTableType> {
        return Linq4j.asEnumerable(
            ImmutableList.of(
                MetaTableType("TABLE"), MetaTableType("VIEW")
            )
        )
    }

    fun schemas(catalog: String?): Enumerable<MetaSchema> {
        return Linq4j.asEnumerable(
            connection.rootSchema.getSubSchemaMap().values()
        )
            .select(Function1<CalciteSchema, MetaSchema> { calciteSchema ->
                CalciteMetaSchema(
                    calciteSchema, catalog,
                    calciteSchema.getName()
                )
            } as Function1<CalciteSchema?, MetaSchema?>?)
            .orderBy(Function1<MetaSchema, Comparable> { metaSchema ->
                FlatLists.of(
                    Util.first(metaSchema.tableCatalog, ""),
                    metaSchema.tableSchem
                )
            } as Function1<MetaSchema?, Comparable?>?)
    }

    fun tables(catalog: String?): Enumerable<MetaTable> {
        return schemas(catalog)
            .selectMany { schema -> tables(schema, Functions.< String > truePredicate1 < String ? > ()) }
    }

    fun tables(schema_: MetaSchema): Enumerable<MetaTable> {
        val schema = schema_ as CalciteMetaSchema
        return Linq4j.asEnumerable(schema.calciteSchema.getTableNames())
            .select(Function1<String, MetaTable> { name ->
                val table: Table = requireNonNull(
                    schema.calciteSchema.getTable(name, true)
                ) { "table $name is not found (case sensitive)" }.getTable()
                CalciteMetaTable(
                    table,
                    schema.tableCatalog,
                    schema.tableSchem,
                    name
                )
            } as Function1<String?, MetaTable?>?)
            .concat(
                Linq4j.asEnumerable(
                    schema.calciteSchema.getTablesBasedOnNullaryFunctions()
                        .entrySet()
                )
                    .select { pair ->
                        val table: Table = pair.getValue()
                        CalciteMetaTable(
                            table,
                            schema.tableCatalog,
                            schema.tableSchem,
                            pair.getKey()
                        )
                    })
    }

    fun tables(
        schema: MetaSchema?,
        matcher: Predicate1<String?>
    ): Enumerable<MetaTable> {
        return tables(schema)
            .where { v1 -> matcher.apply(v1.getName()) }
    }// All types are nullable

    // Making all type searchable; we may want to
    // be specific and declare under SqlTypeName
    // Skip internal types (NULL, ANY, SYMBOL, SARG).
    private val allDefaultType: ImmutableList<MetaTypeInfo>
        private get() {
            val allTypeList: ImmutableList.Builder<MetaTypeInfo> = ImmutableList.builder()
            val typeSystem: RelDataTypeSystem = connection.typeFactory.getTypeSystem()
            for (sqlTypeName in SqlTypeName.values()) {
                if (sqlTypeName.isSpecial()) {
                    // Skip internal types (NULL, ANY, SYMBOL, SARG).
                    continue
                }
                allTypeList.add(
                    MetaTypeInfo(
                        sqlTypeName.getName(),
                        sqlTypeName.getJdbcOrdinal(),
                        typeSystem.getMaxPrecision(sqlTypeName),
                        typeSystem.getLiteral(sqlTypeName, true),
                        typeSystem.getLiteral(sqlTypeName, false),  // All types are nullable
                        DatabaseMetaData.typeNullable as Short,
                        typeSystem.isCaseSensitive(sqlTypeName),  // Making all type searchable; we may want to
                        // be specific and declare under SqlTypeName
                        DatabaseMetaData.typeSearchable as Short,
                        false,
                        false,
                        typeSystem.isAutoincrement(sqlTypeName),
                        sqlTypeName.getMinScale() as Short,
                        typeSystem.getMaxScale(sqlTypeName) as Short,
                        typeSystem.getNumTypeRadix(sqlTypeName)
                    )
                )
            }
            return allTypeList.build()
        }

    protected fun allTypeInfo(): Enumerable<MetaTypeInfo> {
        return Linq4j.asEnumerable(allDefaultType)
    }

    fun columns(table_: MetaTable): Enumerable<MetaColumn> {
        val table = table_ as CalciteMetaTable
        val rowType: RelDataType = table.calciteTable.getRowType(connection.typeFactory)
        return Linq4j.asEnumerable(rowType.getFieldList())
            .select { field ->
                val precision = if (field.getType().getSqlTypeName().allowsPrec()
                    && field.getType() !is RelDataTypeFactoryImpl.JavaType
                ) field.getType().getPrecision() else -1
                MetaColumn(
                    table.tableCat,
                    table.tableSchem,
                    table.tableName,
                    field.getName(),
                    field.getType().getSqlTypeName().getJdbcOrdinal(),
                    field.getType().getFullTypeString(),
                    precision,
                    if (field.getType().getSqlTypeName().allowsScale()) field.getType().getScale() else null,
                    10,
                    if (field.getType()
                            .isNullable()
                    ) DatabaseMetaData.columnNullable else DatabaseMetaData.columnNoNulls,
                    precision,
                    field.getIndex() + 1,
                    if (field.getType().isNullable()) "YES" else "NO"
                )
            }
    }

    @Override
    fun getSchemas(
        ch: ConnectionHandle?, catalog: String?,
        schemaPattern: Pat
    ): MetaResultSet {
        val schemaMatcher: Predicate1<MetaSchema> = namedMatcher<Named>(schemaPattern)
        return createResultSet(
            schemas(catalog).where(schemaMatcher),
            MetaSchema::class.java,
            "TABLE_SCHEM",
            "TABLE_CATALOG"
        )
    }

    @Override
    fun getCatalogs(ch: ConnectionHandle?): MetaResultSet {
        return createResultSet<Any>(
            catalogs(),
            MetaCatalog::class.java,
            "TABLE_CAT"
        )
    }

    @Override
    fun getTableTypes(ch: ConnectionHandle?): MetaResultSet {
        return createResultSet<Any>(
            tableTypes(),
            MetaTableType::class.java,
            "TABLE_TYPE"
        )
    }

    @Override
    fun getFunctions(
        ch: ConnectionHandle?,
        catalog: String?,
        schemaPattern: Pat,
        functionNamePattern: Pat
    ): MetaResultSet {
        val schemaMatcher: Predicate1<MetaSchema> = namedMatcher<Named>(schemaPattern)
        return createResultSet(schemas(catalog)
            .where(schemaMatcher)
            .selectMany { schema -> functions(schema, catalog, matcher(functionNamePattern)) }
            .orderBy { x ->
                FlatLists.of(
                    x.functionCat, x.functionSchem, x.functionName, x.specificName
                )
            },
            MetaFunction::class.java,
            "FUNCTION_CAT",
            "FUNCTION_SCHEM",
            "FUNCTION_NAME",
            "REMARKS",
            "FUNCTION_TYPE",
            "SPECIFIC_NAME"
        )
    }

    fun functions(schema_: MetaSchema, catalog: String?): Enumerable<MetaFunction> {
        val schema = schema_ as CalciteMetaSchema
        var opTableFunctions: Enumerable<MetaFunction?> = Linq4j.emptyEnumerable()
        if (schema.calciteSchema.schema.equals(MetadataSchema.INSTANCE)) {
            val opTable: SqlOperatorTable = connection.config()
                .`fun`(SqlOperatorTable::class.java, SqlStdOperatorTable.instance())
            val q: List<SqlOperator> = opTable.getOperatorList()
            opTableFunctions = Linq4j.asEnumerable(q)
                .where { op -> SqlKind.FUNCTION.contains(op.getKind()) }
                .select { op ->
                    MetaFunction(
                        catalog,
                        schema.getName(),
                        op.getName(),
                        DatabaseMetaData.functionResultUnknown as Short,
                        op.getName()
                    )
                }
        }
        return Linq4j.asEnumerable(schema.calciteSchema.getFunctionNames())
            .selectMany { name ->
                Linq4j.asEnumerable(
                    schema.calciteSchema.getFunctions(
                        name,
                        true
                    )
                ) //exclude materialized views from the result set
                    .where { fn -> fn !is MaterializedViewTable.MaterializedViewTableMacro }
                    .select { fnx ->
                        MetaFunction(
                            catalog,
                            schema.getName(),
                            name,
                            DatabaseMetaData.functionResultUnknown as Short,
                            name
                        )
                    }
            }
            .concat(opTableFunctions)
    }

    fun functions(
        schema: MetaSchema, catalog: String?,
        functionNameMatcher: Predicate1<String?>
    ): Enumerable<MetaFunction> {
        return functions(schema, catalog)
            .where { v1 -> functionNameMatcher.apply(v1.functionName) }
    }

    @Override
    fun createIterable(
        handle: StatementHandle?, state: QueryState?,
        signature: Signature, @Nullable parameterValues: List<TypedValue?>?, @Nullable firstFrame: Frame?
    ): Iterable<Object> {
        // Drop QueryState
        return _createIterable(handle, signature, parameterValues, firstFrame)
    }

    fun _createIterable(
        handle: StatementHandle?,
        signature: Signature, @Nullable parameterValues: List<TypedValue?>?, @Nullable firstFrame: Frame?
    ): Iterable<Object> {
        return try {
            connection.enumerable(handle, signature, parameterValues)
        } catch (e: SQLException) {
            throw RuntimeException(e.getMessage())
        }
    }

    @Override
    fun prepare(
        ch: ConnectionHandle?, sql: String,
        maxRowCount: Long
    ): StatementHandle {
        val h: StatementHandle = createStatement(ch)
        val calciteConnection: CalciteConnectionImpl = connection
        val statement: CalciteServerStatement
        statement = try {
            calciteConnection.server.getStatement(h)
        } catch (e: NoSuchStatementException) {
            // Not possible. We just created a statement.
            throw AssertionError("missing statement", e)
        }
        val context: Context = statement.createPrepareContext()
        val query: Query<Object> = toQuery(context, sql)
        h.signature = calciteConnection.parseQuery(query, context, maxRowCount)
        statement.setSignature(h.signature)
        return h
    }

    @SuppressWarnings("deprecation")
    @Override
    @Throws(NoSuchStatementException::class)
    fun prepareAndExecute(
        h: StatementHandle,
        sql: String, maxRowCount: Long, callback: PrepareCallback
    ): ExecuteResult {
        return prepareAndExecute(h, sql, maxRowCount, -1, callback)
    }

    @Override
    @Throws(NoSuchStatementException::class)
    fun prepareAndExecute(
        h: StatementHandle,
        sql: String, maxRowCount: Long, maxRowsInFirstFrame: Int,
        callback: PrepareCallback
    ): ExecuteResult {
        var signature: CalcitePrepare.CalciteSignature<Object>
        return try {
            var updateCount: Int
            synchronized(callback.getMonitor()) {
                callback.clear()
                val calciteConnection: CalciteConnectionImpl = connection
                val statement: CalciteServerStatement = calciteConnection.server.getStatement(h)
                val context: Context = statement.createPrepareContext()
                val query: Query<Object> = toQuery(context, sql)
                signature = calciteConnection.parseQuery(query, context, maxRowCount)
                statement.setSignature(signature)
                updateCount = when (signature.statementType) {
                    CREATE, DROP, ALTER, OTHER_DDL -> 0 // DDL produces no result set
                    else -> -1 // SELECT and DML produces result set
                }
                callback.assign(signature, null, updateCount)
            }
            callback.execute()
            val metaResultSet: MetaResultSet =
                MetaResultSet.create(h.connectionId, h.id, false, signature, null, updateCount)
            ExecuteResult(ImmutableList.of(metaResultSet))
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }
        // TODO: share code with prepare and createIterable
    }

    @Override
    @Throws(NoSuchStatementException::class)
    fun fetch(
        h: StatementHandle?, offset: Long,
        fetchMaxRowCount: Int
    ): Frame {
        val calciteConnection: CalciteConnectionImpl = connection
        val stmt: CalciteServerStatement = calciteConnection.server.getStatement(h)
        val signature: Signature = requireNonNull(
            stmt.getSignature()
        ) { "stmt.getSignature() is null for $stmt" }
        val iterator: Iterator<Object>
        val stmtResultSet: Iterator<Object> = stmt.getResultSet()
        if (stmtResultSet == null) {
            val iterable: Iterable<Object> = _createIterable(h, signature, null, null)
            iterator = iterable.iterator()
            stmt.setResultSet(iterator)
        } else {
            iterator = stmtResultSet
        }
        val rows: List = MetaImpl.collect(
            signature.cursorFactory,
            LimitIterator.of<Any>(iterator, fetchMaxRowCount.toLong()),
            ArrayList<List<Object>>()
        )
        val done = fetchMaxRowCount == 0 || rows.size() < fetchMaxRowCount
        return Frame(offset, done, rows)
    }

    @SuppressWarnings("deprecation")
    @Override
    @Throws(NoSuchStatementException::class)
    fun execute(
        h: StatementHandle,
        parameterValues: List<TypedValue?>?, maxRowCount: Long
    ): ExecuteResult {
        return execute(h, parameterValues, Ints.saturatedCast(maxRowCount))
    }

    @Override
    @Throws(NoSuchStatementException::class)
    fun execute(
        h: StatementHandle,
        parameterValues: List<TypedValue?>?, maxRowsInFirstFrame: Int
    ): ExecuteResult {
        val calciteConnection: CalciteConnectionImpl = connection
        val stmt: CalciteServerStatement = calciteConnection.server.getStatement(h)
        val signature: Signature = requireNonNull(
            stmt.getSignature()
        ) { "stmt.getSignature() is null for $stmt" }
        val metaResultSet: MetaResultSet
        metaResultSet = if (signature.statementType.canUpdate()) {
            val iterable: Iterable<Object> = _createIterable(h, signature, parameterValues, null)
            val iterator: Iterator<Object> = iterable.iterator()
            stmt.setResultSet(iterator)
            MetaResultSet.count(
                h.connectionId, h.id,
                (iterator.next() as Number).intValue()
            )
        } else {
            // Don't populate the first frame.
            // It's not worth saving a round-trip, since we're local.
            val frame: Meta.Frame = Frame(0, false, Collections.emptyList())
            MetaResultSet.create(h.connectionId, h.id, false, signature, frame)
        }
        return ExecuteResult(ImmutableList.of(metaResultSet))
    }

    @Override
    @Throws(NoSuchStatementException::class)
    fun executeBatch(
        h: StatementHandle,
        parameterValueLists: List<List<TypedValue?>?>
    ): ExecuteBatchResult {
        val updateCounts: List<Long> = ArrayList()
        for (parameterValueList in parameterValueLists) {
            val executeResult: ExecuteResult = execute(h, parameterValueList, -1)
            val updateCount =
                if (executeResult.resultSets.size() === 1) executeResult.resultSets.get(0).updateCount else -1L
            updateCounts.add(updateCount)
        }
        return ExecuteBatchResult(Longs.toArray(updateCounts))
    }

    @Override
    @Throws(NoSuchStatementException::class)
    fun prepareAndExecuteBatch(
        h: StatementHandle,
        sqlCommands: List<String>
    ): ExecuteBatchResult {
        val calciteConnection: CalciteConnectionImpl = connection
        val statement: CalciteServerStatement = calciteConnection.server.getStatement(h)
        val updateCounts: List<Long> = ArrayList()
        val callback: Meta.PrepareCallback = object : PrepareCallback() {
            var updateCount: Long = 0

            @Nullable
            var signature: Signature? = null

            @get:Override
            val monitor: Object
                get() = statement

            @Override
            @Throws(SQLException::class)
            fun clear() {
            }

            @Override
            @Throws(SQLException::class)
            fun assign(
                signature: Meta.Signature?, firstFrame: @Nullable Meta.Frame?,
                updateCount: Long
            ) {
                this.signature = signature
                this.updateCount = updateCount
            }

            @Override
            @Throws(SQLException::class)
            fun execute() {
                val signature: Signature = requireNonNull(signature, "signature")
                if (signature.statementType.canUpdate()) {
                    val iterable: Iterable<Object> = _createIterable(
                        h, signature, ImmutableList.of(),
                        null
                    )
                    val iterator: Iterator<Object> = iterable.iterator()
                    updateCount = (iterator.next() as Number).longValue()
                }
                updateCounts.add(updateCount)
            }
        }
        for (sqlCommand in sqlCommands) {
            Util.discard(prepareAndExecute(h, sqlCommand, -1L, -1, callback))
        }
        return ExecuteBatchResult(Longs.toArray(updateCounts))
    }

    @Override
    @Throws(NoSuchStatementException::class)
    fun syncResults(h: StatementHandle?, state: QueryState?, offset: Long): Boolean {
        // Doesn't have application in Calcite itself.
        throw UnsupportedOperationException()
    }

    @Override
    fun commit(ch: ConnectionHandle?) {
        throw UnsupportedOperationException()
    }

    @Override
    fun rollback(ch: ConnectionHandle?) {
        throw UnsupportedOperationException()
    }

    /** Metadata describing a Calcite table.  */
    private class CalciteMetaTable internal constructor(
        calciteTable: Table, tableCat: String?,
        tableSchem: String?, tableName: String?
    ) : MetaTable(
        tableCat, tableSchem, tableName,
        calciteTable.getJdbcTableType().jdbcName
    ) {
        val calciteTable: Table

        init {
            this.calciteTable = requireNonNull(calciteTable, "calciteTable")
        }
    }

    /** Metadata describing a Calcite schema.  */
    private class CalciteMetaSchema internal constructor(
        calciteSchema: CalciteSchema,
        tableCatalog: String?, tableSchem: String?
    ) : MetaSchema(tableCatalog, tableSchem) {
        val calciteSchema: CalciteSchema

        init {
            this.calciteSchema = calciteSchema
        }
    }

    /** Table whose contents are metadata.
     *
     * @param <E> element type
    </E> */
    internal abstract class MetadataTable<E>(clazz: Class<E>?) : AbstractQueryableTable(clazz) {
        @Override
        fun getRowType(typeFactory: RelDataTypeFactory): RelDataType {
            return (typeFactory as JavaTypeFactory).createType(elementType)
        }

        @get:Override
        val jdbcTableType: Schema.TableType
            get() = Schema.TableType.SYSTEM_TABLE

        @get:Override
        @get:SuppressWarnings("unchecked")
        val elementType: Class<E>
            get() = field as Class<E>

        protected abstract fun enumerator(connection: CalciteMetaImpl?): Enumerator<E>?
        @Override
        fun <T> asQueryable(
            queryProvider: QueryProvider,
            schema: SchemaPlus?, tableName: String?
        ): Queryable<T> {
            return object : AbstractTableQueryable<T>(
                queryProvider, schema, this,
                tableName
            ) {
                @SuppressWarnings("unchecked")
                @Override
                fun enumerator(): Enumerator<T> {
                    return this@MetadataTable.enumerator(
                        (queryProvider as CalciteConnectionImpl).meta()
                    ) as Enumerator<T>
                }
            }
        }
    }

    /** Iterator that returns at most `limit` rows from an underlying
     * [Iterator].
     *
     * @param <E> element type
    </E> */
    private class LimitIterator<E> private constructor(private val iterator: Iterator<E>, private val limit: Long) :
        Iterator<E> {
        var i = 0

        @Override
        override fun hasNext(): Boolean {
            return iterator.hasNext() && i < limit
        }

        @Override
        override fun next(): E {
            ++i
            return iterator.next()
        }

        @Override
        fun remove() {
            throw UnsupportedOperationException()
        }

        companion object {
            fun <E> of(iterator: Iterator<E>, limit: Long): Iterator<E> {
                return if (limit <= 0) {
                    iterator
                } else LimitIterator<Any>(iterator, limit)
            }
        }
    }

    companion object {
        val DRIVER: Driver = Driver()
        fun <T : Named?> namedMatcher(pattern: Pat): Predicate1<T> {
            if (pattern.s == null || pattern.s.equals("%")) {
                return Functions.truePredicate1()
            }
            val regex: Pattern = likeToRegex(pattern)
            return Predicate1<T> { v1 -> regex.matcher(v1.getName()).matches() }
        }

        fun matcher(pattern: Pat): Predicate1<String> {
            if (pattern.s == null || pattern.s.equals("%")) {
                return Functions.truePredicate1()
            }
            val regex: Pattern = likeToRegex(pattern)
            return Predicate1<String> { v1 -> regex.matcher(v1).matches() }
        }

        /** Converts a LIKE-style pattern (where '%' represents a wild-card, escaped
         * using '\') to a Java regex.  */
        fun likeToRegex(pattern: Pat): Pattern {
            val buf = StringBuilder("^")
            val charArray: CharArray = pattern.s.toCharArray()
            var slash = -2
            for (i in charArray.indices) {
                val c = charArray[i]
                if (slash == i - 1) {
                    buf.append('[').append(c).append(']')
                } else {
                    when (c) {
                        '\\' -> slash = i
                        '%' -> buf.append(".*")
                        '[' -> buf.append("\\[")
                        ']' -> buf.append("\\]")
                        else -> buf.append('[').append(c).append(']')
                    }
                }
            }
            buf.append("$")
            return Pattern.compile(buf.toString())
        }

        private fun addProperty(
            builder: ImmutableMap.Builder<DatabaseProperty, Object>,
            p: DatabaseProperty
        ): ImmutableMap.Builder<DatabaseProperty, Object> {
            return when (p) {
                GET_S_Q_L_KEYWORDS -> builder.put(
                    p,
                    SqlParser.create("").getMetadata().getJdbcKeywords()
                )
                GET_NUMERIC_FUNCTIONS -> builder.put(p, SqlJdbcFunctionCall.getNumericFunctions())
                GET_STRING_FUNCTIONS -> builder.put(p, SqlJdbcFunctionCall.getStringFunctions())
                GET_SYSTEM_FUNCTIONS -> builder.put(p, SqlJdbcFunctionCall.getSystemFunctions())
                GET_TIME_DATE_FUNCTIONS -> builder.put(p, SqlJdbcFunctionCall.getTimeDateFunctions())
                else -> builder
            }
        }

        /** Wraps the SQL string in a
         * [org.apache.calcite.jdbc.CalcitePrepare.Query] object, giving the
         * [Hook.STRING_TO_QUERY] hook chance to override.  */
        private fun toQuery(
            context: Context, sql: String
        ): Query<Object> {
            val queryHolder: Holder<Query<Object>> = Holder.of(CalcitePrepare.Query.of(sql))
            val config: FrameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .defaultSchema(context.getRootSchema().plus())
                .build()
            Hook.STRING_TO_QUERY.run(Pair.of(config, queryHolder))
            return queryHolder.get()
        }

        /** A trojan-horse method, subject to change without notice.  */
        @VisibleForTesting
        fun createDataContext(connection: CalciteConnection): DataContext {
            return (connection as CalciteConnectionImpl)
                .createDataContext(
                    ImmutableMap.of(),
                    CalciteSchema.from(connection.getRootSchema())
                )
        }

        /** A trojan-horse method, subject to change without notice.  */
        @VisibleForTesting
        fun connect(
            schema: CalciteSchema?,
            @Nullable typeFactory: JavaTypeFactory?
        ): CalciteConnection? {
            return DRIVER.connect(schema, typeFactory)
        }
    }
}
