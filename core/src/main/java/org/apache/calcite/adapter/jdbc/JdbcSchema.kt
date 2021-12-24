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
package org.apache.calcite.adapter.jdbc

import org.apache.calcite.avatica.AvaticaUtils

/**
 * Implementation of [Schema] that is backed by a JDBC data source.
 *
 *
 * The tables in the JDBC data source appear to be tables in this schema;
 * queries against this schema are executed against those tables, pushing down
 * as much as possible of the query logic to SQL.
 */
class JdbcSchema private constructor(
    dataSource: DataSource, dialect: SqlDialect,
    convention: JdbcConvention, @Nullable catalog: String?, @Nullable schema: String?,
    @Nullable tableMap: ImmutableMap<String, JdbcTable>
) : Schema {
    val dataSource: DataSource

    @Nullable
    val catalog: String?

    @Nullable
    val schema: String?
    val dialect: SqlDialect
    val convention: JdbcConvention

    @Nullable
    private var tableMap: ImmutableMap<String, JdbcTable>
    private val snapshot: Boolean

    /**
     * Creates a JDBC schema.
     *
     * @param dataSource Data source
     * @param dialect SQL dialect
     * @param convention Calling convention
     * @param catalog Catalog name, or null
     * @param schema Schema name pattern
     */
    constructor(
        dataSource: DataSource, dialect: SqlDialect,
        convention: JdbcConvention, @Nullable catalog: String?, @Nullable schema: String?
    ) : this(dataSource, dialect, convention, catalog, schema, null) {
    }

    init {
        this.dataSource = requireNonNull(dataSource, "dataSource")
        this.dialect = requireNonNull(dialect, "dialect")
        this.convention = convention
        this.catalog = catalog
        this.schema = schema
        this.tableMap = tableMap
        snapshot = tableMap != null
    }

    @get:Override
    val isMutable: Boolean
        get() = false

    @Override
    fun snapshot(version: SchemaVersion?): Schema {
        return JdbcSchema(
            dataSource, dialect, convention, catalog, schema,
            tableMap
        )
    }

    // Used by generated code.
    fun getDataSource(): DataSource {
        return dataSource
    }

    @Override
    fun getExpression(@Nullable parentSchema: SchemaPlus?, name: String?): Expression {
        requireNonNull(parentSchema, "parentSchema must not be null for JdbcSchema")
        return Schemas.subSchemaExpression(parentSchema, name, JdbcSchema::class.java)
    }

    // TODO: populate map from JDBC metadata
    protected val functions: Multimap<String, Function>
        protected get() =// TODO: populate map from JDBC metadata
            ImmutableMultimap.of()

    @Override
    fun getFunctions(name: String?): Collection<Function> {
        return functions.get(name) // never null
    }

    @get:Override
    val functionNames: Set<String>
        get() = functions.keySet()

    private fun computeTables(): ImmutableMap<String, JdbcTable> {
        var connection: Connection? = null
        var resultSet: ResultSet? = null
        return try {
            connection = dataSource.getConnection()
            val catalogSchema: Pair<String, String> = getCatalogSchema(connection)
            val catalog: String = catalogSchema.left
            val schema: String = catalogSchema.right
            val tableDefs: Iterable<MetaImpl.MetaTable>
            val threadMetadata: Foo = THREAD_METADATA.get()
            if (threadMetadata != null) {
                tableDefs = threadMetadata.apply(catalog, schema)
            } else {
                val tableDefList: List<MetaImpl.MetaTable> = ArrayList()
                val metaData: DatabaseMetaData = connection.getMetaData()
                resultSet = metaData.getTables(catalog, schema, null, null)
                while (resultSet.next()) {
                    val catalogName: String = resultSet.getString(1)
                    val schemaName: String = resultSet.getString(2)
                    val tableName: String = resultSet.getString(3)
                    val tableTypeName: String = resultSet.getString(4)
                    tableDefList.add(
                        MetaTable(
                            catalogName, schemaName, tableName,
                            tableTypeName
                        )
                    )
                }
                tableDefs = tableDefList
            }
            val builder: ImmutableMap.Builder<String, JdbcTable> = ImmutableMap.builder()
            for (tableDef in tableDefs) {
                // Clean up table type. In particular, this ensures that 'SYSTEM TABLE',
                // returned by Phoenix among others, maps to TableType.SYSTEM_TABLE.
                // We know enum constants are upper-case without spaces, so we can't
                // make things worse.
                //
                // PostgreSQL returns tableTypeName==null for pg_toast* tables
                // This can happen if you start JdbcSchema off a "public" PG schema
                // The tables are not designed to be queried by users, however we do
                // not filter them as we keep all the other table types.
                val tableTypeName2: String? =
                    if (tableDef.tableType == null) null else tableDef.tableType.toUpperCase(Locale.ROOT)
                        .replace(' ', '_')
                val tableType: TableType = Util.enumVal(TableType.OTHER, tableTypeName2)
                if (tableType === TableType.OTHER && tableTypeName2 != null) {
                    System.out.println("Unknown table type: $tableTypeName2")
                }
                val table = JdbcTable(
                    this, tableDef.tableCat, tableDef.tableSchem,
                    tableDef.tableName, tableType
                )
                builder.put(tableDef.tableName, table)
            }
            builder.build()
        } catch (e: SQLException) {
            throw RuntimeException(
                "Exception while reading tables", e
            )
        } finally {
            close(connection, null, resultSet)
        }
    }

    /** Returns a pair of (catalog, schema) for the current connection.  */
    @Throws(SQLException::class)
    private fun getCatalogSchema(connection: Connection?): Pair<String, String> {
        val metaData: DatabaseMetaData = connection.getMetaData()
        val version41: List<Integer> = ImmutableList.of(4, 1) // JDBC 4.1
        var catalog = catalog
        var schema = schema
        val jdbc41OrAbove: Boolean = VERSION_ORDERING.compare(version(metaData), version41) >= 0
        if (catalog == null && jdbc41OrAbove) {
            // From JDBC 4.1, catalog and schema can be retrieved from the connection
            // object, hence try to get it from there if it was not specified by user
            catalog = connection.getCatalog()
        }
        if (schema == null && jdbc41OrAbove) {
            schema = connection.getSchema()
            if ("".equals(schema)) {
                schema = null // PostgreSQL returns useless "" sometimes
            }
        }
        if ((catalog == null || schema == null)
            && metaData.getDatabaseProductName().equals("PostgreSQL")
        ) {
            val sql = "select current_database(), current_schema()"
            connection.createStatement().use { statement ->
                statement.executeQuery(sql).use { resultSet ->
                    if (resultSet.next()) {
                        catalog = resultSet.getString(1)
                        schema = resultSet.getString(2)
                    }
                }
            }
        }
        return Pair.of(catalog, schema)
    }

    @Override
    @Nullable
    fun getTable(name: String?): Table {
        return getTableMap(false).get(name)
    }

    @Synchronized
    private fun getTableMap(
        force: Boolean
    ): ImmutableMap<String, JdbcTable> {
        if (force || tableMap == null) {
            tableMap = computeTables()
        }
        return tableMap
    }

    @Throws(SQLException::class)
    fun getRelDataType(
        catalogName: String?, schemaName: String?,
        tableName: String?
    ): RelProtoDataType {
        var connection: Connection? = null
        return try {
            connection = dataSource.getConnection()
            val metaData: DatabaseMetaData = connection.getMetaData()
            getRelDataType(metaData, catalogName, schemaName, tableName)
        } finally {
            close(connection, null, null)
        }
    }

    @Throws(SQLException::class)
    fun getRelDataType(
        metaData: DatabaseMetaData, catalogName: String?,
        schemaName: String?, tableName: String?
    ): RelProtoDataType {
        val resultSet: ResultSet = metaData.getColumns(catalogName, schemaName, tableName, null)

        // Temporary type factory, just for the duration of this method. Allowable
        // because we're creating a proto-type, not a type; before being used, the
        // proto-type will be copied into a real type factory.
        val typeFactory: RelDataTypeFactory = SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)
        val fieldInfo: RelDataTypeFactory.Builder = typeFactory.builder()
        while (resultSet.next()) {
            val columnName: String = requireNonNull(resultSet.getString(4), "columnName")
            val dataType: Int = resultSet.getInt(5)
            val typeString: String = resultSet.getString(6)
            val precision: Int
            val scale: Int
            when (SqlType.valueOf(dataType)) {
                TIMESTAMP, TIME -> {
                    precision = resultSet.getInt(9) // SCALE
                    scale = 0
                }
                else -> {
                    precision = resultSet.getInt(7) // SIZE
                    scale = resultSet.getInt(9) // SCALE
                }
            }
            val sqlType: RelDataType = sqlType(typeFactory, dataType, precision, scale, typeString)
            val nullable = resultSet.getInt(11) !== DatabaseMetaData.columnNoNulls
            fieldInfo.add(columnName, sqlType).nullable(nullable)
        }
        resultSet.close()
        return RelDataTypeImpl.proto(fieldInfo.build())
    }

    // This method is called during a cache refresh. We can take it as a signal
    // that we need to re-build our own cache.
    @get:Override
    val tableNames: Set<String>
        get() =// This method is called during a cache refresh. We can take it as a signal
            // that we need to re-build our own cache.
            getTableMap(!snapshot).keySet()

    // TODO: populate map from JDBC metadata
    protected val types: Map<String, Any>
        protected get() =// TODO: populate map from JDBC metadata
            ImmutableMap.of()

    @Override
    @Nullable
    fun getType(name: String): RelProtoDataType? {
        return types[name]
    }

    @get:Override
    val typeNames: Set<String>
        get() = types.keySet() as Set<String>

    @Override
    @Nullable
    fun getSubSchema(name: String?): Schema? {
        // JDBC does not support sub-schemas.
        return null
    }

    @get:Override
    val subSchemaNames: Set<String>
        get() = ImmutableSet.of()

    /** Schema factory that creates a
     * [org.apache.calcite.adapter.jdbc.JdbcSchema].
     *
     *
     * This allows you to create a jdbc schema inside a model.json file, like
     * this:
     *
     * <blockquote><pre>
     * {
     * "version": "1.0",
     * "defaultSchema": "FOODMART_CLONE",
     * "schemas": [
     * {
     * "name": "FOODMART_CLONE",
     * "type": "custom",
     * "factory": "org.apache.calcite.adapter.jdbc.JdbcSchema$Factory",
     * "operand": {
     * "jdbcDriver": "com.mysql.jdbc.Driver",
     * "jdbcUrl": "jdbc:mysql://localhost/foodmart",
     * "jdbcUser": "foodmart",
     * "jdbcPassword": "foodmart"
     * }
     * }
     * ]
     * }</pre></blockquote>
     */
    class Factory private constructor() : SchemaFactory {
        @Override
        fun create(
            parentSchema: SchemaPlus?,
            name: String?,
            operand: Map<String?, Object?>
        ): Schema {
            return JdbcSchema.create(parentSchema, name, operand)
        }

        companion object {
            val INSTANCE = Factory()
        }
    }

    /** Do not use.  */
    @Experimental
    interface Foo : BiFunction<String?, String?, Iterable<MetaImpl.MetaTable?>?>
    companion object {
        @Experimental
        val THREAD_METADATA: ThreadLocal<Foo> = ThreadLocal()
        private val VERSION_ORDERING: Ordering<Iterable<Integer>> =
            Ordering.< Integer > natural < Integer ? > ().lexicographical()

        fun create(
            parentSchema: SchemaPlus?,
            name: String?,
            dataSource: DataSource,
            @Nullable catalog: String?,
            @Nullable schema: String?
        ): JdbcSchema {
            return create(
                parentSchema, name, dataSource,
                SqlDialectFactoryImpl.INSTANCE, catalog, schema
            )
        }

        fun create(
            parentSchema: SchemaPlus?,
            name: String?,
            dataSource: DataSource,
            dialectFactory: SqlDialectFactory?,
            @Nullable catalog: String?,
            @Nullable schema: String?
        ): JdbcSchema {
            val expression: Expression = Schemas.subSchemaExpression(parentSchema, name, JdbcSchema::class.java)
            val dialect: SqlDialect = createDialect(dialectFactory, dataSource)
            val convention: JdbcConvention = JdbcConvention.of(dialect, expression, name)
            return JdbcSchema(dataSource, dialect, convention, catalog, schema)
        }

        /**
         * Creates a JdbcSchema, taking credentials from a map.
         *
         * @param parentSchema Parent schema
         * @param name Name
         * @param operand Map of property/value pairs
         * @return A JdbcSchema
         */
        fun create(
            parentSchema: SchemaPlus?,
            name: String?,
            operand: Map<String?, Object?>
        ): JdbcSchema {
            val dataSource: DataSource
            dataSource = try {
                val dataSourceName = operand["dataSource"] as String?
                if (dataSourceName != null) {
                    AvaticaUtils.instantiatePlugin(DataSource::class.java, dataSourceName)
                } else {
                    val jdbcUrl = requireNonNull(operand["jdbcUrl"], "jdbcUrl") as String
                    val jdbcDriver = operand["jdbcDriver"] as String?
                    val jdbcUser = operand["jdbcUser"] as String?
                    val jdbcPassword = operand["jdbcPassword"] as String?
                    dataSource(jdbcUrl, jdbcDriver, jdbcUser, jdbcPassword)
                }
            } catch (e: Exception) {
                throw RuntimeException("Error while reading dataSource", e)
            }
            val jdbcCatalog = operand["jdbcCatalog"] as String?
            val jdbcSchema = operand["jdbcSchema"] as String?
            val sqlDialectFactory = operand["sqlDialectFactory"] as String?
            return if (sqlDialectFactory == null || sqlDialectFactory.isEmpty()) {
                create(
                    parentSchema, name, dataSource, jdbcCatalog, jdbcSchema
                )
            } else {
                val factory: SqlDialectFactory = AvaticaUtils.instantiatePlugin(
                    SqlDialectFactory::class.java, sqlDialectFactory
                )
                create(
                    parentSchema, name, dataSource, factory, jdbcCatalog, jdbcSchema
                )
            }
        }

        /**
         * Returns a suitable SQL dialect for the given data source.
         *
         * @param dataSource The data source
         *
         */
        @Deprecated // to be removed before 2.0
        @Deprecated("Use {@link #createDialect(SqlDialectFactory, DataSource)} instead")
        fun createDialect(dataSource: DataSource?): SqlDialect {
            return createDialect(SqlDialectFactoryImpl.INSTANCE, dataSource)
        }

        /** Returns a suitable SQL dialect for the given data source.  */
        fun createDialect(
            dialectFactory: SqlDialectFactory?,
            dataSource: DataSource?
        ): SqlDialect {
            return JdbcUtils.DialectPool.INSTANCE.get(dialectFactory, dataSource)
        }

        /** Creates a JDBC data source with the given specification.  */
        fun dataSource(
            url: String, @Nullable driverClassName: String?,
            @Nullable username: String?, @Nullable password: String?
        ): DataSource {
            if (url.startsWith("jdbc:hsqldb:")) {
                // Prevent hsqldb from screwing up java.util.logging.
                System.setProperty("hsqldb.reconfig_logging", "false")
            }
            return JdbcUtils.DataSourcePool.INSTANCE.get(
                url, driverClassName, username,
                password
            )
        }

        /** Returns [major, minor] version from a database metadata.  */
        @Throws(SQLException::class)
        private fun version(metaData: DatabaseMetaData): List<Integer> {
            return ImmutableList.of(
                metaData.getJDBCMajorVersion(),
                metaData.getJDBCMinorVersion()
            )
        }

        private fun sqlType(
            typeFactory: RelDataTypeFactory, dataType: Int,
            precision: Int, scale: Int, @Nullable typeString: String?
        ): RelDataType {
            // Fall back to ANY if type is unknown
            val sqlTypeName: SqlTypeName = Util.first(SqlTypeName.getNameForJdbcType(dataType), SqlTypeName.ANY)
            when (sqlTypeName) {
                ARRAY -> {
                    var component: RelDataType? = null
                    if (typeString != null && typeString.endsWith(" ARRAY")) {
                        // E.g. hsqldb gives "INTEGER ARRAY", so we deduce the component type
                        // "INTEGER".
                        val remaining: String = typeString.substring(
                            0,
                            typeString.length() - " ARRAY".length()
                        )
                        component = parseTypeString(typeFactory, remaining)
                    }
                    if (component == null) {
                        component = typeFactory.createTypeWithNullability(
                            typeFactory.createSqlType(SqlTypeName.ANY), true
                        )
                    }
                    return typeFactory.createArrayType(component, -1)
                }
                else -> {}
            }
            return if (precision >= 0 && scale >= 0 && sqlTypeName.allowsPrecScale(true, true)) {
                typeFactory.createSqlType(sqlTypeName, precision, scale)
            } else if (precision >= 0 && sqlTypeName.allowsPrecNoScale()) {
                typeFactory.createSqlType(sqlTypeName, precision)
            } else {
                assert(sqlTypeName.allowsNoPrecNoScale())
                typeFactory.createSqlType(sqlTypeName)
            }
        }

        /** Given "INTEGER", returns BasicSqlType(INTEGER).
         * Given "VARCHAR(10)", returns BasicSqlType(VARCHAR, 10).
         * Given "NUMERIC(10, 2)", returns BasicSqlType(NUMERIC, 10, 2).  */
        private fun parseTypeString(
            typeFactory: RelDataTypeFactory,
            typeString: String
        ): RelDataType {
            var typeString = typeString
            var precision = -1
            var scale = -1
            val open: Int = typeString.indexOf("(")
            if (open >= 0) {
                val close: Int = typeString.indexOf(")", open)
                if (close >= 0) {
                    val rest: String = typeString.substring(open + 1, close)
                    typeString = typeString.substring(0, open)
                    val comma: Int = rest.indexOf(",")
                    if (comma >= 0) {
                        precision = Integer.parseInt(rest.substring(0, comma))
                        scale = Integer.parseInt(rest.substring(comma))
                    } else {
                        precision = Integer.parseInt(rest)
                    }
                }
            }
            return try {
                val typeName: SqlTypeName = SqlTypeName.valueOf(typeString)
                if (typeName.allowsPrecScale(true, true)) typeFactory.createSqlType(
                    typeName,
                    precision,
                    scale
                ) else if (typeName.allowsPrecScale(true, false)) typeFactory.createSqlType(
                    typeName,
                    precision
                ) else typeFactory.createSqlType(typeName)
            } catch (e: IllegalArgumentException) {
                typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.ANY), true
                )
            }
        }

        private fun close(
            @Nullable connection: Connection?,
            @Nullable statement: Statement?,
            @Nullable resultSet: ResultSet?
        ) {
            if (resultSet != null) {
                try {
                    resultSet.close()
                } catch (e: SQLException) {
                    // ignore
                }
            }
            if (statement != null) {
                try {
                    statement.close()
                } catch (e: SQLException) {
                    // ignore
                }
            }
            if (connection != null) {
                try {
                    connection.close()
                } catch (e: SQLException) {
                    // ignore
                }
            }
        }
    }
}
