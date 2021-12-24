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
 * Implementation of JDBC connection
 * in the Calcite engine.
 *
 *
 * Abstract to allow newer versions of JDBC to add methods.
 */
abstract class CalciteConnectionImpl protected constructor(
    driver: Driver, factory: AvaticaFactory?,
    url: String?, info: Properties?, @Nullable rootSchema: CalciteSchema?,
    @Nullable typeFactory: JavaTypeFactory?
) : AvaticaConnection(driver, factory, url, info), CalciteConnection, QueryProvider {
    override val typeFactory: JavaTypeFactory? = null
    override val rootSchema: CalciteSchema
    val prepareFactory: Function0<CalcitePrepare>
    val server: CalciteServer = CalciteServerImpl()

    /**
     * Creates a CalciteConnectionImpl.
     *
     *
     * Not public; method is called only from the driver.
     *
     * @param driver Driver
     * @param factory Factory for JDBC objects
     * @param url Server URL
     * @param info Other connection properties
     * @param rootSchema Root schema, or null
     * @param typeFactory Type factory, or null
     */
    init {
        val cfg: CalciteConnectionConfig = CalciteConnectionConfigImpl(info)
        prepareFactory = driver.prepareFactory
        if (typeFactory != null) {
            this.typeFactory = typeFactory
        } else {
            var typeSystem: RelDataTypeSystem = cfg.typeSystem(RelDataTypeSystem::class.java, RelDataTypeSystem.DEFAULT)
            if (cfg.conformance().shouldConvertRaggedUnionTypesToVarying()) {
                typeSystem = object : DelegatingTypeSystem(typeSystem) {
                    @Override
                    fun shouldConvertRaggedUnionTypesToVarying(): Boolean {
                        return true
                    }
                }
            }
            this.typeFactory = JavaTypeFactoryImpl(typeSystem)
        }
        this.rootSchema = requireNonNull(if (rootSchema != null) rootSchema else CalciteSchema.createRootSchema(true))
        Preconditions.checkArgument(this.rootSchema.isRoot(), "must be root schema")
        properties.put(InternalProperty.CASE_SENSITIVE, cfg.caseSensitive())
        properties.put(InternalProperty.UNQUOTED_CASING, cfg.unquotedCasing())
        properties.put(InternalProperty.QUOTED_CASING, cfg.quotedCasing())
        properties.put(InternalProperty.QUOTING, cfg.quoting())
    }

    fun meta(): CalciteMetaImpl {
        return meta
    }

    @Override
    override fun config(): CalciteConnectionConfig {
        return CalciteConnectionConfigImpl(info)
    }

    @Override
    override fun createPrepareContext(): Context {
        return ContextImpl(this)
    }

    /** Called after the constructor has completed and the model has been
     * loaded.  */
    fun init() {
        val service: MaterializationService = MaterializationService.instance()
        for (e in Schemas.getLatticeEntries(rootSchema)) {
            val lattice: Lattice = e.getLattice()
            for (tile in lattice.computeTiles()) {
                service.defineTile(
                    lattice, tile.bitSet(), tile.measures, e.schema,
                    true, true
                )
            }
        }
    }

    @Override
    @Throws(SQLException::class)
    fun <T> unwrap(iface: Class<T>): T {
        return if (iface === RelRunner::class.java) {
            iface.cast(object : RelRunner() {
                @Override
                @Throws(SQLException::class)
                fun prepareStatement(rel: RelNode?): PreparedStatement {
                    return prepareStatement_(
                        CalcitePrepare.Query.of(rel),
                        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                        getHoldability()
                    )
                }

                @SuppressWarnings("deprecation")
                @Override
                fun prepare(rel: RelNode?): PreparedStatement {
                    return try {
                        prepareStatement(rel)
                    } catch (e: SQLException) {
                        throw Util.throwAsRuntime(e)
                    }
                }
            })
        } else super.unwrap(iface)
    }

    @Override
    @Throws(SQLException::class)
    fun createStatement(
        resultSetType: Int,
        resultSetConcurrency: Int, resultSetHoldability: Int
    ): CalciteStatement {
        return super.createStatement(
            resultSetType,
            resultSetConcurrency, resultSetHoldability
        ) as CalciteStatement
    }

    @Override
    @Throws(SQLException::class)
    fun prepareStatement(
        sql: String?,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): CalcitePreparedStatement {
        val query: Query<Object> = CalcitePrepare.Query.of(sql)
        return prepareStatement_(
            query, resultSetType, resultSetConcurrency,
            resultSetHoldability
        )
    }

    @Throws(SQLException::class)
    private fun prepareStatement_(
        query: Query<*>,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): CalcitePreparedStatement {
        return try {
            val signature: Meta.Signature = parseQuery<Any>(query, createPrepareContext(), -1)
            val calcitePreparedStatement: CalcitePreparedStatement = factory.newPreparedStatement(
                this, null,
                signature, resultSetType, resultSetConcurrency, resultSetHoldability
            )
            server.getStatement(calcitePreparedStatement.handle).setSignature(signature)
            calcitePreparedStatement
        } catch (e: Exception) {
            val message =
                if (query.rel == null) "Error while preparing statement [" + query.sql.toString() + "]" else "Error while preparing plan [" + RelOptUtil.toString(
                    query.rel
                ).toString() + "]"
            throw Helper.INSTANCE.createException(message, e)
        }
    }

    fun <T> parseQuery(
        query: Query<T>?,
        prepareContext: CalcitePrepare.Context, maxRowCount: Long
    ): CalcitePrepare.CalciteSignature<T> {
        CalcitePrepare.Dummy.push(prepareContext)
        return try {
            val prepare: CalcitePrepare = prepareFactory.apply()
            prepare.prepareSql(
                prepareContext, query, Array<Object>::class.java,
                maxRowCount
            )
        } finally {
            CalcitePrepare.Dummy.pop(prepareContext)
        }
    }

    @Override
    @Throws(NoSuchStatementException::class)
    fun getCancelFlag(handle: Meta.StatementHandle?): AtomicBoolean {
        val serverStatement: CalciteServerStatement = server.getStatement(handle)
        return (serverStatement as CalciteServerStatementImpl).cancelFlag
    }

    // CalciteConnection methods
    @Override
    fun getRootSchema(): SchemaPlus {
        return rootSchema.plus()
    }

    @Override
    fun getTypeFactory(): JavaTypeFactory? {
        return typeFactory
    }

    @get:Override
    override val properties: Properties
        get() = info

    // QueryProvider methods
    @Override
    fun <T> createQuery(
        expression: Expression?, rowType: Class<T>?
    ): Queryable<T> {
        return CalciteQueryable<T>(this, rowType, expression)
    }

    @Override
    fun <T> createQuery(expression: Expression?, rowType: Type?): Queryable<T> {
        return CalciteQueryable<T>(this, rowType, expression)
    }

    @Override
    fun <T> execute(expression: Expression?, type: Type?): T {
        return castNonNull(null) // TODO:
    }

    @Override
    fun <T> execute(expression: Expression?, type: Class<T>?): T {
        return castNonNull(null) // TODO:
    }

    @Override
    fun <T> executeQuery(queryable: Queryable<T>?): Enumerator<T> {
        return try {
            val statement: CalciteStatement = createStatement()
            val signature: CalcitePrepare.CalciteSignature<T> = statement.prepare(queryable)
            enumerable<Any>(statement.handle, signature, null).enumerator()
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }
    }

    @Throws(SQLException::class)
    fun <T> enumerable(
        handle: Meta.StatementHandle?,
        signature: CalcitePrepare.CalciteSignature<T>,
        @Nullable parameterValues0: List<TypedValue?>?
    ): Enumerable<T> {
        val map: Map<String?, Object> = LinkedHashMap()
        val statement: AvaticaStatement = lookupStatement(handle)
        val parameterValues: List<TypedValue?>
        parameterValues = if (parameterValues0 == null || parameterValues0.isEmpty()) {
            TROJAN.getParameterValues(statement)
        } else {
            parameterValues0
        }
        if (MetaImpl.checkParameterValueHasNull(parameterValues)) {
            throw SQLException("exception while executing query: unbound parameter")
        }
        Ord.forEach(
            parameterValues
        ) { e, i -> map.put("?$i", e.toLocal()) }
        map.putAll(signature.internalParameters)
        val cancelFlag: AtomicBoolean
        cancelFlag = try {
            getCancelFlag(handle)
        } catch (e: NoSuchStatementException) {
            throw RuntimeException(e)
        }
        map.put(DataContext.Variable.CANCEL_FLAG.camelName, cancelFlag)
        val queryTimeout: Int = statement.getQueryTimeout()
        // Avoid overflow
        if (queryTimeout > 0 && queryTimeout < Integer.MAX_VALUE / 1000) {
            map.put(DataContext.Variable.TIMEOUT.camelName, queryTimeout * 1000L)
        }
        val dataContext: DataContext = createDataContext(map, signature.rootSchema)
        return signature.enumerable(dataContext)
    }

    fun createDataContext(
        parameterValues: Map<String?, Object?>,
        @Nullable rootSchema: CalciteSchema?
    ): DataContext {
        return if (config().spark()) {
            DataContexts.EMPTY
        } else DataContextImpl(this, parameterValues, rootSchema)
    }

    // do not make public
    val driver: UnregisteredDriver

    // do not make public
    val factory: AvaticaFactory

    /** Implementation of Queryable.
     *
     * @param <T> element type
    </T> */
    internal class CalciteQueryable<T>(
        connection: CalciteConnection?, elementType: Type?,
        expression: Expression?
    ) : BaseQueryable<T>(connection, elementType, expression) {
        val connection: org.apache.calcite.jdbc.CalciteConnection
            get() = provider as CalciteConnection
    }

    /** Implementation of Server.  */
    private class CalciteServerImpl : CalciteServer {
        val statementMap: Map<Integer, CalciteServerStatement> = HashMap()
        @Override
        fun removeStatement(h: Meta.StatementHandle) {
            statementMap.remove(h.id)
        }

        @Override
        fun addStatement(
            connection: CalciteConnection?,
            h: Meta.StatementHandle
        ) {
            val c = connection as CalciteConnectionImpl?
            val previous: CalciteServerStatement = statementMap.put(h.id, CalciteServerStatementImpl(c))
            if (previous != null) {
                throw AssertionError()
            }
        }

        @Override
        @Throws(NoSuchStatementException::class)
        fun getStatement(h: Meta.StatementHandle): CalciteServerStatement {
            return statementMap[h.id] ?: throw NoSuchStatementException(h)
        }
    }

    /** Schema that has no parents.  */
    internal class RootSchema : AbstractSchema() {
        @Override
        fun getExpression(
            @Nullable parentSchema: SchemaPlus?,
            name: String?
        ): Expression {
            return Expressions.call(
                DataContext.ROOT,
                BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method
            )
        }
    }

    /** Implementation of DataContext.  */
    internal class DataContextImpl(
        connection: CalciteConnectionImpl,
        parameters: Map<String?, Object?>, @Nullable rootSchema: CalciteSchema?
    ) : DataContext {
        private val map: ImmutableMap<Object, Object>

        @Nullable
        private val rootSchema: CalciteSchema?
        private val queryProvider: QueryProvider
        private val typeFactory: JavaTypeFactory?

        init {
            queryProvider = connection
            typeFactory = connection.getTypeFactory()
            this.rootSchema = rootSchema

            // Store the time at which the query started executing. The SQL
            // standard says that functions such as CURRENT_TIMESTAMP return the
            // same value throughout the query.
            val timeHolder: Holder<Long> = Holder.of(System.currentTimeMillis())

            // Give a hook chance to alter the clock.
            Hook.CURRENT_TIME.run(timeHolder)
            val time: Long = timeHolder.get()
            val timeZone: TimeZone = connection.getTimeZone()
            val localOffset: Long = timeZone.getOffset(time)
            val user = "sa"
            val systemUser: String = System.getProperty("user.name")
            val localeName: String = connection.config().locale()
            val locale: Locale = if (localeName != null) Util.parseLocale(localeName) else Locale.ROOT

            // Give a hook chance to alter standard input, output, error streams.
            val streamHolder: Holder<Array<Object>> = Holder.of(arrayOf<Object>(System.`in`, System.out, System.err))
            Hook.STANDARD_STREAMS.run(streamHolder)
            val builder: ImmutableMap.Builder<Object, Object> = ImmutableMap.builder()
            builder.put(Variable.UTC_TIMESTAMP.camelName, time)
                .put(Variable.CURRENT_TIMESTAMP.camelName, time + localOffset)
                .put(Variable.LOCAL_TIMESTAMP.camelName, time + localOffset)
                .put(Variable.TIME_ZONE.camelName, timeZone)
                .put(Variable.USER.camelName, user)
                .put(Variable.SYSTEM_USER.camelName, systemUser)
                .put(Variable.LOCALE.camelName, locale)
                .put(Variable.STDIN.camelName, streamHolder.get().get(0))
                .put(Variable.STDOUT.camelName, streamHolder.get().get(1))
                .put(Variable.STDERR.camelName, streamHolder.get().get(2))
            for (entry in parameters.entrySet()) {
                var e: Object = entry.getValue()
                if (e == null) {
                    e = AvaticaSite.DUMMY_VALUE
                }
                builder.put(entry.getKey(), e)
            }
            map = builder.build()
        }

        @Override
        @Nullable
        @Synchronized
        operator fun get(name: String?): Object? {
            val o: Object = map.get(name)
            if (o === AvaticaSite.DUMMY_VALUE) {
                return null
            }
            return if (o == null && Variable.SQL_ADVISOR.camelName.equals(name)) {
                sqlAdvisor
            } else o
        }

        // This duplicates org.apache.calcite.prepare.CalcitePrepareImpl.prepare2_
        private val sqlAdvisor: SqlAdvisor
            private get() {
                val con = queryProvider as CalciteConnectionImpl
                val schemaName: String?
                try {
                    schemaName = con.getSchema()
                } catch (e: SQLException) {
                    throw RuntimeException(e)
                }
                val schemaPath: List<String> =
                    if (schemaName == null) ImmutableList.of() else ImmutableList.of(schemaName)
                val validator: SqlValidatorWithHints = SqlAdvisorValidator(
                    SqlStdOperatorTable.instance(),
                    CalciteCatalogReader(
                        requireNonNull(rootSchema, "rootSchema"),
                        schemaPath, typeFactory, con.config()
                    ),
                    typeFactory, SqlValidator.Config.DEFAULT
                )
                val config: CalciteConnectionConfig = con.config()
                // This duplicates org.apache.calcite.prepare.CalcitePrepareImpl.prepare2_
                val parserConfig: SqlParser.Config = SqlParser.config()
                    .withQuotedCasing(config.quotedCasing())
                    .withUnquotedCasing(config.unquotedCasing())
                    .withQuoting(config.quoting())
                    .withConformance(config.conformance())
                    .withCaseSensitive(config.caseSensitive())
                return SqlAdvisor(validator, parserConfig)
            }

        @Override
        @Nullable
        fun getRootSchema(): SchemaPlus? {
            return if (rootSchema == null) null else rootSchema.plus()
        }

        @Override
        fun getTypeFactory(): JavaTypeFactory? {
            return typeFactory
        }

        @Override
        fun getQueryProvider(): QueryProvider {
            return queryProvider
        }
    }

    /** Implementation of Context.  */
    internal class ContextImpl(connection: CalciteConnectionImpl) : CalcitePrepare.Context {
        private val connection: CalciteConnectionImpl
        private override val mutableRootSchema: CalciteSchema
        private override val rootSchema: CalciteSchema

        init {
            this.connection = requireNonNull(connection, "connection")
            val now: Long = System.currentTimeMillis()
            val schemaVersion: SchemaVersion = LongSchemaVersion(now)
            mutableRootSchema = connection.rootSchema
            rootSchema = mutableRootSchema.createSnapshot(schemaVersion)
        }

        @Override
        fun getTypeFactory(): JavaTypeFactory? {
            return connection.typeFactory
        }

        @Override
        fun getRootSchema(): CalciteSchema {
            return rootSchema
        }

        @Override
        fun getMutableRootSchema(): CalciteSchema {
            return mutableRootSchema
        }

        @get:Override
        override val defaultSchemaPath: List<String?>?
            get() {
                val schemaName: String?
                try {
                    schemaName = connection.getSchema()
                } catch (e: SQLException) {
                    throw RuntimeException(e)
                }
                return if (schemaName == null) ImmutableList.of() else ImmutableList.of(schemaName)
            }

        @get:Nullable
        @get:Override
        override val objectPath: List<String>?
            get() = null

        @Override
        override fun config(): CalciteConnectionConfig {
            return connection.config()
        }

        @get:Override
        override val dataContext: DataContext
            get() = connection.createDataContext(
                ImmutableMap.of(),
                rootSchema
            )

        @get:Override
        override val relRunner: RelRunner
            get() {
                val runner: RelRunner?
                try {
                    runner = connection.unwrap(RelRunner::class.java)
                } catch (e: SQLException) {
                    throw RuntimeException(e)
                }
                if (runner == null) {
                    throw UnsupportedOperationException()
                }
                return runner
            }

        @Override
        override fun spark(): CalcitePrepare.SparkHandler? {
            val enable: Boolean = config().spark()
            return CalcitePrepare.Dummy.getSparkHandler(enable)
        }
    }

    /** Implementation of [CalciteServerStatement].  */
    internal class CalciteServerStatementImpl(connection: CalciteConnectionImpl?) : CalciteServerStatement {
        private val connection: CalciteConnectionImpl

        @Nullable
        private var iterator: Iterator<Object>? = null

        @get:Override
        @set:Override
        var signature: @Nullable Meta.Signature? = null
        val cancelFlag: AtomicBoolean = AtomicBoolean()

        init {
            this.connection = requireNonNull(connection, "connection")
        }

        @Override
        fun createPrepareContext(): Context {
            return connection.createPrepareContext()
        }

        @Override
        fun getConnection(): CalciteConnection {
            return connection
        }

        @get:Nullable
        @get:Override
        @set:Override
        var resultSet: Iterator<Any>?
            get() = iterator
            set(iterator) {
                this.iterator = iterator
            }
    }

    companion object {
        // must be package-protected
        val TROJAN: Trojan = createTrojan()
    }
}
