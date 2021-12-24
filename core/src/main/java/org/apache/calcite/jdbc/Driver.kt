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

import org.apache.calcite.adapter.java.JavaTypeFactory

/**
 * Calcite JDBC driver.
 */
class Driver @SuppressWarnings("method.invocation.invalid") constructor() : UnregisteredDriver() {
    val prepareFactory: Function0<CalcitePrepare>

    init {
        prepareFactory = createPrepareFactory()
    }

    protected fun createPrepareFactory(): Function0<CalcitePrepare> {
        return CalcitePrepare.DEFAULT_FACTORY
    }

    @Override
    protected fun getFactoryClassName(jdbcVersion: JdbcVersion): String {
        return when (jdbcVersion) {
            JDBC_30, JDBC_40 -> throw IllegalArgumentException(
                "JDBC version not supported: "
                        + jdbcVersion
            )
            JDBC_41 -> "org.apache.calcite.jdbc.CalciteJdbc41Factory"
            else -> "org.apache.calcite.jdbc.CalciteJdbc41Factory"
        }
    }

    @Override
    protected fun createDriverVersion(): DriverVersion {
        return CalciteDriverVersion.INSTANCE
    }

    @Override
    protected fun createHandler(): Handler {
        return object : HandlerImpl() {
            @Override
            @Throws(SQLException::class)
            fun onConnectionInit(connection_: AvaticaConnection) {
                val connection: CalciteConnectionImpl = connection_
                super.onConnectionInit(connection)
                val model = model(connection)
                if (model != null) {
                    try {
                        ModelHandler(connection, model)
                    } catch (e: IOException) {
                        throw SQLException(e)
                    }
                }
                connection.init()
            }

            @Nullable
            fun model(connection: CalciteConnectionImpl): String? {
                val model: String = connection.config().model()
                if (model != null) {
                    return model
                }
                var schemaFactory: SchemaFactory = connection.config().schemaFactory(SchemaFactory::class.java, null)
                val info: Properties = connection.getProperties()
                val schemaName: String = Util.first(connection.config().schema(), "adhoc")
                if (schemaFactory == null) {
                    val schemaType: JsonSchema.Type = connection.config().schemaType()
                    if (schemaType != null) {
                        when (schemaType) {
                            JDBC -> schemaFactory = JdbcSchema.Factory.INSTANCE
                            MAP -> schemaFactory = AbstractSchema.Factory.INSTANCE
                            else -> {}
                        }
                    }
                }
                if (schemaFactory != null) {
                    val json = JsonBuilder()
                    val root: Map<String, Object> = json.map()
                    root.put("version", "1.0")
                    root.put("defaultSchema", schemaName)
                    val schemaList: List<Object> = json.list()
                    root.put("schemas", schemaList)
                    val schema: Map<String, Object> = json.map()
                    schemaList.add(schema)
                    schema.put("type", "custom")
                    schema.put("name", schemaName)
                    schema.put("factory", schemaFactory.getClass().getName())
                    val operandMap: Map<String, Object> = json.map()
                    schema.put("operand", operandMap)
                    for (entry in Util.toMap(info).entrySet()) {
                        if (entry.getKey().startsWith("schema.")) {
                            operandMap.put(
                                entry.getKey().substring("schema.".length()),
                                entry.getValue()
                            )
                        }
                    }
                    return "inline:" + json.toJsonString(root)
                }
                return null
            }
        }
    }

    @get:Override
    protected val connectionProperties: Collection<Any>
        protected get() {
            val list: List<ConnectionProperty> = ArrayList()
            Collections.addAll(list, BuiltInConnectionProperty.values())
            Collections.addAll(list, CalciteConnectionProperty.values())
            return list
        }

    @Override
    fun createMeta(connection: AvaticaConnection?): Meta {
        return CalciteMetaImpl(connection as CalciteConnectionImpl?)
    }

    /** Creates an internal connection.  */
    fun connect(
        rootSchema: CalciteSchema?,
        @Nullable typeFactory: JavaTypeFactory?
    ): CalciteConnection? {
        return (factory as CalciteFactory)
            .newConnection(
                this, factory, connectStringPrefix, Properties(),
                rootSchema, typeFactory
            )
    }

    /** Creates an internal connection.  */
    fun connect(
        rootSchema: CalciteSchema?,
        @Nullable typeFactory: JavaTypeFactory?, properties: Properties?
    ): CalciteConnection? {
        return (factory as CalciteFactory)
            .newConnection(
                this, factory, connectStringPrefix, properties,
                rootSchema, typeFactory
            )
    }

    companion object {
        @get:Override
        protected val connectStringPrefix = "jdbc:calcite:"
            protected get() = Companion.field

        init {
            Driver().register()
        }
    }
}
