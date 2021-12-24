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

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.tree.Expression
import org.apache.calcite.linq4j.tree.Expressions
import org.apache.calcite.schema.Schema
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.Schemas
import org.apache.calcite.schema.impl.AbstractSchema
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.SqlDialectFactory
import org.apache.calcite.sql.SqlDialectFactoryImpl
import org.apache.calcite.util.BuiltInMethod
import com.google.common.base.Supplier
import com.google.common.base.Suppliers
import com.google.common.collect.ImmutableMap
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.util.Map
import javax.sql.DataSource
import java.util.Objects.requireNonNull

/**
 * Schema based upon a JDBC catalog (database).
 *
 *
 * This schema does not directly contain tables, but contains a sub-schema
 * for each schema in the catalog in the back-end. Each of those sub-schemas is
 * an instance of [JdbcSchema].
 *
 *
 * This schema is lazy: it does not compute the list of schema names until
 * the first call to [.getSubSchemaMap]. Then it creates a
 * [JdbcSchema] for each schema name. Each JdbcSchema will populate its
 * tables on demand.
 */
class JdbcCatalogSchema(
    dataSource: DataSource?, dialect: SqlDialect?,
    convention: JdbcConvention?, catalog: String
) : AbstractSchema() {
    val dataSource: DataSource
    val dialect: SqlDialect
    val convention: JdbcConvention
    val catalog: String

    /** Sub-schemas by name, lazily initialized.  */
    @SuppressWarnings("method.invocation.invalid")
    val subSchemaMapSupplier: Supplier<SubSchemaMap> = Suppliers.memoize { computeSubSchemaMap() }

    /** Creates a JdbcCatalogSchema.  */
    init {
        this.dataSource = requireNonNull(dataSource, "dataSource")
        this.dialect = requireNonNull(dialect, "dialect")
        this.convention = requireNonNull(convention, "convention")
        this.catalog = catalog
    }

    private fun computeSubSchemaMap(): SubSchemaMap {
        val builder: ImmutableMap.Builder<String, Schema> = ImmutableMap.builder()
        var defaultSchemaName: String
        try {
            dataSource.getConnection().use { connection ->
                connection.getMetaData().getSchemas(catalog, null).use { resultSet ->
                    defaultSchemaName = connection.getSchema()
                    while (resultSet.next()) {
                        val schemaName: String = requireNonNull(
                            resultSet.getString(1)
                        ) { "got null schemaName from the database" }
                        builder.put(
                            schemaName,
                            JdbcSchema(dataSource, dialect, convention, catalog, schemaName)
                        )
                    }
                }
            }
        } catch (e: SQLException) {
            throw RuntimeException(e)
        }
        return SubSchemaMap(defaultSchemaName, builder.build())
    }

    @get:Override
    protected val subSchemaMap: Map<String, Any>
        protected get() = subSchemaMapSupplier.get().map

    /** Returns the name of the default sub-schema.  */
    val defaultSubSchemaName: String
        get() = subSchemaMapSupplier.get().defaultSchemaName

    /** Returns the data source.  */
    fun getDataSource(): DataSource {
        return dataSource
    }

    /** Contains sub-schemas by name, and the name of the default schema.  */
    class SubSchemaMap(
        val defaultSchemaName: String,
        map: ImmutableMap<String, Schema>
    ) {
        val map: ImmutableMap<String, Schema>

        init {
            this.map = map
        }
    }

    companion object {
        fun create(
            @Nullable parentSchema: SchemaPlus?,
            name: String,
            dataSource: DataSource?,
            catalog: String
        ): JdbcCatalogSchema {
            return create(
                parentSchema, name, dataSource,
                SqlDialectFactoryImpl.INSTANCE, catalog
            )
        }

        fun create(
            @Nullable parentSchema: SchemaPlus?,
            name: String,
            dataSource: DataSource?,
            dialectFactory: SqlDialectFactory?,
            catalog: String
        ): JdbcCatalogSchema {
            val expression: Expression = if (parentSchema != null) Schemas.subSchemaExpression(
                parentSchema, name,
                JdbcCatalogSchema::class.java
            ) else Expressions.call(
                DataContext.ROOT,
                BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method
            )
            val dialect: SqlDialect = JdbcSchema.createDialect(dialectFactory, dataSource)
            val convention: JdbcConvention = JdbcConvention.of(dialect, expression, name)
            return JdbcCatalogSchema(dataSource, dialect, convention, catalog)
        }
    }
}
