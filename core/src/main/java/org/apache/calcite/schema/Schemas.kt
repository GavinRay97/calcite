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
package org.apache.calcite.schema

import org.apache.calcite.DataContext

/**
 * Utility functions for schemas.
 */
class Schemas private constructor() {
    init {
        throw AssertionError("no instances!")
    }

    /** Implementation of [Path].  */
    class PathImpl internal constructor(pairs: ImmutableList<Pair<String?, Schema?>?>) :
        AbstractList<Pair<String?, Schema?>?>(), Path {
        private val pairs: ImmutableList<Pair<String, Schema>>

        init {
            this.pairs = pairs
        }

        @Override
        override fun equals(@Nullable o: Object): Boolean {
            return (this === o
                    || o is PathImpl
                    && pairs.equals((o as PathImpl).pairs))
        }

        @Override
        override fun hashCode(): Int {
            return pairs.hashCode()
        }

        @Override
        override operator fun get(index: Int): Pair<String, Schema> {
            return pairs.get(index)
        }

        @Override
        fun size(): Int {
            return pairs.size()
        }

        @Override
        override fun parent(): Path {
            if (pairs.isEmpty()) {
                throw IllegalArgumentException("at root")
            }
            return PathImpl(pairs.subList(0, pairs.size() - 1))
        }

        @Override
        override fun names(): List<String> {
            return object : AbstractList<String?>() {
                @Override
                operator fun get(index: Int): String {
                    return pairs.get(index + 1).left
                }

                @Override
                fun size(): Int {
                    return pairs.size() - 1
                }
            }
        }

        @Override
        override fun schemas(): List<Schema> {
            return Pair.right(pairs)
        }

        companion object {
            val EMPTY = PathImpl(ImmutableList.of())
        }
    }

    companion object {
        fun resolve(
            typeFactory: RelDataTypeFactory,
            name: String,
            functionEntries: Collection<CalciteSchema.FunctionEntry?>,
            argumentTypes: List<RelDataType>
        ): @Nullable CalciteSchema.FunctionEntry? {
            val matches: List<CalciteSchema.FunctionEntry> = ArrayList()
            for (entry in functionEntries) {
                if (matches(typeFactory, entry.getFunction(), argumentTypes)) {
                    matches.add(entry)
                }
            }
            return when (matches.size()) {
                0 -> null
                1 -> matches[0]
                else -> throw RuntimeException(
                    "More than one match for " + name
                            + " with arguments " + argumentTypes
                )
            }
        }

        private fun matches(
            typeFactory: RelDataTypeFactory,
            member: Function, argumentTypes: List<RelDataType>
        ): Boolean {
            val parameters: List<FunctionParameter> = member.getParameters()
            if (parameters.size() !== argumentTypes.size()) {
                return false
            }
            for (i in 0 until argumentTypes.size()) {
                val argumentType: RelDataType = argumentTypes[i]
                val parameter: FunctionParameter = parameters[i]
                if (!canConvert(argumentType, parameter.getType(typeFactory))) {
                    return false
                }
            }
            return true
        }

        private fun canConvert(fromType: RelDataType, toType: RelDataType): Boolean {
            return SqlTypeUtil.canAssignFrom(toType, fromType)
        }

        /** Returns the expression for a schema.  */
        fun expression(schema: SchemaPlus): Expression {
            return schema.getExpression(schema.getParentSchema(), schema.getName())
        }

        /** Returns the expression for a sub-schema.  */
        fun subSchemaExpression(
            schema: SchemaPlus, name: String?,
            type: Class?
        ): Expression {
            // (Type) schemaExpression.getSubSchema("name")
            val schemaExpression: Expression = expression(schema)
            val call: Expression = Expressions.call(
                schemaExpression,
                BuiltInMethod.SCHEMA_GET_SUB_SCHEMA.method,
                Expressions.constant(name)
            )
            //CHECKSTYLE: IGNORE 2
            return if (false && type != null && !type.isAssignableFrom(Schema::class.java)) {
                unwrap(call, type)
            } else call
        }

        /** Converts a schema expression to a given type by calling the
         * [SchemaPlus.unwrap] method.  */
        fun unwrap(call: Expression?, type: Class?): Expression {
            return Expressions.convert_(
                Expressions.call(
                    call, BuiltInMethod.SCHEMA_PLUS_UNWRAP.method,
                    Expressions.constant(type)
                ),
                type
            )
        }

        /** Returns the expression to access a table within a schema.  */
        fun tableExpression(
            schema: SchemaPlus, elementType: Type?,
            tableName: String?, clazz: Class?
        ): Expression {
            val expression: MethodCallExpression
            if (Table::class.java.isAssignableFrom(clazz)) {
                expression = Expressions.call(
                    expression(schema),
                    BuiltInMethod.SCHEMA_GET_TABLE.method,
                    Expressions.constant(tableName)
                )
                if (ScannableTable::class.java.isAssignableFrom(clazz)) {
                    return Expressions.call(
                        BuiltInMethod.SCHEMAS_ENUMERABLE_SCANNABLE.method,
                        Expressions.convert_(expression, ScannableTable::class.java),
                        DataContext.ROOT
                    )
                }
                if (FilterableTable::class.java.isAssignableFrom(clazz)) {
                    return Expressions.call(
                        BuiltInMethod.SCHEMAS_ENUMERABLE_FILTERABLE.method,
                        Expressions.convert_(expression, FilterableTable::class.java),
                        DataContext.ROOT
                    )
                }
                if (ProjectableFilterableTable::class.java.isAssignableFrom(clazz)) {
                    return Expressions.call(
                        BuiltInMethod.SCHEMAS_ENUMERABLE_PROJECTABLE_FILTERABLE.method,
                        Expressions.convert_(expression, ProjectableFilterableTable::class.java),
                        DataContext.ROOT
                    )
                }
            } else {
                expression = Expressions.call(
                    BuiltInMethod.SCHEMAS_QUERYABLE.method,
                    DataContext.ROOT,
                    expression(schema),
                    Expressions.constant(elementType),
                    Expressions.constant(tableName)
                )
            }
            return EnumUtils.convert(expression, clazz)
        }

        fun createDataContext(
            connection: Connection?, @Nullable rootSchema: SchemaPlus?
        ): DataContext {
            return DataContexts.of(connection as CalciteConnection?, rootSchema)
        }

        /** Returns a [Queryable], given a fully-qualified table name.  */
        fun <E> queryable(
            root: DataContext?, clazz: Class<E>?,
            vararg names: String?
        ): Queryable<E> {
            return queryable(root, clazz, Arrays.asList(names))
        }

        /** Returns a [Queryable], given a fully-qualified table name as an
         * iterable.  */
        fun <E> queryable(
            root: DataContext, clazz: Class<E>?,
            names: Iterable<String?>
        ): Queryable<E> {
            var schema: SchemaPlus = root.getRootSchema()
            val iterator = names.iterator()
            while (true) {
                val name = iterator.next()
                requireNonNull(schema, "schema")
                schema = if (iterator.hasNext()) {
                    val next: SchemaPlus = schema.getSubSchema(name)
                        ?: throw IllegalArgumentException("schema $name is not found in $schema")
                    next
                } else {
                    return queryable<Any>(root, schema, clazz, name)
                }
            }
        }

        /** Returns a [Queryable], given a schema and table name.  */
        fun <E> queryable(
            root: DataContext, schema: SchemaPlus,
            clazz: Class<E>?, tableName: String?
        ): Queryable<E> {
            val table: QueryableTable = requireNonNull(
                schema.getTable(tableName)
            ) { "table $tableName is not found in $schema" } as QueryableTable
            val queryProvider: QueryProvider = root.getQueryProvider()
            return table.asQueryable(queryProvider, schema, tableName)
        }

        /** Returns an [org.apache.calcite.linq4j.Enumerable] over the rows of
         * a given table, representing each row as an object array.  */
        fun enumerable(
            table: ScannableTable,
            root: DataContext?
        ): Enumerable<Array<Object>> {
            return table.scan(root)
        }

        /** Returns an [org.apache.calcite.linq4j.Enumerable] over the rows of
         * a given table, not applying any filters, representing each row as an object
         * array.  */
        fun enumerable(
            table: FilterableTable,
            root: DataContext?
        ): Enumerable<Array<Object>> {
            return table.scan(root, ArrayList())
        }

        /** Returns an [org.apache.calcite.linq4j.Enumerable] over the rows of
         * a given table, not applying any filters and projecting all columns,
         * representing each row as an object array.  */
        fun enumerable(
            table: ProjectableFilterableTable, root: DataContext
        ): Enumerable<Array<Object>> {
            val typeFactory: JavaTypeFactory = root.getTypeFactory()
            return table.scan(
                root, ArrayList(),
                identity(table.getRowType(typeFactory).getFieldCount())
            )
        }

        private fun identity(count: Int): IntArray {
            val integers = IntArray(count)
            for (i in integers.indices) {
                integers[i] = i
            }
            return integers
        }

        /** Returns an [org.apache.calcite.linq4j.Enumerable] over object
         * arrays, given a fully-qualified table name which leads to a
         * [ScannableTable].  */
        @Nullable
        fun table(root: DataContext, vararg names: String?): Table {
            var schema: SchemaPlus = root.getRootSchema()
            val nameList: List<String> = Arrays.asList(names)
            val iterator: Iterator<String?> = nameList.iterator()
            while (true) {
                val name = iterator.next()
                requireNonNull(schema, "schema")
                schema = if (iterator.hasNext()) {
                    val next: SchemaPlus = schema.getSubSchema(name)
                        ?: throw IllegalArgumentException("schema $name is not found in $schema")
                    next
                } else {
                    return schema.getTable(name)!!
                }
            }
        }

        /** Parses and validates a SQL query. For use within Calcite only.  */
        fun parse(
            connection: CalciteConnection?, schema: CalciteSchema,
            @Nullable schemaPath: List<String>, sql: String?
        ): ParseResult {
            val prepare: CalcitePrepare = CalcitePrepare.DEFAULT_FACTORY.apply()
            val propValues: ImmutableMap<CalciteConnectionProperty, String> = ImmutableMap.of()
            val context: CalcitePrepare.Context = makeContext(connection, schema, schemaPath, null, propValues)
            CalcitePrepare.Dummy.push(context)
            return try {
                prepare.parse(context, sql)
            } finally {
                CalcitePrepare.Dummy.pop(context)
            }
        }

        /** Parses and validates a SQL query and converts to relational algebra. For
         * use within Calcite only.  */
        fun convert(
            connection: CalciteConnection?, schema: CalciteSchema,
            schemaPath: List<String>, sql: String?
        ): CalcitePrepare.ConvertResult {
            val prepare: CalcitePrepare = CalcitePrepare.DEFAULT_FACTORY.apply()
            val propValues: ImmutableMap<CalciteConnectionProperty, String> = ImmutableMap.of()
            val context: CalcitePrepare.Context = makeContext(connection, schema, schemaPath, null, propValues)
            CalcitePrepare.Dummy.push(context)
            return try {
                prepare.convert(context, sql)
            } finally {
                CalcitePrepare.Dummy.pop(context)
            }
        }

        /** Analyzes a view. For use within Calcite only.  */
        fun analyzeView(
            connection: CalciteConnection?, schema: CalciteSchema,
            @Nullable schemaPath: List<String>, viewSql: String?,
            @Nullable viewPath: List<String>?, fail: Boolean
        ): CalcitePrepare.AnalyzeViewResult {
            val prepare: CalcitePrepare = CalcitePrepare.DEFAULT_FACTORY.apply()
            val propValues: ImmutableMap<CalciteConnectionProperty, String> = ImmutableMap.of()
            val context: CalcitePrepare.Context = makeContext(connection, schema, schemaPath, viewPath, propValues)
            CalcitePrepare.Dummy.push(context)
            return try {
                prepare.analyzeView(context, viewSql, fail)
            } finally {
                CalcitePrepare.Dummy.pop(context)
            }
        }

        /** Prepares a SQL query for execution. For use within Calcite only.  */
        fun prepare(
            connection: CalciteConnection?, schema: CalciteSchema,
            @Nullable schemaPath: List<String>, sql: String?,
            map: ImmutableMap<CalciteConnectionProperty, String>
        ): CalcitePrepare.CalciteSignature<Object> {
            val prepare: CalcitePrepare = CalcitePrepare.DEFAULT_FACTORY.apply()
            val context: CalcitePrepare.Context = makeContext(connection, schema, schemaPath, null, map)
            CalcitePrepare.Dummy.push(context)
            return try {
                prepare.prepareSql(
                    context, CalcitePrepare.Query.of(sql),
                    Array<Object>::class.java, -1
                )
            } finally {
                CalcitePrepare.Dummy.pop(context)
            }
        }

        /**
         * Creates a context for the purposes of preparing a statement.
         *
         * @param connection Connection
         * @param schema Schema
         * @param schemaPath Path wherein to look for functions
         * @param objectPath Path of the object being analyzed (usually a view),
         * or null
         * @param propValues Connection properties
         * @return Context
         */
        private fun makeContext(
            connection: CalciteConnection?, schema: CalciteSchema,
            @Nullable schemaPath: List<String>, @Nullable objectPath: List<String>?,
            propValues: ImmutableMap<CalciteConnectionProperty, String>
        ): CalcitePrepare.Context {
            return if (connection == null) {
                val context0: CalcitePrepare.Context = CalcitePrepare.Dummy.peek()
                val config: CalciteConnectionConfig =
                    mutate(context0.config(), propValues)
                makeContext(
                    config, context0.getTypeFactory(),
                    context0.getDataContext(), schema, schemaPath, objectPath
                )
            } else {
                val config: CalciteConnectionConfig =
                    mutate(connection.config(), propValues)
                makeContext(
                    config, connection.getTypeFactory(),
                    DataContexts.of(connection, schema.root().plus()), schema,
                    schemaPath, objectPath
                )
            }
        }

        private fun mutate(
            config: CalciteConnectionConfig,
            propValues: ImmutableMap<CalciteConnectionProperty, String>
        ): CalciteConnectionConfig {
            var config: CalciteConnectionConfig = config
            for (e in propValues.entrySet()) {
                config = (config as CalciteConnectionConfigImpl).set(e.getKey(), e.getValue())
            }
            return config
        }

        private fun makeContext(
            connectionConfig: CalciteConnectionConfig,
            typeFactory: JavaTypeFactory,
            dataContext: DataContext,
            schema: CalciteSchema,
            @Nullable schemaPath: List<String>?, @Nullable objectPath_: List<String>?
        ): CalcitePrepare.Context {
            @Nullable val objectPath: ImmutableList<String>? =
                if (objectPath_ == null) null else ImmutableList.copyOf(objectPath_)
            return object : Context() {
                @get:Override
                val typeFactory: JavaTypeFactory
                    get() = typeFactory

                @get:Override
                val rootSchema: CalciteSchema
                    get() = schema.root()

                @get:Override
                val mutableRootSchema: CalciteSchema
                    get() = rootSchema

                // schemaPath is usually null. If specified, it overrides schema
                // as the context within which the SQL is validated.
                @get:Override
                val defaultSchemaPath: List<String>
                    get() =// schemaPath is usually null. If specified, it overrides schema
                        // as the context within which the SQL is validated.
                        schemaPath ?: schema.path(null)

                @get:Nullable
                @get:Override
                val objectPath: List<String>?
                    get() = objectPath

                @Override
                fun config(): CalciteConnectionConfig {
                    return connectionConfig
                }

                @get:Override
                val dataContext: DataContext
                    get() = dataContext

                @get:Override
                val relRunner: RelRunner
                    get() {
                        throw UnsupportedOperationException()
                    }

                @Override
                fun spark(): CalcitePrepare.SparkHandler {
                    val enable: Boolean = config().spark()
                    return CalcitePrepare.Dummy.getSparkHandler(enable)
                }
            }
        }

        /** Returns an implementation of
         * [RelProtoDataType]
         * that asks a given table for its row type with a given type factory.  */
        fun proto(table: Table?): RelProtoDataType {
            return table::getRowType
        }

        /** Returns an implementation of [RelProtoDataType]
         * that asks a given scalar function for its return type with a given type
         * factory.  */
        fun proto(function: ScalarFunction?): RelProtoDataType {
            return function::getReturnType
        }

        /** Returns the star tables defined in a schema.
         *
         * @param schema Schema
         */
        fun getStarTables(
            schema: CalciteSchema
        ): List<CalciteSchema.TableEntry> {
            val list: List<CalciteSchema.LatticeEntry> = getLatticeEntries(schema)
            return Util.transform(list) { entry ->
                val starTable: CalciteSchema.TableEntry = requireNonNull(entry, "entry").getStarTable()
                assert(
                    starTable.getTable().getJdbcTableType()
                            === Schema.TableType.STAR
                )
                entry.getStarTable()
            }
        }

        /** Returns the lattices defined in a schema.
         *
         * @param schema Schema
         */
        fun getLattices(schema: CalciteSchema): List<Lattice> {
            val list: List<CalciteSchema.LatticeEntry> = getLatticeEntries(schema)
            return Util.transform(list, LatticeEntry::getLattice)
        }

        /** Returns the lattices defined in a schema.
         *
         * @param schema Schema
         */
        fun getLatticeEntries(
            schema: CalciteSchema
        ): List<CalciteSchema.LatticeEntry> {
            val list: List<LatticeEntry> = ArrayList()
            gatherLattices(schema, list)
            return list
        }

        private fun gatherLattices(
            schema: CalciteSchema,
            list: List<CalciteSchema.LatticeEntry>
        ) {
            list.addAll(schema.getLatticeMap().values())
            for (subSchema in schema.getSubSchemaMap().values()) {
                gatherLattices(subSchema, list)
            }
        }

        /** Returns a sub-schema of a given schema obtained by following a sequence
         * of names.
         *
         *
         * The result is null if the initial schema is null or any sub-schema does
         * not exist.
         */
        @Nullable
        fun subSchema(
            schema: CalciteSchema?,
            names: Iterable<String?>
        ): CalciteSchema? {
            @Nullable var current: CalciteSchema? = schema
            for (string in names) {
                if (current == null) {
                    return null
                }
                current = current.getSubSchema(string, false)
            }
            return current
        }

        /** Generates a table name that is unique within the given schema.  */
        fun uniqueTableName(schema: CalciteSchema, base: String): String {
            var t: String = requireNonNull(base, "base")
            var x = 0
            while (schema.getTable(t, true) != null) {
                t = base + x
                x++
            }
            return t
        }

        /** Creates a path with a given list of names starting from a given root
         * schema.  */
        fun path(rootSchema: CalciteSchema, names: Iterable<String>): Path {
            val builder: ImmutableList.Builder<Pair<String, Schema>> = ImmutableList.builder()
            var schema: Schema = rootSchema.plus()
            val iterator = names.iterator()
            if (!iterator.hasNext()) {
                return PathImpl.EMPTY
            }
            if (!rootSchema.name.isEmpty()) {
                // If path starts with the name of the root schema, ignore the first step
                // in the path.
                Preconditions.checkState(rootSchema.name.equals(iterator.next()))
            }
            while (true) {
                val name = iterator.next()
                builder.add(Pair.of(name, schema))
                if (!iterator.hasNext()) {
                    return path(builder.build())
                }
                val next: Schema = schema.getSubSchema(name)
                    ?: throw IllegalArgumentException("schema $name is not found in $schema")
                schema = next
            }
        }

        fun path(build: ImmutableList<Pair<String?, Schema?>?>): PathImpl {
            return PathImpl(build)
        }

        /** Returns the path to get to a schema from its root.  */
        fun path(schema: SchemaPlus?): Path {
            val list: List<Pair<String, Schema>> = ArrayList()
            var s: SchemaPlus? = schema
            while (s != null) {
                list.add(Pair.of(s.getName(), s))
                s = s.getParentSchema()
            }
            return PathImpl(ImmutableList.copyOf(Lists.reverse(list)))
        }
    }
}
