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
package org.apache.calcite.model

import org.apache.calcite.adapter.jdbc.JdbcSchema
import org.apache.calcite.avatica.AvaticaUtils
import org.apache.calcite.jdbc.CalciteConnection
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.materialize.Lattice
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.schema.AggregateFunction
import org.apache.calcite.schema.Function
import org.apache.calcite.schema.ScalarFunction
import org.apache.calcite.schema.Schema
import org.apache.calcite.schema.SchemaFactory
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.schema.Table
import org.apache.calcite.schema.TableFactory
import org.apache.calcite.schema.TableFunction
import org.apache.calcite.schema.TableMacro
import org.apache.calcite.schema.impl.AbstractSchema
import org.apache.calcite.schema.impl.AggregateFunctionImpl
import org.apache.calcite.schema.impl.MaterializedViewTable
import org.apache.calcite.schema.impl.ScalarFunctionImpl
import org.apache.calcite.schema.impl.TableFunctionImpl
import org.apache.calcite.schema.impl.TableMacroImpl
import org.apache.calcite.schema.impl.ViewTable
import org.apache.calcite.sql.SqlDialectFactory
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.util.Pair
import org.apache.calcite.util.Util
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import java.io.File
import java.io.IOException
import java.sql.SQLException
import java.util.ArrayDeque
import java.util.Collections
import java.util.Deque
import java.util.List
import java.util.Locale
import java.util.Map
import javax.sql.DataSource
import java.util.Objects.requireNonNull

/**
 * Reads a model and creates schema objects accordingly.
 */
class ModelHandler @SuppressWarnings("method.invocation.invalid") constructor(
    connection: CalciteConnection,
    uri: String
) {
    private val connection: CalciteConnection
    private val schemaStack: Deque<Pair<out String?, SchemaPlus>> = ArrayDeque()
    private val modelUri: String
    var latticeBuilder: @Nullable Lattice.Builder? = null
    var tileBuilder: @Nullable Lattice.TileBuilder? = null

    init {
        this.connection = connection
        modelUri = uri
        val root: JsonRoot
        val mapper: ObjectMapper
        if (uri.startsWith("inline:")) {
            // trim here is to correctly autodetect if it is json or not in case of leading spaces
            val inline: String = uri.substring("inline:".length()).trim()
            mapper = if (inline.startsWith("/*") || inline.startsWith("{")) JSON_MAPPER else YAML_MAPPER
            root = mapper.readValue(inline, JsonRoot::class.java)
        } else {
            mapper = if (uri.endsWith(".yaml") || uri.endsWith(".yml")) YAML_MAPPER else JSON_MAPPER
            root = mapper.readValue(File(uri), JsonRoot::class.java)
        }
        visit(root)
    }

    fun visit(jsonRoot: JsonRoot) {
        val pair: Pair<String, SchemaPlus> = Pair.of(null, connection.getRootSchema())
        schemaStack.push(pair)
        for (schema in jsonRoot.schemas) {
            schema.accept(this)
        }
        val p: Pair<out String?, SchemaPlus> = schemaStack.pop()
        assert(p === pair)
        if (jsonRoot.defaultSchema != null) {
            try {
                connection.setSchema(jsonRoot.defaultSchema)
            } catch (e: SQLException) {
                throw RuntimeException(e)
            }
        }
    }

    fun visit(jsonSchema: JsonMapSchema) {
        val parentSchema: SchemaPlus = currentMutableSchema("schema")
        val schema: SchemaPlus = parentSchema.add(jsonSchema.name, AbstractSchema())
        if (jsonSchema.path != null) {
            schema.setPath(stringListList(jsonSchema.path))
        }
        populateSchema(jsonSchema, schema)
    }

    private fun populateSchema(jsonSchema: JsonSchema, schema: SchemaPlus) {
        if (jsonSchema.cache != null) {
            schema.setCacheEnabled(jsonSchema.cache)
        }
        val pair: Pair<String, SchemaPlus> = Pair.of(jsonSchema.name, schema)
        schemaStack.push(pair)
        jsonSchema.visitChildren(this)
        val p: Pair<out String?, SchemaPlus> = schemaStack.pop()
        assert(p === pair)
    }

    fun visit(jsonSchema: JsonCustomSchema) {
        try {
            val parentSchema: SchemaPlus = currentMutableSchema("sub-schema")
            val schemaFactory: SchemaFactory = AvaticaUtils.instantiatePlugin(
                SchemaFactory::class.java,
                jsonSchema.factory
            )
            val schema: Schema = schemaFactory.create(
                parentSchema, jsonSchema.name, operandMap(jsonSchema, jsonSchema.operand)
            )
            val schemaPlus: SchemaPlus = parentSchema.add(jsonSchema.name, schema)
            populateSchema(jsonSchema, schemaPlus)
        } catch (e: Exception) {
            throw RuntimeException("Error instantiating $jsonSchema", e)
        }
    }

    /** Adds extra entries to an operand to a custom schema.  */
    protected fun operandMap(
        @Nullable jsonSchema: JsonSchema?,
        @Nullable operand: Map<String?, Object?>?
    ): Map<String, Object> {
        if (operand == null) {
            return ImmutableMap.of()
        }
        val builder: ImmutableMap.Builder<String, Object> = ImmutableMap.builder()
        builder.putAll(operand)
        for (extraOperand in ExtraOperand.values()) {
            if (!operand.containsKey(extraOperand.camelName)) {
                when (extraOperand) {
                    ExtraOperand.MODEL_URI -> builder.put(extraOperand.camelName, modelUri)
                    ExtraOperand.BASE_DIRECTORY -> {
                        var f: File? = null
                        if (!modelUri.startsWith("inline:")) {
                            val file = File(modelUri)
                            f = file.getParentFile()
                        }
                        if (f == null) {
                            f = File("")
                        }
                        builder.put(extraOperand.camelName, f)
                    }
                    ExtraOperand.TABLES -> if (jsonSchema is JsonCustomSchema) {
                        builder.put(
                            extraOperand.camelName,
                            jsonSchema.tables
                        )
                    }
                    else -> {}
                }
            }
        }
        return builder.build()
    }

    fun visit(jsonSchema: JsonJdbcSchema) {
        val parentSchema: SchemaPlus = currentMutableSchema("jdbc schema")
        val dataSource: DataSource = JdbcSchema.dataSource(
            jsonSchema.jdbcUrl,
            jsonSchema.jdbcDriver,
            jsonSchema.jdbcUser,
            jsonSchema.jdbcPassword
        )
        val schema: JdbcSchema
        schema = if (jsonSchema.sqlDialectFactory == null || jsonSchema.sqlDialectFactory.isEmpty()) {
            JdbcSchema.create(
                parentSchema, jsonSchema.name, dataSource,
                jsonSchema.jdbcCatalog, jsonSchema.jdbcSchema
            )
        } else {
            val factory: SqlDialectFactory = AvaticaUtils.instantiatePlugin(
                SqlDialectFactory::class.java, jsonSchema.sqlDialectFactory
            )
            JdbcSchema.create(
                parentSchema, jsonSchema.name, dataSource,
                factory, jsonSchema.jdbcCatalog, jsonSchema.jdbcSchema
            )
        }
        val schemaPlus: SchemaPlus = parentSchema.add(jsonSchema.name, schema)
        populateSchema(jsonSchema, schemaPlus)
    }

    fun visit(jsonMaterialization: JsonMaterialization) {
        try {
            val schema: SchemaPlus = currentSchema()
            if (!schema.isMutable()) {
                throw RuntimeException(
                    "Cannot define materialization; parent schema '"
                            + currentSchemaName()
                            + "' is not a SemiMutableSchema"
                )
            }
            val calciteSchema: CalciteSchema = CalciteSchema.from(schema)
            val viewName: String
            val existing: Boolean
            if (jsonMaterialization.view == null) {
                // If the user did not supply a view name, that means the materialized
                // view is pre-populated. Generate a synthetic view name.
                viewName = "$" + schema.getTableNames().size()
                existing = true
            } else {
                viewName = jsonMaterialization.view
                existing = false
            }
            val viewPath: List<String> = calciteSchema.path(viewName)
            schema.add(
                viewName,
                MaterializedViewTable.create(
                    calciteSchema,
                    jsonMaterialization.getSql(), jsonMaterialization.viewSchemaPath, viewPath,
                    jsonMaterialization.table, existing
                )
            )
        } catch (e: Exception) {
            throw RuntimeException(
                "Error instantiating $jsonMaterialization",
                e
            )
        }
    }

    fun visit(jsonLattice: JsonLattice) {
        try {
            val schema: SchemaPlus = currentSchema()
            if (!schema.isMutable()) {
                throw RuntimeException(
                    "Cannot define lattice; parent schema '"
                            + currentSchemaName()
                            + "' is not a SemiMutableSchema"
                )
            }
            val calciteSchema: CalciteSchema = CalciteSchema.from(schema)
            val latticeBuilder: Lattice.Builder = Lattice.builder(calciteSchema, jsonLattice.getSql())
                .auto(jsonLattice.auto)
                .algorithm(jsonLattice.algorithm)
            if (jsonLattice.rowCountEstimate != null) {
                latticeBuilder.rowCountEstimate(jsonLattice.rowCountEstimate)
            }
            if (jsonLattice.statisticProvider != null) {
                latticeBuilder.statisticProvider(jsonLattice.statisticProvider)
            }
            populateLattice(jsonLattice, latticeBuilder)
            schema.add(jsonLattice.name, latticeBuilder.build())
        } catch (e: Exception) {
            throw RuntimeException("Error instantiating $jsonLattice", e)
        }
    }

    private fun populateLattice(
        jsonLattice: JsonLattice,
        latticeBuilder: Lattice.Builder
    ) {
        assert(this.latticeBuilder == null)
        this.latticeBuilder = latticeBuilder
        jsonLattice.visitChildren(this)
        this.latticeBuilder = null
    }

    fun visit(jsonTable: JsonCustomTable) {
        try {
            val schema: SchemaPlus = currentMutableSchema("table")
            val tableFactory: TableFactory = AvaticaUtils.instantiatePlugin(
                TableFactory::class.java,
                jsonTable.factory
            )
            val table: Table = tableFactory.create(
                schema, jsonTable.name,
                operandMap(null, jsonTable.operand), null
            )
            for (column in jsonTable.columns) {
                column.accept(this)
            }
            schema.add(jsonTable.name, table)
        } catch (e: Exception) {
            throw RuntimeException("Error instantiating $jsonTable", e)
        }
    }

    fun visit(jsonColumn: JsonColumn?) {}
    fun visit(jsonView: JsonView) {
        try {
            val schema: SchemaPlus = currentMutableSchema("view")
            val path: List<String> = Util.first(jsonView.path, currentSchemaPath())
            val viewPath: List<String> = ImmutableList.< String > builder < String ? > ().addAll(path)
                .add(jsonView.name).build()
            schema.add(
                jsonView.name,
                ViewTable.viewMacro(
                    schema, jsonView.getSql(), path, viewPath,
                    jsonView.modifiable
                )
            )
        } catch (e: Exception) {
            throw RuntimeException("Error instantiating $jsonView", e)
        }
    }

    private fun currentSchemaPath(): List<String> {
        return Collections.singletonList(currentSchemaName())
    }

    private fun nameAndSchema(): Pair<out String?, SchemaPlus> {
        return schemaStack.getFirst()
    }

    private fun currentSchema(): SchemaPlus {
        return nameAndSchema().right
    }

    private fun currentSchemaName(): String {
        return requireNonNull(nameAndSchema().left, "currentSchema.name")
    }

    private fun currentMutableSchema(elementType: String): SchemaPlus {
        val schema: SchemaPlus = currentSchema()
        if (!schema.isMutable()) {
            throw RuntimeException(
                "Cannot define " + elementType
                        + "; parent schema '" + schema.getName() + "' is not mutable"
            )
        }
        return schema
    }

    fun visit(jsonType: JsonType) {
        try {
            val schema: SchemaPlus = currentMutableSchema("type")
            schema.add(jsonType.name) { typeFactory ->
                if (jsonType.type != null) {
                    return@add typeFactory.createSqlType(
                        requireNonNull(
                            SqlTypeName.get(jsonType.type)
                        ) { "SqlTypeName.get for " + jsonType.type })
                } else {
                    val builder: RelDataTypeFactory.Builder = typeFactory.builder()
                    for (jsonTypeAttribute in jsonType.attributes) {
                        val typeName: SqlTypeName = requireNonNull(
                            SqlTypeName.get(jsonTypeAttribute.type)
                        ) { "SqlTypeName.get for " + jsonTypeAttribute.type }
                        var type: RelDataType = typeFactory.createSqlType(typeName)
                        if (type == null) {
                            type = requireNonNull(
                                currentSchema().getType(jsonTypeAttribute.type)
                            ) {
                                ("type " + jsonTypeAttribute.type.toString() + " is not found in schema "
                                        + currentSchemaName())
                            }
                                .apply(typeFactory)
                        }
                        builder.add(jsonTypeAttribute.name, type)
                    }
                    return@add builder.build()
                }
            }
        } catch (e: Exception) {
            throw RuntimeException("Error instantiating $jsonType", e)
        }
    }

    fun visit(jsonFunction: JsonFunction) {
        // "name" is not required - a class can have several functions
        try {
            val schema: SchemaPlus = currentMutableSchema("function")
            val path: List<String> = Util.first(jsonFunction.path, currentSchemaPath())
            addFunctions(
                schema, jsonFunction.name, path, jsonFunction.className,
                jsonFunction.methodName, false
            )
        } catch (e: Exception) {
            throw RuntimeException("Error instantiating $jsonFunction", e)
        }
    }

    fun visit(jsonMeasure: JsonMeasure) {
        assert(latticeBuilder != null)
        val distinct = false // no distinct field in JsonMeasure.yet
        val measure: Lattice.Measure = latticeBuilder.resolveMeasure(
            jsonMeasure.agg, distinct,
            jsonMeasure.args
        )
        if (tileBuilder != null) {
            tileBuilder.addMeasure(measure)
        } else if (latticeBuilder != null) {
            latticeBuilder.addMeasure(measure)
        } else {
            throw AssertionError("nowhere to put measure")
        }
    }

    fun visit(jsonTile: JsonTile) {
        assert(tileBuilder == null)
        tileBuilder = Lattice.Tile.builder()
        val tileBuilder: Lattice.TileBuilder? = tileBuilder
        for (jsonMeasure in jsonTile.measures) {
            jsonMeasure.accept(this)
        }
        val latticeBuilder: Lattice.Builder = requireNonNull(latticeBuilder, "latticeBuilder")
        for (dimension in jsonTile.dimensions) {
            val column: Lattice.Column = latticeBuilder.resolveColumn(dimension)
            tileBuilder.addDimension(column)
        }
        latticeBuilder.addTile(tileBuilder.build())
        this.tileBuilder = null
    }

    /** Extra operands automatically injected into a
     * [JsonCustomSchema.operand], as extra context for the adapter.  */
    enum class ExtraOperand(val camelName: String) {
        /** URI of model, e.g. "target/test-classes/model.json",
         * "http://localhost/foo/bar.json", "inline:{...}",
         * "target/test-classes/model.yaml",
         * "http://localhost/foo/bar.yaml", "inline:..."
         */
        MODEL_URI("modelUri"),

        /** Base directory from which to read files.  */
        BASE_DIRECTORY("baseDirectory"),

        /** Tables defined in this schema.  */
        TABLES("tables");
    }

    companion object {
        private val JSON_MAPPER: ObjectMapper = ObjectMapper()
            .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
            .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
            .configure(JsonParser.Feature.ALLOW_COMMENTS, true)
        private val YAML_MAPPER: ObjectMapper = YAMLMapper()
        // CHECKSTYLE: IGNORE 1

        @Deprecated
        @Deprecated("Use {@link #addFunctions}. ")
        fun create(
            schema: SchemaPlus, functionName: String?,
            path: List<String?>?, className: String, methodName: String?
        ) {
            addFunctions(schema, functionName, path, className, methodName, false)
        }

        /** Creates and validates a [ScalarFunctionImpl], and adds it to a
         * schema. If `methodName` is "*", may add more than one function.
         *
         * @param schema Schema to add to
         * @param functionName Name of function; null to derived from method name
         * @param path Path to look for functions
         * @param className Class to inspect for methods that may be user-defined
         * functions
         * @param methodName Method name;
         * null means use the class as a UDF;
         * "*" means add all methods
         * @param upCase Whether to convert method names to upper case, so that they
         * can be called without using quotes
         */
        fun addFunctions(
            schema: SchemaPlus, @Nullable functionName: String?,
            path: List<String?>?, className: String, @Nullable methodName: String?, upCase: Boolean
        ) {
            val clazz: Class<*>
            clazz = try {
                Class.forName(className)
            } catch (e: ClassNotFoundException) {
                throw RuntimeException(
                    "UDF class '"
                            + className + "' not found"
                )
            }
            val methodNameOrDefault: String = Util.first(methodName, "eval")
            var actualFunctionName: String
            actualFunctionName = functionName ?: methodNameOrDefault
            if (upCase) {
                actualFunctionName = actualFunctionName.toUpperCase(Locale.ROOT)
            }
            val tableFunction: TableFunction = TableFunctionImpl.create(clazz, methodNameOrDefault)
            if (tableFunction != null) {
                schema.add(
                    Util.first(functionName, methodNameOrDefault),
                    tableFunction
                )
                return
            }
            // Must look for TableMacro before ScalarFunction. Both have an "eval"
            // method.
            val macro: TableMacro = TableMacroImpl.create(clazz)
            if (macro != null) {
                schema.add(actualFunctionName, macro)
                return
            }
            if (methodName != null && methodName.equals("*")) {
                for (entry in ScalarFunctionImpl.functions(clazz).entries()) {
                    var name: String = entry.getKey()
                    if (upCase) {
                        name = name.toUpperCase(Locale.ROOT)
                    }
                    schema.add(name, entry.getValue())
                }
                return
            } else {
                val function: ScalarFunction = ScalarFunctionImpl.create(clazz, methodNameOrDefault)
                if (function != null) {
                    schema.add(actualFunctionName, function)
                    return
                }
            }
            if (methodName == null) {
                val aggFunction: AggregateFunction = AggregateFunctionImpl.create(clazz)
                if (aggFunction != null) {
                    schema.add(actualFunctionName, aggFunction)
                    return
                }
            }
            throw RuntimeException(
                "Not a valid function class: " + clazz
                        + ". Scalar functions and table macros have an 'eval' method; "
                        + "aggregate functions have 'init' and 'add' methods, and optionally "
                        + "'initAdd', 'merge' and 'result' methods."
            )
        }

        private fun stringListList(
            path: List
        ): ImmutableList<ImmutableList<String>> {
            val builder: ImmutableList.Builder<ImmutableList<String>> = ImmutableList.builder()
            for (s in path) {
                builder.add(stringList(s))
            }
            return builder.build()
        }

        private fun stringList(s: Object): ImmutableList<String> {
            return if (s is String) {
                ImmutableList.of(s as String)
            } else if (s is List) {
                val builder2: ImmutableList.Builder<String> = ImmutableList.builder()
                for (o in s) {
                    if (o is String) {
                        builder2.add(o)
                    } else {
                        throw RuntimeException(
                            "Invalid path element " + o
                                    + "; was expecting string"
                        )
                    }
                }
                builder2.build()
            } else {
                throw RuntimeException(
                    "Invalid path element " + s
                            + "; was expecting string or list of string"
                )
            }
        }
    }
}
