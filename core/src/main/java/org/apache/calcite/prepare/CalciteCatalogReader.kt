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
package org.apache.calcite.prepare

import org.apache.calcite.config.CalciteConnectionConfig

/**
 * Implementation of [org.apache.calcite.prepare.Prepare.CatalogReader]
 * and also [org.apache.calcite.sql.SqlOperatorTable] based on tables and
 * functions defined schemas.
 */
class CalciteCatalogReader protected constructor(
    rootSchema: CalciteSchema?,
    nameMatcher: SqlNameMatcher, schemaPaths: List<List<String?>?>?,
    typeFactory: RelDataTypeFactory?, config: CalciteConnectionConfig?
) : CatalogReader {
    val rootSchema: CalciteSchema
    protected val typeFactory: RelDataTypeFactory?

    @get:Override
    val schemaPaths: List<List<String>>
    protected val nameMatcher: SqlNameMatcher
    protected val config: CalciteConnectionConfig?

    constructor(
        rootSchema: CalciteSchema?,
        defaultSchema: List<String?>?, typeFactory: RelDataTypeFactory?, config: CalciteConnectionConfig?
    ) : this(
        rootSchema, SqlNameMatchers.withCaseSensitive(config != null && config.caseSensitive()),
        ImmutableList.of(
            Objects.requireNonNull(defaultSchema, "defaultSchema"),
            ImmutableList.of()
        ),
        typeFactory, config
    ) {
    }

    init {
        this.rootSchema = Objects.requireNonNull(rootSchema, "rootSchema")
        this.nameMatcher = nameMatcher
        this.schemaPaths =
            Util.immutableCopy(if (Util.isDistinct(schemaPaths)) schemaPaths else LinkedHashSet(schemaPaths))
        this.typeFactory = typeFactory
        this.config = config
    }

    @Override
    fun withSchemaPath(schemaPath: List<String?>?): CalciteCatalogReader {
        return CalciteCatalogReader(
            rootSchema, nameMatcher,
            ImmutableList.of(schemaPath, ImmutableList.of()), typeFactory, config
        )
    }

    @Override
    fun getTable(names: List<String?>?): @Nullable Prepare.PreparingTable? {
        // First look in the default schema, if any.
        // If not found, look in the root schema.
        val entry: CalciteSchema.TableEntry = SqlValidatorUtil.getTableEntry(this, names)
        if (entry != null) {
            val table: Table = entry.getTable()
            if (table is Wrapper) {
                val relOptTable: Prepare.PreparingTable = (table as Wrapper).unwrap(Prepare.PreparingTable::class.java)
                if (relOptTable != null) {
                    return relOptTable
                }
            }
            return RelOptTableImpl.create(
                this,
                table.getRowType(typeFactory), entry, null
            )
        }
        return null
    }

    @Override
    fun getConfig(): CalciteConnectionConfig? {
        return config
    }

    private fun getFunctionsFrom(
        names: List<String>
    ): Collection<org.apache.calcite.schema.Function> {
        val functions2: List<org.apache.calcite.schema.Function> = ArrayList()
        val schemaNameList: List<List<String>> = ArrayList()
        if (names.size() > 1) {
            // Name qualified: ignore path. But we do look in "/catalog" and "/",
            // the last 2 items in the path.
            if (schemaPaths.size() > 1) {
                schemaNameList.addAll(Util.skip(schemaPaths))
            } else {
                schemaNameList.addAll(schemaPaths)
            }
        } else {
            for (schemaPath in schemaPaths) {
                val schema: CalciteSchema = SqlValidatorUtil.getSchema(rootSchema, schemaPath, nameMatcher)
                if (schema != null) {
                    schemaNameList.addAll(schema.getPath())
                }
            }
        }
        for (schemaNames in schemaNameList) {
            val schema: CalciteSchema = SqlValidatorUtil.getSchema(
                rootSchema,
                Iterables.concat(schemaNames, Util.skipLast(names)), nameMatcher
            )
            if (schema != null) {
                val name: String = Util.last(names)
                val caseSensitive: Boolean = nameMatcher.isCaseSensitive()
                functions2.addAll(schema.getFunctions(name, caseSensitive))
            }
        }
        return functions2
    }

    @Override
    @Nullable
    fun getNamedType(typeName: SqlIdentifier?): RelDataType? {
        val typeEntry: CalciteSchema.TypeEntry = SqlValidatorUtil.getTypeEntry(getRootSchema(), typeName)
        return if (typeEntry != null) {
            typeEntry.getType().apply(typeFactory)
        } else {
            null
        }
    }

    @Override
    fun getAllSchemaObjectNames(names: List<String?>?): List<SqlMoniker> {
        val schema: CalciteSchema =
            SqlValidatorUtil.getSchema(rootSchema, names, nameMatcher) ?: return ImmutableList.of()
        val result: ImmutableList.Builder<SqlMoniker> = Builder()

        // Add root schema if not anonymous
        if (!schema.name.equals("")) {
            result.add(moniker(schema, null, SqlMonikerType.SCHEMA))
        }
        val schemaMap: Map<String, CalciteSchema> = schema.getSubSchemaMap()
        for (subSchema in schemaMap.keySet()) {
            result.add(moniker(schema, subSchema, SqlMonikerType.SCHEMA))
        }
        for (table in schema.getTableNames()) {
            result.add(moniker(schema, table, SqlMonikerType.TABLE))
        }
        val functions: NavigableSet<String> = schema.getFunctionNames()
        for (function in functions) { // views are here as well
            result.add(moniker(schema, function, SqlMonikerType.FUNCTION))
        }
        return result.build()
    }

    @Override
    fun getTableForMember(names: List<String?>?): @Nullable Prepare.PreparingTable? {
        return getTable(names)
    }

    @SuppressWarnings("deprecation")
    @Override
    @Nullable
    fun field(rowType: RelDataType?, alias: String?): RelDataTypeField {
        return nameMatcher.field(rowType, alias)
    }

    @SuppressWarnings("deprecation")
    @Override
    fun matches(string: String?, name: String?): Boolean {
        return nameMatcher.matches(string, name)
    }

    @Override
    fun createTypeFromProjection(
        type: RelDataType?,
        columnNameList: List<String?>?
    ): RelDataType {
        return SqlValidatorUtil.createTypeFromProjection(
            type, columnNameList,
            typeFactory, nameMatcher.isCaseSensitive()
        )
    }

    @Override
    fun lookupOperatorOverloads(
        opName: SqlIdentifier,
        @Nullable category: SqlFunctionCategory?,
        syntax: SqlSyntax,
        operatorList: List<SqlOperator?>?,
        nameMatcher: SqlNameMatcher?
    ) {
        if (syntax !== SqlSyntax.FUNCTION) {
            return
        }
        val predicate: Predicate<org.apache.calcite.schema.Function>
        if (category == null) {
            predicate = Predicate<org.apache.calcite.schema.Function> { function -> true }
        } else if (category.isTableFunction()) {
            predicate = Predicate<org.apache.calcite.schema.Function> { function ->
                (function is TableMacro
                        || function is TableFunction)
            }
        } else {
            predicate = Predicate<org.apache.calcite.schema.Function> { function ->
                !(function is TableMacro
                        || function is TableFunction)
            }
        }
        getFunctionsFrom(opName.names)
            .stream()
            .filter(predicate)
            .map { function -> toOp(opName, function) }
            .forEachOrdered(operatorList::add)
    }

    @get:Override
    val operatorList: List<Any>
        get() {
            val builder: ImmutableList.Builder<SqlOperator> = ImmutableList.builder()
            for (schemaPath in schemaPaths) {
                val schema: CalciteSchema = SqlValidatorUtil.getSchema(rootSchema, schemaPath, nameMatcher)
                if (schema != null) {
                    for (name in schema.getFunctionNames()) {
                        schema.getFunctions(name, true)
                            .forEach { f -> builder.add(toOp(SqlIdentifier(name, SqlParserPos.ZERO), f)) }
                    }
                }
            }
            return builder.build()
        }

    @Override
    fun getRootSchema(): CalciteSchema {
        return rootSchema
    }

    @Override
    fun getTypeFactory(): RelDataTypeFactory? {
        return typeFactory
    }

    @Override
    fun registerRules(planner: RelOptPlanner?) {
    }

    @get:Override
    @get:SuppressWarnings("deprecation")
    val isCaseSensitive: Boolean
        get() = nameMatcher.isCaseSensitive()

    @Override
    fun nameMatcher(): SqlNameMatcher {
        return nameMatcher
    }

    @Override
    fun <C : Object?> unwrap(aClass: Class<C>): @Nullable C? {
        return if (aClass.isInstance(this)) {
            aClass.cast(this)
        } else null
    }

    companion object {
        private fun moniker(
            schema: CalciteSchema?, @Nullable name: String?,
            type: SqlMonikerType
        ): SqlMonikerImpl {
            var type: SqlMonikerType = type
            val path: List<String> = schema.path(name)
            if (path.size() === 1 && !schema.root().name.equals("")
                && type === SqlMonikerType.SCHEMA
            ) {
                type = SqlMonikerType.CATALOG
            }
            return SqlMonikerImpl(path, type)
        }

        /** Creates an operator table that contains functions in the given class
         * or classes.
         *
         * @see ModelHandler.addFunctions
         */
        fun operatorTable(vararg classNames: String?): SqlOperatorTable {
            // Dummy schema to collect the functions
            val schema: CalciteSchema = CalciteSchema.createRootSchema(false, false)
            for (className in classNames) {
                ModelHandler.addFunctions(
                    schema.plus(), null, ImmutableList.of(),
                    className, "*", true
                )
            }
            val table = ListSqlOperatorTable()
            for (name in schema.getFunctionNames()) {
                schema.getFunctions(name, true).forEach { function ->
                    val id = SqlIdentifier(name, SqlParserPos.ZERO)
                    table.add(toOp(id, function))
                }
            }
            return table
        }

        /** Converts a function to a [org.apache.calcite.sql.SqlOperator].  */
        private fun toOp(
            name: SqlIdentifier,
            function: org.apache.calcite.schema.Function
        ): SqlOperator {
            val argTypesFactory: Function<RelDataTypeFactory, List<RelDataType>> =
                Function<RelDataTypeFactory, List<RelDataType>> { typeFactory ->
                    function.getParameters()
                        .stream()
                        .map { o -> o.getType(typeFactory) }
                        .collect(Util.toImmutableList())
                }
            val typeFamiliesFactory: Function<RelDataTypeFactory, List<SqlTypeFamily>> =
                Function<RelDataTypeFactory, List<SqlTypeFamily>> { typeFactory ->
                    argTypesFactory.apply(typeFactory)
                        .stream()
                        .map { type ->
                            Util.first(
                                type.getSqlTypeName().getFamily(),
                                SqlTypeFamily.ANY
                            )
                        }
                        .collect(Util.toImmutableList())
                }
            val paramTypesFactory: Function<RelDataTypeFactory, List<RelDataType>> =
                Function<RelDataTypeFactory, List<RelDataType>> { typeFactory ->
                    argTypesFactory.apply(typeFactory)
                        .stream()
                        .map { type -> toSql(typeFactory, type) }
                        .collect(Util.toImmutableList())
                }

            // Use a short-lived type factory to populate "typeFamilies" and "argTypes".
            // SqlOperandMetadata.paramTypes will use the real type factory, during
            // validation.
            val dummyTypeFactory: RelDataTypeFactory = JavaTypeFactoryImpl()
            val argTypes: List<RelDataType> = argTypesFactory.apply(dummyTypeFactory)
            val typeFamilies: List<SqlTypeFamily> = typeFamiliesFactory.apply(dummyTypeFactory)
            val operandTypeInference: SqlOperandTypeInference = InferTypes.explicit(argTypes)
            val operandMetadata: SqlOperandMetadata = OperandTypes.operandMetadata(typeFamilies, paramTypesFactory,
                { i -> function.getParameters().get(i).getName() }
            ) { i -> function.getParameters().get(i).isOptional() }
            val kind: SqlKind = kind(function)
            return if (function is ScalarFunction) {
                val returnTypeInference: SqlReturnTypeInference = infer(function as ScalarFunction)
                SqlUserDefinedFunction(
                    name, kind, returnTypeInference,
                    operandTypeInference, operandMetadata, function
                )
            } else if (function is AggregateFunction) {
                val returnTypeInference: SqlReturnTypeInference = infer(function as AggregateFunction)
                SqlUserDefinedAggFunction(
                    name, kind,
                    returnTypeInference, operandTypeInference,
                    operandMetadata, function as AggregateFunction, false, false,
                    Optionality.FORBIDDEN
                )
            } else if (function is TableMacro) {
                SqlUserDefinedTableMacro(
                    name, kind, ReturnTypes.CURSOR,
                    operandTypeInference, operandMetadata, function as TableMacro
                )
            } else if (function is TableFunction) {
                SqlUserDefinedTableFunction(
                    name, kind, ReturnTypes.CURSOR,
                    operandTypeInference, operandMetadata, function as TableFunction
                )
            } else {
                throw AssertionError("unknown function type $function")
            }
        }

        /** Deduces the [org.apache.calcite.sql.SqlKind] of a user-defined
         * function based on a [Hints] annotation, if present.  */
        private fun kind(function: org.apache.calcite.schema.Function): SqlKind {
            if (function is ScalarFunctionImpl) {
                val hints: Hints = (function as ScalarFunctionImpl).method.getAnnotation(Hints::class.java)
                if (hints != null) {
                    for (hint in hints.value()) {
                        if (hint.startsWith("SqlKind:")) {
                            return SqlKind.valueOf(hint.substring("SqlKind:".length()))
                        }
                    }
                }
            }
            return SqlKind.OTHER_FUNCTION
        }

        private fun infer(function: ScalarFunction): SqlReturnTypeInference {
            return SqlReturnTypeInference { opBinding ->
                val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
                val type: RelDataType
                type = if (function is ScalarFunctionImpl) {
                    (function as ScalarFunctionImpl).getReturnType(
                        typeFactory,
                        opBinding
                    )
                } else {
                    function.getReturnType(typeFactory)
                }
                toSql(typeFactory, type)
            }
        }

        private fun infer(
            function: AggregateFunction
        ): SqlReturnTypeInference {
            return SqlReturnTypeInference { opBinding ->
                val typeFactory: RelDataTypeFactory = opBinding.getTypeFactory()
                val type: RelDataType = function.getReturnType(typeFactory)
                toSql(typeFactory, type)
            }
        }

        private fun toSql(
            typeFactory: RelDataTypeFactory,
            type: RelDataType
        ): RelDataType {
            return if (type is RelDataTypeFactoryImpl.JavaType
                && (type as RelDataTypeFactoryImpl.JavaType).getJavaClass()
                === Object::class.java
            ) {
                typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.ANY), true
                )
            } else JavaTypeFactoryImpl.toSql(typeFactory, type)
        }
    }
}
