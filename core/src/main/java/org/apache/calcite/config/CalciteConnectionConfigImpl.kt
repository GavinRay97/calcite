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
package org.apache.calcite.config

import org.apache.calcite.avatica.ConnectionConfigImpl
import org.apache.calcite.avatica.util.Casing
import org.apache.calcite.avatica.util.Quoting
import org.apache.calcite.model.JsonSchema
import org.apache.calcite.runtime.ConsList
import org.apache.calcite.sql.SqlOperatorTable
import org.apache.calcite.sql.`fun`.SqlLibrary
import org.apache.calcite.sql.`fun`.SqlLibraryOperatorTableFactory
import org.apache.calcite.sql.validate.SqlConformance
import org.apache.calcite.sql.validate.SqlConformanceEnum
import org.checkerframework.checker.nullness.qual.PolyNull
import java.util.List
import java.util.Properties

/** Implementation of [CalciteConnectionConfig].  */
class CalciteConnectionConfigImpl(properties: Properties?) : ConnectionConfigImpl(properties), CalciteConnectionConfig {
    /** Returns a copy of this configuration with one property changed.
     *
     *
     * Does not modify this configuration.  */
    operator fun set(
        property: CalciteConnectionProperty,
        value: String?
    ): CalciteConnectionConfigImpl {
        val newProperties: Properties = properties.clone() as Properties
        newProperties.setProperty(property.camelName(), value)
        return CalciteConnectionConfigImpl(newProperties)
    }

    /** Returns a copy of this configuration with the value of a property
     * removed.
     *
     *
     * Does not modify this configuration.  */
    fun unset(property: CalciteConnectionProperty): CalciteConnectionConfigImpl {
        val newProperties: Properties = properties.clone() as Properties
        newProperties.remove(property.camelName())
        return CalciteConnectionConfigImpl(newProperties)
    }

    /** Returns whether a given property has been assigned a value.
     *
     *
     * If not, the value returned for the property will be its default value.
     */
    fun isSet(property: CalciteConnectionProperty): Boolean {
        return properties.containsKey(property.camelName())
    }

    @Override
    override fun approximateDistinctCount(): Boolean {
        return CalciteConnectionProperty.APPROXIMATE_DISTINCT_COUNT.wrap(properties)
            .getBoolean()
    }

    @Override
    override fun approximateTopN(): Boolean {
        return CalciteConnectionProperty.APPROXIMATE_TOP_N.wrap(properties)
            .getBoolean()
    }

    @Override
    override fun approximateDecimal(): Boolean {
        return CalciteConnectionProperty.APPROXIMATE_DECIMAL.wrap(properties)
            .getBoolean()
    }

    @Override
    override fun nullEqualToEmpty(): Boolean {
        return CalciteConnectionProperty.NULL_EQUAL_TO_EMPTY.wrap(properties).getBoolean()
    }

    @Override
    override fun autoTemp(): Boolean {
        return CalciteConnectionProperty.AUTO_TEMP.wrap(properties).getBoolean()
    }

    @Override
    override fun materializationsEnabled(): Boolean {
        return CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.wrap(properties)
            .getBoolean()
    }

    @Override
    override fun createMaterializations(): Boolean {
        return CalciteConnectionProperty.CREATE_MATERIALIZATIONS.wrap(properties)
            .getBoolean()
    }

    @Override
    override fun defaultNullCollation(): NullCollation {
        return CalciteConnectionProperty.DEFAULT_NULL_COLLATION.wrap(properties)
            .getEnum(NullCollation::class.java, NullCollation.HIGH)
    }

    @Override
    override fun <T> `fun`(
        operatorTableClass: Class<T>,
        @PolyNull defaultOperatorTable: T
    ): @PolyNull T? {
        val `fun`: String = CalciteConnectionProperty.FUN.wrap(properties).getString()
        if (`fun` == null || `fun`.equals("") || `fun`.equals("standard")) {
            return defaultOperatorTable
        }
        val libraryList: List<SqlLibrary> = SqlLibrary.parse(`fun`)
        val operatorTable: SqlOperatorTable = SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
            ConsList.of(SqlLibrary.STANDARD, libraryList)
        )
        return operatorTableClass.cast(operatorTable)
    }

    @Override
    @Nullable
    override fun model(): String {
        return CalciteConnectionProperty.MODEL.wrap(properties).getString()
    }

    @Override
    override fun lex(): Lex {
        return CalciteConnectionProperty.LEX.wrap(properties).getEnum(Lex::class.java)
    }

    @Override
    override fun quoting(): Quoting {
        return CalciteConnectionProperty.QUOTING.wrap(properties)
            .getEnum(Quoting::class.java, lex().quoting)
    }

    @Override
    override fun unquotedCasing(): Casing {
        return CalciteConnectionProperty.UNQUOTED_CASING.wrap(properties)
            .getEnum(Casing::class.java, lex().unquotedCasing)
    }

    @Override
    override fun quotedCasing(): Casing {
        return CalciteConnectionProperty.QUOTED_CASING.wrap(properties)
            .getEnum(Casing::class.java, lex().quotedCasing)
    }

    @Override
    override fun caseSensitive(): Boolean {
        return CalciteConnectionProperty.CASE_SENSITIVE.wrap(properties)
            .getBoolean(lex().caseSensitive)
    }

    @Override
    override fun <T> parserFactory(
        parserFactoryClass: Class<T>?,
        @PolyNull defaultParserFactory: T
    ): @PolyNull T? {
        return CalciteConnectionProperty.PARSER_FACTORY.wrap(properties)
            .getPlugin(parserFactoryClass, defaultParserFactory)
    }

    @Override
    override fun <T> schemaFactory(
        schemaFactoryClass: Class<T>?,
        @PolyNull defaultSchemaFactory: T
    ): @PolyNull T? {
        return CalciteConnectionProperty.SCHEMA_FACTORY.wrap(properties)
            .getPlugin(schemaFactoryClass, defaultSchemaFactory)
    }

    @Override
    override fun schemaType(): JsonSchema.Type {
        return CalciteConnectionProperty.SCHEMA_TYPE.wrap(properties)
            .getEnum(JsonSchema.Type::class.java)
    }

    @Override
    override fun spark(): Boolean {
        return CalciteConnectionProperty.SPARK.wrap(properties).getBoolean()
    }

    @Override
    override fun forceDecorrelate(): Boolean {
        return CalciteConnectionProperty.FORCE_DECORRELATE.wrap(properties)
            .getBoolean()
    }

    @Override
    override fun <T> typeSystem(
        typeSystemClass: Class<T>?,
        @PolyNull defaultTypeSystem: T
    ): @PolyNull T? {
        return CalciteConnectionProperty.TYPE_SYSTEM.wrap(properties)
            .getPlugin(typeSystemClass, defaultTypeSystem)
    }

    @Override
    override fun conformance(): SqlConformance {
        return CalciteConnectionProperty.CONFORMANCE.wrap(properties)
            .getEnum(SqlConformanceEnum::class.java)
    }

    @Override
    override fun timeZone(): String {
        return CalciteConnectionProperty.TIME_ZONE.wrap(properties)
            .getString()
    }

    @Override
    override fun locale(): String {
        return CalciteConnectionProperty.LOCALE.wrap(properties)
            .getString()
    }

    @Override
    override fun typeCoercion(): Boolean {
        return CalciteConnectionProperty.TYPE_COERCION.wrap(properties)
            .getBoolean()
    }

    @Override
    override fun lenientOperatorLookup(): Boolean {
        return CalciteConnectionProperty.LENIENT_OPERATOR_LOOKUP.wrap(properties)
            .getBoolean()
    }

    @Override
    override fun topDownOpt(): Boolean {
        return CalciteConnectionProperty.TOPDOWN_OPT.wrap(properties).getBoolean()
    }
}
