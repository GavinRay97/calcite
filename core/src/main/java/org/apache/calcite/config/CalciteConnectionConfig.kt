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

import org.apache.calcite.avatica.ConnectionConfig
import org.apache.calcite.avatica.util.Casing
import org.apache.calcite.avatica.util.Quoting
import org.apache.calcite.model.JsonSchema
import org.apache.calcite.sql.validate.SqlConformance
import org.checkerframework.checker.nullness.qual.PolyNull
import java.util.Properties

/** Interface for reading connection properties within Calcite code. There is
 * a method for every property. At some point there will be similar config
 * classes for system and statement properties.  */
interface CalciteConnectionConfig : ConnectionConfig {
    /** Returns the value of
     * [CalciteConnectionProperty.APPROXIMATE_DISTINCT_COUNT].  */
    fun approximateDistinctCount(): Boolean

    /** Returns the value of
     * [CalciteConnectionProperty.APPROXIMATE_TOP_N].  */
    fun approximateTopN(): Boolean

    /** Returns the value of
     * [CalciteConnectionProperty.APPROXIMATE_DECIMAL].  */
    fun approximateDecimal(): Boolean

    /** Returns the value of
     * [CalciteConnectionProperty.NULL_EQUAL_TO_EMPTY].  */
    fun nullEqualToEmpty(): Boolean

    /** Returns the value of
     * [CalciteConnectionProperty.AUTO_TEMP].  */
    fun autoTemp(): Boolean

    /** Returns the value of
     * [CalciteConnectionProperty.MATERIALIZATIONS_ENABLED].  */
    fun materializationsEnabled(): Boolean

    /** Returns the value of
     * [CalciteConnectionProperty.CREATE_MATERIALIZATIONS].  */
    fun createMaterializations(): Boolean

    /** Returns the value of
     * [CalciteConnectionProperty.DEFAULT_NULL_COLLATION].  */
    fun defaultNullCollation(): NullCollation?

    /** Returns the value of [CalciteConnectionProperty.FUN],
     * or a default operator table if not set. If `defaultOperatorTable`
     * is not null, the result is never null.  */
    fun <T> `fun`(
        operatorTableClass: Class<T>?,
        @PolyNull defaultOperatorTable: T
    ): @PolyNull T?

    /** Returns the value of [CalciteConnectionProperty.MODEL].  */
    @Nullable
    fun model(): String?

    /** Returns the value of [CalciteConnectionProperty.LEX].  */
    fun lex(): Lex?

    /** Returns the value of [CalciteConnectionProperty.QUOTING].  */
    fun quoting(): Quoting?

    /** Returns the value of [CalciteConnectionProperty.UNQUOTED_CASING].  */
    fun unquotedCasing(): Casing?

    /** Returns the value of [CalciteConnectionProperty.QUOTED_CASING].  */
    fun quotedCasing(): Casing?

    /** Returns the value of [CalciteConnectionProperty.CASE_SENSITIVE].  */
    fun caseSensitive(): Boolean

    /** Returns the value of [CalciteConnectionProperty.PARSER_FACTORY],
     * or a default parser if not set. If `defaultParserFactory`
     * is not null, the result is never null.  */
    fun <T> parserFactory(
        parserFactoryClass: Class<T>?,
        @PolyNull defaultParserFactory: T
    ): @PolyNull T?

    /** Returns the value of [CalciteConnectionProperty.SCHEMA_FACTORY],
     * or a default schema factory if not set. If `defaultSchemaFactory`
     * is not null, the result is never null.  */
    fun <T> schemaFactory(
        schemaFactoryClass: Class<T>?,
        @PolyNull defaultSchemaFactory: T
    ): @PolyNull T?

    /** Returns the value of [CalciteConnectionProperty.SCHEMA_TYPE].  */
    fun schemaType(): JsonSchema.Type?

    /** Returns the value of [CalciteConnectionProperty.SPARK].  */
    fun spark(): Boolean

    /** Returns the value of
     * [CalciteConnectionProperty.FORCE_DECORRELATE].  */
    fun forceDecorrelate(): Boolean

    /** Returns the value of [CalciteConnectionProperty.TYPE_SYSTEM],
     * or a default type system if not set. If `defaultTypeSystem`
     * is not null, the result is never null.  */
    fun <T> typeSystem(
        typeSystemClass: Class<T>?,
        @PolyNull defaultTypeSystem: T
    ): @PolyNull T?

    /** Returns the value of [CalciteConnectionProperty.CONFORMANCE].  */
    fun conformance(): SqlConformance?

    /** Returns the value of [CalciteConnectionProperty.TIME_ZONE].  */
    @Override
    fun timeZone(): String?

    /** Returns the value of [CalciteConnectionProperty.LOCALE].  */
    fun locale(): String?

    /** Returns the value of [CalciteConnectionProperty.TYPE_COERCION].  */
    fun typeCoercion(): Boolean

    /** Returns the value of
     * [CalciteConnectionProperty.LENIENT_OPERATOR_LOOKUP].  */
    fun lenientOperatorLookup(): Boolean

    /** Returns the value of [CalciteConnectionProperty.TOPDOWN_OPT].  */
    fun topDownOpt(): Boolean

    companion object {
        /** Default configuration.  */
        val DEFAULT: CalciteConnectionConfigImpl = CalciteConnectionConfigImpl(Properties())
    }
}
