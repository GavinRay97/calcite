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
package org.apache.calcite.sql.type

import com.google.common.collect.ImmutableMap

/**
 * Rules that determine whether a type is castable from another type.
 *
 *
 * These rules specify the conversion matrix with explicit CAST.
 *
 *
 * The implicit type coercion matrix should be a sub-set of this explicit one.
 * We do not define an implicit type coercion matrix, instead we have specific
 * coercion rules for all kinds of SQL contexts which actually define the "matrix".
 *
 *
 * To add a new implementation to this class, follow
 * these steps:
 *
 *
 *  1. Initialize a [SqlTypeMappingRules.Builder] instance
 * with default mappings of [SqlTypeCoercionRule.INSTANCE].
 *  1. Modify the mappings with the Builder.
 *  1. Construct a new [SqlTypeCoercionRule] instance with method
 * [.instance].
 *  1. Set the [SqlTypeCoercionRule] instance into the
 * [org.apache.calcite.sql.validate.SqlValidator].
 *
 *
 *
 * The code snippet below illustrates how to implement a customized instance.
 *
 * <pre>
 * // Initialize a Builder instance with the default mappings.
 * Builder builder = SqlTypeMappingRules.builder();
 * builder.addAll(SqlTypeCoercionRules.instance().getTypeMapping());
 *
 * // Do the tweak, for example, if we want to add a rule to allow
 * // coerce BOOLEAN to TIMESTAMP.
 * builder.add(SqlTypeName.TIMESTAMP,
 * builder.copyValues(SqlTypeName.TIMESTAMP)
 * .add(SqlTypeName.BOOLEAN).build());
 *
 * // Initialize a SqlTypeCoercionRules with the new builder mappings.
 * SqlTypeCoercionRules typeCoercionRules = SqlTypeCoercionRules.instance(builder.map);
 *
 * // Set the SqlTypeCoercionRules instance into the SqlValidator.
 * SqlValidator.Config validatorConf ...;
 * validatorConf.withTypeCoercionRules(typeCoercionRules);
 * // Use this conf to initialize the SqlValidator.
</pre> *
 */
class SqlTypeCoercionRule private constructor(map: Map<SqlTypeName, ImmutableSet<SqlTypeName>>) : SqlTypeMappingRule {
    //~ Instance fields --------------------------------------------------------
    private val map: Map<SqlTypeName, ImmutableSet<SqlTypeName>>
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a `SqlTypeCoercionRules` with specified type mappings `map`.
     *
     *
     * Make this constructor private intentionally, use [.instance].
     *
     * @param map The type mapping, for each map entry, the values types can be coerced to
     * the key type
     */
    init {
        this.map = ImmutableMap.copyOf(map)
    }

    @get:Override
    override val typeMapping: Map<org.apache.calcite.sql.type.SqlTypeName?, Any?>?
        get() = map

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        private val INSTANCE: SqlTypeCoercionRule? = null
        val THREAD_PROVIDERS: ThreadLocal<SqlTypeCoercionRule> = ThreadLocal.withInitial { INSTANCE }

        init {
            // We use coerceRules when we're casting
            val coerceRules: SqlTypeMappingRules.Builder = SqlTypeMappingRules.builder()
            org.apache.calcite.sql.type.coerceRules.addAll(SqlTypeAssignmentRule.instance().getTypeMapping())
            val rule: Set<SqlTypeName> = HashSet()

            // Make numbers symmetrical,
            // and make VARCHAR, CHAR, BOOLEAN and TIMESTAMP castable to/from numbers
            org.apache.calcite.sql.type.rule.add(SqlTypeName.TINYINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.SMALLINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.INTEGER)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.BIGINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.DECIMAL)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.FLOAT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.REAL)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.DOUBLE)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.CHAR)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.VARCHAR)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.BOOLEAN)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.TIMESTAMP)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
            org.apache.calcite.sql.type.coerceRules.add(SqlTypeName.TINYINT, org.apache.calcite.sql.type.rule)
            org.apache.calcite.sql.type.coerceRules.add(SqlTypeName.SMALLINT, org.apache.calcite.sql.type.rule)
            org.apache.calcite.sql.type.coerceRules.add(SqlTypeName.INTEGER, org.apache.calcite.sql.type.rule)
            org.apache.calcite.sql.type.coerceRules.add(SqlTypeName.BIGINT, org.apache.calcite.sql.type.rule)
            org.apache.calcite.sql.type.coerceRules.add(SqlTypeName.FLOAT, org.apache.calcite.sql.type.rule)
            org.apache.calcite.sql.type.coerceRules.add(SqlTypeName.REAL, org.apache.calcite.sql.type.rule)
            org.apache.calcite.sql.type.coerceRules.add(SqlTypeName.DECIMAL, org.apache.calcite.sql.type.rule)
            org.apache.calcite.sql.type.coerceRules.add(SqlTypeName.DOUBLE, org.apache.calcite.sql.type.rule)
            org.apache.calcite.sql.type.coerceRules.add(SqlTypeName.CHAR, org.apache.calcite.sql.type.rule)
            org.apache.calcite.sql.type.coerceRules.add(SqlTypeName.VARCHAR, org.apache.calcite.sql.type.rule)

            // Exact numeric types are castable from intervals
            for (exactType in SqlTypeName.EXACT_TYPES) {
                org.apache.calcite.sql.type.coerceRules.add(
                    org.apache.calcite.sql.type.exactType,
                    org.apache.calcite.sql.type.coerceRules.copyValues(org.apache.calcite.sql.type.exactType)
                        .addAll(SqlTypeName.INTERVAL_TYPES)
                        .build()
                )
            }

            // Intervals are castable from exact numeric
            for (typeName in SqlTypeName.INTERVAL_TYPES) {
                org.apache.calcite.sql.type.coerceRules.add(
                    org.apache.calcite.sql.type.typeName,
                    org.apache.calcite.sql.type.coerceRules.copyValues(org.apache.calcite.sql.type.typeName)
                        .add(SqlTypeName.TINYINT)
                        .add(SqlTypeName.SMALLINT)
                        .add(SqlTypeName.INTEGER)
                        .add(SqlTypeName.BIGINT)
                        .add(SqlTypeName.DECIMAL)
                        .add(SqlTypeName.CHAR)
                        .add(SqlTypeName.VARCHAR)
                        .build()
                )
            }

            // BINARY is castable from VARBINARY, CHARACTERS.
            org.apache.calcite.sql.type.coerceRules.add(
                SqlTypeName.BINARY,
                org.apache.calcite.sql.type.coerceRules.copyValues(SqlTypeName.BINARY)
                    .add(SqlTypeName.VARBINARY)
                    .addAll(SqlTypeName.CHAR_TYPES)
                    .build()
            )

            // VARBINARY is castable from BINARY, CHARACTERS.
            org.apache.calcite.sql.type.coerceRules.add(
                SqlTypeName.VARBINARY,
                org.apache.calcite.sql.type.coerceRules.copyValues(SqlTypeName.VARBINARY)
                    .add(SqlTypeName.BINARY)
                    .addAll(SqlTypeName.CHAR_TYPES)
                    .build()
            )

            // VARCHAR is castable from BOOLEAN, DATE, TIME, TIMESTAMP, numeric types, binary and
            // intervals
            org.apache.calcite.sql.type.coerceRules.add(
                SqlTypeName.VARCHAR,
                org.apache.calcite.sql.type.coerceRules.copyValues(SqlTypeName.VARCHAR)
                    .add(SqlTypeName.CHAR)
                    .add(SqlTypeName.BOOLEAN)
                    .add(SqlTypeName.DATE)
                    .add(SqlTypeName.TIME)
                    .add(SqlTypeName.TIMESTAMP)
                    .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                    .addAll(SqlTypeName.BINARY_TYPES)
                    .addAll(SqlTypeName.NUMERIC_TYPES)
                    .addAll(SqlTypeName.INTERVAL_TYPES)
                    .build()
            )

            // CHAR is castable from BOOLEAN, DATE, TIME, TIMESTAMP, numeric types, binary and
            // intervals
            org.apache.calcite.sql.type.coerceRules.add(
                SqlTypeName.CHAR,
                org.apache.calcite.sql.type.coerceRules.copyValues(SqlTypeName.CHAR)
                    .add(SqlTypeName.VARCHAR)
                    .add(SqlTypeName.BOOLEAN)
                    .add(SqlTypeName.DATE)
                    .add(SqlTypeName.TIME)
                    .add(SqlTypeName.TIMESTAMP)
                    .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                    .addAll(SqlTypeName.BINARY_TYPES)
                    .addAll(SqlTypeName.NUMERIC_TYPES)
                    .addAll(SqlTypeName.INTERVAL_TYPES)
                    .build()
            )

            // BOOLEAN is castable from ...
            org.apache.calcite.sql.type.coerceRules.add(
                SqlTypeName.BOOLEAN,
                org.apache.calcite.sql.type.coerceRules.copyValues(SqlTypeName.BOOLEAN)
                    .add(SqlTypeName.CHAR)
                    .add(SqlTypeName.VARCHAR)
                    .addAll(SqlTypeName.NUMERIC_TYPES)
                    .build()
            )

            // DATE, TIME, and TIMESTAMP are castable from
            // CHAR and VARCHAR.

            // DATE is castable from...
            org.apache.calcite.sql.type.coerceRules.add(
                SqlTypeName.DATE,
                org.apache.calcite.sql.type.coerceRules.copyValues(SqlTypeName.DATE)
                    .add(SqlTypeName.TIMESTAMP)
                    .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                    .add(SqlTypeName.CHAR)
                    .add(SqlTypeName.VARCHAR)
                    .addAll(SqlTypeName.BINARY_TYPES)
                    .build()
            )

            // TIME is castable from...
            org.apache.calcite.sql.type.coerceRules.add(
                SqlTypeName.TIME,
                org.apache.calcite.sql.type.coerceRules.copyValues(SqlTypeName.TIME)
                    .add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
                    .add(SqlTypeName.TIMESTAMP)
                    .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                    .add(SqlTypeName.CHAR)
                    .add(SqlTypeName.VARCHAR)
                    .addAll(SqlTypeName.BINARY_TYPES)
                    .build()
            )

            // TIME WITH LOCAL TIME ZONE is castable from...
            org.apache.calcite.sql.type.coerceRules.add(
                SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE,
                org.apache.calcite.sql.type.coerceRules.copyValues(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
                    .add(SqlTypeName.TIME)
                    .add(SqlTypeName.TIMESTAMP)
                    .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                    .add(SqlTypeName.CHAR)
                    .add(SqlTypeName.VARCHAR)
                    .addAll(SqlTypeName.BINARY_TYPES)
                    .build()
            )

            // TIMESTAMP is castable from...
            org.apache.calcite.sql.type.coerceRules.add(
                SqlTypeName.TIMESTAMP,
                org.apache.calcite.sql.type.coerceRules.copyValues(SqlTypeName.TIMESTAMP)
                    .add(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                    .add(SqlTypeName.DATE)
                    .add(SqlTypeName.TIME)
                    .add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
                    .add(SqlTypeName.CHAR)
                    .add(SqlTypeName.VARCHAR)
                    .addAll(SqlTypeName.BINARY_TYPES)
                    .addAll(SqlTypeName.NUMERIC_TYPES)
                    .build()
            )

            // TIMESTAMP WITH LOCAL TIME ZONE is castable from...
            org.apache.calcite.sql.type.coerceRules.add(
                SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                org.apache.calcite.sql.type.coerceRules.copyValues(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                    .add(SqlTypeName.TIMESTAMP)
                    .add(SqlTypeName.DATE)
                    .add(SqlTypeName.TIME)
                    .add(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
                    .add(SqlTypeName.CHAR)
                    .add(SqlTypeName.VARCHAR)
                    .addAll(SqlTypeName.BINARY_TYPES)
                    .addAll(SqlTypeName.NUMERIC_TYPES)
                    .build()
            )
            INSTANCE = SqlTypeCoercionRule(org.apache.calcite.sql.type.coerceRules.map)
        }
        //~ Methods ----------------------------------------------------------------
        /** Returns an instance.  */
        fun instance(): SqlTypeCoercionRule {
            return Objects.requireNonNull(THREAD_PROVIDERS.get(), "threadProviders")
        }

        /** Returns an instance with specified type mappings.  */
        fun instance(
            map: Map<SqlTypeName, ImmutableSet<SqlTypeName>>
        ): SqlTypeCoercionRule {
            return SqlTypeCoercionRule(map)
        }
    }
}
