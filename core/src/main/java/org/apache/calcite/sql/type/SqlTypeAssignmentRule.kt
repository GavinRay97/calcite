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
 * Rules that determine whether a type is assignable from another type.
 */
class SqlTypeAssignmentRule private constructor(
    map: Map<SqlTypeName, ImmutableSet<SqlTypeName>>
) : SqlTypeMappingRule {
    //~ Instance fields --------------------------------------------------------
    private val map: Map<SqlTypeName, ImmutableSet<SqlTypeName>>
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a `SqlTypeAssignmentRules` with specified type mappings `map`.
     *
     *
     * Make this constructor private intentionally, use [.instance].
     *
     * @param map The type mapping, for each map entry, the values types can be assigned to
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
        private val INSTANCE: SqlTypeAssignmentRule? = null

        init {
            val rules: SqlTypeMappingRules.Builder = SqlTypeMappingRules.builder()
            val rule: Set<SqlTypeName> = HashSet()

            // IntervalYearMonth is assignable from...
            for (interval in SqlTypeName.YEAR_INTERVAL_TYPES) {
                org.apache.calcite.sql.type.rules.add(
                    org.apache.calcite.sql.type.interval,
                    SqlTypeName.YEAR_INTERVAL_TYPES
                )
            }
            for (interval in SqlTypeName.DAY_INTERVAL_TYPES) {
                org.apache.calcite.sql.type.rules.add(
                    org.apache.calcite.sql.type.interval,
                    SqlTypeName.DAY_INTERVAL_TYPES
                )
            }

            // MULTISET is assignable from...
            org.apache.calcite.sql.type.rules.add(SqlTypeName.MULTISET, EnumSet.of(SqlTypeName.MULTISET))

            // TINYINT is assignable from...
            org.apache.calcite.sql.type.rules.add(SqlTypeName.TINYINT, EnumSet.of(SqlTypeName.TINYINT))

            // SMALLINT is assignable from...
            org.apache.calcite.sql.type.rule.clear()
            org.apache.calcite.sql.type.rule.add(SqlTypeName.TINYINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.SMALLINT)
            org.apache.calcite.sql.type.rules.add(SqlTypeName.SMALLINT, org.apache.calcite.sql.type.rule)

            // INTEGER is assignable from...
            org.apache.calcite.sql.type.rule.clear()
            org.apache.calcite.sql.type.rule.add(SqlTypeName.TINYINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.SMALLINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.INTEGER)
            org.apache.calcite.sql.type.rules.add(SqlTypeName.INTEGER, org.apache.calcite.sql.type.rule)

            // BIGINT is assignable from...
            org.apache.calcite.sql.type.rule.clear()
            org.apache.calcite.sql.type.rule.add(SqlTypeName.TINYINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.SMALLINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.INTEGER)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.BIGINT)
            org.apache.calcite.sql.type.rules.add(SqlTypeName.BIGINT, org.apache.calcite.sql.type.rule)

            // FLOAT (up to 64 bit floating point) is assignable from...
            org.apache.calcite.sql.type.rule.clear()
            org.apache.calcite.sql.type.rule.add(SqlTypeName.TINYINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.SMALLINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.INTEGER)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.BIGINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.DECIMAL)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.FLOAT)
            org.apache.calcite.sql.type.rules.add(SqlTypeName.FLOAT, org.apache.calcite.sql.type.rule)

            // REAL (32 bit floating point) is assignable from...
            org.apache.calcite.sql.type.rule.clear()
            org.apache.calcite.sql.type.rule.add(SqlTypeName.TINYINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.SMALLINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.INTEGER)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.BIGINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.DECIMAL)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.FLOAT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.REAL)
            org.apache.calcite.sql.type.rules.add(SqlTypeName.REAL, org.apache.calcite.sql.type.rule)

            // DOUBLE is assignable from...
            org.apache.calcite.sql.type.rule.clear()
            org.apache.calcite.sql.type.rule.add(SqlTypeName.TINYINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.SMALLINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.INTEGER)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.BIGINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.DECIMAL)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.FLOAT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.REAL)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.DOUBLE)
            org.apache.calcite.sql.type.rules.add(SqlTypeName.DOUBLE, org.apache.calcite.sql.type.rule)

            // DECIMAL is assignable from...
            org.apache.calcite.sql.type.rule.clear()
            org.apache.calcite.sql.type.rule.add(SqlTypeName.TINYINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.SMALLINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.INTEGER)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.BIGINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.REAL)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.DOUBLE)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.DECIMAL)
            org.apache.calcite.sql.type.rules.add(SqlTypeName.DECIMAL, org.apache.calcite.sql.type.rule)

            // VARBINARY is assignable from...
            org.apache.calcite.sql.type.rule.clear()
            org.apache.calcite.sql.type.rule.add(SqlTypeName.VARBINARY)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.BINARY)
            org.apache.calcite.sql.type.rules.add(SqlTypeName.VARBINARY, org.apache.calcite.sql.type.rule)

            // CHAR is assignable from...
            org.apache.calcite.sql.type.rules.add(SqlTypeName.CHAR, EnumSet.of(SqlTypeName.CHAR))

            // VARCHAR is assignable from...
            org.apache.calcite.sql.type.rule.clear()
            org.apache.calcite.sql.type.rule.add(SqlTypeName.CHAR)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.VARCHAR)
            org.apache.calcite.sql.type.rules.add(SqlTypeName.VARCHAR, org.apache.calcite.sql.type.rule)

            // BOOLEAN is assignable from...
            org.apache.calcite.sql.type.rules.add(SqlTypeName.BOOLEAN, EnumSet.of(SqlTypeName.BOOLEAN))

            // BINARY is assignable from...
            org.apache.calcite.sql.type.rule.clear()
            org.apache.calcite.sql.type.rule.add(SqlTypeName.BINARY)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.VARBINARY)
            org.apache.calcite.sql.type.rules.add(SqlTypeName.BINARY, org.apache.calcite.sql.type.rule)

            // DATE is assignable from...
            org.apache.calcite.sql.type.rule.clear()
            org.apache.calcite.sql.type.rule.add(SqlTypeName.DATE)
            org.apache.calcite.sql.type.rules.add(SqlTypeName.DATE, org.apache.calcite.sql.type.rule)

            // TIME is assignable from...
            org.apache.calcite.sql.type.rule.clear()
            org.apache.calcite.sql.type.rule.add(SqlTypeName.TIME)
            org.apache.calcite.sql.type.rules.add(SqlTypeName.TIME, org.apache.calcite.sql.type.rule)

            // TIME WITH LOCAL TIME ZONE is assignable from...
            org.apache.calcite.sql.type.rules.add(
                SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE,
                EnumSet.of(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE)
            )

            // TIMESTAMP is assignable from ...
            org.apache.calcite.sql.type.rules.add(SqlTypeName.TIMESTAMP, EnumSet.of(SqlTypeName.TIMESTAMP))

            // TIMESTAMP WITH LOCAL TIME ZONE is assignable from...
            org.apache.calcite.sql.type.rules.add(
                SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                EnumSet.of(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
            )

            // GEOMETRY is assignable from ...
            org.apache.calcite.sql.type.rules.add(SqlTypeName.GEOMETRY, EnumSet.of(SqlTypeName.GEOMETRY))

            // ARRAY is assignable from ...
            org.apache.calcite.sql.type.rules.add(SqlTypeName.ARRAY, EnumSet.of(SqlTypeName.ARRAY))

            // MAP is assignable from ...
            org.apache.calcite.sql.type.rules.add(SqlTypeName.MAP, EnumSet.of(SqlTypeName.MAP))

            // ANY is assignable from ...
            org.apache.calcite.sql.type.rule.clear()
            org.apache.calcite.sql.type.rule.add(SqlTypeName.TINYINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.SMALLINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.INTEGER)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.BIGINT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.DECIMAL)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.FLOAT)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.REAL)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.TIME)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.DATE)
            org.apache.calcite.sql.type.rule.add(SqlTypeName.TIMESTAMP)
            org.apache.calcite.sql.type.rules.add(SqlTypeName.ANY, org.apache.calcite.sql.type.rule)
            INSTANCE = SqlTypeAssignmentRule(org.apache.calcite.sql.type.rules.map)
        }
        //~ Methods ----------------------------------------------------------------
        /** Returns an instance.  */
        fun instance(): SqlTypeAssignmentRule? {
            return INSTANCE
        }
    }
}
