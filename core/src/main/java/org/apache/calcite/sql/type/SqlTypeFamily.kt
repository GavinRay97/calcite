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

import org.apache.calcite.avatica.util.TimeUnit

/**
 * SqlTypeFamily provides SQL type categorization.
 *
 *
 * The *primary* family categorization is a complete disjoint
 * partitioning of SQL types into families, where two types are members of the
 * same primary family iff instances of the two types can be the operands of an
 * SQL equality predicate such as `WHERE v1 = v2`. Primary families
 * are returned by RelDataType.getFamily().
 *
 *
 * There is also a *secondary* family categorization which overlaps
 * with the primary categorization. It is used in type strategies for more
 * specific or more general categorization than the primary families. Secondary
 * families are never returned by RelDataType.getFamily().
 */
enum class SqlTypeFamily : RelDataTypeFamily {
    // Primary families.
    CHARACTER, BINARY, NUMERIC, DATE, TIME, TIMESTAMP, BOOLEAN, INTERVAL_YEAR_MONTH, INTERVAL_DAY_TIME,  // Secondary families.
    STRING, APPROXIMATE_NUMERIC, EXACT_NUMERIC, DECIMAL, INTEGER, DATETIME, DATETIME_INTERVAL, MULTISET, ARRAY, MAP, NULL, ANY, CURSOR, COLUMN_LIST, GEO,

    /** Like ANY, but do not even validate the operand. It may not be an
     * expression.  */
    IGNORE;

    /** For this type family, returns the allow types of the difference between
     * two values of this family.
     *
     *
     * Equivalently, given an `ORDER BY` expression with one key,
     * returns the allowable type families of the difference between two keys.
     *
     *
     * Example 1. For `ORDER BY empno`, a NUMERIC, the difference
     * between two `empno` values is also NUMERIC.
     *
     *
     * Example 2. For `ORDER BY hireDate`, a DATE, the difference
     * between two `hireDate` values might be an INTERVAL_DAY_TIME
     * or INTERVAL_YEAR_MONTH.
     *
     *
     * The result determines whether a [SqlWindow] with a `RANGE`
     * is valid (for example, `OVER (ORDER BY empno RANGE 10` is valid
     * because `10` is numeric);
     * and whether a call to
     * [PERCENTILE_CONT][org.apache.calcite.sql.fun.SqlStdOperatorTable.PERCENTILE_CONT]
     * is valid (for example, `PERCENTILE_CONT(0.25)` ORDER BY (hireDate)}
     * is valid because `hireDate` values may be interpolated by adding
     * values of type `INTERVAL_DAY_TIME`.  */
    fun allowableDifferenceTypes(): List<SqlTypeFamily> {
        return when (this) {
            NUMERIC -> ImmutableList.of(NUMERIC)
            DATE, TIME, TIMESTAMP -> ImmutableList.of(
                INTERVAL_DAY_TIME,
                INTERVAL_YEAR_MONTH
            )
            else -> ImmutableList.of()
        }
    }

    /** Returns the collection of [SqlTypeName]s included in this family.  */
    val typeNames: Collection<org.apache.calcite.sql.type.SqlTypeName?>
        get() = when (this) {
            CHARACTER -> SqlTypeName.CHAR_TYPES
            BINARY -> SqlTypeName.BINARY_TYPES
            NUMERIC -> SqlTypeName.NUMERIC_TYPES
            DECIMAL -> ImmutableList.of(SqlTypeName.DECIMAL)
            DATE -> ImmutableList.of(SqlTypeName.DATE)
            TIME -> ImmutableList.of(
                SqlTypeName.TIME,
                SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE
            )
            TIMESTAMP -> ImmutableList.of(
                SqlTypeName.TIMESTAMP,
                SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
            )
            BOOLEAN -> SqlTypeName.BOOLEAN_TYPES
            INTERVAL_YEAR_MONTH -> SqlTypeName.YEAR_INTERVAL_TYPES
            INTERVAL_DAY_TIME -> SqlTypeName.DAY_INTERVAL_TYPES
            STRING -> SqlTypeName.STRING_TYPES
            APPROXIMATE_NUMERIC -> SqlTypeName.APPROX_TYPES
            EXACT_NUMERIC -> SqlTypeName.EXACT_TYPES
            INTEGER -> SqlTypeName.INT_TYPES
            DATETIME -> SqlTypeName.DATETIME_TYPES
            DATETIME_INTERVAL -> SqlTypeName.INTERVAL_TYPES
            GEO -> ImmutableList.of(SqlTypeName.GEOMETRY)
            MULTISET -> ImmutableList.of(SqlTypeName.MULTISET)
            ARRAY -> ImmutableList.of(SqlTypeName.ARRAY)
            MAP -> ImmutableList.of(SqlTypeName.MAP)
            NULL -> ImmutableList.of(SqlTypeName.NULL)
            ANY -> SqlTypeName.ALL_TYPES
            CURSOR -> ImmutableList.of(SqlTypeName.CURSOR)
            COLUMN_LIST -> ImmutableList.of(SqlTypeName.COLUMN_LIST)
            else -> throw IllegalArgumentException()
        }

    /** Return the default [RelDataType] that belongs to this family.  */
    @Nullable
    fun getDefaultConcreteType(factory: RelDataTypeFactory): RelDataType? {
        return when (this) {
            CHARACTER -> factory.createSqlType(SqlTypeName.VARCHAR)
            BINARY -> factory.createSqlType(SqlTypeName.VARBINARY)
            NUMERIC -> SqlTypeUtil.getMaxPrecisionScaleDecimal(factory)
            DATE -> factory.createSqlType(SqlTypeName.DATE)
            TIME -> factory.createSqlType(SqlTypeName.TIME)
            TIMESTAMP -> factory.createSqlType(SqlTypeName.TIMESTAMP)
            BOOLEAN -> factory.createSqlType(SqlTypeName.BOOLEAN)
            STRING -> factory.createSqlType(SqlTypeName.VARCHAR)
            APPROXIMATE_NUMERIC -> factory.createSqlType(SqlTypeName.DOUBLE)
            EXACT_NUMERIC -> SqlTypeUtil.getMaxPrecisionScaleDecimal(factory)
            INTEGER -> factory.createSqlType(SqlTypeName.BIGINT)
            DECIMAL -> factory.createSqlType(SqlTypeName.DECIMAL)
            DATETIME -> factory.createSqlType(SqlTypeName.TIMESTAMP)
            INTERVAL_DAY_TIME -> factory.createSqlIntervalType(
                SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO)
            )
            INTERVAL_YEAR_MONTH -> factory.createSqlIntervalType(
                SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO)
            )
            GEO -> factory.createSqlType(SqlTypeName.GEOMETRY)
            MULTISET -> factory.createMultisetType(
                factory.createSqlType(
                    SqlTypeName.ANY
                ), -1
            )
            ARRAY -> factory.createArrayType(
                factory.createSqlType(SqlTypeName.ANY),
                -1
            )
            MAP -> factory.createMapType(
                factory.createSqlType(SqlTypeName.ANY),
                factory.createSqlType(SqlTypeName.ANY)
            )
            NULL -> factory.createSqlType(SqlTypeName.NULL)
            CURSOR -> factory.createSqlType(SqlTypeName.CURSOR)
            COLUMN_LIST -> factory.createSqlType(SqlTypeName.COLUMN_LIST)
            else -> null
        }
    }

    operator fun contains(type: RelDataType): Boolean {
        return SqlTypeUtil.isOfSameTypeName(typeNames, type)
    }

    companion object {
        private val JDBC_TYPE_TO_FAMILY: Map<Integer, SqlTypeFamily> =
            ImmutableMap.< Integer, SqlTypeFamily>builder<Integer?, org.apache.calcite.sql.type.SqlTypeFamily?>() // Not present:
        // SqlTypeName.MULTISET shares Types.ARRAY with SqlTypeName.ARRAY;
        // SqlTypeName.MAP has no corresponding JDBC type
        // SqlTypeName.COLUMN_LIST has no corresponding JDBC type
        .put(Types.BIT, org.apache.calcite.sql.type.SqlTypeFamily.NUMERIC)
        .put(Types.TINYINT, org.apache.calcite.sql.type.SqlTypeFamily.NUMERIC)
        .put(Types.SMALLINT, org.apache.calcite.sql.type.SqlTypeFamily.NUMERIC)
        .put(Types.BIGINT, org.apache.calcite.sql.type.SqlTypeFamily.NUMERIC)
        .put(Types.INTEGER, org.apache.calcite.sql.type.SqlTypeFamily.NUMERIC)
        .put(Types.NUMERIC, org.apache.calcite.sql.type.SqlTypeFamily.NUMERIC)
        .put(Types.DECIMAL, org.apache.calcite.sql.type.SqlTypeFamily.NUMERIC)
        .put(Types.FLOAT, org.apache.calcite.sql.type.SqlTypeFamily.NUMERIC)
        .put(Types.REAL, org.apache.calcite.sql.type.SqlTypeFamily.NUMERIC)
        .put(Types.DOUBLE, org.apache.calcite.sql.type.SqlTypeFamily.NUMERIC)
        .put(Types.CHAR, org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER)
        .put(Types.VARCHAR, org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER)
        .put(Types.LONGVARCHAR, org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER)
        .put(Types.CLOB, org.apache.calcite.sql.type.SqlTypeFamily.CHARACTER)
        .put(Types.BINARY, org.apache.calcite.sql.type.SqlTypeFamily.BINARY)
        .put(Types.VARBINARY, org.apache.calcite.sql.type.SqlTypeFamily.BINARY)
        .put(Types.LONGVARBINARY, org.apache.calcite.sql.type.SqlTypeFamily.BINARY)
        .put(Types.BLOB, org.apache.calcite.sql.type.SqlTypeFamily.BINARY)
        .put(Types.DATE, org.apache.calcite.sql.type.SqlTypeFamily.DATE)
        .put(Types.TIME, org.apache.calcite.sql.type.SqlTypeFamily.TIME)
        .put(ExtraSqlTypes.TIME_WITH_TIMEZONE, org.apache.calcite.sql.type.SqlTypeFamily.TIME)
        .put(Types.TIMESTAMP, org.apache.calcite.sql.type.SqlTypeFamily.TIMESTAMP)
        .put(ExtraSqlTypes.TIMESTAMP_WITH_TIMEZONE, org.apache.calcite.sql.type.SqlTypeFamily.TIMESTAMP)
        .put(Types.BOOLEAN, org.apache.calcite.sql.type.SqlTypeFamily.BOOLEAN)
        .put(ExtraSqlTypes.REF_CURSOR, org.apache.calcite.sql.type.SqlTypeFamily.CURSOR)
        .put(Types.ARRAY, org.apache.calcite.sql.type.SqlTypeFamily.ARRAY)
        .build()
        /**
         * Gets the primary family containing a JDBC type.
         *
         * @param jdbcType the JDBC type of interest
         * @return containing family
         */
        @Nullable
        fun getFamilyForJdbcType(jdbcType: Int): SqlTypeFamily? {
            return JDBC_TYPE_TO_FAMILY[jdbcType]
        }
    }
}
