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

import org.apache.calcite.rel.type.RelDataType

/**
 * SqlTypeExplicitPrecedenceList implements the
 * [RelDataTypePrecedenceList] interface via an explicit list of
 * [SqlTypeName] entries.
 */
class SqlTypeExplicitPrecedenceList(typeNames: Iterable<SqlTypeName?>?) : RelDataTypePrecedenceList {
    //~ Instance fields --------------------------------------------------------
    private val typeNames: List<SqlTypeName>

    //~ Constructors -----------------------------------------------------------
    init {
        this.typeNames = ImmutableNullableList.copyOf(typeNames)
    }

    // implement RelDataTypePrecedenceList
    @Override
    fun containsType(type: RelDataType): Boolean {
        val typeName: SqlTypeName = type.getSqlTypeName()
        return typeName != null && typeNames.contains(typeName)
    }

    // implement RelDataTypePrecedenceList
    @Override
    fun compareTypePrecedence(type1: RelDataType, type2: RelDataType): Int {
        assert(containsType(type1)) { type1 }
        assert(containsType(type2)) { type2 }
        val p1 = getListPosition(
            type1.getSqlTypeName(),
            typeNames
        )
        val p2 = getListPosition(
            type2.getSqlTypeName(),
            typeNames
        )
        return p2 - p1
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        private val NUMERIC_TYPES: List<SqlTypeName> = ImmutableNullableList.of(
            SqlTypeName.TINYINT,
            SqlTypeName.SMALLINT,
            SqlTypeName.INTEGER,
            SqlTypeName.BIGINT,
            SqlTypeName.DECIMAL,
            SqlTypeName.REAL,
            SqlTypeName.FLOAT,
            SqlTypeName.DOUBLE
        )

        /**
         * Map from SqlTypeName to corresponding precedence list.
         *
         * @see Glossary.SQL2003 SQL:2003 Part 2 Section 9.5
         */
        private val TYPE_NAME_TO_PRECEDENCE_LIST: Map<SqlTypeName, SqlTypeExplicitPrecedenceList> =
            ImmutableMap.< SqlTypeName, SqlTypeExplicitPrecedenceList>builder<SqlTypeName?, org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList?>()
        .put(SqlTypeName.BOOLEAN, list(SqlTypeName.BOOLEAN))
        .put(SqlTypeName.TINYINT, org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList.Companion.numeric(SqlTypeName.TINYINT))
        .put(SqlTypeName.SMALLINT, org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList.Companion.numeric(SqlTypeName.SMALLINT))
        .put(SqlTypeName.INTEGER, org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList.Companion.numeric(SqlTypeName.INTEGER))
        .put(SqlTypeName.BIGINT, org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList.Companion.numeric(SqlTypeName.BIGINT))
        .put(SqlTypeName.DECIMAL, org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList.Companion.numeric(SqlTypeName.DECIMAL))
        .put(SqlTypeName.REAL, org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList.Companion.numeric(SqlTypeName.REAL))
        .put(SqlTypeName.FLOAT, org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList.Companion.list(SqlTypeName.FLOAT, SqlTypeName.REAL, SqlTypeName.DOUBLE))
        .put(SqlTypeName.DOUBLE, org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList.Companion.list(SqlTypeName.DOUBLE, SqlTypeName.DECIMAL))
        .put(SqlTypeName.CHAR, org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList.Companion.list(SqlTypeName.CHAR, SqlTypeName.VARCHAR))
        .put(SqlTypeName.VARCHAR, list(SqlTypeName.VARCHAR))
        .put(SqlTypeName.BINARY,
        org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList.Companion.list(SqlTypeName.BINARY, SqlTypeName.VARBINARY))
        .put(SqlTypeName.VARBINARY, list(SqlTypeName.VARBINARY))
        .put(SqlTypeName.DATE, list(SqlTypeName.DATE))
        .put(SqlTypeName.TIME, list(SqlTypeName.TIME))
        .put(SqlTypeName.TIMESTAMP,
        org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList.Companion.list(SqlTypeName.TIMESTAMP, SqlTypeName.DATE, SqlTypeName.TIME))
        .put(SqlTypeName.INTERVAL_YEAR,
        list(SqlTypeName.YEAR_INTERVAL_TYPES))
        .put(SqlTypeName.INTERVAL_YEAR_MONTH,
        list(SqlTypeName.YEAR_INTERVAL_TYPES))
        .put(SqlTypeName.INTERVAL_MONTH,
        list(SqlTypeName.YEAR_INTERVAL_TYPES))
        .put(SqlTypeName.INTERVAL_DAY,
        list(SqlTypeName.DAY_INTERVAL_TYPES))
        .put(SqlTypeName.INTERVAL_DAY_HOUR,
        list(SqlTypeName.DAY_INTERVAL_TYPES))
        .put(SqlTypeName.INTERVAL_DAY_MINUTE,
        list(SqlTypeName.DAY_INTERVAL_TYPES))
        .put(SqlTypeName.INTERVAL_DAY_SECOND,
        list(SqlTypeName.DAY_INTERVAL_TYPES))
        .put(SqlTypeName.INTERVAL_HOUR,
        list(SqlTypeName.DAY_INTERVAL_TYPES))
        .put(SqlTypeName.INTERVAL_HOUR_MINUTE,
        list(SqlTypeName.DAY_INTERVAL_TYPES))
        .put(SqlTypeName.INTERVAL_HOUR_SECOND,
        list(SqlTypeName.DAY_INTERVAL_TYPES))
        .put(SqlTypeName.INTERVAL_MINUTE,
        list(SqlTypeName.DAY_INTERVAL_TYPES))
        .put(SqlTypeName.INTERVAL_MINUTE_SECOND,
        list(SqlTypeName.DAY_INTERVAL_TYPES))
        .put(SqlTypeName.INTERVAL_SECOND,
        list(SqlTypeName.DAY_INTERVAL_TYPES))
        .build()
        //~ Methods ----------------------------------------------------------------
        private fun list(vararg typeNames: SqlTypeName): SqlTypeExplicitPrecedenceList {
            return list(Arrays.asList(typeNames))
        }

        private fun list(typeNames: Iterable<SqlTypeName>): SqlTypeExplicitPrecedenceList {
            return SqlTypeExplicitPrecedenceList(typeNames)
        }

        private fun numeric(typeName: SqlTypeName): SqlTypeExplicitPrecedenceList {
            val i = getListPosition(typeName, NUMERIC_TYPES)
            return SqlTypeExplicitPrecedenceList(
                Util.skip(NUMERIC_TYPES, i)
            )
        }

        private fun getListPosition(type: SqlTypeName, list: List<SqlTypeName>): Int {
            val i = list.indexOf(type)
            assert(i != -1)
            return i
        }

        @Nullable
        fun getListForType(type: RelDataType): RelDataTypePrecedenceList? {
            val typeName: SqlTypeName = type.getSqlTypeName() ?: return null
            return TYPE_NAME_TO_PRECEDENCE_LIST[typeName]
        }
    }
}
