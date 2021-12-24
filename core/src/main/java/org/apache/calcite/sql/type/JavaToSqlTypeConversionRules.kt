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

import org.apache.calcite.avatica.util.ArrayImpl

/**
 * JavaToSqlTypeConversionRules defines mappings from common Java types to
 * corresponding SQL types.
 */
class JavaToSqlTypeConversionRules {
    //~ Instance fields --------------------------------------------------------
    private val rules: Map<Class<*>, SqlTypeName> =
        ImmutableMap.< Class <?>, SqlTypeName>builder<Class<*>?, SqlTypeName?>()
    .put(Integer::
    class.java, SqlTypeName.INTEGER)
    .put(Int::
    class.javaPrimitiveType, SqlTypeName.INTEGER)
    .put(Long::
    class.java, SqlTypeName.BIGINT)
    .put(Long::
    class.javaPrimitiveType, SqlTypeName.BIGINT)
    .put(Short::
    class.java, SqlTypeName.SMALLINT)
    .put(Short::
    class.javaPrimitiveType, SqlTypeName.SMALLINT)
    .put(kotlin.Byte::
    class.javaPrimitiveType, SqlTypeName.TINYINT)
    .put(Byte::
    class.java, SqlTypeName.TINYINT)
    .put(Float::
    class.java, SqlTypeName.REAL)
    .put(kotlin.Float::
    class.javaPrimitiveType, SqlTypeName.REAL)
    .put(Double::
    class.java, SqlTypeName.DOUBLE)
    .put(kotlin.Double::
    class.javaPrimitiveType, SqlTypeName.DOUBLE)
    .put(Boolean::
    class.javaPrimitiveType, SqlTypeName.BOOLEAN)
    .put(Boolean::
    class.java, SqlTypeName.BOOLEAN)
    .put(kotlin.ByteArray::
    class.java, SqlTypeName.VARBINARY)
    .put(String::
    class.java, SqlTypeName.VARCHAR)
    .put(kotlin.CharArray::
    class.java, SqlTypeName.VARCHAR)
    .put(Character::
    class.java, SqlTypeName.CHAR)
    .put(kotlin.Char::
    class.javaPrimitiveType, SqlTypeName.CHAR)
    .put(java.util.Date::
    class.java, SqlTypeName.TIMESTAMP)
    .put(Date::
    class.java, SqlTypeName.DATE)
    .put(Timestamp::
    class.java, SqlTypeName.TIMESTAMP)
    .put(Time::
    class.java, SqlTypeName.TIME)
    .put(BigDecimal::
    class.java, SqlTypeName.DECIMAL)
    .put(Geometries.Geom::
    class.java, SqlTypeName.GEOMETRY)
    .put(ResultSet::
    class.java, SqlTypeName.CURSOR)
    .put(org.apache.calcite.sql.type.JavaToSqlTypeConversionRules.ColumnList::
    class.java, SqlTypeName.COLUMN_LIST)
    .put(ArrayImpl::
    class.java, SqlTypeName.ARRAY)
    .put(List::
    class.java, SqlTypeName.ARRAY)
    .put(Map::
    class.java, SqlTypeName.MAP)
    .put(Void::
    class.java, SqlTypeName.NULL)
    .build()
    /**
     * Returns a corresponding [SqlTypeName] for a given Java class.
     *
     * @param javaClass the Java class to lookup
     * @return a corresponding SqlTypeName if found, otherwise null is returned
     */
    @Nullable
    fun lookup(javaClass: Class): SqlTypeName? {
        return rules[javaClass]
    }

    /**
     * Make this public when needed. To represent COLUMN_LIST SQL value, we need
     * a type distinguishable from [List] in user-defined types.
     */
    private interface ColumnList : List
    companion object {
        //~ Static fields/initializers ---------------------------------------------
        private val INSTANCE = JavaToSqlTypeConversionRules()
        //~ Methods ----------------------------------------------------------------
        /**
         * Returns the
         * [singleton][org.apache.calcite.util.Glossary.SINGLETON_PATTERN]
         * instance.
         */
        fun instance(): JavaToSqlTypeConversionRules {
            return INSTANCE
        }
    }
}
