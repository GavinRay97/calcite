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
 * SQL map type.
 */
class MapSqlType(
    keyType: RelDataType?, valueType: RelDataType?, isNullable: Boolean
) : AbstractSqlType(SqlTypeName.MAP, isNullable, null) {
    //~ Instance fields --------------------------------------------------------
    private val keyType: RelDataType?
    private val valueType: RelDataType?
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a MapSqlType. This constructor should only be called
     * from a factory method.
     */
    init {
        assert(keyType != null)
        assert(valueType != null)
        this.keyType = keyType
        this.valueType = valueType
        computeDigest()
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun getValueType(): RelDataType? {
        return valueType
    }

    @Override
    fun getKeyType(): RelDataType? {
        return keyType
    }

    // implement RelDataTypeImpl
    @Override
    protected fun generateTypeString(sb: StringBuilder, withDetail: Boolean) {
        sb.append("(")
            .append(
                if (withDetail) keyType.getFullTypeString() else keyType.toString()
            )
            .append(", ")
            .append(
                if (withDetail) valueType.getFullTypeString() else valueType.toString()
            )
            .append(") MAP")
    }

    // implement RelDataType
    @get:Override
    override val family: RelDataTypeFamily
        get() = this
}
