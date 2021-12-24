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
 * Abstract base class for SQL implementations of [RelDataType].
 */
abstract class AbstractSqlType protected constructor(
    typeName: SqlTypeName,
    isNullable: Boolean,
    @Nullable fields: List<RelDataTypeField?>?
) : RelDataTypeImpl(fields), Cloneable, Serializable {
    //~ Instance fields --------------------------------------------------------
    protected val typeName: SqlTypeName

    @get:Override
    var isNullable: Boolean
        protected set
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates an AbstractSqlType.
     *
     * @param typeName   Type name
     * @param isNullable Whether nullable
     * @param fields     Fields of type, or null if not a record type
     */
    init {
        this.typeName = Objects.requireNonNull(typeName, "typeName")
        this.isNullable = isNullable || typeName === SqlTypeName.NULL
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val sqlTypeName: org.apache.calcite.sql.type.SqlTypeName
        get() = typeName

    // If typename does not have family, treat the current type as the only member its family
    @get:Override
    val family: RelDataTypeFamily
        get() {
            val family: SqlTypeFamily = typeName.getFamily()
            // If typename does not have family, treat the current type as the only member its family
            return if (family != null) family else this
        }

    @get:Override
    val precedenceList: RelDataTypePrecedenceList
        get() {
            val list: RelDataTypePrecedenceList = SqlTypeExplicitPrecedenceList.getListForType(this)
            return if (list != null) {
                list
            } else super.getPrecedenceList()
        }
}
