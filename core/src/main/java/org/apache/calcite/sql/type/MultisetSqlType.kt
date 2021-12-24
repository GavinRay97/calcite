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
 * MultisetSqlType represents a standard SQL2003 multiset type.
 */
class MultisetSqlType(elementType: RelDataType?, isNullable: Boolean) :
    AbstractSqlType(SqlTypeName.MULTISET, isNullable, null) {
    //~ Instance fields --------------------------------------------------------
    private val elementType: RelDataType?
    //~ Constructors -----------------------------------------------------------
    /**
     * Constructs a new MultisetSqlType. This constructor should only be called
     * from a factory method.
     */
    init {
        assert(elementType != null)
        this.elementType = elementType
        computeDigest()
    }

    //~ Methods ----------------------------------------------------------------
    // implement RelDataTypeImpl
    @Override
    protected fun generateTypeString(sb: StringBuilder, withDetail: Boolean) {
        if (withDetail) {
            sb.append(elementType.getFullTypeString())
        } else {
            sb.append(elementType.toString())
        }
        sb.append(" MULTISET")
    }

    // implement RelDataType
    @get:Override
    val componentType: RelDataType?
        get() = elementType// TODO jvs 2-Dec-2004:  This gives each multiset type its

    // own family.  But that's not quite correct; the family should
    // be based on the element type for proper comparability
    // semantics (per 4.10.4 of SQL/2003).  So either this should
    // make up canonical families dynamically, or the
    // comparison type-checking should not rely on this.  I
    // think the same goes for ROW types.
    // implement RelDataType
    @get:Override
    override val family: RelDataTypeFamily
        get() =// TODO jvs 2-Dec-2004:  This gives each multiset type its
        // own family.  But that's not quite correct; the family should
        // be based on the element type for proper comparability
        // semantics (per 4.10.4 of SQL/2003).  So either this should
        // make up canonical families dynamically, or the
        // comparison type-checking should not rely on this.  I
            // think the same goes for ROW types.
            this

    @get:Override
    override val precedenceList: RelDataTypePrecedenceList
        get() = object : RelDataTypePrecedenceList() {
            @Override
            fun containsType(type: RelDataType): Boolean {
                if (type.getSqlTypeName() !== getSqlTypeName()) {
                    return false
                }
                val otherComponentType: RelDataType = type.getComponentType()
                return (otherComponentType != null
                        && componentType.getPrecedenceList().containsType(otherComponentType))
            }

            @Override
            fun compareTypePrecedence(type1: RelDataType, type2: RelDataType): Int {
                if (!containsType(type1)) {
                    throw IllegalArgumentException("must contain type: $type1")
                }
                if (!containsType(type2)) {
                    throw IllegalArgumentException("must contain type: $type2")
                }
                return componentType.getPrecedenceList()
                    .compareTypePrecedence(
                        getComponentTypeOrThrow(type1),
                        getComponentTypeOrThrow(type2)
                    )
            }
        }
}
