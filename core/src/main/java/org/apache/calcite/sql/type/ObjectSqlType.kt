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

import org.apache.calcite.rel.type.RelDataTypeComparability

/**
 * ObjectSqlType represents an SQL structured user-defined type.
 */
class ObjectSqlType(
    typeName: SqlTypeName,
    @Nullable sqlIdentifier: SqlIdentifier,
    nullable: Boolean,
    fields: List<RelDataTypeField?>?,
    comparability: RelDataTypeComparability
) : AbstractSqlType(typeName, nullable, fields) {
    //~ Instance fields --------------------------------------------------------
    @Nullable
    private val sqlIdentifier: SqlIdentifier
    private val comparability: RelDataTypeComparability

    //~ Methods ----------------------------------------------------------------
    // each UDT is in its own lonely family, until one day when
    // we support inheritance (at which time also need to implement
    // getPrecedenceList).
    @get:Override
    @Nullable
    override var family: RelDataTypeFamily? = null
        get() {
            // each UDT is in its own lonely family, until one day when
            // we support inheritance (at which time also need to implement
            // getPrecedenceList).
            val family: RelDataTypeFamily? = field
            return if (family != null) family else this
        }
    //~ Constructors -----------------------------------------------------------
    /**
     * Constructs an object type. This should only be called from a factory
     * method.
     *
     * @param typeName      SqlTypeName for this type (either Distinct or
     * Structured)
     * @param sqlIdentifier identifier for this type
     * @param nullable      whether type accepts nulls
     * @param fields        object attribute definitions
     */
    init {
        this.sqlIdentifier = sqlIdentifier
        this.comparability = comparability
        computeDigest()
    }

    @Override
    fun getComparability(): RelDataTypeComparability {
        return comparability
    }

    @Override
    @Nullable
    fun getSqlIdentifier(): SqlIdentifier {
        return sqlIdentifier
    }

    @Override
    protected fun generateTypeString(sb: StringBuilder, withDetail: Boolean) {
        // TODO jvs 10-Feb-2005:  proper quoting; dump attributes withDetail?
        sb.append("ObjectSqlType(")
        sb.append(sqlIdentifier)
        sb.append(")")
    }
}
