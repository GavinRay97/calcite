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
package org.apache.calcite.rel.type

import org.apache.calcite.sql.type.SqlTypeExplicitPrecedenceList
import org.apache.calcite.sql.type.SqlTypeFamily
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.util.Pair
import com.google.common.collect.ImmutableList
import java.util.List

/**
 * Implementation of [RelDataType] for a dynamic table.
 *
 *
 * It's used during SQL validation, where the field list is mutable for
 * the getField() call. After SQL validation, a normal [RelDataTypeImpl]
 * with an immutable field list takes the place of the DynamicRecordTypeImpl
 * instance.
 */
class DynamicRecordTypeImpl @SuppressWarnings("method.invocation.invalid") constructor(typeFactory: RelDataTypeFactory) :
    DynamicRecordType() {
    private val holder: RelDataTypeHolder

    /** Creates a DynamicRecordTypeImpl.  */
    init {
        holder = RelDataTypeHolder(typeFactory)
        computeDigest()
    }

    @Override
    override fun getFieldList(): List<RelDataTypeField> {
        return holder.getFieldList()
    }

    @Override
    fun getFieldCount(): Int {
        return holder.getFieldCount()
    }

    @Override
    @Nullable
    override fun getField(
        fieldName: String,
        caseSensitive: Boolean, elideRecord: Boolean
    ): RelDataTypeField {
        val pair: Pair<RelDataTypeField, Boolean> = holder.getFieldOrInsert(fieldName, caseSensitive)
        // If a new field is added, we should re-compute the digest.
        if (pair.right) {
            computeDigest()
        }
        return pair.left
    }

    @Override
    fun getFieldNames(): List<String> {
        return holder.getFieldNames()
    }

    @Override
    fun getSqlTypeName(): SqlTypeName {
        return SqlTypeName.ROW
    }

    @Override
    fun getPrecedenceList(): RelDataTypePrecedenceList {
        return SqlTypeExplicitPrecedenceList(ImmutableList.of())
    }

    @Override
    protected override fun generateTypeString(sb: StringBuilder, withDetail: Boolean) {
        sb.append("(DynamicRecordRow").append(getFieldNames()).append(")")
    }

    @Override
    fun isStruct(): Boolean {
        return true
    }

    @Override
    fun getFamily(): RelDataTypeFamily {
        val family: SqlTypeFamily = getSqlTypeName().getFamily()
        return if (family != null) family else this
    }
}
