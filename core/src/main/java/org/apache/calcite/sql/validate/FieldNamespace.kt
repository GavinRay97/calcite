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
package org.apache.calcite.sql.validate

import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.sql.SqlNode
import java.util.Objects.requireNonNull

/**
 * Implementation of [SqlValidatorNamespace] for a field of a record.
 *
 *
 * A field is not a very interesting namespace - except if the field has a
 * record or multiset type - but this class exists to make fields behave
 * similarly to other records for purposes of name resolution.
 */
internal class FieldNamespace(
    validator: SqlValidatorImpl?,
    dataType: RelDataType
) : AbstractNamespace(validator, null) {
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a FieldNamespace.
     *
     * @param validator Validator
     * @param dataType  Data type of field
     */
    init {
        assert(dataType != null)
        this.rowType = dataType
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun setType(type: RelDataType?) {
        throw UnsupportedOperationException()
    }

    @Override
    protected fun validateImpl(targetRowType: RelDataType?): RelDataType {
        return requireNonNull(rowType, "rowType")
    }

    @get:Nullable
    @get:Override
    val node: SqlNode?
        get() = null

    @Override
    @Nullable
    fun lookupChild(name: String?): SqlValidatorNamespace? {
        return if (requireNonNull(rowType, "rowType").isStruct()) {
            validator.lookupFieldNamespace(
                rowType,
                name
            )
        } else null
    }

    @Override
    fun fieldExists(name: String?): Boolean {
        return false
    }
}
