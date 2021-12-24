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

import org.apache.calcite.sql.type.SqlTypeName
import java.io.Serializable
import java.util.Objects

/**
 * Default implementation of [RelDataTypeField].
 */
class RelDataTypeFieldImpl(
    name: String?,
    index: Int,
    type: RelDataType?
) : RelDataTypeField, Serializable {
    //~ Instance fields --------------------------------------------------------
    private val type: RelDataType?
    private val name: String?
    private val index: Int
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a RelDataTypeFieldImpl.
     */
    init {
        assert(name != null)
        assert(type != null)
        this.name = name
        this.index = index
        this.type = type
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun hashCode(): Int {
        return Objects.hash(index, name, type)
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        if (this === obj) {
            return true
        }
        if (obj !is RelDataTypeFieldImpl) {
            return false
        }
        val that = obj as RelDataTypeFieldImpl
        return (index == that.index && name!!.equals(that.name)
                && type!!.equals(that.type))
    }

    // implement RelDataTypeField
    @Override
    override fun getName(): String? {
        return name
    }

    // implement RelDataTypeField
    @Override
    override fun getIndex(): Int {
        return index
    }

    // implement RelDataTypeField
    @Override
    override fun getType(): RelDataType? {
        return type
    }

    // implement Map.Entry
    @Override
    fun getKey(): String? {
        return getName()
    }

    // implement Map.Entry
    @Override
    fun getValue(): RelDataType? {
        return getType()
    }

    // implement Map.Entry
    @Override
    fun setValue(value: RelDataType?): RelDataType {
        throw UnsupportedOperationException()
    }

    // for debugging
    @Override
    override fun toString(): String {
        return "#$index: $name $type"
    }

    @Override
    override fun isDynamicStar(): Boolean {
        return type.getSqlTypeName() === SqlTypeName.DYNAMIC_STAR
    }
}
