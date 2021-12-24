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

import java.util.Map

/**
 * RelDataTypeField represents the definition of a field in a structured
 * [RelDataType].
 *
 *
 * Extends the [java.util.Map.Entry] interface to allow convenient
 * inter-operation with Java collections classes. In any implementation of this
 * interface, [.getKey] must be equivalent to [.getName]
 * and [.getValue] must be equivalent to [.getType].
 */
interface RelDataTypeField : Map.Entry<String?, RelDataType?> {
    /**
     * Function to transform a set of [RelDataTypeField] to
     * a set of [Integer] of the field keys.
     *
     */
    @Deprecated // to be removed before 2.0
    @SuppressWarnings("nullability")
    @Deprecated("Use {@code RelDataTypeField::getIndex}")
    class ToFieldIndex : com.google.common.base.Function<RelDataTypeField?, Integer?> {
        @Override
        fun apply(o: RelDataTypeField): Integer {
            return o.getIndex()
        }
    }

    /**
     * Function to transform a set of [RelDataTypeField] to
     * a set of [String] of the field names.
     *
     */
    @Deprecated // to be removed before 2.0
    @SuppressWarnings("nullability")
    @Deprecated("Use {@code RelDataTypeField::getName}")
    class ToFieldName : com.google.common.base.Function<RelDataTypeField?, String?> {
        @Override
        fun apply(o: RelDataTypeField): String {
            return o.getName()
        }
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Gets the name of this field, which is unique within its containing type.
     *
     * @return field name
     */
    fun getName(): String

    /**
     * Gets the ordinal of this field within its containing type.
     *
     * @return 0-based ordinal
     */
    fun getIndex(): Int

    /**
     * Gets the type of this field.
     *
     * @return field type
     */
    fun getType(): RelDataType

    /**
     * Returns true if this is a dynamic star field.
     */
    fun isDynamicStar(): Boolean
}
