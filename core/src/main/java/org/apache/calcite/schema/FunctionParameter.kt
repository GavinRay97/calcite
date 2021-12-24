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
package org.apache.calcite.schema

import org.apache.calcite.rel.type.RelDataType

/**
 * Parameter to a [Function].
 *
 *
 * NOTE: We'd have called it `Parameter` but the overlap with
 * [java.lang.reflect.Parameter] was too confusing.
 */
interface FunctionParameter {
    /**
     * Zero-based ordinal of this parameter within the member's parameter
     * list.
     *
     * @return Parameter ordinal
     */
    val ordinal: Int

    /**
     * Name of the parameter.
     *
     * @return Parameter name
     */
    val name: String?

    /**
     * Returns the type of this parameter.
     *
     * @param typeFactory Type factory to be used to create the type
     *
     * @return Parameter type.
     */
    fun getType(typeFactory: RelDataTypeFactory?): RelDataType

    /**
     * Returns whether this parameter is optional.
     *
     *
     * If true, the value of the parameter can be supplied using the DEFAULT
     * SQL keyword, or it can be omitted from a function called using argument
     * assignment, or the function can be called with fewer parameters (if all
     * parameters after it are optional too).
     *
     *
     * If a parameter is optional its default value is NULL. We may in future
     * allow functions to specify other default values.
     */
    val isOptional: Boolean
}
