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
package org.apache.calcite.model

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import java.util.List
import java.util.Objects.requireNonNull

/**
 * Function schema element.
 *
 * @see JsonRoot Description of schema elements
 */
class JsonFunction @JsonCreator constructor(
    /** Name of this function.
     *
     *
     * Required.
     */
    @param:JsonProperty("name") val name: String,
    @JsonProperty(value = "className", required = true) className: String?,
    @JsonProperty("methodName") @Nullable methodName: String,
    @JsonProperty("path") @Nullable path: List<String>
) {
    /** Name of the class that implements this function.
     *
     *
     * Required.
     */
    val className: String

    /** Name of the method that implements this function.
     *
     *
     * Optional.
     *
     *
     * If specified, the method must exist (case-sensitive) and Calcite
     * will create a scalar function. The method may be static or non-static, but
     * if non-static, the class must have a public constructor with no parameters.
     *
     *
     * If "*", Calcite creates a function for every method
     * in this class.
     *
     *
     * If not specified, Calcite looks for a method called "eval", and
     * if found, creates a a table macro or scalar function.
     * It also looks for methods "init", "add", "merge", "result", and
     * if found, creates an aggregate function.
     */
    @Nullable
    val methodName: String

    /** Path for resolving this function.
     *
     *
     * Optional.
     */
    @Nullable
    val path: List<String>

    init {
        this.className = requireNonNull(className, "className")
        this.methodName = methodName
        this.path = path
    }

    fun accept(handler: ModelHandler) {
        handler.visit(this)
    }
}
