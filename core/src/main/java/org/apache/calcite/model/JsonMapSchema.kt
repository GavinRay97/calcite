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
import java.util.ArrayList
import java.util.List

/**
 * JSON object representing a schema whose tables are explicitly specified.
 *
 *
 * Like the base class [JsonSchema],
 * occurs within [JsonRoot.schemas].
 *
 * @see JsonRoot Description of JSON schema elements
 */
class JsonMapSchema @JsonCreator constructor(
    @JsonProperty(value = "name", required = true) name: String,
    @JsonProperty("path") @Nullable path: List<Object>,
    @JsonProperty("cache") @Nullable cache: Boolean?,
    @JsonProperty("autoLattice") @Nullable autoLattice: Boolean
) : JsonSchema(name, path, cache, autoLattice) {
    /** Tables in this schema.
     *
     *
     * The list may be empty.
     */
    val tables: List<JsonTable> = ArrayList()

    /** Types in this schema.
     *
     *
     * The list may be empty.
     */
    val types: List<JsonType> = ArrayList()

    /** Functions in this schema.
     *
     *
     * The list may be empty.
     */
    val functions: List<JsonFunction> = ArrayList()
    @Override
    fun accept(handler: ModelHandler) {
        handler.visit(this)
    }

    @Override
    override fun visitChildren(modelHandler: ModelHandler) {
        super.visitChildren(modelHandler)
        for (jsonTable in tables) {
            jsonTable.accept(modelHandler)
        }
        for (jsonFunction in functions) {
            jsonFunction.accept(modelHandler)
        }
        for (jsonType in types) {
            jsonType.accept(modelHandler)
        }
    }
}
