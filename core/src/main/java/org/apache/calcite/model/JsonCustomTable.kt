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
import java.util.Map
import java.util.Objects.requireNonNull

/**
 * Custom table schema element.
 *
 *
 * Like base class [JsonTable],
 * occurs within [JsonMapSchema.tables].
 *
 * @see JsonRoot Description of schema elements
 */
class JsonCustomTable @JsonCreator constructor(
    @JsonProperty(value = "name", required = true) name: String?,
    @JsonProperty("stream") stream: JsonStream?,
    @JsonProperty(value = "factory", required = true) factory: String?,
    @JsonProperty("operand") @Nullable operand: Map<String?, Object>
) : JsonTable(name, stream) {
    /** Name of the factory class for this table.
     *
     *
     * Required. Must implement interface
     * [org.apache.calcite.schema.TableFactory] and have a public default
     * constructor.
     */
    val factory: String

    /** Contains attributes to be passed to the factory.
     *
     *
     * May be a JSON object (represented as Map) or null.
     */
    @Nullable
    val operand: Map<String?, Object>

    init {
        this.factory = requireNonNull(factory, "factory")
        this.operand = operand
    }

    @Override
    fun accept(handler: ModelHandler) {
        handler.visit(this)
    }
}
