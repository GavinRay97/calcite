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

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.util.ArrayList
import java.util.List
import java.util.Objects.requireNonNull

/**
 * Table schema element.
 *
 *
 * Occurs within [JsonMapSchema.tables].
 *
 * @see JsonRoot Description of schema elements
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = JsonCustomTable::class)
@JsonSubTypes(
    [JsonSubTypes.Type(
        value = JsonCustomTable::class,
        name = "custom"
    ), JsonSubTypes.Type(value = JsonView::class, name = "view")]
)
abstract class JsonTable protected constructor(name: String?, @Nullable stream: JsonStream?) {
    /** Name of this table.
     *
     *
     * Required. Must be unique within the schema.
     */
    val name: String

    /** Definition of the columns of this table.
     *
     *
     * Required for some kinds of type,
     * optional for others (such as [JsonView]).
     */
    val columns: List<JsonColumn> = ArrayList()

    /** Information about whether the table can be streamed, and if so, whether
     * the history of the table is also available.  */
    @Nullable
    val stream: JsonStream?

    init {
        this.name = requireNonNull(name, "name")
        this.stream = stream
    }

    abstract fun accept(handler: ModelHandler?)
}
