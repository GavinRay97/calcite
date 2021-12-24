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
import java.util.Objects.requireNonNull

/**
 * Root schema element.
 *
 *
 * A POJO with fields of [Boolean], [String], [ArrayList],
 * [LinkedHashMap][java.util.LinkedHashMap], per Jackson simple data
 * binding.
 *
 *
 * Schema structure is as follows:
 *
 *
 * <pre>`Root`
 * [JsonSchema] (in collection [schemas][JsonRoot.schemas])
 * [JsonType] (in collection [types][JsonMapSchema.types])
 * [JsonTable] (in collection [tables][JsonMapSchema.tables])
 * [JsonColumn] (in collection [columns][JsonTable.columns])
 * [JsonStream] (in field [stream][JsonTable.stream])
 * [JsonView]
 * [JsonFunction] (in collection [functions][JsonMapSchema.functions])
 * [JsonLattice] (in collection [lattices][JsonSchema.lattices])
 * [JsonMeasure] (in collection [defaultMeasures][JsonLattice.defaultMeasures])
 * [JsonTile] (in collection [tiles][JsonLattice.tiles])
 * [JsonMeasure] (in collection [measures][JsonTile.measures])
 * [JsonMaterialization] (in collection [materializations][JsonSchema.materializations])
</pre> *
 *
 *
 *
 * See the [JSON
 * model reference](http://calcite.apache.org/docs/model.html).
 */
class JsonRoot @JsonCreator constructor(
    @JsonProperty(value = "version", required = true) version: String?,
    @JsonProperty("defaultSchema") @Nullable defaultSchema: String?
) {
    /** Schema model version number. Required, must have value "1.0".  */
    val version: String

    /** Name of the schema that will become the default schema for connections
     * to Calcite that use this model.
     *
     *
     * Optional, case-sensitive. If specified, there must be a schema in this
     * model with this name.
     */
    @Nullable
    val defaultSchema: String?

    /** List of schema elements.
     *
     *
     * The list may be empty.
     */
    val schemas: List<JsonSchema> = ArrayList()

    init {
        this.version = requireNonNull(version, "version")
        this.defaultSchema = defaultSchema
    }
}
