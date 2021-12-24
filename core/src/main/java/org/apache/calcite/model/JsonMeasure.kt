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
import java.util.Objects.requireNonNull

/**
 * An aggregate function applied to a column (or columns) of a lattice.
 *
 *
 * Occurs in a [org.apache.calcite.model.JsonTile],
 * and there is a default list in
 * [org.apache.calcite.model.JsonLattice].
 *
 * @see JsonRoot Description of schema elements
 */
class JsonMeasure @JsonCreator constructor(
    @JsonProperty(value = "agg", required = true) agg: String?,
    @JsonProperty("args") @Nullable args: Object?
) {
    /** The name of an aggregate function.
     *
     *
     * Required. Usually `count`, `sum`,
     * `min`, `max`.
     */
    val agg: String

    /** Arguments to the measure.
     *
     *
     * Valid values are:
     *
     *  * Not specified: no arguments
     *  * null: no arguments
     *  * Empty list: no arguments
     *  * String: single argument, the name of a lattice column
     *  * List: multiple arguments, each a column name
     *
     *
     *
     * Unlike lattice dimensions, measures can not be specified in qualified
     * format, `["table", "column"]`. When you define a lattice, make sure
     * that each column you intend to use as a measure has a unique name within
     * the lattice (using "`AS alias`" if necessary).
     */
    @Nullable
    val args: Object?

    init {
        this.agg = requireNonNull(agg, "agg")
        this.args = args
    }

    fun accept(modelHandler: ModelHandler) {
        modelHandler.visit(this)
    }
}
