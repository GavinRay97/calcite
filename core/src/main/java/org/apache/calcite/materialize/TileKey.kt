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
package org.apache.calcite.materialize

import org.apache.calcite.util.ImmutableBitSet

/** Definition of a particular combination of dimensions and measures of a
 * lattice that is the basis of a materialization.
 *
 *
 * Holds similar information to a
 * [org.apache.calcite.materialize.Lattice.Tile] but a lattice is
 * immutable and tiles are not added after their creation.  */
class TileKey(
    lattice: Lattice, dimensions: ImmutableBitSet,
    measures: ImmutableList<Lattice.Measure?>
) {
    val lattice: Lattice
    val dimensions: ImmutableBitSet
    val measures: ImmutableList<Lattice.Measure>

    /** Creates a TileKey.  */
    init {
        this.lattice = lattice
        this.dimensions = dimensions
        this.measures = measures
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(lattice, dimensions)
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (obj === this
                || (obj is TileKey
                && lattice === (obj as TileKey).lattice && dimensions.equals((obj as TileKey).dimensions)
                && measures.equals((obj as TileKey).measures)))
    }

    @Override
    override fun toString(): String {
        return "dimensions: $dimensions, measures: $measures"
    }
}
