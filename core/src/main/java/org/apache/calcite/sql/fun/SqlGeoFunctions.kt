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
package org.apache.calcite.sql.`fun`

import org.apache.calcite.DataContext

/**
 * Utilities for Geo/Spatial functions.
 *
 *
 * Includes some table functions, and may in future include other functions
 * that have dependencies beyond the `org.apache.calcite.runtime` package.
 */
object SqlGeoFunctions {
    // Geometry table functions =================================================
    /** Calculates a regular grid of polygons based on `geom`.
     *
     * @see GeoFunctions ST_MakeGrid
     */
    @SuppressWarnings(["WeakerAccess", "unused"])
    fun ST_MakeGrid(
        geom: Geom?,
        deltaX: BigDecimal?, deltaY: BigDecimal?
    ): ScannableTable {
        return GridTable(geom, deltaX, deltaY, false)
    }

    /** Calculates a regular grid of points based on `geom`.
     *
     * @see GeoFunctions ST_MakeGridPoints
     */
    @SuppressWarnings(["WeakerAccess", "unused"])
    fun ST_MakeGridPoints(
        geom: Geom?,
        deltaX: BigDecimal?, deltaY: BigDecimal?
    ): ScannableTable {
        return GridTable(geom, deltaX, deltaY, true)
    }

    /** Returns the points or rectangles in a grid that covers a given
     * geometry.  */
    class GridTable internal constructor(
        geom: Geom?, deltaX: BigDecimal?, deltaY: BigDecimal?,
        point: Boolean
    ) : ScannableTable {
        private val geom: Geom?
        private val deltaX: BigDecimal?
        private val deltaY: BigDecimal?
        private val point: Boolean

        init {
            this.geom = geom
            this.deltaX = deltaX
            this.deltaY = deltaY
            this.point = point
        }

        @Override
        fun getRowType(typeFactory: RelDataTypeFactory): RelDataType {
            return typeFactory.builder() // a point (for ST_MakeGridPoints) or a rectangle (for ST_MakeGrid)
                .add("THE_GEOM", SqlTypeName.GEOMETRY) // in [0, width * height)
                .add("ID", SqlTypeName.INTEGER) // in [1, width]
                .add("ID_COL", SqlTypeName.INTEGER) // in [1, height]
                .add("ID_ROW", SqlTypeName.INTEGER) // absolute column, with 0 somewhere near the origin; not standard
                .add("ABS_COL", SqlTypeName.INTEGER) // absolute row, with 0 somewhere near the origin; not standard
                .add("ABS_ROW", SqlTypeName.INTEGER)
                .build()
        }

        @Override
        fun scan(root: DataContext?): Enumerable<Array<Object>> {
            if (geom != null && deltaX != null && deltaY != null) {
                val geometry: Geometry = geom.g()
                val envelope = Envelope()
                geometry.queryEnvelope(envelope)
                if (deltaX.compareTo(BigDecimal.ZERO) > 0
                    && deltaY.compareTo(BigDecimal.ZERO) > 0
                ) {
                    return GridEnumerable(
                        envelope, deltaX, deltaY,
                        point
                    )
                }
            }
            return Linq4j.emptyEnumerable()
        }

        @get:Override
        val statistic: Statistic
            get() = Statistics.of(100.0, ImmutableList.of(ImmutableBitSet.of(0, 1)))

        @get:Override
        val jdbcTableType: Schema.TableType
            get() = Schema.TableType.OTHER

        @Override
        fun isRolledUp(column: String?): Boolean {
            return false
        }

        @Override
        fun rolledUpColumnValidInsideAgg(
            column: String?, call: SqlCall?,
            @Nullable parent: SqlNode?, @Nullable config: CalciteConnectionConfig?
        ): Boolean {
            return false
        }
    }
}
