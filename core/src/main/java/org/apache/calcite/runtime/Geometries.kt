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
package org.apache.calcite.runtime

import org.apache.calcite.linq4j.function.Deterministic
import org.apache.calcite.linq4j.function.Experimental
import org.apache.calcite.linq4j.function.Strict
import org.apache.calcite.util.Util
import com.esri.core.geometry.Envelope
import com.esri.core.geometry.Geometry
import com.esri.core.geometry.Line
import com.esri.core.geometry.MapGeometry
import com.esri.core.geometry.Operator
import com.esri.core.geometry.OperatorFactoryLocal
import com.esri.core.geometry.OperatorIntersects
import com.esri.core.geometry.Point
import com.esri.core.geometry.Polyline
import com.esri.core.geometry.SpatialReference
import com.google.common.collect.ImmutableList
import java.util.Objects

/**
 * Utilities for geometry.
 */
@SuppressWarnings(["WeakerAccess", "unused"])
@Deterministic
@Strict
@Experimental
object Geometries {
    const val NO_SRID = 0
    private val SPATIAL_REFERENCE: SpatialReference = SpatialReference.create(4326)
    fun todo(): UnsupportedOperationException {
        return UnsupportedOperationException()
    }

    /** Returns a Geom that is a Geometry bound to a SRID.  */
    internal fun bind(geometry: Geometry?, srid: Int): Geom {
        return if (srid == NO_SRID) {
            SimpleGeom(geometry)
        } else bind(geometry, SpatialReference.create(srid))
    }

    fun bind(geometry: Geometry?, sr: SpatialReference?): MapGeom {
        return MapGeom(MapGeometry(geometry, sr))
    }

    fun makeLine(vararg geoms: Geom?): Geom {
        return makeLine(ImmutableList.copyOf(geoms))
    }

    fun makeLine(geoms: Iterable<Geom>): Geom {
        val g = Polyline()
        var p: Point? = null
        for (geom in geoms) {
            if (geom.g() is Point) {
                val prev: Point? = p
                p = geom.g() as Point
                if (prev != null) {
                    val line = Line()
                    line.setStart(prev)
                    line.setEnd(p)
                    g.addSegment(line, false)
                }
            }
        }
        return SimpleGeom(g)
    }

    fun point(x: Double, y: Double): Geom {
        val g: Geometry = Point(x, y)
        return SimpleGeom(g)
    }

    /** Returns the OGC type of a geometry.  */
    fun type(g: Geometry): Type {
        return when (g.getType()) {
            Point -> Type.POINT
            Polyline -> Type.LINESTRING
            Polygon -> Type.POLYGON
            MultiPoint -> Type.MULTIPOINT
            Envelope -> Type.POLYGON
            Line -> Type.LINESTRING
            Unknown -> Type.Geometry
            else -> throw AssertionError(g)
        }
    }

    fun envelope(g: Geometry): Envelope {
        val env = Envelope()
        g.queryEnvelope(env)
        return env
    }

    fun intersects(
        g1: Geometry?, g2: Geometry?,
        sr: SpatialReference?
    ): Boolean {
        val op: OperatorIntersects = OperatorFactoryLocal
            .getInstance().getOperator(Operator.Type.Intersects) as OperatorIntersects
        return op.execute(g1, g2, sr, null)
    }

    fun buffer(
        geom: Geom?, bufferSize: Double,
        quadSegCount: Int, endCapStyle: CapStyle, joinStyle: JoinStyle,
        mitreLimit: Float
    ): Geom {
        Util.discard(
            endCapStyle.toString() + ":" + joinStyle + ":" + mitreLimit
                    + ":" + quadSegCount
        )
        throw todo()
    }

    /** How the "buffer" command terminates the end of a line.  */
    enum class CapStyle {
        ROUND, FLAT, SQUARE;

        companion object {
            fun of(value: String): CapStyle {
                return when (value) {
                    "round" -> ROUND
                    "flat", "butt" -> FLAT
                    "square" -> SQUARE
                    else -> throw IllegalArgumentException("unknown endcap value: $value")
                }
            }
        }
    }

    /** How the "buffer" command decorates junctions between line segments.  */
    enum class JoinStyle {
        ROUND, MITRE, BEVEL;

        companion object {
            fun of(value: String): JoinStyle {
                return when (value) {
                    "round" -> ROUND
                    "mitre", "miter" -> MITRE
                    "bevel" -> BEVEL
                    else -> throw IllegalArgumentException("unknown join value: $value")
                }
            }
        }
    }

    /** Geometry types, with the names and codes assigned by OGC.  */
    enum class Type(val code: Int) {
        Geometry(0), POINT(1), LINESTRING(2), POLYGON(3), MULTIPOINT(4), MULTILINESTRING(5), MULTIPOLYGON(6), GEOMCOLLECTION(
            7
        ),
        CURVE(13), SURFACE(14), POLYHEDRALSURFACE(15);
    }

    /** Geometry. It may or may not have a spatial reference
     * associated with it.  */
    interface Geom : Comparable<Geom?> {
        fun g(): Geometry
        fun type(): Type?
        fun sr(): SpatialReference?
        fun transform(srid: Int): Geom
        fun wrap(g: Geometry?): Geom
    }

    /** Sub-class of geometry that has no spatial reference.  */
    internal class SimpleGeom(g: Geometry?) : Geom {
        val g: Geometry

        init {
            this.g = Objects.requireNonNull(g, "g")
        }

        @Override
        override fun toString(): String {
            return g.toString()
        }

        @Override
        operator fun compareTo(o: Geom): Int {
            return toString().compareTo(o.toString())
        }

        @Override
        override fun g(): Geometry {
            return g
        }

        @Override
        override fun type(): Type {
            return type(g)
        }

        @Override
        override fun sr(): SpatialReference {
            return SPATIAL_REFERENCE
        }

        @Override
        override fun transform(srid: Int): Geom {
            return if (srid == SPATIAL_REFERENCE.getID()) {
                this
            } else bind(g, srid)
        }

        @Override
        override fun wrap(g: Geometry?): Geom {
            return SimpleGeom(g)
        }
    }

    /** Sub-class of geometry that has a spatial reference.  */
    class MapGeom(mg: MapGeometry?) : Geom {
        val mg: MapGeometry

        init {
            this.mg = Objects.requireNonNull(mg, "mg")
        }

        @Override
        override fun toString(): String {
            return mg.toString()
        }

        @Override
        operator fun compareTo(o: Geom): Int {
            return toString().compareTo(o.toString())
        }

        @Override
        override fun g(): Geometry {
            return mg.getGeometry()
        }

        @Override
        override fun type(): Type {
            return type(mg.getGeometry())
        }

        @Override
        override fun sr(): SpatialReference {
            return mg.getSpatialReference()
        }

        @Override
        override fun transform(srid: Int): Geom {
            if (srid == NO_SRID) {
                return SimpleGeom(mg.getGeometry())
            }
            return if (srid == mg.getSpatialReference().getID()) {
                this
            } else bind(Objects.requireNonNull(mg.getGeometry()), srid)
        }

        @Override
        override fun wrap(g: Geometry?): Geom {
            return bind(g, mg.getSpatialReference())
        }
    }
}
