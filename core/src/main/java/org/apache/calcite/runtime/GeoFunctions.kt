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

import org.apache.calcite.linq4j.AbstractEnumerable
import org.apache.calcite.linq4j.Enumerator
import org.apache.calcite.linq4j.function.Deterministic
import org.apache.calcite.linq4j.function.Experimental
import org.apache.calcite.linq4j.function.Hints
import org.apache.calcite.linq4j.function.SemiStrict
import org.apache.calcite.linq4j.function.Strict
import org.apache.calcite.runtime.Geometries.CapStyle
import org.apache.calcite.runtime.Geometries.Geom
import org.apache.calcite.runtime.Geometries.JoinStyle
import com.esri.core.geometry.Envelope
import com.esri.core.geometry.Geometry
import com.esri.core.geometry.GeometryEngine
import com.esri.core.geometry.Line
import com.esri.core.geometry.OperatorBoundary
import com.esri.core.geometry.Point
import com.esri.core.geometry.Polygon
import com.esri.core.geometry.Polyline
import com.esri.core.geometry.SpatialReference
import com.esri.core.geometry.WktExportFlags
import com.esri.core.geometry.WktImportFlags
import java.math.BigDecimal
import java.util.Objects
import org.apache.calcite.runtime.Geometries.NO_SRID
import org.apache.calcite.runtime.Geometries.bind
import org.apache.calcite.runtime.Geometries.buffer
import org.apache.calcite.runtime.Geometries.envelope
import org.apache.calcite.runtime.Geometries.intersects
import org.apache.calcite.runtime.Geometries.makeLine
import org.apache.calcite.runtime.Geometries.point
import org.apache.calcite.runtime.Geometries.todo

/**
 * Helper methods to implement Geo-spatial functions in generated code.
 *
 *
 * Remaining tasks:
 *
 *
 *  * Determine type code for
 * [org.apache.calcite.sql.type.ExtraSqlTypes.GEOMETRY]
 *  * Should we create aliases for functions in upper-case?
 * Without ST_ prefix?
 *  * Consider adding spatial literals, e.g. `GEOMETRY 'POINT (30 10)'`
 *  * Integer arguments, e.g. SELECT ST_MakePoint(1, 2, 1.5),
 * ST_MakePoint(1, 2)
 *  * Are GEOMETRY values comparable? If so add ORDER BY test
 *  * We have to add 'Z' to create 3D objects. This is inconsistent with
 * PostGIS. Who is right? At least document the difference.
 *  * Should add GeometryEngine.intersects; similar to disjoint etc.
 *  * Make [.ST_MakeLine] varargs
 *
 */
@SuppressWarnings(["WeakerAccess", "unused"])
@Deterministic
@Strict
@Experimental
object GeoFunctions {
    // Geometry conversion functions (2D and 3D) ================================
    fun ST_AsText(g: Geom): String {
        return ST_AsWKT(g)
    }

    fun ST_AsWKT(g: Geom): String {
        return GeometryEngine.geometryToWkt(
            g.g(),
            WktExportFlags.wktExportDefaults
        )
    }

    @Nullable
    fun ST_GeomFromText(s: String?): Geom? {
        return ST_GeomFromText(s, NO_SRID)
    }

    @Nullable
    fun ST_GeomFromText(s: String?, srid: Int): Geom? {
        val g: Geometry = fromWkt(
            s,
            WktImportFlags.wktImportDefaults, Geometry.Type.Unknown
        )
        return if (g == null) null else bind(g, srid)
    }

    @Nullable
    fun ST_LineFromText(s: String?): Geom? {
        return ST_GeomFromText(s, NO_SRID)
    }

    @Nullable
    fun ST_LineFromText(wkt: String?, srid: Int): Geom? {
        val g: Geometry = fromWkt(
            wkt,
            WktImportFlags.wktImportDefaults,
            Geometry.Type.Line
        )
        return if (g == null) null else bind(g, srid)
    }

    @Nullable
    fun ST_MPointFromText(s: String?): Geom? {
        return ST_GeomFromText(s, NO_SRID)
    }

    @Nullable
    fun ST_MPointFromText(wkt: String?, srid: Int): Geom? {
        val g: Geometry = fromWkt(
            wkt,
            WktImportFlags.wktImportDefaults,
            Geometry.Type.MultiPoint
        )
        return if (g == null) null else bind(g, srid)
    }

    @Nullable
    fun ST_PointFromText(s: String?): Geom? {
        return ST_GeomFromText(s, NO_SRID)
    }

    @Nullable
    fun ST_PointFromText(wkt: String?, srid: Int): Geom? {
        val g: Geometry = fromWkt(wkt, WktImportFlags.wktImportDefaults, Geometry.Type.Point)
        return if (g == null) null else bind(g, srid)
    }

    @Nullable
    fun ST_PolyFromText(s: String?): Geom? {
        return ST_GeomFromText(s, NO_SRID)
    }

    @Nullable
    fun ST_PolyFromText(wkt: String?, srid: Int): Geom? {
        val g: Geometry = fromWkt(
            wkt,
            WktImportFlags.wktImportDefaults,
            Geometry.Type.Polygon
        )
        return if (g == null) null else bind(g, srid)
    }

    @Nullable
    fun ST_MLineFromText(s: String?): Geom? {
        return ST_GeomFromText(s, NO_SRID)
    }

    @Nullable
    fun ST_MLineFromText(wkt: String?, srid: Int): Geom? {
        val g: Geometry = fromWkt(
            wkt,
            WktImportFlags.wktImportDefaults,
            Geometry.Type.Unknown
        ) // NOTE: there is no Geometry.Type.MultiLine
        return if (g == null) null else bind(g, srid)
    }

    @Nullable
    fun ST_MPolyFromText(s: String?): Geom? {
        return ST_GeomFromText(s, NO_SRID)
    }

    @Nullable
    fun ST_MPolyFromText(wkt: String?, srid: Int): Geom? {
        val g: Geometry = fromWkt(
            wkt,
            WktImportFlags.wktImportDefaults,
            Geometry.Type.Unknown
        ) // NOTE: there is no Geometry.Type.MultiPolygon
        return if (g == null) null else bind(g, srid)
    }
    // Geometry creation functions ==============================================
    /** Calculates a regular grid of polygons based on `geom`.  */
    private fun ST_MakeGrid(
        geom: Geom,
        deltaX: BigDecimal, deltaY: BigDecimal
    ) {
        // This is a dummy function. We cannot include table functions in this
        // package, because they have too many dependencies. See the real definition
        // in SqlGeoFunctions.
    }

    /** Calculates a regular grid of points based on `geom`.  */
    private fun ST_MakeGridPoints(
        geom: Geom,
        deltaX: BigDecimal, deltaY: BigDecimal
    ) {
        // This is a dummy function. We cannot include table functions in this
        // package, because they have too many dependencies. See the real definition
        // in SqlGeoFunctions.
    }

    /** Creates a rectangular Polygon.  */
    fun ST_MakeEnvelope(
        xMin: BigDecimal, yMin: BigDecimal,
        xMax: BigDecimal, yMax: BigDecimal, srid: Int
    ): Geom {
        val geom: Geom? = ST_GeomFromText(
            "POLYGON(("
                    + xMin + " " + yMin + ", "
                    + xMin + " " + yMax + ", "
                    + xMax + " " + yMax + ", "
                    + xMax + " " + yMin + ", "
                    + xMin + " " + yMin + "))", srid
        )
        return Objects.requireNonNull(geom, "geom")
    }

    /** Creates a rectangular Polygon.  */
    fun ST_MakeEnvelope(
        xMin: BigDecimal, yMin: BigDecimal,
        xMax: BigDecimal, yMax: BigDecimal
    ): Geom {
        return ST_MakeEnvelope(xMin, yMin, xMax, yMax, NO_SRID)
    }

    /** Creates a line-string from the given POINTs (or MULTIPOINTs).  */
    @Hints(["SqlKind:ST_MAKE_LINE"])
    fun ST_MakeLine(geom1: Geom?, geom2: Geom?): Geom {
        return makeLine(geom1, geom2)
    }

    @Hints(["SqlKind:ST_MAKE_LINE"])
    fun ST_MakeLine(geom1: Geom?, geom2: Geom?, geom3: Geom?): Geom {
        return makeLine(geom1, geom2, geom3)
    }

    @Hints(["SqlKind:ST_MAKE_LINE"])
    fun ST_MakeLine(
        geom1: Geom?, geom2: Geom?, geom3: Geom?,
        geom4: Geom?
    ): Geom {
        return makeLine(geom1, geom2, geom3, geom4)
    }

    @Hints(["SqlKind:ST_MAKE_LINE"])
    fun ST_MakeLine(
        geom1: Geom?, geom2: Geom?, geom3: Geom?,
        geom4: Geom?, geom5: Geom?
    ): Geom {
        return makeLine(geom1, geom2, geom3, geom4, geom5)
    }

    @Hints(["SqlKind:ST_MAKE_LINE"])
    fun ST_MakeLine(
        geom1: Geom?, geom2: Geom?, geom3: Geom?,
        geom4: Geom?, geom5: Geom?, geom6: Geom?
    ): Geom {
        return makeLine(geom1, geom2, geom3, geom4, geom5, geom6)
    }

    /** Alias for [.ST_Point].  */
    @Hints(["SqlKind:ST_POINT"])
    fun ST_MakePoint(x: BigDecimal, y: BigDecimal): Geom {
        return ST_Point(x, y)
    }

    /** Alias for [.ST_Point].  */
    @Hints(["SqlKind:ST_POINT3"])
    fun ST_MakePoint(x: BigDecimal, y: BigDecimal, z: BigDecimal): Geom {
        return ST_Point(x, y, z)
    }

    /** Constructs a 2D point from coordinates.  */
    @Hints(["SqlKind:ST_POINT"])
    fun ST_Point(x: BigDecimal, y: BigDecimal): Geom {
        // NOTE: Combine the double and BigDecimal variants of this function
        return point(x.doubleValue(), y.doubleValue())
    }

    /** Constructs a 3D point from coordinates.  */
    @Hints(["SqlKind:ST_POINT3"])
    fun ST_Point(x: BigDecimal, y: BigDecimal, z: BigDecimal): Geom {
        val g: Geometry = Point(
            x.doubleValue(), y.doubleValue(),
            z.doubleValue()
        )
        return SimpleGeom(g)
    }
    // Geometry properties (2D and 3D) ==========================================
    /** Returns whether `geom` has at least one z-coordinate.  */
    fun ST_Is3D(geom: Geom): Boolean {
        return geom.g().hasZ()
    }

    /** Returns the x-value of the first coordinate of `geom`.  */
    @Nullable
    fun ST_X(geom: Geom): Double? {
        return if (geom.g() is Point) (geom.g() as Point).getX() else null
    }

    /** Returns the y-value of the first coordinate of `geom`.  */
    @Nullable
    fun ST_Y(geom: Geom): Double? {
        return if (geom.g() is Point) (geom.g() as Point).getY() else null
    }

    /** Returns the z-value of the first coordinate of `geom`.  */
    @Nullable
    fun ST_Z(geom: Geom): Double? {
        return if (geom.g().getDescription().hasZ() && geom.g() is Point) (geom.g() as Point).getZ() else null
    }

    /** Returns the boundary of `geom`.  */
    fun ST_Boundary(geom: Geom): Geom {
        val op: OperatorBoundary = OperatorBoundary.local()
        val result: Geometry = op.execute(geom.g(), null)
        return geom.wrap(result)
    }

    /** Returns the distance between `geom1` and `geom2`.  */
    fun ST_Distance(geom1: Geom, geom2: Geom): Double {
        return GeometryEngine.distance(geom1.g(), geom2.g(), geom1.sr())
    }

    /** Returns the type of `geom`.  */
    fun ST_GeometryType(geom: Geom): String {
        return Geometries.type(geom.g()).name()
    }

    /** Returns the OGC SFS type code of `geom`.  */
    fun ST_GeometryTypeCode(geom: Geom): Int {
        return Geometries.type(geom.g()).code
    }

    /** Returns the minimum bounding box of `geom` (which may be a
     * GEOMETRYCOLLECTION).  */
    fun ST_Envelope(geom: Geom): Geom {
        val env: Envelope = envelope(geom.g())
        return geom.wrap(env)
    }
    // Geometry predicates ======================================================
    /** Returns whether `geom1` contains `geom2`.  */
    @Hints(["SqlKind:ST_CONTAINS"])
    fun ST_Contains(geom1: Geom, geom2: Geom): Boolean {
        return GeometryEngine.contains(geom1.g(), geom2.g(), geom1.sr())
    }

    /** Returns whether `geom1` contains `geom2` but does not
     * intersect its boundary.  */
    fun ST_ContainsProperly(geom1: Geom, geom2: Geom): Boolean {
        return (GeometryEngine.contains(geom1.g(), geom2.g(), geom1.sr())
                && !GeometryEngine.crosses(geom1.g(), geom2.g(), geom1.sr()))
    }

    /** Returns whether no point in `geom2` is outside `geom1`.  */
    private fun ST_Covers(geom1: Geom, geom2: Geom): Boolean {
        throw todo()
    }

    /** Returns whether `geom1` crosses `geom2`.  */
    fun ST_Crosses(geom1: Geom, geom2: Geom): Boolean {
        return GeometryEngine.crosses(geom1.g(), geom2.g(), geom1.sr())
    }

    /** Returns whether `geom1` and `geom2` are disjoint.  */
    fun ST_Disjoint(geom1: Geom, geom2: Geom): Boolean {
        return GeometryEngine.disjoint(geom1.g(), geom2.g(), geom1.sr())
    }

    /** Returns whether the envelope of `geom1` intersects the envelope of
     * `geom2`.  */
    fun ST_EnvelopesIntersect(geom1: Geom, geom2: Geom): Boolean {
        val e1: Geometry = envelope(geom1.g())
        val e2: Geometry = envelope(geom2.g())
        return intersects(e1, e2, geom1.sr())
    }

    /** Returns whether `geom1` equals `geom2`.  */
    fun ST_Equals(geom1: Geom, geom2: Geom): Boolean {
        return GeometryEngine.equals(geom1.g(), geom2.g(), geom1.sr())
    }

    /** Returns whether `geom1` intersects `geom2`.  */
    fun ST_Intersects(geom1: Geom, geom2: Geom): Boolean {
        val g1: Geometry = geom1.g()
        val g2: Geometry = geom2.g()
        val sr: SpatialReference = geom1.sr()
        return intersects(g1, g2, sr)
    }

    /** Returns whether `geom1` equals `geom2` and their coordinates
     * and component Geometries are listed in the same order.  */
    fun ST_OrderingEquals(geom1: Geom, geom2: Geom): Boolean {
        return GeometryEngine.equals(geom1.g(), geom2.g(), geom1.sr())
    }

    /** Returns `geom1` overlaps `geom2`.  */
    fun ST_Overlaps(geom1: Geom, geom2: Geom): Boolean {
        return GeometryEngine.overlaps(geom1.g(), geom2.g(), geom1.sr())
    }

    /** Returns whether `geom1` touches `geom2`.  */
    fun ST_Touches(geom1: Geom, geom2: Geom): Boolean {
        return GeometryEngine.touches(geom1.g(), geom2.g(), geom1.sr())
    }

    /** Returns whether `geom1` is within `geom2`.  */
    fun ST_Within(geom1: Geom, geom2: Geom): Boolean {
        return GeometryEngine.within(geom1.g(), geom2.g(), geom1.sr())
    }

    /** Returns whether `geom1` and `geom2` are within
     * `distance` of each other.  */
    @Hints(["SqlKind:ST_DWITHIN"])
    fun ST_DWithin(geom1: Geom, geom2: Geom, distance: Double): Boolean {
        val distance1: Double = GeometryEngine.distance(geom1.g(), geom2.g(), geom1.sr())
        return distance1 <= distance
    }
    // Geometry operators (2D and 3D) ===========================================
    /** Computes a buffer around `geom`.  */
    fun ST_Buffer(geom: Geom, distance: Double): Geom {
        val g: Polygon = GeometryEngine.buffer(geom.g(), geom.sr(), distance)
        return geom.wrap(g)
    }

    /** Computes a buffer around `geom` with .  */
    fun ST_Buffer(geom: Geom?, distance: Double, quadSegs: Int): Geom {
        throw todo()
    }

    /** Computes a buffer around `geom`.  */
    fun ST_Buffer(geom: Geom?, bufferSize: Double, style: String): Geom {
        var quadSegCount = 8
        var endCapStyle: CapStyle = CapStyle.ROUND
        var joinStyle: JoinStyle = JoinStyle.ROUND
        var mitreLimit = 5f
        var i = 0
        parse@ while (true) {
            val equals: Int = style.indexOf('=', i)
            if (equals < 0) {
                break
            }
            var space: Int = style.indexOf(' ', equals)
            if (space < 0) {
                space = style.length()
            }
            val name: String = style.substring(i, equals)
            val value: String = style.substring(equals + 1, space)
            when (name) {
                "quad_segs" -> quadSegCount = Integer.parseInt(value)
                "endcap" -> endCapStyle = CapStyle.of(value)
                "join" -> joinStyle = JoinStyle.of(value)
                "mitre_limit", "miter_limit" -> mitreLimit = Float.parseFloat(value)
                else -> {}
            }
            i = space
            while (true) {
                if (i >= style.length()) {
                    break@parse
                }
                if (style.charAt(i) !== ' ') {
                    break
                }
                ++i
            }
        }
        return buffer(
            geom, bufferSize, quadSegCount, endCapStyle, joinStyle,
            mitreLimit
        )
    }

    /** Computes the union of `geom1` and `geom2`.  */
    fun ST_Union(geom1: Geom, geom2: Geom): Geom {
        val sr: SpatialReference = geom1.sr()
        val g: Geometry = GeometryEngine.union(arrayOf<Geometry>(geom1.g(), geom2.g()), sr)
        return bind(g, sr)
    }

    /** Computes the union of the geometries in `geomCollection`.  */
    @SemiStrict
    fun ST_Union(geomCollection: Geom): Geom {
        val sr: SpatialReference = geomCollection.sr()
        val g: Geometry = GeometryEngine.union(arrayOf<Geometry>(geomCollection.g()), sr)
        return bind(g, sr)
    }
    // Geometry projection functions ============================================
    /** Transforms `geom` from one coordinate reference
     * system (CRS) to the CRS specified by `srid`.  */
    fun ST_Transform(geom: Geom, srid: Int): Geom {
        return geom.transform(srid)
    }

    /** Returns a copy of `geom` with a new SRID.  */
    fun ST_SetSRID(geom: Geom, srid: Int): Geom {
        return geom.transform(srid)
    }
    // Space-filling curves
    /** Returns the position of a point on the Hilbert curve,
     * or null if it is not a 2-dimensional point.  */
    @Hints(["SqlKind:HILBERT"])
    @Nullable
    fun hilbert(geom: Geom): Long? {
        val g: Geometry = geom.g()
        if (g is Point) {
            val x: Double = (g as Point).getX()
            val y: Double = (g as Point).getY()
            return HilbertCurve2D(8).toIndex(x, y)
        }
        return null
    }

    /** Returns the position of a point on the Hilbert curve.  */
    @Hints(["SqlKind:HILBERT"])
    fun hilbert(x: BigDecimal, y: BigDecimal): Long {
        return HilbertCurve2D(8).toIndex(x.doubleValue(), y.doubleValue())
    }

    /** Creates a geometry from a WKT.
     * If the engine returns a null, throws; never returns null.  */
    @Nullable
    private fun fromWkt(
        wkt: String?, importFlags: Int,
        geometryType: Geometry.Type
    ): Geometry {
        return GeometryEngine.geometryFromWkt(wkt, importFlags, geometryType)
    }
    // Inner classes ============================================================
    /** Used at run time by the [.ST_MakeGrid] and
     * [.ST_MakeGridPoints] functions.  */
    class GridEnumerable(
        envelope: Envelope, deltaX: BigDecimal,
        deltaY: BigDecimal, point: Boolean
    ) : AbstractEnumerable<Array<Object?>?>() {
        private val envelope: Envelope
        private val point: Boolean
        private val deltaX: Double
        private val deltaY: Double
        private val minX: Double
        private val minY: Double
        private val baseX: Int
        private val baseY: Int
        private val spanX: Int
        private val spanY: Int
        private val area: Int

        init {
            this.envelope = envelope
            this.deltaX = deltaX.doubleValue()
            this.deltaY = deltaY.doubleValue()
            this.point = point
            spanX = Math.floor(
                (envelope.getXMax() - envelope.getXMin())
                        / this.deltaX
            ) as Int + 1
            baseX = Math.floor(envelope.getXMin() / this.deltaX)
            minX = this.deltaX * baseX
            spanY = Math.floor(
                (envelope.getYMax() - envelope.getYMin())
                        / this.deltaY
            ) as Int + 1
            baseY = Math.floor(envelope.getYMin() / this.deltaY)
            minY = this.deltaY * baseY
            area = spanX * spanY
        }

        @Override
        fun enumerator(): Enumerator<Array<Object>> {
            return object : Enumerator<Array<Object?>?>() {
                var id = -1
                @Override
                fun current(): Array<Object> {
                    val geom: Geom
                    val x = id % spanX
                    val y = id / spanX
                    if (point) {
                        val xCurrent = minX + (x + 0.5) * deltaX
                        val yCurrent = minY + (y + 0.5) * deltaY
                        geom = ST_MakePoint(
                            BigDecimal.valueOf(xCurrent),
                            BigDecimal.valueOf(yCurrent)
                        )
                    } else {
                        val polygon = Polygon()
                        val left = minX + x * deltaX
                        val right = left + deltaX
                        val bottom = minY + y * deltaY
                        val top = bottom + deltaY
                        val polyline = Polyline()
                        polyline.addSegment(Line(left, bottom, right, bottom), true)
                        polyline.addSegment(Line(right, bottom, right, top), false)
                        polyline.addSegment(Line(right, top, left, top), false)
                        polyline.addSegment(Line(left, top, left, bottom), false)
                        polygon.add(polyline, false)
                        geom = SimpleGeom(polygon)
                    }
                    return arrayOf(geom, id, x + 1, y + 1, baseX + x, baseY + y)
                }

                @Override
                fun moveNext(): Boolean {
                    return ++id < area
                }

                @Override
                fun reset() {
                    id = -1
                }

                @Override
                fun close() {
                }
            }
        }
    }
}
