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
package org.apache.calcite.rel.rules

import org.apache.calcite.plan.RelOptPredicateList

/**
 * Collection of planner rules that convert
 * calls to spatial functions into more efficient expressions.
 *
 *
 * The rules allow Calcite to use spatial indexes. For example the following
 * query:
 *
 * <blockquote>SELECT ...
 * FROM Restaurants AS r
 * WHERE ST_DWithin(ST_Point(10, 20), ST_Point(r.longitude, r.latitude), 5)
</blockquote> *
 *
 *
 * is rewritten to
 *
 * <blockquote>SELECT ...
 * FROM Restaurants AS r
 * WHERE (r.h BETWEEN 100 AND 150
 * OR r.h BETWEEN 170 AND 185)
 * AND ST_DWithin(ST_Point(10, 20), ST_Point(r.longitude, r.latitude), 5)
</blockquote> *
 *
 *
 * if there is the constraint
 *
 * <blockquote>CHECK (h = Hilbert(8, r.longitude, r.latitude))</blockquote>
 *
 *
 * If the `Restaurants` table is sorted on `h` then the latter
 * query can be answered using two limited range-scans, and so is much more
 * efficient.
 *
 *
 * Note that the original predicate
 * `ST_DWithin(ST_Point(10, 20), ST_Point(r.longitude, r.latitude), 5)`
 * is still present, but is evaluated after the approximate predicate has
 * eliminated many potential matches.
 */
@Value.Enclosing
object SpatialRules {
    private val DWITHIN_FINDER: RexUtil.RexFinder = RexUtil.find(EnumSet.of(SqlKind.ST_DWITHIN, SqlKind.ST_CONTAINS))
    private val HILBERT_FINDER: RexUtil.RexFinder = RexUtil.find(SqlKind.HILBERT)
    val INSTANCE: RelOptRule = FilterHilbertRule.Config.DEFAULT.toRule()

    /** Returns a geometry if an expression is constant, null otherwise.  */
    private fun constantGeom(e: RexNode): @Nullable Geometries.Geom? {
        return when (e.getKind()) {
            CAST -> constantGeom((e as RexCall).getOperands().get(0))
            LITERAL -> (e as RexLiteral).getValue() as Geometries.Geom
            else -> null
        }
    }

    /** Rule that converts ST_DWithin in a Filter condition into a predicate on
     * a Hilbert curve.  */
    @SuppressWarnings("WeakerAccess")
    class FilterHilbertRule protected constructor(config: Config?) : RelRule<FilterHilbertRule.Config?>(config) {
        @Override
        fun onMatch(call: RelOptRuleCall) {
            val filter: Filter = call.rel(0)
            val conjunctions: List<RexNode> = ArrayList()
            RelOptUtil.decomposeConjunction(filter.getCondition(), conjunctions)

            // Match a predicate
            //   r.hilbert = hilbert(r.longitude, r.latitude)
            // to one of the conjunctions
            //   ST_DWithin(ST_Point(x, y), ST_Point(r.longitude, r.latitude), d)
            // and if it matches add a new conjunction before it,
            //   r.hilbert between h1 and h2
            //   or r.hilbert between h3 and h4
            // where {[h1, h2], [h3, h4]} are the ranges of the Hilbert curve
            // intersecting the square
            //   (r.longitude - d, r.latitude - d, r.longitude + d, r.latitude + d)
            val predicates: RelOptPredicateList = call.getMetadataQuery().getAllPredicates(filter.getInput()) ?: return
            var changeCount = 0
            for (predicate in predicates.pulledUpPredicates) {
                val builder: RelBuilder = call.builder()
                if (predicate.getKind() === SqlKind.EQUALS) {
                    val eqCall: RexCall = predicate as RexCall
                    if (eqCall.operands.get(0) is RexInputRef
                        && eqCall.operands.get(1).getKind() === SqlKind.HILBERT
                    ) {
                        val ref: RexInputRef = eqCall.operands.get(0) as RexInputRef
                        val hilbert: RexCall = eqCall.operands.get(1) as RexCall
                        val finder: RexUtil.RexFinder = RexUtil.find(ref)
                        if (finder.anyContain(conjunctions)) {
                            // If the condition already contains "ref", it is probable that
                            // this rule has already fired once.
                            continue
                        }
                        var i = 0
                        while (i < conjunctions.size()) {
                            val replacements: List<RexNode>? = replaceSpatial(
                                conjunctions[i], builder, ref, hilbert
                            )
                            if (replacements != null) {
                                conjunctions.remove(i)
                                conjunctions.addAll(i, replacements)
                                i += replacements.size()
                                ++changeCount
                            } else {
                                ++i
                            }
                        }
                    }
                }
                if (changeCount > 0) {
                    call.transformTo(
                        builder.push(filter.getInput())
                            .filter(conjunctions)
                            .build()
                    )
                    return  // we found one useful constraint; don't look for more
                }
            }
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface Config : RelRule.Config {
            @Override
            fun toRule(): FilterHilbertRule {
                return FilterHilbertRule(this)
            }

            companion object {
                val DEFAULT: Config = ImmutableSpatialRules.Config.of()
                    .withOperandSupplier { b ->
                        b.operand(Filter::class.java)
                            .predicate { f ->
                                (DWITHIN_FINDER.inFilter(f)
                                        && !HILBERT_FINDER.inFilter(f))
                            }
                            .anyInputs()
                    }
                    .`as`(Config::class.java)
            }
        }

        companion object {
            /** Rewrites a spatial predicate to a predicate on a Hilbert curve.
             *
             *
             * Returns null if the predicate cannot be rewritten;
             * a 1-element list (new) if the predicate can be fully rewritten;
             * returns a 2-element list (new, original) if the new predicate allows
             * some false positives.
             *
             * @param conjunction Original predicate
             * @param builder Builder
             * @param ref Reference to Hilbert column
             * @param hilbert Function call that populates Hilbert column
             *
             * @return List containing rewritten predicate and original, or null
             */
            @Nullable
            fun replaceSpatial(
                conjunction: RexNode, builder: RelBuilder,
                ref: RexInputRef?, hilbert: RexCall
            ): List<RexNode>? {
                var conjunction: RexNode = conjunction
                val op0: RexNode
                val op1: RexNode
                val g0: Geometries.Geom?
                return when (conjunction.getKind()) {
                    ST_DWITHIN -> {
                        val within: RexCall = conjunction as RexCall
                        op0 = within.operands.get(0)
                        g0 = constantGeom(op0)
                        op1 = within.operands.get(1)
                        val g1: Geometries.Geom? = constantGeom(op1)
                        if (RexUtil.isLiteral(within.operands.get(2), true)) {
                            val distance: Number = requireNonNull(
                                value(within.operands.get(2)) as Number?
                            ) { "distance for $within" }
                            return when (Double.compare(distance.doubleValue(), 0.0)) {
                                -1 -> ImmutableList.of(builder.getRexBuilder().makeLiteral(false))
                                0 -> {
                                    // Change "ST_DWithin(g, p, 0)" to "g = p"
                                    conjunction = builder.equals(op0, op1)
                                    if (g0 != null && op1.getKind() === SqlKind.ST_POINT && (op1 as RexCall).operands.equals(
                                            hilbert.operands
                                        )
                                    ) {
                                        // Add the new predicate before the existing predicate
                                        // because it is cheaper to execute (albeit less selective).
                                        return ImmutableList.of(
                                            hilbertPredicate(
                                                builder.getRexBuilder(),
                                                ref,
                                                g0,
                                                distance
                                            ),
                                            conjunction
                                        )
                                    } else if (g1 != null && op0.getKind() === SqlKind.ST_POINT && (op0 as RexCall).operands.equals(
                                            hilbert.operands
                                        )
                                    ) {
                                        // Add the new predicate before the existing predicate
                                        // because it is cheaper to execute (albeit less selective).
                                        return ImmutableList.of(
                                            hilbertPredicate(
                                                builder.getRexBuilder(),
                                                ref,
                                                g1,
                                                distance
                                            ),
                                            conjunction
                                        )
                                    }
                                    null // cannot rewrite
                                }
                                1 -> {
                                    if (g0 != null && op1.getKind() === SqlKind.ST_POINT && (op1 as RexCall).operands.equals(
                                            hilbert.operands
                                        )
                                    ) {
                                        return ImmutableList.of(
                                            hilbertPredicate(
                                                builder.getRexBuilder(),
                                                ref,
                                                g0,
                                                distance
                                            ),
                                            conjunction
                                        )
                                    } else if (g1 != null && op0.getKind() === SqlKind.ST_POINT && (op0 as RexCall).operands.equals(
                                            hilbert.operands
                                        )
                                    ) {
                                        return ImmutableList.of(
                                            hilbertPredicate(
                                                builder.getRexBuilder(),
                                                ref,
                                                g1,
                                                distance
                                            ),
                                            conjunction
                                        )
                                    }
                                    null
                                }
                                else -> throw AssertionError("invalid sign: $distance")
                            }
                        }
                        null // cannot rewrite
                    }
                    ST_CONTAINS -> {
                        val contains: RexCall = conjunction as RexCall
                        op0 = contains.operands.get(0)
                        g0 = constantGeom(op0)
                        op1 = contains.operands.get(1)
                        if (g0 != null && op1.getKind() === SqlKind.ST_POINT && (op1 as RexCall).operands.equals(hilbert.operands)
                        ) {
                            // Add the new predicate before the existing predicate
                            // because it is cheaper to execute (albeit less selective).
                            ImmutableList.of(
                                hilbertPredicate(
                                    builder.getRexBuilder(),
                                    ref,
                                    g0
                                ),
                                conjunction
                            )
                        } else null
                        // cannot rewrite
                    }
                    else -> null // cannot rewrite
                }
            }

            /** Creates a predicate on the column that contains the index on the Hilbert
             * curve.
             *
             *
             * The predicate is a safe approximation. That is, it may allow some
             * points that are not within the distance, but will never disallow a point
             * that is within the distance.
             *
             *
             * Returns FALSE if the distance is negative (the ST_DWithin function
             * would always return FALSE) and returns an `=` predicate if distance
             * is 0. But usually returns a list of ranges,
             * `ref BETWEEN c1 AND c2 OR ref BETWEEN c3 AND c4`.  */
            private fun hilbertPredicate(
                rexBuilder: RexBuilder,
                ref: RexInputRef, g: Geometries.Geom, distance: Number
            ): RexNode {
                if (distance.doubleValue() === 0.0
                    && Geometries.type(g.g()) === Geometries.Type.POINT
                ) {
                    val p: Point = g.g() as Point
                    val hilbert = HilbertCurve2D(8)
                    val index: Long = hilbert.toIndex(p.getX(), p.getY())
                    return rexBuilder.makeCall(
                        SqlStdOperatorTable.EQUALS, ref,
                        rexBuilder.makeExactLiteral(BigDecimal.valueOf(index))
                    )
                }
                val g2: Geometries.Geom = GeoFunctions.ST_Buffer(g, distance.doubleValue())
                return hilbertPredicate(rexBuilder, ref, g2)
            }

            private fun hilbertPredicate(
                rexBuilder: RexBuilder,
                ref: RexInputRef, g2: Geometries.Geom
            ): RexNode {
                val g3: Geometries.Geom = GeoFunctions.ST_Envelope(g2)
                val env: Envelope = g3.g() as Envelope
                val hilbert = HilbertCurve2D(8)
                val ranges: List<SpaceFillingCurve2D.IndexRange> = hilbert.toRanges(
                    env.getXMin(), env.getYMin(), env.getXMax(),
                    env.getYMax(), RangeComputeHints()
                )
                val nodes: List<RexNode> = ArrayList()
                for (range in ranges) {
                    val lowerBd: BigDecimal = BigDecimal.valueOf(range.lower())
                    val upperBd: BigDecimal = BigDecimal.valueOf(range.upper())
                    nodes.add(
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.AND,
                            rexBuilder.makeCall(
                                SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                                ref,
                                rexBuilder.makeExactLiteral(lowerBd)
                            ),
                            rexBuilder.makeCall(
                                SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                                ref,
                                rexBuilder.makeExactLiteral(upperBd)
                            )
                        )
                    )
                }
                return rexBuilder.makeCall(SqlStdOperatorTable.OR, nodes)
            }
        }
    }
}
