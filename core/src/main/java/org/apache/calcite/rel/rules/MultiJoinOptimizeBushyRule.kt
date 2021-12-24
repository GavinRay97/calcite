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

import org.apache.calcite.config.CalciteSystemProperty

/**
 * Planner rule that finds an approximately optimal ordering for join operators
 * using a heuristic algorithm.
 *
 *
 * It is triggered by the pattern
 * [org.apache.calcite.rel.logical.LogicalProject] ([MultiJoin]).
 *
 *
 * It is similar to
 * [org.apache.calcite.rel.rules.LoptOptimizeJoinRule]
 * ([CoreRules.MULTI_JOIN_OPTIMIZE]).
 * `LoptOptimizeJoinRule` is only capable of producing left-deep joins;
 * this rule is capable of producing bushy joins.
 *
 *
 * TODO:
 *
 *  1. Join conditions that touch 1 factor.
 *  1. Join conditions that touch 3 factors.
 *  1. More than 1 join conditions that touch the same pair of factors,
 * e.g. `t0.c1 = t1.c1 and t1.c2 = t0.c3`
 *
 *
 * @see CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY
 */
@Value.Enclosing
class MultiJoinOptimizeBushyRule
/** Creates a MultiJoinOptimizeBushyRule.  */
protected constructor(config: Config?) : RelRule<MultiJoinOptimizeBushyRule.Config?>(config), TransformationRule {
    @Nullable
    private val pw: PrintWriter? = if (CalciteSystemProperty.DEBUG.value()) Util.printWriter(System.out) else null

    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        joinFactory: RelFactories.JoinFactory?,
        projectFactory: RelFactories.ProjectFactory?
    ) : this(RelBuilder.proto(joinFactory, projectFactory)) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val multiJoinRel: MultiJoin = call.rel(0)
        val rexBuilder: RexBuilder = multiJoinRel.getCluster().getRexBuilder()
        val relBuilder: RelBuilder = call.builder()
        val mq: RelMetadataQuery = call.getMetadataQuery()
        val multiJoin = LoptMultiJoin(multiJoinRel)
        val vertexes: List<Vertex> = ArrayList()
        var x = 0
        for (i in 0 until multiJoin.getNumJoinFactors()) {
            val rel: RelNode = multiJoin.getJoinFactor(i)
            val cost: Double = mq.getRowCount(rel)
            vertexes.add(LeafVertex(i, rel, cost, x))
            x += rel.getRowType().getFieldCount()
        }
        assert(x == multiJoin.getNumTotalFields())
        val unusedEdges: List<Edge> = ArrayList()
        for (node in multiJoin.getJoinFilters()) {
            unusedEdges.add(multiJoin.createEdge(node))
        }

        // Comparator that chooses the best edge. A "good edge" is one that has
        // a large difference in the number of rows on LHS and RHS.
        val edgeComparator: Comparator<LoptMultiJoin.Edge?> = object : Comparator<LoptMultiJoin.Edge?>() {
            @Override
            fun compare(e0: LoptMultiJoin.Edge, e1: LoptMultiJoin.Edge): Int {
                return Double.compare(rowCountDiff(e0), rowCountDiff(e1))
            }

            private fun rowCountDiff(edge: LoptMultiJoin.Edge): Double {
                assert(edge.factors.cardinality() === 2) { edge.factors }
                val factor0: Int = edge.factors.nextSetBit(0)
                val factor1: Int = edge.factors.nextSetBit(factor0 + 1)
                return Math.abs(
                    vertexes[factor0].cost
                            - vertexes[factor1].cost
                )
            }
        }
        val usedEdges: List<Edge> = ArrayList()
        while (true) {
            val edgeOrdinal = chooseBestEdge(unusedEdges, edgeComparator)
            if (pw != null) {
                trace(vertexes, unusedEdges, usedEdges, edgeOrdinal, pw)
            }
            val factors: IntArray
            factors = if (edgeOrdinal == -1) {
                // No more edges. Are there any un-joined vertexes?
                val lastVertex: Vertex = Util.last(vertexes)
                val z: Int = lastVertex.factors.previousClearBit(lastVertex.id - 1)
                if (z < 0) {
                    break
                }
                intArrayOf(z, lastVertex.id)
            } else {
                val bestEdge: LoptMultiJoin.Edge = unusedEdges[edgeOrdinal]
                assert(bestEdge.factors.cardinality() === 2)
                bestEdge.factors.toArray()
            }

            // Determine which factor is to be on the LHS of the join.
            val majorFactor: Int
            val minorFactor: Int
            if (vertexes[factors[0]].cost <= vertexes[factors[1]].cost) {
                majorFactor = factors[0]
                minorFactor = factors[1]
            } else {
                majorFactor = factors[1]
                minorFactor = factors[0]
            }
            val majorVertex = vertexes[majorFactor]
            val minorVertex = vertexes[minorFactor]

            // Find the join conditions. All conditions whose factors are now all in
            // the join can now be used.
            val v: Int = vertexes.size()
            val newFactors: ImmutableBitSet = majorVertex.factors
                .rebuild()
                .addAll(minorVertex.factors)
                .set(v)
                .build()
            val conditions: List<RexNode> = ArrayList()
            val edgeIterator: Iterator<LoptMultiJoin.Edge> = unusedEdges.iterator()
            while (edgeIterator.hasNext()) {
                val edge: LoptMultiJoin.Edge = edgeIterator.next()
                if (newFactors.contains(edge.factors)) {
                    conditions.add(edge.condition)
                    edgeIterator.remove()
                    usedEdges.add(edge)
                }
            }
            val cost: Double = (majorVertex.cost
                    * minorVertex.cost
                    * RelMdUtil.guessSelectivity(
                RexUtil.composeConjunction(rexBuilder, conditions)
            ))
            val newVertex: Vertex = JoinVertex(
                v, majorFactor, minorFactor, newFactors,
                cost, ImmutableList.copyOf(conditions)
            )
            vertexes.add(newVertex)

            // Re-compute selectivity of edges above the one just chosen.
            // Suppose that we just chose the edge between "product" (10k rows) and
            // "product_class" (10 rows).
            // Both of those vertices are now replaced by a new vertex "P-PC".
            // This vertex has fewer rows (1k rows) -- a fact that is critical to
            // decisions made later. (Hence "greedy" algorithm not "simple".)
            // The adjacent edges are modified.
            val merged: ImmutableBitSet = ImmutableBitSet.of(minorFactor, majorFactor)
            for (i in 0 until unusedEdges.size()) {
                val edge: LoptMultiJoin.Edge = unusedEdges[i]
                if (edge.factors.intersects(merged)) {
                    val newEdgeFactors: ImmutableBitSet = edge.factors
                        .rebuild()
                        .removeAll(newFactors)
                        .set(v)
                        .build()
                    assert(newEdgeFactors.cardinality() === 2)
                    val newEdge: LoptMultiJoin.Edge = Edge(
                        edge.condition, newEdgeFactors,
                        edge.columns
                    )
                    unusedEdges.set(i, newEdge)
                }
            }
        }

        // We have a winner!
        val relNodes: List<Pair<RelNode, TargetMapping>> = ArrayList()
        for (vertex in vertexes) {
            if (vertex is LeafVertex) {
                val leafVertex = vertex
                val mapping: Mappings.TargetMapping = Mappings.offsetSource(
                    Mappings.createIdentity(
                        leafVertex.rel.getRowType().getFieldCount()
                    ),
                    leafVertex.fieldOffset,
                    multiJoin.getNumTotalFields()
                )
                relNodes.add(Pair.of(leafVertex.rel, mapping))
            } else {
                val joinVertex = vertex as JoinVertex
                val leftPair: Pair<RelNode, Mappings.TargetMapping> = relNodes[joinVertex.leftFactor]
                val left: RelNode = leftPair.left
                val leftMapping: Mappings.TargetMapping = leftPair.right
                val rightPair: Pair<RelNode, Mappings.TargetMapping> = relNodes[joinVertex.rightFactor]
                val right: RelNode = rightPair.left
                val rightMapping: Mappings.TargetMapping = rightPair.right
                val mapping: Mappings.TargetMapping = Mappings.merge(
                    leftMapping,
                    Mappings.offsetTarget(
                        rightMapping,
                        left.getRowType().getFieldCount()
                    )
                )
                if (pw != null) {
                    pw.println("left: $leftMapping")
                    pw.println("right: $rightMapping")
                    pw.println("combined: $mapping")
                    pw.println()
                }
                val shuttle: RexVisitor<RexNode> = RexPermuteInputsShuttle(mapping, left, right)
                val condition: RexNode = RexUtil.composeConjunction(rexBuilder, joinVertex.conditions)
                val join: RelNode = relBuilder.push(left)
                    .push(right)
                    .join(JoinRelType.INNER, condition.accept(shuttle))
                    .build()
                relNodes.add(Pair.of(join, mapping))
            }
            if (pw != null) {
                pw.println(Util.last(relNodes))
            }
        }
        val top: Pair<RelNode, Mappings.TargetMapping> = Util.last(relNodes)
        relBuilder.push(top.left)
            .project(relBuilder.fields(top.right))
        call.transformTo(relBuilder.build())
    }

    fun chooseBestEdge(
        edges: List<LoptMultiJoin.Edge?>,
        comparator: Comparator<LoptMultiJoin.Edge?>
    ): Int {
        return minPos<Any?>(edges, comparator)
    }

    /** Participant in a join (relation or join).  */
    internal abstract class Vertex(val id: Int, factors: ImmutableBitSet, cost: Double) {
        val factors: ImmutableBitSet
        val cost: Double

        init {
            this.factors = factors
            this.cost = cost
        }
    }

    /** Relation participating in a join.  */
    internal class LeafVertex(id: Int, rel: RelNode, cost: Double, fieldOffset: Int) :
        Vertex(id, ImmutableBitSet.of(id), cost) {
        val rel: RelNode
        val fieldOffset: Int

        init {
            this.rel = rel
            this.fieldOffset = fieldOffset
        }

        @Override
        override fun toString(): String {
            return ("LeafVertex(id: " + id
                    + ", cost: " + Util.human(cost)
                    + ", factors: " + factors
                    + ", fieldOffset: " + fieldOffset
                    + ")")
        }
    }

    /** Participant in a join which is itself a join.  */
    internal class JoinVertex(
        id: Int, val leftFactor: Int, val rightFactor: Int, factors: ImmutableBitSet,
        cost: Double, conditions: ImmutableList<RexNode?>?
    ) : Vertex(id, factors, cost) {
        /** Zero or more join conditions. All are in terms of the original input
         * columns (not in terms of the outputs of left and right input factors).  */
        val conditions: ImmutableList<RexNode>

        init {
            this.conditions = Objects.requireNonNull(conditions, "conditions")
        }

        @Override
        override fun toString(): String {
            return ("JoinVertex(id: " + id
                    + ", cost: " + Util.human(cost)
                    + ", factors: " + factors
                    + ", leftFactor: " + leftFactor
                    + ", rightFactor: " + rightFactor
                    + ")")
        }
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): MultiJoinOptimizeBushyRule? {
            return MultiJoinOptimizeBushyRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableMultiJoinOptimizeBushyRule.Config.of()
                .withOperandSupplier { b -> b.operand(MultiJoin::class.java).anyInputs() }
        }
    }

    companion object {
        private fun trace(
            vertexes: List<Vertex>,
            unusedEdges: List<LoptMultiJoin.Edge>, usedEdges: List<LoptMultiJoin.Edge>,
            edgeOrdinal: Int, pw: PrintWriter
        ) {
            pw.println("bestEdge: $edgeOrdinal")
            pw.println("vertexes:")
            for (vertex in vertexes) {
                pw.println(vertex)
            }
            pw.println("unused edges:")
            for (edge in unusedEdges) {
                pw.println(edge)
            }
            pw.println("edges:")
            for (edge in usedEdges) {
                pw.println(edge)
            }
            pw.println()
            pw.flush()
        }

        /** Returns the index within a list at which compares least according to a
         * comparator.
         *
         *
         * In the case of a tie, returns the earliest such element.
         *
         *
         * If the list is empty, returns -1.
         */
        fun <E> minPos(list: List<E>, fn: Comparator<E>): Int {
            if (list.isEmpty()) {
                return -1
            }
            var eBest = list[0]
            var iBest = 0
            for (i in 1 until list.size()) {
                val e = list[i]
                if (fn.compare(e, eBest) < 0) {
                    eBest = e
                    iBest = i
                }
            }
            return iBest
        }
    }
}
