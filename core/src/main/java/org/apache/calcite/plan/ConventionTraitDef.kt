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
package org.apache.calcite.plan

import org.apache.calcite.rel.RelNode

/**
 * Definition of the convention trait.
 * A new set of conversion information is created for
 * each planner that registers at least one [ConverterRule] instance.
 *
 *
 * Conversion data is held in a [LoadingCache]
 * with weak keys so that the JVM's garbage
 * collector may reclaim the conversion data after the planner itself has been
 * garbage collected. The conversion information consists of a graph of
 * conversions (from one calling convention to another) and a map of graph arcs
 * to [ConverterRule]s.
 */
class ConventionTraitDef  //~ Constructors -----------------------------------------------------------
private constructor() : RelTraitDef<Convention?>() {
    //~ Instance fields --------------------------------------------------------
    /**
     * Weak-key cache of RelOptPlanner to ConversionData. The idea is that when
     * the planner goes away, so does the cache entry.
     */
    private val conversionCache: LoadingCache<RelOptPlanner, ConversionData> = CacheBuilder.newBuilder().weakKeys()
        .build(CacheLoader.from { ConversionData() })

    //~ Methods ----------------------------------------------------------------
    // implement RelTraitDef
    @get:Override
    val traitClass: Class<Convention>
        get() = Convention::class.java

    @get:Override
    val simpleName: String
        get() = "convention"

    @get:Override
    val default: org.apache.calcite.plan.Convention
        get() = Convention.NONE

    @Override
    fun registerConverterRule(
        planner: RelOptPlanner,
        converterRule: ConverterRule
    ) {
        if (converterRule.isGuaranteed()) {
            val conversionData = getConversionData(planner)
            val inConvention: Convention = converterRule.getInTrait()
            val outConvention: Convention = converterRule.getOutTrait()
            conversionData.conversionGraph.addVertex(inConvention)
            conversionData.conversionGraph.addVertex(outConvention)
            conversionData.conversionGraph.addEdge(inConvention, outConvention)
            conversionData.mapArcToConverterRule.put(
                Pair.of(inConvention, outConvention), converterRule
            )
        }
    }

    @Override
    fun deregisterConverterRule(
        planner: RelOptPlanner,
        converterRule: ConverterRule
    ) {
        if (converterRule.isGuaranteed()) {
            val conversionData = getConversionData(planner)
            val inConvention: Convention = converterRule.getInTrait()
            val outConvention: Convention = converterRule.getOutTrait()
            val removed: Boolean = conversionData.conversionGraph.removeEdge(
                inConvention, outConvention
            )
            assert(removed)
            conversionData.mapArcToConverterRule.remove(
                Pair.of(inConvention, outConvention), converterRule
            )
        }
    }

    // implement RelTraitDef
    @Override
    @Nullable
    fun convert(
        planner: RelOptPlanner,
        rel: RelNode,
        toConvention: Convention,
        allowInfiniteCostConverters: Boolean
    ): RelNode? {
        val mq: RelMetadataQuery = rel.getCluster().getMetadataQuery()
        val conversionData = getConversionData(planner)
        val fromConvention: Convention = requireNonNull(
            rel.getConvention()
        ) { "convention is null for rel $rel" }
        val conversionPaths: List<List<Convention>> = conversionData.getPaths(fromConvention, toConvention)
        loop@ for (conversionPath in conversionPaths) {
            assert(conversionPath[0] === fromConvention)
            assert(
                conversionPath[conversionPath.size() - 1]
                        === toConvention
            )
            var converted: RelNode? = rel
            var previous: Convention? = null
            for (arc in conversionPath) {
                val cost: RelOptCost = planner.getCost(converted, mq)
                if ((cost == null || cost.isInfinite())
                    && !allowInfiniteCostConverters
                ) {
                    continue@loop
                }
                if (previous != null) {
                    converted = changeConvention(
                        converted, previous, arc,
                        conversionData.mapArcToConverterRule
                    )
                    if (converted == null) {
                        throw AssertionError(
                            "Converter from " + previous + " to " + arc
                                    + " guaranteed that it could convert any relexp"
                        )
                    }
                }
                previous = arc
            }
            return converted
        }
        return null
    }

    @Override
    fun canConvert(
        planner: RelOptPlanner,
        fromConvention: Convention,
        toConvention: Convention?
    ): Boolean {
        val conversionData = getConversionData(planner)
        return (fromConvention.canConvertConvention(toConvention)
                || conversionData.getShortestDistance(fromConvention, toConvention) != -1)
    }

    private fun getConversionData(planner: RelOptPlanner): ConversionData {
        return conversionCache.getUnchecked(planner)
    }
    //~ Inner Classes ----------------------------------------------------------
    /** Workspace for converting from one convention to another.  */
    private class ConversionData {
        val conversionGraph: DirectedGraph<Convention, DefaultEdge> = DefaultDirectedGraph.create()

        /**
         * For a given source/target convention, there may be several possible
         * conversion rules. Maps [DefaultEdge] to a
         * collection of [ConverterRule] objects.
         */
        val mapArcToConverterRule: Multimap<Pair<Convention, Convention>, ConverterRule> = HashMultimap.create()
        private var pathMap: @MonotonicNonNull Graphs.FrozenGraph<Convention, DefaultEdge>? = null
            private get() {
                if (field == null) {
                    field = Graphs.makeImmutable(conversionGraph)
                }
                return field
            }

        fun getPaths(
            fromConvention: Convention?,
            toConvention: Convention?
        ): List<List<Convention>> {
            return pathMap.getPaths(fromConvention, toConvention)
        }

        fun getShortestDistance(
            fromConvention: Convention?,
            toConvention: Convention?
        ): Int {
            return pathMap.getShortestDistance(fromConvention, toConvention)
        }
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        val INSTANCE = ConventionTraitDef()

        /**
         * Tries to convert a relational expression to the target convention of an
         * arc.
         */
        @Nullable
        private fun changeConvention(
            rel: RelNode?,
            source: Convention,
            target: Convention,
            mapArcToConverterRule: Multimap<Pair<Convention, Convention>, ConverterRule>
        ): RelNode? {
            assert(source === rel.getConvention())

            // Try to apply each converter rule for this arc's source/target calling
            // conventions.
            val key: Pair<Convention, Convention> = Pair.of(source, target)
            for (rule in mapArcToConverterRule.get(key)) {
                assert(rule.getInTrait() === source)
                assert(rule.getOutTrait() === target)
                val converted: RelNode = rule.convert(rel)
                if (converted != null) {
                    return converted
                }
            }
            return null
        }
    }
}
