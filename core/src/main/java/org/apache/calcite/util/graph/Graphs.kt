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
package org.apache.calcite.util.graph

import org.apache.calcite.util.Pair
import com.google.common.collect.ImmutableList
import java.util.AbstractList
import java.util.ArrayList
import java.util.Comparator
import java.util.HashMap
import java.util.HashSet
import java.util.List
import java.util.Map
import java.util.Set
import java.util.Objects.requireNonNull

/**
 * Miscellaneous graph utilities.
 */
object Graphs {
    fun <V, E : DefaultEdge?> predecessorListOf(
        graph: DirectedGraph<V, E>, vertex: V
    ): List<V> {
        val edges: List<E> = graph.getInwardEdges(vertex)
        return object : AbstractList<V>() {
            @Override
            operator fun get(index: Int): V {
                return edges[index]!!.source
            }

            @Override
            fun size(): Int {
                return edges.size()
            }
        }
    }

    /** Returns a map of the shortest paths between any pair of nodes.  */
    fun <V, E : DefaultEdge?> makeImmutable(
        graph: DirectedGraph<V, E>
    ): FrozenGraph<V, E> {
        val graph1: DefaultDirectedGraph<V, E> = graph as DefaultDirectedGraph<V, E>
        val shortestDistances: Map<Pair<V, V>, IntArray> = HashMap()
        for (arc in graph1.vertexMap.values()) {
            for (edge in arc.outEdges) {
                val source: V = graph1.source(edge)
                val target: V = graph1.target(edge)
                shortestDistances.put(Pair.of(source, target), intArrayOf(1))
            }
        }
        while (true) {
            // Take a copy of the map's keys to avoid
            // ConcurrentModificationExceptions.
            val previous: List<Pair<V, V>> = ImmutableList.copyOf(shortestDistances.keySet())
            var changed = false
            for (edge in graph.edgeSet()) {
                for (edge2 in previous) {
                    if (edge!!.target.equals(edge2.left)) {
                        val key: Pair<V, V> = Pair.of(graph1.source(edge), edge2.right)
                        val bestDistance = shortestDistances[key]
                        val arc2Distance: IntArray = requireNonNull(
                            shortestDistances[edge2]
                        ) { "shortestDistances.get(edge2) for $edge2" }
                        if (bestDistance == null
                            || bestDistance[0] > arc2Distance[0] + 1
                        ) {
                            shortestDistances.put(key, intArrayOf(arc2Distance[0] + 1))
                            changed = true
                        }
                    }
                }
            }
            if (!changed) {
                break
            }
        }
        return FrozenGraph(graph1, shortestDistances)
    }

    /**
     * Immutable grap.
     *
     * @param <V> Vertex type
     * @param <E> Edge type
    </E></V> */
    class FrozenGraph<V : Object?, E : DefaultEdge?> internal constructor(
        graph: DefaultDirectedGraph<V, E>,
        shortestDistances: Map<Pair<V, V>, IntArray?>
    ) {
        private val graph: DefaultDirectedGraph<V, E>
        private val shortestDistances: Map<Pair<V, V>, IntArray>

        /** Creates a frozen graph as a copy of another graph.  */
        init {
            this.graph = graph
            this.shortestDistances = shortestDistances
        }

        /**
         * Returns an iterator of all paths between two nodes,
         * in non-decreasing order of path lengths.
         *
         *
         * The current implementation is not optimal.
         */
        fun getPaths(from: V, to: V): List<List<V>> {
            val list: List<List<V>> = ArrayList()
            if (from!!.equals(to)) {
                list.add(ImmutableList.of(from))
            }
            findPaths(from, to, list)
            list.sort(Comparator.comparingInt(List::size))
            return list
        }

        /**
         * Returns the shortest distance between two points, -1, if there is no path.
         * @param from From
         * @param to To
         * @return The shortest distance, -1, if there is no path.
         */
        fun getShortestDistance(from: V, to: V): Int {
            if (from!!.equals(to)) {
                return 0
            }
            val distance = shortestDistances[Pair.of(from, to)]
            return distance?.get(0) ?: -1
        }

        private fun findPaths(from: V, to: V, list: List<List<V>>) {
            if (getShortestDistance(from, to) == -1) {
                return
            }
            //      final E edge = graph.getEdge(from, to);
//      if (edge != null) {
//        list.add(ImmutableList.of(from, to));
//      }
            val prefix: List<V> = ArrayList()
            prefix.add(from)
            findPathsExcluding(from, to, list, HashSet(), prefix)
        }

        /**
         * Finds all paths from "from" to "to" of length 2 or greater, such that the
         * intermediate nodes are not contained in "excludedNodes".
         */
        private fun findPathsExcluding(
            from: V, to: V, list: List<List<V>>,
            excludedNodes: Set<V>, prefix: List<V>
        ) {
            excludedNodes.add(from)
            for (edge in graph.edges) {
                if (edge!!.source.equals(from)) {
                    val target: V = graph.target(edge)
                    if (target!!.equals(to)) {
                        // We found a path.
                        prefix.add(target)
                        list.add(ImmutableList.copyOf(prefix))
                        prefix.remove(prefix.size() - 1)
                    } else if (excludedNodes.contains(target)) {
                        // ignore it
                    } else {
                        prefix.add(target)
                        findPathsExcluding(target, to, list, excludedNodes, prefix)
                        prefix.remove(prefix.size() - 1)
                    }
                }
            }
            excludedNodes.remove(from)
        }
    }
}
