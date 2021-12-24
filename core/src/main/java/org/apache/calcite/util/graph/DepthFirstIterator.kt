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

import java.util.ArrayList
import java.util.Collection
import java.util.HashSet
import java.util.Iterator
import java.util.List
import java.util.Set

/**
 * Iterates over the vertices in a directed graph in depth-first order.
 *
 * @param <V> Vertex type
 * @param <E> Edge type
</E></V> */
class DepthFirstIterator<V, E : DefaultEdge?>(graph: DirectedGraph<V, E>, start: V) : Iterator<V> {
    private val iterator: Iterator<V>

    init {
        // Dumb implementation that builds the list first.
        iterator = buildList<V, DefaultEdge>(graph, start).iterator()
    }

    @Override
    override fun hasNext(): Boolean {
        return iterator.hasNext()
    }

    @Override
    override fun next(): V {
        return iterator.next()
    }

    @Override
    fun remove() {
        throw UnsupportedOperationException()
    }

    companion object {
        private fun <V, E : DefaultEdge?> buildList(
            graph: DirectedGraph<V, E>, start: V
        ): List<V> {
            val list: List<V> = ArrayList()
            buildListRecurse(list, HashSet(), graph, start)
            return list
        }

        /** Creates an iterable over the vertices in the given graph in a depth-first
         * iteration order.  */
        fun <V, E : DefaultEdge?> of(
            graph: DirectedGraph<V, E>, start: V
        ): Iterable<V> {
            // Doesn't actually return a DepthFirstIterator, but a list with the same
            // contents, which is more efficient.
            return buildList<V, DefaultEdge>(graph, start)
        }

        /** Populates a collection with the nodes reachable from a given node.  */
        fun <V, E : DefaultEdge?> reachable(
            list: Collection<V>,
            graph: DirectedGraph<V, E>, start: V
        ) {
            buildListRecurse(list, HashSet(), graph, start)
        }

        private fun <V, E : DefaultEdge?> buildListRecurse(
            list: Collection<V>, activeVertices: Set<V>, graph: DirectedGraph<V, E>,
            start: V
        ) {
            if (!activeVertices.add(start)) {
                return
            }
            list.add(start)
            val edges: List<E> = graph.getOutwardEdges(start)
            for (edge in edges) {
                buildListRecurse<V, DefaultEdge>(list, activeVertices, graph, edge!!.target as V)
            }
            activeVertices.remove(start)
        }
    }
}
