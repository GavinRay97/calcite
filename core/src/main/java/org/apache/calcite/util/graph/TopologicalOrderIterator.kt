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

import org.checkerframework.checker.initialization.qual.UnderInitialization
import org.checkerframework.checker.nullness.qual.RequiresNonNull
import java.util.ArrayList
import java.util.HashMap
import java.util.Iterator
import java.util.List
import java.util.Map
import java.util.Set
import java.util.Objects.requireNonNull

/**
 * Iterates over the edges of a graph in topological order.
 *
 * @param <V> Vertex type
 * @param <E> Edge type
</E></V> */
class TopologicalOrderIterator<V, E : DefaultEdge?>(graph: DirectedGraph<V, E>) : Iterator<V> {
    val countMap: Map<V, IntArray> = HashMap()
    val empties: List<V> = ArrayList()
    private val graph: DefaultDirectedGraph<V, E>

    init {
        this.graph = graph as DefaultDirectedGraph<V, E>
        populate(countMap, empties)
    }

    @RequiresNonNull("graph")
    private fun populate(
        countMap: Map<V, IntArray>, empties: List<V>
    ) {
        for (v in graph.vertexMap.keySet()) {
            countMap.put(v, intArrayOf(0))
        }
        for (info in graph.vertexMap.values()) {
            for (edge in info.outEdges) {
                val ints: IntArray = requireNonNull(
                    countMap[edge!!.target]
                ) { "no value for " + edge!!.target }
                ++ints[0]
            }
        }
        for (entry in countMap.entrySet()) {
            if (entry.getValue().get(0) === 0) {
                empties.add(entry.getKey())
            }
        }
        countMap.keySet().removeAll(empties)
    }

    @Override
    override fun hasNext(): Boolean {
        return !empties.isEmpty()
    }

    @Override
    override fun next(): V {
        val v: V = empties.remove(0)
        val vertexInfo: DefaultDirectedGraph.VertexInfo<V, E> = requireNonNull(
            graph.vertexMap.get(v)
        ) { "no vertex $v" }
        for (o in vertexInfo.outEdges) {
            val target = o!!.target as V
            val ints: IntArray = requireNonNull(
                countMap[target]
            ) { "no counts found for target $target" }
            if (--ints[0] == 0) {
                countMap.remove(target)
                empties.add(target)
            }
        }
        return v
    }

    @Override
    fun remove() {
        throw UnsupportedOperationException()
    }

    fun findCycles(): Set<V> {
        while (hasNext()) {
            next()
        }
        return countMap.keySet() as Set<V>
    }

    companion object {
        fun <V, E : DefaultEdge?> of(
            graph: DirectedGraph<V, E>
        ): Iterable<V> {
            return Iterable<V> { TopologicalOrderIterator<Any, DefaultEdge>(graph) }
        }
    }
}
