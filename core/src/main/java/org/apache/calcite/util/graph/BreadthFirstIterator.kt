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

import java.util.ArrayDeque
import java.util.Deque
import java.util.HashSet
import java.util.Iterator
import java.util.Set

/**
 * Iterates over the vertices in a directed graph in breadth-first order.
 *
 * @param <V> Vertex type
 * @param <E> Edge type
</E></V> */
class BreadthFirstIterator<V, E : DefaultEdge?>(graph: DirectedGraph<V, E>, root: V) : Iterator<V> {
    private val graph: DirectedGraph<V, E>
    private val deque: Deque<V> = ArrayDeque()
    private val set: Set<V> = HashSet()

    init {
        this.graph = graph
        deque.add(root)
    }

    @Override
    override fun hasNext(): Boolean {
        return !deque.isEmpty()
    }

    @Override
    override fun next(): V {
        val v: V = deque.removeFirst()
        for (e in graph.getOutwardEdges(v)) {
            @SuppressWarnings("unchecked") val target = e!!.target as V
            if (set.add(target)) {
                deque.addLast(target)
            }
        }
        return v
    }

    @Override
    fun remove() {
        throw UnsupportedOperationException()
    }

    companion object {
        fun <V, E : DefaultEdge?> of(
            graph: DirectedGraph<V, E>, root: V
        ): Iterable<V> {
            return Iterable<V> { BreadthFirstIterator<V, DefaultEdge>(graph, root) }
        }

        /** Populates a set with the nodes reachable from a given node.  */
        fun <V, E : DefaultEdge?> reachable(
            set: Set<V>,
            graph: DirectedGraph<V, E>, root: V
        ) {
            val deque: Deque<V> = ArrayDeque()
            deque.add(root)
            set.add(root)
            while (!deque.isEmpty()) {
                val v: V = deque.removeFirst()
                for (e in graph.getOutwardEdges(v)) {
                    @SuppressWarnings("unchecked") val target = e!!.target as V
                    if (set.add(target)) {
                        deque.addLast(target)
                    }
                }
            }
        }
    }
}
