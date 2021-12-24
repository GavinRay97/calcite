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

import com.google.common.collect.Ordering
import org.apiguardian.api.API
import org.checkerframework.checker.initialization.qual.NotOnlyInitialized
import org.checkerframework.checker.initialization.qual.UnknownInitialization
import java.util.ArrayList
import java.util.Collection
import java.util.Collections
import java.util.HashSet
import java.util.LinkedHashMap
import java.util.LinkedHashSet
import java.util.List
import java.util.Map
import java.util.Set
import org.apache.calcite.linq4j.Nullness.castNonNull

/**
 * Default implementation of [DirectedGraph].
 *
 * @param <V> Vertex type
 * @param <E> Edge type
</E></V> */
class DefaultDirectedGraph<V, E : DefaultEdge?>(@UnknownInitialization edgeFactory: EdgeFactory<V, E>) :
    DirectedGraph<V, E> {
    val edges: Set<E> = LinkedHashSet()
    val vertexMap: Map<V, VertexInfo<V, E>> = LinkedHashMap()

    @NotOnlyInitialized
    val edgeFactory: EdgeFactory<V, E>

    /** Creates a graph.  */
    init {
        this.edgeFactory = edgeFactory
    }

    fun toStringUnordered(): String {
        return ("graph("
                + "vertices: " + vertexMap.keySet()
                + ", edges: " + edges + ")")
    }

    @Override
    override fun toString(): String {
        @SuppressWarnings("unchecked") val vertexOrdering: Ordering<V> = Ordering.usingToString() as Ordering
        @SuppressWarnings("unchecked") val edgeOrdering: Ordering<E> = Ordering.usingToString() as Ordering
        return toString(vertexOrdering, edgeOrdering)
    }

    /** Returns the string representation of this graph, using the given
     * orderings to ensure that the output order of vertices and edges is
     * deterministic.  */
    private fun toString(
        vertexOrdering: Ordering<V>,
        edgeOrdering: Ordering<E>
    ): String {
        return ("graph("
                + "vertices: " + vertexOrdering.sortedCopy(vertexMap.keySet() as Set<V>?)
                + ", edges: " + edgeOrdering.sortedCopy(edges) + ")")
    }

    @Override
    override fun addVertex(vertex: V): Boolean {
        return if (vertexMap.containsKey(vertex)) {
            false
        } else {
            vertexMap.put(vertex, VertexInfo<V, E>())
            true
        }
    }

    @API(since = "1.26", status = API.Status.EXPERIMENTAL)
    protected fun getVertex(vertex: V): VertexInfo<V, E> {
        return vertexMap[vertex]
            ?: throw IllegalArgumentException("no vertex $vertex")
    }

    @Override
    override fun edgeSet(): Set<E> {
        return Collections.unmodifiableSet(edges)
    }

    @Override
    @Nullable
    override fun addEdge(vertex: V, targetVertex: V): E? {
        val info = getVertex(vertex)
        val targetInfo = getVertex(targetVertex)
        val edge: E = edgeFactory.createEdge(vertex, targetVertex)
        return if (edges.add(edge)) {
            info.outEdges.add(edge)
            targetInfo.inEdges.add(edge)
            edge
        } else {
            null
        }
    }

    @Override
    @Nullable
    override fun getEdge(source: V, target: V): E? {
        // REVIEW: could instead use edges.get(new DefaultEdge(source, target))
        val info = getVertex(source)
        for (outEdge in info.outEdges) {
            if (outEdge!!.target.equals(target)) {
                return outEdge
            }
        }
        return null
    }

    @Override
    override fun removeEdge(source: V, target: V): Boolean {
        // remove out edges
        val outEdges = getVertex(source).outEdges
        var outRemoved = false
        run {
            var i = 0
            val size: Int = outEdges.size()
            while (i < size) {
                val edge = outEdges[i]
                if (edge!!.target.equals(target)) {
                    outEdges.remove(i)
                    edges.remove(edge)
                    outRemoved = true
                    break
                }
                i++
            }
        }

        // remove in edges
        val inEdges = getVertex(target).inEdges
        var inRemoved = false
        var i = 0
        val size: Int = inEdges.size()
        while (i < size) {
            val edge = inEdges[i]
            if (edge!!.source.equals(source)) {
                inEdges.remove(i)
                inRemoved = true
                break
            }
            i++
        }
        assert(outRemoved == inRemoved)
        return outRemoved
    }

    @SuppressWarnings("return.type.incompatible")
    @Override
    override fun vertexSet(): Set<V> {
        // Set<V extends @KeyFor("this.vertexMap") Object> -> Set<V>
        return vertexMap.keySet()
    }

    @Override
    fun removeAllVertices(collection: Collection<V>) {
        // The point at which collection is large enough to make the 'majority'
        // algorithm more efficient.
        var collection = collection
        val threshold = 0.35f
        val thresholdSize = (vertexMap.size() * threshold) as Int
        if (collection.size() > thresholdSize && collection !is Set) {
            // Convert collection to a set, so that collection.contains() is
            // faster. If there are duplicates, collection.size() will get smaller.
            collection = HashSet(collection)
        }
        if (collection.size() > thresholdSize) {
            removeMajorityVertices(collection as Set<V>)
        } else {
            removeMinorityVertices(collection)
        }

        // remove all edges ref from this.edges
        for (v in collection) {
            edges.removeIf { e -> e.source.equals(v) || e.target.equals(v) }
        }
    }

    /** Implementation of [.removeAllVertices] that is efficient
     * if `collection` is a small fraction of the set of vertices.  */
    private fun removeMinorityVertices(collection: Collection<V>) {
        for (v in collection) {
            @SuppressWarnings("argument.type.incompatible") // nullable keys are supported by .get
            val info = vertexMap[v] ?: continue

            // remove all edges pointing to v
            for (edge in info.inEdges) {
                @SuppressWarnings("unchecked") val source = edge!!.source as V
                val sourceInfo = getVertex(source)
                sourceInfo.outEdges.removeIf { e -> e.target.equals(v) }
            }

            // remove all edges starting from v
            for (edge in info.outEdges) {
                @SuppressWarnings("unchecked") val target = edge!!.target as V
                val targetInfo = getVertex(target)
                targetInfo.inEdges.removeIf { e -> e.source.equals(v) }
            }
        }
        vertexMap.keySet().removeAll(collection)
    }

    /** Implementation of [.removeAllVertices] that is efficient
     * if `vertexSet` is a large fraction of the set of vertices in the
     * graph.  */
    private fun removeMajorityVertices(vertexSet: Set<V>) {
        vertexMap.keySet().removeAll(vertexSet)
        for (info in vertexMap.values()) {
            info.outEdges.removeIf { e -> vertexSet.contains(castNonNull(e.target as V)) }
            info.inEdges.removeIf { e -> vertexSet.contains(castNonNull(e.source as V)) }
        }
    }

    @Override
    override fun getOutwardEdges(source: V): List<E> {
        return getVertex(source).outEdges
    }

    @Override
    override fun getInwardEdges(target: V): List<E> {
        return getVertex(target).inEdges
    }

    fun source(edge: E): V {
        return edge!!.source
    }

    fun target(edge: E): V {
        return edge!!.target
    }

    /**
     * Information about a vertex.
     *
     * @param <V> Vertex type
     * @param <E> Edge type
    </E></V> */
    class VertexInfo<V, E> {
        val outEdges: List<E> = ArrayList()
        val inEdges: List<E> = ArrayList()
    }

    companion object {
        fun <V> create(): DefaultDirectedGraph<V, DefaultEdge> {
            return create(DefaultEdge.factory())
        }

        fun <V, E : DefaultEdge?> create(
            edgeFactory: EdgeFactory<V, E>
        ): DefaultDirectedGraph<V, E> {
            return DefaultDirectedGraph<Any, DefaultEdge>(edgeFactory)
        }
    }
}
