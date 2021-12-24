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

import org.apache.calcite.util.Util
import org.checkerframework.checker.initialization.qual.UnknownInitialization
import java.util.List

/**
 * Directed graph where edges have attributes and allows multiple edges between
 * any two vertices provided that their attributes are different.
 *
 * @param <V> Vertex type
 * @param <E> Edge type
</E></V> */
class AttributedDirectedGraph<V, E : DefaultEdge?>
/** Creates an attributed graph.  */
    (@UnknownInitialization edgeFactory: AttributedEdgeFactory<V, E>) : DefaultDirectedGraph<V, E>(edgeFactory) {
    /** Returns the first edge between one vertex to another.  */
    @Override
    @Nullable
    override fun getEdge(source: V, target: V): E? {
        val info: VertexInfo<V, E> = getVertex(source)
        for (outEdge in info.outEdges) {
            if (outEdge!!.target.equals(target)) {
                return outEdge
            }
        }
        return null
    }
    // CHECKSTYLE: IGNORE 1

    @Deprecated
    @Override
    @Nullable
    @Deprecated("Use {@link #addEdge(Object, Object, Object...)}. ")
    override fun addEdge(vertex: V, targetVertex: V): E {
        return super.addEdge(vertex, targetVertex)
    }

    @Nullable
    fun addEdge(vertex: V, targetVertex: V, vararg attributes: Object?): E? {
        val info: VertexInfo<V, E> = getVertex(vertex)
        val targetInfo: VertexInfo<V, E> = getVertex(targetVertex)
        @SuppressWarnings("unchecked") val f: AttributedEdgeFactory<V, E> =
            this.edgeFactory as AttributedEdgeFactory<*, *>
        val edge = f.createEdge(vertex, targetVertex, *attributes)
        return if (edges.add(edge)) {
            info.outEdges.add(edge)
            targetInfo.inEdges.add(edge)
            edge
        } else {
            null
        }
    }

    /** Returns all edges between one vertex to another.  */
    fun getEdges(source: V, target: V): Iterable<E> {
        val info: VertexInfo<V, E> = getVertex(source)
        return Util.filter(info.outEdges) { outEdge -> outEdge.target.equals(target) }
    }

    /** Removes all edges from a given vertex to another.
     * Returns whether any were removed.  */
    @Override
    override fun removeEdge(source: V, target: V): Boolean {
        // remove out edges
        val outEdges: List<E> = getVertex(source).outEdges
        var removeOutCount = 0
        run {
            var i = 0
            val size: Int = outEdges.size()
            while (i < size) {
                val edge = outEdges[i]
                if (edge!!.target.equals(target)) {
                    outEdges.remove(i)
                    edges.remove(edge)
                    ++removeOutCount
                }
                i++
            }
        }

        // remove in edges
        val inEdges: List<E> = getVertex(target).inEdges
        var removeInCount = 0
        var i = 0
        val size: Int = inEdges.size()
        while (i < size) {
            val edge = inEdges[i]
            if (edge!!.source.equals(source)) {
                inEdges.remove(i)
                ++removeInCount
            }
            i++
        }
        assert(removeOutCount == removeInCount)
        return removeOutCount > 0
    }

    /** Factory for edges that have attributes.
     *
     * @param <V> Vertex type
     * @param <E> Edge type
    </E></V> */
    interface AttributedEdgeFactory<V, E> : EdgeFactory<V, E> {
        fun createEdge(v0: V, v1: V, vararg attributes: Object?): E
    }

    companion object {
        fun <V, E : DefaultEdge?> create(
            edgeFactory: AttributedEdgeFactory<V, E>
        ): AttributedDirectedGraph<V, E> {
            return AttributedDirectedGraph(edgeFactory)
        }
    }
}
