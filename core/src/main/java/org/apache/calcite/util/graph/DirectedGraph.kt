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

import java.util.Collection
import java.util.List
import java.util.Set

/**
 * Directed graph.
 *
 * @param <V> Vertex type
 * @param <E> Edge type
</E></V> */
interface DirectedGraph<V, E> {
    /** Adds a vertex to this graph.
     *
     * @param vertex Vertex
     * @return Whether vertex was added
     */
    fun addVertex(vertex: V): Boolean

    /** Adds an edge to this graph.
     *
     * @param vertex Source vertex
     * @param targetVertex Target vertex
     * @return New edge, if added, otherwise null
     * @throws IllegalArgumentException if either vertex is not already in graph
     */
    @Nullable
    fun addEdge(vertex: V, targetVertex: V): E

    @Nullable
    fun getEdge(source: V, target: V): E
    fun removeEdge(vertex: V, targetVertex: V): Boolean
    fun vertexSet(): Set<V>?

    /** Removes from this graph all vertices that are in `collection`,
     * and the edges into and out of those vertices.  */
    fun removeAllVertices(collection: Collection<V>?)
    fun getOutwardEdges(source: V): List<E>
    fun getInwardEdges(vertex: V): List<E>
    fun edgeSet(): Set<E>

    /** Factory for edges.
     *
     * @param <V> Vertex type
     * @param <E> Edge type
    </E></V> */
    interface EdgeFactory<V, E> {
        fun createEdge(v0: V, v1: V): E
    }
}
