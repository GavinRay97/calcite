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

import java.util.Objects

/**
 * Default implementation of Edge.
 */
class DefaultEdge(source: Object?, target: Object?) {
    val source: Object
    val target: Object

    init {
        this.source = Objects.requireNonNull(source, "source")
        this.target = Objects.requireNonNull(target, "target")
    }

    @Override
    override fun hashCode(): Int {
        return source.hashCode() * 31 + target.hashCode()
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (this === obj
                || (obj is DefaultEdge
                && (obj as DefaultEdge).source.equals(source)
                && (obj as DefaultEdge).target.equals(target)))
    }

    @Override
    override fun toString(): String {
        return source.toString() + " -> " + target
    }

    companion object {
        fun <V : Object?> factory(): DirectedGraph.EdgeFactory<V, DefaultEdge> {
            // see https://github.com/typetools/checker-framework/issues/3637
            return DirectedGraph.EdgeFactory<V, DefaultEdge> { source1, target1 -> DefaultEdge(source1, target1) }
        }
    }
}
