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

import org.apache.calcite.rel.RelRoot

/**
 * Utilities for [RelOptTable.ViewExpander] and
 * [RelOptTable.ToRelContext].
 */
object ViewExpanders {
    /** Converts a `ViewExpander` to a `ToRelContext`.  */
    fun toRelContext(
        viewExpander: RelOptTable.ViewExpander,
        cluster: RelOptCluster,
        hints: List<RelHint>
    ): RelOptTable.ToRelContext {
        return object : ToRelContext() {
            @get:Override
            val cluster: RelOptCluster
                get() = cluster

            @get:Override
            val tableHints: List<Any>
                get() = hints

            @Override
            fun expandView(
                rowType: RelDataType?, queryString: String?,
                schemaPath: List<String?>?, @Nullable viewPath: List<String?>?
            ): RelRoot {
                return viewExpander.expandView(
                    rowType, queryString, schemaPath,
                    viewPath
                )
            }
        }
    }

    /** Converts a `ViewExpander` to a `ToRelContext`.  */
    fun toRelContext(
        viewExpander: RelOptTable.ViewExpander,
        cluster: RelOptCluster
    ): RelOptTable.ToRelContext {
        return toRelContext(viewExpander, cluster, ImmutableList.of())
    }

    /** Creates a simple `ToRelContext` that cannot expand views.  */
    fun simpleContext(cluster: RelOptCluster?): RelOptTable.ToRelContext {
        return simpleContext(cluster, ImmutableList.of())
    }

    /** Creates a simple `ToRelContext` that cannot expand views.  */
    fun simpleContext(
        cluster: RelOptCluster?,
        hints: List<RelHint>
    ): RelOptTable.ToRelContext {
        return object : ToRelContext() {
            @get:Override
            val cluster: RelOptCluster?
                get() = cluster

            @Override
            fun expandView(
                rowType: RelDataType?, queryString: String?,
                schemaPath: List<String?>?, @Nullable viewPath: List<String?>?
            ): RelRoot {
                throw UnsupportedOperationException()
            }

            @get:Override
            val tableHints: List<Any>
                get() = hints
        }
    }
}
