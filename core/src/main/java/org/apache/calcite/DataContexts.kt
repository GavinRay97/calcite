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
package org.apache.calcite

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.jdbc.CalciteConnection
import org.apache.calcite.linq4j.QueryProvider
import org.apache.calcite.schema.SchemaPlus
import com.google.common.collect.ImmutableMap
import java.io.Serializable
import java.util.Map
import java.util.function.Function
import java.util.Objects.requireNonNull

/** Utilities for [DataContext].  */
object DataContexts {
    /** Instance of [DataContext] that has no variables.  */
    val EMPTY: DataContext = EmptyDataContext()

    /** Returns an instance of [DataContext] with the given map.  */
    fun of(map: Map<String?, *>?): DataContext {
        return MapDataContext(map)
    }

    /** Returns an instance of [DataContext] with the given function.  */
    fun of(fn: Function<String?, out Object?>?): DataContext {
        return FunctionDataContext(fn)
    }

    /** Returns an instance of [DataContext] with the given connection
     * and root schema but no variables.  */
    fun of(
        connection: CalciteConnection?,
        @Nullable rootSchema: SchemaPlus?
    ): DataContext {
        return DataContextImpl(connection, rootSchema, ImmutableMap.of())
    }

    /** Implementation of [DataContext] that has no variables.
     *
     *
     * It is [Serializable] for Spark's benefit.  */
    private class EmptyDataContext : DataContext, Serializable {
        @get:Nullable
        @get:Override
        override val rootSchema: SchemaPlus?
            get() = null

        @get:Override
        override val typeFactory: JavaTypeFactory
            get() {
                throw UnsupportedOperationException()
            }

        @get:Override
        override val queryProvider: QueryProvider
            get() {
                throw UnsupportedOperationException()
            }

        @Override
        @Nullable
        override operator fun get(name: String?): Object? {
            return null
        }
    }

    /** Implementation of [DataContext] backed by a Map.
     *
     *
     * Keys and values in the map must not be null. Rather than storing a null
     * value for a key, remove the key from the map; the effect will be the
     * same.  */
    private class MapDataContext internal constructor(map: Map<String?, *>?) : EmptyDataContext() {
        private val map: ImmutableMap<String, *>

        init {
            this.map = ImmutableMap.copyOf(map)
        }

        @Override
        @Nullable
        override fun get(name: String?): Object {
            return map.get(name)
        }
    }

    /** Implementation of [DataContext] backed by a Function.  */
    private class FunctionDataContext internal constructor(fn: Function<String?, out Object?>?) : EmptyDataContext() {
        private val fn: Function<String, out Object?>

        init {
            this.fn = requireNonNull(fn, "fn")
        }

        @Override
        @Nullable
        override fun get(name: String?): Object {
            return fn.apply(name)
        }
    }

    /** Implementation of [DataContext] backed by a Map.  */
    private class DataContextImpl internal constructor(
        connection: CalciteConnection?,
        @Nullable rootSchema: SchemaPlus?, map: Map<String?, Object?>?
    ) : MapDataContext(map) {
        private val connection: CalciteConnection

        @Nullable
        private override val rootSchema: SchemaPlus

        init {
            this.connection = requireNonNull(connection, "connection")
            this.rootSchema = requireNonNull(rootSchema, "rootSchema")
        }

        @get:Override
        override val typeFactory: JavaTypeFactory
            get() = connection.getTypeFactory()

        @Override
        @Nullable
        override fun getRootSchema(): SchemaPlus {
            return rootSchema
        }

        @get:Override
        override val queryProvider: QueryProvider
            get() = connection
    }
}
