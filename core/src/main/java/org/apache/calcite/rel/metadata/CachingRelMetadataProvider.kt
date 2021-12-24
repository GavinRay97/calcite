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
package org.apache.calcite.rel.metadata

import org.apache.calcite.plan.RelOptPlanner

/**
 * Implementation of the [RelMetadataProvider]
 * interface that caches results from an underlying provider.
 */
@Deprecated // to be removed before 2.0
class CachingRelMetadataProvider(
    underlyingProvider: RelMetadataProvider,
    planner: RelOptPlanner
) : RelMetadataProvider {
    //~ Instance fields --------------------------------------------------------
    private val cache: Map<List, CacheEntry> = HashMap()
    private val underlyingProvider: RelMetadataProvider
    private val planner: RelOptPlanner

    //~ Constructors -----------------------------------------------------------
    init {
        this.underlyingProvider = underlyingProvider
        this.planner = planner
    }

    //~ Methods ----------------------------------------------------------------
    @Deprecated // to be removed before 2.0
    @Override
    override fun <M : Metadata?> apply(
        relClass: Class<out RelNode?>,
        metadataClass: Class<out M>
    ): @Nullable UnboundMetadata<M>? {
        val function: UnboundMetadata<M> = underlyingProvider.apply(relClass, metadataClass) ?: return null

        // TODO jvs 30-Mar-2006: Use meta-metadata to decide which metadata
        // query results can stay fresh until the next Ice Age.
        return UnboundMetadata<M> { rel, mq ->
            val metadata: Metadata = requireNonNull(
                function.bind(rel, mq)
            ) {
                ("metadata must not be null, relClass=" + relClass
                        + ", metadataClass=" + metadataClass)
            }
            metadataClass.cast(
                Proxy.newProxyInstance(
                    metadataClass.getClassLoader(), arrayOf<Class>(metadataClass),
                    CachingInvocationHandler(metadata)
                )
            )
        }
    }

    @Deprecated // to be removed before 2.0
    @Override
    fun <M : Metadata?> handlers(
        def: MetadataDef<M?>?
    ): Multimap<Method, MetadataHandler<M>> {
        return underlyingProvider.handlers(def)
    }

    @Override
    override fun handlers(
        handlerClass: Class<out MetadataHandler<*>?>?
    ): List<MetadataHandler<*>> {
        return underlyingProvider.handlers(handlerClass)
    }
    //~ Inner Classes ----------------------------------------------------------
    /** An entry in the cache. Consists of the cached object and the timestamp
     * when the entry is valid. If read at a later timestamp, the entry will be
     * invalid and will be re-computed as if it did not exist. The net effect is a
     * lazy-flushing cache.  */
    private class CacheEntry {
        var timestamp: Long = 0

        @Nullable
        var result: Object? = null
    }

    /** Implementation of [InvocationHandler] for calls to a
     * [CachingRelMetadataProvider]. Each request first looks in the cache;
     * if the cache entry is present and not expired, returns the cache entry,
     * otherwise computes the value and stores in the cache.  */
    private inner class CachingInvocationHandler internal constructor(metadata: Metadata?) : InvocationHandler {
        private val metadata: Metadata

        init {
            this.metadata = requireNonNull(metadata, "metadata")
        }

        @Override
        @Nullable
        @Throws(Throwable::class)
        operator fun invoke(proxy: Object?, method: Method, @Nullable args: Array<Object?>?): Object? {
            // Compute hash key.
            val builder: ImmutableList.Builder<Object> = ImmutableList.builder()
            builder.add(method)
            builder.add(metadata.rel())
            if (args != null) {
                for (arg in args) {
                    // Replace null values because ImmutableList does not allow them.
                    builder.add(NullSentinel.mask(arg))
                }
            }
            val key: List<Object> = builder.build()
            val timestamp: Long = planner.getRelMetadataTimestamp(metadata.rel())

            // Perform cache lookup.
            var entry = cache[key]
            if (entry != null) {
                if (timestamp == entry.timestamp) {
                    return entry.result
                }
            }

            // Cache miss or stale.
            return try {
                val result: Object = method.invoke(metadata, args)
                if (result != null) {
                    entry = CacheEntry()
                    entry.timestamp = timestamp
                    entry.result = result
                    cache.put(key, entry)
                }
                result
            } catch (e: InvocationTargetException) {
                throw castNonNull(e.getCause())
            }
        }
    }
}
