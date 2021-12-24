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

import org.apache.calcite.rel.RelNode

/**
 * Implementation of [MetadataFactory] that gets providers from a
 * [RelMetadataProvider] and stores them in a cache.
 *
 *
 * The cache does not store metadata. It remembers which providers can
 * provide which kinds of metadata, for which kinds of relational
 * expressions.
 *
 */
@Deprecated // to be removed before 2.0
@Deprecated("Use {@link RelMetadataQuery}.")
class MetadataFactoryImpl(provider: RelMetadataProvider) : MetadataFactory {
    private val cache: LoadingCache<Pair<Class<RelNode>, Class<Metadata>>, UnboundMetadata<Metadata>>

    init {
        cache = CacheBuilder.newBuilder().build(loader(provider))
    }

    @Override
    override fun <M : Metadata?> query(
        rel: RelNode, mq: RelMetadataQuery?,
        metadataClazz: Class<M?>
    ): M {
        return try {
            val key: Pair<Class<RelNode>, Class<Metadata>> =
                Pair.of(rel.getClass() as Class<RelNode?>, metadataClazz as Class<Metadata?>)
            val apply: Metadata = cache.get(key).bind(rel, mq)
            metadataClazz.cast(apply)
        } catch (e: UncheckedExecutionException) {
            throw Util.throwAsRuntime(Util.causeOrSelf(e))
        } catch (e: ExecutionException) {
            throw Util.throwAsRuntime(Util.causeOrSelf(e))
        }
    }

    companion object {
        @SuppressWarnings("unchecked")
        val DUMMY: UnboundMetadata<Metadata> = UnboundMetadata<Metadata> { rel, mq -> null }
        private fun loader(provider: RelMetadataProvider): CacheLoader<Pair<Class<RelNode>, Class<Metadata>>, UnboundMetadata<Metadata>> {
            return CacheLoader.< Pair < Class < RelNode >, Class<Metadata>>, UnboundMetadata<Metadata>>from<Pair<Class<RelNode?>?, Class<Metadata?>?>?, UnboundMetadata<Metadata?>?>({ key ->
                val function: UnboundMetadata<Metadata?> = provider.apply(key.left, key.right)
                if (function != null) function else DUMMY
            })
        }
    }
}
