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
 * Implementation of the [RelMetadataProvider]
 * interface via the
 * [org.apache.calcite.util.Glossary.CHAIN_OF_RESPONSIBILITY_PATTERN].
 *
 *
 * When a consumer calls the [.apply] method to ask for a provider
 * for a particular type of [RelNode] and [Metadata], scans the list
 * of underlying providers.
 */
class ChainedRelMetadataProvider @SuppressWarnings("argument.type.incompatible") protected constructor(
    providers: ImmutableList<RelMetadataProvider?>
) : RelMetadataProvider {
    //~ Instance fields --------------------------------------------------------
    private val providers: ImmutableList<RelMetadataProvider>
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a chain.
     */
    init {
        this.providers = providers
        assert(!providers.contains(this))
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (obj === this
                || obj is ChainedRelMetadataProvider
                && providers.equals((obj as ChainedRelMetadataProvider).providers))
    }

    @Override
    override fun hashCode(): Int {
        return providers.hashCode()
    }

    @Deprecated // to be removed before 2.0
    @Override
    override fun <M : Metadata?> apply(
        relClass: Class<out RelNode?>?,
        metadataClass: Class<out M>
    ): @Nullable UnboundMetadata<M>? {
        val functions: List<UnboundMetadata<M>> = ArrayList()
        for (provider in providers) {
            val function: UnboundMetadata<M> = provider.apply(relClass, metadataClass) ?: continue
            functions.add(function)
        }
        return when (functions.size()) {
            0 -> null
            1 -> functions[0]
            else -> UnboundMetadata<M> { rel, mq ->
                val metadataList: List<Metadata> = ArrayList()
                for (function in functions) {
                    val metadata: Metadata = function.bind(rel, mq)
                    if (metadata != null) {
                        metadataList.add(metadata)
                    }
                }
                metadataClass.cast(
                    Proxy.newProxyInstance(
                        metadataClass.getClassLoader(), arrayOf<Class>(metadataClass),
                        ChainedInvocationHandler(metadataList)
                    )
                )
            }
        }
    }

    @Deprecated // to be removed before 2.0
    @Override
    fun <M : Metadata?> handlers(
        def: MetadataDef<M?>?
    ): Multimap<Method, MetadataHandler<M>> {
        val builder: ImmutableMultimap.Builder<Method, MetadataHandler<M>> = ImmutableMultimap.builder()
        for (provider in providers.reverse()) {
            builder.putAll(provider.handlers(def))
        }
        return builder.build()
    }

    @Override
    override fun handlers(
        handlerClass: Class<out MetadataHandler<*>?>?
    ): List<MetadataHandler<*>> {
        val builder: ImmutableList.Builder<MetadataHandler<*>> = ImmutableList.builder()
        for (provider in providers) {
            builder.addAll(provider.handlers(handlerClass))
        }
        return builder.build()
    }

    /** Invocation handler that calls a list of [Metadata] objects,
     * returning the first non-null value.  */
    private class ChainedInvocationHandler internal constructor(metadataList: List<Metadata?>?) : InvocationHandler {
        private val metadataList: List<Metadata>

        init {
            this.metadataList = ImmutableList.copyOf(metadataList)
        }

        @Override
        @Nullable
        @Throws(Throwable::class)
        operator fun invoke(proxy: Object?, method: Method, @Nullable args: Array<Object?>?): Object? {
            for (metadata in metadataList) {
                try {
                    val o: Object = method.invoke(metadata, args)
                    if (o != null) {
                        return o
                    }
                } catch (e: InvocationTargetException) {
                    throw Util.throwAsRuntime(Util.causeOrSelf(e))
                }
            }
            return null
        }
    }

    companion object {
        /** Creates a chain.  */
        fun of(list: List<RelMetadataProvider?>?): RelMetadataProvider {
            return ChainedRelMetadataProvider(ImmutableList.copyOf(list))
        }
    }
}
