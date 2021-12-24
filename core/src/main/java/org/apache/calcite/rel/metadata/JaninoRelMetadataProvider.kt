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

import org.apache.calcite.config.CalciteSystemProperty

/**
 * Implementation of the [RelMetadataProvider] interface that generates
 * a class that dispatches to the underlying providers.
 */
class JaninoRelMetadataProvider private constructor(provider: RelMetadataProvider) : RelMetadataProvider,
    MetadataHandlerProvider {
    private val provider: RelMetadataProvider

    /** Private constructor; use [.of].  */
    init {
        this.provider = provider
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (obj === this
                || obj is JaninoRelMetadataProvider
                && (obj as JaninoRelMetadataProvider).provider.equals(provider))
    }

    @Override
    override fun hashCode(): Int {
        return 109 + provider.hashCode()
    }

    @Deprecated // to be removed before 2.0
    @Override
    override fun <M : Metadata?> apply(
        relClass: Class<out RelNode?>?, metadataClass: Class<out M>?
    ): UnboundMetadata<M> {
        throw UnsupportedOperationException()
    }

    @Deprecated // to be removed before 2.0
    @Override
    fun <M : Metadata?> handlers(def: MetadataDef<M?>?): Multimap<Method, MetadataHandler<M>> {
        return provider.handlers(def)
    }

    @Override
    override fun handlers(
        handlerClass: Class<out MetadataHandler<*>?>?
    ): List<MetadataHandler<*>> {
        return provider.handlers(handlerClass)
    }

    @Override
    @Synchronized
    override fun <H : MetadataHandler<*>?> revise(handlerClass: Class<H?>): H {
        return try {
            val key = Key(handlerClass, provider)
            handlerClass.cast(HANDLERS.get(key))
        } catch (e: UncheckedExecutionException) {
            throw Util.throwAsRuntime(Util.causeOrSelf(e))
        } catch (e: ExecutionException) {
            throw Util.throwAsRuntime(Util.causeOrSelf(e))
        }
    }

    /** Registers some classes. Does not flush the providers, but next time we
     * need to generate a provider, it will handle all of these classes. So,
     * calling this method reduces the number of times we need to re-generate.  */
    @Deprecated
    fun register(classes: Iterable<Class<out RelNode?>?>?) {
    }

    /** Exception that indicates there there should be a handler for
     * this class but there is not. The action is probably to
     * re-generate the handler class. Use [MetadataHandlerProvider.NoHandler] instead.
     */
    @Deprecated
    class NoHandler(relClass: Class<out RelNode?>) : MetadataHandlerProvider.NoHandler(relClass)

    /** Key for the cache.  */
    private class Key(
        handlerClass: Class<out MetadataHandler<*>?>,
        provider: RelMetadataProvider
    ) {
        val handlerClass: Class<out MetadataHandler<out Metadata?>?>
        val provider: RelMetadataProvider

        init {
            this.handlerClass = handlerClass
            this.provider = provider
        }

        @Override
        override fun hashCode(): Int {
            return (handlerClass.hashCode() * 37
                    + provider.hashCode()) * 37
        }

        @Override
        override fun equals(@Nullable obj: Object): Boolean {
            return (this === obj
                    || (obj is Key
                    && (obj as Key).handlerClass.equals(handlerClass)
                    && (obj as Key).provider.equals(provider)))
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    override fun <MH : MetadataHandler<*>?> handler(handlerClass: Class<MH>): MH {
        return handlerClass.cast(
            Proxy.newProxyInstance(
                RelMetadataQuery::class.java.getClassLoader(),
                arrayOf<Class>(handlerClass)
            ) { proxy, method, args ->
                val r: RelNode = requireNonNull(args.get(0) as RelNode, "(RelNode) args[0]")
                throw NoHandler(r.getClass())
            })
    }

    companion object {
        // Constants and static fields
        val DEFAULT = of(DefaultRelMetadataProvider.INSTANCE)

        /** Cache of pre-generated handlers by provider and kind of metadata.
         * For the cache to be effective, providers should implement identity
         * correctly.  */
        private val HANDLERS: LoadingCache<Key, MetadataHandler<*>> = maxSize(
            CacheBuilder.newBuilder(),
            CalciteSystemProperty.METADATA_HANDLER_CACHE_MAXIMUM_SIZE.value()
        )
            .build(
                CacheLoader.from { key ->
                    generateCompileAndInstantiate(
                        key.handlerClass,
                        key.provider.handlers(key.handlerClass)
                    )
                })

        /** Creates a JaninoRelMetadataProvider.
         *
         * @param provider Underlying provider
         */
        fun of(provider: RelMetadataProvider): JaninoRelMetadataProvider {
            return if (provider is JaninoRelMetadataProvider) {
                provider
            } else JaninoRelMetadataProvider(provider)
        }

        // helper for initialization
        private fun <K, V> maxSize(
            builder: CacheBuilder<K, V>,
            size: Int
        ): CacheBuilder<K, V> {
            if (size >= 0) {
                builder.maximumSize(size)
            }
            return builder
        }

        private fun <MH : MetadataHandler<*>?> generateCompileAndInstantiate(
            handlerClass: Class<MH>,
            handlers: List<MetadataHandler<out Metadata?>?>
        ): MH {
            val uniqueHandlers: List<MetadataHandler<out Metadata?>?> = handlers.stream()
                .distinct()
                .collect(Collectors.toList())
            val handlerNameAndGeneratedCode: RelMetadataHandlerGeneratorUtil.HandlerNameAndGeneratedCode =
                RelMetadataHandlerGeneratorUtil.generateHandler(handlerClass, uniqueHandlers)
            return try {
                compile(
                    handlerNameAndGeneratedCode.getHandlerName(),
                    handlerNameAndGeneratedCode.getGeneratedCode(), handlerClass, uniqueHandlers
                )
            } catch (e: CompileException) {
                throw RuntimeException(
                    """
    Error compiling:
    ${handlerNameAndGeneratedCode.getGeneratedCode()}
    """.trimIndent(), e
                )
            } catch (e: IOException) {
                throw RuntimeException(
                    """
    Error compiling:
    ${handlerNameAndGeneratedCode.getGeneratedCode()}
    """.trimIndent(), e
                )
            }
        }

        @Throws(CompileException::class, IOException::class)
        fun <MH : MetadataHandler<*>?> compile(
            className: String?,
            generatedCode: String?, handlerClass: Class<MH>,
            argList: List<Object?>
        ): MH {
            val compilerFactory: ICompilerFactory
            val classLoader: ClassLoader =
                Objects.requireNonNull(JaninoRelMetadataProvider::class.java.getClassLoader(), "classLoader")
            compilerFactory = try {
                CompilerFactoryFactory.getDefaultCompilerFactory(classLoader)
            } catch (e: Exception) {
                throw IllegalStateException(
                    "Unable to instantiate java compiler", e
                )
            }
            val compiler: ISimpleCompiler = compilerFactory.newSimpleCompiler()
            compiler.setParentClassLoader(JaninoRexCompiler::class.java.getClassLoader())
            if (CalciteSystemProperty.DEBUG.value()) {
                // Add line numbers to the generated janino class
                compiler.setDebuggingInformation(true, true, true)
                System.out.println(generatedCode)
            }
            compiler.cook(generatedCode)
            val constructor: Constructor
            val o: Object
            try {
                constructor = compiler.getClassLoader().loadClass(className)
                    .getDeclaredConstructors().get(0)
                o = constructor.newInstance(argList.toArray())
            } catch (e: InstantiationException) {
                throw RuntimeException(e)
            } catch (e: IllegalAccessException) {
                throw RuntimeException(e)
            } catch (e: InvocationTargetException) {
                throw RuntimeException(e)
            } catch (e: ClassNotFoundException) {
                throw RuntimeException(e)
            }
            return handlerClass.cast(o)
        }
    }
}
