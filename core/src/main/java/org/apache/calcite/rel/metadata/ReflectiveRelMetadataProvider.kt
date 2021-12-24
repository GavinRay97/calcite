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
 * Implementation of the [RelMetadataProvider] interface that dispatches
 * metadata methods to methods on a given object via reflection.
 *
 *
 * The methods on the target object must be public and non-static, and have
 * the same signature as the implemented metadata method except for an
 * additional first parameter of type [RelNode] or a sub-class. That
 * parameter gives this provider an indication of that relational expressions it
 * can handle.
 *
 *
 * For an example, see [RelMdColumnOrigins.SOURCE].
 */
class ReflectiveRelMetadataProvider protected constructor(
    map: ConcurrentMap<Class<RelNode?>?, UnboundMetadata?>,
    metadataClass0: Class<out Metadata?>,
    handlerMap: Multimap<Method?, MetadataHandler<*>?>,
    handlerClass: Class<out MetadataHandler<*>?>
) : RelMetadataProvider, ReflectiveVisitor {
    //~ Instance fields --------------------------------------------------------
    @Deprecated // to be removed before 2.0
    private val map: ConcurrentMap<Class<RelNode>, UnboundMetadata>

    @Deprecated // to be removed before 2.0
    private val metadataClass0: Class<out Metadata?>

    @Deprecated // to be removed before 2.0
    private val handlerMap: ImmutableMultimap<Method, MetadataHandler>
    private val handlerClass: Class<out MetadataHandler<*>?>
    private val handlers: ImmutableList<MetadataHandler<*>>
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a ReflectiveRelMetadataProvider.
     *
     * @param map Map
     * @param metadataClass0 Metadata class
     * @param handlerMap Methods handled and the objects to call them on
     */
    init {
        Preconditions.checkArgument(
            !map.isEmpty(), "ReflectiveRelMetadataProvider "
                    + "methods map is empty; are your methods named wrong?"
        )
        this.map = map
        this.metadataClass0 = metadataClass0
        this.handlerMap = ImmutableMultimap.copyOf(handlerMap)
        this.handlerClass = handlerClass
        handlers = ImmutableList.copyOf(handlerMap.values())
    }

    @Deprecated // to be removed before 2.0
    @Override
    fun <M : Metadata?> handlers(
        def: MetadataDef<M>
    ): Multimap<Method, MetadataHandler<M>> {
        val builder: ImmutableMultimap.Builder<Method, MetadataHandler<M>> = ImmutableMultimap.builder()
        for (entry in handlerMap.entries()) {
            if (def.methods.contains(entry.getKey())) {
                builder.put(entry.getKey(), entry.getValue())
            }
        }
        return builder.build()
    }

    @Override
    override fun handlers(
        handlerClass: Class<out MetadataHandler<*>?>?
    ): List<MetadataHandler<*>> {
        return if (this.handlerClass.isAssignableFrom(handlerClass)) {
            handlers
        } else {
            ImmutableList.of()
        }
    }

    //~ Methods ----------------------------------------------------------------
    @Deprecated // to be removed before 2.0
    @Override
    override fun <M : Metadata?> apply(
        relClass: Class<out RelNode?>?, metadataClass: Class<out M>
    ): @Nullable UnboundMetadata<M>? {
        return if (metadataClass === metadataClass0) {
            apply<Metadata>(relClass)
        } else {
            null
        }
    }

    @SuppressWarnings(["unchecked", "SuspiciousMethodCalls"])
    @Deprecated // to be removed before 2.0
    fun <M : Metadata?> apply(
        relClass: Class<out RelNode?>?
    ): @Nullable UnboundMetadata<M>? {
        var relClass: Class<out RelNode?>? = relClass
        val newSources: List<Class<out RelNode?>> = ArrayList()
        while (true) {
            val function: UnboundMetadata<M> = map.get(relClass)
            if (function != null) {
                for (@SuppressWarnings("rawtypes") clazz in newSources) {
                    map.put(clazz, function)
                }
                return function
            } else {
                newSources.add(relClass)
            }
            for (interfaceClass in relClass.getInterfaces()) {
                if (RelNode::class.java.isAssignableFrom(interfaceClass)) {
                    val function2: UnboundMetadata<M> = map.get(interfaceClass)
                    if (function2 != null) {
                        for (@SuppressWarnings("rawtypes") clazz in newSources) {
                            map.put(clazz, function2)
                        }
                        return function2
                    }
                }
            }
            val superclass: Class<*> = relClass.getSuperclass()
            relClass = if (superclass != null && RelNode::class.java.isAssignableFrom(superclass)) {
                superclass as Class<RelNode?>
            } else {
                return null
            }
        }
    }

    /** Workspace for computing which methods can act as handlers for
     * given metadata methods.  */
    @Deprecated // to be removed before 2.0
    internal class Space(providerMap: Multimap<Method?, MetadataHandler<*>?>) {
        val classes: Set<Class<RelNode>> = HashSet()
        val handlerMap: Map<Pair<Class<RelNode>, Method>, Method> = HashMap()
        val providerMap: ImmutableMultimap<Method, MetadataHandler<*>>

        init {
            this.providerMap = ImmutableMultimap.copyOf(providerMap)

            // Find the distinct set of RelNode classes handled by this provider,
            // ordered base-class first.
            for (entry in providerMap.entries()) {
                val method: Method = entry.getKey()
                val provider: MetadataHandler<*> = entry.getValue()
                for (handlerMethod in provider.getClass().getMethods()) {
                    if (couldImplement(handlerMethod, method)) {
                        @SuppressWarnings("unchecked") val relNodeClass: Class<RelNode> =
                            handlerMethod.getParameterTypes().get(0) as Class<RelNode>
                        classes.add(relNodeClass)
                        handlerMap.put(Pair.of(relNodeClass, method), handlerMethod)
                    }
                }
            }
        }

        /** Finds an implementation of a method for `relNodeClass` or its
         * nearest base class. Assumes that base classes have already been added to
         * `map`.  */
        @SuppressWarnings(["unchecked", "SuspiciousMethodCalls"])
        fun find(relNodeClass: Class<out RelNode?>, method: Method): Method {
            Objects.requireNonNull(relNodeClass, "relNodeClass")
            var r: Class = relNodeClass
            while (true) {
                var implementingMethod: Method? = handlerMap[Pair.of(r, method)]
                if (implementingMethod != null) {
                    return implementingMethod
                }
                for (clazz in r.getInterfaces()) {
                    if (RelNode::class.java.isAssignableFrom(clazz)) {
                        implementingMethod = handlerMap[Pair.of(clazz, method)]
                        if (implementingMethod != null) {
                            return implementingMethod
                        }
                    }
                }
                r = r.getSuperclass()
                if (r == null || !RelNode::class.java.isAssignableFrom(r)) {
                    throw IllegalArgumentException(
                        "No handler for method [" + method
                                + "] applied to argument of type [" + relNodeClass
                                + "]; we recommend you create a catch-all (RelNode) handler"
                    )
                }
            }
        }
    }

    /** Extended work space.  */
    @Deprecated // to be removed before 2.0
    internal class Space2(
        metadataClass0: Class<Metadata?>,
        providerMap: ImmutableMultimap<Method?, MetadataHandler<*>?>
    ) : Space(providerMap) {
        val metadataClass0: Class<Metadata>

        init {
            this.metadataClass0 = metadataClass0
        }

        companion object {
            @Deprecated // to be removed before 2.0
            fun create(
                target: MetadataHandler<*>?,
                methods: ImmutableList<Method?>
            ): Space2 {
                assert(methods.size() > 0)
                val method0: Method = methods.get(0)
                val metadataClass0: Class<Metadata?> = method0.getDeclaringClass() as Class
                assert(Metadata::class.java.isAssignableFrom(metadataClass0))
                for (method in methods) {
                    assert(method.getDeclaringClass() === metadataClass0)
                }
                val providerBuilder: ImmutableMultimap.Builder<Method, MetadataHandler<*>> = ImmutableMultimap.builder()
                for (method in methods) {
                    providerBuilder.put(method, target)
                }
                return Space2(metadataClass0, providerBuilder.build())
            }
        }
    }

    companion object {
        /** Returns an implementation of [RelMetadataProvider] that scans for
         * methods with a preceding argument.
         *
         *
         * For example, [BuiltInMetadata.Selectivity] has a method
         * [BuiltInMetadata.Selectivity.getSelectivity].
         * A class
         *
         * <blockquote><pre>`
         * class RelMdSelectivity {
         * public Double getSelectivity(Union rel, RexNode predicate) { }
         * public Double getSelectivity(Filter rel, RexNode predicate) { }
        `</pre></blockquote> *
         *
         *
         * provides implementations of selectivity for relational expressions
         * that extend [org.apache.calcite.rel.core.Union]
         * or [org.apache.calcite.rel.core.Filter].
         */
        @Deprecated // to be removed before 2.0
        fun reflectiveSource(
            method: Method?,
            target: MetadataHandler
        ): RelMetadataProvider {
            return reflectiveSource(target, ImmutableList.of(method), target.getDef().handlerClass)
        }

        /** Returns a reflective metadata provider that implements several
         * methods.  */
        @Deprecated // to be removed before 2.0
        fun reflectiveSource(
            target: MetadataHandler,
            vararg methods: Method?
        ): RelMetadataProvider {
            return reflectiveSource(target, ImmutableList.copyOf(methods), target.getDef().handlerClass)
        }

        @SuppressWarnings("deprecation")
        fun <M : Metadata?> reflectiveSource(
            handler: MetadataHandler<out M?>, handlerClass: Class<out MetadataHandler<M?>?>
        ): RelMetadataProvider {
            //When deprecated code is removed, handler.getDef().methods will no longer be required
            return reflectiveSource(handler, handler.getDef().methods, handlerClass)
        }

        @Deprecated // to be removed before 2.0
        private fun reflectiveSource(
            target: MetadataHandler, methods: ImmutableList<Method?>,
            handlerClass: Class<out MetadataHandler<*>?>
        ): RelMetadataProvider {
            val space = Space2.create(target, methods)

            // This needs to be a concurrent map since RelMetadataProvider are cached in static
            // fields, thus the map is subject to concurrent modifications later.
            // See map.put in org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider.apply(
            // java.lang.Class<? extends org.apache.calcite.rel.RelNode>)
            val methodsMap: ConcurrentMap<Class<RelNode?>?, UnboundMetadata?> = ConcurrentHashMap()
            for (key in space.classes) {
                val builder: ImmutableNullableList.Builder<Method> = ImmutableNullableList.builder()
                for (method in methods) {
                    builder.add(space.find(key, method))
                }
                val handlerMethods: List<Method> = builder.build()
                val function = UnboundMetadata { rel, mq ->
                    Proxy.newProxyInstance(
                        space.metadataClass0.getClassLoader(), arrayOf<Class>(space.metadataClass0)
                    ) { proxy, method, args ->
                        // Suppose we are an implementation of Selectivity
                        // that wraps "filter", a LogicalFilter. Then we
                        // implement
                        //   Selectivity.selectivity(rex)
                        // by calling method
                        //   new SelectivityImpl().selectivity(filter, rex)
                        if (method.equals(BuiltInMethod.METADATA_REL.method)) {
                            return@newProxyInstance rel
                        }
                        if (method.equals(BuiltInMethod.OBJECT_TO_STRING.method)) {
                            return@newProxyInstance space.metadataClass0.getSimpleName() + "(" + rel + ")"
                        }
                        val i: Int = methods.indexOf(method)
                        if (i < 0) {
                            throw AssertionError(
                                "not handled: " + method
                                        + " for " + rel
                            )
                        }
                        val handlerMethod: Method = handlerMethods[i]
                            ?: throw AssertionError(
                                "not handled: " + method
                                        + " for " + rel
                            )
                        val args1: Array<Object?>
                        val key1: List
                        if (args == null) {
                            args1 = arrayOf<Object?>(rel, mq)
                            key1 = FlatLists.of(rel, method)
                        } else {
                            args1 = arrayOfNulls<Object>(args.length + 2)
                            args1[0] = rel
                            args1[1] = mq
                            System.arraycopy(args, 0, args1, 2, args.length)
                            val args2: Array<Object?> = args1.clone()
                            args2[1] = method // replace RelMetadataQuery with method
                            for (j in args2.indices) {
                                if (args2[j] == null) {
                                    args2[j] = NullSentinel.INSTANCE
                                } else if (args2[j] is RexNode) {
                                    // Can't use RexNode.equals - it is not deep
                                    args2[j] = args2[j].toString()
                                }
                            }
                            key1 = FlatLists.copyOf(args2)
                        }
                        if (mq.map.put(rel, key1, NullSentinel.INSTANCE) != null) {
                            throw CyclicMetadataException()
                        }
                        try {
                            return@newProxyInstance handlerMethod.invoke(target, args1)
                        } catch (e: InvocationTargetException) {
                            throw Util.throwAsRuntime(Util.causeOrSelf(e))
                        } catch (e: UndeclaredThrowableException) {
                            throw Util.throwAsRuntime(Util.causeOrSelf(e))
                        } finally {
                            mq.map.remove(rel, key1)
                        }
                    }
                }
                methodsMap.put(key, function)
            }
            return ReflectiveRelMetadataProvider(
                methodsMap, space.metadataClass0,
                space.providerMap, handlerClass
            )
        }

        @Deprecated // to be removed before 2.0
        private fun couldImplement(handlerMethod: Method, method: Method): Boolean {
            if (!handlerMethod.getName().equals(method.getName())
                || handlerMethod.getModifiers() and Modifier.STATIC !== 0 || handlerMethod.getModifiers() and Modifier.PUBLIC === 0
            ) {
                return false
            }
            val parameterTypes1: Array<Class<*>> = handlerMethod.getParameterTypes()
            val parameterTypes: Array<Class<*>> = method.getParameterTypes()
            return (parameterTypes1.size == parameterTypes.size + 2 && RelNode::class.java.isAssignableFrom(
                parameterTypes1[0]
            )
                    && RelMetadataQuery::class.java === parameterTypes1[1] && Arrays.asList(parameterTypes)
                .equals(Util.skip(Arrays.asList(parameterTypes1), 2)))
        }
    }
}
