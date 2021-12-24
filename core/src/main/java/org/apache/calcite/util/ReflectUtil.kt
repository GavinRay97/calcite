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
package org.apache.calcite.util

import org.apache.calcite.linq4j.function.Parameter

/**
 * Static utilities for Java reflection.
 */
object ReflectUtil {
    //~ Static fields/initializers ---------------------------------------------
    private var primitiveToBoxingMap: Map<Class, Class>? = null
    private var primitiveToByteBufferReadMethod: Map<Class, Method>? = null
    private var primitiveToByteBufferWriteMethod: Map<Class, Method>? = null

    init {
        primitiveToBoxingMap = HashMap()
        primitiveToBoxingMap.put(Boolean.TYPE, Boolean::class.java)
        primitiveToBoxingMap.put(Byte.TYPE, Byte::class.java)
        primitiveToBoxingMap.put(Character.TYPE, Character::class.java)
        primitiveToBoxingMap.put(Double.TYPE, Double::class.java)
        primitiveToBoxingMap.put(Float.TYPE, Float::class.java)
        primitiveToBoxingMap.put(Integer.TYPE, Integer::class.java)
        primitiveToBoxingMap.put(Long.TYPE, Long::class.java)
        primitiveToBoxingMap.put(Short.TYPE, Short::class.java)
        primitiveToByteBufferReadMethod = HashMap()
        primitiveToByteBufferWriteMethod = HashMap()
        val methods: Array<Method> = ByteBuffer::class.java.getDeclaredMethods()
        for (method in org.apache.calcite.util.methods) {
            val paramTypes: Array<Class> = org.apache.calcite.util.method.getParameterTypes()
            if (org.apache.calcite.util.method.getName().startsWith("get")) {
                if (!org.apache.calcite.util.method.getReturnType().isPrimitive()) {
                    continue
                }
                if (org.apache.calcite.util.paramTypes.size != 1) {
                    continue
                }
                primitiveToByteBufferReadMethod.put(
                    org.apache.calcite.util.method.getReturnType(), org.apache.calcite.util.method
                )

                // special case for Boolean:  treat as byte
                if (org.apache.calcite.util.method.getReturnType().equals(Byte.TYPE)) {
                    primitiveToByteBufferReadMethod.put(Boolean.TYPE, org.apache.calcite.util.method)
                }
            } else if (org.apache.calcite.util.method.getName().startsWith("put")) {
                if (org.apache.calcite.util.paramTypes.size != 2) {
                    continue
                }
                if (!org.apache.calcite.util.paramTypes.get(1).isPrimitive()) {
                    continue
                }
                primitiveToByteBufferWriteMethod.put(
                    org.apache.calcite.util.paramTypes.get(1),
                    org.apache.calcite.util.method
                )

                // special case for Boolean:  treat as byte
                if (org.apache.calcite.util.paramTypes.get(1).equals(Byte.TYPE)) {
                    primitiveToByteBufferWriteMethod.put(Boolean.TYPE, org.apache.calcite.util.method)
                }
            }
        }
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Uses reflection to find the correct java.nio.ByteBuffer "absolute get"
     * method for a given primitive type.
     *
     * @param clazz the Class object representing the primitive type
     * @return corresponding method
     */
    fun getByteBufferReadMethod(clazz: Class): Method {
        assert(clazz.isPrimitive())
        return castNonNull(primitiveToByteBufferReadMethod!![clazz])
    }

    /**
     * Uses reflection to find the correct java.nio.ByteBuffer "absolute put"
     * method for a given primitive type.
     *
     * @param clazz the Class object representing the primitive type
     * @return corresponding method
     */
    fun getByteBufferWriteMethod(clazz: Class): Method {
        assert(clazz.isPrimitive())
        return castNonNull(primitiveToByteBufferWriteMethod!![clazz])
    }

    /**
     * Gets the Java boxing class for a primitive class.
     *
     * @param primitiveClass representative class for primitive (e.g.
     * java.lang.Integer.TYPE)
     * @return corresponding boxing Class (e.g. java.lang.Integer)
     */
    fun getBoxingClass(primitiveClass: Class): Class {
        assert(primitiveClass.isPrimitive())
        return castNonNull(primitiveToBoxingMap!![primitiveClass])
    }

    /**
     * Gets the name of a class with no package qualifiers; if it's an inner
     * class, it will still be qualified by the containing class (X$Y).
     *
     * @param c the class of interest
     * @return the unqualified name
     */
    fun getUnqualifiedClassName(c: Class): String {
        val className: String = c.getName()
        val lastDot: Int = className.lastIndexOf('.')
        return if (lastDot < 0) {
            className
        } else className.substring(lastDot + 1)
    }

    /**
     * Composes a string representing a human-readable method name (with neither
     * exception nor return type information).
     *
     * @param declaringClass class on which method is defined
     * @param methodName     simple name of method without signature
     * @param paramTypes     method parameter types
     * @return unmangled method name
     */
    fun getUnmangledMethodName(
        declaringClass: Class,
        methodName: String?,
        paramTypes: Array<Class>
    ): String {
        val sb = StringBuilder()
        sb.append(declaringClass.getName())
        sb.append(".")
        sb.append(methodName)
        sb.append("(")
        for (i in paramTypes.indices) {
            if (i > 0) {
                sb.append(", ")
            }
            sb.append(paramTypes[i].getName())
        }
        sb.append(")")
        return sb.toString()
    }

    /**
     * Composes a string representing a human-readable method name (with neither
     * exception nor return type information).
     *
     * @param method method whose name is to be generated
     * @return unmangled method name
     */
    fun getUnmangledMethodName(
        method: Method
    ): String {
        return getUnmangledMethodName(
            method.getDeclaringClass(),
            method.getName(),
            method.getParameterTypes()
        )
    }

    /**
     * Implements the [org.apache.calcite.util.Glossary.VISITOR_PATTERN] via
     * reflection. The basic technique is taken from [a
 * Javaworld article](http://www.javaworld.com/javaworld/javatips/jw-javatip98.html). For an example of how to use it, see
     * `ReflectVisitorTest`.
     *
     *
     * Visit method lookup follows the same rules as if
     * compile-time resolution for VisitorClass.visit(VisiteeClass) were
     * performed. An ambiguous match due to multiple interface inheritance
     * results in an IllegalArgumentException. A non-match is indicated by
     * returning false.
     *
     * @param visitor         object whose visit method is to be invoked
     * @param visitee         object to be passed as a parameter to the visit
     * method
     * @param hierarchyRoot   if non-null, visitor method will only be invoked if
     * it takes a parameter whose type is a subtype of
     * hierarchyRoot
     * @param visitMethodName name of visit method, e.g. "visit"
     * @return true if a matching visit method was found and invoked
     */
    fun invokeVisitor(
        visitor: ReflectiveVisitor,
        visitee: Object,
        hierarchyRoot: Class?,
        visitMethodName: String
    ): Boolean {
        return invokeVisitorInternal(
            visitor,
            visitee,
            hierarchyRoot,
            visitMethodName
        )
    }

    /**
     * Shared implementation of the two forms of invokeVisitor.
     *
     * @param visitor         object whose visit method is to be invoked
     * @param visitee         object to be passed as a parameter to the visit
     * method
     * @param hierarchyRoot   if non-null, visitor method will only be invoked if
     * it takes a parameter whose type is a subtype of
     * hierarchyRoot
     * @param visitMethodName name of visit method, e.g. "visit"
     * @return true if a matching visit method was found and invoked
     */
    private fun invokeVisitorInternal(
        visitor: Object,
        visitee: Object,
        hierarchyRoot: Class?,
        visitMethodName: String
    ): Boolean {
        val visitorClass: Class<*> = visitor.getClass()
        val visiteeClass: Class = visitee.getClass()
        val method: Method = lookupVisitMethod(
            visitorClass,
            visiteeClass,
            visitMethodName
        ) ?: return false
        if (hierarchyRoot != null) {
            val paramType: Class = method.getParameterTypes().get(0)
            if (!hierarchyRoot.isAssignableFrom(paramType)) {
                return false
            }
        }
        try {
            method.invoke(
                visitor,
                visitee
            )
        } catch (ex: IllegalAccessException) {
            throw RuntimeException(ex)
        } catch (ex: InvocationTargetException) {
            // visit methods aren't allowed to have throws clauses,
            // so the only exceptions which should come
            // to us are RuntimeExceptions and Errors
            throw Util.throwAsRuntime(Util.causeOrSelf(ex))
        }
        return true
    }

    /**
     * Looks up a visit method.
     *
     * @param visitorClass    class of object whose visit method is to be invoked
     * @param visiteeClass    class of object to be passed as a parameter to the
     * visit method
     * @param visitMethodName name of visit method
     * @return method found, or null if none found
     */
    @Nullable
    fun lookupVisitMethod(
        visitorClass: Class<*>,
        visiteeClass: Class<*>,
        visitMethodName: String?
    ): Method? {
        return lookupVisitMethod(
            visitorClass,
            visiteeClass,
            visitMethodName,
            Collections.emptyList()
        )
    }

    /**
     * Looks up a visit method taking additional parameters beyond the
     * overloaded visitee type.
     *
     * @param visitorClass             class of object whose visit method is to be
     * invoked
     * @param visiteeClass             class of object to be passed as a parameter
     * to the visit method
     * @param visitMethodName          name of visit method
     * @param additionalParameterTypes list of additional parameter types
     * @return method found, or null if none found
     * @see .createDispatcher
     */
    @Nullable
    fun lookupVisitMethod(
        visitorClass: Class<*>,
        visiteeClass: Class<*>,
        visitMethodName: String?,
        additionalParameterTypes: List<Class?>
    ): Method? {
        // Prepare an array to re-use in recursive calls.  The first argument
        // will have the visitee class substituted into it.
        val paramTypes: Array<Class<*>?> = arrayOfNulls<Class>(1 + additionalParameterTypes.size())
        var iParam = 1
        for (paramType in additionalParameterTypes) {
            paramTypes[iParam++] = paramType
        }

        // Cache Class to candidate Methods, to optimize the case where
        // the original visiteeClass has a diamond-shaped interface inheritance
        // graph. (This is common, for example, in JMI.) The idea is to avoid
        // iterating over a single interface's method more than once in a call.
        val cache: Map<Class<*>, Method> = HashMap()
        return lookupVisitMethod(
            visitorClass,
            visiteeClass,
            visitMethodName,
            paramTypes,
            cache
        )
    }

    @Nullable
    private fun lookupVisitMethod(
        visitorClass: Class<*>,
        visiteeClass: Class<*>,
        visitMethodName: String?,
        paramTypes: Array<Class<*>?>,
        cache: Map<Class<*>, Method>
    ): Method? {
        // Use containsKey since the result for a Class might be null.
        if (cache.containsKey(visiteeClass)) {
            return cache[visiteeClass]
        }
        var candidateMethod: Method? = null
        paramTypes[0] = visiteeClass
        try {
            candidateMethod = visitorClass.getMethod(
                visitMethodName,
                paramTypes
            )
            cache.put(visiteeClass, candidateMethod)
            return candidateMethod
        } catch (ex: NoSuchMethodException) {
            // not found:  carry on with lookup
        }
        val superClass: Class<*> = visiteeClass.getSuperclass()
        if (superClass != null) {
            candidateMethod = lookupVisitMethod(
                visitorClass,
                superClass,
                visitMethodName,
                paramTypes,
                cache
            )
        }
        val interfaces: Array<Class<*>> = visiteeClass.getInterfaces()
        for (anInterface in interfaces) {
            val method: Method? = lookupVisitMethod(
                visitorClass, anInterface,
                visitMethodName, paramTypes, cache
            )
            if (method != null) {
                if (candidateMethod != null) {
                    if (!method.equals(candidateMethod)) {
                        val c1: Class<*> = method.getParameterTypes().get(0)
                        val c2: Class<*> = candidateMethod.getParameterTypes().get(0)
                        if (c1.isAssignableFrom(c2)) {
                            // c2 inherits from c1, so keep candidateMethod
                            // (which is more specific than method)
                            continue
                        } else if (c2.isAssignableFrom(c1)) {
                            // c1 inherits from c2 (method is more specific
                            // than candidate method), so fall through
                            // to set candidateMethod = method
                        } else {
                            // c1 and c2 are not directly related
                            throw IllegalArgumentException(
                                "dispatch ambiguity between "
                                        + candidateMethod + " and " + method
                            )
                        }
                    }
                }
                candidateMethod = method
            }
        }
        cache.put(visiteeClass, candidateMethod)
        return candidateMethod
    }

    /**
     * Creates a dispatcher for calls to [.lookupVisitMethod]. The
     * dispatcher caches methods between invocations.
     *
     * @param visitorBaseClazz Visitor base class
     * @param visiteeBaseClazz Visitee base class
     * @return cache of methods
     */
    fun <R : ReflectiveVisitor?, E : Object?> createDispatcher(
        visitorBaseClazz: Class<R>?,
        visiteeBaseClazz: Class<E>?
    ): ReflectiveVisitDispatcher<R, E> {
        assert(ReflectiveVisitor::class.java.isAssignableFrom(visitorBaseClazz))
        assert(Object::class.java.isAssignableFrom(visiteeBaseClazz))
        return object : ReflectiveVisitDispatcher<R, E>() {
            val map: Map<List<Object>, Method> = HashMap()

            @Override
            @Nullable
            override fun lookupVisitMethod(
                visitorClass: Class<out R>,
                visiteeClass: Class<out E>,
                visitMethodName: String?
            ): Method? {
                return lookupVisitMethod(
                    visitorClass,
                    visiteeClass,
                    visitMethodName,
                    Collections.emptyList()
                )
            }

            @Override
            @Nullable
            fun lookupVisitMethod(
                visitorClass: Class<out R>,
                visiteeClass: Class<out E>,
                visitMethodName: String?,
                additionalParameterTypes: List<Class?>
            ): Method? {
                val key: List<Object> = ImmutableList.of(
                    visitorClass,
                    visiteeClass,
                    visitMethodName,
                    additionalParameterTypes
                )
                var method: Method? = map[key]
                if (method == null) {
                    if (map.containsKey(key)) {
                        // We already looked for the method and found nothing.
                    } else {
                        method = ReflectUtil.lookupVisitMethod(
                            visitorClass,
                            visiteeClass,
                            visitMethodName,
                            additionalParameterTypes
                        )
                        map.put(key, method)
                    }
                }
                return method
            }

            @Override
            fun invokeVisitor(
                visitor: R,
                visitee: E,
                visitMethodName: String
            ): Boolean {
                return invokeVisitor(
                    visitor,
                    visitee,
                    visiteeBaseClazz,
                    visitMethodName
                )
            }
        }
    }

    /**
     * Creates a dispatcher for calls to a single multi-method on a particular
     * object.
     *
     *
     * Calls to that multi-method are resolved by looking for a method on
     * the runtime type of that object, with the required name, and with
     * the correct type or a subclass for the first argument, and precisely the
     * same types for other arguments.
     *
     *
     * For instance, a dispatcher created for the method
     *
     * <blockquote>String foo(Vehicle, int, List)</blockquote>
     *
     *
     * could be used to call the methods
     *
     * <blockquote>String foo(Car, int, List)<br></br>
     * String foo(Bus, int, List)</blockquote>
     *
     *
     * (because Car and Bus are subclasses of Vehicle, and they occur in the
     * polymorphic first argument) but not the method
     *
     * <blockquote>String foo(Car, int, ArrayList)</blockquote>
     *
     *
     * (only the first argument is polymorphic).
     *
     *
     * You must create an implementation of the method for the base class.
     * Otherwise throws [IllegalArgumentException].
     *
     * @param returnClazz     Return type of method
     * @param visitor         Object on which to invoke the method
     * @param methodName      Name of method
     * @param arg0Clazz       Base type of argument zero
     * @param otherArgClasses Types of remaining arguments
     */
    fun <E : Object?, T> createMethodDispatcher(
        returnClazz: Class<T>,
        visitor: ReflectiveVisitor,
        methodName: String,
        arg0Clazz: Class<E>,
        vararg otherArgClasses: Class?
    ): MethodDispatcher<T> {
        val otherArgClassList: List<Class> = ImmutableList.copyOf(otherArgClasses)
        @SuppressWarnings(["unchecked"]) val dispatcher: ReflectiveVisitDispatcher<ReflectiveVisitor, E> =
            createDispatcher<ReflectiveVisitor, Object>(
                visitor.getClass() as Class<ReflectiveVisitor>, arg0Clazz
            )
        return object : MethodDispatcher<T> {
            @Override
            override operator fun invoke(@Nullable vararg args: Object?): T {
                val method: Method = lookupMethod(castNonNull(args[0]))
                return try {
                    // castNonNull is here because method.invoke can return null, and we don't know if
                    // T is nullable
                    val o: Object = castNonNull(method.invoke(visitor, args))
                    returnClazz.cast(o)
                } catch (e: IllegalAccessException) {
                    throw RuntimeException(
                        "While invoking method '$method'",
                        e
                    )
                } catch (e: InvocationTargetException) {
                    val target: Throwable = e.getTargetException()
                    if (target is RuntimeException) {
                        throw target as RuntimeException
                    }
                    if (target is Error) {
                        throw target as Error
                    }
                    throw RuntimeException(
                        "While invoking method '$method'",
                        e
                    )
                }
            }

            private fun lookupMethod(arg0: Object): Method {
                if (!arg0Clazz.isInstance(arg0)) {
                    throw IllegalArgumentException()
                }
                val method: Method = dispatcher.lookupVisitMethod(
                    visitor.getClass(),
                    arg0.getClass() as Class<out E>,
                    methodName,
                    otherArgClassList
                )
                if (method == null) {
                    val classList: List<Class> = ArrayList()
                    classList.add(arg0Clazz)
                    classList.addAll(otherArgClassList)
                    throw IllegalArgumentException(
                        "Method not found: " + methodName
                                + "(" + classList + ")"
                    )
                }
                return method
            }
        }
    }

    /** Derives the name of the `i`th parameter of a method.  */
    fun getParameterName(method: Method, i: Int): String {
        for (annotation in method.getParameterAnnotations().get(i)) {
            if (annotation.annotationType() === Parameter::class.java) {
                return (annotation as Parameter).name()
            }
        }
        return method.getParameters().get(i).getName()
    }

    /** Derives whether the `i`th parameter of a method is optional.  */
    fun isParameterOptional(method: Method, i: Int): Boolean {
        for (annotation in method.getParameterAnnotations().get(i)) {
            if (annotation.annotationType() === Parameter::class.java) {
                return (annotation as Parameter).optional()
            }
        }
        return false
    }

    /** Returns whether a parameter of a given type could possibly have an
     * argument of a given type.
     *
     *
     * For example, consider method
     *
     * <blockquote>
     * `foo(Object o, String s, int i, Number n, BigDecimal d`
    </blockquote> *
     *
     *
     * To which which of those parameters could I pass a value that is an
     * instance of [java.util.HashMap]? The answer:
     *
     *
     *  * `o` yes,
     *  * `s` no (`String` is a final class),
     *  * `i` no,
     *  * `n` yes (`Number` is an interface, and `HashMap` is
     * a non-final class, so I could create a sub-class of `HashMap`
     * that implements `Number`,
     *  * `d` yes (`BigDecimal` is a non-final class).
     *
     */
    fun mightBeAssignableFrom(
        parameterType: Class<*>,
        argumentType: Class<*>
    ): Boolean {
        // TODO: think about arrays (e.g. int[] and String[])
        if (parameterType === argumentType) {
            return true
        }
        if (Primitive.`is`(argumentType)) {
            return false
        }
        return if (!parameterType.isInterface()
            && Modifier.isFinal(parameterType.getModifiers())
        ) {
            // parameter is a final class
            // e.g. parameter String, argument Serializable
            // e.g. parameter String, argument Map
            // e.g. parameter String, argument Object
            // e.g. parameter String, argument HashMap
            argumentType.isAssignableFrom(parameterType)
        } else {
            // parameter is an interface or non-final class
            if (!argumentType.isInterface()
                && Modifier.isFinal(argumentType.getModifiers())
            ) {
                // argument is a final class
                // e.g. parameter Object, argument String
                // e.g. parameter Serializable, argument String
                parameterType.isAssignableFrom(argumentType)
            } else {
                // argument is an interface or non-final class
                // e.g. parameter Map, argument Number
                true
            }
        }
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Can invoke a method on an object of type E with return type T.
     *
     * @param <T> Return type of method
    </T> */
    interface MethodDispatcher<T> {
        /**
         * Invokes method on an object with a given set of arguments.
         *
         * @param args Arguments to method
         * @return Return value of method
         */
        operator fun invoke(@Nullable vararg args: Object?): T
    }
}
