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

import java.lang.reflect.Method

/**
 * Interface for looking up methods relating to reflective visitation. One
 * possible implementation would cache the results.
 *
 *
 * Type parameter 'R' is the base class of visitoR class; type parameter 'E'
 * is the base class of visiteE class.
 *
 *
 * TODO: obsolete [ReflectUtil.lookupVisitMethod], and use caching in
 * implementing that method.
 *
 * @param <E> Argument type
 * @param <R> Return type
</R></E> */
interface ReflectiveVisitDispatcher<R : ReflectiveVisitor?, E : Object?> {
    //~ Methods ----------------------------------------------------------------
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
     */
    @Nullable
    fun lookupVisitMethod(
        visitorClass: Class<out R>?,
        visiteeClass: Class<out E>?,
        visitMethodName: String?,
        additionalParameterTypes: List<Class?>?
    ): Method?

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
        visitorClass: Class<out R>?,
        visiteeClass: Class<out E>?,
        visitMethodName: String?
    ): Method?

    /**
     * Implements the [org.apache.calcite.util.Glossary.VISITOR_PATTERN] via
     * reflection. The basic technique is taken from [a
 * Javaworld article](http://www.javaworld.com/javaworld/javatips/jw-javatip98.html). For an example of how to use it, see
     * `ReflectVisitorTest`.
     *
     *
     * Visit method lookup follows the same rules as if compile-time resolution
     * for VisitorClass.visit(VisiteeClass) were performed. An ambiguous match due
     * to multiple interface inheritance results in an IllegalArgumentException. A
     * non-match is indicated by returning false.
     *
     * @param visitor         object whose visit method is to be invoked
     * @param visitee         object to be passed as a parameter to the visit
     * method
     * @param visitMethodName name of visit method, e.g. "visit"
     * @return true if a matching visit method was found and invoked
     */
    fun invokeVisitor(
        visitor: R,
        visitee: E,
        visitMethodName: String?
    ): Boolean
}
