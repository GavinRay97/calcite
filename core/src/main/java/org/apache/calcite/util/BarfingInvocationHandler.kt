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

import java.lang.reflect.InvocationHandler

/**
 * A class derived from `BarfingInvocationHandler` handles a method
 * call by looking for a method in itself with identical parameters. If no such
 * method is found, it throws [UnsupportedOperationException].
 *
 *
 * It is useful when you are prototyping code. You can rapidly create a
 * prototype class which implements the important methods in an interface, then
 * implement other methods as they are called.
 *
 * @see DelegatingInvocationHandler
 */
class BarfingInvocationHandler  //~ Constructors -----------------------------------------------------------
protected constructor() : InvocationHandler {
    //~ Methods ----------------------------------------------------------------
    @Override
    @Nullable
    @Throws(Throwable::class)
    operator fun invoke(
        proxy: Object?,
        method: Method,
        @Nullable args: Array<Object?>?
    ): Object {
        val clazz: Class = getClass()
        val matchingMethod: Method
        matchingMethod = try {
            clazz.getMethod(
                method.getName(),
                method.getParameterTypes()
            )
        } catch (e: NoSuchMethodException) {
            throw noMethod(method)
        } catch (e: SecurityException) {
            throw noMethod(method)
        }
        if (matchingMethod.getReturnType() !== method.getReturnType()) {
            throw noMethod(method)
        }

        // Invoke the method in the derived class.
        return try {
            matchingMethod.invoke(this, args)
        } catch (e: UndeclaredThrowableException) {
            throw e.getCause()
        }
    }

    /**
     * Called when this class (or its derived class) does not have the required
     * method from the interface.
     */
    protected fun noMethod(method: Method): UnsupportedOperationException {
        val buf = StringBuilder()
        val parameterTypes: Array<Class> = method.getParameterTypes()
        for (i in parameterTypes.indices) {
            if (i > 0) {
                buf.append(",")
            }
            buf.append(parameterTypes[i].getName())
        }
        val signature: String = (method.getReturnType().getName() + " "
                + method.getDeclaringClass().getName() + "." + method.getName()
                + "(" + buf.toString() + ")")
        return UnsupportedOperationException(signature)
    }
}
