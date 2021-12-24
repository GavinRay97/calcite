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
 * A class derived from `DelegatingInvocationHandler` handles a
 * method call by looking for a method in itself with identical parameters. If
 * no such method is found, it forwards the call to a fallback object, which
 * must implement all of the interfaces which this proxy implements.
 *
 *
 * It is useful in creating a wrapper class around an interface which may
 * change over time.
 *
 *
 * Example:
 *
 * <blockquote>
 * <pre>import java.sql.Connection;
 * Connection connection = ...;
 * Connection tracingConnection = (Connection) Proxy.newProxyInstance(
 * null,
 * new Class[] {Connection.class},
 * new DelegatingInvocationHandler() {
 * protected Object getTarget() {
 * return connection;
 * }
 * Statement createStatement() {
 * System.out.println("statement created");
 * return connection.createStatement();
 * }
 * });</pre>
</blockquote> *
 */
abstract class DelegatingInvocationHandler : InvocationHandler {
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
        val matchingMethod: Method?
        matchingMethod = try {
            clazz.getMethod(
                method.getName(),
                method.getParameterTypes()
            )
        } catch (e: NoSuchMethodException) {
            null
        } catch (e: SecurityException) {
            null
        }
        return try {
            if (matchingMethod != null) {
                // Invoke the method in the derived class.
                matchingMethod.invoke(this, args)
            } else {
                // Invoke the method on the proxy.
                method.invoke(
                    target,
                    args
                )
            }
        } catch (e: InvocationTargetException) {
            throw Util.first(e.getCause(), e)
        }
    }

    /**
     * Returns the object to forward method calls to, should the derived class
     * not implement the method. Generally, this object will be a member of the
     * derived class, supplied as a parameter to its constructor.
     */
    protected abstract val target: Object?
}
