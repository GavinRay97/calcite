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

import java.lang.invoke.MethodHandles

/** Compatibility layer.
 *
 *
 * Allows to use advanced functionality if the latest JDK or Guava version
 * is present.
 */
interface Compatible {
    /** Same as `MethodHandles#privateLookupIn()`.
     * (On JDK 8, only [MethodHandles.lookup] is available.  */
    fun <T> lookupPrivate(clazz: Class<T>?): MethodHandles.Lookup?

    /** Creates the implementation of Compatible suitable for the
     * current environment.  */
    class Factory {
        fun create(): Compatible {
            return Proxy.newProxyInstance(
                Compatible::class.java.getClassLoader(), arrayOf<Class<*>>(Compatible::class.java)
            ) { proxy, method, args ->
                if (method.getName().equals("lookupPrivate")) {
                    // Use MethodHandles.privateLookupIn if it is available (JDK 9
                    // and above)
                    @SuppressWarnings("rawtypes") val clazz: Class<*> = requireNonNull(args.get(0), "args[0]") as Class
                    try {
                        val privateLookupMethod: Method = MethodHandles::class.java.getMethod(
                            "privateLookupIn",
                            Class::class.java, MethodHandles.Lookup::class.java
                        )
                        val lookup: MethodHandles.Lookup = MethodHandles.lookup()
                        return@newProxyInstance privateLookupMethod.invoke(null, clazz, lookup)
                    } catch (e: NoSuchMethodException) {
                        return@newProxyInstance privateLookupJdk8<Any>(clazz)
                    }
                }
                null
            }
        }

        companion object {
            /** Emulates MethodHandles.privateLookupIn on JDK 8;
             * in later JDK versions, throws.  */
            @SuppressWarnings("deprecation")
            fun <T> privateLookupJdk8(clazz: Class<T>?): MethodHandles.Lookup {
                return try {
                    val constructor: Constructor<MethodHandles.Lookup> =
                        MethodHandles.Lookup::class.java.getDeclaredConstructor(
                            Class::class.java,
                            Int::class.javaPrimitiveType
                        )
                    if (!constructor.isAccessible()) {
                        constructor.setAccessible(true)
                    }
                    constructor.newInstance(clazz, MethodHandles.Lookup.PRIVATE)
                } catch (e: InstantiationException) {
                    throw RuntimeException(e)
                } catch (e: IllegalAccessException) {
                    throw RuntimeException(e)
                } catch (e: InvocationTargetException) {
                    throw RuntimeException(e)
                } catch (e: NoSuchMethodException) {
                    throw RuntimeException(e)
                }
            }
        }
    }

    companion object {
        val INSTANCE = Factory().create()
    }
}
