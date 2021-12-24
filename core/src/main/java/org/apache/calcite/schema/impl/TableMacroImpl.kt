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
package org.apache.calcite.schema.impl

import org.apache.calcite.schema.TableMacro

/**
 * Implementation of [org.apache.calcite.schema.TableMacro] based on a
 * method.
 */
class TableMacroImpl
/** Private constructor; use [.create].  */
private constructor(method: Method) : ReflectiveFunctionBase(method), TableMacro {
    /**
     * Applies arguments to yield a table.
     *
     * @param arguments Arguments
     * @return Table
     */
    @Override
    fun apply(arguments: List<Object?>): TranslatableTable {
        return try {
            var o: Object? = null
            if (!Modifier.isStatic(method.getModifiers())) {
                val constructor: Constructor<*> = method.getDeclaringClass().getConstructor()
                o = constructor.newInstance()
            }
            requireNonNull(
                method.invoke(o, arguments.toArray())
            ) { "got null from " + method.toString() + " with arguments " + arguments } as TranslatableTable
        } catch (e: IllegalArgumentException) {
            throw RuntimeException(
                ("Expected "
                        + Arrays.toString(method.getParameterTypes())) + " actual "
                        + arguments,
                e
            )
        } catch (e: IllegalAccessException) {
            throw RuntimeException(e)
        } catch (e: InvocationTargetException) {
            throw RuntimeException(e)
        } catch (e: NoSuchMethodException) {
            throw RuntimeException(e)
        } catch (e: InstantiationException) {
            throw RuntimeException(e)
        }
    }

    companion object {
        /** Creates a `TableMacro` from a class, looking for an "eval"
         * method. Returns null if there is no such method.  */
        @Nullable
        fun create(clazz: Class<*>): TableMacro? {
            val method: Method = findMethod(clazz, "eval") ?: return null
            return create(method)
        }

        /** Creates a `TableMacro` from a method.  */
        @Nullable
        fun create(method: Method): TableMacro? {
            val clazz: Class = method.getDeclaringClass()
            if (!Modifier.isStatic(method.getModifiers())) {
                if (!classHasPublicZeroArgsConstructor(clazz)) {
                    throw RESOURCE.requireDefaultConstructor(clazz.getName()).ex()
                }
            }
            val returnType: Class<*> = method.getReturnType()
            return if (!TranslatableTable::class.java.isAssignableFrom(returnType)) {
                null
            } else TableMacroImpl(method)
        }
    }
}
