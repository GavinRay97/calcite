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
package org.apache.calcite.plan

import org.apache.calcite.config.CalciteConnectionConfig

/**
 * Utilities for [Context].
 */
object Contexts {
    val EMPTY_CONTEXT = EmptyContext()

    /** Returns a context that contains a
     * [org.apache.calcite.config.CalciteConnectionConfig].
     *
     */
    @Deprecated // to be removed before 2.0
    @Deprecated("Use {@link #of}")
    fun withConfig(config: CalciteConnectionConfig?): Context {
        return of(config)
    }

    /** Returns a context that returns null for all inquiries.  */
    fun empty(): Context {
        return EMPTY_CONTEXT
    }

    /** Returns a context that wraps an object.
     *
     *
     * A call to `unwrap(C)` will return `target` if it is an
     * instance of `C`.
     */
    fun of(o: Object?): Context {
        return WrapContext(o)
    }

    /** Returns a context that wraps an array of objects, ignoring any nulls.  */
    fun of(@Nullable vararg os: Object?): Context {
        val contexts: List<Context> = ArrayList()
        for (o in os) {
            if (o != null) {
                contexts.add(of(o))
            }
        }
        return chain(contexts)
    }

    /** Returns a context that wraps a list of contexts.
     *
     *
     * A call to `unwrap(C)` will return the first object that is an
     * instance of `C`.
     *
     *
     * If any of the contexts is a [Context], recursively looks in that
     * object. Thus this method can be used to chain contexts.
     */
    fun chain(vararg contexts: Context?): Context {
        return chain(ImmutableList.copyOf(contexts))
    }

    private fun chain(contexts: Iterable<Context?>): Context {
        // Flatten any chain contexts in the list, and remove duplicates
        val list: List<Context> = ArrayList()
        for (context in contexts) {
            build(list, context)
        }
        return when (list.size()) {
            0 -> empty()
            1 -> list[0]
            else -> ChainContext(ImmutableList.copyOf(list))
        }
    }

    /** Recursively populates a list of contexts.  */
    private fun build(list: List<Context?>, context: Context?) {
        if (context === EMPTY_CONTEXT || list.contains(context)) {
            return
        }
        if (context is ChainContext) {
            for (child in context.contexts) {
                build(list, child)
            }
        } else {
            list.add(context)
        }
    }

    /** Context that wraps an object.  */
    private class WrapContext internal constructor(target: Object?) : Context {
        val target: Object

        init {
            this.target = Objects.requireNonNull(target, "target")
        }

        @Override
        fun <T : Object?> unwrap(clazz: Class<T>): @Nullable T? {
            return if (clazz.isInstance(target)) {
                clazz.cast(target)
            } else null
        }
    }

    /** Empty context.  */
    class EmptyContext : Context {
        @Override
        fun <T : Object?> unwrap(clazz: Class<T>?): @Nullable T? {
            return null
        }
    }

    /** Context that wraps a chain of contexts.  */
    private class ChainContext internal constructor(contexts: ImmutableList<Context?>) : Context {
        val contexts: ImmutableList<Context>

        init {
            this.contexts = Objects.requireNonNull(contexts, "contexts")
            for (context in contexts) {
                assert(context !is ChainContext) { "must be flat" }
            }
        }

        @Override
        fun <T : Object?> unwrap(clazz: Class<T>?): @Nullable T? {
            for (context in contexts) {
                val t: T = context.unwrap(clazz)
                if (t != null) {
                    return t
                }
            }
            return null
        }
    }
}
