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

import org.slf4j.helpers.MessageFormatter

/**
 * Callback to be called when a test for validity succeeds or fails.
 */
interface Litmus {
    /** Called when test fails. Returns false or throws.
     *
     * @param message Message
     * @param args Arguments
     */
    fun fail(@Nullable message: String?, @Nullable vararg args: Object?): Boolean

    /** Called when test succeeds. Returns true.  */
    fun succeed(): Boolean

    /** Checks a condition.
     *
     *
     * If the condition is true, calls [.succeed];
     * if the condition is false, calls [.fail],
     * converting `info` into a string message.
     */
    fun check(condition: Boolean, @Nullable message: String?, @Nullable vararg args: Object?): Boolean

    companion object {
        /** Implementation of [org.apache.calcite.util.Litmus] that throws
         * an [java.lang.AssertionError] on failure.  */
        val THROW: Litmus = object : Litmus {
            @Override
            override fun fail(@Nullable message: String?, @Nullable vararg args: Object?): Boolean {
                val s: String? = if (message == null) null else MessageFormatter.arrayFormat(message, args).getMessage()
                throw AssertionError(s)
            }

            @Override
            override fun succeed(): Boolean {
                return true
            }

            @Override
            override fun check(
                condition: Boolean, @Nullable message: String?,
                @Nullable vararg args: Object?
            ): Boolean {
                return if (condition) {
                    succeed()
                } else {
                    fail(message, *args)
                }
            }
        }

        /** Implementation of [org.apache.calcite.util.Litmus] that returns
         * a status code but does not throw.  */
        val IGNORE: Litmus = object : Litmus {
            @Override
            override fun fail(@Nullable message: String?, @Nullable vararg args: Object?): Boolean {
                return false
            }

            @Override
            override fun succeed(): Boolean {
                return true
            }

            @Override
            override fun check(
                condition: Boolean, @Nullable message: String?,
                @Nullable vararg args: Object?
            ): Boolean {
                return condition
            }
        }
    }
}
