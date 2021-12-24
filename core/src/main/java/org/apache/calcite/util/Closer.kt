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

import java.io.IOException

/** Helper that holds onto [AutoCloseable] resources and releases them
 * when its `#close` method is called.
 *
 *
 * Similar to `com.google.common.io.Closer` but can deal with
 * [AutoCloseable], and doesn't throw [IOException].  */
class Closer : AutoCloseable {
    private val list: List<AutoCloseable> = ArrayList()

    /** Registers a resource.  */
    fun <E : AutoCloseable?> add(e: E): E {
        list.add(e)
        return e
    }

    @Override
    fun close() {
        for (closeable in list) {
            try {
                closeable.close()
            } catch (e: RuntimeException) {
                throw e
            } catch (e: Exception) {
                throw RuntimeException(e)
            }
        }
    }
}
