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
package org.apache.calcite.schema

import org.apiguardian.api.API

/**
 * Mix-in interface that allows you to find sub-objects.
 */
interface Wrapper {
    /** Finds an instance of an interface implemented by this object,
     * or returns null if this object does not support that interface.  */
    fun <C : Object?> unwrap(aClass: Class<C>?): @Nullable C?

    /** Finds an instance of an interface implemented by this object,
     * or throws NullPointerException if this object does not support
     * that interface.  */
    @API(since = "1.27", status = API.Status.INTERNAL)
    fun <C : Object?> unwrapOrThrow(aClass: Class<C>): C {
        return requireNonNull(
            unwrap(aClass)
        ) { "Can't unwrap $aClass from $this" }
    }

    /** Finds an instance of an interface implemented by this object,
     * or returns [Optional.empty] if this object does not support
     * that interface.  */
    @API(since = "1.27", status = API.Status.INTERNAL)
    fun <C : Object?> maybeUnwrap(aClass: Class<C>?): Optional<C>? {
        return Optional.ofNullable(unwrap(aClass))
    }
}
