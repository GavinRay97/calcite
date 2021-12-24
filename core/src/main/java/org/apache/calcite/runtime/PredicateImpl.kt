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
package org.apache.calcite.runtime

import com.google.common.base.Predicate

/**
 * Abstract implementation of [com.google.common.base.Predicate].
 *
 *
 * Derived class needs to implement the [.test] method.
 *
 *
 * Helps with the transition to `java.util.function.Predicate`,
 * which was introduced in JDK 1.8, and is required in Guava 21.0 and higher,
 * but still works on JDK 1.7.
 *
 * @param <T> the type of the input to the predicate
 *
</T> */
@Deprecated(
    """Now Calcite is Java 8 and higher, we recommend that you
  implement {@link java.util.function.Predicate} directly."""
)
abstract class PredicateImpl<T> : Predicate<T> {
    @Override
    fun apply(@Nullable input: T): Boolean {
        return test(input)
    }

    /** Overrides `java.util.function.Predicate#test` in JDK8 and higher.  */ // Suppress ErrorProne's MissingOverride warning. The @Override annotation
    // would be incorrect on Guava < 21 because Guava's interface Predicate does
    // not implement Java's interface Predicate until Guava 21, and we need the
    // code to compile on all versions.
    @SuppressWarnings("MissingOverride")
    abstract fun test(@Nullable t: T): Boolean
}
