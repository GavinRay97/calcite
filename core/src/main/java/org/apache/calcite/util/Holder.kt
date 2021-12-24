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

import java.util.function.UnaryOperator

/**
 * A mutable slot that can contain one object.
 *
 *
 * A holder is useful for implementing OUT or IN-OUT parameters.
 *
 *
 * It is possible to sub-class to receive events on get or set.
 *
 * @param <E> Element type
</E> */
class Holder<E>
/** Creates a Holder containing a given value.
 *
 *
 * Call this method from a derived constructor or via the [.of]
 * method.  */ protected constructor(private var e: E) {
    /** Sets the value.  */
    fun set(e: E) {
        this.e = e
    }

    /** Gets the value.  */
    fun get(): E {
        return e
    }

    /** Applies a transform to the value.  */
    fun accept(transform: UnaryOperator<E>): Holder<E> {
        e = transform.apply(e)
        return this
    }

    companion object {
        /** Creates a holder containing a given value.  */
        fun <E> of(e: E): Holder<E> {
            return Holder(e)
        }

        /** Creates a holder containing null.  */
        @SuppressWarnings("ConstantConditions")
        fun <E> empty(): Holder<E?> {
            return Holder(null)
        }
    }
}
