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

import kotlin.Throws

/**
 * Thread-local variable that returns a handle that can be closed.
 *
 * @param <T> Value type
</T> */
class TryThreadLocal<T> private constructor(private val initialValue: T) : ThreadLocal<T>() {
    // It is important that this method is final.
    // This ensures that the sub-class does not choose a different initial
    // value. Then the close logic can detect whether the previous value was
    // equal to the initial value.
    @Override
    protected fun initialValue(): T {
        return initialValue
    }

    /** Assigns the value as `value` for the current thread.
     * Returns a [Memo] which, when closed, will assign the value
     * back to the previous value.  */
    fun push(value: T): Memo {
        val previous: T = get()
        set(value)
        return Memo {
            if (previous === initialValue) {
                remove()
            } else {
                set(previous)
            }
        }
    }

    /** Remembers to set the value back.  */
    interface Memo : AutoCloseable {
        /** Sets the value back; never throws.  */
        @Override
        fun close()
    }

    companion object {
        /** Creates a TryThreadLocal.
         *
         * @param initialValue Initial value
         */
        fun <T> of(initialValue: T): TryThreadLocal<T> {
            return TryThreadLocal(initialValue)
        }
    }
}
