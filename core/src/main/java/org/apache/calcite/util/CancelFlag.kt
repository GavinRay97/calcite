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

import org.apache.calcite.plan.Context

/**
 * CancelFlag is used to post and check cancellation requests.
 *
 *
 * Pass it to [RelOptPlanner] by putting it into a [Context].
 */
class CancelFlag(atomicBoolean: AtomicBoolean?) {
    //~ Instance fields --------------------------------------------------------
    /** The flag that holds the cancel state.
     * Feel free to use the flag directly.  */
    val atomicBoolean: AtomicBoolean

    init {
        this.atomicBoolean = Objects.requireNonNull(atomicBoolean, "atomicBoolean")
    }
    //~ Methods ----------------------------------------------------------------
    /** Returns whether a cancellation has been requested.  */
    val isCancelRequested: Boolean
        get() = atomicBoolean.get()

    /**
     * Requests a cancellation.
     */
    fun requestCancel() {
        atomicBoolean.compareAndSet(false, true)
    }

    /**
     * Clears any pending cancellation request.
     */
    fun clearCancel() {
        atomicBoolean.compareAndSet(true, false)
    }
}
