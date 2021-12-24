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

import java.util.ArrayList
import java.util.List

/**
 * MulticastRelOptListener implements the [RelOptListener] interface by
 * forwarding events on to a collection of other listeners.
 */
class MulticastRelOptListener : RelOptListener {
    //~ Instance fields --------------------------------------------------------
    private val listeners: List<RelOptListener>
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a new empty multicast listener.
     */
    init {
        listeners = ArrayList()
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Adds a listener which will receive multicast events.
     *
     * @param listener listener to add
     */
    fun addListener(listener: RelOptListener?) {
        listeners.add(listener)
    }

    // implement RelOptListener
    @Override
    fun relEquivalenceFound(event: RelEquivalenceEvent?) {
        for (listener in listeners) {
            listener.relEquivalenceFound(event)
        }
    }

    // implement RelOptListener
    @Override
    fun ruleAttempted(event: RuleAttemptedEvent?) {
        for (listener in listeners) {
            listener.ruleAttempted(event)
        }
    }

    // implement RelOptListener
    @Override
    fun ruleProductionSucceeded(event: RuleProductionEvent?) {
        for (listener in listeners) {
            listener.ruleProductionSucceeded(event)
        }
    }

    // implement RelOptListener
    @Override
    fun relChosen(event: RelChosenEvent?) {
        for (listener in listeners) {
            listener.relChosen(event)
        }
    }

    // implement RelOptListener
    @Override
    fun relDiscarded(event: RelDiscardedEvent?) {
        for (listener in listeners) {
            listener.relDiscarded(event)
        }
    }
}
