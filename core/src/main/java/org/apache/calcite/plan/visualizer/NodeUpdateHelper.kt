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
package org.apache.calcite.plan.visualizer

import org.apache.calcite.rel.RelNode
import java.util.Collections
import java.util.LinkedHashMap
import java.util.List
import java.util.Map
import java.util.Objects

/**
 * Helper class to create the node update.
 */
internal class NodeUpdateHelper(val key: String, @Nullable rel: RelNode?) {

    @Nullable
    private val rel: RelNode?
    private val state: NodeUpdateInfo

    @Nullable
    private var update: NodeUpdateInfo? = null

    init {
        this.rel = rel
        state = NodeUpdateInfo()
    }

    @Nullable
    fun getRel(): RelNode? {
        return rel
    }

    fun updateAttribute(attr: String?, newValue: Object) {
        if (Objects.equals(newValue, state.get(attr))) {
            return
        }
        state.put(attr, newValue)
        if (update == null) {
            update = NodeUpdateInfo()
        }
        if (newValue is List
            && (newValue as List<*>).size() === 0 && !update.containsKey(attr)
        ) {
            return
        }
        update.put(attr, newValue)
    }

    val isEmptyUpdate: Boolean
        get() = update == null || update.isEmpty()

    /**
     * Gets an object representing all the changes since the last call to this method.
     *
     * @return an object or null if there are no changes.
     */
    @get:Nullable
    val andResetUpdate: Object?
        get() {
            if (isEmptyUpdate) {
                return null
            }
            val update = update
            this.update = null
            return update
        }

    fun getState(): Map<String, Object> {
        return Collections.unmodifiableMap(state)
    }

    /**
     * Get the current value for the attribute.
     */
    @Nullable
    fun getValue(attr: String?): Object {
        return state.get(attr)
    }

    /**
     * Type alias.
     */
    private class NodeUpdateInfo : LinkedHashMap<String?, Object?>()
}
