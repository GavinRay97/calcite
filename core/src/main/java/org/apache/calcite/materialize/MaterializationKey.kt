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
package org.apache.calcite.materialize

import java.io.Serializable

/**
 * Unique identifier for a materialization.
 *
 *
 * It is immutable and can only be created by the
 * [MaterializationService]. For communicating with the service.
 */
class MaterializationKey : Serializable {
    private val uuid: UUID = UUID.randomUUID()

    @Override
    override fun hashCode(): Int {
        return uuid.hashCode()
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (this === obj
                || obj is MaterializationKey
                && uuid.equals((obj as MaterializationKey).uuid))
    }

    @Override
    override fun toString(): String {
        return uuid.toString()
    }
}
