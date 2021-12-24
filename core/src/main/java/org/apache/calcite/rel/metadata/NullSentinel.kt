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
package org.apache.calcite.rel.metadata

import org.apache.calcite.rel.metadata.BuiltInMetadata.Size
import kotlin.Throws
import kotlin.jvm.Synchronized
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider.Space
import BuiltInMetadata.Size

/** Placeholder for null values.  */
enum class NullSentinel {
    /** Placeholder for a null value.  */
    INSTANCE {
        @Override
        override fun toString(): String {
            return "NULL"
        }
    },

    /** Placeholder that means that a request for metadata is already active,
     * therefore this request forms a cycle.  */
    ACTIVE;

    companion object {
        fun mask(@Nullable value: Comparable?): Comparable {
            return if (value == null) {
                INSTANCE
            } else value
        }

        fun mask(@Nullable value: Object?): Object {
            return if (value == null) {
                INSTANCE
            } else value
        }
    }
}
