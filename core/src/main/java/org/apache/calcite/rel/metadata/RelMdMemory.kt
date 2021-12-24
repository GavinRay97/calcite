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

import org.apache.calcite.rel.RelNode

/**
 * Default implementations of the
 * [org.apache.calcite.rel.metadata.BuiltInMetadata.Memory]
 * metadata provider for the standard logical algebra.
 *
 * @see RelMetadataQuery.isPhaseTransition
 *
 * @see RelMetadataQuery.splitCount
 */
class RelMdMemory  //~ Constructors -----------------------------------------------------------
protected constructor() : MetadataHandler<BuiltInMetadata.Memory?> {
    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        get() = BuiltInMetadata.Memory.DEF

    /** Catch-all implementation for
     * [BuiltInMetadata.Memory.memory],
     * invoked using reflection.
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery.memory
     */
    @Nullable
    fun memory(rel: RelNode?, mq: RelMetadataQuery?): Double? {
        return null
    }

    /** Catch-all implementation for
     * [BuiltInMetadata.Memory.cumulativeMemoryWithinPhase],
     * invoked using reflection.
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery.memory
     */
    @Nullable
    fun cumulativeMemoryWithinPhase(rel: RelNode, mq: RelMetadataQuery): Double? {
        var nullable: Double = mq.memory(rel) ?: return null
        val isPhaseTransition: Boolean = mq.isPhaseTransition(rel) ?: return null
        var d = nullable
        if (!isPhaseTransition) {
            for (input in rel.getInputs()) {
                nullable = mq.cumulativeMemoryWithinPhase(input)
                if (nullable == null) {
                    return null
                }
                d += nullable
            }
        }
        return d
    }

    /** Catch-all implementation for
     * [BuiltInMetadata.Memory.cumulativeMemoryWithinPhaseSplit],
     * invoked using reflection.
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery.cumulativeMemoryWithinPhaseSplit
     */
    @Nullable
    fun cumulativeMemoryWithinPhaseSplit(
        rel: RelNode?,
        mq: RelMetadataQuery
    ): Double? {
        val memoryWithinPhase: Double = mq.cumulativeMemoryWithinPhase(rel)
        val splitCount: Integer = mq.splitCount(rel)
        return if (memoryWithinPhase == null || splitCount == null) {
            null
        } else memoryWithinPhase / splitCount
    }

    companion object {
        /** Source for
         * [org.apache.calcite.rel.metadata.BuiltInMetadata.Memory].  */
        val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
            RelMdMemory(),
            BuiltInMetadata.Memory.Handler::class.java
        )
    }
}
