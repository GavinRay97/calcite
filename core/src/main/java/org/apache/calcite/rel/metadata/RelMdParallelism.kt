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
 * [org.apache.calcite.rel.metadata.BuiltInMetadata.Parallelism]
 * metadata provider for the standard logical algebra.
 *
 * @see org.apache.calcite.rel.metadata.RelMetadataQuery.isPhaseTransition
 *
 * @see org.apache.calcite.rel.metadata.RelMetadataQuery.splitCount
 */
class RelMdParallelism  //~ Constructors -----------------------------------------------------------
protected constructor() : MetadataHandler<BuiltInMetadata.Parallelism?> {
    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        get() = BuiltInMetadata.Parallelism.DEF

    /** Catch-all implementation for
     * [BuiltInMetadata.Parallelism.isPhaseTransition],
     * invoked using reflection.
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery.isPhaseTransition
     */
    fun isPhaseTransition(rel: RelNode?, mq: RelMetadataQuery?): Boolean {
        return false
    }

    fun isPhaseTransition(rel: TableScan?, mq: RelMetadataQuery?): Boolean {
        return true
    }

    fun isPhaseTransition(rel: Values?, mq: RelMetadataQuery?): Boolean {
        return true
    }

    fun isPhaseTransition(rel: Exchange?, mq: RelMetadataQuery?): Boolean {
        return true
    }

    /** Catch-all implementation for
     * [BuiltInMetadata.Parallelism.splitCount],
     * invoked using reflection.
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery.splitCount
     */
    fun splitCount(rel: RelNode?, mq: RelMetadataQuery?): Integer {
        return 1
    }

    companion object {
        /** Source for
         * [org.apache.calcite.rel.metadata.BuiltInMetadata.Parallelism].  */
        val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
            RelMdParallelism(),
            BuiltInMetadata.Parallelism.Handler::class.java
        )
    }
}
