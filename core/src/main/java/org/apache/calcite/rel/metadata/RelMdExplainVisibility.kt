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
 * RelMdExplainVisibility supplies a default implementation of
 * [RelMetadataQuery.isVisibleInExplain] for the standard logical algebra.
 */
class RelMdExplainVisibility  //~ Constructors -----------------------------------------------------------
private constructor() : MetadataHandler<BuiltInMetadata.ExplainVisibility?> {
    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        get() = BuiltInMetadata.ExplainVisibility.DEF

    /** Catch-all implementation for
     * [BuiltInMetadata.ExplainVisibility.isVisibleInExplain],
     * invoked using reflection.
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery.isVisibleInExplain
     */
    @Nullable
    fun isVisibleInExplain(
        rel: RelNode?, mq: RelMetadataQuery?,
        explainLevel: SqlExplainLevel?
    ): Boolean? {
        // no information available
        return null
    }

    companion object {
        val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
            RelMdExplainVisibility(), BuiltInMetadata.ExplainVisibility.Handler::class.java
        )
    }
}
