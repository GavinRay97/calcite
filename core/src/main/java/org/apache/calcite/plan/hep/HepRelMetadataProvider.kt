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
package org.apache.calcite.plan.hep

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.Metadata
import org.apache.calcite.rel.metadata.MetadataDef
import org.apache.calcite.rel.metadata.MetadataHandler
import org.apache.calcite.rel.metadata.RelMetadataProvider
import org.apache.calcite.rel.metadata.UnboundMetadata
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMultimap
import com.google.common.collect.Multimap
import java.lang.reflect.Method
import java.util.List
import java.util.Objects.requireNonNull

/**
 * HepRelMetadataProvider implements the [RelMetadataProvider] interface
 * by combining metadata from the rels inside of a [HepRelVertex].
 */
@Deprecated
internal class HepRelMetadataProvider : RelMetadataProvider {
    //~ Methods ----------------------------------------------------------------
    @Override
    override fun equals(@Nullable obj: Object?): Boolean {
        return obj is HepRelMetadataProvider
    }

    @Override
    override fun hashCode(): Int {
        return 107
    }

    @Deprecated // to be removed before 2.0
    @Override
    fun <M : Metadata?> apply(
        relClass: Class<out RelNode?>?,
        metadataClass: Class<out M>
    ): UnboundMetadata<M> {
        return label@ UnboundMetadata<M> { rel, mq ->
            if (rel !is HepRelVertex) {
                return@label null
            }
            val rel2: RelNode = rel.getCurrentRel()
            val function: UnboundMetadata<M> =
                requireNonNull(rel.getCluster().getMetadataProvider(), "metadataProvider")
                    .apply(rel2.getClass(), metadataClass)
            requireNonNull(
                function
            ) { "no metadata provider for class $metadataClass" }
                .bind(rel2, mq)
        }
    }

    @Deprecated // to be removed before 2.0
    @Override
    fun <M : Metadata?> handlers(
        def: MetadataDef<M>?
    ): Multimap<Method, MetadataHandler<M>> {
        return ImmutableMultimap.of()
    }

    @Override
    fun handlers(
        handlerClass: Class<out MetadataHandler<*>?>?
    ): List<MetadataHandler<*>> {
        return ImmutableList.of()
    }
}
