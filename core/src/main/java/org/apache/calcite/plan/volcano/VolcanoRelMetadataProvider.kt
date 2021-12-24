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
package org.apache.calcite.plan.volcano

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
import java.util.Objects

/**
 * VolcanoRelMetadataProvider implements the [RelMetadataProvider]
 * interface by combining metadata from the rels making up an equivalence class.
 */
@Deprecated // to be removed before 2.0
class VolcanoRelMetadataProvider : RelMetadataProvider {
    //~ Methods ----------------------------------------------------------------
    @Override
    override fun equals(@Nullable obj: Object?): Boolean {
        return obj is VolcanoRelMetadataProvider
    }

    @Override
    override fun hashCode(): Int {
        return 103
    }

    @Deprecated // to be removed before 2.0
    @Override
    fun <M : Metadata?> apply(
        relClass: Class<out RelNode?>,
        metadataClass: Class<out M>?
    ): @Nullable UnboundMetadata<M>? {
        return if (relClass !== RelSubset::class.java) {
            // let someone else further down the chain sort it out
            null
        } else label@ UnboundMetadata<M> { rel, mq ->
            val subset: RelSubset = rel as RelSubset
            val provider: RelMetadataProvider = Objects.requireNonNull(
                rel.getCluster().getMetadataProvider(),
                "metadataProvider"
            )

            // REVIEW jvs 29-Mar-2006: I'm not sure what the correct precedence
            // should be here.  Letting the current best plan take the first shot is
            // probably the right thing to do for physical estimates such as row
            // count.  Dunno about others, and whether we need a way to
            // discriminate.

            // First, try current best implementation.  If it knows how to answer
            // this query, treat it as the most reliable.
            if (subset.best != null) {
                val best: RelNode = subset.best
                val function: UnboundMetadata<M> = provider.apply(best.getClass(), metadataClass)
                if (function != null) {
                    val metadata: M = function.bind(best, mq)
                    if (metadata != null) {
                        return@label metadata
                    }
                }
            }

            // Otherwise, try rels in same logical equivalence class to see if any
            // of them have a good answer.  We use the full logical equivalence
            // class rather than just the subset because many metadata providers
            // only know about logical metadata.

            // Equivalence classes can get tangled up in interesting ways, so avoid
            // an infinite loop.  REVIEW: There's a chance this will cause us to
            // fail on metadata queries which invoke other queries, e.g.
            // PercentageOriginalRows -> Selectivity.  If we implement caching at
            // this level, we could probably kill two birds with one stone (use
            // presence of pending cache entry to detect re-entrancy at the correct
            // granularity).
            if (subset.set.inMetadataQuery) {
                return@label null
            }
            subset.set.inMetadataQuery = true
            try {
                for (relCandidate in subset.set.rels) {
                    val function: UnboundMetadata<M> = provider.apply(relCandidate.getClass(), metadataClass)
                    if (function != null) {
                        val result: M = function.bind(relCandidate, mq)
                        if (result != null) {
                            return@label result
                        }
                    }
                }
            } finally {
                subset.set.inMetadataQuery = false
            }
            null
        }
    }

    @Deprecated
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
