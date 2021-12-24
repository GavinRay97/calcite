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
package org.apache.calcite.sql2rel

import org.apache.calcite.rel.RelHomogeneousShuttle
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.CorrelationId
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexCorrelVariable
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexShuttle
import org.apache.calcite.rex.RexSubQuery
import com.google.common.collect.ImmutableSet
import org.checkerframework.checker.initialization.qual.NotOnlyInitialized
import org.checkerframework.checker.initialization.qual.UnderInitialization

/**
 * Rewrites relations to ensure the same correlation is referenced by the same
 * correlation variable.
 */
class DeduplicateCorrelateVariables private constructor(
    builder: RexBuilder,
    canonicalId: CorrelationId, alternateIds: ImmutableSet<CorrelationId>
) : RelHomogeneousShuttle() {
    @NotOnlyInitialized
    private val dedupRex: RexShuttle

    /** Creates a DeduplicateCorrelateVariables.  */
    init {
        dedupRex = DeduplicateCorrelateVariablesShuttle(
            builder,
            canonicalId, alternateIds, this
        )
    }

    @Override
    fun visit(other: RelNode?): RelNode {
        val next: RelNode = super.visit(other)
        return next.accept(dedupRex)
    }

    /**
     * Replaces alternative names of correlation variable to its canonical name.
     */
    private class DeduplicateCorrelateVariablesShuttle(
        builder: RexBuilder,
        canonicalId: CorrelationId, alternateIds: ImmutableSet<CorrelationId>,
        @UnderInitialization shuttle: DeduplicateCorrelateVariables
    ) : RexShuttle() {
        private val builder: RexBuilder
        private val canonicalId: CorrelationId
        private val alternateIds: ImmutableSet<CorrelationId>

        @NotOnlyInitialized
        private val shuttle: DeduplicateCorrelateVariables?

        init {
            this.builder = builder
            this.canonicalId = canonicalId
            this.alternateIds = alternateIds
            this.shuttle = shuttle
        }

        @Override
        fun visitCorrelVariable(variable: RexCorrelVariable): RexNode {
            return if (!alternateIds.contains(variable.id)) {
                variable
            } else builder.makeCorrel(variable.getType(), canonicalId)
        }

        @Override
        fun visitSubQuery(subQuery: RexSubQuery): RexNode {
            var subQuery: RexSubQuery = subQuery
            if (shuttle != null) {
                val r: RelNode = subQuery.rel.accept(shuttle) // look inside sub-queries
                if (r !== subQuery.rel) {
                    subQuery = subQuery.clone(r)
                }
            }
            return super.visitSubQuery(subQuery)
        }
    }

    companion object {
        /**
         * Rewrites a relational expression, replacing alternate correlation variables
         * with a canonical correlation variable.
         */
        fun go(
            builder: RexBuilder, canonicalId: CorrelationId,
            alternateIds: Iterable<CorrelationId?>?, r: RelNode
        ): RelNode {
            return r.accept(
                DeduplicateCorrelateVariables(
                    builder, canonicalId,
                    ImmutableSet.copyOf(alternateIds)
                )
            )
        }
    }
}
