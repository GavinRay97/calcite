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
package org.apache.calcite.rel.logical

import org.apache.calcite.plan.Convention

/**
 * Sub-class of [org.apache.calcite.rel.core.Sort] not
 * targeted at any particular engine or calling convention.
 */
class LogicalSort : Sort {
    private constructor(
        cluster: RelOptCluster, traitSet: RelTraitSet,
        input: RelNode, collation: RelCollation, @Nullable offset: RexNode, @Nullable fetch: RexNode
    ) : super(cluster, traitSet, input, collation, offset, fetch) {
        assert(traitSet.containsIfApplicable(Convention.NONE))
    }

    /**
     * Creates a LogicalSort by parsing serialized output.
     */
    constructor(input: RelInput?) : super(input) {}

    //~ Methods ----------------------------------------------------------------
    @Override
    fun copy(
        traitSet: RelTraitSet, newInput: RelNode,
        newCollation: RelCollation, @Nullable offset: RexNode, @Nullable fetch: RexNode
    ): Sort {
        return LogicalSort(
            getCluster(), traitSet, newInput, newCollation,
            offset, fetch
        )
    }

    @Override
    fun accept(shuttle: RelShuttle): RelNode {
        return shuttle.visit(this)
    }

    companion object {
        /**
         * Creates a LogicalSort.
         *
         * @param input     Input relational expression
         * @param collation array of sort specifications
         * @param offset    Expression for number of rows to discard before returning
         * first row
         * @param fetch     Expression for number of rows to fetch
         */
        fun create(
            input: RelNode, collation: RelCollation,
            @Nullable offset: RexNode, @Nullable fetch: RexNode
        ): LogicalSort {
            var collation: RelCollation = collation
            val cluster: RelOptCluster = input.getCluster()
            collation = RelCollationTraitDef.INSTANCE.canonize(collation)
            val traitSet: RelTraitSet = input.getTraitSet().replace(Convention.NONE).replace(collation)
            return LogicalSort(cluster, traitSet, input, collation, offset, fetch)
        }
    }
}
