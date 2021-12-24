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
 * Sub-class of [org.apache.calcite.rel.core.TableFunctionScan]
 * not targeted at any particular engine or calling convention.
 */
class LogicalTableFunctionScan : TableFunctionScan {
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a `LogicalTableFunctionScan`.
     *
     * @param cluster        Cluster that this relational expression belongs to
     * @param inputs         0 or more relational inputs
     * @param traitSet       Trait set
     * @param rexCall        Function invocation expression
     * @param elementType    Element type of the collection that will implement
     * this table
     * @param rowType        Row type produced by function
     * @param columnMappings Column mappings associated with this function
     */
    constructor(
        cluster: RelOptCluster?,
        traitSet: RelTraitSet?,
        inputs: List<RelNode?>?,
        rexCall: RexNode?,
        @Nullable elementType: Type?, rowType: RelDataType?,
        @Nullable columnMappings: Set<RelColumnMapping?>?
    ) : super(
        cluster, traitSet, inputs, rexCall, elementType, rowType,
        columnMappings
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster,
        inputs: List<RelNode?>?,
        rexCall: RexNode?,
        @Nullable elementType: Type?, rowType: RelDataType?,
        @Nullable columnMappings: Set<RelColumnMapping?>?
    ) : this(
        cluster, cluster.traitSetOf(Convention.NONE), inputs, rexCall,
        elementType, rowType, columnMappings
    ) {
    }

    /**
     * Creates a LogicalTableFunctionScan by parsing serialized output.
     */
    constructor(input: RelInput?) : super(input) {}

    //~ Methods ----------------------------------------------------------------
    @Override
    fun copy(
        traitSet: RelTraitSet,
        inputs: List<RelNode?>?,
        rexCall: RexNode?,
        @Nullable elementType: Type?,
        rowType: RelDataType?,
        @Nullable columnMappings: Set<RelColumnMapping?>?
    ): LogicalTableFunctionScan {
        assert(traitSet.containsIfApplicable(Convention.NONE))
        return LogicalTableFunctionScan(
            getCluster(),
            traitSet,
            inputs,
            rexCall,
            elementType,
            rowType,
            columnMappings
        )
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner,
        mq: RelMetadataQuery?
    ): RelOptCost {
        // REVIEW jvs 8-Jan-2006:  what is supposed to be here
        // for an abstract rel?
        return planner.getCostFactory().makeHugeCost()
    }

    companion object {
        /** Creates a LogicalTableFunctionScan.  */
        fun create(
            cluster: RelOptCluster,
            inputs: List<RelNode?>?,
            rexCall: RexNode?,
            @Nullable elementType: Type?, rowType: RelDataType?,
            @Nullable columnMappings: Set<RelColumnMapping?>?
        ): LogicalTableFunctionScan {
            val traitSet: RelTraitSet = cluster.traitSetOf(Convention.NONE)
            return LogicalTableFunctionScan(
                cluster, traitSet, inputs, rexCall,
                elementType, rowType, columnMappings
            )
        }
    }
}
