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
package org.apache.calcite.rel.convert

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptCost
import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.plan.RelTraitDef
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.SingleRel
import org.apache.calcite.rel.metadata.RelMetadataQuery

/**
 * Abstract implementation of [Converter].
 */
abstract class ConverterImpl protected constructor(
    cluster: RelOptCluster?,
    @Nullable traitDef: RelTraitDef,
    traits: RelTraitSet?,
    child: RelNode
) : SingleRel(cluster, traits, child), Converter {
    //~ Instance fields --------------------------------------------------------
    protected var inTraits: RelTraitSet

    @Nullable
    protected val traitDef: RelTraitDef
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a ConverterImpl.
     *
     * @param cluster  planner's cluster
     * @param traitDef the RelTraitDef this converter converts
     * @param traits   the output traits of this converter
     * @param child    child rel (provides input traits)
     */
    init {
        inTraits = child.getTraitSet()
        this.traitDef = traitDef
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner,
        mq: RelMetadataQuery
    ): RelOptCost {
        val dRows: Double = mq.getRowCount(getInput())
        val dIo = 0.0
        return planner.getCostFactory().makeCost(dRows, dRows, dIo)
    }

    @Deprecated // to be removed before 2.0
    protected fun cannotImplement(): Error {
        return AssertionError(
            getClass() + " cannot convert from "
                    + inTraits + " traits"
        )
    }

    @get:Override
    val inputTraits: RelTraitSet
        get() = inTraits

    @Override
    @Nullable
    fun getTraitDef(): RelTraitDef {
        return traitDef
    }
}
