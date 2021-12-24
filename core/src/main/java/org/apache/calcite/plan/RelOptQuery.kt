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
package org.apache.calcite.plan

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.CorrelationId
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.rex.RexBuilder
import java.util.HashMap
import java.util.Map
import java.util.concurrent.atomic.AtomicInteger

/**
 * A `RelOptQuery` represents a set of
 * [relational expressions][RelNode] which derive from the same
 * `select` statement.
 */
class RelOptQuery internal constructor(
    planner: RelOptPlanner?, nextCorrel: AtomicInteger,
    mapCorrelToRel: Map<String, RelNode>
) {
    //~ Instance fields --------------------------------------------------------
    /**
     * Maps name of correlating variable (e.g. "$cor3") to the [RelNode]
     * which implements it.
     */
    val mapCorrelToRel: Map<String, RelNode>
    private val planner: RelOptPlanner?
    val nextCorrel: AtomicInteger
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a query.
     *
     * @param planner Planner
     */
    @Deprecated // to be removed before 2.0
    constructor(planner: RelOptPlanner?) : this(planner, AtomicInteger(0), HashMap()) {
    }

    /** For use by RelOptCluster only.  */
    init {
        this.planner = planner
        this.nextCorrel = nextCorrel
        this.mapCorrelToRel = mapCorrelToRel
    }

    /**
     * Creates a cluster.
     *
     * @param typeFactory Type factory
     * @param rexBuilder  Expression builder
     * @return New cluster
     */
    @Deprecated // to be removed before 2.0
    fun createCluster(
        typeFactory: RelDataTypeFactory?,
        rexBuilder: RexBuilder?
    ): RelOptCluster {
        return RelOptCluster(
            planner, typeFactory, rexBuilder, nextCorrel,
            mapCorrelToRel
        )
    }

    /**
     * Constructs a new name for a correlating variable. It is unique within the
     * whole query.
     *
     */
    @Deprecated // to be removed before 2.0
    @Deprecated("Use {@link RelOptCluster#createCorrel()}")
    fun createCorrel(): String {
        val n: Int = nextCorrel.getAndIncrement()
        return CORREL_PREFIX + n
    }

    /**
     * Returns the relational expression which populates a correlating variable.
     */
    @Nullable
    fun lookupCorrel(name: String): RelNode? {
        return mapCorrelToRel[name]
    }

    /**
     * Maps a correlating variable to a [RelNode].
     */
    fun mapCorrel(
        name: String?,
        rel: RelNode?
    ) {
        mapCorrelToRel.put(name, rel)
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        /**
         * Prefix to the name of correlating variables.
         */
        val CORREL_PREFIX: String = CorrelationId.CORREL_PREFIX
        //~ Methods ----------------------------------------------------------------
        /**
         * Converts a correlating variable name into an ordinal, unique within the
         * query.
         *
         * @param correlName Name of correlating variable
         * @return Correlating variable ordinal
         */
        @Deprecated // to be removed before 2.0
        fun getCorrelOrdinal(correlName: String): Int {
            assert(correlName.startsWith(CORREL_PREFIX))
            return Integer.parseInt(correlName.substring(CORREL_PREFIX.length()))
        }
    }
}
