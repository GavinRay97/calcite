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

import org.apache.calcite.config.CalciteConnectionConfig
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.materialize.Lattice
import org.apache.calcite.materialize.MaterializationService
import org.apache.calcite.materialize.TileKey
import org.apache.calcite.rel.RelNode
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.util.Pair
import java.util.List

/**
 * Use of a lattice by the query optimizer.
 */
class RelOptLattice(lattice: Lattice, starRelOptTable: RelOptTable) {
    val lattice: Lattice
    val starRelOptTable: RelOptTable

    init {
        this.lattice = lattice
        this.starRelOptTable = starRelOptTable
    }

    fun rootTable(): RelOptTable {
        return lattice.rootNode.relOptTable()
    }

    /** Rewrites a relational expression to use a lattice.
     *
     *
     * Returns null if a rewrite is not possible.
     *
     * @param node Relational expression
     * @return Rewritten query
     */
    @Nullable
    fun rewrite(node: RelNode?): RelNode {
        return RelOptMaterialization.tryUseStar(node, starRelOptTable)
    }

    /** Retrieves a materialized table that will satisfy an aggregate query on
     * the star table.
     *
     *
     * The current implementation creates a materialization and populates it,
     * provided that [Lattice.auto] is true.
     *
     *
     * Future implementations might return materializations at a different
     * level of aggregation, from which the desired result can be obtained by
     * rolling up.
     *
     * @param planner Current planner
     * @param groupSet Grouping key
     * @param measureList Calls to aggregate functions
     * @return Materialized table
     */
    @Nullable
    fun getAggregate(
        planner: RelOptPlanner, groupSet: ImmutableBitSet?,
        measureList: List<Lattice.Measure?>?
    ): Pair<CalciteSchema.TableEntry, TileKey>? {
        val config: CalciteConnectionConfig =
            planner.getContext().unwrap(CalciteConnectionConfig::class.java) ?: return null
        val service: MaterializationService = MaterializationService.instance()
        val create = lattice.auto && config.createMaterializations()
        val schema: CalciteSchema = starRelOptTable.unwrap(CalciteSchema::class.java)
        assert(schema != null) { "Can't get CalciteSchema from $starRelOptTable" }
        return service.defineTile(
            lattice, groupSet, measureList, schema, create,
            false
        )
    }
}
