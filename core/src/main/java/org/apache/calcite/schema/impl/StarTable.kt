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
package org.apache.calcite.schema.impl

import org.apache.calcite.materialize.Lattice

/**
 * Virtual table that is composed of two or more tables joined together.
 *
 *
 * Star tables do not occur in end-user queries. They are introduced by the
 * optimizer to help matching queries to materializations, and used only
 * during the planning process.
 *
 *
 * When a materialization is defined, if it involves a join, it is converted
 * to a query on top of a star table. Queries that are candidates to map onto
 * the materialization are mapped onto the same star table.
 */
class StarTable private constructor(lattice: Lattice, tables: ImmutableList<Table>) : AbstractTable(),
    TranslatableTable {
    val lattice: Lattice

    // TODO: we'll also need a list of join conditions between tables. For now
    //  we assume that join conditions match
    val tables: ImmutableList<Table>

    /** Number of fields in each table's row type.  */
    @MonotonicNonNull
    var fieldCounts: ImmutableIntList? = null

    /** Creates a StarTable.  */
    init {
        this.lattice = Objects.requireNonNull(lattice, "lattice")
        this.tables = tables
    }

    @get:Override
    override val jdbcTableType: Schema.TableType
        get() = Schema.TableType.STAR

    @Override
    fun getRowType(typeFactory: RelDataTypeFactory): RelDataType {
        val typeList: List<RelDataType> = ArrayList()
        val fieldCounts: List<Integer> = ArrayList()
        for (table in tables) {
            val rowType: RelDataType = table.getRowType(typeFactory)
            typeList.addAll(RelOptUtil.getFieldTypeList(rowType))
            fieldCounts.add(rowType.getFieldCount())
        }
        // Compute fieldCounts the first time this method is called. Safe to assume
        // that the field counts will be the same whichever type factory is used.
        if (this.fieldCounts == null) {
            this.fieldCounts = ImmutableIntList.copyOf(fieldCounts)
        }
        return typeFactory.createStructType(typeList, lattice.uniqueColumnNames())
    }

    @Override
    fun toRel(context: RelOptTable.ToRelContext, table: RelOptTable?): RelNode {
        // Create a table scan of infinite cost.
        return StarTableScan(context.getCluster(), table)
    }

    fun add(table: Table?): StarTable {
        return of(
            lattice,
            ImmutableList.< Table > builder < Table ? > ().addAll(tables).add(table).build()
        )
    }

    /** Returns the column offset of the first column of `table` in this
     * star table's output row type.
     *
     * @param table Table
     * @return Column offset
     * @throws IllegalArgumentException if table is not in this star
     */
    fun columnOffset(table: Table): Int {
        var n = 0
        for (pair in Pair.zip(tables, castNonNull(fieldCounts))) {
            if (pair.left === table) {
                return n
            }
            n += pair.right
        }
        throw IllegalArgumentException(
            "star table " + this
                    + " does not contain table " + table
        )
    }

    /** Relational expression that scans a [StarTable].
     *
     *
     * It has infinite cost.
     */
    class StarTableScan(cluster: RelOptCluster, relOptTable: RelOptTable?) :
        TableScan(cluster, cluster.traitSetOf(Convention.NONE), ImmutableList.of(), relOptTable) {
        @Override
        @Nullable
        fun computeSelfCost(
            planner: RelOptPlanner,
            mq: RelMetadataQuery?
        ): RelOptCost {
            return planner.getCostFactory().makeInfiniteCost()
        }
    }

    companion object {
        /** Creates a StarTable and registers it in a schema.  */
        fun of(lattice: Lattice, tables: List<Table?>?): StarTable {
            return StarTable(lattice, ImmutableList.copyOf(tables))
        }
    }
}
