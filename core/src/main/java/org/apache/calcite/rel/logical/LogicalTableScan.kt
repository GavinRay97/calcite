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
 * A `LogicalTableScan` reads all the rows from a
 * [RelOptTable].
 *
 *
 * If the table is a `net.sf.saffron.ext.JdbcTable`, then this is
 * literally possible. But for other kinds of tables, there may be many ways to
 * read the data from the table. For some kinds of table, it may not even be
 * possible to read all of the rows unless some narrowing constraint is applied.
 *
 *
 * In the example of the `net.sf.saffron.ext.ReflectSchema`
 * schema,
 *
 * <blockquote>
 * <pre>select from fields</pre>
</blockquote> *
 *
 *
 * cannot be implemented, but
 *
 * <blockquote>
 * <pre>select from fields as f
 * where f.getClass().getName().equals("java.lang.String")</pre>
</blockquote> *
 *
 *
 * can. It is the optimizer's responsibility to find these ways, by applying
 * transformation rules.
 */
class LogicalTableScan : TableScan {
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a LogicalTableScan.
     *
     *
     * Use [.create] unless you know what you're doing.
     */
    constructor(
        cluster: RelOptCluster?, traitSet: RelTraitSet?,
        hints: List<RelHint?>?, table: RelOptTable?
    ) : super(cluster, traitSet, hints, table) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster?, traitSet: RelTraitSet?,
        table: RelOptTable?
    ) : this(cluster, traitSet, ImmutableList.of(), table) {
    }

    @Deprecated // to be removed before 2.0
    constructor(cluster: RelOptCluster, table: RelOptTable?) : this(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        ImmutableList.of(),
        table
    ) {
    }

    /**
     * Creates a LogicalTableScan by parsing serialized output.
     */
    constructor(input: RelInput?) : super(input) {}

    @Override
    fun copy(traitSet: RelTraitSet, inputs: List<RelNode?>): RelNode {
        assert(traitSet.containsIfApplicable(Convention.NONE))
        assert(inputs.isEmpty())
        return this
    }

    @Override
    fun withHints(hintList: List<RelHint?>?): RelNode {
        return LogicalTableScan(getCluster(), traitSet, hintList, table)
    }

    companion object {
        /** Creates a LogicalTableScan.
         *
         * @param cluster     Cluster
         * @param relOptTable Table
         * @param hints       The hints
         */
        fun create(
            cluster: RelOptCluster,
            relOptTable: RelOptTable, hints: List<RelHint?>?
        ): LogicalTableScan {
            val table: Table = relOptTable.unwrap(Table::class.java)
            val traitSet: RelTraitSet = cluster.traitSetOf(Convention.NONE)
                .replaceIfs(RelCollationTraitDef.INSTANCE) {
                    if (table != null) {
                        return@replaceIfs table.getStatistic().getCollations()
                    }
                    ImmutableList.of()
                }
            return LogicalTableScan(cluster, traitSet, hints, relOptTable)
        }
    }
}
