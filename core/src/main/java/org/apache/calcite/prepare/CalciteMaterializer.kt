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
package org.apache.calcite.prepare

import org.apache.calcite.adapter.enumerable.EnumerableRel

/**
 * Context for populating a [Prepare.Materialization].
 */
internal class CalciteMaterializer(
    prepare: CalcitePrepareImpl,
    context: CalcitePrepare.Context?,
    catalogReader: CatalogReader, schema: CalciteSchema,
    cluster: RelOptCluster, convertletTable: SqlRexConvertletTable
) : CalcitePrepareImpl.CalcitePreparingStmt(
    prepare, context, catalogReader, catalogReader.getTypeFactory(),
    schema, EnumerableRel.Prefer.ANY, cluster, BindableConvention.INSTANCE,
    convertletTable
) {
    /** Populates a materialization record, converting a table path
     * (essentially a list of strings, like ["hr", "sales"]) into a table object
     * that can be used in the planning process.  */
    fun populate(materialization: Materialization) {
        val parser: SqlParser = SqlParser.create(materialization.sql)
        val node: SqlNode
        node = try {
            parser.parseStmt()
        } catch (e: SqlParseException) {
            throw RuntimeException("parse failed", e)
        }
        val config: SqlToRelConverter.Config = SqlToRelConverter.config().withTrimUnusedFields(true)
        val sqlToRelConverter2: SqlToRelConverter = getSqlToRelConverter(getSqlValidator(), catalogReader, config)
        val root: RelRoot = sqlToRelConverter2.convertQuery(node, true, true)
        materialization.queryRel = trimUnusedFields(root).rel

        // Identify and substitute a StarTable in queryRel.
        //
        // It is possible that no StarTables match. That is OK, but the
        // materialization patterns that are recognized will not be as rich.
        //
        // It is possible that more than one StarTable matches. TBD: should we
        // take the best (whatever that means), or all of them?
        useStar(schema, materialization)
        val tableName: List<String> = materialization.materializedTable.path()
        val table: RelOptTable = requireNonNull(
            this.catalogReader.getTable(tableName)
        ) { "table $tableName is not found" }
        materialization.tableRel = sqlToRelConverter2.toRel(table, ImmutableList.of())
    }

    /** Converts a relational expression to use a
     * [StarTable] defined in `schema`.
     * Uses the first star table that fits.  */
    private fun useStar(schema: CalciteSchema, materialization: Materialization) {
        val queryRel: RelNode = requireNonNull(materialization.queryRel, "materialization.queryRel")
        for (x in useStar(schema, queryRel)) {
            // Success -- we found a star table that matches.
            materialization.materialize(x.rel, x.starRelOptTable)
            if (CalciteSystemProperty.DEBUG.value()) {
                System.out.println(
                    ("Materialization "
                            + materialization.materializedTable) + " matched star table "
                            + x.starTable.toString() + "; query after re-write: "
                            + RelOptUtil.toString(queryRel)
                )
            }
        }
    }

    /** Converts a relational expression to use a
     * [org.apache.calcite.schema.impl.StarTable] defined in `schema`.
     * Uses the first star table that fits.  */
    private fun useStar(schema: CalciteSchema, queryRel: RelNode): Iterable<Callback> {
        val starTables: List<CalciteSchema.TableEntry> = Schemas.getStarTables(schema.root())
        if (starTables.isEmpty()) {
            // Don't waste effort converting to leaf-join form.
            return ImmutableList.of()
        }
        val list: List<Callback> = ArrayList()
        val rel2: RelNode = RelOptMaterialization.toLeafJoinForm(queryRel)
        for (starTable in starTables) {
            val table: Table = starTable.getTable()
            assert(table is StarTable)
            val starRelOptTable: RelOptTableImpl = RelOptTableImpl.create(
                catalogReader, table.getRowType(typeFactory),
                starTable, null
            )
            val rel3: RelNode = RelOptMaterialization.tryUseStar(rel2, starRelOptTable)
            if (rel3 != null) {
                list.add(Callback(rel3, starTable, starRelOptTable))
            }
        }
        return list
    }

    /** Implementation of [RelShuttle] that returns each relational
     * expression unchanged. It does not visit inputs.  */
    internal class RelNullShuttle : RelShuttle {
        @Override
        fun visit(scan: TableScan): RelNode {
            return scan
        }

        @Override
        fun visit(scan: TableFunctionScan): RelNode {
            return scan
        }

        @Override
        fun visit(values: LogicalValues): RelNode {
            return values
        }

        @Override
        fun visit(filter: LogicalFilter): RelNode {
            return filter
        }

        @Override
        fun visit(calc: LogicalCalc): RelNode {
            return calc
        }

        @Override
        fun visit(project: LogicalProject): RelNode {
            return project
        }

        @Override
        fun visit(join: LogicalJoin): RelNode {
            return join
        }

        @Override
        fun visit(correlate: LogicalCorrelate): RelNode {
            return correlate
        }

        @Override
        fun visit(union: LogicalUnion): RelNode {
            return union
        }

        @Override
        fun visit(intersect: LogicalIntersect): RelNode {
            return intersect
        }

        @Override
        fun visit(minus: LogicalMinus): RelNode {
            return minus
        }

        @Override
        fun visit(aggregate: LogicalAggregate): RelNode {
            return aggregate
        }

        @Override
        fun visit(match: LogicalMatch): RelNode {
            return match
        }

        @Override
        fun visit(sort: LogicalSort): RelNode {
            return sort
        }

        @Override
        fun visit(exchange: LogicalExchange): RelNode {
            return exchange
        }

        @Override
        fun visit(modify: LogicalTableModify): RelNode {
            return modify
        }

        @Override
        fun visit(other: RelNode): RelNode {
            return other
        }
    }

    /** Called when we discover a star table that matches.  */
    internal class Callback(
        rel: RelNode,
        starTable: CalciteSchema.TableEntry,
        starRelOptTable: RelOptTableImpl
    ) {
        val rel: RelNode
        val starTable: CalciteSchema.TableEntry
        val starRelOptTable: RelOptTableImpl

        init {
            this.rel = rel
            this.starTable = starTable
            this.starRelOptTable = starRelOptTable
        }
    }
}
