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

import org.apache.calcite.config.CalciteSystemProperty
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.RelShuttleImpl
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider
import org.apache.calcite.rel.rules.CoreRules
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexUtil
import org.apache.calcite.schema.Table
import org.apache.calcite.schema.impl.StarTable
import org.apache.calcite.sql.SqlExplainFormat
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.tools.Program
import org.apache.calcite.tools.Programs
import org.apache.calcite.util.Util
import org.apache.calcite.util.mapping.Mappings
import com.google.common.collect.ImmutableList
import java.util.ArrayList
import java.util.List
import java.util.Objects
import org.apache.calcite.linq4j.Nullness.castNonNull

/**
 * Records that a particular query is materialized by a particular table.
 */
class RelOptMaterialization(
    tableRel: RelNode?, queryRel: RelNode,
    @Nullable starRelOptTable: RelOptTable?, qualifiedTableName: List<String>
) {
    val tableRel: RelNode

    @Nullable
    val starRelOptTable: RelOptTable?

    @Nullable
    var starTable: StarTable? = null
    val qualifiedTableName: List<String>
    val queryRel: RelNode

    /**
     * Creates a RelOptMaterialization.
     */
    init {
        this.tableRel = RelOptUtil.createCastRel(
            Objects.requireNonNull(tableRel, "tableRel"),
            Objects.requireNonNull(queryRel, "queryRel").getRowType(),
            false
        )
        this.starRelOptTable = starRelOptTable
        if (starRelOptTable == null) {
            starTable = null
        } else {
            starTable = starRelOptTable.unwrapOrThrow(StarTable::class.java)
        }
        this.qualifiedTableName = qualifiedTableName
        this.queryRel = queryRel
    }

    /** A table scan and optional project mapping and filter condition.  */
    private class ProjectFilterTable private constructor(
        @Nullable condition: RexNode?,
        mapping: @Nullable Mappings.TargetMapping?, scan: TableScan
    ) {
        @Nullable
        val condition: RexNode?
        val mapping: @Nullable Mappings.TargetMapping?
        val scan: TableScan

        init {
            this.condition = condition
            this.mapping = mapping
            this.scan = Objects.requireNonNull(scan, "scan")
        }

        fun mapping(): Mappings.TargetMapping {
            return if (mapping != null) mapping else Mappings.createIdentity(scan.getRowType().getFieldCount())
        }

        val table: RelOptTable
            get() = scan.getTable()

        companion object {
            @Nullable
            fun of(node: RelNode): ProjectFilterTable? {
                return if (node is Filter) {
                    val filter: Filter = node as Filter
                    of2(
                        filter.getCondition(),
                        filter.getInput()
                    )
                } else {
                    of2(null, node)
                }
            }

            @Nullable
            private fun of2(@Nullable condition: RexNode?, node: RelNode): ProjectFilterTable? {
                return if (node is Project) {
                    val project: Project = node as Project
                    of3(
                        condition,
                        project.getMapping(),
                        project.getInput()
                    )
                } else {
                    of3(condition, null, node)
                }
            }

            @Nullable
            private fun of3(
                @Nullable condition: RexNode?,
                mapping: @Nullable Mappings.TargetMapping?, node: RelNode
            ): ProjectFilterTable? {
                return if (node is TableScan) {
                    ProjectFilterTable(
                        condition, mapping,
                        node as TableScan
                    )
                } else {
                    null
                }
            }
        }
    }

    companion object {
        /**
         * Converts a relational expression to one that uses a
         * [org.apache.calcite.schema.impl.StarTable].
         *
         *
         * The relational expression is already in leaf-join-form, per
         * [.toLeafJoinForm].
         *
         * @return Rewritten expression, or null if expression cannot be rewritten
         * to use the star
         */
        @Nullable
        fun tryUseStar(
            rel: RelNode,
            starRelOptTable: RelOptTable
        ): RelNode? {
            val starTable: StarTable = starRelOptTable.unwrapOrThrow(StarTable::class.java)
            val rel2: RelNode = rel.accept(
                object : RelShuttleImpl() {
                    @Override
                    fun visit(scan: TableScan): RelNode {
                        val relOptTable: RelOptTable = scan.getTable()
                        val table: Table = relOptTable.unwrap(Table::class.java)
                        if (Objects.equals(table, starTable.tables.get(0))) {
                            val mapping: Mappings.TargetMapping = Mappings.createShiftMapping(
                                starRelOptTable.getRowType().getFieldCount(),
                                0, 0, relOptTable.getRowType().getFieldCount()
                            )
                            val cluster: RelOptCluster = scan.getCluster()
                            val scan2: RelNode = starRelOptTable.toRel(ViewExpanders.simpleContext(cluster))
                            return RelOptUtil.createProject(
                                scan2,
                                Mappings.asListNonNull(mapping.inverse())
                            )
                        }
                        return scan
                    }

                    @Override
                    fun visit(join: LogicalJoin): RelNode {
                        var join: LogicalJoin = join
                        while (true) {
                            val rel: RelNode = super.visit(join)
                            if (rel === join || rel !is LogicalJoin) {
                                return rel
                            }
                            join = rel as LogicalJoin
                            val left = ProjectFilterTable.of(join.getLeft())
                            if (left != null) {
                                val right = ProjectFilterTable.of(join.getRight())
                                if (right != null) {
                                    try {
                                        match(left, right, join.getCluster())
                                    } catch (e: Util.FoundOne) {
                                        return Objects.requireNonNull(e.getNode(), "FoundOne.getNode") as RelNode
                                    }
                                }
                            }
                        }
                    }

                    /** Throws a [org.apache.calcite.util.Util.FoundOne] containing
                     * a [org.apache.calcite.rel.logical.LogicalTableScan] on
                     * success.  (Yes, an exception for normal operation.)  */
                    private fun match(
                        left: ProjectFilterTable, right: ProjectFilterTable,
                        cluster: RelOptCluster
                    ) {
                        val leftMapping: Mappings.TargetMapping = left.mapping()
                        val rightMapping: Mappings.TargetMapping = right.mapping()
                        val leftRelOptTable: RelOptTable = left.getTable()
                        val leftTable: Table = leftRelOptTable.unwrap(Table::class.java)
                        val leftCount: Int = leftRelOptTable.getRowType().getFieldCount()
                        val rightRelOptTable: RelOptTable = right.getTable()
                        val rightTable: Table = rightRelOptTable.unwrap(Table::class.java)
                        if (leftTable is StarTable
                            && rightTable != null && (leftTable as StarTable).tables.contains(rightTable)
                        ) {
                            val offset: Int = (leftTable as StarTable).columnOffset(rightTable)
                            val mapping: Mappings.TargetMapping = Mappings.merge(
                                leftMapping,
                                Mappings.offsetTarget(
                                    Mappings.offsetSource(rightMapping, offset),
                                    leftMapping.getTargetCount()
                                )
                            )
                            val project: RelNode = RelOptUtil.createProject(
                                leftRelOptTable.toRel(ViewExpanders.simpleContext(cluster)),
                                Mappings.asListNonNull(mapping.inverse())
                            )
                            val conditions: List<RexNode> = ArrayList()
                            if (left.condition != null) {
                                conditions.add(left.condition)
                            }
                            if (right.condition != null) {
                                conditions.add(
                                    RexUtil.apply(
                                        mapping,
                                        RexUtil.shift(right.condition, offset)
                                    )
                                )
                            }
                            val filter: RelNode = RelOptUtil.createFilter(project, conditions)
                            throw FoundOne(filter)
                        }
                        if (rightTable is StarTable
                            && leftTable != null && (rightTable as StarTable).tables.contains(leftTable)
                        ) {
                            val offset: Int = (rightTable as StarTable).columnOffset(leftTable)
                            val mapping: Mappings.TargetMapping = Mappings.merge(
                                Mappings.offsetSource(leftMapping, offset),
                                Mappings.offsetTarget(rightMapping, leftCount)
                            )
                            val project: RelNode = RelOptUtil.createProject(
                                rightRelOptTable.toRel(ViewExpanders.simpleContext(cluster)),
                                Mappings.asListNonNull(mapping.inverse())
                            )
                            val conditions: List<RexNode> = ArrayList()
                            if (left.condition != null) {
                                conditions.add(
                                    RexUtil.apply(
                                        mapping,
                                        RexUtil.shift(left.condition, offset)
                                    )
                                )
                            }
                            if (right.condition != null) {
                                conditions.add(RexUtil.apply(mapping, right.condition))
                            }
                            val filter: RelNode = RelOptUtil.createFilter(project, conditions)
                            throw FoundOne(filter)
                        }
                    }
                })
            if (rel2 === rel) {
                // No rewrite happened.
                return null
            }
            val program: Program = Programs.hep(
                ImmutableList.of(
                    CoreRules.PROJECT_FILTER_TRANSPOSE, CoreRules.AGGREGATE_PROJECT_MERGE,
                    CoreRules.AGGREGATE_FILTER_TRANSPOSE
                ),
                false,
                DefaultRelMetadataProvider.INSTANCE
            )
            return program.run(
                castNonNull(null), rel2, castNonNull(null),
                ImmutableList.of(),
                ImmutableList.of()
            )
        }

        /**
         * Converts a relational expression to a form where
         * [org.apache.calcite.rel.logical.LogicalJoin]s are
         * as close to leaves as possible.
         */
        fun toLeafJoinForm(rel: RelNode?): RelNode {
            val program: Program = Programs.hep(
                ImmutableList.of(
                    CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE,
                    CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE,
                    CoreRules.FILTER_INTO_JOIN,
                    CoreRules.PROJECT_REMOVE,
                    CoreRules.PROJECT_MERGE
                ),
                false,
                DefaultRelMetadataProvider.INSTANCE
            )
            if (CalciteSystemProperty.DEBUG.value()) {
                System.out.println(
                    RelOptUtil.dumpPlan(
                        "before", rel, SqlExplainFormat.TEXT,
                        SqlExplainLevel.DIGEST_ATTRIBUTES
                    )
                )
            }
            val rel2: RelNode = program.run(
                castNonNull(null), rel, castNonNull(null),
                ImmutableList.of(),
                ImmutableList.of()
            )
            if (CalciteSystemProperty.DEBUG.value()) {
                System.out.println(
                    RelOptUtil.dumpPlan(
                        "after", rel2, SqlExplainFormat.TEXT,
                        SqlExplainLevel.DIGEST_ATTRIBUTES
                    )
                )
            }
            return rel2
        }
    }
}
