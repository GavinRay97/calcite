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
package org.apache.calcite.rel.rules

import org.apache.calcite.config.CalciteConnectionConfig
import org.apache.calcite.config.CalciteSystemProperty
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.materialize.Lattice
import org.apache.calcite.materialize.TileKey
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptLattice
import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelOptRuleOperand
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.RelRule
import org.apache.calcite.plan.ViewExpanders
import org.apache.calcite.prepare.RelOptTableImpl
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.schema.Table
import org.apache.calcite.schema.impl.StarTable
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.util.Pair
import org.apache.calcite.util.mapping.AbstractSourceMapping
import com.google.common.collect.ImmutableList
import org.immutables.value.Value
import java.util.ArrayList
import java.util.List
import java.util.Optional
import java.util.Objects.requireNonNull

/**
 * Planner rule that matches an [org.apache.calcite.rel.core.Aggregate] on
 * top of a [org.apache.calcite.schema.impl.StarTable.StarTableScan].
 *
 *
 * This pattern indicates that an aggregate table may exist. The rule asks
 * the star table for an aggregate table at the required level of aggregation.
 *
 * @see AggregateProjectStarTableRule
 *
 * @see CoreRules.AGGREGATE_STAR_TABLE
 *
 * @see CoreRules.AGGREGATE_PROJECT_STAR_TABLE
 */
@Value.Enclosing
class AggregateStarTableRule
/** Creates an AggregateStarTableRule.  */
protected constructor(config: Config?) : RelRule<AggregateStarTableRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        operand: RelOptRuleOperand?,
        relBuilderFactory: RelBuilderFactory?, description: String?
    ) : this(Config.DEFAULT
        .withRelBuilderFactory(relBuilderFactory)
        .withDescription(description)
        .withOperandSupplier { b -> b.exactly(operand) }
        .`as`(Config::class.java)) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val aggregate: Aggregate = call.rel(0)
        val scan: StarTable.StarTableScan = call.rel(1)
        apply(call, null, aggregate, scan)
    }

    protected fun apply(
        call: RelOptRuleCall, @Nullable postProject: Project?,
        aggregate: Aggregate, scan: StarTable.StarTableScan
    ) {
        val planner: RelOptPlanner = call.getPlanner()
        val config: Optional<CalciteConnectionConfig> =
            planner.getContext().maybeUnwrap(CalciteConnectionConfig::class.java)
        if (!(config.isPresent() && config.get().createMaterializations())) {
            // Disable this rule if we if materializations are disabled - in
            // particular, if we are in a recursive statement that is being used to
            // populate a materialization
            return
        }
        val cluster: RelOptCluster = scan.getCluster()
        val table: RelOptTable = scan.getTable()
        val lattice: RelOptLattice = requireNonNull(
            planner.getLattice(table)
        ) { "planner.getLattice(table) is null for $table" }
        val measures: List<Lattice.Measure> = lattice.lattice.toMeasures(aggregate.getAggCallList())
        val pair: Pair<CalciteSchema.TableEntry, TileKey> =
            lattice.getAggregate(planner, aggregate.getGroupSet(), measures)
                ?: return
        val relBuilder: RelBuilder = call.builder()
        val tableEntry: CalciteSchema.TableEntry = pair.left
        val tileKey: TileKey = pair.right
        val mq: RelMetadataQuery = call.getMetadataQuery()
        val rowCount: Double = aggregate.estimateRowCount(mq)
        val aggregateTable: Table = tableEntry.getTable()
        val aggregateTableRowType: RelDataType = aggregateTable.getRowType(cluster.getTypeFactory())
        val aggregateRelOptTable: RelOptTable = RelOptTableImpl.create(
            table.getRelOptSchema(),
            aggregateTableRowType,
            tableEntry,
            rowCount
        )
        relBuilder.push(aggregateRelOptTable.toRel(ViewExpanders.simpleContext(cluster)))
        if (tileKey == null) {
            if (CalciteSystemProperty.DEBUG.value()) {
                System.out.println(
                    ("Using materialization "
                            + aggregateRelOptTable.getQualifiedName()
                            ) + " (exact match)"
                )
            }
        } else if (!tileKey.dimensions.equals(aggregate.getGroupSet())) {
            // Aggregate has finer granularity than we need. Roll up.
            if (CalciteSystemProperty.DEBUG.value()) {
                System.out.println(
                    ("Using materialization "
                            + aggregateRelOptTable.getQualifiedName()
                            ) + ", rolling up " + tileKey.dimensions.toString() + " to "
                            + aggregate.getGroupSet()
                )
            }
            assert(tileKey.dimensions.contains(aggregate.getGroupSet()))
            val aggCalls: List<AggregateCall> = ArrayList()
            val groupSet: ImmutableBitSet.Builder = ImmutableBitSet.builder()
            for (key in aggregate.getGroupSet()) {
                groupSet.set(tileKey.dimensions.indexOf(key))
            }
            for (aggCall in aggregate.getAggCallList()) {
                val copy: AggregateCall = rollUp(groupSet.cardinality(), relBuilder, aggCall, tileKey)
                    ?: return
                aggCalls.add(copy)
            }
            relBuilder.push(
                aggregate.copy(
                    aggregate.getTraitSet(), relBuilder.build(),
                    groupSet.build(), null, aggCalls
                )
            )
        } else if (!tileKey.measures.equals(measures)) {
            if (CalciteSystemProperty.DEBUG.value()) {
                System.out.println(
                    ("Using materialization "
                            + aggregateRelOptTable.getQualifiedName()
                            ) + ", right granularity, but different measures "
                            + aggregate.getAggCallList()
                )
            }
            relBuilder.project(
                relBuilder.fields(
                    object : AbstractSourceMapping(
                        tileKey.dimensions.cardinality() + tileKey.measures.size(),
                        aggregate.getRowType().getFieldCount()
                    ) {
                        @Override
                        fun getSourceOpt(source: Int): Int {
                            if (source < aggregate.getGroupCount()) {
                                val `in`: Int = tileKey.dimensions.nth(source)
                                return aggregate.getGroupSet().indexOf(`in`)
                            }
                            val measure: Lattice.Measure = measures[source - aggregate.getGroupCount()]
                            val i: Int = tileKey.measures.indexOf(measure)
                            assert(i >= 0)
                            return tileKey.dimensions.cardinality() + i
                        }
                    }.inverse()
                )
            )
        }
        if (postProject != null) {
            relBuilder.push(
                postProject.copy(
                    postProject.getTraitSet(),
                    ImmutableList.of(relBuilder.peek())
                )
            )
        }
        call.transformTo(relBuilder.build())
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): AggregateStarTableRule? {
            return AggregateStarTableRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            aggregateClass: Class<out Aggregate?>?,
            scanClass: Class<StarTable.StarTableScan?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(aggregateClass)
                    .predicate(Aggregate::isSimple)
                    .oneInput { b1 -> b1.operand(scanClass).noInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableAggregateStarTableRule.Config.of()
                .withOperandFor(Aggregate::class.java, StarTable.StarTableScan::class.java)
        }
    }

    companion object {
        @Nullable
        private fun rollUp(
            groupCount: Int, relBuilder: RelBuilder,
            aggregateCall: AggregateCall, tileKey: TileKey?
        ): AggregateCall? {
            if (aggregateCall.isDistinct()) {
                return null
            }
            val aggregation: SqlAggFunction = aggregateCall.getAggregation()
            val seek: Pair<SqlAggFunction, List<Integer>> = Pair.of(aggregation, aggregateCall.getArgList())
            val offset: Int = tileKey.dimensions.cardinality()
            val measures: ImmutableList<Lattice.Measure> = tileKey.measures

            // First, try to satisfy the aggregation by rolling up an aggregate in the
            // materialization.
            val i = find(measures, seek)
            tryRoll@ if (i >= 0) {
                val roll: SqlAggFunction = aggregation.getRollup() ?: break@tryRoll
                return AggregateCall.create(
                    roll, false, aggregateCall.isApproximate(),
                    aggregateCall.ignoreNulls(), ImmutableList.of(offset + i), -1,
                    aggregateCall.distinctKeys, aggregateCall.collation,
                    groupCount, relBuilder.peek(), null, aggregateCall.name
                )
            }

            // Second, try to satisfy the aggregation based on group set columns.
            tryGroup@{
                val newArgs: List<Integer> = ArrayList()
                for (arg in aggregateCall.getArgList()) {
                    val z: Int = tileKey.dimensions.indexOf(arg)
                    if (z < 0) {
                        break@tryGroup
                    }
                    newArgs.add(z)
                }
                return AggregateCall.create(
                    aggregation, false,
                    aggregateCall.isApproximate(), aggregateCall.ignoreNulls(),
                    newArgs, -1, aggregateCall.distinctKeys, aggregateCall.collation,
                    groupCount, relBuilder.peek(), null, aggregateCall.name
                )
            }

            // No roll up possible.
            return null
        }

        private fun find(
            measures: ImmutableList<Lattice.Measure>,
            seek: Pair<SqlAggFunction, List<Integer>>
        ): Int {
            for (i in 0 until measures.size()) {
                val measure: Lattice.Measure = measures.get(i)
                if (measure.agg.equals(seek.left)
                    && measure.argOrdinals().equals(seek.right)
                ) {
                    return i
                }
            }
            return -1
        }
    }
}
