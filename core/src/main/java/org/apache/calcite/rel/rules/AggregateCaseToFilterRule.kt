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

import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelRule
import org.apache.calcite.rel.RelCollations
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlPostfixOperator
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilderFactory
import com.google.common.collect.ImmutableList
import org.immutables.value.Value
import java.math.BigDecimal
import java.util.ArrayList
import java.util.List

/**
 * Rule that converts CASE-style filtered aggregates into true filtered
 * aggregates.
 *
 *
 * For example,
 *
 * <blockquote>
 * `SELECT SUM(CASE WHEN gender = 'F' THEN salary END)<br></br>
 * FROM Emp`
</blockquote> *
 *
 *
 * becomes
 *
 * <blockquote>
 * `SELECT SUM(salary) FILTER (WHERE gender = 'F')<br></br>
 * FROM Emp`
</blockquote> *
 *
 * @see CoreRules.AGGREGATE_CASE_TO_FILTER
 */
@Value.Enclosing
class AggregateCaseToFilterRule
/** Creates an AggregateCaseToFilterRule.  */
protected constructor(config: Config?) : RelRule<AggregateCaseToFilterRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    protected constructor(
        relBuilderFactory: RelBuilderFactory?,
        description: String?
    ) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .withDescription(description)
            .`as`(Config::class.java)
    ) {
    }

    @Override
    fun matches(call: RelOptRuleCall): Boolean {
        val aggregate: Aggregate = call.rel(0)
        val project: Project = call.rel(1)
        for (aggregateCall in aggregate.getAggCallList()) {
            val singleArg = soleArgument(aggregateCall)
            if (singleArg >= 0
                && isThreeArgCase(project.getProjects().get(singleArg))
            ) {
                return true
            }
        }
        return false
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val aggregate: Aggregate = call.rel(0)
        val project: Project = call.rel(1)
        val rexBuilder: RexBuilder = aggregate.getCluster().getRexBuilder()
        val newCalls: List<AggregateCall> = ArrayList(aggregate.getAggCallList().size())
        val newProjects: List<RexNode> = ArrayList(project.getProjects())
        val newCasts: List<RexNode> = ArrayList()
        for (fieldNumber in aggregate.getGroupSet()) {
            newCasts.add(
                rexBuilder.makeInputRef(
                    project.getProjects().get(fieldNumber).getType(), fieldNumber
                )
            )
        }
        for (aggregateCall in aggregate.getAggCallList()) {
            val newCall: AggregateCall? = transform(aggregateCall, project, newProjects)

            // Possibly CAST the new aggregator to an appropriate type.
            val i: Int = newCasts.size()
            val oldType: RelDataType = aggregate.getRowType().getFieldList().get(i).getType()
            if (newCall == null) {
                newCalls.add(aggregateCall)
                newCasts.add(rexBuilder.makeInputRef(oldType, i))
            } else {
                newCalls.add(newCall)
                newCasts.add(
                    rexBuilder.makeCast(
                        oldType,
                        rexBuilder.makeInputRef(newCall.getType(), i)
                    )
                )
            }
        }
        if (newCalls.equals(aggregate.getAggCallList())) {
            return
        }
        val relBuilder: RelBuilder = call.builder()
            .push(project.getInput())
            .project(newProjects)
        val groupKey: RelBuilder.GroupKey = relBuilder.groupKey(aggregate.getGroupSet(), aggregate.getGroupSets())
        relBuilder.aggregate(groupKey, newCalls)
            .convert(aggregate.getRowType(), false)
        call.transformTo(relBuilder.build())
        call.getPlanner().prune(aggregate)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): AggregateCaseToFilterRule? {
            return AggregateCaseToFilterRule(this)
        }

        companion object {
            val DEFAULT: Config = ImmutableAggregateCaseToFilterRule.Config.of()
                .withOperandSupplier { b0 ->
                    b0.operand(Aggregate::class.java).oneInput { b1 -> b1.operand(Project::class.java).anyInputs() }
                }
        }
    }

    companion object {
        @Nullable
        private fun transform(
            aggregateCall: AggregateCall,
            project: Project, newProjects: List<RexNode>
        ): AggregateCall? {
            val singleArg = soleArgument(aggregateCall)
            if (singleArg < 0) {
                return null
            }
            val rexNode: RexNode = project.getProjects().get(singleArg)
            if (!isThreeArgCase(rexNode)) {
                return null
            }
            val cluster: RelOptCluster = project.getCluster()
            val rexBuilder: RexBuilder = cluster.getRexBuilder()
            val caseCall: RexCall = rexNode as RexCall

            // If one arg is null and the other is not, reverse them and set "flip",
            // which negates the filter.
            val flip = (RexLiteral.isNullLiteral(caseCall.operands.get(1))
                    && !RexLiteral.isNullLiteral(caseCall.operands.get(2)))
            val arg1: RexNode = caseCall.operands.get(if (flip) 2 else 1)
            val arg2: RexNode = caseCall.operands.get(if (flip) 1 else 2)

            // Operand 1: Filter
            val op: SqlPostfixOperator = if (flip) SqlStdOperatorTable.IS_NOT_TRUE else SqlStdOperatorTable.IS_TRUE
            val filterFromCase: RexNode = rexBuilder.makeCall(op, caseCall.operands.get(0))

            // Combine the CASE filter with an honest-to-goodness SQL FILTER, if the
            // latter is present.
            val filter: RexNode
            filter = if (aggregateCall.filterArg >= 0) {
                rexBuilder.makeCall(
                    SqlStdOperatorTable.AND,
                    project.getProjects().get(aggregateCall.filterArg), filterFromCase
                )
            } else {
                filterFromCase
            }
            val kind: SqlKind = aggregateCall.getAggregation().getKind()
            if (aggregateCall.isDistinct()) {
                // Just one style supported:
                //   COUNT(DISTINCT CASE WHEN x = 'foo' THEN y END)
                // =>
                //   COUNT(DISTINCT y) FILTER(WHERE x = 'foo')
                if (kind === SqlKind.COUNT
                    && RexLiteral.isNullLiteral(arg2)
                ) {
                    newProjects.add(arg1)
                    newProjects.add(filter)
                    return AggregateCall.create(
                        SqlStdOperatorTable.COUNT, true, false,
                        false, ImmutableList.of(newProjects.size() - 2),
                        newProjects.size() - 1, null, RelCollations.EMPTY,
                        aggregateCall.getType(), aggregateCall.getName()
                    )
                }
                return null
            }

            // Four styles supported:
            //
            // A1: AGG(CASE WHEN x = 'foo' THEN cnt END)
            //   => operands (x = 'foo', cnt, null)
            // A2: SUM(CASE WHEN x = 'foo' THEN cnt ELSE 0 END)
            //   => operands (x = 'foo', cnt, 0); must be SUM
            // B: SUM(CASE WHEN x = 'foo' THEN 1 ELSE 0 END)
            //   => operands (x = 'foo', 1, 0); must be SUM
            // C: COUNT(CASE WHEN x = 'foo' THEN 'dummy' END)
            //   => operands (x = 'foo', 'dummy', null)
            return if (kind === SqlKind.COUNT // Case C
                && arg1.isA(SqlKind.LITERAL)
                && !RexLiteral.isNullLiteral(arg1)
                && RexLiteral.isNullLiteral(arg2)
            ) {
                newProjects.add(filter)
                AggregateCall.create(
                    SqlStdOperatorTable.COUNT, false, false,
                    false, ImmutableList.of(), newProjects.size() - 1, null,
                    RelCollations.EMPTY, aggregateCall.getType(),
                    aggregateCall.getName()
                )
            } else if (kind === SqlKind.SUM // Case B
                && isIntLiteral(arg1, BigDecimal.ONE)
                && isIntLiteral(arg2, BigDecimal.ZERO)
            ) {
                newProjects.add(filter)
                val typeFactory: RelDataTypeFactory = cluster.getTypeFactory()
                val dataType: RelDataType = typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(SqlTypeName.BIGINT), false
                )
                AggregateCall.create(
                    SqlStdOperatorTable.COUNT, false, false,
                    false, ImmutableList.of(), newProjects.size() - 1, null,
                    RelCollations.EMPTY, dataType, aggregateCall.getName()
                )
            } else if ((RexLiteral.isNullLiteral(arg2) // Case A1
                        && aggregateCall.getAggregation().allowsFilter())
                || (kind === SqlKind.SUM // Case A2
                        && isIntLiteral(arg2, BigDecimal.ZERO))
            ) {
                newProjects.add(arg1)
                newProjects.add(filter)
                AggregateCall.create(
                    aggregateCall.getAggregation(), false,
                    false, false, ImmutableList.of(newProjects.size() - 2),
                    newProjects.size() - 1, null, RelCollations.EMPTY,
                    aggregateCall.getType(), aggregateCall.getName()
                )
            } else {
                null
            }
        }

        /** Returns the argument, if an aggregate call has a single argument,
         * otherwise -1.  */
        private fun soleArgument(aggregateCall: AggregateCall): Int {
            return if (aggregateCall.getArgList().size() === 1) aggregateCall.getArgList().get(0) else -1
        }

        private fun isThreeArgCase(rexNode: RexNode): Boolean {
            return (rexNode.getKind() === SqlKind.CASE
                    && (rexNode as RexCall).operands.size() === 3)
        }

        private fun isIntLiteral(rexNode: RexNode, value: BigDecimal): Boolean {
            return (rexNode is RexLiteral
                    && SqlTypeName.INT_TYPES.contains(rexNode.getType().getSqlTypeName())
                    && value.equals((rexNode as RexLiteral).getValueAs(BigDecimal::class.java)))
        }
    }
}
