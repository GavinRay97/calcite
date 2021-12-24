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

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelRule
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.core.Values
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilderFactory
import org.apache.calcite.util.Util
import com.google.common.collect.ImmutableList
import org.immutables.value.Value
import java.math.BigDecimal
import java.util.ArrayList
import java.util.List

/**
 * Rule that applies [Aggregate] to a [Values] (currently just an
 * empty `Value`s).
 *
 *
 * This is still useful because [PruneEmptyRules.AGGREGATE_INSTANCE]
 * doesn't handle `Aggregate`, which is in turn because `Aggregate`
 * of empty relations need some special handling: a single row will be
 * generated, where each column's value depends on the specific aggregate calls
 * (e.g. COUNT is 0, SUM is NULL).
 *
 *
 * Sample query where this matters:
 *
 * <blockquote>`SELECT COUNT(*) FROM s.foo WHERE 1 = 0`</blockquote>
 *
 *
 * This rule only applies to "grand totals", that is, `GROUP BY ()`.
 * Any non-empty `GROUP BY` clause will return one row per group key
 * value, and each group will consist of at least one row.
 *
 * @see CoreRules.AGGREGATE_VALUES
 */
@Value.Enclosing
class AggregateValuesRule
/** Creates an AggregateValuesRule.  */
protected constructor(config: Config?) : RelRule<AggregateValuesRule.Config?>(config), SubstitutionRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT
            .withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
    ) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val aggregate: Aggregate = call.rel(0)
        val values: Values = call.rel(1)
        Util.discard(values)
        val relBuilder: RelBuilder = call.builder()
        val rexBuilder: RexBuilder = relBuilder.getRexBuilder()
        val literals: List<RexLiteral> = ArrayList()
        for (aggregateCall in aggregate.getAggCallList()) {
            when (aggregateCall.getAggregation().getKind()) {
                COUNT, SUM0 -> literals.add(
                    rexBuilder.makeLiteral(BigDecimal.ZERO, aggregateCall.getType())
                )
                MIN, MAX, SUM -> literals.add(rexBuilder.makeNullLiteral(aggregateCall.getType()))
                else ->         // Unknown what this aggregate call should do on empty Values. Bail out to be safe.
                    return
            }
        }
        call.transformTo(
            relBuilder.values(ImmutableList.of(literals), aggregate.getRowType())
                .build()
        )

        // New plan is absolutely better than old plan.
        call.getPlanner().prune(aggregate)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): AggregateValuesRule? {
            return AggregateValuesRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            aggregateClass: Class<out Aggregate?>?,
            valuesClass: Class<out Values?>?
        ): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(aggregateClass)
                    .predicate { aggregate -> aggregate.getGroupCount() === 0 }
                    .oneInput { b1 ->
                        b1.operand(valuesClass)
                            .predicate { values -> values.getTuples().isEmpty() }
                            .noInputs()
                    }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableAggregateValuesRule.Config.of()
                .withOperandFor(Aggregate::class.java, Values::class.java)
        }
    }
}
