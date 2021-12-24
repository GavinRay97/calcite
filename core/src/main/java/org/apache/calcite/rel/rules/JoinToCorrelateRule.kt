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

import org.apache.calcite.plan.Contexts

/**
 * Rule that converts a [org.apache.calcite.rel.core.Join]
 * into a [org.apache.calcite.rel.logical.LogicalCorrelate], which can
 * then be implemented using nested loops.
 *
 *
 * For example,
 *
 * <blockquote>`select * from emp join dept on emp.deptno =
 * dept.deptno`</blockquote>
 *
 *
 * becomes a Correlator which restarts LogicalTableScan("DEPT") for each
 * row read from LogicalTableScan("EMP").
 *
 *
 * This rule is not applicable if for certain types of outer join. For
 * example,
 *
 * <blockquote>`select * from emp right join dept on emp.deptno =
 * dept.deptno`</blockquote>
 *
 *
 * would require emitting a NULL emp row if a certain department contained no
 * employees, and Correlator cannot do that.
 *
 * @see CoreRules.JOIN_TO_CORRELATE
 */
@Value.Enclosing
class JoinToCorrelateRule
/** Creates a JoinToCorrelateRule.  */
protected constructor(config: Config?) : RelRule<JoinToCorrelateRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(relBuilderFactory: RelBuilderFactory?) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(LogicalJoin::class.java)
    ) {
    }

    @Deprecated // to be removed before 2.0
    protected constructor(filterFactory: FilterFactory?) : this(
        Config.DEFAULT
            .withRelBuilderFactory(RelBuilder.proto(Contexts.of(filterFactory)))
            .`as`(Config::class.java)
            .withOperandFor(LogicalJoin::class.java)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun matches(call: RelOptRuleCall): Boolean {
        val join: Join = call.rel(0)
        return !join.getJoinType().generatesNullsOnLeft()
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        assert(matches(call))
        val join: Join = call.rel(0)
        val right: RelNode = join.getRight()
        val left: RelNode = join.getLeft()
        val leftFieldCount: Int = left.getRowType().getFieldCount()
        val cluster: RelOptCluster = join.getCluster()
        val rexBuilder: RexBuilder = cluster.getRexBuilder()
        val relBuilder: RelBuilder = call.builder()
        val correlationId: CorrelationId = cluster.createCorrel()
        val corrVar: RexNode = rexBuilder.makeCorrel(left.getRowType(), correlationId)
        val requiredColumns: ImmutableBitSet.Builder = ImmutableBitSet.builder()

        // Replace all references of left input with FieldAccess(corrVar, field)
        val joinCondition: RexNode = join.getCondition().accept(object : RexShuttle() {
            @Override
            fun visitInputRef(input: RexInputRef): RexNode {
                val field: Int = input.getIndex()
                if (field >= leftFieldCount) {
                    return rexBuilder.makeInputRef(
                        input.getType(),
                        input.getIndex() - leftFieldCount
                    )
                }
                requiredColumns.set(field)
                return rexBuilder.makeFieldAccess(corrVar, field)
            }
        })
        relBuilder.push(right).filter(joinCondition)
        val newRel: RelNode = LogicalCorrelate.create(
            left,
            relBuilder.build(),
            correlationId,
            requiredColumns.build(),
            join.getJoinType()
        )
        call.transformTo(newRel)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): JoinToCorrelateRule? {
            return JoinToCorrelateRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(joinClass: Class<out Join?>?): Config? {
            return withOperandSupplier { b -> b.operand(joinClass).anyInputs() }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableJoinToCorrelateRule.Config.of()
                .withOperandFor(LogicalJoin::class.java)
        }
    }
}
