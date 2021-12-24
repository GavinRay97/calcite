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

/**
 * Planner rule that pushes down expressions in "equal" join condition.
 *
 *
 * For example, given
 * "emp JOIN dept ON emp.deptno + 1 = dept.deptno", adds a project above
 * "emp" that computes the expression
 * "emp.deptno + 1". The resulting join condition is a simple combination
 * of AND, equals, and input fields, plus the remaining non-equal conditions.
 *
 * @see CoreRules.JOIN_PUSH_EXPRESSIONS
 */
@Value.Enclosing
class JoinPushExpressionsRule
/** Creates a JoinPushExpressionsRule.  */
protected constructor(config: Config?) : RelRule<JoinPushExpressionsRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        joinClass: Class<out Join?>?,
        relBuilderFactory: RelBuilderFactory?
    ) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(joinClass)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        joinClass: Class<out Join?>?,
        projectFactory: RelFactories.ProjectFactory?
    ) : this(
        Config.DEFAULT.withRelBuilderFactory(RelBuilder.proto(projectFactory))
            .`as`(Config::class.java)
            .withOperandFor(joinClass)
    ) {
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val join: Join = call.rel(0)

        // Push expression in join condition into Project below Join.
        val newJoin: RelNode = RelOptUtil.pushDownJoinConditions(join, call.builder())

        // If the join is the same, we bail out
        if (newJoin is Join) {
            val newCondition: RexNode = (newJoin as Join).getCondition()
            if (join.getCondition().equals(newCondition)) {
                return
            }
        }
        call.transformTo(newJoin)
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): JoinPushExpressionsRule? {
            return JoinPushExpressionsRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(joinClass: Class<out Join?>?): Config? {
            return withOperandSupplier { b -> b.operand(joinClass).anyInputs() }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableJoinPushExpressionsRule.Config.of()
                .withOperandFor(Join::class.java)
        }
    }
}
