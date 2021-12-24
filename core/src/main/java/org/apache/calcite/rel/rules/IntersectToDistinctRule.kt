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

/**
 * Planner rule that translates a distinct
 * [org.apache.calcite.rel.core.Intersect]
 * (`all` = `false`)
 * into a group of operators composed of
 * [org.apache.calcite.rel.core.Union],
 * [org.apache.calcite.rel.core.Aggregate], etc.
 *
 *
 *  Rewrite: (GB-Union All-GB)-GB-UDTF (on all attributes)
 *
 * <h2>Example</h2>
 *
 *
 * Query: `R1 Intersect All R2`
 *
 *
 * `R3 = GB(R1 on all attributes, count(*) as c)<br></br>
 * union all<br></br>
 * GB(R2 on all attributes, count(*) as c)`
 *
 *
 * `R4 = GB(R3 on all attributes, count(c) as cnt, min(c) as m)`
 *
 *
 * Note that we do not need `min(c)` in intersect distinct.
 *
 *
 * `R5 = Filter(cnt == #branch)`
 *
 *
 * If it is intersect all then
 *
 *
 * `R6 = UDTF (R5) which will explode the tuples based on min(c)<br></br>
 * R7 = Project(R6 on all attributes)`
 *
 *
 * Else
 *
 *
 * `R6 = Proj(R5 on all attributes)`
 *
 * @see org.apache.calcite.rel.rules.UnionToDistinctRule
 *
 * @see CoreRules.INTERSECT_TO_DISTINCT
 */
@Value.Enclosing
class IntersectToDistinctRule
/** Creates an IntersectToDistinctRule.  */
protected constructor(config: Config?) : RelRule<IntersectToDistinctRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        intersectClass: Class<out Intersect?>?,
        relBuilderFactory: RelBuilderFactory?
    ) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .`as`(Config::class.java)
            .withOperandFor(intersectClass)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val intersect: Intersect = call.rel(0)
        if (intersect.all) {
            return  // nothing we can do
        }
        val cluster: RelOptCluster = intersect.getCluster()
        val rexBuilder: RexBuilder = cluster.getRexBuilder()
        val relBuilder: RelBuilder = call.builder()

        // 1st level GB: create a GB (col0, col1, count() as c) for each branch
        for (input in intersect.getInputs()) {
            relBuilder.push(input)
            relBuilder.aggregate(
                relBuilder.groupKey(relBuilder.fields()),
                relBuilder.countStar(null)
            )
        }

        // create a union above all the branches
        val branchCount: Int = intersect.getInputs().size()
        relBuilder.union(true, branchCount)
        val union: RelNode = relBuilder.peek()

        // 2nd level GB: create a GB (col0, col1, count(c)) for each branch
        // the index of c is union.getRowType().getFieldList().size() - 1
        val fieldCount: Int = union.getRowType().getFieldCount()
        val groupSet: ImmutableBitSet = ImmutableBitSet.range(fieldCount - 1)
        relBuilder.aggregate(
            relBuilder.groupKey(groupSet),
            relBuilder.countStar(null)
        )

        // add a filter count(c) = #branches
        relBuilder.filter(
            relBuilder.equals(
                relBuilder.field(fieldCount - 1),
                rexBuilder.makeBigintLiteral(BigDecimal(branchCount))
            )
        )

        // Project all but the last field
        relBuilder.project(Util.skipLast(relBuilder.fields()))

        // the schema for intersect distinct is like this
        // R3 on all attributes + count(c) as cnt
        // finally add a project to project out the last column
        call.transformTo(relBuilder.build())
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): IntersectToDistinctRule? {
            return IntersectToDistinctRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(intersectClass: Class<out Intersect?>?): Config? {
            return withOperandSupplier { b -> b.operand(intersectClass).anyInputs() }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableIntersectToDistinctRule.Config.of()
                .withOperandFor(LogicalIntersect::class.java)
        }
    }
}
