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

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.RelRule
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.SingleRel
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rel.core.Join
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.core.Values
import org.apache.calcite.rel.logical.LogicalIntersect
import org.apache.calcite.rel.logical.LogicalMinus
import org.apache.calcite.rel.logical.LogicalUnion
import org.apache.calcite.rel.logical.LogicalValues
import org.apache.calcite.rex.RexDynamicParam
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.tools.RelBuilderFactory
import org.immutables.value.Value
import java.util.Collections
import java.util.List
import java.util.function.Predicate

/**
 * Collection of rules which remove sections of a query plan known never to
 * produce any rows.
 *
 *
 * Conventionally, the way to represent an empty relational expression is
 * with a [Values] that has no tuples.
 *
 * @see LogicalValues.createEmpty
 */
object PruneEmptyRules {
    /**
     * Rule that removes empty children of a
     * [org.apache.calcite.rel.logical.LogicalUnion].
     *
     *
     * Examples:
     *
     *
     *  * Union(Rel, Empty, Rel2) becomes Union(Rel, Rel2)
     *  * Union(Rel, Empty, Empty) becomes Rel
     *  * Union(Empty, Empty) becomes Empty
     *
     */
    val UNION_INSTANCE: RelOptRule = ImmutableUnionEmptyPruneRuleConfig.of()
        .withOperandSupplier { b0 ->
            b0.operand(LogicalUnion::class.java).unorderedInputs { b1 ->
                b1.operand(Values::class.java)
                    .predicate(Values::isEmpty).noInputs()
            }
        }
        .withDescription("Union")
        .toRule()

    /**
     * Rule that removes empty children of a
     * [org.apache.calcite.rel.logical.LogicalMinus].
     *
     *
     * Examples:
     *
     *
     *  * Minus(Rel, Empty, Rel2) becomes Minus(Rel, Rel2)
     *  * Minus(Empty, Rel) becomes Empty
     *
     */
    val MINUS_INSTANCE: RelOptRule = ImmutableMinusEmptyPruneRuleConfig.of()
        .withOperandSupplier { b0 ->
            b0.operand(LogicalMinus::class.java).unorderedInputs { b1 ->
                b1.operand(Values::class.java).predicate(Values::isEmpty)
                    .noInputs()
            }
        }
        .withDescription("Minus")
        .toRule()

    /**
     * Rule that converts a
     * [org.apache.calcite.rel.logical.LogicalIntersect] to
     * empty if any of its children are empty.
     *
     *
     * Examples:
     *
     *
     *  * Intersect(Rel, Empty, Rel2) becomes Empty
     *  * Intersect(Empty, Rel) becomes Empty
     *
     */
    val INTERSECT_INSTANCE: RelOptRule = ImmutableIntersectEmptyPruneRuleConfig.of()
        .withOperandSupplier { b0 ->
            b0.operand(LogicalIntersect::class.java).unorderedInputs { b1 ->
                b1.operand(Values::class.java).predicate(Values::isEmpty)
                    .noInputs()
            }
        }
        .withDescription("Intersect")
        .toRule()

    private fun isEmpty(node: RelNode): Boolean {
        if (node is Values) {
            return (node as Values).getTuples().isEmpty()
        }
        if (node is HepRelVertex) {
            return isEmpty((node as HepRelVertex).getCurrentRel())
        }
        // Note: relation input might be a RelSubset, so we just iterate over the relations
        // in order to check if the subset is equivalent to an empty relation.
        if (node !is RelSubset) {
            return false
        }
        val subset: RelSubset = node as RelSubset
        for (rel in subset.getRels()) {
            if (isEmpty(rel)) {
                return true
            }
        }
        return false
    }

    /**
     * Rule that converts a [org.apache.calcite.rel.logical.LogicalProject]
     * to empty if its child is empty.
     *
     *
     * Examples:
     *
     *
     *  * Project(Empty) becomes Empty
     *
     */
    val PROJECT_INSTANCE: RelOptRule = ImmutableRemoveEmptySingleRuleConfig.of()
        .withDescription("PruneEmptyProject")
        .withOperandFor(Project::class.java) { project -> true }
        .toRule()

    /**
     * Rule that converts a [org.apache.calcite.rel.logical.LogicalFilter]
     * to empty if its child is empty.
     *
     *
     * Examples:
     *
     *
     *  * Filter(Empty) becomes Empty
     *
     */
    val FILTER_INSTANCE: RelOptRule = ImmutableRemoveEmptySingleRuleConfig.of()
        .withDescription("PruneEmptyFilter")
        .withOperandFor(Filter::class.java) { singleRel -> true }
        .toRule()

    /**
     * Rule that converts a [org.apache.calcite.rel.core.Sort]
     * to empty if its child is empty.
     *
     *
     * Examples:
     *
     *
     *  * Sort(Empty) becomes Empty
     *
     */
    val SORT_INSTANCE: RelOptRule = ImmutableRemoveEmptySingleRuleConfig.of()
        .withDescription("PruneEmptySort")
        .withOperandFor(Sort::class.java) { singleRel -> true }
        .toRule()

    /**
     * Rule that converts a [org.apache.calcite.rel.core.Sort]
     * to empty if it has `LIMIT 0`.
     *
     *
     * Examples:
     *
     *
     *  * Sort[fetch=0] becomes Empty
     *
     */
    val SORT_FETCH_ZERO_INSTANCE: RelOptRule = ImmutableSortFetchZeroRuleConfig.of()
        .withOperandSupplier { b -> b.operand(Sort::class.java).anyInputs() }
        .withDescription("PruneSortLimit0")
        .toRule()

    /**
     * Rule that converts an [org.apache.calcite.rel.core.Aggregate]
     * to empty if its child is empty.
     *
     *
     * Examples:
     *
     *
     *  * `Aggregate(key: [1, 3], Empty)`  `Empty`
     *
     *  * `Aggregate(key: [], Empty)` is unchanged, because an aggregate
     * without a GROUP BY key always returns 1 row, even over empty input
     *
     *
     * @see AggregateValuesRule
     */
    val AGGREGATE_INSTANCE: RelOptRule = ImmutableRemoveEmptySingleRuleConfig.of()
        .withDescription("PruneEmptyAggregate")
        .withOperandFor(Aggregate::class.java, Aggregate::isNotGrandTotal)
        .toRule()

    /**
     * Rule that converts a [org.apache.calcite.rel.core.Join]
     * to empty if its left child is empty.
     *
     *
     * Examples:
     *
     *
     *  * Join(Empty, Scan(Dept), INNER) becomes Empty
     *  * Join(Empty, Scan(Dept), LEFT) becomes Empty
     *  * Join(Empty, Scan(Dept), SEMI) becomes Empty
     *  * Join(Empty, Scan(Dept), ANTI) becomes Empty
     *
     */
    val JOIN_LEFT_INSTANCE: RelOptRule = ImmutableJoinLeftEmptyRuleConfig.of()
        .withOperandSupplier { b0 ->
            b0.operand(Join::class.java).inputs(
                { b1 ->
                    b1.operand(Values::class.java)
                        .predicate(Values::isEmpty).noInputs()
                }
            ) { b2 -> b2.operand(RelNode::class.java).anyInputs() }
        }
        .withDescription("PruneEmptyJoin(left)")
        .toRule()

    /**
     * Rule that converts a [org.apache.calcite.rel.core.Join]
     * to empty if its right child is empty.
     *
     *
     * Examples:
     *
     *
     *  * Join(Scan(Emp), Empty, INNER) becomes Empty
     *  * Join(Scan(Emp), Empty, RIGHT) becomes Empty
     *  * Join(Scan(Emp), Empty, SEMI) becomes Empty
     *  * Join(Scan(Emp), Empty, ANTI) becomes Scan(Emp)
     *
     */
    val JOIN_RIGHT_INSTANCE: RelOptRule = ImmutableJoinRightEmptyRuleConfig.of()
        .withOperandSupplier { b0 ->
            b0.operand(Join::class.java).inputs(
                { b1 -> b1.operand(RelNode::class.java).anyInputs() }
            ) { b2 ->
                b2.operand(Values::class.java).predicate(Values::isEmpty)
                    .noInputs()
            }
        }
        .withDescription("PruneEmptyJoin(right)")
        .toRule()
    //~ Static fields/initializers ---------------------------------------------
    /**
     * Abstract prune empty rule that implements SubstitutionRule interface.
     */
    protected abstract class PruneEmptyRule protected constructor(config: Config?) :
        RelRule<PruneEmptyRule.Config?>(config), SubstitutionRule {
        @Override
        fun autoPruneOld(): Boolean {
            return true
        }

        /** Rule configuration.  */
        interface Config : RelRule.Config {
            @Override
            fun toRule(): PruneEmptyRule?
        }
    }

    /** Planner rule that converts a single-rel (e.g. project, sort, aggregate or
     * filter) on top of the empty relational expression into empty.  */
    class RemoveEmptySingleRule
    /** Creates a RemoveEmptySingleRule.  */
    internal constructor(config: RemoveEmptySingleRuleConfig?) : PruneEmptyRule(config) {
        @Deprecated // to be removed before 2.0
        constructor(
            clazz: Class<R?>?,
            description: String?
        ) : this(ImmutableRemoveEmptySingleRuleConfig.of().withDescription(description)
            .`as`(ImmutableRemoveEmptySingleRuleConfig::class.java)
            .withOperandFor(clazz) { singleRel -> true }) {
        }

        @Deprecated // to be removed before 2.0
        constructor(
            clazz: Class<R?>?,
            predicate: Predicate<R?>?, relBuilderFactory: RelBuilderFactory?,
            description: String?
        ) : this(
            ImmutableRemoveEmptySingleRuleConfig.of().withRelBuilderFactory(relBuilderFactory)
                .withDescription(description)
                .`as`(ImmutableRemoveEmptySingleRuleConfig::class.java)
                .withOperandFor(clazz, predicate)
        ) {
        }

        @SuppressWarnings(["Guava", "UnnecessaryMethodReference"])
        @Deprecated // to be removed before 2.0
        constructor(
            clazz: Class<R?>?,
            predicate: com.google.common.base.Predicate<R?>?,
            relBuilderFactory: RelBuilderFactory?, description: String?
        ) : this(
            ImmutableRemoveEmptySingleRuleConfig.of().withRelBuilderFactory(relBuilderFactory)
                .withDescription(description)
                .`as`(ImmutableRemoveEmptySingleRuleConfig::class.java)
                .withOperandFor(clazz, predicate::apply)
        ) {
        }

        @Override
        fun onMatch(call: RelOptRuleCall) {
            val singleRel: SingleRel = call.rel(0)
            var emptyValues: RelNode = call.builder().push(singleRel).empty().build()
            var traits: RelTraitSet = singleRel.getTraitSet()
            // propagate all traits (except convention) from the original singleRel into the empty values
            if (emptyValues.getConvention() != null) {
                traits = traits.replace(emptyValues.getConvention())
            }
            emptyValues = emptyValues.copy(traits, Collections.emptyList())
            call.transformTo(emptyValues)
        }

        /** Rule configuration.  */
        @Value.Immutable
        interface RemoveEmptySingleRuleConfig : Config {
            @Override
            override fun toRule(): RemoveEmptySingleRule {
                return RemoveEmptySingleRule(this)
            }

            /** Defines an operand tree for the given classes.  */
            fun <R : RelNode?> withOperandFor(
                relClass: Class<R>?,
                predicate: Predicate<R>?
            ): RemoveEmptySingleRuleConfig? {
                return withOperandSupplier { b0 ->
                    b0.operand(relClass).predicate(predicate).oneInput { b1 ->
                        b1.operand(
                            Values::class.java
                        ).predicate(Values::isEmpty).noInputs()
                    }
                }
                    .`as`(RemoveEmptySingleRuleConfig::class.java)
            }
        }
    }

    /** Configuration for a rule that prunes empty inputs from a Minus.  */
    @Value.Immutable
    interface UnionEmptyPruneRuleConfig : PruneEmptyRule.Config {
        @Override
        override fun toRule(): PruneEmptyRule {
            return object : PruneEmptyRule(this) {
                @Override
                fun onMatch(call: RelOptRuleCall) {
                    val union: LogicalUnion = call.rel(0)
                    val inputs: List<RelNode> = union.getInputs()
                    assert(inputs != null)
                    val builder: RelBuilder = call.builder()
                    var nonEmptyInputs = 0
                    for (input in inputs) {
                        if (!isEmpty(input)) {
                            builder.push(input)
                            nonEmptyInputs++
                        }
                    }
                    assert(nonEmptyInputs < inputs.size()) {
                        ("planner promised us at least one Empty child: "
                                + RelOptUtil.toString(union))
                    }
                    if (nonEmptyInputs == 0) {
                        builder.push(union).empty()
                    } else {
                        builder.union(union.all, nonEmptyInputs)
                        builder.convert(union.getRowType(), true)
                    }
                    call.transformTo(builder.build())
                }
            }
        }
    }

    /** Configuration for a rule that prunes empty inputs from a Minus.  */
    @Value.Immutable
    interface MinusEmptyPruneRuleConfig : PruneEmptyRule.Config {
        @Override
        override fun toRule(): PruneEmptyRule {
            return object : PruneEmptyRule(this) {
                @Override
                fun onMatch(call: RelOptRuleCall) {
                    val minus: LogicalMinus = call.rel(0)
                    val inputs: List<RelNode> = minus.getInputs()
                    assert(inputs != null)
                    var nonEmptyInputs = 0
                    val builder: RelBuilder = call.builder()
                    for (input in inputs) {
                        if (!isEmpty(input)) {
                            builder.push(input)
                            nonEmptyInputs++
                        } else if (nonEmptyInputs == 0) {
                            // If the first input of Minus is empty, the whole thing is
                            // empty.
                            break
                        }
                    }
                    assert(nonEmptyInputs < inputs.size()) {
                        ("planner promised us at least one Empty child: "
                                + RelOptUtil.toString(minus))
                    }
                    if (nonEmptyInputs == 0) {
                        builder.push(minus).empty()
                    } else {
                        builder.minus(minus.all, nonEmptyInputs)
                        builder.convert(minus.getRowType(), true)
                    }
                    call.transformTo(builder.build())
                }
            }
        }
    }

    /** Configuration for a rule that prunes an Intersect if any of its inputs
     * is empty.  */
    @Value.Immutable
    interface IntersectEmptyPruneRuleConfig : PruneEmptyRule.Config {
        @Override
        override fun toRule(): PruneEmptyRule {
            return object : PruneEmptyRule(this) {
                @Override
                fun onMatch(call: RelOptRuleCall) {
                    val intersect: LogicalIntersect = call.rel(0)
                    val builder: RelBuilder = call.builder()
                    builder.push(intersect).empty()
                    call.transformTo(builder.build())
                }
            }
        }
    }

    /** Configuration for a rule that prunes a Sort if it has limit 0.  */
    @Value.Immutable
    interface SortFetchZeroRuleConfig : PruneEmptyRule.Config {
        @Override
        override fun toRule(): PruneEmptyRule {
            return object : PruneEmptyRule(this) {
                @Override
                fun onMatch(call: RelOptRuleCall) {
                    val sort: Sort = call.rel(0)
                    if (sort.fetch != null && sort.fetch !is RexDynamicParam
                        && RexLiteral.intValue(sort.fetch) === 0
                    ) {
                        var emptyValues: RelNode = call.builder().push(sort).empty().build()
                        var traits: RelTraitSet = sort.getTraitSet()
                        // propagate all traits (except convention) from the original sort into the empty values
                        if (emptyValues.getConvention() != null) {
                            traits = traits.replace(emptyValues.getConvention())
                        }
                        emptyValues = emptyValues.copy(traits, Collections.emptyList())
                        call.transformTo(emptyValues)
                    }
                }
            }
        }
    }

    /** Configuration for rule that prunes a join it its left input is
     * empty.  */
    @Value.Immutable
    interface JoinLeftEmptyRuleConfig : PruneEmptyRule.Config {
        @Override
        override fun toRule(): PruneEmptyRule {
            return object : PruneEmptyRule(this) {
                @Override
                fun onMatch(call: RelOptRuleCall) {
                    val join: Join = call.rel(0)
                    if (join.getJoinType().generatesNullsOnLeft()) {
                        // "select * from emp right join dept" is not necessarily empty if
                        // emp is empty
                        return
                    }
                    call.transformTo(call.builder().push(join).empty().build())
                }
            }
        }
    }

    /** Configuration for rule that prunes a join it its right input is
     * empty.  */
    @Value.Immutable
    interface JoinRightEmptyRuleConfig : PruneEmptyRule.Config {
        @Override
        override fun toRule(): PruneEmptyRule {
            return object : PruneEmptyRule(this) {
                @Override
                fun onMatch(call: RelOptRuleCall) {
                    val join: Join = call.rel(0)
                    if (join.getJoinType().generatesNullsOnRight()) {
                        // "select * from emp left join dept" is not necessarily empty if
                        // dept is empty
                        return
                    }
                    if (join.getJoinType() === JoinRelType.ANTI) {
                        // In case of anti join: Join(X, Empty, ANTI) becomes X
                        call.transformTo(join.getLeft())
                        return
                    }
                    call.transformTo(call.builder().push(join).empty().build())
                }
            }
        }
    }
}
