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
 * UnionMergeRule implements the rule for combining two
 * non-distinct [org.apache.calcite.rel.core.SetOp]s
 * into a single [org.apache.calcite.rel.core.SetOp].
 *
 *
 * Originally written for [Union] (hence the name),
 * but now also applies to [Intersect] and [Minus].
 */
@Value.Enclosing
class UnionMergeRule
/** Creates a UnionMergeRule.  */
protected constructor(config: Config?) : RelRule<UnionMergeRule.Config?>(config), TransformationRule {
    @Deprecated // to be removed before 2.0
    constructor(
        setOpClass: Class<out SetOp?>?, description: String?,
        relBuilderFactory: RelBuilderFactory?
    ) : this(
        Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
            .withDescription(description)
            .`as`(Config::class.java)
            .withOperandFor(setOpClass)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        setOpClass: Class<out Union?>?,
        setOpFactory: RelFactories.SetOpFactory?
    ) : this(
        Config.DEFAULT.withRelBuilderFactory(RelBuilder.proto(setOpFactory))
            .`as`(Config::class.java)
            .withOperandFor(setOpClass)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun matches(call: RelOptRuleCall): Boolean {
        // It avoids adding the rule match to the match queue in case the rule is known to be a no-op
        val topOp: SetOp = call.rel(0)
        @SuppressWarnings("unchecked") val setOpClass: Class<out SetOp?> =
            operands.get(0).getMatchedClass() as Class<out SetOp?>
        val bottomOp: SetOp
        bottomOp = if (setOpClass.isInstance(call.rel(2))
            && !Minus::class.java.isAssignableFrom(setOpClass)
        ) {
            call.rel(2)
        } else if (setOpClass.isInstance(call.rel(1))) {
            call.rel(1)
        } else {
            return false
        }
        return if (topOp.all && !bottomOp.all) {
            false
        } else true
    }

    @Override
    fun onMatch(call: RelOptRuleCall) {
        val topOp: SetOp = call.rel(0)
        @SuppressWarnings("unchecked") val setOpClass: Class<out SetOp?> = operands.get(0).getMatchedClass() as Class

        // For Union and Intersect, we want to combine the set-op that's in the
        // second input first.
        //
        // For example, we reduce
        //    Union(Union(a, b), Union(c, d))
        // to
        //    Union(Union(a, b), c, d)
        // in preference to
        //    Union(a, b, Union(c, d))
        //
        // But for Minus, we can only reduce the left input. It is not valid to
        // reduce
        //    Minus(a, Minus(b, c))
        // to
        //    Minus(a, b, c)
        //
        // Hence, that's why the rule pattern matches on generic RelNodes rather
        // than explicit sub-classes of SetOp.  By doing so, and firing this rule
        // in a bottom-up order, it allows us to only specify a single
        // pattern for this rule.
        val bottomOp: SetOp
        bottomOp = if (setOpClass.isInstance(call.rel(2))
            && !Minus::class.java.isAssignableFrom(setOpClass)
        ) {
            call.rel(2)
        } else if (setOpClass.isInstance(call.rel(1))) {
            call.rel(1)
        } else {
            return
        }

        // Can only combine (1) if all operators are ALL,
        // or (2) top operator is DISTINCT (i.e. not ALL).
        // In case (2), all operators become DISTINCT.
        if (topOp.all && !bottomOp.all) {
            return
        }

        // Combine the inputs from the bottom set-op with the other inputs from
        // the top set-op.
        val relBuilder: RelBuilder = call.builder()
        if (setOpClass.isInstance(call.rel(2))
            && !Minus::class.java.isAssignableFrom(setOpClass)
        ) {
            relBuilder.push(topOp.getInput(0))
            relBuilder.pushAll(bottomOp.getInputs())
            // topOp.getInputs().size() may be more than 2
            for (index in 2 until topOp.getInputs().size()) {
                relBuilder.push(topOp.getInput(index))
            }
        } else {
            relBuilder.pushAll(bottomOp.getInputs())
            relBuilder.pushAll(Util.skip(topOp.getInputs()))
        }
        val n: Int = (bottomOp.getInputs().size()
                + topOp.getInputs().size()
                - 1)
        if (topOp is Union) {
            relBuilder.union(topOp.all, n)
        } else if (topOp is Intersect) {
            relBuilder.intersect(topOp.all, n)
        } else if (topOp is Minus) {
            relBuilder.minus(topOp.all, n)
        }
        call.transformTo(relBuilder.build())
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): UnionMergeRule? {
            return UnionMergeRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(setOpClass: Class<out RelNode?>?): Config? {
            return withOperandSupplier { b0 ->
                b0.operand(setOpClass).inputs(
                    { b1 -> b1.operand(RelNode::class.java).anyInputs() }
                ) { b2 -> b2.operand(RelNode::class.java).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableUnionMergeRule.Config.of()
                .withDescription("UnionMergeRule")
                .withOperandFor(LogicalUnion::class.java)
            val INTERSECT: Config = ImmutableUnionMergeRule.Config.of()
                .withDescription("IntersectMergeRule")
                .withOperandFor(LogicalIntersect::class.java)
            val MINUS: Config = ImmutableUnionMergeRule.Config.of()
                .withDescription("MinusMergeRule")
                .withOperandFor(LogicalMinus::class.java)
        }
    }
}
