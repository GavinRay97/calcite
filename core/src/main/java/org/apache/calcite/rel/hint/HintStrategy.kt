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
package org.apache.calcite.rel.hint

import org.apache.calcite.plan.RelOptRule

/**
 * Represents a hint strategy entry of [HintStrategyTable].
 *
 *
 * A `HintStrategy` defines:
 *
 *
 *  * [HintPredicate]: tests whether a hint should apply to
 * a relational expression;
 *  * [HintOptionChecker]: validates the hint options;
 *  * `excludedRules`: rules to exclude when a relational expression
 * is going to apply a planner rule;
 *  * `converterRules`: fallback rules to apply when there are
 * no proper implementations after excluding the `excludedRules`.
 *
 *
 *
 * The [HintPredicate] is required, all the other items are optional.
 *
 *
 * [HintStrategy] is immutable.
 */
class HintStrategy private constructor(
    predicate: HintPredicate,
    @Nullable hintOptionChecker: HintOptionChecker?,
    excludedRules: ImmutableSet<RelOptRule>,
    converterRules: ImmutableSet<ConverterRule>
) {
    //~ Instance fields --------------------------------------------------------
    val predicate: HintPredicate

    @Nullable
    val hintOptionChecker: HintOptionChecker?
    val excludedRules: ImmutableSet<RelOptRule>
    val converterRules: ImmutableSet<ConverterRule>

    //~ Constructors -----------------------------------------------------------
    init {
        this.predicate = predicate
        this.hintOptionChecker = hintOptionChecker
        this.excludedRules = excludedRules
        this.converterRules = converterRules
    }
    //~ Inner Class ------------------------------------------------------------
    /** Builder for [HintStrategy].  */
    class Builder(predicate: HintPredicate) {
        private val predicate: HintPredicate

        @Nullable
        private var optionChecker: HintOptionChecker? = null
        private var excludedRules: ImmutableSet<RelOptRule>
        private var converterRules: ImmutableSet<ConverterRule>

        init {
            this.predicate = Objects.requireNonNull(predicate, "predicate")
            excludedRules = ImmutableSet.of()
            converterRules = ImmutableSet.of()
        }

        /** Registers a hint option checker to validate the hint options.  */
        fun optionChecker(optionChecker: HintOptionChecker?): Builder {
            this.optionChecker = Objects.requireNonNull(optionChecker, "optionChecker")
            return this
        }

        /**
         * Registers an array of rules to exclude during the
         * [org.apache.calcite.plan.RelOptPlanner] planning.
         *
         *
         * The desired converter rules work together with the excluded rules.
         * We have no validation here but they expect to have the same
         * function(semantic equivalent).
         *
         *
         * A rule fire cancels if:
         *
         *
         *  1. The registered [.excludedRules] contains the rule
         *  1. And the desired converter rules conversion is not possible
         * for the rule matched root node
         *
         *
         * @param rules excluded rules
         */
        fun excludedRules(vararg rules: RelOptRule?): Builder {
            excludedRules = ImmutableSet.copyOf(rules)
            return this
        }

        /**
         * Registers an array of desired converter rules during the
         * [org.apache.calcite.plan.RelOptPlanner] planning.
         *
         *
         * The desired converter rules work together with the excluded rules.
         * We have no validation here but they expect to have the same
         * function(semantic equivalent).
         *
         *
         * A rule fire cancels if:
         *
         *
         *  1. The registered [.excludedRules] contains the rule
         *  1. And the desired converter rules conversion is not possible
         * for the rule matched root node
         *
         *
         *
         * If no converter rules are specified, we assume the conversion is possible.
         *
         * @param rules desired converter rules
         */
        fun converterRules(vararg rules: ConverterRule?): Builder {
            converterRules = ImmutableSet.copyOf(rules)
            return this
        }

        fun build(): HintStrategy {
            return HintStrategy(predicate, optionChecker, excludedRules, converterRules)
        }
    }

    companion object {
        /**
         * Returns a [HintStrategy] builder with given hint predicate.
         *
         * @param hintPredicate hint predicate
         * @return [Builder] instance
         */
        fun builder(hintPredicate: HintPredicate): Builder {
            return Builder(hintPredicate)
        }
    }
}
