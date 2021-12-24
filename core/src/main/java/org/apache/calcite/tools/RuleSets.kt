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
package org.apache.calcite.tools

import org.apache.calcite.plan.RelOptRule

/**
 * Utilities for creating and composing rule sets.
 *
 * @see org.apache.calcite.tools.RuleSet
 */
object RuleSets {
    /** Creates a rule set with a given array of rules.  */
    fun ofList(vararg rules: RelOptRule?): RuleSet {
        return ListRuleSet(ImmutableList.copyOf(rules))
    }

    /** Creates a rule set with a given collection of rules.  */
    fun ofList(rules: Iterable<RelOptRule?>?): RuleSet {
        return ListRuleSet(ImmutableList.copyOf(rules))
    }

    /** Rule set that consists of a list of rules.  */
    private class ListRuleSet internal constructor(rules: ImmutableList<RelOptRule?>) : RuleSet {
        private val rules: ImmutableList<RelOptRule>

        init {
            this.rules = rules
        }

        @Override
        override fun hashCode(): Int {
            return rules.hashCode()
        }

        @Override
        override fun equals(@Nullable obj: Object): Boolean {
            return (obj === this
                    || obj is ListRuleSet
                    && rules.equals((obj as ListRuleSet).rules))
        }

        @Override
        override operator fun iterator(): Iterator<RelOptRule> {
            return rules.iterator()
        }
    }
}
