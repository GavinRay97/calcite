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
package org.apache.calcite.plan.hep

import org.apache.calcite.plan.RelOptRule
import org.checkerframework.checker.nullness.qual.MonotonicNonNull
import java.util.Collection
import java.util.HashSet
import java.util.Set

/**
 * HepInstruction represents one instruction in a HepProgram. The actual
 * instruction set is defined here via inner classes; if these grow too big,
 * they should be moved out to top-level classes.
 */
abstract class HepInstruction {
    //~ Methods ----------------------------------------------------------------
    fun initialize(clearCache: Boolean) {}

    // typesafe dispatch via the visitor pattern
    abstract fun execute(planner: HepPlanner?)
    //~ Inner Classes ----------------------------------------------------------
    /** Instruction that executes all rules of a given class.
     *
     * @param <R> rule type
    </R> */
    class RuleClass<R : RelOptRule?> : HepInstruction() {
        @Nullable
        var ruleClass: Class<R>? = null

        /**
         * Actual rule set instantiated during planning by filtering all of the
         * planner's rules through ruleClass.
         */
        @Nullable
        var ruleSet: Set<RelOptRule>? = null
        @Override
        override fun initialize(clearCache: Boolean) {
            if (!clearCache) {
                return
            }
            ruleSet = null
        }

        @Override
        override fun execute(planner: HepPlanner) {
            planner.executeInstruction(this)
        }
    }

    /** Instruction that executes all rules in a given collection.  */
    class RuleCollection : HepInstruction() {
        /**
         * Collection of rules to apply.
         */
        @Nullable
        var rules: Collection<RelOptRule>? = null
        @Override
        override fun execute(planner: HepPlanner) {
            planner.executeInstruction(this)
        }
    }

    /** Instruction that executes converter rules.  */
    class ConverterRules : HepInstruction() {
        var guaranteed = false

        /**
         * Actual rule set instantiated during planning by filtering all of the
         * planner's rules, looking for the desired converters.
         */
        @MonotonicNonNull
        var ruleSet: Set<RelOptRule>? = null
        @Override
        override fun execute(planner: HepPlanner) {
            planner.executeInstruction(this)
        }
    }

    /** Instruction that finds common relational sub-expressions.  */
    class CommonRelSubExprRules : HepInstruction() {
        @Nullable
        var ruleSet: Set<RelOptRule>? = null
        @Override
        override fun execute(planner: HepPlanner) {
            planner.executeInstruction(this)
        }
    }

    /** Instruction that executes a given rule.  */
    class RuleInstance : HepInstruction() {
        /**
         * Description to look for, or null if rule specified explicitly.
         */
        @Nullable
        var ruleDescription: String? = null

        /**
         * Explicitly specified rule, or rule looked up by planner from
         * description.
         */
        @Nullable
        var rule: RelOptRule? = null
        @Override
        override fun initialize(clearCache: Boolean) {
            if (!clearCache) {
                return
            }
            if (ruleDescription != null) {
                // Look up anew each run.
                rule = null
            }
        }

        @Override
        override fun execute(planner: HepPlanner) {
            planner.executeInstruction(this)
        }
    }

    /** Instruction that sets match order.  */
    class MatchOrder : HepInstruction() {
        @Nullable
        var order: HepMatchOrder? = null
        @Override
        override fun execute(planner: HepPlanner) {
            planner.executeInstruction(this)
        }
    }

    /** Instruction that sets match limit.  */
    class MatchLimit : HepInstruction() {
        var limit = 0
        @Override
        override fun execute(planner: HepPlanner) {
            planner.executeInstruction(this)
        }
    }

    /** Instruction that executes a sub-program.  */
    class Subprogram : HepInstruction() {
        @Nullable
        var subprogram: HepProgram? = null
        @Override
        override fun initialize(clearCache: Boolean) {
            if (subprogram != null) {
                subprogram.initialize(clearCache)
            }
        }

        @Override
        override fun execute(planner: HepPlanner) {
            planner.executeInstruction(this)
        }
    }

    /** Instruction that begins a group.  */
    class BeginGroup : HepInstruction() {
        @Nullable
        var endGroup: EndGroup? = null
        @Override
        override fun initialize(clearCache: Boolean) {
        }

        @Override
        override fun execute(planner: HepPlanner) {
            planner.executeInstruction(this)
        }
    }

    /** Instruction that ends a group.  */
    class EndGroup : HepInstruction() {
        /**
         * Actual rule set instantiated during planning by collecting grouped
         * rules.
         */
        @Nullable
        var ruleSet: Set<RelOptRule>? = null
        var collecting = false
        @Override
        override fun initialize(clearCache: Boolean) {
            if (!clearCache) {
                return
            }
            ruleSet = HashSet()
            collecting = true
        }

        @Override
        override fun execute(planner: HepPlanner) {
            planner.executeInstruction(this)
        }
    }
}
