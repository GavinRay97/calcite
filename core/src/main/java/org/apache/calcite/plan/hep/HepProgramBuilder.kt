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

import org.apache.calcite.plan.CommonRelSubExprRule
import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.plan.RelOptRule
import java.util.ArrayList
import java.util.Collection
import java.util.List
import java.util.Objects.requireNonNull

/**
 * HepProgramBuilder creates instances of [HepProgram].
 */
class HepProgramBuilder  //~ Constructors -----------------------------------------------------------
/**
 * Creates a new HepProgramBuilder with an initially empty program. The
 * program under construction has an initial match order of
 * [HepMatchOrder.DEPTH_FIRST], and an initial match limit of
 * [HepProgram.MATCH_UNTIL_FIXPOINT].
 */
{
    //~ Instance fields --------------------------------------------------------
    private val instructions: List<HepInstruction> = ArrayList()
    private var group: @Nullable HepInstruction.BeginGroup? = null

    //~ Methods ----------------------------------------------------------------
    private fun clear() {
        instructions.clear()
        group = null
    }

    /**
     * Adds an instruction to attempt to match any rules of a given class. The
     * order in which the rules within a class will be attempted is arbitrary,
     * so if more control is needed, use addRuleInstance instead.
     *
     *
     * Note that when this method is used, it is also necessary to add the
     * actual rule objects of interest to the planner via
     * [RelOptPlanner.addRule]. If the planner does not have any
     * rules of the given class, this instruction is a nop.
     *
     *
     * TODO: support classification via rule annotations.
     *
     * @param ruleClass class of rules to fire, e.g. ConverterRule.class
     */
    fun <R : RelOptRule?> addRuleClass(
        ruleClass: Class<R>
    ): HepProgramBuilder {
        val instruction: HepInstruction.RuleClass = RuleClass<R>()
        instruction.ruleClass = ruleClass
        instructions.add(instruction)
        return this
    }

    /**
     * Adds an instruction to attempt to match any rules in a given collection.
     * The order in which the rules within a collection will be attempted is
     * arbitrary, so if more control is needed, use addRuleInstance instead. The
     * collection can be "live" in the sense that not all rule instances need to
     * have been added to it at the time this method is called. The collection
     * contents are reevaluated for each execution of the program.
     *
     *
     * Note that when this method is used, it is NOT necessary to add the
     * rules to the planner via [RelOptPlanner.addRule]; the instances
     * supplied here will be used. However, adding the rules to the planner
     * redundantly is good form since other planners may require it.
     *
     * @param rules collection of rules to fire
     */
    fun addRuleCollection(rules: Collection<RelOptRule>?): HepProgramBuilder {
        val instruction: HepInstruction.RuleCollection = RuleCollection()
        instruction.rules = rules
        instructions.add(instruction)
        return this
    }

    /**
     * Adds an instruction to attempt to match a specific rule object.
     *
     *
     * Note that when this method is used, it is NOT necessary to add the
     * rule to the planner via [RelOptPlanner.addRule]; the instance
     * supplied here will be used. However, adding the rule to the planner
     * redundantly is good form since other planners may require it.
     *
     * @param rule rule to fire
     */
    fun addRuleInstance(rule: RelOptRule?): HepProgramBuilder {
        val instruction: HepInstruction.RuleInstance = RuleInstance()
        instruction.rule = requireNonNull(rule, "rule")
        instructions.add(instruction)
        return this
    }

    /**
     * Adds an instruction to attempt to match a specific rule identified by its
     * unique description.
     *
     *
     * Note that when this method is used, it is necessary to also add the
     * rule object of interest to the planner via [RelOptPlanner.addRule].
     * This allows for some decoupling between optimizers and plugins: the
     * optimizer only knows about rule descriptions, while the plugins supply
     * the actual instances. If the planner does not have a rule matching the
     * description, this instruction is a nop.
     *
     * @param ruleDescription description of rule to fire
     */
    fun addRuleByDescription(ruleDescription: String?): HepProgramBuilder {
        val instruction: HepInstruction.RuleInstance = RuleInstance()
        instruction.ruleDescription = ruleDescription
        instructions.add(instruction)
        return this
    }

    /**
     * Adds an instruction to begin a group of rules. All subsequent rules added
     * (until the next endRuleGroup) will be collected into the group rather
     * than firing individually. After addGroupBegin has been called, only
     * addRuleXXX methods may be called until the next addGroupEnd.
     */
    fun addGroupBegin(): HepProgramBuilder {
        assert(group == null)
        val instruction: HepInstruction.BeginGroup = BeginGroup()
        instructions.add(instruction)
        group = instruction
        return this
    }

    /**
     * Adds an instruction to end a group of rules, firing the group
     * collectively. The order in which the rules within a group will be
     * attempted is arbitrary. Match order and limit applies to the group as a
     * whole.
     */
    fun addGroupEnd(): HepProgramBuilder {
        assert(group != null)
        val instruction: HepInstruction.EndGroup = EndGroup()
        instructions.add(instruction)
        requireNonNull(group, "group").endGroup = instruction
        group = null
        return this
    }

    /**
     * Adds an instruction to attempt to match instances of
     * [org.apache.calcite.rel.convert.ConverterRule],
     * but only where a conversion is actually required.
     *
     * @param guaranteed if true, use only guaranteed converters; if false, use
     * only non-guaranteed converters
     */
    fun addConverters(guaranteed: Boolean): HepProgramBuilder {
        assert(group == null)
        val instruction: HepInstruction.ConverterRules = ConverterRules()
        instruction.guaranteed = guaranteed
        instructions.add(instruction)
        return this
    }

    /**
     * Adds an instruction to attempt to match instances of
     * [CommonRelSubExprRule], but only in cases where vertices have more
     * than one parent.
     */
    fun addCommonRelSubExprInstruction(): HepProgramBuilder {
        assert(group == null)
        val instruction: HepInstruction.CommonRelSubExprRules = CommonRelSubExprRules()
        instructions.add(instruction)
        return this
    }

    /**
     * Adds an instruction to change the order of pattern matching for
     * subsequent instructions. The new order will take effect for the rest of
     * the program (not counting subprograms) or until another match order
     * instruction is encountered.
     *
     * @param order new match direction to set
     */
    fun addMatchOrder(order: HepMatchOrder?): HepProgramBuilder {
        assert(group == null)
        val instruction: HepInstruction.MatchOrder = MatchOrder()
        instruction.order = order
        instructions.add(instruction)
        return this
    }

    /**
     * Adds an instruction to limit the number of pattern matches for subsequent
     * instructions. The limit will take effect for the rest of the program (not
     * counting subprograms) or until another limit instruction is encountered.
     *
     * @param limit limit to set; use [HepProgram.MATCH_UNTIL_FIXPOINT] to
     * remove limit
     */
    fun addMatchLimit(limit: Int): HepProgramBuilder {
        assert(group == null)
        val instruction: HepInstruction.MatchLimit = MatchLimit()
        instruction.limit = limit
        instructions.add(instruction)
        return this
    }

    /**
     * Adds an instruction to execute a subprogram. Note that this is different
     * from adding the instructions from the subprogram individually. When added
     * as a subprogram, the sequence will execute repeatedly until a fixpoint is
     * reached, whereas when the instructions are added individually, the
     * sequence will only execute once (with a separate fixpoint for each
     * instruction).
     *
     *
     * The subprogram has its own state for match order and limit
     * (initialized to the defaults every time the subprogram is executed) and
     * any changes it makes to those settings do not affect the parent program.
     *
     * @param program subprogram to execute
     */
    fun addSubprogram(program: HepProgram?): HepProgramBuilder {
        assert(group == null)
        val instruction: HepInstruction.Subprogram = Subprogram()
        instruction.subprogram = program
        instructions.add(instruction)
        return this
    }

    /**
     * Returns the constructed program, clearing the state of this program
     * builder as a side-effect.
     *
     * @return immutable program
     */
    fun build(): HepProgram {
        assert(group == null)
        val program = HepProgram(instructions)
        clear()
        return program
    }
}
