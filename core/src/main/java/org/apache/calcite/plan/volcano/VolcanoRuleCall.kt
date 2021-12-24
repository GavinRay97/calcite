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
package org.apache.calcite.plan.volcano

import org.apache.calcite.plan.RelHintsPropagator
import org.apache.calcite.plan.RelOptListener
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelOptRuleOperand
import org.apache.calcite.plan.RelOptRuleOperandChildPolicy
import org.apache.calcite.rel.PhysicalNode
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.rules.SubstitutionRule
import org.apache.calcite.rel.rules.TransformationRule
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Lists
import java.util.ArrayList
import java.util.Arrays
import java.util.Collection
import java.util.HashSet
import java.util.List
import java.util.Map
import java.util.Set
import java.util.stream.Collectors
import org.apache.calcite.linq4j.Nullness.castNonNull
import java.util.Objects.requireNonNull

/**
 * `VolcanoRuleCall` implements the [RelOptRuleCall] interface
 * for VolcanoPlanner.
 */
class VolcanoRuleCall protected constructor(
    planner: VolcanoPlanner,
    operand: RelOptRuleOperand?,
    rels: Array<RelNode?>?,
    nodeInputs: Map<RelNode?, List<RelNode?>?>?
) : RelOptRuleCall(planner, operand, rels, nodeInputs) {
    //~ Instance fields --------------------------------------------------------
    protected val volcanoPlanner: VolcanoPlanner

    /**
     * List of [RelNode] generated by this call. For debugging purposes.
     */
    @Nullable
    private var generatedRelList: List<RelNode>? = null
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a rule call, internal, with array to hold bindings.
     *
     * @param planner Planner
     * @param operand First operand of the rule
     * @param rels    Array which will hold the matched relational expressions
     * @param nodeInputs For each node which matched with `matchAnyChildren`
     * = true, a list of the node's inputs
     */
    init {
        volcanoPlanner = planner
    }

    /**
     * Creates a rule call.
     *
     * @param planner Planner
     * @param operand First operand of the rule
     */
    internal constructor(
        planner: VolcanoPlanner,
        operand: RelOptRuleOperand
    ) : this(
        planner,
        operand, arrayOfNulls<RelNode>(operand.getRule().operands.size()),
        ImmutableMap.of()
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun transformTo(
        rel: RelNode, equiv: Map<RelNode?, RelNode?>,
        handler: RelHintsPropagator
    ) {
        var rel: RelNode = rel
        if (rel is PhysicalNode
            && rule is TransformationRule
        ) {
            throw RuntimeException(
                rel.toString() + " is a PhysicalNode, which is not allowed in " + rule
            )
        }
        rel = handler.propagate(rels.get(0), rel)
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "Transform to: rel#{} via {}{}", rel.getId(), getRule(),
                if (equiv.isEmpty()) "" else " with equivalences $equiv"
            )
            if (generatedRelList != null) {
                generatedRelList.add(rel)
            }
        }
        try {
            // It's possible that rel is a subset or is already registered.
            // Is there still a point in continuing? Yes, because we might
            // discover that two sets of expressions are actually equivalent.
            if (volcanoPlanner.getListener() != null) {
                val event: RelOptListener.RuleProductionEvent = RuleProductionEvent(
                    volcanoPlanner,
                    rel,
                    this,
                    true
                )
                volcanoPlanner.getListener().ruleProductionSucceeded(event)
            }
            if (this.getRule() is SubstitutionRule
                && (getRule() as SubstitutionRule).autoPruneOld()
            ) {
                volcanoPlanner.prune(rels.get(0))
            }

            // Registering the root relational expression implicitly registers
            // its descendants. Register any explicit equivalences first, so we
            // don't register twice and cause churn.
            for (entry in equiv.entrySet()) {
                volcanoPlanner.ensureRegistered(
                    entry.getKey(), entry.getValue()
                )
            }
            // The subset is not used, but we need it, just for debugging
            @SuppressWarnings("unused") val subset: RelSubset = volcanoPlanner.ensureRegistered(rel, rels.get(0))
            if (volcanoPlanner.getListener() != null) {
                val event: RelOptListener.RuleProductionEvent = RuleProductionEvent(
                    volcanoPlanner,
                    rel,
                    this,
                    false
                )
                volcanoPlanner.getListener().ruleProductionSucceeded(event)
            }
        } catch (e: Exception) {
            throw RuntimeException(
                "Error occurred while applying rule "
                        + getRule(), e
            )
        }
    }

    /**
     * Called when all operands have matched.
     */
    protected fun onMatch() {
        assert(getRule().matches(this))
        volcanoPlanner.checkCancel()
        try {
            if (volcanoPlanner.isRuleExcluded(getRule())) {
                LOGGER.debug("Rule [{}] not fired due to exclusion filter", getRule())
                return
            }
            if (isRuleExcluded()) {
                LOGGER.debug("Rule [{}] not fired due to exclusion hint", getRule())
                return
            }
            for (i in 0 until rels.length) {
                val rel: RelNode = rels.get(i)
                val subset: RelSubset = volcanoPlanner.getSubset(rel)
                if (subset == null) {
                    LOGGER.debug(
                        "Rule [{}] not fired because operand #{} ({}) has no subset",
                        getRule(), i, rel
                    )
                    return
                }
                if (subset.set.equivalentSet != null // When rename RelNode via VolcanoPlanner#rename(RelNode rel),
                    // we may remove rel from its subset: "subset.set.rels.remove(rel)".
                    // Skip rule match when the rel has been removed from set.
                    || subset !== rel && !subset.contains(rel)
                ) {
                    LOGGER.debug(
                        "Rule [{}] not fired because operand #{} ({}) belongs to obsolete set",
                        getRule(), i, rel
                    )
                    return
                }
                if (volcanoPlanner.prunedNodes.contains(rel)) {
                    LOGGER.debug(
                        "Rule [{}] not fired because operand #{} ({}) has importance=0",
                        getRule(), i, rel
                    )
                    return
                }
            }
            if (volcanoPlanner.getListener() != null) {
                val event: RelOptListener.RuleAttemptedEvent = RuleAttemptedEvent(
                    volcanoPlanner,
                    rels.get(0),
                    this,
                    true
                )
                volcanoPlanner.getListener().ruleAttempted(event)
            }
            if (LOGGER.isDebugEnabled()) {
                generatedRelList = ArrayList()
            }
            volcanoPlanner.ruleCallStack.push(this)
            try {
                getRule().onMatch(this)
            } finally {
                volcanoPlanner.ruleCallStack.pop()
            }
            if (generatedRelList != null) {
                if (generatedRelList!!.isEmpty()) {
                    LOGGER.debug("call#{} generated 0 successors.", id)
                } else {
                    LOGGER.debug(
                        "call#{} generated {} successors: {}",
                        id, generatedRelList!!.size(), generatedRelList
                    )
                }
                generatedRelList = null
            }
            if (volcanoPlanner.getListener() != null) {
                val event: RelOptListener.RuleAttemptedEvent = RuleAttemptedEvent(
                    volcanoPlanner,
                    rels.get(0),
                    this,
                    false
                )
                volcanoPlanner.getListener().ruleAttempted(event)
            }
        } catch (e: Exception) {
            throw RuntimeException(
                "Error while applying rule " + getRule()
                    .toString() + ", args " + Arrays.toString(rels), e
            )
        }
    }

    /**
     * Applies this rule, with a given relational expression in the first slot.
     */
    fun match(rel: RelNode) {
        assert(getOperand0().matches(rel)) { "precondition" }
        val solve = 0
        val operandOrdinal: Int = castNonNull(getOperand0().solveOrder).get(solve)
        this.rels.get(operandOrdinal) = rel
        matchRecurse(solve + 1)
    }

    /**
     * Recursively matches operands above a given solve order.
     *
     * @param solve Solve order of operand (&gt; 0 and  the operand count)
     */
    private fun matchRecurse(solve: Int) {
        assert(solve > 0)
        assert(solve <= rule.operands.size())
        val operands: List<RelOptRuleOperand> = getRule().operands
        if (solve == operands.size()) {
            // We have matched all operands. Now ask the rule whether it
            // matches; this gives the rule chance to apply side-conditions.
            // If the side-conditions are satisfied, we have a match.
            if (getRule().matches(this)) {
                onMatch()
            }
        } else {
            val solveOrder: IntArray = castNonNull(operand0.solveOrder)
            val operandOrdinal = solveOrder[solve]
            val previousOperandOrdinal = solveOrder[solve - 1]
            val ascending = operandOrdinal < previousOperandOrdinal
            val previousOperand: RelOptRuleOperand = operands[previousOperandOrdinal]
            val operand: RelOptRuleOperand = operands[operandOrdinal]
            val previous: RelNode = rels.get(previousOperandOrdinal)
            val parentOperand: RelOptRuleOperand
            val successors: Collection<RelNode?>
            if (ascending) {
                assert(previousOperand.getParent() === operand)
                assert(operand.getMatchedClass() !== RelSubset::class.java)
                if (previousOperand.getMatchedClass() !== RelSubset::class.java
                    && previous is RelSubset
                ) {
                    throw RuntimeException(
                        "RelSubset should not match with "
                                + previousOperand.getMatchedClass().getSimpleName()
                    )
                }
                parentOperand = operand
                val subset: RelSubset = volcanoPlanner.getSubsetNonNull(previous)
                successors = subset.getParentRels()
            } else {
                parentOperand = requireNonNull(
                    operand.getParent()
                ) { "operand.getParent() for $operand" }
                val parentRel: RelNode = rels.get(parentOperand.ordinalInRule)
                val inputs: List<RelNode> = parentRel.getInputs()
                // if the child is unordered, then add all rels in all input subsets to the successors list
                // because unordered can match child in any ordinal
                successors = if (parentOperand.childPolicy === RelOptRuleOperandChildPolicy.UNORDERED) {
                    if (operand.getMatchedClass() === RelSubset::class.java) {
                        // Find all the sibling subsets that satisfy this subset's traitSet
                        inputs.stream()
                            .flatMap { subset -> (subset as RelSubset).getSubsetsSatisfyingThis() }
                            .collect(Collectors.toList())
                    } else {
                        val allRelsInAllSubsets: List<RelNode> = ArrayList()
                        val duplicates: Set<RelNode> = HashSet()
                        for (input in inputs) {
                            if (!duplicates.add(input)) {
                                // Ignore duplicate subsets
                                continue
                            }
                            val inputSubset: RelSubset = input as RelSubset
                            for (rel in inputSubset.getRels()) {
                                if (!duplicates.add(rel)) {
                                    // Ignore duplicate relations
                                    continue
                                }
                                allRelsInAllSubsets.add(rel)
                            }
                        }
                        allRelsInAllSubsets
                    }
                } else if (operand.ordinalInParent < inputs.size()) {
                    // child policy is not unordered
                    // we need to find the exact input node based on child operand's ordinalInParent
                    val subset: RelSubset = inputs[operand.ordinalInParent] as RelSubset
                    if (operand.getMatchedClass() === RelSubset::class.java) {
                        // Find all the sibling subsets that satisfy this subset'straitSet
                        subset.getSubsetsSatisfyingThis().collect(Collectors.toList())
                    } else {
                        subset.getRelList()
                    }
                } else {
                    // The operand expects parentRel to have a certain number
                    // of inputs and it does not.
                    ImmutableList.of()
                }
            }
            for (rel in successors) {
                if (operand.getRule() is TransformationRule
                    && rel.getConvention() !== previous.getConvention()
                ) {
                    continue
                }
                if (!operand.matches(rel)) {
                    continue
                }
                if (ascending && operand.childPolicy !== RelOptRuleOperandChildPolicy.UNORDERED) {
                    // We know that the previous operand was *a* child of its parent,
                    // but now check that it is the *correct* child.
                    if (previousOperand.ordinalInParent >= rel.getInputs().size()) {
                        continue
                    }
                    val input: RelSubset = rel.getInput(previousOperand.ordinalInParent) as RelSubset
                    if (previousOperand.getMatchedClass() === RelSubset::class.java) {
                        // The matched subset (previous) should satisfy our input subset (input)
                        if (input.getSubsetsSatisfyingThis().noneMatch(previous::equals)) {
                            continue
                        }
                    } else {
                        if (!input.contains(previous)) {
                            continue
                        }
                    }
                }
                when (parentOperand.childPolicy) {
                    UNORDERED ->           // Note: below is ill-defined. Suppose there's a union with 3 inputs,
                        // and the rule is written as Union.class, unordered(...)
                        // What should be provided for the rest 2 arguments?
                        // RelSubsets? Random relations from those subsets?
                        // For now, Calcite code does not use getChildRels, so the bug is just waiting its day
                        if (ascending) {
                            val inputs: List<RelNode> = Lists.newArrayList(rel.getInputs())
                            inputs.set(previousOperand.ordinalInParent, previous)
                            setChildRels(rel, inputs)
                        } else {
                            var inputs: List<RelNode?> = getChildRels(previous)
                            if (inputs == null) {
                                inputs = Lists.newArrayList(previous.getInputs())
                            }
                            inputs.set(operand.ordinalInParent, rel)
                            setChildRels(previous, inputs)
                        }
                    else -> {}
                }
                rels.get(operandOrdinal) = rel
                matchRecurse(solve + 1)
            }
        }
    }
}
