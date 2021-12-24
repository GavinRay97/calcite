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

import org.apache.calcite.linq4j.function.Function2
import org.apache.calcite.linq4j.function.Functions
import org.apache.calcite.plan.AbstractRelOptPlanner
import org.apache.calcite.plan.CommonRelSubExprRule
import org.apache.calcite.plan.Context
import org.apache.calcite.plan.RelDigest
import org.apache.calcite.plan.RelOptCost
import org.apache.calcite.plan.RelOptCostFactory
import org.apache.calcite.plan.RelOptCostImpl
import org.apache.calcite.plan.RelOptMaterialization
import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRuleOperand
import org.apache.calcite.plan.RelTrait
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.Converter
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.TraitMatchingRule
import org.apache.calcite.rel.core.RelFactories
import org.apache.calcite.rel.metadata.RelMdUtil
import org.apache.calcite.rel.metadata.RelMetadataProvider
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.util.Pair
import org.apache.calcite.util.Util
import org.apache.calcite.util.graph.BreadthFirstIterator
import org.apache.calcite.util.graph.CycleDetector
import org.apache.calcite.util.graph.DefaultDirectedGraph
import org.apache.calcite.util.graph.DefaultEdge
import org.apache.calcite.util.graph.DepthFirstIterator
import org.apache.calcite.util.graph.DirectedGraph
import org.apache.calcite.util.graph.Graphs
import org.apache.calcite.util.graph.TopologicalOrderIterator
import com.google.common.collect.ImmutableList
import java.util.ArrayDeque
import java.util.ArrayList
import java.util.Collection
import java.util.Collections
import java.util.HashMap
import java.util.HashSet
import java.util.Iterator
import java.util.LinkedHashSet
import java.util.List
import java.util.Map
import java.util.Queue
import java.util.Set
import org.apache.calcite.linq4j.Nullness.castNonNull
import java.util.Objects.requireNonNull

/**
 * HepPlanner is a heuristic implementation of the [RelOptPlanner]
 * interface.
 */
class HepPlanner(
    program: HepProgram,
    @Nullable context: Context?,
    noDag: Boolean,
    @Nullable onCopyHook: Function2<RelNode?, RelNode?, Void?>?,
    costFactory: RelOptCostFactory?
) : AbstractRelOptPlanner(costFactory, context) {
    //~ Instance fields --------------------------------------------------------
    private val mainProgram: HepProgram

    @Nullable
    private var currentProgram: HepProgram? = null

    @Nullable
    private var root: HepRelVertex? = null

    @Nullable
    private var requestedRootTraits: RelTraitSet? = null

    /**
     * [RelDataType] is represented with its field types as `List<RelDataType>`.
     * This enables to treat as equal projects that differ in expression names only.
     */
    private val mapDigestToVertex: Map<RelDigest, HepRelVertex> = HashMap()
    private var nTransformations = 0
    private var graphSizeLastGC = 0
    private var nTransformationsLastGC = 0
    private val noDag: Boolean

    /**
     * Query graph, with edges directed from parent to child. This is a
     * single-rooted DAG, possibly with additional roots corresponding to
     * discarded plan fragments which remain to be garbage-collected.
     */
    private val graph: DirectedGraph<HepRelVertex, DefaultEdge> = DefaultDirectedGraph.create()
    private val onCopyHook: Function2<RelNode, RelNode, Void>
    private val materializations: List<RelOptMaterialization> = ArrayList()
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a new HepPlanner that allows DAG.
     *
     * @param program program controlling rule application
     */
    constructor(program: HepProgram) : this(program, null, false, null, RelOptCostImpl.FACTORY) {}

    /**
     * Creates a new HepPlanner that allows DAG.
     *
     * @param program program controlling rule application
     * @param context to carry while planning
     */
    constructor(program: HepProgram, @Nullable context: Context?) : this(
        program,
        context,
        false,
        null,
        RelOptCostImpl.FACTORY
    ) {
    }

    /**
     * Creates a new HepPlanner with the option to keep the graph a
     * tree (noDag = true) or allow DAG (noDag = false).
     *
     * @param noDag      If false, create shared nodes if expressions are
     * identical
     * @param program    Program controlling rule application
     * @param onCopyHook Function to call when a node is copied
     */
    init {
        mainProgram = program
        this.onCopyHook = Util.first(onCopyHook, Functions.ignore2())
        this.noDag = noDag
    }

    //~ Methods ----------------------------------------------------------------
    // implement RelOptPlanner
    @Override
    fun setRoot(rel: RelNode?) {
        root = addRelToGraph(rel)
        dumpGraph()
    }

    // implement RelOptPlanner
    @Override
    @Nullable
    fun getRoot(): RelNode? {
        return root
    }

    @Override
    fun clear() {
        super.clear()
        for (rule in getRules()) {
            removeRule(rule)
        }
        materializations.clear()
    }

    // implement RelOptPlanner
    @Override
    fun changeTraits(rel: RelNode, toTraits: RelTraitSet?): RelNode {
        // Ignore traits, except for the root, where we remember
        // what the final conversion should be.
        if (rel === root || rel === requireNonNull(root, "root").getCurrentRel()) {
            requestedRootTraits = toTraits
        }
        return rel
    }

    // implement RelOptPlanner
    @Override
    fun findBestExp(): RelNode? {
        assert(root != null)
        executeProgram(mainProgram)

        // Get rid of everything except what's in the final plan.
        collectGarbage()
        dumpRuleAttemptsInfo()
        return buildFinalPlan(requireNonNull(root, "root"))
    }

    private fun executeProgram(program: HepProgram) {
        val savedProgram: HepProgram? = currentProgram
        currentProgram = program
        currentProgram.initialize(program === mainProgram)
        for (instruction in currentProgram.instructions) {
            instruction.execute(this)
            val delta = nTransformations - nTransformationsLastGC
            if (delta > graphSizeLastGC) {
                // The number of transformations performed since the last
                // garbage collection is greater than the number of vertices in
                // the graph at that time.  That means there should be a
                // reasonable amount of garbage to collect now.  We do it this
                // way to amortize garbage collection cost over multiple
                // instructions, while keeping the highwater memory usage
                // proportional to the graph size.
                collectGarbage()
            }
        }
        currentProgram = savedProgram
    }

    fun executeInstruction(
        instruction: HepInstruction.MatchLimit
    ) {
        LOGGER.trace("Setting match limit to {}", instruction.limit)
        assert(currentProgram != null) { "currentProgram must not be null" }
        currentProgram!!.matchLimit = instruction.limit
    }

    fun executeInstruction(
        instruction: HepInstruction.MatchOrder
    ) {
        LOGGER.trace("Setting match order to {}", instruction.order)
        assert(currentProgram != null) { "currentProgram must not be null" }
        currentProgram!!.matchOrder = instruction.order
    }

    fun executeInstruction(
        instruction: HepInstruction.RuleInstance
    ) {
        if (skippingGroup()) {
            return
        }
        if (instruction.rule == null) {
            assert(instruction.ruleDescription != null)
            instruction.rule = getRuleByDescription(instruction.ruleDescription)
            LOGGER.trace(
                "Looking up rule with description {}, found {}",
                instruction.ruleDescription, instruction.rule
            )
        }
        if (instruction.rule != null) {
            applyRules(
                Collections.singleton(instruction.rule),
                true
            )
        }
    }

    fun executeInstruction(
        instruction: HepInstruction.RuleClass<*>
    ) {
        if (skippingGroup()) {
            return
        }
        LOGGER.trace("Applying rule class {}", instruction.ruleClass)
        var ruleSet: Set<RelOptRule>? = instruction.ruleSet
        if (ruleSet == null) {
            ruleSet = LinkedHashSet()
            instruction.ruleSet = ruleSet
            val ruleClass: Class<*> = requireNonNull(instruction.ruleClass, "instruction.ruleClass")
            for (rule in mapDescToRule.values()) {
                if (ruleClass.isInstance(rule)) {
                    ruleSet.add(rule)
                }
            }
        }
        applyRules(ruleSet, true)
    }

    fun executeInstruction(
        instruction: HepInstruction.RuleCollection
    ) {
        if (skippingGroup()) {
            return
        }
        assert(instruction.rules != null) { "instruction.rules must not be null" }
        applyRules(instruction.rules, true)
    }

    private fun skippingGroup(): Boolean {
        return if (currentProgram != null && currentProgram.group != null) {
            // Skip if we've already collected the ruleset.
            !currentProgram.group.collecting
        } else {
            // Not grouping.
            false
        }
    }

    fun executeInstruction(
        instruction: HepInstruction.ConverterRules
    ) {
        assert(currentProgram != null) { "currentProgram must not be null" }
        assert(currentProgram!!.group == null)
        if (instruction.ruleSet == null) {
            instruction.ruleSet = LinkedHashSet()
            for (rule in mapDescToRule.values()) {
                if (rule !is ConverterRule) {
                    continue
                }
                val converter: ConverterRule = rule as ConverterRule
                if (converter.isGuaranteed() !== instruction.guaranteed) {
                    continue
                }

                // Add the rule itself to work top-down
                instruction.ruleSet.add(converter)
                if (!instruction.guaranteed) {
                    // Add a TraitMatchingRule to work bottom-up
                    instruction.ruleSet.add(
                        TraitMatchingRule.config(converter, RelFactories.LOGICAL_BUILDER)
                            .toRule()
                    )
                }
            }
        }
        applyRules(instruction.ruleSet, instruction.guaranteed)
    }

    fun executeInstruction(instruction: HepInstruction.CommonRelSubExprRules) {
        assert(currentProgram != null) { "currentProgram must not be null" }
        assert(currentProgram!!.group == null)
        var ruleSet: Set<RelOptRule>? = instruction.ruleSet
        if (ruleSet == null) {
            ruleSet = LinkedHashSet()
            instruction.ruleSet = ruleSet
            for (rule in mapDescToRule.values()) {
                if (rule !is CommonRelSubExprRule) {
                    continue
                }
                ruleSet.add(rule)
            }
        }
        applyRules(ruleSet, true)
    }

    fun executeInstruction(
        instruction: HepInstruction.Subprogram
    ) {
        LOGGER.trace("Entering subprogram")
        while (true) {
            val nTransformationsBefore = nTransformations
            executeProgram(requireNonNull(instruction.subprogram, "instruction.subprogram"))
            if (nTransformations == nTransformationsBefore) {
                // Nothing happened this time around.
                break
            }
        }
        LOGGER.trace("Leaving subprogram")
    }

    fun executeInstruction(
        instruction: HepInstruction.BeginGroup
    ) {
        assert(currentProgram != null) { "currentProgram must not be null" }
        assert(currentProgram!!.group == null)
        currentProgram!!.group = instruction.endGroup
        LOGGER.trace("Entering group")
    }

    fun executeInstruction(
        instruction: HepInstruction.EndGroup
    ) {
        assert(currentProgram != null) { "currentProgram must not be null" }
        assert(currentProgram!!.group === instruction)
        currentProgram!!.group = null
        instruction.collecting = false
        applyRules(requireNonNull(instruction.ruleSet, "instruction.ruleSet"), true)
        LOGGER.trace("Leaving group")
    }

    private fun depthFirstApply(
        iter: Iterator<HepRelVertex>,
        rules: Collection<RelOptRule>?,
        forceConversions: Boolean, nMatches: Int
    ): Int {
        var nMatches = nMatches
        while (iter.hasNext()) {
            val vertex: HepRelVertex = iter.next()
            for (rule in rules) {
                val newVertex: HepRelVertex? = applyRule(rule, vertex, forceConversions)
                if (newVertex == null || newVertex === vertex) {
                    continue
                }
                assert(currentProgram != null) { "currentProgram must not be null" }
                ++nMatches
                if (nMatches >= currentProgram!!.matchLimit) {
                    return nMatches
                }
                // To the extent possible, pick up where we left
                // off; have to create a new iterator because old
                // one was invalidated by transformation.
                val depthIter: Iterator<HepRelVertex> = getGraphIterator(newVertex)
                nMatches = depthFirstApply(
                    depthIter, rules, forceConversions,
                    nMatches
                )
                break
            }
        }
        return nMatches
    }

    private fun applyRules(
        rules: Collection<RelOptRule>?,
        forceConversions: Boolean
    ) {
        assert(currentProgram != null) { "currentProgram must not be null" }
        if (currentProgram!!.group != null) {
            assert(currentProgram!!.group.collecting)
            val ruleSet: Set<RelOptRule> = requireNonNull(
                currentProgram!!.group.ruleSet,
                "currentProgram.group.ruleSet"
            )
            ruleSet.addAll(rules)
            return
        }
        LOGGER.trace("Applying rule set {}", rules)
        requireNonNull(currentProgram, "currentProgram")
        val fullRestartAfterTransformation = (currentProgram!!.matchOrder !== HepMatchOrder.ARBITRARY
                && currentProgram!!.matchOrder !== HepMatchOrder.DEPTH_FIRST)
        var nMatches = 0
        var fixedPoint: Boolean
        do {
            var iter: Iterator<HepRelVertex> = getGraphIterator(requireNonNull(root, "root"))
            fixedPoint = true
            while (iter.hasNext()) {
                val vertex: HepRelVertex = iter.next()
                for (rule in rules) {
                    val newVertex: HepRelVertex? = applyRule(rule, vertex, forceConversions)
                    if (newVertex == null || newVertex === vertex) {
                        continue
                    }
                    ++nMatches
                    if (nMatches >= requireNonNull(currentProgram, "currentProgram").matchLimit) {
                        return
                    }
                    if (fullRestartAfterTransformation) {
                        iter = getGraphIterator(requireNonNull(root, "root"))
                    } else {
                        // To the extent possible, pick up where we left
                        // off; have to create a new iterator because old
                        // one was invalidated by transformation.
                        iter = getGraphIterator(newVertex)
                        if (requireNonNull(currentProgram, "currentProgram").matchOrder
                            === HepMatchOrder.DEPTH_FIRST
                        ) {
                            nMatches = depthFirstApply(iter, rules, forceConversions, nMatches)
                            if (nMatches >= requireNonNull(currentProgram, "currentProgram").matchLimit) {
                                return
                            }
                        }
                        // Remember to go around again since we're
                        // skipping some stuff.
                        fixedPoint = false
                    }
                    break
                }
            }
        } while (!fixedPoint)
    }

    private fun getGraphIterator(start: HepRelVertex): Iterator<HepRelVertex> {
        // Make sure there's no garbage, because topological sort
        // doesn't start from a specific root, and rules can't
        // deal with firing on garbage.

        // FIXME jvs 25-Sept-2006:  I had to move this earlier because
        // of FRG-215, which is still under investigation.  Once we
        // figure that one out, move down to location below for
        // better optimizer performance.
        collectGarbage()
        assert(currentProgram != null) { "currentProgram must not be null" }
        return when (requireNonNull(currentProgram!!.matchOrder, "currentProgram.matchOrder")) {
            ARBITRARY, DEPTH_FIRST -> DepthFirstIterator.of(graph, start).iterator()
            TOP_DOWN -> {
                assert(start === root)
                // see above
/*
        collectGarbage();
*/TopologicalOrderIterator.of(graph).iterator()
            }
            BOTTOM_UP -> {
                assert(start === root)

                // see above
/*
        collectGarbage();
*/

                // TODO jvs 4-Apr-2006:  enhance TopologicalOrderIterator
                // to support reverse walk.
                val list: List<HepRelVertex> = ArrayList()
                for (vertex in TopologicalOrderIterator.of(graph)) {
                    list.add(vertex)
                }
                Collections.reverse(list)
                list.iterator()
            }
            else -> {
                assert(start === root)
                val list: List<HepRelVertex> = ArrayList()
                for (vertex in TopologicalOrderIterator.of(graph)) {
                    list.add(vertex)
                }
                Collections.reverse(list)
                list.iterator()
            }
        }
    }

    @Nullable
    private fun applyRule(
        rule: RelOptRule,
        vertex: HepRelVertex,
        forceConversions: Boolean
    ): HepRelVertex? {
        if (!graph.vertexSet().contains(vertex)) {
            return null
        }
        var parentTrait: RelTrait? = null
        var parents: List<RelNode?>? = null
        if (rule is ConverterRule) {
            // Guaranteed converter rules require special casing to make sure
            // they only fire where actually needed, otherwise they tend to
            // fire to infinity and beyond.
            val converterRule: ConverterRule = rule as ConverterRule
            if (converterRule.isGuaranteed() || !forceConversions) {
                if (!doesConverterApply(converterRule, vertex)) {
                    return null
                }
                parentTrait = converterRule.getOutTrait()
            }
        } else if (rule is CommonRelSubExprRule) {
            // Only fire CommonRelSubExprRules if the vertex is a common
            // subexpression.
            val parentVertices: List<HepRelVertex> = getVertexParents(vertex)
            if (parentVertices.size() < 2) {
                return null
            }
            parents = ArrayList()
            for (pVertex in parentVertices) {
                parents.add(pVertex.getCurrentRel())
            }
        }
        val bindings: List<RelNode> = ArrayList()
        val nodeChildren: Map<RelNode, List<RelNode>> = HashMap()
        val match = matchOperands(
            rule.getOperand(),
            vertex.getCurrentRel(),
            bindings,
            nodeChildren
        )
        if (!match) {
            return null
        }
        val call = HepRuleCall(
            this,
            rule.getOperand(),
            bindings.toArray(arrayOfNulls<RelNode>(0)),
            nodeChildren,
            parents
        )

        // Allow the rule to apply its own side-conditions.
        if (!rule.matches(call)) {
            return null
        }
        fireRule(call)
        return if (!call.getResults().isEmpty()) {
            applyTransformationResults(
                vertex,
                call,
                parentTrait
            )
        } else null
    }

    private fun doesConverterApply(
        converterRule: ConverterRule,
        vertex: HepRelVertex
    ): Boolean {
        val outTrait: RelTrait = converterRule.getOutTrait()
        val parents: List<HepRelVertex> = Graphs.predecessorListOf(graph, vertex)
        for (parent in parents) {
            val parentRel: RelNode = parent.getCurrentRel()
            if (parentRel is Converter) {
                // We don't support converter chains.
                continue
            }
            if (parentRel.getTraitSet().contains(outTrait)) {
                // This parent wants the traits produced by the converter.
                return true
            }
        }
        return (vertex === root
                && requestedRootTraits != null
                && requestedRootTraits.contains(outTrait))
    }

    /**
     * Retrieves the parent vertices of a vertex.  If a vertex appears multiple
     * times as an input into a parent, then that counts as multiple parents,
     * one per input reference.
     *
     * @param vertex the vertex
     * @return the list of parents for the vertex
     */
    private fun getVertexParents(vertex: HepRelVertex): List<HepRelVertex> {
        val parents: List<HepRelVertex> = ArrayList()
        val parentVertices: List<HepRelVertex> = Graphs.predecessorListOf(graph, vertex)
        for (pVertex in parentVertices) {
            val parent: RelNode = pVertex.getCurrentRel()
            for (i in 0 until parent.getInputs().size()) {
                if (parent.getInputs().get(i) === vertex) {
                    parents.add(pVertex)
                }
            }
        }
        return parents
    }

    private fun applyTransformationResults(
        vertex: HepRelVertex,
        call: HepRuleCall,
        @Nullable parentTrait: RelTrait?
    ): HepRelVertex? {
        // TODO jvs 5-Apr-2006:  Take the one that gives the best
        // global cost rather than the best local cost.  That requires
        // "tentative" graph edits.
        assert(!call.getResults().isEmpty())
        var bestRel: RelNode? = null
        if (call.getResults().size() === 1) {
            // No costing required; skip it to minimize the chance of hitting
            // rels without cost information.
            bestRel = call.getResults().get(0)
        } else {
            var bestCost: RelOptCost? = null
            val mq: RelMetadataQuery = call.getMetadataQuery()
            for (rel in call.getResults()) {
                val thisCost: RelOptCost = getCost(rel, mq)
                if (LOGGER.isTraceEnabled()) {
                    // Keep in the isTraceEnabled for the getRowCount method call
                    LOGGER.trace(
                        "considering {} with cumulative cost={} and rowcount={}",
                        rel, thisCost, mq.getRowCount(rel)
                    )
                }
                if (thisCost == null) {
                    continue
                }
                if (bestRel == null || thisCost.isLt(castNonNull(bestCost))) {
                    bestRel = rel
                    bestCost = thisCost
                }
            }
        }
        ++nTransformations
        notifyTransformation(
            call,
            requireNonNull(bestRel, "bestRel"),
            true
        )

        // Before we add the result, make a copy of the list of vertex's
        // parents.  We'll need this later during contraction so that
        // we only update the existing parents, not the new parents
        // (otherwise loops can result).  Also take care of filtering
        // out parents by traits in case we're dealing with a converter rule.
        val allParents: List<HepRelVertex> = Graphs.predecessorListOf(graph, vertex)
        val parents: List<HepRelVertex?> = ArrayList()
        for (parent in allParents) {
            if (parentTrait != null) {
                val parentRel: RelNode = parent.getCurrentRel()
                if (parentRel is Converter) {
                    // We don't support automatically chaining conversions.
                    // Treating a converter as a candidate parent here
                    // can cause the "iParentMatch" check below to
                    // throw away a new converter needed in
                    // the multi-parent DAG case.
                    continue
                }
                if (!parentRel.getTraitSet().contains(parentTrait)) {
                    // This parent does not want the converted result.
                    continue
                }
            }
            parents.add(parent)
        }
        var newVertex: HepRelVertex? = addRelToGraph(bestRel)

        // There's a chance that newVertex is the same as one
        // of the parents due to common subexpression recognition
        // (e.g. the LogicalProject added by JoinCommuteRule).  In that
        // case, treat the transformation as a nop to avoid
        // creating a loop.
        val iParentMatch = parents.indexOf(newVertex)
        if (iParentMatch != -1) {
            newVertex = parents[iParentMatch]
        } else {
            contractVertices(newVertex, vertex, parents)
        }
        if (getListener() != null) {
            // Assume listener doesn't want to see garbage.
            collectGarbage()
        }
        notifyTransformation(
            call,
            bestRel,
            false
        )
        dumpGraph()
        return newVertex
    }

    // implement RelOptPlanner
    @Override
    fun register(
        rel: RelNode,
        @Nullable equivRel: RelNode?
    ): RelNode {
        // Ignore; this call is mostly to tell Volcano how to avoid
        // infinite loops.
        return rel
    }

    @Override
    fun onCopy(rel: RelNode?, newRel: RelNode?) {
        onCopyHook.apply(rel, newRel)
    }

    // implement RelOptPlanner
    @Override
    fun ensureRegistered(rel: RelNode, @Nullable equivRel: RelNode?): RelNode {
        return rel
    }

    // implement RelOptPlanner
    @Override
    fun isRegistered(rel: RelNode?): Boolean {
        return true
    }

    private fun addRelToGraph(
        rel: RelNode?
    ): HepRelVertex? {
        // Check if a transformation already produced a reference
        // to an existing vertex.
        var rel: RelNode? = rel
        if (graph.vertexSet().contains(rel)) {
            return rel
        }

        // Recursively add children, replacing this rel's inputs
        // with corresponding child vertices.
        val inputs: List<RelNode> = rel.getInputs()
        val newInputs: List<RelNode> = ArrayList()
        for (input1 in inputs) {
            val childVertex: HepRelVertex? = addRelToGraph(input1)
            newInputs.add(childVertex)
        }
        if (!Util.equalShallow(inputs, newInputs)) {
            val oldRel: RelNode? = rel
            rel = rel.copy(rel.getTraitSet(), newInputs)
            onCopy(oldRel, rel)
        }
        // Compute digest first time we add to DAG,
        // otherwise can't get equivVertex for common sub-expression
        rel.recomputeDigest()

        // try to find equivalent rel only if DAG is allowed
        if (!noDag) {
            // Now, check if an equivalent vertex already exists in graph.
            val equivVertex: HepRelVertex? = mapDigestToVertex[rel.getRelDigest()]
            if (equivVertex != null) {
                // Use existing vertex.
                return equivVertex
            }
        }

        // No equivalence:  create a new vertex to represent this rel.
        val newVertex = HepRelVertex(rel)
        graph.addVertex(newVertex)
        updateVertex(newVertex, rel)
        for (input in rel.getInputs()) {
            graph.addEdge(newVertex, input as HepRelVertex)
        }
        nTransformations++
        return newVertex
    }

    private fun contractVertices(
        preservedVertex: HepRelVertex?,
        discardedVertex: HepRelVertex,
        parents: List<HepRelVertex?>
    ) {
        if (preservedVertex === discardedVertex) {
            // Nop.
            return
        }
        val rel: RelNode = preservedVertex!!.getCurrentRel()
        updateVertex(preservedVertex, rel)

        // Update specified parents of discardedVertex.
        for (parent in parents) {
            val parentRel: RelNode = parent!!.getCurrentRel()
            val inputs: List<RelNode> = parentRel.getInputs()
            for (i in 0 until inputs.size()) {
                val child: RelNode = inputs[i]
                if (child !== discardedVertex) {
                    continue
                }
                parentRel.replaceInput(i, preservedVertex)
            }
            clearCache(parent)
            graph.removeEdge(parent, discardedVertex)
            graph.addEdge(parent, preservedVertex)
            updateVertex(parent, parentRel)
        }

        // NOTE:  we don't actually do graph.removeVertex(discardedVertex),
        // because it might still be reachable from preservedVertex.
        // Leave that job for garbage collection.
        if (discardedVertex === root) {
            root = preservedVertex
        }
    }

    /**
     * Clears metadata cache for the RelNode and its ancestors.
     *
     * @param vertex relnode
     */
    private fun clearCache(vertex: HepRelVertex?) {
        RelMdUtil.clearCache(vertex!!.getCurrentRel())
        if (!RelMdUtil.clearCache(vertex)) {
            return
        }
        val queue: Queue<DefaultEdge> = ArrayDeque(graph.getInwardEdges(vertex))
        while (!queue.isEmpty()) {
            val edge: DefaultEdge = queue.remove()
            val source: HepRelVertex = edge.source
            RelMdUtil.clearCache(source.getCurrentRel())
            if (RelMdUtil.clearCache(source)) {
                queue.addAll(graph.getInwardEdges(source))
            }
        }
    }

    private fun updateVertex(vertex: HepRelVertex?, rel: RelNode?) {
        if (rel !== vertex!!.getCurrentRel()) {
            // REVIEW jvs 5-Apr-2006:  We'll do this again later
            // during garbage collection.  Or we could get rid
            // of mark/sweep garbage collection and do it precisely
            // at this point by walking down to all rels which are only
            // reachable from here.
            notifyDiscard(vertex!!.getCurrentRel())
        }
        val oldKey: RelDigest = vertex!!.getCurrentRel().getRelDigest()
        if (mapDigestToVertex[oldKey] === vertex) {
            mapDigestToVertex.remove(oldKey)
        }
        // When a transformation happened in one rule apply, support
        // vertex2 replace vertex1, but the current relNode of
        // vertex1 and vertex2 is same,
        // then the digest is also same. but we can't remove vertex2,
        // otherwise the digest will be removed wrongly in the mapDigestToVertex
        //  when collectGC
        // so it must update the digest that map to vertex
        mapDigestToVertex.put(rel.getRelDigest(), vertex)
        if (rel !== vertex!!.getCurrentRel()) {
            vertex!!.replaceRel(rel)
        }
        notifyEquivalence(
            rel,
            vertex,
            false
        )
    }

    private fun buildFinalPlan(vertex: HepRelVertex?): RelNode? {
        val rel: RelNode = vertex!!.getCurrentRel()
        notifyChosen(rel)

        // Recursively process children, replacing this rel's inputs
        // with corresponding child rels.
        val inputs: List<RelNode> = rel.getInputs()
        for (i in 0 until inputs.size()) {
            var child: RelNode? = inputs[i] as? HepRelVertex
                ?: // Already replaced.
                continue
            child = buildFinalPlan(child as HepRelVertex?)
            rel.replaceInput(i, child)
        }
        RelMdUtil.clearCache(rel)
        rel.recomputeDigest()
        return rel
    }

    private fun collectGarbage() {
        if (nTransformations == nTransformationsLastGC) {
            // No modifications have taken place since the last gc,
            // so there can't be any garbage.
            return
        }
        nTransformationsLastGC = nTransformations
        LOGGER.trace("collecting garbage")

        // Yer basic mark-and-sweep.
        val rootSet: Set<HepRelVertex> = HashSet()
        val root: HepRelVertex = requireNonNull(root, "this.root")
        if (graph.vertexSet().contains(root)) {
            BreadthFirstIterator.reachable(rootSet, graph, root)
        }
        if (rootSet.size() === graph.vertexSet().size()) {
            // Everything is reachable:  no garbage to collect.
            return
        }
        val sweepSet: Set<HepRelVertex> = HashSet()
        for (vertex in graph.vertexSet()) {
            if (!rootSet.contains(vertex)) {
                sweepSet.add(vertex)
                val rel: RelNode = vertex.getCurrentRel()
                notifyDiscard(rel)
            }
        }
        assert(!sweepSet.isEmpty())
        graph.removeAllVertices(sweepSet)
        graphSizeLastGC = graph.vertexSet().size()

        // Clean up digest map too.
        val digestIter: Iterator<Map.Entry<RelDigest, HepRelVertex>> = mapDigestToVertex.entrySet().iterator()
        while (digestIter.hasNext()) {
            val vertex: HepRelVertex = digestIter.next().getValue()
            if (sweepSet.contains(vertex)) {
                digestIter.remove()
            }
        }
    }

    private fun assertNoCycles() {
        // Verify that the graph is acyclic.
        val cycleDetector: CycleDetector<HepRelVertex, DefaultEdge> = CycleDetector(graph)
        val cyclicVertices: Set<HepRelVertex> = cycleDetector.findCycles()
        if (cyclicVertices.isEmpty()) {
            return
        }
        throw AssertionError(
            "Query graph cycle detected in HepPlanner: "
                    + cyclicVertices
        )
    }

    private fun dumpGraph() {
        if (!LOGGER.isTraceEnabled()) {
            return
        }
        assertNoCycles()
        val root: HepRelVertex? = root
        if (root == null) {
            LOGGER.trace("dumpGraph: root is null")
            return
        }
        val mq: RelMetadataQuery = root.getCluster().getMetadataQuery()
        val sb = StringBuilder()
        sb.append("\nBreadth-first from root:  {\n")
        for (vertex in BreadthFirstIterator.of(graph, root)) {
            sb.append("    ")
                .append(vertex)
                .append(" = ")
            val rel: RelNode = vertex.getCurrentRel()
            sb.append(rel)
                .append(", rowcount=")
                .append(mq.getRowCount(rel))
                .append(", cumulative cost=")
                .append(getCost(rel, mq))
                .append('\n')
        }
        sb.append("}")
        LOGGER.trace(sb.toString())
    }

    // implement RelOptPlanner
    @Deprecated // to be removed before 2.0
    @Override
    fun registerMetadataProviders(list: List<RelMetadataProvider?>) {
        list.add(0, HepRelMetadataProvider())
    }

    // implement RelOptPlanner
    @Deprecated // to be removed before 2.0
    @Override
    fun getRelMetadataTimestamp(rel: RelNode?): Long {
        // TODO jvs 20-Apr-2006: This is overly conservative.  Better would be
        // to keep a timestamp per HepRelVertex, and update only affected
        // vertices and all ancestors on each transformation.
        return nTransformations.toLong()
    }

    @Override
    fun getMaterializations(): ImmutableList<RelOptMaterialization> {
        return ImmutableList.copyOf(materializations)
    }

    @Override
    fun addMaterialization(materialization: RelOptMaterialization?) {
        materializations.add(materialization)
    }

    companion object {
        private fun matchOperands(
            operand: RelOptRuleOperand,
            rel: RelNode?,
            bindings: List<RelNode>,
            nodeChildren: Map<RelNode, List<RelNode>>
        ): Boolean {
            if (!operand.matches(rel)) {
                return false
            }
            for (input in rel.getInputs()) {
                if (input !is HepRelVertex) {
                    // The graph could be partially optimized for materialized view. In that
                    // case, the input would be a RelNode and shouldn't be matched again here.
                    return false
                }
            }
            bindings.add(rel)
            @SuppressWarnings("unchecked") val childRels: List<HepRelVertex> = rel.getInputs()
            return when (operand.childPolicy) {
                ANY -> true
                UNORDERED -> {
                    // For each operand, at least one child must match. If
                    // matchAnyChildren, usually there's just one operand.
                    for (childOperand in operand.getChildOperands()) {
                        var match = false
                        for (childRel in childRels) {
                            match = matchOperands(
                                childOperand,
                                childRel.getCurrentRel(),
                                bindings,
                                nodeChildren
                            )
                            if (match) {
                                break
                            }
                        }
                        if (!match) {
                            return false
                        }
                    }
                    val children: List<RelNode> = ArrayList(childRels.size())
                    for (childRel in childRels) {
                        children.add(childRel.getCurrentRel())
                    }
                    nodeChildren.put(rel, children)
                    true
                }
                else -> {
                    val n: Int = operand.getChildOperands().size()
                    if (childRels.size() < n) {
                        return false
                    }
                    for (pair in Pair.zip(childRels, operand.getChildOperands())) {
                        val match = matchOperands(
                            pair.right,
                            pair.left.getCurrentRel(),
                            bindings,
                            nodeChildren
                        )
                        if (!match) {
                            return false
                        }
                    }
                    true
                }
            }
        }
    }
}
