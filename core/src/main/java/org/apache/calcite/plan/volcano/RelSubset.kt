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

import org.apache.calcite.linq4j.Linq4j
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptCost
import org.apache.calcite.plan.RelOptListener
import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.RelTrait
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.AbstractRelNode
import org.apache.calcite.rel.PhysicalNode
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.core.CorrelationId
import org.apache.calcite.rel.externalize.RelWriterImpl
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.util.Litmus
import org.apache.calcite.util.Pair
import org.apache.calcite.util.Util
import org.apache.calcite.util.trace.CalciteTrace
import com.google.common.collect.Sets
import org.apiguardian.api.API
import org.checkerframework.checker.initialization.qual.UnderInitialization
import org.checkerframework.checker.nullness.qual.EnsuresNonNull
import org.slf4j.Logger
import java.io.PrintWriter
import java.io.StringWriter
import java.util.ArrayList
import java.util.Collection
import java.util.Comparator
import java.util.HashSet
import java.util.LinkedHashSet
import java.util.List
import java.util.Map
import java.util.Set
import java.util.function.Function
import java.util.stream.Collectors
import java.util.stream.Stream
import org.apache.calcite.linq4j.Nullness.castNonNull
import java.util.Objects.requireNonNull

/**
 * Subset of an equivalence class where all relational expressions have the
 * same physical properties.
 *
 *
 * Physical properties are instances of the [RelTraitSet], and consist
 * of traits such as calling convention and collation (sort-order).
 *
 *
 * For some traits, a relational expression can have more than one instance.
 * For example, R can be sorted on both [X] and [Y, Z]. In which case, R would
 * belong to the sub-sets for [X] and [Y, Z]; and also the leading edges [Y] and
 * [].
 *
 * @see RelNode
 *
 * @see RelSet
 *
 * @see RelTrait
 */
class RelSubset internal constructor(
    cluster: RelOptCluster,
    set: RelSet,
    traits: RelTraitSet
) : AbstractRelNode(cluster, traits) {
    //~ Instance fields --------------------------------------------------------
    /** Optimization task state.  */
    @Nullable
    var taskState: OptimizeState? = null

    /** Cost of best known plan (it may have improved since).  */
    var bestCost: RelOptCost? = null

    /** The set this subset belongs to.  */
    val set: RelSet

    /** Best known plan.  */
    @Nullable
    var best: RelNode? = null

    /** Timestamp for metadata validity.  */
    var timestamp: Long = 0

    /**
     * Physical property state of current subset. Values:
     *
     *
     *  * 0: logical operators, NONE convention is neither DELIVERED nor REQUIRED
     *  * 1: traitSet DELIVERED from child operators or itself
     *  * 2: traitSet REQUIRED from parent operators
     *  * 3: both DELIVERED and REQUIRED
     *
     */
    private var state = 0

    /**
     * This subset should trigger rules when it becomes delivered.
     */
    var triggerRule = false

    /**
     * When the subset state is REQUIRED, whether enable property enforcing
     * between this subset and other delivered subsets. When it is true,
     * no enforcer operators will be added even if the other subset can't
     * satisfy current subset's required traitSet.
     */
    var isEnforceDisabled = false
        private set

    /**
     * The upper bound of the last OptimizeGroup call.
     */
    var upperBound: RelOptCost?

    /**
     * A cache that recognize which RelNode has invoked the passThrough method
     * so as to avoid duplicate invocation.
     */
    @Nullable
    var passThroughCache: Set<RelNode>? = null

    //~ Constructors -----------------------------------------------------------
    init {
        this.set = set
        assert(traits.allSimple())
        computeBestCost(cluster, cluster.getPlanner())
        upperBound = bestCost
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Computes the best [RelNode] in this subset.
     *
     *
     * Only necessary when a subset is created in a set that has subsets that
     * subsume it. Rationale:
     *
     *
     *  1. If the are no subsuming subsets, the subset is initially empty.
     *  1. After creation, `best` and `bestCost` are maintained
     * incrementally by [VolcanoPlanner.propagateCostImprovements] and
     * [RelSet.mergeWith].
     *
     */
    @EnsuresNonNull("bestCost")
    private fun computeBestCost(
        cluster: RelOptCluster,
        planner: RelOptPlanner
    ) {
        bestCost = planner.getCostFactory().makeInfiniteCost()
        val mq: RelMetadataQuery = cluster.getMetadataQuery()
        @SuppressWarnings("method.invocation.invalid") val rels: Iterable<RelNode> = rels
        for (rel in rels) {
            val cost: RelOptCost = planner.getCost(rel, mq) ?: continue
            if (cost.isLt(bestCost)) {
                bestCost = cost
                best = rel
            }
        }
    }

    fun setDelivered() {
        triggerRule = !isDelivered
        state = state or DELIVERED
    }

    fun setRequired() {
        triggerRule = false
        state = state or REQUIRED
    }

    @get:API(since = "1.23", status = API.Status.EXPERIMENTAL)
    val isDelivered: Boolean
        get() = state and DELIVERED == DELIVERED

    @get:API(since = "1.23", status = API.Status.EXPERIMENTAL)
    val isRequired: Boolean
        get() = state and REQUIRED == REQUIRED

    fun disableEnforcing() {
        assert(isDelivered)
        isEnforceDisabled = true
    }

    @Nullable
    fun getBest(): RelNode? {
        return best
    }

    @get:Nullable
    val original: RelNode?
        get() = set.rel

    @get:API(since = "1.27", status = API.Status.INTERNAL)
    val bestOrOriginal: RelNode
        get() {
            val result: RelNode? = getBest()
            return if (result != null) {
                result
            } else requireNonNull(original, "both best and original nodes are null")
        }

    @Override
    fun copy(traitSet: RelTraitSet, inputs: List<RelNode?>): RelNode {
        if (inputs.isEmpty()) {
            val traitSet1: RelTraitSet = traitSet.simplify()
            return if (traitSet1.equals(traitSet)) {
                this
            } else set.getOrCreateSubset(getCluster(), traitSet1, isRequired)
        }
        throw UnsupportedOperationException()
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner,
        mq: RelMetadataQuery?
    ): RelOptCost {
        return planner.getCostFactory().makeZeroCost()
    }

    @Override
    fun estimateRowCount(mq: RelMetadataQuery): Double {
        return if (best != null) {
            mq.getRowCount(best)
        } else {
            mq.getRowCount(castNonNull(set.rel))
        }
    }

    @Override
    fun explain(pw: RelWriter) {
        // Not a typical implementation of "explain". We don't gather terms &
        // values to be printed later. We actually do the work.
        pw.item("subset", toString())
        val input: AbstractRelNode = Util.first(getBest(), original) as AbstractRelNode ?: return
        input.explainTerms(pw)
        pw.done(input)
    }

    @Override
    fun deepEquals(@Nullable obj: Object): Boolean {
        return this === obj
    }

    @Override
    fun deepHashCode(): Int {
        return this.hashCode()
    }

    @Override
    protected fun deriveRowType(): RelDataType {
        return castNonNull(set.rel).getRowType()
    }// see usage of this method in propagateCostImprovements0()

    /**
     * Returns the collection of RelNodes one of whose inputs is in this
     * subset.
     */
    val parents: Set<Any>
        get() {
            val list: Set<RelNode> = LinkedHashSet()
            for (parent in set.getParentRels()) {
                for (rel in inputSubsets(parent)) {
                    // see usage of this method in propagateCostImprovements0()
                    if (rel === this) {
                        list.add(parent)
                    }
                }
            }
            return list
        }

    /**
     * Returns the collection of distinct subsets that contain a RelNode one
     * of whose inputs is in this subset.
     */
    fun getParentSubsets(planner: VolcanoPlanner): Set<RelSubset> {
        val list: Set<RelSubset> = LinkedHashSet()
        for (parent in set.getParentRels()) {
            for (rel in inputSubsets(parent)) {
                if (rel.set === set && rel.getTraitSet().equals(traitSet)) {
                    list.add(planner.getSubsetNonNull(parent))
                }
            }
        }
        return list
    }

    /**
     * Returns a list of relational expressions one of whose children is this
     * subset. The elements of the list are distinct.
     */
    val parentRels: Collection<Any>
        get() {
            val list: Set<RelNode> = LinkedHashSet()
            parentLoop@ for (parent in set.getParentRels()) {
                for (rel in inputSubsets(parent)) {
                    if (rel.set === set && traitSet.satisfies(rel.getTraitSet())) {
                        list.add(parent)
                        continue@parentLoop
                    }
                }
            }
            return list
        }

    fun getSet(): RelSet {
        return set
    }

    /**
     * Adds expression `rel` to this subset.
     */
    fun add(rel: RelNode) {
        if (set.rels.contains(rel)) {
            return
        }
        val planner: VolcanoPlanner = rel.getCluster().getPlanner() as VolcanoPlanner
        if (planner.getListener() != null) {
            val event: RelOptListener.RelEquivalenceEvent = RelEquivalenceEvent(
                planner,
                rel,
                this,
                true
            )
            planner.getListener().relEquivalenceFound(event)
        }

        // If this isn't the first rel in the set, it must have compatible
        // row type.
        if (set.rel != null) {
            RelOptUtil.equal(
                "rowtype of new rel", rel.getRowType(),
                "rowtype of set", getRowType(), Litmus.THROW
            )
        }
        set.addInternal(rel)
        if (false) {
            val variablesSet: Set<CorrelationId> = RelOptUtil.getVariablesSet(rel)
            val variablesStopped: Set<CorrelationId> = rel.getVariablesSet()
            val variablesPropagated: Set<CorrelationId> = Util.minus(variablesSet, variablesStopped)
            assert(set.variablesPropagated.containsAll(variablesPropagated))
            val variablesUsed: Set<CorrelationId> = RelOptUtil.getVariablesUsed(rel)
            assert(set.variablesUsed.containsAll(variablesUsed))
        }
    }

    /**
     * Recursively builds a tree consisting of the cheapest plan at each node.
     */
    fun buildCheapestPlan(planner: VolcanoPlanner): RelNode {
        val replacer = CheapestPlanReplacer(planner)
        val cheapest: RelNode = replacer.visit(this, -1, null)
        if (planner.getListener() != null) {
            val event: RelOptListener.RelChosenEvent = RelChosenEvent(
                planner,
                null
            )
            planner.getListener().relChosen(event)
        }
        return cheapest
    }

    @Override
    fun collectVariablesUsed(variableSet: Set<CorrelationId?>) {
        variableSet.addAll(set.variablesUsed)
    }

    @Override
    fun collectVariablesSet(variableSet: Set<CorrelationId?>) {
        variableSet.addAll(set.variablesPropagated)
    }

    /**
     * Returns the rel nodes in this rel subset.  All rels must have the same
     * traits and are logically equivalent.
     *
     * @return all the rels in the subset
     */
    val rels: Iterable<Any>
        get() = Iterable<RelNode> {
            Linq4j.asEnumerable(set.rels)
                .where { v1 -> v1.getTraitSet().satisfies(traitSet) }
                .iterator()
        }

    /**
     * As [.getRels] but returns a list.
     */
    val relList: List<Any>
        get() {
            val list: List<RelNode> = ArrayList()
            for (rel in set.rels) {
                if (rel.getTraitSet().satisfies(traitSet)) {
                    list.add(rel)
                }
            }
            return list
        }

    /**
     * Returns whether this subset contains the specified relational expression.
     */
    operator fun contains(node: RelNode): Boolean {
        return set.rels.contains(node) && node.getTraitSet().satisfies(traitSet)
    }

    /**
     * Returns stream of subsets whose traitset satisfies
     * current subset's traitset.
     */
    @get:API(since = "1.23", status = API.Status.EXPERIMENTAL)
    val subsetsSatisfyingThis: Stream<RelSubset>
        get() = set.subsets.stream()
            .filter { s -> s.getTraitSet().satisfies(traitSet) }

    /**
     * Returns stream of subsets whose traitset is satisfied
     * by current subset's traitset.
     */
    @get:API(since = "1.23", status = API.Status.EXPERIMENTAL)
    val satisfyingSubsets: Stream<RelSubset>
        get() = set.subsets.stream()
            .filter { s -> traitSet.satisfies(s.getTraitSet()) }// if bestCost != upperBound, it means optimize failed

    /**
     * Returns the best cost if this subset is fully optimized
     * or null if the subset is not fully optimized.
     */
    @get:Nullable
    @get:API(since = "1.24", status = API.Status.INTERNAL)
    val winnerCost: RelOptCost?
        get() = if (taskState == OptimizeState.COMPLETED && bestCost.isLe(upperBound)) {
            bestCost
        } else null

    // if bestCost != upperBound, it means optimize failed
    fun startOptimize(ub: RelOptCost?) {
        assert(winnerCost == null) { "$this is already optimized" }
        if (upperBound.isLt(ub)) {
            upperBound = ub
            if (bestCost.isLt(upperBound)) {
                upperBound = bestCost
            }
        }
        taskState = OptimizeState.OPTIMIZING
    }

    fun setOptimized() {
        taskState = OptimizeState.COMPLETED
    }

    fun resetTaskState(): Boolean {
        val optimized = taskState != null
        taskState = null
        upperBound = bestCost
        return optimized
    }

    @Nullable
    fun passThrough(rel: RelNode): RelNode? {
        if (rel !is PhysicalNode) {
            return null
        }
        if (passThroughCache == null) {
            passThroughCache = Sets.newIdentityHashSet()
            passThroughCache.add(rel)
        } else if (!passThroughCache.add(rel)) {
            return null
        }
        return (rel as PhysicalNode).passThrough(this.getTraitSet())
    }

    val isExplored: Boolean
        get() = set.exploringState === RelSet.ExploringState.EXPLORED

    fun explore(): Boolean {
        if (set.exploringState != null) {
            return false
        }
        set.exploringState = RelSet.ExploringState.EXPLORING
        return true
    }

    fun setExplored() {
        set.exploringState = RelSet.ExploringState.EXPLORED
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Identifies the leaf-most non-implementable nodes.
     */
    internal class DeadEndFinder {
        val deadEnds: Set<RelSubset> = HashSet()

        // To save time
        private val visitedNodes: Set<RelNode> = HashSet()

        // For cycle detection
        private val activeNodes: Set<RelNode> = HashSet()
        fun visit(p: RelNode): Boolean {
            if (p is RelSubset) {
                visitSubset(p as RelSubset)
                return false
            }
            return visitRel(p)
        }

        private fun visitSubset(subset: RelSubset) {
            val cheapest: RelNode? = subset.getBest()
            if (cheapest != null) {
                // Subset is implementable, and we are looking for bad ones, so stop here
                return
            }
            var isEmpty = true
            for (rel in subset.rels) {
                if (rel is AbstractConverter) {
                    // Converters are not implementable
                    continue
                }
                if (!activeNodes.add(rel)) {
                    continue
                }
                val res = visit(rel)
                isEmpty = isEmpty and res
                activeNodes.remove(rel)
            }
            if (isEmpty) {
                deadEnds.add(subset)
            }
        }

        /**
         * Returns true when input `RelNode` is cyclic.
         */
        private fun visitRel(p: RelNode): Boolean {
            // If one of the inputs is in "active" set, that means the rel forms a cycle,
            // then we just ignore it. Cyclic rels are not implementable.
            for (oldInput in p.getInputs()) {
                if (activeNodes.contains(oldInput)) {
                    return true
                }
            }
            // The same subset can be used multiple times (e.g. union all with the same inputs),
            // so it is important to perform "contains" and "add" in different loops
            activeNodes.addAll(p.getInputs())
            for (oldInput in p.getInputs()) {
                if (!visitedNodes.add(oldInput)) {
                    // We don't want to explore the same subset twice
                    continue
                }
                visit(oldInput)
            }
            activeNodes.removeAll(p.getInputs())
            return false
        }
    }

    @get:Override
    val digest: String
        get() = "RelSubset#" + set.id + '.' + getTraitSet()

    /**
     * Visitor which walks over a tree of [RelSet]s, replacing each node
     * with the cheapest implementation of the expression.
     */
    internal class CheapestPlanReplacer(planner: VolcanoPlanner) {
        var planner: VolcanoPlanner

        init {
            this.planner = planner
        }

        fun visit(
            p: RelNode,
            ordinal: Int,
            @Nullable parent: RelNode?
        ): RelNode {
            var p: RelNode = p
            if (p is RelSubset) {
                val subset = p as RelSubset
                val cheapest: RelNode? = subset.best
                if (cheapest == null) {
                    // Dump the planner's expression pool so we can figure
                    // out why we reached impasse.
                    val sw = StringWriter()
                    val pw = PrintWriter(sw)
                    pw.print("There are not enough rules to produce a node with desired properties")
                    val desiredTraits: RelTraitSet = subset.getTraitSet()
                    var sep = ": "
                    for (trait in desiredTraits) {
                        pw.print(sep)
                        pw.print(trait.getTraitDef().getSimpleName())
                        pw.print("=")
                        pw.print(trait)
                        sep = ", "
                    }
                    pw.print(".")
                    val finder = DeadEndFinder()
                    finder.visit(subset)
                    if (finder.deadEnds.isEmpty()) {
                        pw.print(" All the inputs have relevant nodes, however the cost is still infinite.")
                    } else {
                        val problemCounts: Map<String, Long> = finder.deadEnds.stream()
                            .filter { deadSubset -> deadSubset.getOriginal() != null }
                            .map { x ->
                                val original: RelNode = castNonNull(x.getOriginal())
                                (original.getClass().getSimpleName()
                                        + traitDiff(original.getTraitSet(), x.getTraitSet()))
                            }
                            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                        // Sort problems from most often to less often ones
                        val problems: String = problemCounts.entrySet().stream()
                            .sorted(Comparator.comparingLong(Map.Entry<String?, Long?>?::getValue).reversed())
                            .map { e ->
                                e.getKey() + if (e.getValue() > 1) " (" + e.getValue().toString() + " cases)" else ""
                            }
                            .collect(Collectors.joining(", "))
                        pw.println()
                        pw.print("Missing conversion")
                        pw.print(if (finder.deadEnds.size() === 1) " is " else "s are ")
                        pw.print(problems)
                        pw.println()
                        if (finder.deadEnds.size() === 1) {
                            pw.print("There is 1 empty subset: ")
                        }
                        if (finder.deadEnds.size() > 1) {
                            pw.println("There are " + finder.deadEnds.size().toString() + " empty subsets:")
                        }
                        var i = 0
                        var rest: Int = finder.deadEnds.size()
                        for (deadEnd in finder.deadEnds) {
                            if (finder.deadEnds.size() > 1) {
                                pw.print("Empty subset ")
                                pw.print(i)
                                pw.print(": ")
                            }
                            pw.print(deadEnd)
                            pw.println(", the relevant part of the original plan is as follows")
                            val original: RelNode? = deadEnd.original
                            if (original != null) {
                                original.explain(
                                    RelWriterImpl(pw, SqlExplainLevel.EXPPLAN_ATTRIBUTES, true)
                                )
                            }
                            i++
                            rest--
                            if (rest > 0) {
                                pw.println()
                            }
                            if (i >= 10 && rest > 1) {
                                pw.print("The rest ")
                                pw.print(rest)
                                pw.println(" leafs are omitted.")
                                break
                            }
                        }
                    }
                    pw.println()
                    planner.dump(pw)
                    pw.flush()
                    val dump: String = sw.toString()
                    val e: RuntimeException = CannotPlanException(dump)
                    LOGGER.trace("Caught exception in class={}, method=visit", getClass().getName(), e)
                    throw e
                }
                p = cheapest
            }
            if (ordinal != -1) {
                if (planner.getListener() != null) {
                    val event: RelOptListener.RelChosenEvent = RelChosenEvent(
                        planner,
                        p
                    )
                    planner.getListener().relChosen(event)
                }
            }
            val oldInputs: List<RelNode> = p.getInputs()
            val inputs: List<RelNode> = ArrayList()
            for (i in 0 until oldInputs.size()) {
                val oldInput: RelNode = oldInputs[i]
                val input: RelNode = visit(oldInput, i, p)
                inputs.add(input)
            }
            if (!inputs.equals(oldInputs)) {
                val pOld: RelNode = p
                p = p.copy(p.getTraitSet(), inputs)
                planner.provenanceMap.put(
                    p, DirectProvenance(pOld)
                )
            }
            return p
        }

        companion object {
            private fun traitDiff(original: RelTraitSet, desired: RelTraitSet): String {
                return Pair.zip(original, desired)
                    .stream()
                    .filter { p -> !p.left.satisfies(p.right) }
                    .map { p -> p.left.getTraitDef().getSimpleName() + ": " + p.left + " -> " + p.right }
                    .collect(Collectors.joining(", ", "[", "]"))
            }
        }
    }

    /** State of optimizer.  */
    enum class OptimizeState {
        OPTIMIZING, COMPLETED
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        private val LOGGER: Logger = CalciteTrace.getPlannerTracer()
        private const val DELIVERED = 1
        private const val REQUIRED = 2
        private fun inputSubsets(parent: RelNode): List<RelSubset> {
            return parent.getInputs() as List<RelSubset>
        }
    }
}
