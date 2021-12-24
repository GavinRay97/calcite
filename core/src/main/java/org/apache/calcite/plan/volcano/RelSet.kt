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

import org.apache.calcite.plan.Convention
import org.apache.calcite.plan.RelOptCluster
import org.apache.calcite.plan.RelOptListener
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.RelTrait
import org.apache.calcite.plan.RelTraitDef
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.Converter
import org.apache.calcite.rel.core.CorrelationId
import org.apache.calcite.rel.core.Spool
import org.apache.calcite.util.Pair
import org.apache.calcite.util.trace.CalciteTrace
import com.google.common.collect.ImmutableList
import org.checkerframework.checker.nullness.qual.MonotonicNonNull
import org.slf4j.Logger
import java.util.ArrayList
import java.util.HashSet
import java.util.List
import java.util.Set
import java.util.stream.Collectors
import org.apache.calcite.linq4j.Nullness.castNonNull
import java.util.Objects.requireNonNull

/**
 * A `RelSet` is an equivalence-set of expressions; that is, a set of
 * expressions which have identical semantics. We are generally interested in
 * using the expression which has the lowest cost.
 *
 *
 * All of the expressions in an `RelSet` have the same calling
 * convention.
 */
class RelSet(
    val id: Int,
    variablesPropagated: Set<CorrelationId>,
    variablesUsed: Set<CorrelationId>
) {
    //~ Instance fields --------------------------------------------------------
    val rels: List<RelNode> = ArrayList()

    /**
     * Relational expressions that have a subset in this set as a child. This
     * is a multi-set. If multiple relational expressions in this set have the
     * same parent, there will be multiple entries.
     */
    val parents: List<RelNode> = ArrayList()
    val subsets: List<RelSubset> = ArrayList()

    /**
     * Set to the superseding set when this is found to be equivalent to another
     * set.
     */
    @MonotonicNonNull
    var equivalentSet: RelSet? = null

    @MonotonicNonNull
    var rel: RelNode? = null

    /**
     * Exploring state of current RelSet.
     */
    @Nullable
    var exploringState: ExploringState? = null

    /**
     * Records conversions / enforcements that have happened on the
     * pair of derived and required traitset.
     */
    val conversions: Set<Pair<RelTraitSet, RelTraitSet>> = HashSet()

    /**
     * Variables that are set by relational expressions in this set
     * and available for use by parent and child expressions.
     */
    val variablesPropagated: Set<CorrelationId>

    /**
     * Variables that are used by relational expressions in this set.
     */
    val variablesUsed: Set<CorrelationId>

    /**
     * Reentrancy flag.
     */
    var inMetadataQuery = false

    //~ Constructors -----------------------------------------------------------
    init {
        this.variablesPropagated = variablesPropagated
        this.variablesUsed = variablesUsed
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Returns all of the [RelNode]s which reference [RelNode]s in
     * this set.
     */
    val parentRels: List<Any>
        get() = parents

    /**
     * Returns the child RelSet for the current set.
     */
    fun getChildSets(planner: VolcanoPlanner): Set<RelSet> {
        val childSets: Set<RelSet> = HashSet()
        for (node in rels) {
            if (node is Converter) {
                continue
            }
            for (child in node.getInputs()) {
                val childSet: RelSet = planner.equivRoot((child as RelSubset).getSet())
                if (childSet.id != id) {
                    childSets.add(childSet)
                }
            }
        }
        return childSets
    }

    /**
     * Returns all of the [RelNode]s contained by any subset of this set
     * (does not include the subset objects themselves).
     */
    val relsFromAllSubsets: List<Any>
        get() = rels

    @Nullable
    fun getSubset(traits: RelTraitSet?): RelSubset? {
        for (subset in subsets) {
            if (subset.getTraitSet().equals(traits)) {
                return subset
            }
        }
        return null
    }

    /**
     * Removes all references to a specific [RelNode] in both the subsets
     * and their parent relationships.
     */
    fun obliterateRelNode(rel: RelNode?) {
        parents.remove(rel)
    }

    /**
     * Adds a relational expression to a set, with its results available under a
     * particular calling convention. An expression may be in the set several
     * times with different calling conventions (and hence different costs).
     */
    fun add(rel: RelNode): RelSubset {
        assert(equivalentSet == null) { "adding to a dead set" }
        val traitSet: RelTraitSet = rel.getTraitSet().simplify()
        val subset: RelSubset = getOrCreateSubset(
            rel.getCluster(), traitSet, rel.isEnforcer()
        )
        subset.add(rel)
        return subset
    }

    /**
     * If the subset is required, convert delivered subsets to this subset.
     * Otherwise, convert this subset to required subsets in this RelSet.
     * The subset can be both required and delivered.
     */
    fun addConverters(
        subset: RelSubset, required: Boolean,
        useAbstractConverter: Boolean
    ) {
        val cluster: RelOptCluster = subset.getCluster()
        val others: List<RelSubset> = subsets.stream().filter { n -> if (required) n.isDelivered() else n.isRequired() }
            .collect(Collectors.toList())
        for (other in others) {
            assert(other.getTraitSet().size() === subset.getTraitSet().size())
            var from: RelSubset = subset
            var to: RelSubset = other
            if (required) {
                from = other
                to = subset
            }
            if (from === to || to.isEnforceDisabled()
                || (useAbstractConverter
                        && from.getConvention() != null && !from.getConvention().useAbstractConvertersForConversion(
                    from.getTraitSet(), to.getTraitSet()
                ))
            ) {
                continue
            }
            if (!conversions.add(Pair.of(from.getTraitSet(), to.getTraitSet()))) {
                continue
            }
            val difference: ImmutableList<RelTrait> = to.getTraitSet().difference(from.getTraitSet())
            var needsConverter = false
            for (fromTrait in difference) {
                val traitDef: RelTraitDef = fromTrait.getTraitDef()
                val toTrait: RelTrait = to.getTraitSet().getTrait(traitDef)
                if (toTrait == null || !traitDef.canConvert(
                        cluster.getPlanner(), fromTrait, toTrait
                    )
                ) {
                    needsConverter = false
                    break
                }
                if (!fromTrait.satisfies(toTrait)) {
                    needsConverter = true
                }
            }
            if (needsConverter) {
                val enforcer: RelNode?
                if (useAbstractConverter) {
                    enforcer = AbstractConverter(
                        cluster, from, null, to.getTraitSet()
                    )
                } else {
                    val convention: Convention = requireNonNull(
                        subset.getConvention()
                    ) { "convention is null for $subset" }
                    enforcer = convention.enforce(from, to.getTraitSet())
                }
                if (enforcer != null) {
                    cluster.getPlanner().register(enforcer, to)
                }
            }
        }
    }

    fun getOrCreateSubset(
        cluster: RelOptCluster, traits: RelTraitSet, required: Boolean
    ): RelSubset {
        var needsConverter = false
        val planner: VolcanoPlanner = cluster.getPlanner() as VolcanoPlanner
        var subset: RelSubset? = getSubset(traits)
        if (subset == null) {
            needsConverter = true
            subset = RelSubset(cluster, this, traits)

            // Need to first add to subset before adding the abstract
            // converters (for others->subset), since otherwise during
            // register() the planner will try to add this subset again.
            subsets.add(subset)
            if (planner.getListener() != null) {
                postEquivalenceEvent(planner, subset)
            }
        } else if (required && !subset.isRequired()
            || !required && !subset.isDelivered()
        ) {
            needsConverter = true
        }
        if (subset.getConvention() === Convention.NONE) {
            needsConverter = false
        } else if (required) {
            subset.setRequired()
        } else {
            subset.setDelivered()
        }
        if (needsConverter) {
            addConverters(subset, required, !planner.topDownOpt)
        }
        return subset
    }

    private fun postEquivalenceEvent(planner: VolcanoPlanner, rel: RelNode) {
        val listener: RelOptListener = planner.getListener() ?: return
        val event: RelOptListener.RelEquivalenceEvent = RelEquivalenceEvent(
            planner,
            rel,
            "equivalence class $id",
            false
        )
        listener.relEquivalenceFound(event)
    }

    /**
     * Adds an expression `rel` to this set, without creating a
     * [org.apache.calcite.plan.volcano.RelSubset]. (Called only from
     * [org.apache.calcite.plan.volcano.RelSubset.add].
     *
     * @param rel Relational expression
     */
    fun addInternal(rel: RelNode) {
        if (!rels.contains(rel)) {
            rels.add(rel)
            for (trait in rel.getTraitSet()) {
                assert(trait === trait.getTraitDef().canonize(trait))
            }
            val planner: VolcanoPlanner = rel.getCluster().getPlanner() as VolcanoPlanner
            if (planner.getListener() != null) {
                postEquivalenceEvent(planner, rel)
            }
        }
        if (this.rel == null) {
            this.rel = rel
        } else {
            // Row types must be the same, except for field names.
            RelOptUtil.verifyTypeEquivalence(
                this.rel,
                rel,
                this
            )
        }
    }

    /**
     * Merges `otherSet` into this RelSet.
     *
     *
     * One generally calls this method after discovering that two relational
     * expressions are equivalent, and hence the `RelSet`s they
     * belong to are equivalent also.
     *
     *
     * After this method completes, `otherSet` is obsolete, its
     * [.equivalentSet] member points to this RelSet, and this RelSet is
     * still alive.
     *
     * @param planner  Planner
     * @param otherSet RelSet which is equivalent to this one
     */
    fun mergeWith(
        planner: VolcanoPlanner,
        otherSet: RelSet
    ) {
        assert(this !== otherSet)
        assert(equivalentSet == null)
        assert(otherSet.equivalentSet == null)
        LOGGER.trace("Merge set#{} into set#{}", otherSet.id, id)
        otherSet.equivalentSet = this
        val cluster: RelOptCluster = castNonNull(rel).getCluster()

        // remove from table
        val existed: Boolean = planner.allSets.remove(otherSet)
        assert(existed) { "merging with a dead otherSet" }
        val changedRels: Set<RelNode> = HashSet()

        // merge subsets
        for (otherSubset in otherSet.subsets) {
            var subset: RelSubset? = null
            val otherTraits: RelTraitSet = otherSubset.getTraitSet()

            // If it is logical or delivered physical traitSet
            if (otherSubset.isDelivered() || !otherSubset.isRequired()) {
                subset = getOrCreateSubset(cluster, otherTraits, false)
            }

            // It may be required only, or both delivered and required,
            // in which case, register again.
            if (otherSubset.isRequired()) {
                subset = getOrCreateSubset(cluster, otherTraits, true)
            }
            assert(subset != null)
            if (subset!!.passThroughCache == null) {
                subset!!.passThroughCache = otherSubset.passThroughCache
            } else if (otherSubset.passThroughCache != null) {
                subset!!.passThroughCache.addAll(otherSubset.passThroughCache)
            }

            // collect RelSubset instances, whose best should be changed
            if (otherSubset.bestCost.isLt(subset!!.bestCost) && otherSubset.best != null) {
                changedRels.add(otherSubset.best)
            }
        }
        val parentRels: Set<RelNode> = HashSet(parents)
        for (otherRel in otherSet.rels) {
            if (otherRel !is Spool
                && !otherRel.isEnforcer()
                && parentRels.contains(otherRel)
            ) {
                // If otherRel is a enforcing operator e.g.
                // Sort, Exchange, do not prune it. Just in
                // case it is not marked as an enforcer.
                if (otherRel.getInputs().size() !== 1
                    || otherRel.getInput(0).getTraitSet()
                        .satisfies(otherRel.getTraitSet())
                ) {
                    planner.prune(otherRel)
                }
            }
            planner.reregister(this, otherRel)
        }
        assert(equivalentSet == null)

        // propagate the new best information from changed relNodes.
        for (rel in changedRels) {
            planner.propagateCostImprovements(rel)
        }

        // Update all rels which have a child in the other set, to reflect the
        // fact that the child has been renamed.
        //
        // Copy array to prevent ConcurrentModificationException.
        val previousParents: List<RelNode> = ImmutableList.copyOf(otherSet.parentRels)
        for (parentRel in previousParents) {
            planner.rename(parentRel)
        }

        // Renaming may have caused this set to merge with another. If so,
        // this set is now obsolete. There's no need to update the children
        // of this set - indeed, it could be dangerous.
        if (equivalentSet != null) {
            return
        }

        // Make sure the cost changes as a result of merging are propagated.
        for (parentRel in parentRels) {
            planner.propagateCostImprovements(parentRel)
        }
        assert(equivalentSet == null)

        // Each of the relations in the old set now has new parents, so
        // potentially new rules can fire. Check for rule matches, just as if
        // it were newly registered.  (This may cause rules which have fired
        // once to fire again.)
        for (rel in rels) {
            assert(planner.getSet(rel) === this)
            planner.fireRules(rel)
        }
        // Fire rule match on subsets as well
        for (subset in subsets) {
            planner.fireRules(subset)
        }
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * An enum representing exploring state of current RelSet.
     */
    internal enum class ExploringState {
        /**
         * The RelSet is exploring.
         * It means all possible rule matches are scheduled, but not fully applied.
         * This RelSet will refuse to explore again, but cannot provide a valid LB.
         */
        EXPLORING,

        /**
         * The RelSet is fully explored and is able to provide a valid LB.
         */
        EXPLORED
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        private val LOGGER: Logger = CalciteTrace.getPlannerTracer()
    }
}
