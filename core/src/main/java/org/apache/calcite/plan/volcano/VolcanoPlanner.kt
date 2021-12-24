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

import org.apache.calcite.config.CalciteConnectionConfig
import org.apache.calcite.config.CalciteSystemProperty
import org.apache.calcite.plan.AbstractRelOptPlanner
import org.apache.calcite.plan.Context
import org.apache.calcite.plan.Convention
import org.apache.calcite.plan.ConventionTraitDef
import org.apache.calcite.plan.RelDigest
import org.apache.calcite.plan.RelOptCost
import org.apache.calcite.plan.RelOptCostFactory
import org.apache.calcite.plan.RelOptLattice
import org.apache.calcite.plan.RelOptMaterialization
import org.apache.calcite.plan.RelOptMaterializations
import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelOptRuleOperand
import org.apache.calcite.plan.RelOptSchema
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.RelTrait
import org.apache.calcite.plan.RelTraitDef
import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.PhysicalNode
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.Converter
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.externalize.RelWriterImpl
import org.apache.calcite.rel.metadata.CyclicMetadataException
import org.apache.calcite.rel.metadata.RelMdUtil
import org.apache.calcite.rel.metadata.RelMetadataProvider
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.rules.SubstitutionRule
import org.apache.calcite.rel.rules.TransformationRule
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.runtime.Hook
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.util.Litmus
import org.apache.calcite.util.Pair
import org.apache.calcite.util.Util
import com.google.common.collect.ImmutableList
import com.google.common.collect.LinkedListMultimap
import com.google.common.collect.Multimap
import org.apiguardian.api.API
import org.checkerframework.checker.nullness.qual.EnsuresNonNull
import org.checkerframework.checker.nullness.qual.MonotonicNonNull
import org.checkerframework.checker.nullness.qual.PolyNull
import org.checkerframework.checker.nullness.qual.RequiresNonNull
import org.checkerframework.dataflow.qual.Pure
import java.io.PrintWriter
import java.io.StringWriter
import java.util.ArrayDeque
import java.util.ArrayList
import java.util.Deque
import java.util.HashMap
import java.util.HashSet
import java.util.IdentityHashMap
import java.util.LinkedHashMap
import java.util.List
import java.util.Map
import java.util.PriorityQueue
import java.util.Set
import java.util.regex.Matcher
import java.util.regex.Pattern
import org.apache.calcite.linq4j.Nullness.castNonNull
import java.util.Objects.requireNonNull

/**
 * VolcanoPlanner optimizes queries by transforming expressions selectively
 * according to a dynamic programming algorithm.
 */
class VolcanoPlanner @SuppressWarnings("method.invocation.invalid") constructor(
    @Nullable costFactory: RelOptCostFactory?,
    @Nullable externalContext: Context?
) : AbstractRelOptPlanner(
    if (costFactory == null) VolcanoCost.FACTORY else costFactory,
    externalContext
) {
    //~ Instance fields --------------------------------------------------------
    @MonotonicNonNull
    protected var root: RelSubset? = null

    /**
     * Operands that apply to a given class of [RelNode].
     *
     *
     * Any operand can be an 'entry point' to a rule call, when a RelNode is
     * registered which matches the operand. This map allows us to narrow down
     * operands based on the class of the RelNode.
     */
    private val classOperands: Multimap<Class<out RelNode?>, RelOptRuleOperand> = LinkedListMultimap.create()

    /**
     * List of all sets. Used only for debugging.
     */
    val allSets: List<RelSet> = ArrayList()

    /**
     * Canonical map from [digest][String] to the unique
     * [relational expression][RelNode] with that digest.
     */
    private val mapDigestToRel: Map<RelDigest, RelNode> = HashMap()

    /**
     * Map each registered expression ([RelNode]) to its equivalence set
     * ([RelSubset]).
     *
     *
     * We use an [IdentityHashMap] to simplify the process of merging
     * [RelSet] objects. Most [RelNode] objects are identified by
     * their digest, which involves the set that their child relational
     * expressions belong to. If those children belong to the same set, we have
     * to be careful, otherwise it gets incestuous.
     */
    private val mapRel2Subset: IdentityHashMap<RelNode, RelSubset> = IdentityHashMap()

    /**
     * The nodes to be pruned.
     *
     *
     * If a RelNode is pruned, all [RelOptRuleCall]s using it
     * are ignored, and future RelOptRuleCalls are not queued up.
     */
    val prunedNodes: Set<RelNode> = HashSet()

    /**
     * List of all schemas which have been registered.
     */
    private val registeredSchemas: Set<RelOptSchema> = HashSet()

    /**
     * A driver to manage rule and rule matches.
     */
    var ruleDriver: RuleDriver? = null

    /**
     * Holds the currently registered RelTraitDefs.
     */
    private val traitDefs: List<RelTraitDef> = ArrayList()
    private var nextSetId = 0

    @MonotonicNonNull
    private var originalRoot: RelNode? = null

    @Nullable
    private var rootConvention: Convention? = null

    /**
     * Whether the planner can accept new rules.
     */
    private var locked = false

    /**
     * Whether rels with Convention.NONE has infinite cost.
     */
    private var noneConventionHasInfiniteCost = true
    private val materializations: List<RelOptMaterialization> = ArrayList()

    /**
     * Map of lattices by the qualified name of their star table.
     */
    private val latticeByName: Map<List<String>, RelOptLattice> = LinkedHashMap()
    val provenanceMap: Map<RelNode, Provenance>
    val ruleCallStack: Deque<VolcanoRuleCall> = ArrayDeque()

    /** Zero cost, according to [.costFactory]. Not necessarily a
     * [org.apache.calcite.plan.volcano.VolcanoCost].  */
    val zeroCost: RelOptCost

    /** Infinite cost, according to [.costFactory]. Not necessarily a
     * [org.apache.calcite.plan.volcano.VolcanoCost].  */
    val infCost: RelOptCost

    /**
     * Whether to enable top-down optimization or not.
     */
    var topDownOpt: Boolean = CalciteSystemProperty.TOPDOWN_OPT.value()

    /**
     * Extra roots for explorations.
     */
    var explorationRoots: Set<RelSubset> = HashSet()
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a uninitialized `VolcanoPlanner`. To fully initialize
     * it, the caller must register the desired set of relations, rules, and
     * calling conventions.
     */
    constructor() : this(null, null) {}

    /**
     * Creates a uninitialized `VolcanoPlanner`. To fully initialize
     * it, the caller must register the desired set of relations, rules, and
     * calling conventions.
     */
    constructor(externalContext: Context?) : this(null, externalContext) {}

    /**
     * Creates a `VolcanoPlanner` with a given cost factory.
     */
    init {
        zeroCost = costFactory.makeZeroCost()
        infCost = costFactory.makeInfiniteCost()
        // If LOGGER is debug enabled, enable provenance information to be captured
        provenanceMap = if (LOGGER.isDebugEnabled()) HashMap() else Util.blackholeMap()
        initRuleQueue()
    }

    @EnsuresNonNull("ruleDriver")
    private fun initRuleQueue() {
        if (topDownOpt) {
            ruleDriver = TopDownRuleDriver(this)
        } else {
            ruleDriver = IterativeRuleDriver(this)
        }
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Enable or disable top-down optimization.
     *
     *
     * Note: Enabling top-down optimization will automatically enable
     * top-down trait propagation.
     */
    fun setTopDownOpt(value: Boolean) {
        if (topDownOpt == value) {
            return
        }
        topDownOpt = value
        initRuleQueue()
    }

    // implement RelOptPlanner
    @Override
    fun isRegistered(rel: RelNode?): Boolean {
        return mapRel2Subset.get(rel) != null
    }

    @Override
    fun setRoot(rel: RelNode) {
        root = registerImpl(rel, null)
        if (originalRoot == null) {
            originalRoot = rel
        }
        rootConvention = root.getConvention()
        ensureRootConverters()
    }

    @Pure
    @Override
    @Nullable
    fun getRoot(): RelNode? {
        return root
    }

    @Override
    fun getMaterializations(): List<RelOptMaterialization> {
        return ImmutableList.copyOf(materializations)
    }

    @Override
    fun addMaterialization(
        materialization: RelOptMaterialization?
    ) {
        materializations.add(materialization)
    }

    @Override
    fun addLattice(lattice: RelOptLattice) {
        latticeByName.put(lattice.starRelOptTable.getQualifiedName(), lattice)
    }

    @Override
    @Nullable
    fun getLattice(table: RelOptTable): RelOptLattice? {
        return latticeByName[table.getQualifiedName()]
    }

    protected fun registerMaterializations() {
        // Avoid using materializations while populating materializations!
        val config: CalciteConnectionConfig = context.unwrap(CalciteConnectionConfig::class.java)
        if (config == null || !config.materializationsEnabled()) {
            return
        }
        assert(root != null) { "root" }
        assert(originalRoot != null) { "originalRoot" }

        // Register rels using materialized views.
        val materializationUses: List<Pair<RelNode, List<RelOptMaterialization>>> =
            RelOptMaterializations.useMaterializedViews(originalRoot, materializations)
        for (use in materializationUses) {
            val rel: RelNode = use.left
            Hook.SUB.run(rel)
            registerImpl(rel, root.set)
        }

        // Register table rels of materialized views that cannot find a substitution
        // in root rel transformation but can potentially be useful.
        val applicableMaterializations: Set<RelOptMaterialization> = HashSet(
            RelOptMaterializations.getApplicableMaterializations(
                originalRoot, materializations
            )
        )
        for (use in materializationUses) {
            applicableMaterializations.removeAll(use.right)
        }
        for (materialization in applicableMaterializations) {
            val subset: RelSubset = registerImpl(materialization.queryRel, null)
            explorationRoots.add(subset)
            val tableRel2: RelNode = RelOptUtil.createCastRel(
                materialization.tableRel,
                materialization.queryRel.getRowType(),
                true
            )
            registerImpl(tableRel2, subset.set)
        }

        // Register rels using lattices.
        val latticeUses: List<Pair<RelNode, RelOptLattice>> = RelOptMaterializations.useLattices(
            originalRoot, ImmutableList.copyOf(latticeByName.values())
        )
        if (!latticeUses.isEmpty()) {
            val rel: RelNode = latticeUses[0].left
            Hook.SUB.run(rel)
            registerImpl(rel, root.set)
        }
    }

    /**
     * Finds an expression's equivalence set. If the expression is not
     * registered, returns null.
     *
     * @param rel Relational expression
     * @return Equivalence set that expression belongs to, or null if it is not
     * registered
     */
    @Nullable
    fun getSet(rel: RelNode?): RelSet? {
        assert(rel != null) { "pre: rel != null" }
        val subset: RelSubset = getSubset(rel)
        if (subset != null) {
            assert(subset.set != null)
            return subset.set
        }
        return null
    }

    @Override
    fun addRelTraitDef(relTraitDef: RelTraitDef): Boolean {
        return !traitDefs.contains(relTraitDef) && traitDefs.add(relTraitDef)
    }

    @Override
    fun clearRelTraitDefs() {
        traitDefs.clear()
    }

    @get:Override
    val relTraitDefs: List<Any>
        get() = traitDefs

    @Override
    fun emptyTraitSet(): RelTraitSet {
        var traitSet: RelTraitSet = super.emptyTraitSet()
        for (traitDef in traitDefs) {
            if (traitDef.multiple()) {
                // TODO: restructure RelTraitSet to allow a list of entries
                //  for any given trait
            }
            traitSet = traitSet.plus(traitDef.getDefault())
        }
        return traitSet
    }

    @Override
    fun clear() {
        super.clear()
        for (rule in getRules()) {
            removeRule(rule)
        }
        classOperands.clear()
        allSets.clear()
        mapDigestToRel.clear()
        mapRel2Subset.clear()
        prunedNodes.clear()
        ruleDriver.clear()
        materializations.clear()
        latticeByName.clear()
        provenanceMap.clear()
    }

    @Override
    fun addRule(rule: RelOptRule): Boolean {
        if (locked) {
            return false
        }
        if (!super.addRule(rule)) {
            return false
        }

        // Each of this rule's operands is an 'entry point' for a rule call.
        // Register each operand against all concrete sub-classes that could match
        // it.
        for (operand in rule.getOperands()) {
            for (subClass in subClasses(operand.getMatchedClass())) {
                if (PhysicalNode::class.java.isAssignableFrom(subClass)
                    && rule is TransformationRule
                ) {
                    continue
                }
                classOperands.put(subClass, operand)
            }
        }

        // If this is a converter rule, check that it operates on one of the
        // kinds of trait we are interested in, and if so, register the rule
        // with the trait.
        if (rule is ConverterRule) {
            val converterRule: ConverterRule = rule as ConverterRule
            val ruleTrait: RelTrait = converterRule.getInTrait()
            val ruleTraitDef: RelTraitDef = ruleTrait.getTraitDef()
            if (traitDefs.contains(ruleTraitDef)) {
                ruleTraitDef.registerConverterRule(this, converterRule)
            }
        }
        return true
    }

    @Override
    fun removeRule(rule: RelOptRule): Boolean {
        // Remove description.
        if (!super.removeRule(rule)) {
            // Rule was not present.
            return false
        }

        // Remove operands.
        classOperands.values().removeIf { entry -> entry.getRule().equals(rule) }

        // Remove trait mappings. (In particular, entries from conversion
        // graph.)
        if (rule is ConverterRule) {
            val converterRule: ConverterRule = rule as ConverterRule
            val ruleTrait: RelTrait = converterRule.getInTrait()
            val ruleTraitDef: RelTraitDef = ruleTrait.getTraitDef()
            if (traitDefs.contains(ruleTraitDef)) {
                ruleTraitDef.deregisterConverterRule(this, converterRule)
            }
        }
        return true
    }

    @Override
    protected fun onNewClass(node: RelNode) {
        super.onNewClass(node)
        val isPhysical = node is PhysicalNode
        // Create mappings so that instances of this class will match existing
        // operands.
        val clazz: Class<out RelNode?> = node.getClass()
        for (rule in mapDescToRule.values()) {
            if (isPhysical && rule is TransformationRule) {
                continue
            }
            for (operand in rule.getOperands()) {
                if (operand.getMatchedClass().isAssignableFrom(clazz)) {
                    classOperands.put(clazz, operand)
                }
            }
        }
    }

    @Override
    fun changeTraits(rel: RelNode, toTraits: RelTraitSet): RelNode {
        assert(!rel.getTraitSet().equals(toTraits))
        assert(toTraits.allSimple())
        val rel2: RelSubset = ensureRegistered(rel, null)
        return if (rel2.getTraitSet().equals(toTraits)) {
            rel2
        } else rel2.set.getOrCreateSubset(
            rel.getCluster(), toTraits, true
        )
    }

    @Override
    fun chooseDelegate(): RelOptPlanner {
        return this
    }

    /**
     * Finds the most efficient expression to implement the query given via
     * [org.apache.calcite.plan.RelOptPlanner.setRoot].
     *
     * @return the most efficient RelNode tree found for implementing the given
     * query
     */
    @Override
    fun findBestExp(): RelNode {
        assert(root != null) { "root must not be null" }
        ensureRootConverters()
        registerMaterializations()
        ruleDriver.drive()
        if (LOGGER.isTraceEnabled()) {
            val sw = StringWriter()
            val pw = PrintWriter(sw)
            dump(pw)
            pw.flush()
            LOGGER.info(sw.toString())
        }
        dumpRuleAttemptsInfo()
        val cheapest: RelNode = root.buildCheapestPlan(this)
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "Cheapest plan:\n{}", RelOptUtil.toString(cheapest, SqlExplainLevel.ALL_ATTRIBUTES)
            )
            if (!provenanceMap.isEmpty()) {
                LOGGER.debug("Provenance:\n{}", Dumpers.provenance(provenanceMap, cheapest))
            }
        }
        return cheapest
    }

    @Override
    fun checkCancel() {
        if (cancelFlag.get()) {
            throw VolcanoTimeoutException()
        }
    }

    /** Ensures that the subset that is the root relational expression contains
     * converters to all other subsets in its equivalence set.
     *
     *
     * Thus the planner tries to find cheap implementations of those other
     * subsets, which can then be converted to the root. This is the only place
     * in the plan where explicit converters are required; elsewhere, a consumer
     * will be asking for the result in a particular convention, but the root has
     * no consumers.  */
    @RequiresNonNull("root")
    fun ensureRootConverters() {
        val subsets: Set<RelSubset> = HashSet()
        for (rel in root.getRels()) {
            if (rel is AbstractConverter) {
                subsets.add((rel as AbstractConverter).getInput() as RelSubset)
            }
        }
        for (subset in root.set.subsets) {
            val difference: ImmutableList<RelTrait> = root.getTraitSet().difference(subset.getTraitSet())
            if (difference.size() === 1 && subsets.add(subset)) {
                register(
                    AbstractConverter(
                        subset.getCluster(), subset,
                        difference.get(0).getTraitDef(), root.getTraitSet()
                    ),
                    root
                )
            }
        }
    }

    @Override
    fun register(
        rel: RelNode,
        @Nullable equivRel: RelNode?
    ): RelSubset {
        var equivRel: RelNode? = equivRel
        assert(!isRegistered(rel)) { "pre: isRegistered(rel)" }
        val set: RelSet?
        if (equivRel == null) {
            set = null
        } else {
            val relType: RelDataType = rel.getRowType()
            val equivRelType: RelDataType = equivRel.getRowType()
            if (!RelOptUtil.areRowTypesEqual(
                    relType,
                    equivRelType, false
                )
            ) {
                throw IllegalArgumentException(
                    RelOptUtil.getFullTypeDifferenceString(
                        "rel rowtype", relType,
                        "equiv rowtype", equivRelType
                    )
                )
            }
            equivRel = ensureRegistered(equivRel, null)
            set = getSet(equivRel)
        }
        return registerImpl(rel, set)
    }

    @Override
    fun ensureRegistered(rel: RelNode, @Nullable equivRel: RelNode?): RelSubset {
        val result: RelSubset
        val subset: RelSubset = getSubset(rel)
        result = if (subset != null) {
            if (equivRel != null) {
                val equivSubset: RelSubset = getSubsetNonNull(equivRel)
                if (subset.set !== equivSubset.set) {
                    merge(equivSubset.set, subset.set)
                }
            }
            canonize(subset)
        } else {
            register(rel, equivRel)
        }

        // Checking if tree is valid considerably slows down planning
        // Only doing it if logger level is debug or finer
        if (LOGGER.isDebugEnabled()) {
            assert(isValid(Litmus.THROW))
        }
        return result
    }

    /**
     * Checks internal consistency.
     */
    protected fun isValid(litmus: Litmus): Boolean {
        val root: RelNode = getRoot() ?: return true
        val metaQuery: RelMetadataQuery = root.getCluster().getMetadataQuerySupplier().get()
        for (set in allSets) {
            if (set.equivalentSet != null) {
                return litmus.fail("set [{}] has been merged: it should not be in the list", set)
            }
            for (subset in set.subsets) {
                if (subset.set !== set) {
                    return litmus.fail(
                        "subset [{}] is in wrong set [{}]",
                        subset, set
                    )
                }
                if (subset.best != null) {

                    // Make sure best RelNode is valid
                    if (!subset.set.rels.contains(subset.best)) {
                        return litmus.fail(
                            "RelSubset [{}] does not contain its best RelNode [{}]",
                            subset, subset.best
                        )
                    }

                    // Make sure bestCost is up-to-date
                    try {
                        val bestCost: RelOptCost = getCostOrInfinite(subset.best, metaQuery)
                        if (!subset.bestCost.equals(bestCost)) {
                            return litmus.fail(
                                "RelSubset [" + subset
                                        + "] has wrong best cost "
                                        + subset.bestCost + ". Correct cost is " + bestCost
                            )
                        }
                    } catch (e: CyclicMetadataException) {
                        // ignore
                    }
                }
                for (rel in subset.getRels()) {
                    try {
                        val relCost: RelOptCost? = getCost(rel, metaQuery)
                        if (relCost != null && relCost.isLt(subset.bestCost)) {
                            return litmus.fail(
                                "rel [{}] has lower cost {} than "
                                        + "best cost {} of subset [{}]",
                                rel, relCost, subset.bestCost, subset
                            )
                        }
                    } catch (e: CyclicMetadataException) {
                        // ignore
                    }
                }
            }
        }
        return litmus.succeed()
    }

    fun registerAbstractRelationalRules() {
        RelOptUtil.registerAbstractRelationalRules(this)
    }

    @Override
    fun registerSchema(schema: RelOptSchema) {
        if (registeredSchemas.add(schema)) {
            try {
                schema.registerRules(this)
            } catch (e: Exception) {
                throw AssertionError("While registering schema $schema", e)
            }
        }
    }

    /**
     * Sets whether this planner should consider rel nodes with Convention.NONE
     * to have infinite cost or not.
     * @param infinite Whether to make none convention rel nodes infinite cost
     */
    fun setNoneConventionHasInfiniteCost(infinite: Boolean) {
        noneConventionHasInfiniteCost = infinite
    }

    /**
     * Returns cost of a relation or infinite cost if the cost is not known.
     * @param rel relation t
     * @param mq metadata query
     * @return cost of the relation or infinite cost if the cost is not known
     * @see org.apache.calcite.plan.volcano.RelSubset.bestCost
     */
    private fun getCostOrInfinite(rel: RelNode?, mq: RelMetadataQuery): RelOptCost {
        val cost: RelOptCost? = getCost(rel, mq)
        return if (cost == null) infCost else cost
    }

    @Override
    @Nullable
    fun getCost(rel: RelNode?, mq: RelMetadataQuery): RelOptCost? {
        assert(rel != null) { "pre-condition: rel != null" }
        if (rel is RelSubset) {
            return (rel as RelSubset?).bestCost
        }
        if (noneConventionHasInfiniteCost
            && rel.getTraitSet().getTrait(ConventionTraitDef.INSTANCE) === Convention.NONE
        ) {
            return costFactory.makeInfiniteCost()
        }
        var cost: RelOptCost = mq.getNonCumulativeCost(rel) ?: return null
        if (!zeroCost.isLt(cost)) {
            // cost must be positive, so nudge it
            cost = costFactory.makeTinyCost()
        }
        for (input in rel.getInputs()) {
            val inputCost: RelOptCost = getCost(input, mq) ?: return null
            cost = cost.plus(inputCost)
        }
        return cost
    }

    /**
     * Returns the subset that a relational expression belongs to.
     *
     * @param rel Relational expression
     * @return Subset it belongs to, or null if it is not registered
     */
    @Nullable
    fun getSubset(rel: RelNode): RelSubset {
        assert(rel != null) { "pre: rel != null" }
        return if (rel is RelSubset) {
            rel as RelSubset
        } else {
            mapRel2Subset.get(rel)
        }
    }

    /**
     * Returns the subset that a relational expression belongs to.
     *
     * @param rel Relational expression
     * @return Subset it belongs to, or null if it is not registered
     * @throws AssertionError in case subset is not found
     */
    @API(since = "1.26", status = API.Status.EXPERIMENTAL)
    fun getSubsetNonNull(rel: RelNode): RelSubset {
        return requireNonNull(getSubset(rel)) { "Subset is not found for $rel" }
    }

    @Nullable
    fun getSubset(rel: RelNode, traits: RelTraitSet?): RelSubset? {
        if (rel is RelSubset && rel.getTraitSet().equals(traits)) {
            return rel as RelSubset
        }
        val set: RelSet = getSet(rel) ?: return null
        return set.getSubset(traits)
    }

    @Nullable
    fun changeTraitsUsingConverters(
        rel: RelNode,
        toTraits: RelTraitSet
    ): RelNode? {
        val fromTraits: RelTraitSet = rel.getTraitSet()
        assert(fromTraits.size() >= toTraits.size())
        val allowInfiniteCostConverters: Boolean = CalciteSystemProperty.ALLOW_INFINITE_COST_CONVERTERS.value()

        // Traits may build on top of another...for example a collation trait
        // would typically come after a distribution trait since distribution
        // destroys collation; so when doing the conversion below we use
        // fromTraits as the trait of the just previously converted RelNode.
        // Also, toTraits may have fewer traits than fromTraits, excess traits
        // will be left as is.  Finally, any null entries in toTraits are
        // ignored.
        var converted: RelNode? = rel
        var i = 0
        while (converted != null && i < toTraits.size()) {
            val fromTrait: RelTrait = converted.getTraitSet().getTrait(i)
            val traitDef: RelTraitDef = fromTrait.getTraitDef()
            val toTrait: RelTrait = toTraits.getTrait(i)
            if (toTrait == null) {
                i++
                continue
            }
            assert(traitDef === toTrait.getTraitDef())
            if (fromTrait.satisfies(toTrait)) {
                // No need to convert; it's already correct.
                i++
                continue
            }
            val convertedRel: RelNode = traitDef.convert(
                this,
                converted,
                toTrait,
                allowInfiniteCostConverters
            )
            if (convertedRel != null) {
                assert(castNonNull(convertedRel.getTraitSet().getTrait(traitDef)).satisfies(toTrait))
                register(convertedRel, converted)
            }
            converted = convertedRel
            i++
        }

        // make sure final converted traitset subsumes what was required
        if (converted != null) {
            assert(converted.getTraitSet().satisfies(toTraits))
        }
        return converted
    }

    @Override
    fun prune(rel: RelNode?) {
        prunedNodes.add(rel)
    }

    /**
     * Dumps the internal state of this VolcanoPlanner to a writer.
     *
     * @param pw Print writer
     * @see .normalizePlan
     */
    fun dump(pw: PrintWriter) {
        pw.println("Root: $root")
        pw.println("Original rel:")
        if (originalRoot != null) {
            originalRoot.explain(
                RelWriterImpl(pw, SqlExplainLevel.ALL_ATTRIBUTES, false)
            )
        }
        try {
            if (CalciteSystemProperty.DUMP_SETS.value()) {
                pw.println()
                pw.println("Sets:")
                Dumpers.dumpSets(this, pw)
            }
            if (CalciteSystemProperty.DUMP_GRAPHVIZ.value()) {
                pw.println()
                pw.println("Graphviz:")
                Dumpers.dumpGraphviz(this, pw)
            }
        } catch (e: Exception) {
            pw.println(
                """
                    Error when dumping plan state:
                    $e
                    """.trimIndent()
            )
        } catch (e: AssertionError) {
            pw.println(
                """
                    Error when dumping plan state:
                    $e
                    """.trimIndent()
            )
        }
    }

    fun toDot(): String {
        val sw = StringWriter()
        val pw = PrintWriter(sw)
        Dumpers.dumpGraphviz(this, pw)
        pw.flush()
        return sw.toString()
    }

    /**
     * Re-computes the digest of a [RelNode].
     *
     *
     * Since a relational expression's digest contains the identifiers of its
     * children, this method needs to be called when the child has been renamed,
     * for example if the child's set merges with another.
     *
     * @param rel Relational expression
     */
    fun rename(rel: RelNode) {
        var oldDigest = ""
        if (LOGGER.isTraceEnabled()) {
            oldDigest = rel.getDigest()
        }
        if (fixUpInputs(rel)) {
            val newDigest: RelDigest = rel.getRelDigest()
            LOGGER.trace("Rename #{} from '{}' to '{}'", rel.getId(), oldDigest, newDigest)
            val equivRel: RelNode = mapDigestToRel.put(newDigest, rel)
            if (equivRel != null) {
                assert(equivRel !== rel)

                // There's already an equivalent with the same name, and we
                // just knocked it out. Put it back, and forget about 'rel'.
                LOGGER.trace(
                    "After renaming rel#{} it is now equivalent to rel#{}",
                    rel.getId(), equivRel.getId()
                )
                mapDigestToRel.put(newDigest, equivRel)
                checkPruned(equivRel, rel)
                val equivRelSubset: RelSubset = getSubsetNonNull(equivRel)

                // Remove back-links from children.
                for (input in rel.getInputs()) {
                    (input as RelSubset).set.parents.remove(rel)
                }

                // Remove rel from its subset. (This may leave the subset
                // empty, but if so, that will be dealt with when the sets
                // get merged.)
                val subset: RelSubset = mapRel2Subset.put(rel, equivRelSubset)
                assert(subset != null)
                val existed: Boolean = subset.set.rels.remove(rel)
                assert(existed) { "rel was not known to its set" }
                val equivSubset: RelSubset = getSubsetNonNull(equivRel)
                for (s in subset.set.subsets) {
                    if (s.best === rel) {
                        s.best = equivRel
                        // Propagate cost improvement since this potentially would change the subset's best cost
                        propagateCostImprovements(equivRel)
                    }
                }
                if (equivSubset !== subset) {
                    // The equivalent relational expression is in a different
                    // subset, therefore the sets are equivalent.
                    assert(
                        equivSubset.getTraitSet().equals(
                            subset.getTraitSet()
                        )
                    )
                    assert(equivSubset.set !== subset.set)
                    merge(equivSubset.set, subset.set)
                }
            }
        }
    }

    /**
     * Checks whether a relexp has made any subset cheaper, and if it so,
     * propagate new cost to parent rel nodes.
     *
     * @param rel       Relational expression whose cost has improved
     */
    fun propagateCostImprovements(rel: RelNode?) {
        val mq: RelMetadataQuery = rel.getCluster().getMetadataQuery()
        val propagateRels: Map<RelNode, RelOptCost> = HashMap()
        val propagateHeap: PriorityQueue<RelNode> = PriorityQueue label@{ o1, o2 ->
            val c1: RelOptCost? = propagateRels[o1]
            val c2: RelOptCost? = propagateRels[o2]
            if (c1 == null) {
                return@label if (c2 == null) 0 else -1
            }
            if (c2 == null) {
                return@label 1
            }
            if (c1.equals(c2)) {
                return@label 0
            } else if (c1.isLt(c2)) {
                return@label -1
            }
            1
        }
        propagateRels.put(rel, getCostOrInfinite(rel, mq))
        propagateHeap.offer(rel)
        var relNode: RelNode
        while (propagateHeap.poll().also { relNode = it } != null) {
            val cost: RelOptCost = requireNonNull(propagateRels[relNode], "propagateRels.get(relNode)")
            for (subset in getSubsetNonNull(relNode).set.subsets) {
                if (!relNode.getTraitSet().satisfies(subset.getTraitSet())) {
                    continue
                }
                if (!cost.isLt(subset.bestCost)) {
                    continue
                }
                // Update subset best cost when we find a cheaper rel or the current
                // best's cost is changed
                subset.timestamp++
                LOGGER.trace(
                    "Subset cost changed: subset [{}] cost was {} now {}",
                    subset, subset.bestCost, cost
                )
                subset.bestCost = cost
                subset.best = relNode
                // since best was changed, cached metadata for this subset should be removed
                mq.clearCache(subset)
                for (parent in subset.getParents()) {
                    mq.clearCache(parent)
                    val newCost: RelOptCost = getCostOrInfinite(parent, mq)
                    val existingCost: RelOptCost? = propagateRels[parent]
                    if (existingCost == null || newCost.isLt(existingCost)) {
                        propagateRels.put(parent, newCost)
                        if (existingCost != null) {
                            // Cost reduced, force the heap to adjust its ordering
                            propagateHeap.remove(parent)
                        }
                        propagateHeap.offer(parent)
                    }
                }
            }
        }
    }

    /**
     * Registers a [RelNode], which has already been registered, in a new
     * [RelSet].
     *
     * @param set Set
     * @param rel Relational expression
     */
    fun reregister(
        set: RelSet?,
        rel: RelNode
    ) {
        // Is there an equivalent relational expression? (This might have
        // just occurred because the relational expression's child was just
        // found to be equivalent to another set.)
        val equivRel: RelNode? = mapDigestToRel[rel.getRelDigest()]
        if (equivRel != null && equivRel !== rel) {
            assert(equivRel.getClass() === rel.getClass())
            assert(equivRel.getTraitSet().equals(rel.getTraitSet()))
            checkPruned(equivRel, rel)
            return
        }

        // Add the relational expression into the correct set and subset.
        if (!prunedNodes.contains(rel)) {
            addRelToSet(rel, set)
        }
    }

    /**
     * Prune rel node if the latter one (identical with rel node)
     * is already pruned.
     */
    private fun checkPruned(rel: RelNode, duplicateRel: RelNode) {
        if (prunedNodes.contains(duplicateRel)) {
            prunedNodes.add(rel)
        }
    }

    /**
     * Find the new root subset in case the root is merged with another subset.
     */
    @RequiresNonNull("root")
    fun canonize() {
        root = canonize(root)
    }

    /**
     * Fires all rules matched by a relational expression.
     *
     * @param rel      Relational expression which has just been created (or maybe
     * from the queue)
     */
    fun fireRules(rel: RelNode) {
        for (operand in classOperands.get(rel.getClass())) {
            if (operand.matches(rel)) {
                val ruleCall: VolcanoRuleCall
                ruleCall = DeferringRuleCall(this, operand)
                ruleCall.match(rel)
            }
        }
    }

    private fun fixUpInputs(rel: RelNode): Boolean {
        val inputs: List<RelNode> = rel.getInputs()
        val newInputs: List<RelNode> = ArrayList(inputs.size())
        var changeCount = 0
        for (input in inputs) {
            assert(input is RelSubset)
            val subset: RelSubset = input as RelSubset
            val newSubset: RelSubset = canonize(subset)
            newInputs.add(newSubset)
            if (newSubset !== subset) {
                if (subset.set !== newSubset.set) {
                    subset.set.parents.remove(rel)
                    newSubset.set.parents.add(rel)
                }
                changeCount++
            }
        }
        if (changeCount > 0) {
            RelMdUtil.clearCache(rel)
            val removed: RelNode = mapDigestToRel.remove(rel.getRelDigest())
            assert(removed === rel)
            for (i in 0 until inputs.size()) {
                rel.replaceInput(i, newInputs[i])
            }
            rel.recomputeDigest()
            return true
        }
        return false
    }

    private fun merge(set1: RelSet, set2: RelSet): RelSet {
        var set1: RelSet = set1
        var set2: RelSet = set2
        assert(set1 !== set2) { "pre: set1 != set2" }

        // Find the root of each set's equivalence tree.
        set1 = equivRoot(set1)
        set2 = equivRoot(set2)

        // If set1 and set2 are equivalent, there's nothing to do.
        if (set2 === set1) {
            return set1
        }

        // If necessary, swap the sets, so we're always merging the newer set
        // into the older or merging parent set into child set.
        val swap: Boolean
        val childrenOf1: Set<RelSet> = set1.getChildSets(this)
        val childrenOf2: Set<RelSet> = set2.getChildSets(this)
        val set2IsParentOfSet1 = childrenOf2.contains(set1)
        val set1IsParentOfSet2 = childrenOf1.contains(set2)
        swap = if (set2IsParentOfSet1 && set1IsParentOfSet2) {
            // There is a cycle of length 1; each set is the (direct) parent of the
            // other. Swap so that we are merging into the larger, older set.
            isSmaller(set1, set2)
        } else if (set2IsParentOfSet1) {
            // set2 is a parent of set1. Do not swap. We want to merge set2 into set.
            false
        } else if (set1IsParentOfSet2) {
            // set1 is a parent of set2. Swap, so that we merge set into set2.
            true
        } else {
            // Neither is a parent of the other.
            // Swap so that we are merging into the larger, older set.
            isSmaller(set1, set2)
        }
        if (swap) {
            val t: RelSet = set1
            set1 = set2
            set2 = t
        }

        // Merge.
        set1.mergeWith(this, set2)
        if (root == null) {
            throw IllegalStateException("root must not be null")
        }

        // Was the set we merged with the root? If so, the result is the new
        // root.
        if (set2 === getSet(root)) {
            root = set1.getOrCreateSubset(
                root.getCluster(), root.getTraitSet(), root.isRequired()
            )
            ensureRootConverters()
        }
        if (ruleDriver != null) {
            ruleDriver.onSetMerged(set1)
        }
        return set1
    }

    /**
     * Registers a new expression `exp` and queues up rule matches.
     * If `set` is not null, makes the expression part of that
     * equivalence set. If an identical expression is already registered, we
     * don't need to register this one and nor should we queue up rule matches.
     *
     * @param rel relational expression to register. Must be either a
     * [RelSubset], or an unregistered [RelNode]
     * @param set set that rel belongs to, or `null`
     * @return the equivalence-set
     */
    private fun registerImpl(
        rel: RelNode,
        @Nullable set: RelSet?
    ): RelSubset {
        var rel: RelNode = rel
        var set: RelSet? = set
        if (rel is RelSubset) {
            return registerSubset(set, rel as RelSubset)
        }
        assert(!isRegistered(rel)) { "already been registered: $rel" }
        if (rel.getCluster().getPlanner() !== this) {
            throw AssertionError(
                "Relational expression " + rel
                        + " belongs to a different planner than is currently being used."
            )
        }

        // Now is a good time to ensure that the relational expression
        // implements the interface required by its calling convention.
        val traits: RelTraitSet = rel.getTraitSet()
        val convention: Convention = traits.getTrait(ConventionTraitDef.INSTANCE)
        assert(convention != null)
        if (!convention.getInterface().isInstance(rel)
            && rel !is Converter
        ) {
            throw AssertionError(
                "Relational expression " + rel
                        + " has calling-convention " + convention
                        + " but does not implement the required interface '"
                        + convention.getInterface() + "' of that convention"
            )
        }
        if (traits.size() !== traitDefs.size()) {
            throw AssertionError(
                "Relational expression " + rel
                        + " does not have the correct number of traits: " + traits.size()
                        + " != " + traitDefs.size()
            )
        }

        // Ensure that its sub-expressions are registered.
        rel = rel.onRegister(this)

        // Record its provenance. (Rule call may be null.)
        val ruleCall: VolcanoRuleCall = ruleCallStack.peek()
        if (ruleCall == null) {
            provenanceMap.put(rel, Provenance.EMPTY)
        } else {
            provenanceMap.put(
                rel,
                RuleProvenance(
                    ruleCall.rule,
                    ImmutableList.copyOf(ruleCall.rels),
                    ruleCall.id
                )
            )
        }

        // If it is equivalent to an existing expression, return the set that
        // the equivalent expression belongs to.
        var digest: RelDigest = rel.getRelDigest()
        val equivExp: RelNode? = mapDigestToRel[digest]
        if (equivExp == null) {
            // do nothing
        } else if (equivExp === rel) {
            // The same rel is already registered, so return its subset
            return getSubsetNonNull(equivExp)
        } else {
            if (!RelOptUtil.areRowTypesEqual(
                    equivExp.getRowType(),
                    rel.getRowType(), false
                )
            ) {
                throw IllegalArgumentException(
                    RelOptUtil.getFullTypeDifferenceString(
                        "equiv rowtype",
                        equivExp.getRowType(), "rel rowtype", rel.getRowType()
                    )
                )
            }
            checkPruned(equivExp, rel)
            val equivSet: RelSet? = getSet(equivExp)
            if (equivSet != null) {
                LOGGER.trace(
                    "Register: rel#{} is equivalent to {}", rel.getId(), equivExp
                )
                return registerSubset(set, getSubsetNonNull(equivExp))
            }
        }

        // Converters are in the same set as their children.
        if (rel is Converter) {
            val input: RelNode = (rel as Converter).getInput()
            val childSet: RelSet = castNonNull(getSet(input))
            if (set != null
                && set !== childSet
                && set.equivalentSet == null
            ) {
                LOGGER.trace(
                    "Register #{} {} (and merge sets, because it is a conversion)",
                    rel.getId(), rel.getRelDigest()
                )
                merge(set, childSet)

                // During the mergers, the child set may have changed, and since
                // we're not registered yet, we won't have been informed. So
                // check whether we are now equivalent to an existing
                // expression.
                if (fixUpInputs(rel)) {
                    digest = rel.getRelDigest()
                    val equivRel: RelNode? = mapDigestToRel[digest]
                    if (equivRel !== rel && equivRel != null) {

                        // make sure this bad rel didn't get into the
                        // set in any way (fixupInputs will do this but it
                        // doesn't know if it should so it does it anyway)
                        set.obliterateRelNode(rel)

                        // There is already an equivalent expression. Use that
                        // one, and forget about this one.
                        return getSubsetNonNull(equivRel)
                    }
                }
            } else {
                set = childSet
            }
        }

        // Place the expression in the appropriate equivalence set.
        if (set == null) {
            set = RelSet(
                nextSetId++,
                Util.minus(
                    RelOptUtil.getVariablesSet(rel),
                    rel.getVariablesSet()
                ),
                RelOptUtil.getVariablesUsed(rel)
            )
            allSets.add(set)
        }

        // Chain to find 'live' equivalent set, just in case several sets are
        // merging at the same time.
        while (set.equivalentSet != null) {
            set = set.equivalentSet
        }

        // Allow each rel to register its own rules.
        registerClass(rel)
        val subsetBeforeCount: Int = set.subsets.size()
        val subset: RelSubset = addRelToSet(rel, set)
        val xx: RelNode = mapDigestToRel.putIfAbsent(digest, rel)
        LOGGER.trace("Register {} in {}", rel, subset)

        // This relational expression may have been registered while we
        // recursively registered its children. If this is the case, we're done.
        if (xx != null) {
            return subset
        }
        for (input in rel.getInputs()) {
            val childSubset: RelSubset = input as RelSubset
            childSubset.set.parents.add(rel)
        }

        // Queue up all rules triggered by this relexp's creation.
        fireRules(rel)

        // It's a new subset.
        if (set.subsets.size() > subsetBeforeCount
            || subset.triggerRule
        ) {
            fireRules(subset)
        }
        return subset
    }

    private fun addRelToSet(rel: RelNode, set: RelSet?): RelSubset {
        val subset: RelSubset = set.add(rel)
        mapRel2Subset.put(rel, subset)

        // While a tree of RelNodes is being registered, sometimes nodes' costs
        // improve and the subset doesn't hear about it. You can end up with
        // a subset with a single rel of cost 99 which thinks its best cost is
        // 100. We think this happens because the back-links to parents are
        // not established. So, give the subset another chance to figure out
        // its cost.
        try {
            propagateCostImprovements(rel)
        } catch (e: CyclicMetadataException) {
            // ignore
        }
        if (ruleDriver != null) {
            ruleDriver.onProduce(rel, subset)
        }
        return subset
    }

    private fun registerSubset(
        @Nullable set: RelSet?,
        subset: RelSubset
    ): RelSubset {
        if (set !== subset.set
            && set != null
            && set.equivalentSet == null
        ) {
            LOGGER.trace("Register #{} {}, and merge sets", subset.getId(), subset)
            merge(set, subset.set)
        }
        return canonize(subset)
    }

    // implement RelOptPlanner
    @Deprecated // to be removed before 2.0
    @Override
    fun registerMetadataProviders(list: List<RelMetadataProvider?>) {
        list.add(0, VolcanoRelMetadataProvider())
    }

    // implement RelOptPlanner
    @Deprecated // to be removed before 2.0
    @Override
    fun getRelMetadataTimestamp(rel: RelNode): Long {
        val subset: RelSubset = getSubset(rel)
        return if (subset == null) {
            0
        } else {
            subset.timestamp
        }
    }

    /**
     * Sets whether this planner is locked. A locked planner does not accept
     * new rules. [.addRule] will do
     * nothing and return false.
     *
     * @param locked Whether planner is locked
     */
    fun setLocked(locked: Boolean) {
        this.locked = locked
    }

    /**
     * Decide whether a rule is logical or not.
     * @param rel The specific rel node
     * @return True if the relnode is a logical node
     */
    @API(since = "1.24", status = API.Status.EXPERIMENTAL)
    fun isLogical(rel: RelNode): Boolean {
        return (rel !is PhysicalNode
                && rel.getConvention() !== rootConvention)
    }

    /**
     * Checks whether a rule match is a substitution rule match.
     *
     * @param match The rule match to check
     * @return True if the rule match is a substitution rule match
     */
    @API(since = "1.24", status = API.Status.EXPERIMENTAL)
    protected fun isSubstituteRule(match: VolcanoRuleCall): Boolean {
        return match.getRule() is SubstitutionRule
    }

    /**
     * Checks whether a rule match is a transformation rule match.
     *
     * @param match The rule match to check
     * @return True if the rule match is a transformation rule match
     */
    @API(since = "1.24", status = API.Status.EXPERIMENTAL)
    protected fun isTransformationRule(match: VolcanoRuleCall): Boolean {
        return match.getRule() is TransformationRule
    }

    /**
     * Gets the lower bound cost of a relational operator.
     *
     * @param rel The rel node
     * @return The lower bound cost of the given rel. The value is ensured NOT NULL.
     */
    @API(since = "1.24", status = API.Status.EXPERIMENTAL)
    protected fun getLowerBound(rel: RelNode): RelOptCost {
        val mq: RelMetadataQuery = rel.getCluster().getMetadataQuery()
        return mq.getLowerBoundCost(rel, this) ?: return zeroCost
    }

    /**
     * Gets the upper bound of its inputs.
     * Allow users to overwrite this method as some implementations may have
     * different cost model on some RelNodes, like Spool.
     */
    @API(since = "1.24", status = API.Status.EXPERIMENTAL)
    protected fun upperBoundForInputs(
        mExpr: RelNode, upperBound: RelOptCost
    ): RelOptCost {
        if (!upperBound.isInfinite()) {
            val rootCost: RelOptCost = mExpr.getCluster()
                .getMetadataQuery().getNonCumulativeCost(mExpr)
            if (rootCost != null && !rootCost.isInfinite()) {
                return upperBound.minus(rootCost)
            }
        }
        return upperBound
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * A rule call which defers its actions. Whereas [RelOptRuleCall]
     * invokes the rule when it finds a match, a `DeferringRuleCall`
     * creates a [VolcanoRuleMatch] which can be invoked later.
     */
    private class DeferringRuleCall internal constructor(
        planner: VolcanoPlanner,
        operand: RelOptRuleOperand
    ) : VolcanoRuleCall(planner, operand) {
        /**
         * Rather than invoking the rule (as the base method does), creates a
         * [VolcanoRuleMatch] which can be invoked later.
         */
        @Override
        protected override fun onMatch() {
            val match = VolcanoRuleMatch(
                volcanoPlanner,
                getOperand0(),
                rels,
                nodeInputs
            )
            volcanoPlanner.ruleDriver.getRuleQueue().addMatch(match)
        }
    }

    /**
     * Where a RelNode came from.
     */
    object Provenance {
        val EMPTY: Provenance = UnknownProvenance()
    }

    /**
     * We do not know where this RelNode came from. Probably created by hand,
     * or by sql-to-rel converter.
     */
    private class UnknownProvenance : Provenance()

    /**
     * A RelNode that came directly from another RelNode via a copy.
     */
    internal class DirectProvenance(source: RelNode) : Provenance() {
        val source: RelNode

        init {
            this.source = source
        }
    }

    /**
     * A RelNode that came via the firing of a rule.
     */
    internal class RuleProvenance(rule: RelOptRule, rels: ImmutableList<RelNode?>, callId: Int) : Provenance() {
        val rule: RelOptRule
        val rels: ImmutableList<RelNode>
        val callId: Int

        init {
            this.rule = rule
            this.rels = rels
            this.callId = callId
        }
    }

    companion object {
        /**
         * If a subset has one or more equivalent subsets (owing to a set having
         * merged with another), returns the subset which is the leader of the
         * equivalence class.
         *
         * @param subset Subset
         * @return Leader of subset's equivalence class
         */
        private fun canonize(subset: RelSubset): RelSubset {
            var set: RelSet = subset.set
            if (set.equivalentSet == null) {
                return subset
            }
            do {
                set = set.equivalentSet
            } while (set.equivalentSet != null)
            return set.getOrCreateSubset(
                subset.getCluster(), subset.getTraitSet(), subset.isRequired()
            )
        }

        /** Returns whether `set1` is less popular than `set2`
         * (or smaller, or younger). If so, it will be more efficient to merge set1
         * into set2 than set2 into set1.  */
        private fun isSmaller(set1: RelSet, set2: RelSet): Boolean {
            if (set1.parents.size() !== set2.parents.size()) {
                return set1.parents.size() < set2.parents.size() // true if set1 is less popular than set2
            }
            return if (set1.rels.size() !== set2.rels.size()) {
                set1.rels.size() < set2.rels.size() // true if set1 is smaller than set2
            } else set1.id > set2.id
            // true if set1 is younger than set2
        }

        fun equivRoot(s: RelSet): RelSet {
            var s: RelSet = s
            var p: RelSet = s // iterates at twice the rate, to detect cycles
            while (s.equivalentSet != null) {
                p = forward2(s, p)
                s = s.equivalentSet
            }
            return s
        }

        /** Moves forward two links, checking for a cycle at each.  */
        @Nullable
        private fun forward2(s: RelSet, @Nullable p: RelSet): RelSet {
            var p: RelSet = p
            p = forward1(s, p)
            p = forward1(s, p)
            return p
        }

        /** Moves forward one link, checking for a cycle.  */
        @Nullable
        private fun forward1(s: RelSet, @Nullable p: RelSet): RelSet {
            var p: RelSet = p
            if (p != null) {
                p = p.equivalentSet
                if (p === s) {
                    throw AssertionError("cycle in equivalence tree")
                }
            }
            return p
        }

        /**
         * Normalizes references to subsets within the string representation of a
         * plan.
         *
         *
         * This is useful when writing tests: it helps to ensure that tests don't
         * break when an extra rule is introduced that generates a new subset and
         * causes subsequent subset numbers to be off by one.
         *
         *
         * For example,
         *
         * <blockquote>
         * FennelAggRel.FENNEL_EXEC(child=Subset#17.FENNEL_EXEC,groupCount=1,
         * EXPR$1=COUNT())<br></br>
         * &nbsp;&nbsp;FennelSortRel.FENNEL_EXEC(child=Subset#2.FENNEL_EXEC,
         * key=[0], discardDuplicates=false)<br></br>
         * &nbsp;&nbsp;&nbsp;&nbsp;FennelCalcRel.FENNEL_EXEC(
         * child=Subset#4.FENNEL_EXEC, expr#0..8={inputs}, expr#9=3456,
         * DEPTNO=$t7, $f0=$t9)<br></br>
         * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;MockTableImplRel.FENNEL_EXEC(
         * table=[CATALOG, SALES, EMP])</blockquote>
         *
         *
         * becomes
         *
         * <blockquote>
         * FennelAggRel.FENNEL_EXEC(child=Subset#{0}.FENNEL_EXEC, groupCount=1,
         * EXPR$1=COUNT())<br></br>
         * &nbsp;&nbsp;FennelSortRel.FENNEL_EXEC(child=Subset#{1}.FENNEL_EXEC,
         * key=[0], discardDuplicates=false)<br></br>
         * &nbsp;&nbsp;&nbsp;&nbsp;FennelCalcRel.FENNEL_EXEC(
         * child=Subset#{2}.FENNEL_EXEC,expr#0..8={inputs},expr#9=3456,DEPTNO=$t7,
         * $f0=$t9)<br></br>
         * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;MockTableImplRel.FENNEL_EXEC(
         * table=[CATALOG, SALES, EMP])</blockquote>
         *
         *
         * Returns null if and only if `plan` is null.
         *
         * @param plan Plan
         * @return Normalized plan
         */
        @PolyNull
        fun normalizePlan(@PolyNull plan: String?): String? {
            var plan: String? = plan ?: return null
            val poundDigits: Pattern = Pattern.compile("Subset#[0-9]+\\.")
            var i = 0
            while (true) {
                val matcher: Matcher = poundDigits.matcher(plan)
                if (!matcher.find()) {
                    return plan
                }
                val token: String = matcher.group() // e.g. "Subset#23."
                plan = plan.replace(token, "Subset#{" + i++ + "}.")
            }
        }
    }
}
