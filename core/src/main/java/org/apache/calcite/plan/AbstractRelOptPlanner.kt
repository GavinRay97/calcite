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
package org.apache.calcite.plan

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.RelMetadataProvider
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexExecutor
import org.apache.calcite.util.CancelFlag
import org.apache.calcite.util.Pair
import org.apache.calcite.util.Util
import org.apache.calcite.util.trace.CalciteTrace
import com.google.common.collect.ImmutableList
import org.checkerframework.checker.initialization.qual.UnknownInitialization
import org.checkerframework.checker.nullness.qual.MonotonicNonNull
import org.checkerframework.dataflow.qual.Pure
import org.slf4j.Logger
import java.text.NumberFormat
import java.util.ArrayList
import java.util.Collections
import java.util.HashMap
import java.util.HashSet
import java.util.LinkedHashMap
import java.util.List
import java.util.Locale
import java.util.Map
import java.util.Objects
import java.util.Set
import java.util.concurrent.atomic.AtomicBoolean
import java.util.regex.Pattern
import org.apache.calcite.util.Static.RESOURCE

/**
 * Abstract base for implementations of the [RelOptPlanner] interface.
 */
abstract class AbstractRelOptPlanner protected constructor(
    costFactory: RelOptCostFactory?,
    @Nullable context: Context?
) : RelOptPlanner {
    //~ Instance fields --------------------------------------------------------
    /**
     * Maps rule description to rule, just to ensure that rules' descriptions
     * are unique.
     */
    protected val mapDescToRule: Map<String, RelOptRule> = LinkedHashMap()
    protected val costFactory: RelOptCostFactory

    @MonotonicNonNull
    private var listener: MulticastRelOptListener? = null

    @MonotonicNonNull
    private var ruleAttemptsListener: RuleAttemptsListener? = null

    @Nullable
    private var ruleDescExclusionFilter: Pattern? = null
    protected val cancelFlag: AtomicBoolean
    private val classes: Set<Class<out RelNode?>> = HashSet()
    private val conventions: Set<Convention> = HashSet()

    /** External context. Never null.  */
    protected val context: Context?

    @get:Nullable
    @get:Override
    @set:Override
    @Nullable
    var executor: RexExecutor? = null
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates an AbstractRelOptPlanner.
     */
    init {
        var context: Context? = context
        this.costFactory = Objects.requireNonNull(costFactory, "costFactory")
        if (context == null) {
            context = Contexts.empty()
        }
        this.context = context
        cancelFlag = context.maybeUnwrap(CancelFlag::class.java)
            .map { flag -> flag.atomicBoolean }
            .orElseGet { AtomicBoolean() }

        // Add abstract RelNode classes. No RelNodes will ever be registered with
        // these types, but some operands may use them.
        classes.add(RelNode::class.java)
        classes.add(RelSubset::class.java)
        if (RULE_ATTEMPTS_LOGGER.isDebugEnabled()) {
            ruleAttemptsListener = RuleAttemptsListener()
            addListener(ruleAttemptsListener)
        }
        addListener(RuleEventLogger())
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun clear() {
    }

    @Override
    fun getContext(): Context? {
        return context
    }

    @Override
    fun getCostFactory(): RelOptCostFactory {
        return costFactory
    }

    @SuppressWarnings("deprecation")
    @Override
    fun setCancelFlag(cancelFlag: CancelFlag?) {
        // ignored
    }

    /**
     * Checks to see whether cancellation has been requested, and if so, throws
     * an exception.
     */
    fun checkCancel() {
        if (cancelFlag.get()) {
            throw RESOURCE.preparationAborted().ex()
        }
    }

    @get:Override
    val rules: List<Any>
        get() = ImmutableList.copyOf(mapDescToRule.values())

    @Override
    fun addRule(rule: RelOptRule): Boolean {
        // Check that there isn't a rule with the same description
        val description: String = rule.toString()
        assert(description != null)
        val existingRule: RelOptRule = mapDescToRule.put(description, rule)
        return if (existingRule != null) {
            if (existingRule.equals(rule)) {
                false
            } else {
                // This rule has the same description as one previously
                // registered, yet it is not equal. You may need to fix the
                // rule's equals and hashCode methods.
                throw AssertionError(
                    "Rule's description should be unique; "
                            + "existing rule=" + existingRule + "; new rule=" + rule
                )
            }
        } else true
    }

    @Override
    fun removeRule(rule: RelOptRule): Boolean {
        val description: String = rule.toString()
        val removed: RelOptRule = mapDescToRule.remove(description)
        return removed != null
    }

    /**
     * Returns the rule with a given description.
     *
     * @param description Description
     * @return Rule with given description, or null if not found
     */
    @Nullable
    protected fun getRuleByDescription(description: String): RelOptRule? {
        return mapDescToRule[description]
    }

    @Override
    fun setRuleDescExclusionFilter(@Nullable exclusionFilter: Pattern?) {
        ruleDescExclusionFilter = exclusionFilter
    }

    /**
     * Determines whether a given rule is excluded by ruleDescExclusionFilter.
     *
     * @param rule rule to test
     * @return true iff rule should be excluded
     */
    fun isRuleExcluded(rule: RelOptRule): Boolean {
        return (ruleDescExclusionFilter != null
                && ruleDescExclusionFilter.matcher(rule.toString()).matches())
    }

    @Override
    fun chooseDelegate(): RelOptPlanner {
        return this
    }

    @Override
    fun addMaterialization(materialization: RelOptMaterialization?) {
        // ignore - this planner does not support materializations
    }

    @get:Override
    val materializations: List<Any>
        get() = ImmutableList.of()

    @Override
    fun addLattice(lattice: RelOptLattice?) {
        // ignore - this planner does not support lattices
    }

    @Override
    @Nullable
    fun getLattice(table: RelOptTable?): RelOptLattice? {
        // this planner does not support lattices
        return null
    }

    @Override
    fun registerSchema(schema: RelOptSchema?) {
    }

    @Deprecated // to be removed before 2.0
    @Override
    fun getRelMetadataTimestamp(rel: RelNode?): Long {
        return 0
    }

    @Override
    fun prune(rel: RelNode?) {
    }

    @Override
    fun registerClass(node: RelNode) {
        val clazz: Class<out RelNode?> = node.getClass()
        if (classes.add(clazz)) {
            onNewClass(node)
        }
        val convention: Convention = node.getConvention()
        if (convention != null && conventions.add(convention)) {
            convention.register(this)
        }
    }

    /** Called when a new class of [RelNode] is seen.  */
    protected fun onNewClass(node: RelNode) {
        node.register(this)
    }

    @Override
    fun emptyTraitSet(): RelTraitSet {
        return RelTraitSet.createEmpty()
    }

    @Override
    @Nullable
    fun getCost(rel: RelNode?, mq: RelMetadataQuery): RelOptCost {
        return mq.getCumulativeCost(rel)
    }

    @Deprecated // to be removed before 2.0
    @Override
    @Nullable
    fun getCost(rel: RelNode): RelOptCost {
        val mq: RelMetadataQuery = rel.getCluster().getMetadataQuery()
        return getCost(rel, mq)
    }

    @Override
    fun addListener(
        newListener: RelOptListener?
    ) {
        if (listener == null) {
            listener = MulticastRelOptListener()
        }
        listener.addListener(newListener)
    }

    @Deprecated // to be removed before 2.0
    @Override
    fun registerMetadataProviders(list: List<RelMetadataProvider?>?) {
    }

    @Override
    fun addRelTraitDef(relTraitDef: RelTraitDef?): Boolean {
        return false
    }

    @Override
    fun clearRelTraitDefs() {
    }

    @get:Override
    val relTraitDefs: List<Any>
        get() = ImmutableList.of()

    @Override
    fun onCopy(rel: RelNode?, newRel: RelNode?) {
        // do nothing
    }

    protected fun dumpRuleAttemptsInfo() {
        if (ruleAttemptsListener != null) {
            RULE_ATTEMPTS_LOGGER.debug("Rule Attempts Info for " + this.getClass().getSimpleName())
            RULE_ATTEMPTS_LOGGER.debug(ruleAttemptsListener.dump())
        }
    }

    /**
     * Fires a rule, taking care of tracing and listener notification.
     *
     * @param ruleCall description of rule call
     */
    protected fun fireRule(
        ruleCall: RelOptRuleCall
    ) {
        checkCancel()
        assert(ruleCall.getRule().matches(ruleCall))
        if (isRuleExcluded(ruleCall.getRule())) {
            LOGGER.debug(
                "call#{}: Rule [{}] not fired due to exclusion filter",
                ruleCall.id, ruleCall.getRule()
            )
            return
        }
        if (ruleCall.isRuleExcluded()) {
            LOGGER.debug(
                "call#{}: Rule [{}] not fired due to exclusion hint",
                ruleCall.id, ruleCall.getRule()
            )
            return
        }
        if (listener != null) {
            val event: RelOptListener.RuleAttemptedEvent = RuleAttemptedEvent(
                this,
                ruleCall.rel(0),
                ruleCall,
                true
            )
            listener.ruleAttempted(event)
        }
        ruleCall.getRule().onMatch(ruleCall)
        if (listener != null) {
            val event: RelOptListener.RuleAttemptedEvent = RuleAttemptedEvent(
                this,
                ruleCall.rel(0),
                ruleCall,
                false
            )
            listener.ruleAttempted(event)
        }
    }

    /**
     * Takes care of tracing and listener notification when a rule's
     * transformation is applied.
     *
     * @param ruleCall description of rule call
     * @param newRel   result of transformation
     * @param before   true before registration of new rel; false after
     */
    protected fun notifyTransformation(
        ruleCall: RelOptRuleCall?,
        newRel: RelNode?,
        before: Boolean
    ) {
        if (listener != null) {
            val event: RelOptListener.RuleProductionEvent = RuleProductionEvent(
                this,
                newRel,
                ruleCall,
                before
            )
            listener.ruleProductionSucceeded(event)
        }
    }

    /**
     * Takes care of tracing and listener notification when a rel is chosen as
     * part of the final plan.
     *
     * @param rel chosen rel
     */
    protected fun notifyChosen(rel: RelNode?) {
        LOGGER.debug("For final plan, using {}", rel)
        if (listener != null) {
            val event: RelOptListener.RelChosenEvent = RelChosenEvent(
                this,
                rel
            )
            listener.relChosen(event)
        }
    }

    /**
     * Takes care of tracing and listener notification when a rel equivalence is
     * detected.
     *
     * @param rel chosen rel
     */
    protected fun notifyEquivalence(
        rel: RelNode?,
        equivalenceClass: Object?,
        physical: Boolean
    ) {
        if (listener != null) {
            val event: RelOptListener.RelEquivalenceEvent = RelEquivalenceEvent(
                this,
                rel,
                equivalenceClass,
                physical
            )
            listener.relEquivalenceFound(event)
        }
    }

    /**
     * Takes care of tracing and listener notification when a rel is discarded.
     *
     * @param rel Discarded rel
     */
    protected fun notifyDiscard(rel: RelNode?) {
        if (listener != null) {
            val event: RelOptListener.RelDiscardedEvent = RelDiscardedEvent(
                this,
                rel
            )
            listener.relDiscarded(event)
        }
    }

    @Pure
    @Nullable
    fun getListener(): RelOptListener? {
        return listener
    }

    /** Returns sub-classes of relational expression.  */
    fun subClasses(
        clazz: Class<out RelNode?>
    ): Iterable<Class<out RelNode?>> {
        return Util.filter(classes) { c ->
            // RelSubset must be exact type, not subclass
            if (c === RelSubset::class.java) {
                return@filter c === clazz
            }
            clazz.isAssignableFrom(c)
        }
    }

    /** Listener for counting the attempts of each rule. Only enabled under DEBUG level. */
    private class RuleAttemptsListener internal constructor() : RelOptListener {
        private var beforeTimestamp: Long = 0
        private val ruleAttempts: Map<String, Pair<Long, Long>>

        init {
            ruleAttempts = HashMap()
        }

        @Override
        fun relEquivalenceFound(event: RelEquivalenceEvent?) {
        }

        @Override
        fun ruleAttempted(event: RuleAttemptedEvent) {
            if (event.isBefore()) {
                beforeTimestamp = System.nanoTime()
            } else {
                val elapsed: Long = (System.nanoTime() - beforeTimestamp) / 1000
                val rule: String = event.getRuleCall().getRule().toString()
                if (ruleAttempts.containsKey(rule)) {
                    val p: Pair<Long, Long> = ruleAttempts[rule]
                    ruleAttempts.put(rule, Pair.of(p.left + 1, p.right + elapsed))
                } else {
                    ruleAttempts.put(rule, Pair.of(1L, elapsed))
                }
            }
        }

        @Override
        fun ruleProductionSucceeded(event: RuleProductionEvent?) {
        }

        @Override
        fun relDiscarded(event: RelDiscardedEvent?) {
        }

        @Override
        fun relChosen(event: RelChosenEvent?) {
        }

        fun dump(): String {
            // Sort rules by number of attempts descending, then by rule elapsed time descending,
            // then by rule name ascending.
            val list: List<Map.Entry<String, Pair<Long, Long>>> = ArrayList(ruleAttempts.entrySet())
            Collections.sort(
                list
            ) { left, right ->
                var res: Int = right.getValue().left.compareTo(left.getValue().left)
                if (res == 0) {
                    res = right.getValue().right.compareTo(left.getValue().right)
                }
                if (res == 0) {
                    res = left.getKey().compareTo(right.getKey())
                }
                res
            }

            // Print out rule attempts and time
            val sb = StringBuilder()
            sb.append(
                String
                    .format(Locale.ROOT, "%n%-60s%20s%20s%n", "Rules", "Attempts", "Time (us)")
            )
            val usFormat: NumberFormat = NumberFormat.getNumberInstance(Locale.US)
            var totalAttempts: Long = 0
            var totalTime: Long = 0
            for (entry in list) {
                sb.append(
                    String.format(
                        Locale.ROOT, "%-60s%20s%20s%n",
                        entry.getKey(),
                        usFormat.format(entry.getValue().left),
                        usFormat.format(entry.getValue().right)
                    )
                )
                totalAttempts += entry.getValue().left
                totalTime += entry.getValue().right
            }
            sb.append(
                String.format(
                    Locale.ROOT, "%-60s%20s%20s%n",
                    "* Total",
                    usFormat.format(totalAttempts),
                    usFormat.format(totalTime)
                )
            )
            return sb.toString()
        }
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        /** Logger for rule attempts information.  */
        private val RULE_ATTEMPTS_LOGGER: Logger = CalciteTrace.getRuleAttemptsTracer()
    }
}
