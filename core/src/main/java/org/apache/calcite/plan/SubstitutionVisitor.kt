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

import org.apache.calcite.config.CalciteSystemProperty

/**
 * Substitutes part of a tree of relational expressions with another tree.
 *
 *
 * The call `new SubstitutionVisitor(target, query).go(replacement))`
 * will return `query` with every occurrence of `target` replaced
 * by `replacement`.
 *
 *
 * The following example shows how `SubstitutionVisitor` can be used
 * for materialized view recognition.
 *
 *
 *  * query = SELECT a, c FROM t WHERE x = 5 AND b = 4
 *  * target = SELECT a, b, c FROM t WHERE x = 5
 *  * replacement = SELECT * FROM mv
 *  * result = SELECT a, c FROM mv WHERE b = 4
 *
 *
 *
 * Note that `result` uses the materialized view table `mv` and a
 * simplified condition `b = 4`.
 *
 *
 * Uses a bottom-up matching algorithm. Nodes do not need to be identical.
 * At each level, returns the residue.
 *
 *
 * The inputs must only include the core relational operators:
 * [org.apache.calcite.rel.core.TableScan],
 * [org.apache.calcite.rel.core.Filter],
 * [org.apache.calcite.rel.core.Project],
 * [org.apache.calcite.rel.core.Calc],
 * [org.apache.calcite.rel.core.Join],
 * [org.apache.calcite.rel.core.Union],
 * [org.apache.calcite.rel.core.Intersect],
 * [org.apache.calcite.rel.core.Aggregate].
 */
class SubstitutionVisitor(
    target_: RelNode, query_: RelNode?,
    rules: ImmutableList<UnifyRule?>, relBuilderFactory: RelBuilderFactory
) {
    /**
     * Factory for a builder for relational expressions.
     */
    protected val relBuilder: RelBuilder
    private val rules: ImmutableList<UnifyRule>
    private val ruleMap: Map<Pair<Class, Class>, List<UnifyRule>> = HashMap()
    private val cluster: RelOptCluster
    private val simplify: RexSimplify
    private val query: Holder
    private val target: MutableRel

    /**
     * Nodes in [.target] that have no children.
     */
    val targetLeaves: List<MutableRel>

    /**
     * Nodes in [.query] that have no children.
     */
    val queryLeaves: List<MutableRel>
    val replacementMap: Map<MutableRel, MutableRel> = HashMap()
    val equivalents: Multimap<MutableRel, MutableRel> = LinkedHashMultimap.create()

    /** Workspace while rule is being matched.
     * Careful, re-entrant!
     * Assumes no rule needs more than 2 slots.  */
    protected val slots: Array<MutableRel?> = arrayOfNulls<MutableRel>(2)

    /** Creates a SubstitutionVisitor with the default rule set.  */
    constructor(target_: RelNode, query_: RelNode?) : this(
        target_,
        query_,
        DEFAULT_RULES,
        RelFactories.LOGICAL_BUILDER
    ) {
    }

    /** Creates a SubstitutionVisitor with the default logical builder.  */
    constructor(
        target_: RelNode, query_: RelNode?,
        rules: ImmutableList<UnifyRule?>
    ) : this(target_, query_, rules, RelFactories.LOGICAL_BUILDER) {
    }

    init {
        cluster = target_.getCluster()
        val executor: RexExecutor = Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR)
        val predicates: RelOptPredicateList = RelOptPredicateList.EMPTY
        simplify = RexSimplify(cluster.getRexBuilder(), predicates, executor)
        this.rules = rules
        query = Holder.of(MutableRels.toMutable(query_))
        target = MutableRels.toMutable(target_)
        relBuilder = relBuilderFactory.create(cluster, null)
        val parents: Set<MutableRel> = Sets.newIdentityHashSet()
        val allNodes: List<MutableRel> = ArrayList()
        val visitor: MutableRelVisitor = object : MutableRelVisitor() {
            @Override
            fun visit(@Nullable node: MutableRel) {
                requireNonNull(node, "node")
                parents.add(node.getParent())
                allNodes.add(node)
                super.visit(node)
            }
        }
        visitor.go(target)

        // Populate the list of leaves in the tree under "target".
        // Leaves are all nodes that are not parents.
        // For determinism, it is important that the list is in scan order.
        allNodes.removeAll(parents)
        targetLeaves = ImmutableList.copyOf(allNodes)
        allNodes.clear()
        parents.clear()
        visitor.go(query)
        allNodes.removeAll(parents)
        queryLeaves = ImmutableList.copyOf(allNodes)
    }

    fun register(result: MutableRel?, query: MutableRel?) {}
    @Nullable
    fun go0(replacement_: RelNode?): RelNode? {
        assert(
            false // not called
        )
        val replacement: MutableRel = MutableRels.toMutable(replacement_)
        assert(
            equalType(
                "target", target, "replacement", replacement, Litmus.THROW
            )
        )
        replacementMap.put(target, replacement)
        val unifyResult = matchRecurse(target) ?: return null
        val node0: MutableRel = unifyResult.result
        val node: MutableRel = node0 // replaceAncestors(node0);
        if (DEBUG) {
            System.out.println(
                """
    Convert: query:
    ${query.deep()}
    """.trimIndent() + "\nunify.query:\n"
                        + unifyResult.call.query.deep()
                    .toString() + "\nunify.result:\n"
                        + unifyResult.result.deep()
                    .toString() + "\nunify.target:\n"
                        + unifyResult.call.target.deep()
                    .toString() + "\nnode0:\n"
                        + node0.deep()
                    .toString() + "\nnode:\n"
                        + node.deep()
            )
        }
        return MutableRels.fromMutable(node, relBuilder)
    }

    /**
     * Returns a list of all possible rels that result from substituting the
     * matched RelNode with the replacement RelNode within the query.
     *
     *
     * For example, the substitution result of A join B, while A and B
     * are both a qualified match for replacement R, is R join B, R join R,
     * A join R.
     */
    @SuppressWarnings("MixedMutabilityReturnType")
    fun go(replacement_: RelNode?): List<RelNode> {
        val matches: List<List<Replacement>> = go(MutableRels.toMutable(replacement_))
        if (matches.isEmpty()) {
            return ImmutableList.of()
        }
        val sub: List<RelNode> = ArrayList()
        sub.add(MutableRels.fromMutable(query.getInput(), relBuilder))
        reverseSubstitute(relBuilder, query, matches, sub, 0, matches.size())
        return sub
    }

    /**
     * Substitutes the query with replacement whenever possible but meanwhile
     * keeps track of all the substitutions and their original rel before
     * replacement, so that in later processing stage, the replacement can be
     * recovered individually to produce a list of all possible rels with
     * substitution in different places.
     */
    private fun go(replacement: MutableRel): List<List<Replacement>> {
        assert(
            equalType(
                "target", target, "replacement", replacement, Litmus.THROW
            )
        )
        val queryDescendants: List<MutableRel> = MutableRels.descendants(query)
        val targetDescendants: List<MutableRel> = MutableRels.descendants(target)

        // Populate "equivalents" with (q, t) for each query descendant q and
        // target descendant t that are equal.
        val map: Map<MutableRel, MutableRel> = HashMap()
        for (queryDescendant in queryDescendants) {
            map.put(queryDescendant, queryDescendant)
        }
        for (targetDescendant in targetDescendants) {
            val queryDescendant: MutableRel? = map[targetDescendant]
            if (queryDescendant != null) {
                assert(
                    rowTypesAreEquivalent(
                        queryDescendant, targetDescendant, Litmus.THROW
                    )
                )
                equivalents.put(queryDescendant, targetDescendant)
            }
        }
        map.clear()
        val attempted: List<Replacement> = ArrayList()
        val substitutions: List<List<Replacement>> = ArrayList()
        while (true) {
            var count = 0
            var queryDescendant: MutableRel? = query
            outer@ while (queryDescendant != null) {
                for (r in attempted) {
                    if (r.stopTrying && queryDescendant === r.after) {
                        // This node has been replaced by previous iterations in the
                        // hope to match its ancestors and stopTrying indicates
                        // there's no need to be matched again.
                        queryDescendant = MutableRels.preOrderTraverseNext(queryDescendant)
                        continue@outer
                    }
                }
                val next: MutableRel = MutableRels.preOrderTraverseNext(queryDescendant)
                val childOrNext: MutableRel =
                    if (queryDescendant.getInputs().isEmpty()) next else queryDescendant.getInputs().get(0)
                for (targetDescendant in targetDescendants) {
                    for (rule in applicableRules(queryDescendant, targetDescendant)) {
                        val call = rule.match(this, queryDescendant, targetDescendant)
                        if (call != null) {
                            val result = rule.apply(call)
                            if (result != null) {
                                ++count
                                attempted.add(
                                    Replacement(result.call.query, result.result, result.stopTrying)
                                )
                                result.call.query.replaceInParent(result.result)

                                // Replace previous equivalents with new equivalents, higher up
                                // the tree.
                                for (i in 0 until rule.slotCount) {
                                    val equi: Collection<MutableRel> = equivalents.get(slots[i])
                                    if (!equi.isEmpty()) {
                                        equivalents.remove(slots[i], equi.iterator().next())
                                    }
                                }
                                assert(rowTypesAreEquivalent(result.result, result.call.query, Litmus.THROW))
                                equivalents.put(result.result, result.call.query)
                                if (targetDescendant === target) {
                                    // A real substitution happens. We purge the attempted
                                    // replacement list and add them into substitution list.
                                    // Meanwhile we stop matching the descendants and jump
                                    // to the next subtree in pre-order traversal.
                                    if (!target.equals(replacement)) {
                                        val r = replace(
                                            query.getInput(), target, replacement.clone()
                                        )
                                        assert(r != null) { rule.toString() + "should have returned a result containing the target." }
                                        attempted.add(r)
                                    }
                                    substitutions.add(ImmutableList.copyOf(attempted))
                                    attempted.clear()
                                    queryDescendant = next
                                    continue@outer
                                }
                                // We will try walking the query tree all over again to see
                                // if there can be any substitutions after the replacement
                                // attempt.
                                break@outer
                            }
                        }
                    }
                }
                queryDescendant = childOrNext
            }
            // Quit the entire loop if:
            // 1) we have walked the entire query tree with one or more successful
            //    substitutions, thus count != 0 && attempted.isEmpty();
            // 2) we have walked the entire query tree but have made no replacement
            //    attempt, thus count == 0 && attempted.isEmpty();
            // 3) we had done some replacement attempt in a previous walk, but in
            //    this one we have not found any potential matches or substitutions,
            //    thus count == 0 && !attempted.isEmpty().
            if (count == 0 || attempted.isEmpty()) {
                break
            }
        }
        if (!attempted.isEmpty()) {
            // We had done some replacement attempt in the previous walk, but that
            // did not lead to any substitutions in this walk, so we need to recover
            // the replacement.
            undoReplacement(attempted)
        }
        return substitutions
    }

    /**
     * Represents a replacement action: before  after.
     * `stopTrying` indicates whether there's no need
     * to do matching for the same query node again.
     */
    class Replacement(before: MutableRel, after: MutableRel, stopTrying: Boolean) {
        val before: MutableRel
        val after: MutableRel
        val stopTrying: Boolean

        constructor(before: MutableRel, after: MutableRel) : this(before, after, true) {}

        init {
            this.before = before
            this.after = after
            this.stopTrying = stopTrying
        }
    }

    @Nullable
    private fun matchRecurse(target: MutableRel): UnifyResult? {
        assert(
            false // not called
        )
        val targetInputs: List<MutableRel> = target.getInputs()
        var queryParent: MutableRel? = null
        for (targetInput in targetInputs) {
            val unifyResult = matchRecurse(targetInput) ?: return null
            queryParent = unifyResult.call.query.replaceInParent(unifyResult.result)
        }
        if (targetInputs.isEmpty()) {
            for (queryLeaf in queryLeaves) {
                for (rule in applicableRules(queryLeaf, target)) {
                    val x = apply(rule, queryLeaf, target)
                    if (x != null) {
                        if (DEBUG) {
                            System.out.println(
                                """
    Rule: $rule
    Query:
    $queryParent
    """.trimIndent()
                                        + (if (x.call.query !== queryParent) """

     Query (original):
     $queryParent
     """.trimIndent() else "")
                                        + "\nTarget:\n"
                                        + target.deep()
                                        + "\nResult:\n"
                                        + x.result.deep()
                                        + "\n"
                            )
                        }
                        return x
                    }
                }
            }
        } else {
            assert(queryParent != null)
            for (rule in applicableRules(queryParent, target)) {
                val x = apply(rule, queryParent, target)
                if (x != null) {
                    if (DEBUG) {
                        System.out.println(
                            """
                                Rule: $rule
                                Query:
                                ${queryParent.deep()}
                                """.trimIndent()
                                    + (if (x.call.query !== queryParent) """

     Query (original):
     ${queryParent.toString()}
     """.trimIndent() else "")
                                    + "\nTarget:\n"
                                    + target.deep()
                                    + "\nResult:\n"
                                    + x.result.deep()
                                    + "\n"
                        )
                    }
                    return x
                }
            }
        }
        if (DEBUG) {
            System.out.println(
                """
                    Unify failed:
                    Query:
                    $queryParent
                    Target:
                    ${target.toString()}

                    """.trimIndent()
            )
        }
        return null
    }

    @Nullable
    private fun apply(
        rule: UnifyRule, query: MutableRel?,
        target: MutableRel
    ): UnifyResult? {
        val call: UnifyRuleCall = UnifyRuleCall(rule, query, target, ImmutableList.of())
        return rule.apply(call)
    }

    private fun applicableRules(
        query: MutableRel?,
        target: MutableRel
    ): List<UnifyRule> {
        val queryClass: Class = query.getClass()
        val targetClass: Class = target.getClass()
        val key: Pair<Class, Class> = Pair.of(queryClass, targetClass)
        var list = ruleMap[key]
        if (list == null) {
            val builder: ImmutableList.Builder<UnifyRule> = ImmutableList.builder()
            for (rule in rules) {
                if (mightMatch(rule, queryClass, targetClass)) {
                    builder.add(rule)
                }
            }
            list = builder.build()
            ruleMap.put(key, list)
        }
        return list
    }

    /** Exception thrown to exit a matcher. Not really an error.  */
    protected object MatchFailed : ControlFlowException() {
        @SuppressWarnings("ThrowableInstanceNeverThrown")
        val INSTANCE: MatchFailed = MatchFailed()
    }

    /** Rule that attempts to match a query relational expression
     * against a target relational expression.
     *
     *
     * The rule declares the query and target types; this allows the
     * engine to fire only a few rules in a given context.
     */
    abstract class UnifyRule protected constructor(
        val slotCount: Int, val queryOperand: Operand,
        val targetOperand: Operand
    ) {
        /**
         *
         * Applies this rule to a particular node in a query. The goal is
         * to convert `query` into `target`. Before the rule is
         * invoked, Calcite has made sure that query's children are equivalent
         * to target's children.
         *
         *
         * There are 3 possible outcomes:
         *
         *
         *
         *  * `query` already exactly matches `target`; returns
         * `target`
         *
         *  * `query` is sufficiently close to a match for
         * `target`; returns `target`
         *
         *  * `query` cannot be made to match `target`; returns
         * null
         *
         *
         *
         *
         * REVIEW: Is possible that we match query PLUS one or more of its
         * ancestors?
         *
         * @param call Input parameters
         */
        @Nullable
        abstract fun apply(call: UnifyRuleCall?): UnifyResult?
        @Nullable
        fun match(
            visitor: SubstitutionVisitor, query: MutableRel?,
            target: MutableRel?
        ): UnifyRuleCall? {
            if (queryOperand.matches(visitor, query)) {
                if (targetOperand.matches(visitor, target)) {
                    return visitor.UnifyRuleCall(
                        this, query, target,
                        copy(visitor.slots, slotCount)
                    )
                }
            }
            return null
        }

        protected fun <E> copy(slots: Array<E?>, slotCount: Int): ImmutableList<E> {
            // Optimize if there are 0 or 1 slots.
            return when (slotCount) {
                0 -> ImmutableList.of()
                1 -> ImmutableList.of(slots[0])
                else -> ImmutableList.copyOf(slots).subList(0, slotCount)
            }
        }
    }

    /**
     * Arguments to an application of a [UnifyRule].
     */
    inner class UnifyRuleCall(
        rule: UnifyRule?, query: MutableRel?, target: MutableRel?,
        slots: ImmutableList<MutableRel?>?
    ) {
        protected val rule: UnifyRule
        val query: MutableRel
        val target: MutableRel
        protected val slots: ImmutableList<MutableRel>

        init {
            this.rule = requireNonNull(rule, "rule")
            this.query = requireNonNull(query, "query")
            this.target = requireNonNull(target, "target")
            this.slots = requireNonNull(slots, "slots")
        }

        fun result(result: MutableRel?): UnifyResult {
            return result(result, true)
        }

        fun result(result: MutableRel, stopTrying: Boolean): UnifyResult {
            assert(MutableRels.contains(result, target))
            assert(
                equalType(
                    "result", result, "query", query,
                    Litmus.THROW
                )
            )
            val replace: MutableRel? = replacementMap[target]
            if (replace != null) {
                assert(
                    false // replacementMap is always empty
                )
                // result =
                replace(result, target, replace)
            }
            register(result, query)
            return UnifyResult(this, result, stopTrying)
        }

        /**
         * Creates a [UnifyRuleCall] based on the parent of `query`.
         */
        fun create(query: MutableRel?): UnifyRuleCall {
            return UnifyRuleCall(rule, query, target, slots)
        }

        fun getCluster(): RelOptCluster {
            return cluster
        }

        fun getSimplify(): RexSimplify {
            return simplify
        }
    }

    /**
     * Result of an application of a [UnifyRule] indicating that the
     * rule successfully matched `query` against `target` and
     * generated a `result` that is equivalent to `query` and
     * contains `target`. `stopTrying` indicates whether there's
     * no need to do matching for the same query node again.
     */
    class UnifyResult internal constructor(val call: UnifyRuleCall, result: MutableRel, stopTrying: Boolean) {
        val result: MutableRel
        val stopTrying: Boolean

        init {
            assert(
                equalType(
                    "query", call.query, "result", result,
                    Litmus.THROW
                )
            )
            this.result = result
            this.stopTrying = stopTrying
        }
    }

    /** Abstract base class for implementing [UnifyRule].  */
    abstract class AbstractUnifyRule @SuppressWarnings("method.invocation.invalid") protected constructor(
        queryOperand: Operand, targetOperand: Operand,
        slotCount: Int
    ) : UnifyRule(slotCount, queryOperand, targetOperand) {
        init {
            assert(isValid)
        }

        protected val isValid: Boolean
            protected get() {
                val slotCounter = SlotCounter()
                slotCounter.visit(queryOperand)
                assert(slotCounter.queryCount == slotCount)
                assert(slotCounter.targetCount == 0)
                slotCounter.queryCount = 0
                slotCounter.visit(targetOperand)
                assert(slotCounter.queryCount == 0)
                assert(slotCounter.targetCount == slotCount)
                return true
            }

        companion object {
            /** Creates an operand with given inputs.  */
            protected fun operand(
                clazz: Class<out MutableRel?>,
                vararg inputOperands: Operand?
            ): Operand {
                return InternalOperand(clazz, ImmutableList.copyOf(inputOperands))
            }

            /** Creates an operand that doesn't check inputs.  */
            protected fun any(clazz: Class<out MutableRel?>): Operand {
                return AnyOperand(clazz)
            }

            /** Creates an operand that matches a relational expression in the query.  */
            protected fun query(ordinal: Int): Operand {
                return QueryOperand(ordinal)
            }

            /** Creates an operand that matches a relational expression in the
             * target.  */
            protected fun target(ordinal: Int): Operand {
                return TargetOperand(ordinal)
            }
        }
    }

    /** Implementation of [UnifyRule] that matches if the query is already
     * equal to the target.
     *
     *
     * Matches scans to the same table, because these will be
     * [MutableScan]s with the same
     * [org.apache.calcite.rel.core.TableScan] instance.
     */
    private class TrivialRule private constructor() : AbstractUnifyRule(
        any(
            MutableRel::class.java
        ), any(MutableRel::class.java), 0
    ) {
        @Override
        @Nullable
        override fun apply(call: UnifyRuleCall): UnifyResult? {
            return if (call.query.equals(call.target)) {
                call.result(call.target)
            } else null
        }

        companion object {
            val INSTANCE = TrivialRule()
        }
    }

    /**
     * A [SubstitutionVisitor.UnifyRule] that matches a
     * [MutableScan] to a [MutableCalc]
     * which has [MutableScan] as child.
     */
    private class ScanToCalcUnifyRule private constructor() : AbstractUnifyRule(
        any(
            MutableScan::class.java
        ),
        operand(
            MutableCalc::class.java, any(
                MutableScan::class.java
            )
        ), 0
    ) {
        @Override
        @Nullable
        protected override fun apply(call: UnifyRuleCall): UnifyResult? {
            val query: MutableScan = call.query as MutableScan
            val target: MutableCalc = call.target as MutableCalc
            val targetInput: MutableScan = target.getInput() as MutableScan
            val targetExplained: Pair<RexNode, List<RexNode>> = explainCalc(target)
            val targetCond: RexNode = targetExplained.left
            val targetProjs: List<RexNode> = targetExplained.right
            val rexBuilder: RexBuilder = call.getCluster().getRexBuilder()
            if (!query.equals(targetInput) || !targetCond.isAlwaysTrue()) {
                return null
            }
            val shuttle: RexShuttle = getRexShuttle(targetProjs)
            val compenProjs: List<RexNode>
            compenProjs = try {
                shuttle.apply(
                    rexBuilder.identityProjects(query.rowType)
                )
            } catch (e: MatchFailed) {
                return null
            }
            return if (RexUtil.isIdentity(compenProjs, target.rowType)) {
                call.result(target)
            } else {
                val compenRexProgram: RexProgram = RexProgram.create(
                    target.rowType, compenProjs, null, query.rowType, rexBuilder
                )
                val compenCalc: MutableCalc = MutableCalc.of(target, compenRexProgram)
                tryMergeParentCalcAndGenResult(call, compenCalc)
            }
        }

        companion object {
            val INSTANCE = ScanToCalcUnifyRule()
        }
    }

    /**
     * A [SubstitutionVisitor.UnifyRule] that matches a
     * [MutableCalc] to a [MutableCalc].
     * The matching condition is as below:
     * 1. All columns of query can be expressed by target;
     * 2. The filtering condition of query must equals to or be weaker than target.
     */
    private class CalcToCalcUnifyRule private constructor() : AbstractUnifyRule(
        operand(
            MutableCalc::class.java, query(0)
        ),
        operand(MutableCalc::class.java, target(0)), 1
    ) {
        @Override
        @Nullable
        override fun apply(call: UnifyRuleCall): UnifyResult? {
            val query: MutableCalc = call.query as MutableCalc
            val queryExplained: Pair<RexNode, List<RexNode>> = explainCalc(query)
            val queryCond: RexNode = queryExplained.left
            val queryProjs: List<RexNode> = queryExplained.right
            val target: MutableCalc = call.target as MutableCalc
            val targetExplained: Pair<RexNode, List<RexNode>> = explainCalc(target)
            val targetCond: RexNode = targetExplained.left
            val targetProjs: List<RexNode> = targetExplained.right
            val rexBuilder: RexBuilder = call.getCluster().getRexBuilder()
            return try {
                val shuttle: RexShuttle = getRexShuttle(targetProjs)
                val splitted: RexNode? = splitFilter(call.getSimplify(), queryCond, targetCond)
                val compenCond: RexNode?
                compenCond = if (splitted != null) {
                    if (splitted.isAlwaysTrue()) {
                        null
                    } else {
                        // Compensate the residual filtering condition.
                        shuttle.apply(splitted)
                    }
                } else if (implies(
                        call.getCluster(), queryCond, targetCond, query.getInput().rowType
                    )
                ) {
                    // Fail to split filtering condition, but implies that target contains
                    // all lines of query, thus just set compensating filtering condition
                    // as the filtering condition of query.
                    shuttle.apply(queryCond)
                } else {
                    return null
                }
                val compenProjs: List<RexNode> = shuttle.apply(queryProjs)
                if (compenCond == null
                    && RexUtil.isIdentity(compenProjs, target.rowType)
                ) {
                    call.result(target)
                } else {
                    val compenRexProgram: RexProgram = RexProgram.create(
                        target.rowType, compenProjs, compenCond,
                        query.rowType, rexBuilder
                    )
                    val compenCalc: MutableCalc = MutableCalc.of(target, compenRexProgram)
                    tryMergeParentCalcAndGenResult(call, compenCalc)
                }
            } catch (e: MatchFailed) {
                null
            }
        }

        companion object {
            val INSTANCE = CalcToCalcUnifyRule()
        }
    }

    /**
     * A [SubstitutionVisitor.UnifyRule] that matches a [MutableJoin]
     * which has [MutableCalc] as left child to a [MutableJoin].
     * We try to pull up the [MutableCalc] to top of [MutableJoin],
     * then match the [MutableJoin] in query to [MutableJoin] in target.
     */
    private class JoinOnLeftCalcToJoinUnifyRule private constructor() : AbstractUnifyRule(
        operand(
            MutableJoin::class.java, operand(
                MutableCalc::class.java, query(0)
            ), query(1)
        ),
        operand(MutableJoin::class.java, target(0), target(1)), 2
    ) {
        @Override
        @Nullable
        protected override fun apply(call: UnifyRuleCall): UnifyResult? {
            val query: MutableJoin = call.query as MutableJoin
            val qInput0: MutableCalc = query.getLeft() as MutableCalc
            val qInput1: MutableRel = query.getRight()
            val qInput0Explained: Pair<RexNode, List<RexNode>> = explainCalc(qInput0)
            val qInput0Cond: RexNode = qInput0Explained.left
            val qInput0Projs: List<RexNode> = qInput0Explained.right
            val target: MutableJoin = call.target as MutableJoin
            val rexBuilder: RexBuilder = call.getCluster().getRexBuilder()

            // Check whether is same join type.
            val joinRelType: JoinRelType = sameJoinType(query.joinType, target.joinType)
                ?: return null
            // Check if filter under join can be pulled up.
            if (!canPullUpFilterUnderJoin(joinRelType, qInput0Cond, null)) {
                return null
            }
            // Try pulling up MutableCalc only when Join condition references mapping.
            val identityProjects: List<RexNode> = rexBuilder.identityProjects(qInput1.rowType)
            if (!referenceByMapping(query.condition, qInput0Projs, identityProjects)) {
                return null
            }
            val newQueryJoinCond: RexNode = object : RexShuttle() {
                @Override
                fun visitInputRef(inputRef: RexInputRef): RexNode {
                    val idx: Int = inputRef.getIndex()
                    return if (idx < fieldCnt(qInput0)) {
                        val newIdx: Int = (qInput0Projs[idx] as RexInputRef).getIndex()
                        RexInputRef(newIdx, inputRef.getType())
                    } else {
                        val newIdx = idx - fieldCnt(qInput0) + fieldCnt(qInput0.getInput())
                        RexInputRef(newIdx, inputRef.getType())
                    }
                }
            }.apply(query.condition)
            val splitted: RexNode? = splitFilter(call.getSimplify(), newQueryJoinCond, target.condition)
            // MutableJoin matches only when the conditions are analyzed to be same.
            if (splitted != null && splitted.isAlwaysTrue()) {
                val compenCond: RexNode = qInput0Cond
                val compenProjs: List<RexNode> = ArrayList()
                for (i in 0 until fieldCnt(query)) {
                    if (i < fieldCnt(qInput0)) {
                        compenProjs.add(qInput0Projs[i])
                    } else {
                        val newIdx: Int = i - fieldCnt(qInput0) + fieldCnt(qInput0.getInput())
                        compenProjs.add(
                            RexInputRef(newIdx, query.rowType.getFieldList().get(i).getType())
                        )
                    }
                }
                val compenRexProgram: RexProgram = RexProgram.create(
                    target.rowType, compenProjs, compenCond,
                    query.rowType, rexBuilder
                )
                val compenCalc: MutableCalc = MutableCalc.of(target, compenRexProgram)
                return tryMergeParentCalcAndGenResult(call, compenCalc)
            }
            return null
        }

        companion object {
            val INSTANCE = JoinOnLeftCalcToJoinUnifyRule()
        }
    }

    /**
     * A [SubstitutionVisitor.UnifyRule] that matches a [MutableJoin]
     * which has [MutableCalc] as right child to a [MutableJoin].
     * We try to pull up the [MutableCalc] to top of [MutableJoin],
     * then match the [MutableJoin] in query to [MutableJoin] in target.
     */
    private class JoinOnRightCalcToJoinUnifyRule private constructor() : AbstractUnifyRule(
        operand(
            MutableJoin::class.java, query(0), operand(
                MutableCalc::class.java, query(1)
            )
        ),
        operand(MutableJoin::class.java, target(0), target(1)), 2
    ) {
        @Override
        @Nullable
        protected override fun apply(call: UnifyRuleCall): UnifyResult? {
            val query: MutableJoin = call.query as MutableJoin
            val qInput0: MutableRel = query.getLeft()
            val qInput1: MutableCalc = query.getRight() as MutableCalc
            val qInput1Explained: Pair<RexNode, List<RexNode>> = explainCalc(qInput1)
            val qInput1Cond: RexNode = qInput1Explained.left
            val qInput1Projs: List<RexNode> = qInput1Explained.right
            val target: MutableJoin = call.target as MutableJoin
            val rexBuilder: RexBuilder = call.getCluster().getRexBuilder()

            // Check whether is same join type.
            val joinRelType: JoinRelType = sameJoinType(query.joinType, target.joinType)
                ?: return null
            // Check if filter under join can be pulled up.
            if (!canPullUpFilterUnderJoin(joinRelType, null, qInput1Cond)) {
                return null
            }
            // Try pulling up MutableCalc only when Join condition references mapping.
            val identityProjects: List<RexNode> = rexBuilder.identityProjects(qInput0.rowType)
            if (!referenceByMapping(query.condition, identityProjects, qInput1Projs)) {
                return null
            }
            val newQueryJoinCond: RexNode = object : RexShuttle() {
                @Override
                fun visitInputRef(inputRef: RexInputRef): RexNode {
                    val idx: Int = inputRef.getIndex()
                    return if (idx < fieldCnt(qInput0)) {
                        inputRef
                    } else {
                        val newIdx: Int = (qInput1Projs[idx - fieldCnt(qInput0)] as RexInputRef)
                            .getIndex() + fieldCnt(qInput0)
                        RexInputRef(newIdx, inputRef.getType())
                    }
                }
            }.apply(query.condition)
            val splitted: RexNode? = splitFilter(call.getSimplify(), newQueryJoinCond, target.condition)
            // MutableJoin matches only when the conditions are analyzed to be same.
            if (splitted != null && splitted.isAlwaysTrue()) {
                val compenCond: RexNode = RexUtil.shift(qInput1Cond, qInput0.rowType.getFieldCount())
                val compenProjs: List<RexNode> = ArrayList()
                for (i in 0 until query.rowType.getFieldCount()) {
                    if (i < fieldCnt(qInput0)) {
                        compenProjs.add(
                            RexInputRef(i, query.rowType.getFieldList().get(i).getType())
                        )
                    } else {
                        val shifted: RexNode = RexUtil.shift(
                            qInput1Projs[i - fieldCnt(qInput0)],
                            qInput0.rowType.getFieldCount()
                        )
                        compenProjs.add(shifted)
                    }
                }
                val compensatingRexProgram: RexProgram = RexProgram.create(
                    target.rowType, compenProjs, compenCond,
                    query.rowType, rexBuilder
                )
                val compenCalc: MutableCalc = MutableCalc.of(target, compensatingRexProgram)
                return tryMergeParentCalcAndGenResult(call, compenCalc)
            }
            return null
        }

        companion object {
            val INSTANCE = JoinOnRightCalcToJoinUnifyRule()
        }
    }

    /**
     * A [SubstitutionVisitor.UnifyRule] that matches a [MutableJoin]
     * which has [MutableCalc] as children to a [MutableJoin].
     * We try to pull up the [MutableCalc] to top of [MutableJoin],
     * then match the [MutableJoin] in query to [MutableJoin] in target.
     */
    private class JoinOnCalcsToJoinUnifyRule private constructor() : AbstractUnifyRule(
        operand(
            MutableJoin::class.java,
            operand(MutableCalc::class.java, query(0)), operand(
                MutableCalc::class.java, query(1)
            )
        ),
        operand(MutableJoin::class.java, target(0), target(1)), 2
    ) {
        @Override
        @Nullable
        protected override fun apply(call: UnifyRuleCall): UnifyResult? {
            val query: MutableJoin = call.query as MutableJoin
            val qInput0: MutableCalc = query.getLeft() as MutableCalc
            val qInput1: MutableCalc = query.getRight() as MutableCalc
            val qInput0Explained: Pair<RexNode, List<RexNode>> = explainCalc(qInput0)
            val qInput0Cond: RexNode = qInput0Explained.left
            val qInput0Projs: List<RexNode> = qInput0Explained.right
            val qInput1Explained: Pair<RexNode, List<RexNode>> = explainCalc(qInput1)
            val qInput1Cond: RexNode = qInput1Explained.left
            val qInput1Projs: List<RexNode> = qInput1Explained.right
            val target: MutableJoin = call.target as MutableJoin
            val rexBuilder: RexBuilder = call.getCluster().getRexBuilder()

            // Check whether is same join type.
            val joinRelType: JoinRelType = sameJoinType(query.joinType, target.joinType)
                ?: return null
            // Check if filter under join can be pulled up.
            if (!canPullUpFilterUnderJoin(joinRelType, qInput0Cond, qInput1Cond)) {
                return null
            }
            if (!referenceByMapping(query.condition, qInput0Projs, qInput1Projs)) {
                return null
            }
            val newQueryJoinCond: RexNode = object : RexShuttle() {
                @Override
                fun visitInputRef(inputRef: RexInputRef): RexNode {
                    val idx: Int = inputRef.getIndex()
                    return if (idx < fieldCnt(qInput0)) {
                        val newIdx: Int = (qInput0Projs[idx] as RexInputRef).getIndex()
                        RexInputRef(newIdx, inputRef.getType())
                    } else {
                        val newIdx: Int = (qInput1Projs[idx - fieldCnt(qInput0)] as RexInputRef)
                            .getIndex() + fieldCnt(qInput0.getInput())
                        RexInputRef(newIdx, inputRef.getType())
                    }
                }
            }.apply(query.condition)
            val splitted: RexNode? = splitFilter(call.getSimplify(), newQueryJoinCond, target.condition)
            // MutableJoin matches only when the conditions are analyzed to be same.
            if (splitted != null && splitted.isAlwaysTrue()) {
                val qInput1CondShifted: RexNode = RexUtil.shift(qInput1Cond, fieldCnt(qInput0.getInput()))
                val compenCond: RexNode = RexUtil.composeConjunction(
                    rexBuilder,
                    ImmutableList.of(qInput0Cond, qInput1CondShifted)
                )
                val compenProjs: List<RexNode> = ArrayList()
                for (i in 0 until query.rowType.getFieldCount()) {
                    if (i < fieldCnt(qInput0)) {
                        compenProjs.add(qInput0Projs[i])
                    } else {
                        val shifted: RexNode = RexUtil.shift(
                            qInput1Projs[i - fieldCnt(qInput0)],
                            fieldCnt(qInput0.getInput())
                        )
                        compenProjs.add(shifted)
                    }
                }
                val compensatingRexProgram: RexProgram = RexProgram.create(
                    target.rowType, compenProjs, compenCond,
                    query.rowType, rexBuilder
                )
                val compensatingCalc: MutableCalc = MutableCalc.of(target, compensatingRexProgram)
                return tryMergeParentCalcAndGenResult(call, compensatingCalc)
            }
            return null
        }

        companion object {
            val INSTANCE = JoinOnCalcsToJoinUnifyRule()
        }
    }

    /**
     * A [SubstitutionVisitor.UnifyRule] that matches a [MutableAggregate]
     * which has [MutableCalc] as child to a [MutableAggregate].
     * We try to pull up the [MutableCalc] to top of [MutableAggregate],
     * then match the [MutableAggregate] in query to [MutableAggregate] in target.
     */
    private class AggregateOnCalcToAggregateUnifyRule private constructor() : AbstractUnifyRule(
        operand(
            MutableAggregate::class.java, operand(MutableCalc::class.java, query(0))
        ),
        operand(MutableAggregate::class.java, target(0)), 1
    ) {
        @Override
        @Nullable
        protected override fun apply(call: UnifyRuleCall): UnifyResult? {
            val query: MutableAggregate = call.query as MutableAggregate
            val qInput: MutableCalc = query.getInput() as MutableCalc
            val qInputExplained: Pair<RexNode, List<RexNode>> = explainCalc(qInput)
            val qInputCond: RexNode = qInputExplained.left
            val qInputProjs: List<RexNode> = qInputExplained.right
            val target: MutableAggregate = call.target as MutableAggregate
            val rexBuilder: RexBuilder = call.getCluster().getRexBuilder()
            val mapping: Mappings.TargetMapping = Project.getMapping(fieldCnt(qInput.getInput()), qInputProjs)
                ?: return null
            if (!qInputCond.isAlwaysTrue()) {
                try {
                    // Fail the matching when filtering condition references
                    // non-grouping columns in target.
                    qInputCond.accept(object : RexVisitorImpl<Void?>(true) {
                        @Override
                        fun visitInputRef(inputRef: RexInputRef): Void {
                            if (!target.groupSets.stream()
                                    .allMatch { groupSet -> groupSet.get(inputRef.getIndex()) }
                            ) {
                                throw Util.FoundOne.NULL
                            }
                            return super.visitInputRef(inputRef)
                        }
                    })
                } catch (one: Util.FoundOne) {
                    return null
                }
            }
            val inverseMapping: Mapping = mapping.inverse()
            val aggregate2: MutableAggregate = permute(query, qInput.getInput(), inverseMapping)
            val mappingForQueryCond: Mappings.TargetMapping = Mappings.target(
                target.groupSet::indexOf,
                target.getInput().rowType.getFieldCount(),
                target.groupSet.cardinality()
            )
            val targetCond: RexNode = RexUtil.apply(mappingForQueryCond, qInputCond)
            val unifiedAggregate: MutableRel = unifyAggregates(aggregate2, targetCond, target)
                ?: return null
            // Add Project if the mapping breaks order of fields in GroupSet
            return if (!Mappings.keepsOrdering(mapping)) {
                val posList: List<Integer> = ArrayList()
                val fieldCount: Int = aggregate2.rowType.getFieldCount()
                val pairs: List<Pair<Integer, Integer>> = ArrayList()
                val groupings: List<Integer> = aggregate2.groupSet.toList()
                for (i in 0 until groupings.size()) {
                    pairs.add(Pair.of(mapping.getTarget(groupings[i]), i))
                }
                Collections.sort(pairs)
                pairs.forEach { pair -> posList.add(pair.right) }
                for (i in posList.size() until fieldCount) {
                    posList.add(i)
                }
                val compenProjs: List<RexNode> = MutableRels.createProjectExprs(unifiedAggregate, posList)
                val compensatingRexProgram: RexProgram = RexProgram.create(
                    unifiedAggregate.rowType, compenProjs, null,
                    query.rowType, rexBuilder
                )
                val compenCalc: MutableCalc = MutableCalc.of(unifiedAggregate, compensatingRexProgram)
                if (unifiedAggregate is MutableCalc) {
                    val newCompenCalc: MutableCalc = mergeCalc(rexBuilder, compenCalc, unifiedAggregate as MutableCalc)
                        ?: return null
                    tryMergeParentCalcAndGenResult(call, newCompenCalc)
                } else {
                    tryMergeParentCalcAndGenResult(call, compenCalc)
                }
            } else {
                tryMergeParentCalcAndGenResult(call, unifiedAggregate)
            }
        }

        companion object {
            val INSTANCE = AggregateOnCalcToAggregateUnifyRule()
        }
    }

    /** A [SubstitutionVisitor.UnifyRule] that matches a
     * [org.apache.calcite.rel.core.Aggregate] to a
     * [org.apache.calcite.rel.core.Aggregate], provided
     * that they have the same child.  */
    private class AggregateToAggregateUnifyRule private constructor() : AbstractUnifyRule(
        operand(
            MutableAggregate::class.java, query(0)
        ),
        operand(MutableAggregate::class.java, target(0)), 1
    ) {
        @Override
        @Nullable
        override fun apply(call: UnifyRuleCall): UnifyResult? {
            val query: MutableAggregate = call.query as MutableAggregate
            val target: MutableAggregate = call.target as MutableAggregate
            assert(query !== target)
            // in.query can be rewritten in terms of in.target if its groupSet is
            // a subset, and its aggCalls are a superset. For example:
            //   query: SELECT x, COUNT(b) FROM t GROUP BY x
            //   target: SELECT x, y, SUM(a) AS s, COUNT(b) AS cb FROM t GROUP BY x, y
            // transforms to
            //   result: SELECT x, SUM(cb) FROM (target) GROUP BY x
            if (query.getInput() !== target.getInput()) {
                return null
            }
            if (!target.groupSet.contains(query.groupSet)) {
                return null
            }
            val result: MutableRel = unifyAggregates(query, null, target)
                ?: return null
            return tryMergeParentCalcAndGenResult(call, result)
        }

        companion object {
            val INSTANCE = AggregateToAggregateUnifyRule()
        }
    }

    /**
     * A [SubstitutionVisitor.UnifyRule] that matches a
     * [MutableUnion] to a [MutableUnion] where the query and target
     * have the same inputs but might not have the same order.
     */
    private class UnionToUnionUnifyRule private constructor() : AbstractUnifyRule(
        any(
            MutableUnion::class.java
        ), any(
            MutableUnion::class.java
        ), 0
    ) {
        @Override
        @Nullable
        override fun apply(call: UnifyRuleCall): UnifyResult? {
            val query: MutableUnion = call.query as MutableUnion
            val target: MutableUnion = call.target as MutableUnion
            val queryInputs: List<MutableRel> = ArrayList(query.getInputs())
            val targetInputs: List<MutableRel> = ArrayList(target.getInputs())
            return if (query.isAll() === target.isAll()
                && sameRelCollectionNoOrderConsidered(
                    queryInputs,
                    targetInputs
                )
            ) {
                call.result(target)
            } else null
        }

        companion object {
            val INSTANCE = UnionToUnionUnifyRule()
        }
    }

    /**
     * A [SubstitutionVisitor.UnifyRule] that matches a [MutableUnion]
     * which has [MutableCalc] as child to a [MutableUnion].
     * We try to pull up the [MutableCalc] to top of [MutableUnion],
     * then match the [MutableUnion] in query to [MutableUnion] in target.
     */
    private class UnionOnCalcsToUnionUnifyRule private constructor() : AbstractUnifyRule(
        any(
            MutableUnion::class.java
        ), any(
            MutableUnion::class.java
        ), 0
    ) {
        @Override
        @Nullable
        override fun apply(call: UnifyRuleCall): UnifyResult? {
            return setOpApply(call)
        }

        companion object {
            val INSTANCE = UnionOnCalcsToUnionUnifyRule()
        }
    }

    /**
     * A [SubstitutionVisitor.UnifyRule] that matches a
     * [MutableIntersect] to a [MutableIntersect] where the query and target
     * have the same inputs but might not have the same order.
     */
    private class IntersectToIntersectUnifyRule private constructor() : AbstractUnifyRule(
        any(
            MutableIntersect::class.java
        ), any(
            MutableIntersect::class.java
        ), 0
    ) {
        @Override
        @Nullable
        override fun apply(call: UnifyRuleCall): UnifyResult? {
            val query: MutableIntersect = call.query as MutableIntersect
            val target: MutableIntersect = call.target as MutableIntersect
            val queryInputs: List<MutableRel> = ArrayList(query.getInputs())
            val targetInputs: List<MutableRel> = ArrayList(target.getInputs())
            return if (query.isAll() === target.isAll()
                && sameRelCollectionNoOrderConsidered(
                    queryInputs,
                    targetInputs
                )
            ) {
                call.result(target)
            } else null
        }

        companion object {
            val INSTANCE = IntersectToIntersectUnifyRule()
        }
    }

    /**
     * A [SubstitutionVisitor.UnifyRule] that matches a [MutableIntersect]
     * which has [MutableCalc] as child to a [MutableIntersect].
     * We try to pull up the [MutableCalc] to top of [MutableIntersect],
     * then match the [MutableIntersect] in query to [MutableIntersect] in target.
     */
    private class IntersectOnCalcsToIntersectUnifyRule private constructor() : AbstractUnifyRule(
        any(
            MutableIntersect::class.java
        ), any(
            MutableIntersect::class.java
        ), 0
    ) {
        @Override
        @Nullable
        override fun apply(call: UnifyRuleCall): UnifyResult? {
            return setOpApply(call)
        }

        companion object {
            val INSTANCE = IntersectOnCalcsToIntersectUnifyRule()
        }
    }

    /** Returns if one rel is weaker than another.  */
    protected fun isWeaker(rel0: MutableRel?, rel: MutableRel): Boolean {
        if (rel0 === rel || equivalents.get(rel0).contains(rel)) {
            return false
        }
        if (rel0 !is MutableFilter
            || rel !is MutableFilter
        ) {
            return false
        }
        if (!rel.rowType.equals(rel0.rowType)) {
            return false
        }
        val rel0input: MutableRel = (rel0 as MutableFilter?).getInput()
        val relinput: MutableRel = (rel as MutableFilter).getInput()
        return if (rel0input !== relinput
            && !equivalents.get(rel0input).contains(relinput)
        ) {
            false
        } else implies(
            rel0.cluster, (rel0 as MutableFilter?).condition,
            (rel as MutableFilter).condition, rel.rowType
        )
    }

    /** Operand to a [UnifyRule].  */
    abstract class Operand protected constructor(clazz: Class<out MutableRel?>) {
        val clazz: Class<out MutableRel?>

        init {
            this.clazz = clazz
        }

        abstract fun matches(visitor: SubstitutionVisitor?, rel: MutableRel?): Boolean
        fun isWeaker(visitor: SubstitutionVisitor?, rel: MutableRel?): Boolean {
            return false
        }
    }

    /** Operand to a [UnifyRule] that matches a relational expression of a
     * given type. It has zero or more child operands.  */
    private class InternalOperand internal constructor(clazz: Class<out MutableRel?>, val inputs: List<Operand>) :
        Operand(clazz) {
        @Override
        override fun matches(visitor: SubstitutionVisitor, rel: MutableRel): Boolean {
            return (clazz.isInstance(rel)
                    && allMatch(visitor, inputs, rel.getInputs()))
        }

        @Override
        override fun isWeaker(visitor: SubstitutionVisitor, rel: MutableRel): Boolean {
            return (clazz.isInstance(rel)
                    && allWeaker(visitor, inputs, rel.getInputs()))
        }

        companion object {
            private fun allMatch(
                visitor: SubstitutionVisitor,
                operands: List<Operand>, rels: List<MutableRel>
            ): Boolean {
                if (operands.size() !== rels.size()) {
                    return false
                }
                for (pair in Pair.zip(operands, rels)) {
                    if (!pair.left.matches(visitor, pair.right)) {
                        return false
                    }
                }
                return true
            }

            private fun allWeaker(
                visitor: SubstitutionVisitor,
                operands: List<Operand>, rels: List<MutableRel>
            ): Boolean {
                if (operands.size() !== rels.size()) {
                    return false
                }
                for (pair in Pair.zip(operands, rels)) {
                    if (!pair.left.isWeaker(visitor, pair.right)) {
                        return false
                    }
                }
                return true
            }
        }
    }

    /** Operand to a [UnifyRule] that matches a relational expression of a
     * given type.  */
    private class AnyOperand internal constructor(clazz: Class<out MutableRel?>) : Operand(clazz) {
        @Override
        override fun matches(visitor: SubstitutionVisitor?, rel: MutableRel?): Boolean {
            return clazz.isInstance(rel)
        }
    }

    /** Operand that assigns a particular relational expression to a variable.
     *
     *
     * It is applied to a descendant of the query, writes the operand into the
     * slots array, and always matches.
     * There is a corresponding operand of type [TargetOperand] that checks
     * whether its relational expression, a descendant of the target, is
     * equivalent to this `QueryOperand`'s relational expression.
     */
    private class QueryOperand(private val ordinal: Int) : Operand(MutableRel::class.java) {
        @Override
        override fun matches(visitor: SubstitutionVisitor, rel: MutableRel?): Boolean {
            visitor.slots[ordinal] = rel
            return true
        }
    }

    /** Operand that checks that a relational expression matches the corresponding
     * relational expression that was passed to a [QueryOperand].  */
    private class TargetOperand(private val ordinal: Int) : Operand(MutableRel::class.java) {
        @Override
        override fun matches(visitor: SubstitutionVisitor, rel: MutableRel): Boolean {
            val rel0: MutableRel? = visitor.slots[ordinal]
            assert(rel0 != null) { "QueryOperand should have been called first" }
            return rel0 === rel || visitor.equivalents.get(rel0).contains(rel)
        }

        @Override
        override fun isWeaker(visitor: SubstitutionVisitor, rel: MutableRel): Boolean {
            val rel0: MutableRel? = visitor.slots[ordinal]
            assert(rel0 != null) { "QueryOperand should have been called first" }
            return visitor.isWeaker(rel0, rel)
        }
    }

    /** Visitor that counts how many [QueryOperand] and
     * [TargetOperand] in an operand tree.  */
    private class SlotCounter {
        var queryCount = 0
        var targetCount = 0
        fun visit(operand: Operand) {
            if (operand is QueryOperand) {
                ++queryCount
            } else if (operand is TargetOperand) {
                ++targetCount
            } else if (operand is AnyOperand) {
                // nothing
            } else {
                for (input in (operand as InternalOperand).inputs) {
                    visit(input)
                }
            }
        }
    }

    companion object {
        private val DEBUG: Boolean = CalciteSystemProperty.DEBUG.value()
        val DEFAULT_RULES: ImmutableList<UnifyRule> = ImmutableList.of(
            TrivialRule.INSTANCE,
            ScanToCalcUnifyRule.INSTANCE,
            CalcToCalcUnifyRule.INSTANCE,
            JoinOnLeftCalcToJoinUnifyRule.INSTANCE,
            JoinOnRightCalcToJoinUnifyRule.INSTANCE,
            JoinOnCalcsToJoinUnifyRule.INSTANCE,
            AggregateToAggregateUnifyRule.INSTANCE,
            AggregateOnCalcToAggregateUnifyRule.INSTANCE,
            UnionToUnionUnifyRule.INSTANCE,
            UnionOnCalcsToUnionUnifyRule.INSTANCE,
            IntersectToIntersectUnifyRule.INSTANCE,
            IntersectOnCalcsToIntersectUnifyRule.INSTANCE
        )

        /**
         * Maps a condition onto a target.
         *
         *
         * If condition is stronger than target, returns the residue.
         * If it is equal to target, returns the expression that evaluates to
         * the constant `true`. If it is weaker than target, returns
         * `null`.
         *
         *
         * The terms satisfy the relation
         *
         * <blockquote>
         * <pre>`condition = target AND residue`</pre>
        </blockquote> *
         *
         *
         * and `residue` must be as weak as possible.
         *
         *
         * Example #1: condition stronger than target
         *
         *  * condition: x = 1 AND y = 2
         *  * target: x = 1
         *  * residue: y = 2
         *
         *
         *
         * Note that residue `x > 0 AND y = 2` would also satisfy the
         * relation `condition = target AND residue` but is stronger than
         * necessary, so we prefer `y = 2`.
         *
         *
         * Example #2: target weaker than condition (valid, but not currently
         * implemented)
         *
         *  * condition: x = 1
         *  * target: x = 1 OR z = 3
         *  * residue: x = 1
         *
         *
         *
         * Example #3: condition and target are equivalent
         *
         *  * condition: x = 1 AND y = 2
         *  * target: y = 2 AND x = 1
         *  * residue: TRUE
         *
         *
         *
         * Example #4: condition weaker than target
         *
         *  * condition: x = 1
         *  * target: x = 1 AND y = 2
         *  * residue: null (i.e. no match)
         *
         *
         *
         * There are many other possible examples. It amounts to solving
         * whether `condition AND NOT target` can ever evaluate to
         * true, and therefore is a form of the NP-complete
         * [Satisfiability](http://en.wikipedia.org/wiki/Satisfiability)
         * problem.
         */
        @VisibleForTesting
        @Nullable
        fun splitFilter(
            simplify: RexSimplify,
            condition: RexNode, target: RexNode
        ): RexNode? {
            var condition: RexNode = condition
            var target: RexNode = target
            val rexBuilder: RexBuilder = simplify.rexBuilder
            condition = simplify.simplify(condition)
            target = simplify.simplify(target)
            val condition2: RexNode = canonizeNode(rexBuilder, condition)
            val target2: RexNode = canonizeNode(rexBuilder, target)

            // First, try splitting into ORs.
            // Given target    c1 OR c2 OR c3 OR c4
            // and condition   c2 OR c4
            // residue is      c2 OR c4
            // Also deals with case target [x] condition [x] yields residue [true].
            val z: RexNode? = splitOr(rexBuilder, condition2, target2)
            if (z != null) {
                return z
            }
            if (isEquivalent(condition2, target2)) {
                return rexBuilder.makeLiteral(true)
            }
            val x: RexNode = andNot(rexBuilder, target2, condition2)
            if (mayBeSatisfiable(x)) {
                val x2: RexNode = RexUtil.composeConjunction(
                    rexBuilder,
                    ImmutableList.of(condition2, target2)
                )
                val r: RexNode = canonizeNode(
                    rexBuilder,
                    simplify.simplifyUnknownAsFalse(x2)
                )
                if (!r.isAlwaysFalse() && isEquivalent(condition2, r)) {
                    val conjs: List<RexNode> = RelOptUtil.conjunctions(r)
                    for (e in RelOptUtil.conjunctions(target2)) {
                        removeAll(conjs, e)
                    }
                    return RexUtil.composeConjunction(rexBuilder, conjs)
                }
            }
            return null
        }

        /**
         * Reorders some of the operands in this expression so structural comparison,
         * i.e., based on string representation, can be more precise.
         */
        private fun canonizeNode(rexBuilder: RexBuilder, condition: RexNode): RexNode {
            return when (condition.getKind()) {
                AND, OR -> {
                    val call: RexCall = condition as RexCall
                    val newOperands: NavigableMap<String, RexNode> = TreeMap()
                    for (operand in call.operands) {
                        operand = canonizeNode(rexBuilder, operand)
                        newOperands.put(operand.toString(), operand)
                    }
                    if (newOperands.size() < 2) {
                        newOperands.values().iterator().next()
                    } else rexBuilder.makeCall(
                        call.getOperator(),
                        ImmutableList.copyOf(newOperands.values())
                    )
                }
                EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN_OR_EQUAL -> {
                    var call: RexCall = condition as RexCall
                    val left: RexNode = canonizeNode(rexBuilder, call.getOperands().get(0))
                    val right: RexNode = canonizeNode(rexBuilder, call.getOperands().get(1))
                    call = rexBuilder.makeCall(call.getOperator(), left, right) as RexCall
                    if (left.toString().compareTo(right.toString()) <= 0) {
                        return call
                    }
                    val result: RexNode = RexUtil.invert(rexBuilder, call)
                        ?: throw NullPointerException("RexUtil.invert returned null for $call")
                    result
                }
                SEARCH -> {
                    val e: RexNode = RexUtil.expandSearch(rexBuilder, null, condition)
                    canonizeNode(rexBuilder, e)
                }
                PLUS, TIMES -> {
                    val call: RexCall = condition as RexCall
                    val left: RexNode = canonizeNode(rexBuilder, call.getOperands().get(0))
                    val right: RexNode = canonizeNode(rexBuilder, call.getOperands().get(1))
                    if (left.toString().compareTo(right.toString()) <= 0) {
                        return rexBuilder.makeCall(call.getOperator(), left, right)
                    }
                    val newCall: RexNode = rexBuilder.makeCall(call.getOperator(), right, left)
                    // new call should not be used if its inferred type is not same as old
                    if (!newCall.getType().equals(call.getType())) {
                        call
                    } else newCall
                }
                else -> condition
            }
        }

        @Nullable
        private fun splitOr(
            rexBuilder: RexBuilder, condition: RexNode, target: RexNode
        ): RexNode? {
            val conditions: List<RexNode> = RelOptUtil.disjunctions(condition)
            val conditionsLength: Int = conditions.size()
            var targetsLength = 0
            for (e in RelOptUtil.disjunctions(target)) {
                removeAll(conditions, e)
                targetsLength++
            }
            if (conditions.isEmpty() && conditionsLength == targetsLength) {
                return rexBuilder.makeLiteral(true)
            } else if (conditions.isEmpty()) {
                return condition
            }
            return null
        }

        private fun isEquivalent(condition: RexNode, target: RexNode): Boolean {
            // Example:
            //  e: x = 1 AND y = 2 AND z = 3 AND NOT (x = 1 AND y = 2)
            //  disjunctions: {x = 1, y = 2, z = 3}
            //  notDisjunctions: {x = 1 AND y = 2}
            val conditionDisjunctions: Set<String> = HashSet(
                RexUtil.strings(RelOptUtil.conjunctions(condition))
            )
            val targetDisjunctions: Set<String> = HashSet(
                RexUtil.strings(RelOptUtil.conjunctions(target))
            )
            return if (conditionDisjunctions.equals(targetDisjunctions)) {
                true
            } else false
        }

        /**
         * Returns whether a boolean expression ever returns true.
         *
         *
         * This method may give false positives. For instance, it will say
         * that `x = 5 AND x > 10` is satisfiable, because at present it
         * cannot prove that it is not.
         */
        fun mayBeSatisfiable(e: RexNode?): Boolean {
            // Example:
            //  e: x = 1 AND y = 2 AND z = 3 AND NOT (x = 1 AND y = 2)
            //  disjunctions: {x = 1, y = 2, z = 3}
            //  notDisjunctions: {x = 1 AND y = 2}
            val disjunctions: List<RexNode> = ArrayList()
            val notDisjunctions: List<RexNode> = ArrayList()
            RelOptUtil.decomposeConjunction(e, disjunctions, notDisjunctions)

            // If there is a single FALSE or NOT TRUE, the whole expression is
            // always false.
            for (disjunction in disjunctions) {
                when (disjunction.getKind()) {
                    LITERAL -> if (!RexLiteral.booleanValue(disjunction)) {
                        return false
                    }
                    else -> {}
                }
            }
            for (disjunction in notDisjunctions) {
                when (disjunction.getKind()) {
                    LITERAL -> if (RexLiteral.booleanValue(disjunction)) {
                        return false
                    }
                    else -> {}
                }
            }
            // If one of the not-disjunctions is a disjunction that is wholly
            // contained in the disjunctions list, the expression is not
            // satisfiable.
            //
            // Example #1. x AND y AND z AND NOT (x AND y)  - not satisfiable
            // Example #2. x AND y AND NOT (x AND y)        - not satisfiable
            // Example #3. x AND y AND NOT (x AND y AND z)  - may be satisfiable
            for (notDisjunction in notDisjunctions) {
                val disjunctions2: List<RexNode> = RelOptUtil.conjunctions(notDisjunction)
                if (disjunctions.containsAll(disjunctions2)) {
                    return false
                }
            }
            return true
        }

        /**
         * Equivalence checking for row types, but except for the field names.
         */
        private fun rowTypesAreEquivalent(
            rel0: MutableRel, rel1: MutableRel, litmus: Litmus
        ): Boolean {
            if (rel0.rowType.getFieldCount() !== rel1.rowType.getFieldCount()) {
                return litmus.fail("Mismatch for column count: [{}]", Pair.of(rel0, rel1))
            }
            for (pair in Pair.zip(rel0.rowType.getFieldList(), rel1.rowType.getFieldList())) {
                if (!pair.left.getType().equals(pair.right.getType())) {
                    return litmus.fail("Mismatch for column type: [{}]", Pair.of(rel0, rel1))
                }
            }
            return litmus.succeed()
        }

        /** Within a relational expression `query`, replaces occurrences of
         * `find` with `replace`.
         *
         *
         * Assumes relational expressions (and their descendants) are not null.
         * Does not handle cycles.  */
        @Nullable
        fun replace(
            query: MutableRel, find: MutableRel,
            replace: MutableRel
        ): Replacement? {
            if (find.equals(replace)) {
                // Short-cut common case.
                return null
            }
            assert(equalType("find", find, "replace", replace, Litmus.THROW))
            return replaceRecurse(query, find, replace)
        }

        /** Helper for [.replace].  */
        @Nullable
        private fun replaceRecurse(
            query: MutableRel,
            find: MutableRel, replace: MutableRel
        ): Replacement? {
            if (find.equals(query)) {
                query.replaceInParent(replace)
                return Replacement(query, replace)
            }
            for (input in query.getInputs()) {
                val r = replaceRecurse(input, find, replace)
                if (r != null) {
                    return r
                }
            }
            return null
        }

        private fun undoReplacement(replacement: List<Replacement>) {
            for (i in replacement.size() - 1 downTo 0) {
                val r = replacement[i]
                r.after.replaceInParent(r.before)
            }
        }

        private fun redoReplacement(replacement: List<Replacement>) {
            for (r in replacement) {
                r.before.replaceInParent(r.after)
            }
        }

        private fun reverseSubstitute(
            relBuilder: RelBuilder, query: Holder,
            matches: List<List<Replacement>>, sub: List<RelNode>,
            replaceCount: Int, maxCount: Int
        ) {
            var replaceCount = replaceCount
            if (matches.isEmpty()) {
                return
            }
            val rem = matches.subList(1, matches.size())
            reverseSubstitute(relBuilder, query, rem, sub, replaceCount, maxCount)
            undoReplacement(matches[0])
            if (++replaceCount < maxCount) {
                sub.add(MutableRels.fromMutable(query.getInput(), relBuilder))
            }
            reverseSubstitute(relBuilder, query, rem, sub, replaceCount, maxCount)
            redoReplacement(matches[0])
        }

        private fun mightMatch(
            rule: UnifyRule,
            queryClass: Class, targetClass: Class
        ): Boolean {
            return (rule.queryOperand.clazz.isAssignableFrom(queryClass)
                    && rule.targetOperand.clazz.isAssignableFrom(targetClass))
        }

        /**
         * Applies a AbstractUnifyRule to a particular node in a query. We try to pull up the
         * [MutableCalc] to top of [MutableUnion] or [MutableIntersect], this
         * method not suit for [MutableMinus].
         *
         * @param call Input parameters
         */
        @Nullable
        private fun setOpApply(call: UnifyRuleCall): UnifyResult? {
            if (call.query is MutableMinus && call.target is MutableMinus) {
                return null
            }
            val query: MutableSetOp = call.query as MutableSetOp
            val target: MutableSetOp = call.target as MutableSetOp
            val queryInputs: List<MutableCalc> = ArrayList()
            val queryGrandInputs: List<MutableRel> = ArrayList()
            val targetInputs: List<MutableRel> = ArrayList(target.getInputs())
            val rexBuilder: RexBuilder = call.getCluster().getRexBuilder()
            for (rel in query.getInputs()) {
                if (rel is MutableCalc) {
                    queryInputs.add(rel as MutableCalc)
                    queryGrandInputs.add((rel as MutableCalc).getInput())
                } else {
                    return null
                }
            }
            if (query.isAll() && target.isAll()
                && sameRelCollectionNoOrderConsidered(queryGrandInputs, targetInputs)
            ) {
                val queryInputExplained0: Pair<RexNode, List<RexNode>> = explainCalc(
                    queryInputs[0]
                )
                for (i in 1 until queryGrandInputs.size()) {
                    val queryInputExplained: Pair<RexNode, List<RexNode>> = explainCalc(
                        queryInputs[i]
                    )
                    // Matching fails when filtering conditions are not equal or projects are not equal.
                    val residue: RexNode? = splitFilter(
                        call.getSimplify(), queryInputExplained0.left,
                        queryInputExplained.left
                    )
                    if (residue == null || !residue.isAlwaysTrue()) {
                        return null
                    }
                    for (pair in Pair.zip(
                        queryInputExplained0.right, queryInputExplained.right
                    )) {
                        if (!pair.left.equals(pair.right)) {
                            return null
                        }
                    }
                }
                val projectExprs: List<RexNode> = MutableRels.createProjects(
                    target,
                    queryInputExplained0.right
                )
                val compenRexProgram: RexProgram = RexProgram.create(
                    target.rowType, projectExprs, queryInputExplained0.left,
                    query.rowType, rexBuilder
                )
                val compenCalc: MutableCalc = MutableCalc.of(target, compenRexProgram)
                return tryMergeParentCalcAndGenResult(call, compenCalc)
            }
            return null
        }

        /** Check if list0 and list1 contains the same nodes -- order is not considered.  */
        private fun sameRelCollectionNoOrderConsidered(
            list0: List<MutableRel>, list1: List<MutableRel>
        ): Boolean {
            if (list0.size() !== list1.size()) {
                return false
            }
            for (rel in list0) {
                val index = list1.indexOf(rel)
                if (index == -1) {
                    return false
                } else {
                    list1.remove(index)
                }
            }
            return true
        }

        private fun fieldCnt(rel: MutableRel): Int {
            return rel.rowType.getFieldCount()
        }

        /** Explain filtering condition and projections from MutableCalc.  */
        fun explainCalc(calc: MutableCalc): Pair<RexNode, List<RexNode>> {
            val shuttle: RexShuttle = getExpandShuttle(calc.program)
            val condition: RexNode = shuttle.apply(calc.program.getCondition())
            val projects: List<RexNode> = ArrayList()
            for (rex in shuttle.apply(calc.program.getProjectList())) {
                projects.add(rex)
            }
            return if (condition == null) {
                Pair.of(calc.cluster.getRexBuilder().makeLiteral(true), projects)
            } else {
                Pair.of(condition, projects)
            }
        }

        /**
         * Generate result by merging parent and child if they are both MutableCalc.
         * Otherwise result is the child itself.
         */
        private fun tryMergeParentCalcAndGenResult(
            call: UnifyRuleCall, child: MutableRel
        ): UnifyResult {
            val parent: MutableRel = call.query.getParent()
            if (child is MutableCalc && parent is MutableCalc) {
                val mergedCalc: MutableCalc? = mergeCalc(
                    call.getCluster().getRexBuilder(),
                    parent as MutableCalc, child as MutableCalc
                )
                if (mergedCalc != null) {
                    // Note that property of stopTrying in the result is false
                    // and this query node deserves further matching iterations.
                    return call.create(parent).result(mergedCalc, false)
                }
            }
            return call.result(child)
        }

        /** Merge two MutableCalc together.  */
        @Nullable
        private fun mergeCalc(
            rexBuilder: RexBuilder, topCalc: MutableCalc, bottomCalc: MutableCalc
        ): MutableCalc? {
            val topProgram: RexProgram = topCalc.program
            if (RexOver.containsOver(topProgram)) {
                return null
            }
            val mergedProgram: RexProgram = RexProgramBuilder.mergePrograms(
                topCalc.program,
                bottomCalc.program,
                rexBuilder
            )
            assert(
                mergedProgram.getOutputRowType()
                        === topProgram.getOutputRowType()
            )
            return MutableCalc.of(bottomCalc.getInput(), mergedProgram)
        }

        private fun getExpandShuttle(rexProgram: RexProgram): RexShuttle {
            return object : RexShuttle() {
                @Override
                fun visitLocalRef(localRef: RexLocalRef?): RexNode {
                    return rexProgram.expandLocalRef(localRef)
                }
            }
        }

        /** Check if condition cond0 implies cond1.  */
        private fun implies(
            cluster: RelOptCluster, cond0: RexNode, cond1: RexNode, rowType: RelDataType
        ): Boolean {
            val rexImpl: RexExecutor = Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR)
            val rexImplicationChecker = RexImplicationChecker(cluster.getRexBuilder(), rexImpl, rowType)
            return rexImplicationChecker.implies(cond0, cond1)
        }

        /** Check if join condition only references RexInputRef.  */
        private fun referenceByMapping(
            joinCondition: RexNode, vararg projectsOfInputs: List<RexNode>
        ): Boolean {
            val projects: List<RexNode> = ArrayList()
            for (projectsOfInput in projectsOfInputs) {
                projects.addAll(projectsOfInput)
            }
            try {
                val rexVisitor: RexVisitor = object : RexVisitorImpl<Void?>(true) {
                    @Override
                    fun visitInputRef(inputRef: RexInputRef): Void {
                        if (projects[inputRef.getIndex()] !is RexInputRef) {
                            throw Util.FoundOne.NULL
                        }
                        return super.visitInputRef(inputRef)
                    }
                }
                joinCondition.accept(rexVisitor)
            } catch (e: Util.FoundOne) {
                return false
            }
            return true
        }

        @Nullable
        private fun sameJoinType(type0: JoinRelType, type1: JoinRelType): JoinRelType? {
            return if (type0 === type1) {
                type0
            } else {
                null
            }
        }

        fun permute(
            aggregate: MutableAggregate,
            input: MutableRel?, mapping: Mapping?
        ): MutableAggregate {
            val groupSet: ImmutableBitSet = Mappings.apply(mapping, aggregate.groupSet)
            val groupSets: ImmutableList<ImmutableBitSet> = Mappings.apply2(mapping, aggregate.groupSets)
            val aggregateCalls: List<AggregateCall> =
                Util.transform(aggregate.aggCalls) { call -> call.transform(mapping) }
            return MutableAggregate.of(input, groupSet, groupSets, aggregateCalls)
        }

        @Nullable
        fun unifyAggregates(
            query: MutableAggregate,
            @Nullable targetCond: RexNode?, target: MutableAggregate
        ): MutableRel? {
            val result: MutableRel
            val rexBuilder: RexBuilder = query.cluster.getRexBuilder()
            val targetCondConstantMap: Map<RexNode, RexNode> =
                RexUtil.predicateConstants(RexNode::class.java, rexBuilder, RelOptUtil.conjunctions(targetCond))
            // Collect rexInputRef in constant filter condition.
            val constantCondInputRefs: Set<Integer> = HashSet()
            val targetGroupByIndexList: List<Integer> = target.groupSet.asList()
            val rexShuttle: RexShuttle = object : RexShuttle() {
                @Override
                fun visitInputRef(inputRef: RexInputRef): RexNode {
                    constantCondInputRefs.add(targetGroupByIndexList[inputRef.getIndex()])
                    return super.visitInputRef(inputRef)
                }
            }
            for (rexNode in targetCondConstantMap.keySet()) {
                rexNode.accept(rexShuttle)
            }
            var compenGroupSet: Set<Integer>? = null
            // Calc the missing group list of query, do not cover grouping sets cases.
            if (query.groupSets.size() === 1 && target.groupSets.size() === 1) {
                if (target.groupSet.contains(query.groupSet)) {
                    compenGroupSet = target.groupSets.get(0).except(query.groupSets.get(0)).asSet()
                }
            }
            // If query and target have the same group list,
            // or query has constant filter for missing columns in group by list.
            if (query.groupSets.equals(target.groupSets)
                || compenGroupSet != null && constantCondInputRefs.containsAll(compenGroupSet)
            ) {
                var projOffset = 0
                if (!query.groupSets.equals(target.groupSets)) {
                    projOffset = requireNonNull(compenGroupSet, "compenGroupSet").size()
                }
                // Same level of aggregation. Generate a project.
                val projects: List<Integer> = ArrayList()
                val groupCount: Int = query.groupSet.cardinality()
                for (inputIndex in query.groupSet.asList()) {
                    // Use the index in target group by.
                    val i = targetGroupByIndexList.indexOf(inputIndex)
                    projects.add(i)
                }
                val targetGroupGenAggCalls: List<AggregateCall> = ArrayList()
                for (aggregateCall in query.aggCalls) {
                    val i: Int = target.aggCalls.indexOf(aggregateCall)
                    if (i < 0) {
                        val newAggCall: AggregateCall? = genAggCallWithTargetGrouping(
                            aggregateCall, targetGroupByIndexList
                        )
                        if (newAggCall == null) {
                            return null
                        } else {
                            // Here, we create a new `MutableAggregate` to return.
                            // So, we record this new agg-call.
                            targetGroupGenAggCalls.add(newAggCall)
                        }
                    } else {
                        if (!targetGroupGenAggCalls.isEmpty()) {
                            // Here, we didn't build target's agg-call by ref of mv's agg-call,
                            // if some agg-call is generated by target's grouping.
                            // So, we return null to stop it.
                            return null
                        }
                        projects.add(groupCount + i + projOffset)
                    }
                }
                result = if (targetGroupGenAggCalls.isEmpty()) {
                    val compenProjs: List<RexNode> = MutableRels.createProjectExprs(target, projects)
                    val compenRexProgram: RexProgram = RexProgram.create(
                        target.rowType, compenProjs, targetCond, query.rowType, rexBuilder
                    )
                    MutableCalc.of(target, compenRexProgram)
                } else {
                    MutableAggregate.of(
                        target,
                        target.groupSet, target.groupSets, targetGroupGenAggCalls
                    )
                }
            } else if (target.getGroupType() === Aggregate.Group.SIMPLE) {
                // Query is coarser level of aggregation. Generate an aggregate.
                val map: Map<Integer, Integer> = HashMap()
                target.groupSet.forEach { k -> map.put(k, map.size()) }
                for (c in query.groupSet) {
                    if (!map.containsKey(c)) {
                        return null
                    }
                }
                val groupSet: ImmutableBitSet = query.groupSet.permute(map)
                var groupSets: ImmutableList<ImmutableBitSet?>? = null
                if (query.getGroupType() !== Aggregate.Group.SIMPLE) {
                    groupSets = ImmutableBitSet.ORDERING.immutableSortedCopy(
                        ImmutableBitSet.permute(query.groupSets, map)
                    )
                }
                val aggregateCalls: List<AggregateCall> = ArrayList()
                for (aggregateCall in query.aggCalls) {
                    var newAggCall: AggregateCall? = null
                    // 1. try to find rollup agg-call.
                    if (!aggregateCall.isDistinct()) {
                        val i: Int = target.aggCalls.indexOf(aggregateCall)
                        if (i >= 0) {
                            // When an SqlAggFunction does not support roll up, it will return null,
                            // which means that it cannot do secondary aggregation
                            // and the materialization recognition will fail.
                            val aggFunction: SqlAggFunction = aggregateCall.getAggregation().getRollup()
                            if (aggFunction != null) {
                                newAggCall = AggregateCall.create(
                                    aggFunction,
                                    aggregateCall.isDistinct(), aggregateCall.isApproximate(),
                                    aggregateCall.ignoreNulls(),
                                    ImmutableList.of(target.groupSet.cardinality() + i), -1,
                                    aggregateCall.distinctKeys, aggregateCall.collation,
                                    aggregateCall.type, aggregateCall.name
                                )
                            }
                        }
                    }
                    // 2. try to build a new agg-cal by target's grouping.
                    if (newAggCall == null) {
                        newAggCall = genAggCallWithTargetGrouping(aggregateCall, targetGroupByIndexList)
                    }
                    if (newAggCall == null) {
                        // gen agg call fail.
                        return null
                    }
                    aggregateCalls.add(newAggCall)
                }
                result = if (targetCond != null && !targetCond.isAlwaysTrue()) {
                    val compenRexProgram: RexProgram = RexProgram.create(
                        target.rowType, rexBuilder.identityProjects(target.rowType),
                        targetCond, target.rowType, rexBuilder
                    )
                    MutableAggregate.of(
                        MutableCalc.of(target, compenRexProgram),
                        groupSet, groupSets, aggregateCalls
                    )
                } else {
                    MutableAggregate.of(
                        target, groupSet, groupSets, aggregateCalls
                    )
                }
            } else {
                return null
            }
            return result
        }

        /**
         * Generate agg call by mv's grouping.
         */
        @Nullable
        private fun genAggCallWithTargetGrouping(
            queryAggCall: AggregateCall,
            targetGroupByIndexes: List<Integer>
        ): AggregateCall? {
            val aggregation: SqlAggFunction = queryAggCall.getAggregation()
            val argList: List<Integer> = queryAggCall.getArgList()
            val newArgList: List<Integer> = ArrayList()
            for (arg in argList) {
                val newArgIndex = targetGroupByIndexes.indexOf(arg)
                if (newArgIndex < 0) {
                    return null
                }
                newArgList.add(newArgIndex)
            }
            val isAllowBuild: Boolean
            isAllowBuild = if (newArgList.size() === 0) {
                // Size of agg-call's args is empty, we stop to build a new agg-call,
                // eg: count(1) or count(*).
                false
            } else if (queryAggCall.isDistinct()) {
                // Args of agg-call is distinct, we can build a new agg-call.
                true
            } else if (aggregation.getDistinctOptionality() === Optionality.IGNORED) {
                // If attribute of agg-call's distinct could be ignore, we can build a new agg-call.
                true
            } else {
                false
            }
            return if (!isAllowBuild) {
                null
            } else AggregateCall.create(
                aggregation,
                queryAggCall.isDistinct(), queryAggCall.isApproximate(),
                queryAggCall.ignoreNulls(),
                newArgList, -1, queryAggCall.distinctKeys,
                queryAggCall.collation, queryAggCall.type,
                queryAggCall.name
            )
        }

        @Deprecated // to be removed before 2.0
        @Nullable
        fun getRollup(aggregation: SqlAggFunction): SqlAggFunction? {
            return if (aggregation === SqlStdOperatorTable.SUM || aggregation === SqlStdOperatorTable.MIN || aggregation === SqlStdOperatorTable.MAX || aggregation === SqlStdOperatorTable.SOME || aggregation === SqlStdOperatorTable.EVERY || aggregation === SqlLibraryOperators.BOOL_AND || aggregation === SqlLibraryOperators.BOOL_OR || aggregation === SqlLibraryOperators.LOGICAL_AND || aggregation === SqlLibraryOperators.LOGICAL_OR || aggregation === SqlStdOperatorTable.SUM0 || aggregation === SqlStdOperatorTable.ANY_VALUE) {
                aggregation
            } else if (aggregation === SqlStdOperatorTable.COUNT) {
                SqlStdOperatorTable.SUM0
            } else {
                null
            }
        }

        /** Builds a shuttle that stores a list of expressions, and can map incoming
         * expressions to references to them.  */
        private fun getRexShuttle(rexNodes: List<RexNode>): RexShuttle {
            val map: Map<RexNode, Integer> = HashMap()
            for (i in 0 until rexNodes.size()) {
                val rexNode: RexNode = rexNodes[i]
                if (map.containsKey(rexNode)) {
                    continue
                }
                map.put(rexNode, i)
            }
            return object : RexShuttle() {
                @Override
                fun visitInputRef(ref: RexInputRef): RexNode {
                    val integer: Integer? = map[ref]
                    if (integer != null) {
                        return RexInputRef(integer, ref.getType())
                    }
                    throw MatchFailed.INSTANCE
                }

                @Override
                fun visitCall(call: RexCall): RexNode {
                    val integer: Integer? = map[call]
                    return if (integer != null) {
                        RexInputRef(integer, call.getType())
                    } else super.visitCall(call)
                }

                @Override
                fun visitLiteral(literal: RexLiteral): RexNode {
                    val integer: Integer? = map[literal]
                    return if (integer != null) {
                        RexInputRef(integer, literal.getType())
                    } else super.visitLiteral(literal)
                }
            }
        }

        /** Returns whether two relational expressions have the same row-type.  */
        fun equalType(
            desc0: String?, rel0: MutableRel, desc1: String?,
            rel1: MutableRel, litmus: Litmus?
        ): Boolean {
            return RelOptUtil.equal(desc0, rel0.rowType, desc1, rel1.rowType, litmus)
        }

        /**
         * Check if filter under join can be pulled up,
         * when meeting JoinOnCalc of query unify to Join of target.
         * Working in rules: [JoinOnLeftCalcToJoinUnifyRule] <br></br>
         * [JoinOnRightCalcToJoinUnifyRule] <br></br>
         * [JoinOnCalcsToJoinUnifyRule] <br></br>
         */
        private fun canPullUpFilterUnderJoin(
            joinType: JoinRelType,
            @Nullable leftFilterRexNode: RexNode?, @Nullable rightFilterRexNode: RexNode?
        ): Boolean {
            if (joinType === JoinRelType.INNER) {
                return true
            }
            if (joinType === JoinRelType.LEFT
                && (rightFilterRexNode == null || rightFilterRexNode.isAlwaysTrue())
            ) {
                return true
            }
            if (joinType === JoinRelType.RIGHT
                && (leftFilterRexNode == null || leftFilterRexNode.isAlwaysTrue())
            ) {
                return true
            }
            return if (joinType === JoinRelType.FULL
                && ((rightFilterRexNode == null || rightFilterRexNode.isAlwaysTrue())
                        && (leftFilterRexNode == null || leftFilterRexNode.isAlwaysTrue()))
            ) {
                true
            } else false
        }
    }
}
