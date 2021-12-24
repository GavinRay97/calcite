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

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider
import org.apache.calcite.rel.metadata.RelMetadataProvider
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rex.RexExecutor
import org.apache.calcite.util.CancelFlag
import org.apache.calcite.util.trace.CalciteTrace
import org.slf4j.Logger
import java.util.List
import java.util.regex.Pattern

/**
 * A `RelOptPlanner` is a query optimizer: it transforms a relational
 * expression into a semantically equivalent relational expression, according to
 * a given set of rules and a cost model.
 */
interface RelOptPlanner {
    //~ Methods ----------------------------------------------------------------
    /**
     * Returns the root node of this query.
     *
     * @return Root node
     */
    /**
     * Sets the root node of this query.
     *
     * @param rel Relational expression
     */
    @get:Nullable
    var root: RelNode?

    /**
     * Registers a rel trait definition. If the [RelTraitDef] has already
     * been registered, does nothing.
     *
     * @return whether the RelTraitDef was added, as per
     * [java.util.Collection.add]
     */
    fun addRelTraitDef(relTraitDef: RelTraitDef?): Boolean

    /**
     * Clear all the registered RelTraitDef.
     */
    fun clearRelTraitDefs()

    /**
     * Returns the list of active trait types.
     */
    val relTraitDefs: List<Any?>?

    /**
     * Removes all internal state, including all registered rules,
     * materialized views, and lattices.
     */
    fun clear()

    /**
     * Returns the list of all registered rules.
     */
    val rules: List<Any?>?

    /**
     * Registers a rule.
     *
     *
     * If the rule has already been registered, does nothing.
     * This method determines if the given rule is a
     * [org.apache.calcite.rel.convert.ConverterRule] and pass the
     * ConverterRule to all
     * [registered][.addRelTraitDef] RelTraitDef
     * instances.
     *
     * @return whether the rule was added, as per
     * [java.util.Collection.add]
     */
    fun addRule(rule: RelOptRule?): Boolean

    /**
     * Removes a rule.
     *
     * @return true if the rule was present, as per
     * [java.util.Collection.remove]
     */
    fun removeRule(rule: RelOptRule?): Boolean

    /**
     * Provides the Context created when this planner was constructed.
     *
     * @return Never null; either an externally defined context, or a dummy
     * context that returns null for each requested interface
     */
    val context: Context?

    /**
     * Sets the exclusion filter to use for this planner. Rules which match the
     * given pattern will not be fired regardless of whether or when they are
     * added to the planner.
     *
     * @param exclusionFilter pattern to match for exclusion; null to disable
     * filtering
     */
    fun setRuleDescExclusionFilter(@Nullable exclusionFilter: Pattern?)

    /**
     * Does nothing.
     *
     * @param cancelFlag flag which the planner should periodically check
     */
    @Deprecated
    @Deprecated(
        """Previously, this method installed the cancellation-checking
    flag for this planner, but is now deprecated. Now, you should add a
    {@link CancelFlag} to the {@link Context} passed to the constructor.

    """
    )
    fun  // to be removed before 2.0
            setCancelFlag(cancelFlag: CancelFlag?)

    /**
     * Changes a relational expression to an equivalent one with a different set
     * of traits.
     *
     * @param rel Relational expression (may or may not have been registered; must
     * not have the desired traits)
     * @param toTraits Trait set to convert the relational expression to
     * @return Relational expression with desired traits. Never null, but may be
     * abstract
     */
    fun changeTraits(rel: RelNode?, toTraits: RelTraitSet?): RelNode?

    /**
     * Negotiates an appropriate planner to deal with distributed queries. The
     * idea is that the schemas decide among themselves which has the most
     * knowledge. Right now, the local planner retains control.
     */
    fun chooseDelegate(): RelOptPlanner?

    /**
     * Defines a pair of relational expressions that are equivalent.
     *
     *
     * Typically `tableRel` is a
     * [org.apache.calcite.rel.logical.LogicalTableScan] representing a
     * table that is a materialized view and `queryRel` is the SQL
     * expression that populates that view. The intention is that
     * `tableRel` is cheaper to evaluate and therefore if the query being
     * optimized uses (or can be rewritten to use) `queryRel` as a
     * sub-expression then it can be optimized by using `tableRel`
     * instead.
     */
    fun addMaterialization(materialization: RelOptMaterialization?)

    /**
     * Returns the materializations that have been registered with the planner.
     */
    val materializations: List<Any?>?

    /**
     * Defines a lattice.
     *
     *
     * The lattice may have materializations; it is not necessary to call
     * [.addMaterialization] for these; they are registered implicitly.
     */
    fun addLattice(lattice: RelOptLattice?)

    /**
     * Retrieves a lattice, given its star table.
     */
    @Nullable
    fun getLattice(table: RelOptTable?): RelOptLattice?

    /**
     * Finds the most efficient expression to implement this query.
     *
     * @throws CannotPlanException if cannot find a plan
     */
    fun findBestExp(): RelNode?

    /**
     * Returns the factory that creates
     * [org.apache.calcite.plan.RelOptCost]s.
     */
    val costFactory: RelOptCostFactory?

    /**
     * Computes the cost of a RelNode. In most cases, this just dispatches to
     * [RelMetadataQuery.getCumulativeCost].
     *
     * @param rel Relational expression of interest
     * @param mq Metadata query
     * @return estimated cost
     */
    @Nullable
    fun getCost(rel: RelNode?, mq: RelMetadataQuery?): RelOptCost?
    // CHECKSTYLE: IGNORE 2

    @Deprecated // to be removed before 2.0
    @Nullable
    @Deprecated(
        """Use {@link #getCost(RelNode, RelMetadataQuery)}
    or, better, call {@link RelMetadataQuery#getCumulativeCost(RelNode)}. """
    )
    fun getCost(rel: RelNode?): RelOptCost?

    /**
     * Registers a relational expression in the expression bank.
     *
     *
     * After it has been registered, you may not modify it.
     *
     *
     * The expression must not already have been registered. If you are not
     * sure whether it has been registered, call
     * [.ensureRegistered].
     *
     * @param rel      Relational expression to register (must not already be
     * registered)
     * @param equivRel Relational expression it is equivalent to (may be null)
     * @return the same expression, or an equivalent existing expression
     */
    fun register(
        rel: RelNode?,
        @Nullable equivRel: RelNode?
    ): RelNode?

    /**
     * Registers a relational expression if it is not already registered.
     *
     *
     * If `equivRel` is specified, `rel` is placed in the same
     * equivalence set. It is OK if `equivRel` has different traits;
     * `rel` will end up in a different subset of the same set.
     *
     *
     * It is OK if `rel` is a subset.
     *
     * @param rel      Relational expression to register
     * @param equivRel Relational expression it is equivalent to (may be null)
     * @return Registered relational expression
     */
    fun ensureRegistered(rel: RelNode?, @Nullable equivRel: RelNode?): RelNode?

    /**
     * Determines whether a relational expression has been registered.
     *
     * @param rel expression to test
     * @return whether rel has been registered
     */
    fun isRegistered(rel: RelNode?): Boolean

    /**
     * Tells this planner that a schema exists. This is the schema's chance to
     * tell the planner about all of the special transformation rules.
     */
    fun registerSchema(schema: RelOptSchema?)

    /**
     * Adds a listener to this planner.
     *
     * @param newListener new listener to be notified of events
     */
    fun addListener(newListener: RelOptListener?)

    /**
     * Gives this planner a chance to register one or more
     * [RelMetadataProvider]s in the chain which will be used to answer
     * metadata queries.
     *
     *
     * Planners which use their own relational expressions internally
     * to represent concepts such as equivalence classes will generally need to
     * supply corresponding metadata providers.
     *
     * @param list receives planner's custom providers, if any
     */
    @Deprecated
    fun  // to be removed before 2.0
            registerMetadataProviders(list: List<RelMetadataProvider?>?)

    /**
     * Gets a timestamp for a given rel's metadata. This timestamp is used by
     * [CachingRelMetadataProvider] to decide whether cached metadata has
     * gone stale.
     *
     * @param rel rel of interest
     * @return timestamp of last change which might affect metadata derivation
     */
    @Deprecated
    fun  // to be removed before 2.0
            getRelMetadataTimestamp(rel: RelNode?): Long

    /**
     * Prunes a node from the planner.
     *
     *
     * When a node is pruned, the related pending rule
     * calls are cancelled, and future rules will not fire.
     * This can be used to reduce the search space.
     * @param rel the node to prune.
     */
    fun prune(rel: RelNode?)

    /**
     * Registers a class of RelNode. If this class of RelNode has been seen
     * before, does nothing.
     *
     * @param node Relational expression
     */
    fun registerClass(node: RelNode?)

    /**
     * Creates an empty trait set. It contains all registered traits, and the
     * default values of any traits that have them.
     *
     *
     * The empty trait set acts as the prototype (a kind of factory) for all
     * subsequently created trait sets.
     *
     * @return Empty trait set
     */
    fun emptyTraitSet(): RelTraitSet?
    /** Returns the executor used to evaluate constant expressions.  */
    /** Sets the object that can execute scalar expressions.  */
    @get:Nullable
    var executor: RexExecutor?

    /** Called when a relational expression is copied to a similar expression.  */
    fun onCopy(rel: RelNode?, newRel: RelNode?)
    // CHECKSTYLE: IGNORE 1

    @Deprecated
    @Deprecated("Use {@link RexExecutor} ")
    interface Executor : RexExecutor

    /**
     * Thrown by [org.apache.calcite.plan.RelOptPlanner.findBestExp].
     */
    class CannotPlanException(message: String?) : RuntimeException(message)
    companion object {
        //~ Static fields/initializers ---------------------------------------------
        val LOGGER: Logger = CalciteTrace.getPlannerTracer()
    }
}
