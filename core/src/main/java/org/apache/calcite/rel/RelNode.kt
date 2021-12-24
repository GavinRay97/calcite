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
package org.apache.calcite.rel

import org.apache.calcite.plan.Convention

/**
 * A `RelNode` is a relational expression.
 *
 *
 * Relational expressions process data, so their names are typically verbs:
 * Sort, Join, Project, Filter, Scan, Sample.
 *
 *
 * A relational expression is not a scalar expression; see
 * [org.apache.calcite.sql.SqlNode] and [RexNode].
 *
 *
 * If this type of relational expression has some particular planner rules,
 * it should implement the *public static* method
 * [AbstractRelNode.register].
 *
 *
 * When a relational expression comes to be implemented, the system allocates
 * a [org.apache.calcite.plan.RelImplementor] to manage the process. Every
 * implementable relational expression has a [RelTraitSet] describing its
 * physical attributes. The RelTraitSet always contains a [Convention]
 * describing how the expression passes data to its consuming
 * relational expression, but may contain other traits, including some applied
 * externally. Because traits can be applied externally, implementations of
 * RelNode should never assume the size or contents of their trait set (beyond
 * those traits configured by the RelNode itself).
 *
 *
 * For each calling-convention, there is a corresponding sub-interface of
 * RelNode. For example,
 * `org.apache.calcite.adapter.enumerable.EnumerableRel`
 * has operations to manage the conversion to a graph of
 * `org.apache.calcite.adapter.enumerable.EnumerableConvention`
 * calling-convention, and it interacts with a
 * `EnumerableRelImplementor`.
 *
 *
 * A relational expression is only required to implement its
 * calling-convention's interface when it is actually implemented, that is,
 * converted into a plan/program. This means that relational expressions which
 * cannot be implemented, such as converters, are not required to implement
 * their convention's interface.
 *
 *
 * Every relational expression must derive from [AbstractRelNode]. (Why
 * have the `RelNode` interface, then? We need a root interface,
 * because an interface can only derive from an interface.)
 */
interface RelNode : RelOptNode, Cloneable {
    //~ Methods ----------------------------------------------------------------
    /**
     * Return the CallingConvention trait from this RelNode's
     * [trait set][.getTraitSet].
     *
     * @return this RelNode's CallingConvention
     */
    val convention: Convention?
        @Pure @Nullable get

    /**
     * Returns the name of the variable which is to be implicitly set at runtime
     * each time a row is returned from the first input of this relational
     * expression; or null if there is no variable.
     *
     * @return Name of correlating variable, or null
     */
    val correlVariable: String?
        @Nullable get

    /**
     * Returns the `i`<sup>th</sup> input relational expression.
     *
     * @param i Ordinal of input
     * @return `i`<sup>th</sup> input
     */
    fun getInput(i: Int): RelNode?

    /**
     * Returns the type of the rows returned by this relational expression.
     */
    val rowType: RelDataType
        @Override get

    /**
     * Returns the type of the rows expected for an input. Defaults to
     * [.getRowType].
     *
     * @param ordinalInParent input's 0-based ordinal with respect to this
     * parent rel
     * @return expected row type
     */
    fun getExpectedInputRowType(ordinalInParent: Int): RelDataType?

    /**
     * Returns an array of this relational expression's inputs. If there are no
     * inputs, returns an empty list, not `null`.
     *
     * @return Array of this relational expression's inputs
     */
    val inputs: List<RelNode?>?
        @Override get

    /**
     * Returns an estimate of the number of rows this relational expression will
     * return.
     *
     *
     * NOTE jvs 29-Mar-2006: Don't call this method directly. Instead, use
     * [RelMetadataQuery.getRowCount], which gives plugins a chance to
     * override the rel's default ideas about row count.
     *
     * @param mq Metadata query
     * @return Estimate of the number of rows this relational expression will
     * return
     */
    fun estimateRowCount(mq: RelMetadataQuery?): Double

    /**
     * Returns the variables that are set in this relational
     * expression but also used and therefore not available to parents of this
     * relational expression.
     *
     *
     * Note: only [org.apache.calcite.rel.core.Correlate] should set
     * variables.
     *
     * @return Names of variables which are set in this relational
     * expression
     */
    val variablesSet: Set<Any?>?

    /**
     * Collects variables known to be used by this expression or its
     * descendants. By default, no such information is available and must be
     * derived by analyzing sub-expressions, but some optimizer implementations
     * may insert special expressions which remember such information.
     *
     * @param variableSet receives variables used
     */
    fun collectVariablesUsed(variableSet: Set<CorrelationId?>?)

    /**
     * Collects variables set by this expression.
     * TODO: is this required?
     *
     * @param variableSet receives variables known to be set by
     */
    fun collectVariablesSet(variableSet: Set<CorrelationId?>?)

    /**
     * Interacts with the [RelVisitor] in a
     * [visitor pattern][org.apache.calcite.util.Glossary.VISITOR_PATTERN] to
     * traverse the tree of relational expressions.
     *
     * @param visitor Visitor that will traverse the tree of relational
     * expressions
     */
    fun childrenAccept(visitor: RelVisitor?)

    /**
     * Returns the cost of this plan (not including children). The base
     * implementation throws an error; derived classes should override.
     *
     *
     * NOTE jvs 29-Mar-2006: Don't call this method directly. Instead, use
     * [RelMetadataQuery.getNonCumulativeCost], which gives plugins a
     * chance to override the rel's default ideas about cost.
     *
     * @param planner Planner for cost calculation
     * @param mq Metadata query
     * @return Cost of this plan (not including children)
     */
    @Nullable
    fun computeSelfCost(planner: RelOptPlanner?, mq: RelMetadataQuery?): RelOptCost?

    /**
     * Returns a metadata interface.
     *
     * @param <M> Type of metadata being requested
     * @param metadataClass Metadata interface
     * @param mq Metadata query
     *
     * @return Metadata object that supplies the desired metadata (never null,
     * although if the information is not present the metadata object may
     * return null from all methods)
    </M> */
    @Deprecated
    @Deprecated(
        """Use {@link RelMetadataQuery} via {@link #getCluster()}.

    """
    )
    fun  // to be removed before 2.0
            <M : Metadata?> metadata(metadataClass: Class<M>?, mq: RelMetadataQuery?): M

    /**
     * Describes the inputs and attributes of this relational expression.
     * Each node should call `super.explain`, then call the
     * [org.apache.calcite.rel.externalize.RelWriterImpl.input]
     * and
     * [RelWriter.item]
     * methods for each input and attribute.
     *
     * @param pw Plan writer
     */
    fun explain(pw: RelWriter?)

    /**
     * Returns a relational expression string of this `RelNode`.
     * The string returned is the same as
     * [RelOptUtil.toString].
     *
     * This method is intended mainly for use while debugging in an IDE,
     * as a convenient short-hand for RelOptUtil.toString.
     * We recommend that classes implementing this interface
     * do not override this method.
     *
     * @return Relational expression string of this `RelNode`
     */
    fun explain(): String? {
        return RelOptUtil.toString(this)
    }

    /**
     * Receives notification that this expression is about to be registered. The
     * implementation of this method must at least register all child
     * expressions.
     *
     * @param planner Planner that plans this relational node
     * @return Relational expression that should be used by the planner
     */
    fun onRegister(planner: RelOptPlanner?): RelNode?

    /**
     * Returns a digest string of this `RelNode`.
     *
     *
     * Each call creates a new digest string,
     * so don't forget to cache the result if necessary.
     *
     * @return Digest string of this `RelNode`
     *
     * @see .getRelDigest
     */
    val digest: String?
        @Override get() = relDigest.toString()

    /**
     * Returns a digest of this `RelNode`.
     *
     *
     * INTERNAL USE ONLY. For use by the planner.
     *
     * @return Digest of this `RelNode`
     * @see .getDigest
     */
    val relDigest: RelDigest
        @API(since = "1.24", status = API.Status.INTERNAL) get

    /**
     * Recomputes the digest.
     *
     *
     * INTERNAL USE ONLY. For use by the planner.
     *
     * @see .getDigest
     */
    @API(since = "1.24", status = API.Status.INTERNAL)
    fun recomputeDigest()

    /**
     * Deep equality check for RelNode digest.
     *
     *
     * By default this method collects digest attributes from
     * explain terms, then compares each attribute pair.
     *
     * @return Whether the 2 RelNodes are equivalent or have the same digest.
     * @see .deepHashCode
     */
    @EnsuresNonNullIf(expression = "#1", result = true)
    fun deepEquals(@Nullable obj: Object?): Boolean

    /**
     * Compute deep hash code for RelNode digest.
     *
     * @see .deepEquals
     */
    fun deepHashCode(): Int

    /**
     * Replaces the `ordinalInParent`<sup>th</sup> input. You must
     * override this method if you override [.getInputs].
     *
     * @param ordinalInParent Position of the child input, 0 is the first
     * @param p New node that should be put at position `ordinalInParent`
     */
    fun replaceInput(
        ordinalInParent: Int,
        p: RelNode?
    )

    /**
     * If this relational expression represents an access to a table, returns
     * that table, otherwise returns null.
     *
     * @return If this relational expression represents an access to a table,
     * returns that table, otherwise returns null
     */
    val table: RelOptTable?
        @Nullable get

    /**
     * Returns the name of this relational expression's class, sans package
     * name, for use in explain. For example, for a `
     * org.apache.calcite.rel.ArrayRel.ArrayReader`, this method returns
     * "ArrayReader".
     *
     * @return Name of this relational expression's class, sans package name,
     * for use in explain
     */
    val relTypeName: String?

    /**
     * Returns whether this relational expression is valid.
     *
     *
     * If assertions are enabled, this method is typically called with `
     * litmus` = `THROW`, as follows:
     *
     * <blockquote>
     * <pre>assert rel.isValid(Litmus.THROW)</pre>
    </blockquote> *
     *
     *
     * This signals that the method can throw an [AssertionError] if it
     * is not valid.
     *
     * @param litmus What to do if invalid
     * @param context Context for validity checking
     * @return Whether relational expression is valid
     * @throws AssertionError if this relational expression is invalid and
     * litmus is THROW
     */
    fun isValid(litmus: Litmus?, @Nullable context: Context?): Boolean

    /**
     * Creates a copy of this relational expression, perhaps changing traits and
     * inputs.
     *
     *
     * Sub-classes with other important attributes are encouraged to create
     * variants of this method with more parameters.
     *
     * @param traitSet Trait set
     * @param inputs   Inputs
     * @return Copy of this relational expression, substituting traits and
     * inputs
     */
    fun copy(
        traitSet: RelTraitSet?,
        inputs: List<RelNode?>?
    ): RelNode

    /**
     * Registers any special rules specific to this kind of relational
     * expression.
     *
     *
     * The planner calls this method this first time that it sees a
     * relational expression of this class. The derived class should call
     * [org.apache.calcite.plan.RelOptPlanner.addRule] for each rule, and
     * then call `super.register`.
     *
     * @param planner Planner to be used to register additional relational
     * expressions
     */
    fun register(planner: RelOptPlanner?)

    /**
     * Indicates whether it is an enforcer operator, e.g. PhysicalSort,
     * PhysicalHashDistribute, etc. As an enforcer, the operator must be
     * created only when required traitSet is not satisfied by its input.
     *
     * @return Whether it is an enforcer operator
     */
    val isEnforcer: Boolean
        get() = false

    /**
     * Accepts a visit from a shuttle.
     *
     * @param shuttle Shuttle
     * @return A copy of this node incorporating changes made by the shuttle to
     * this node's children
     */
    fun accept(shuttle: RelShuttle?): RelNode?

    /**
     * Accepts a visit from a shuttle. If the shuttle updates expression, then
     * a copy of the relation should be created. This new relation might have
     * a different row-type.
     *
     * @param shuttle Shuttle
     * @return A copy of this node incorporating changes made by the shuttle to
     * this node's children
     */
    fun accept(shuttle: RexShuttle?): RelNode?

    /** Returns whether a field is nullable.  */
    fun fieldIsNullable(i: Int): Boolean {
        return rowType.getFieldList().get(i).getType().isNullable()
    }

    /** Context of a relational expression, for purposes of checking validity.  */
    interface Context {
        fun correlationIds(): Set<CorrelationId?>?
    }
}
