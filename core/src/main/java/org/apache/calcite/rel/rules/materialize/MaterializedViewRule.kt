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
package org.apache.calcite.rel.rules.materialize

import org.apache.calcite.plan.RelOptMaterialization
import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.plan.RelOptPredicateList
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.RelRule
import org.apache.calcite.plan.SubstitutionVisitor
import org.apache.calcite.plan.hep.HepProgram
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.RelReferentialConstraint
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rel.core.Join
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexExecutor
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexShuttle
import org.apache.calcite.rex.RexSimplify
import org.apache.calcite.rex.RexTableInputRef
import org.apache.calcite.rex.RexTableInputRef.RelTableRef
import org.apache.calcite.rex.RexUtil
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.Pair
import org.apache.calcite.util.Util
import org.apache.calcite.util.graph.DefaultDirectedGraph
import org.apache.calcite.util.graph.DefaultEdge
import org.apache.calcite.util.graph.DirectedGraph
import org.apache.calcite.util.mapping.IntPair
import org.apache.calcite.util.mapping.Mapping
import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.BiMap
import com.google.common.collect.HashBiMap
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Multimap
import com.google.common.collect.Sets
import java.util.ArrayList
import java.util.Collection
import java.util.Collections
import java.util.HashMap
import java.util.HashSet
import java.util.Iterator
import java.util.LinkedHashSet
import java.util.List
import java.util.Map
import java.util.Set
import org.apache.calcite.linq4j.Nullness.castNonNull
import java.util.Objects.requireNonNull

/**
 * Planner rule that converts a [org.apache.calcite.rel.core.Project]
 * followed by [org.apache.calcite.rel.core.Aggregate] or an
 * [org.apache.calcite.rel.core.Aggregate] to a scan (and possibly
 * other operations) over a materialized view.
 *
 * @param <C> Configuration type
</C> */
abstract class MaterializedViewRule<C : MaterializedViewRule.Config?>  //~ Constructors -----------------------------------------------------------
/** Creates a MaterializedViewRule.  */
internal constructor(config: C) : RelRule<C>(config) {
    @Override
    fun matches(call: RelOptRuleCall): Boolean {
        return !call.getPlanner().getMaterializations().isEmpty()
    }

    /**
     * Rewriting logic is based on "Optimizing Queries Using Materialized Views:
     * A Practical, Scalable Solution" by Goldstein and Larson.
     *
     *
     * On the query side, rules matches a Project-node chain or node, where node
     * is either an Aggregate or a Join. Subplan rooted at the node operator must
     * be composed of one or more of the following operators: TableScan, Project,
     * Filter, and Join.
     *
     *
     * For each join MV, we need to check the following:
     *
     *  1.  The plan rooted at the Join operator in the view produces all rows
     * needed by the plan rooted at the Join operator in the query.
     *  1.  All columns required by compensating predicates, i.e., predicates that
     * need to be enforced over the view, are available at the view output.
     *  1.  All output expressions can be computed from the output of the view.
     *  1.  All output rows occur with the correct duplication factor. We might
     * rely on existing Unique-Key - Foreign-Key relationships to extract that
     * information.
     *
     *
     *
     * In turn, for each aggregate MV, we need to check the following:
     *
     *  1.  The plan rooted at the Aggregate operator in the view produces all rows
     * needed by the plan rooted at the Aggregate operator in the query.
     *  1.  All columns required by compensating predicates, i.e., predicates that
     * need to be enforced over the view, are available at the view output.
     *  1.  The grouping columns in the query are a subset of the grouping columns
     * in the view.
     *  1.  All columns required to perform further grouping are available in the
     * view output.
     *  1.  All columns required to compute output expressions are available in the
     * view output.
     *
     *
     *
     * The rule contains multiple extensions compared to the original paper. One of
     * them is the possibility of creating rewritings using Union operators, e.g., if
     * the result of a query is partially contained in the materialized view.
     */
    protected fun perform(call: RelOptRuleCall, @Nullable topProject: Project?, node: RelNode) {
        val rexBuilder: RexBuilder = node.getCluster().getRexBuilder()
        val mq: RelMetadataQuery = call.getMetadataQuery()
        val planner: RelOptPlanner = call.getPlanner()
        val executor: RexExecutor = Util.first(planner.getExecutor(), RexUtil.EXECUTOR)
        val predicates: RelOptPredicateList = RelOptPredicateList.EMPTY
        val simplify = RexSimplify(rexBuilder, predicates, executor)
        val materializations: List<RelOptMaterialization> = planner.getMaterializations()
        if (!materializations.isEmpty()) {
            // 1. Explore query plan to recognize whether preconditions to
            // try to generate a rewriting are met
            if (!isValidPlan(topProject, node, mq)) {
                return
            }

            // 2. Initialize all query related auxiliary data structures
            // that will be used throughout query rewriting process
            // Generate query table references
            val queryTableRefs: Set<RelTableRef> = mq.getTableReferences(node)
                ?: // Bail out
                return

            // Extract query predicates
            val queryPredicateList: RelOptPredicateList = mq.getAllPredicates(node)
                ?: // Bail out
                return
            val pred: RexNode = simplify.simplifyUnknownAsFalse(
                RexUtil.composeConjunction(
                    rexBuilder,
                    queryPredicateList.pulledUpPredicates
                )
            )
            val queryPreds: Pair<RexNode?, RexNode?> = splitPredicates(rexBuilder, pred)

            // Extract query equivalence classes. An equivalence class is a set
            // of columns in the query output that are known to be equal.
            val qEC = EquivalenceClasses()
            for (conj in RelOptUtil.conjunctions(queryPreds.left)) {
                assert(conj.isA(SqlKind.EQUALS))
                val equiCond: RexCall = conj as RexCall
                qEC.addEquivalenceClass(
                    equiCond.getOperands().get(0) as RexTableInputRef,
                    equiCond.getOperands().get(1) as RexTableInputRef
                )
            }

            // 3. We iterate through all applicable materializations trying to
            // rewrite the given query
            for (materialization in materializations) {
                var view: RelNode = materialization.tableRel
                var topViewProject: Project?
                var viewNode: RelNode
                if (materialization.queryRel is Project) {
                    topViewProject = materialization.queryRel as Project
                    viewNode = topViewProject.getInput()
                } else {
                    topViewProject = null
                    viewNode = materialization.queryRel
                }

                // Extract view table references
                val viewTableRefs: Set<RelTableRef> = mq.getTableReferences(viewNode)
                    ?: // Skip it
                    continue

                // Filter relevant materializations. Currently, we only check whether
                // the materialization contains any table that is used by the query
                // TODO: Filtering of relevant materializations can be improved to be more fine-grained.
                var applicable = false
                for (tableRef in viewTableRefs) {
                    if (queryTableRefs.contains(tableRef)) {
                        applicable = true
                        break
                    }
                }
                if (!applicable) {
                    // Skip it
                    continue
                }

                // 3.1. View checks before proceeding
                if (!isValidPlan(topViewProject, viewNode, mq)) {
                    // Skip it
                    continue
                }

                // 3.2. Initialize all query related auxiliary data structures
                // that will be used throughout query rewriting process
                // Extract view predicates
                val viewPredicateList: RelOptPredicateList = mq.getAllPredicates(viewNode)
                    ?: // Skip it
                    continue
                val viewPred: RexNode = simplify.simplifyUnknownAsFalse(
                    RexUtil.composeConjunction(
                        rexBuilder,
                        viewPredicateList.pulledUpPredicates
                    )
                )
                val viewPreds: Pair<RexNode?, RexNode?> = splitPredicates(rexBuilder, viewPred)

                // Extract view tables
                val matchModality: MatchModality
                val compensationEquiColumns: Multimap<RexTableInputRef?, RexTableInputRef?> = ArrayListMultimap.create()
                if (!queryTableRefs.equals(viewTableRefs)) {
                    // We try to compensate, e.g., for join queries it might be
                    // possible to join missing tables with view to compute result.
                    // Two supported cases: query tables are subset of view tables (we need to
                    // check whether they are cardinality-preserving joins), or view tables are
                    // subset of query tables (add additional tables through joins if possible)
                    if (viewTableRefs.containsAll(queryTableRefs)) {
                        matchModality = MatchModality.QUERY_PARTIAL
                        val vEC = EquivalenceClasses()
                        for (conj in RelOptUtil.conjunctions(viewPreds.left)) {
                            assert(conj.isA(SqlKind.EQUALS))
                            val equiCond: RexCall = conj as RexCall
                            vEC.addEquivalenceClass(
                                equiCond.getOperands().get(0) as RexTableInputRef,
                                equiCond.getOperands().get(1) as RexTableInputRef
                            )
                        }
                        if (!compensatePartial(
                                viewTableRefs, vEC, queryTableRefs,
                                compensationEquiColumns
                            )
                        ) {
                            // Cannot rewrite, skip it
                            continue
                        }
                    } else if (queryTableRefs.containsAll(viewTableRefs)) {
                        matchModality = MatchModality.VIEW_PARTIAL
                        val partialRewritingResult = compensateViewPartial(
                            call.builder(), rexBuilder, mq, view,
                            topProject, node, queryTableRefs, qEC,
                            topViewProject, viewNode, viewTableRefs
                        )
                            ?: // Cannot rewrite, skip it
                            continue
                        // Rewrite succeeded
                        view = partialRewritingResult.newView
                        topViewProject = partialRewritingResult.newTopViewProject
                        viewNode = partialRewritingResult.newViewNode
                    } else {
                        // Skip it
                        continue
                    }
                } else {
                    matchModality = MatchModality.COMPLETE
                }

                // 4. We map every table in the query to a table with the same qualified
                // name (all query tables are contained in the view, thus this is equivalent
                // to mapping every table in the query to a view table).
                val multiMapTables: Multimap<RelTableRef?, RelTableRef?> = ArrayListMultimap.create()
                for (queryTableRef1 in queryTableRefs) {
                    for (queryTableRef2 in queryTableRefs) {
                        if (queryTableRef1.getQualifiedName().equals(
                                queryTableRef2.getQualifiedName()
                            )
                        ) {
                            multiMapTables.put(queryTableRef1, queryTableRef2)
                        }
                    }
                }

                // If a table is used multiple times, we will create multiple mappings,
                // and we will try to rewrite the query using each of the mappings.
                // Then, we will try to map every source table (query) to a target
                // table (view), and if we are successful, we will try to create
                // compensation predicates to filter the view results further
                // (if needed).
                val flatListMappings: List<BiMap<RelTableRef, RelTableRef>> = generateTableMappings(multiMapTables)
                for (queryToViewTableMapping in flatListMappings) {
                    // TableMapping : mapping query tables -> view tables
                    // 4.0. If compensation equivalence classes exist, we need to add
                    // the mapping to the query mapping
                    val currQEC = EquivalenceClasses.copy(qEC)
                    if (matchModality == MatchModality.QUERY_PARTIAL) {
                        for (e in compensationEquiColumns.entries()) {
                            // Copy origin
                            val queryTableRef: RelTableRef = queryToViewTableMapping.inverse().get(
                                e.getKey().getTableRef()
                            )
                            val queryColumnRef: RexTableInputRef = RexTableInputRef.of(
                                requireNonNull(
                                    queryTableRef
                                ) { "queryTableRef is null for tableRef " + e.getKey().getTableRef() },
                                e.getKey().getIndex(), e.getKey().getType()
                            )
                            // Add to query equivalence classes and table mapping
                            currQEC.addEquivalenceClass(queryColumnRef, e.getValue())
                            queryToViewTableMapping.put(
                                e.getValue().getTableRef(),
                                e.getValue().getTableRef()
                            ) // identity
                        }
                    }

                    // 4.1. Compute compensation predicates, i.e., predicates that need to be
                    // enforced over the view to retain query semantics. The resulting predicates
                    // are expressed using {@link RexTableInputRef} over the query.
                    // First, to establish relationship, we swap column references of the view
                    // predicates to point to query tables and compute equivalence classes.
                    val viewColumnsEquiPred: RexNode = RexUtil.swapTableReferences(
                        rexBuilder, viewPreds.left, queryToViewTableMapping.inverse()
                    )
                    val queryBasedVEC = EquivalenceClasses()
                    for (conj in RelOptUtil.conjunctions(viewColumnsEquiPred)) {
                        assert(conj.isA(SqlKind.EQUALS))
                        val equiCond: RexCall = conj as RexCall
                        queryBasedVEC.addEquivalenceClass(
                            equiCond.getOperands().get(0) as RexTableInputRef,
                            equiCond.getOperands().get(1) as RexTableInputRef
                        )
                    }
                    var compensationPreds: Pair<RexNode?, RexNode?>? = computeCompensationPredicates(
                        rexBuilder, simplify,
                        currQEC, queryPreds, queryBasedVEC, viewPreds,
                        queryToViewTableMapping
                    )
                    if (compensationPreds == null && config.generateUnionRewriting()) {
                        // Attempt partial rewriting using union operator. This rewriting
                        // will read some data from the view and the rest of the data from
                        // the query computation. The resulting predicates are expressed
                        // using {@link RexTableInputRef} over the view.
                        compensationPreds = computeCompensationPredicates(
                            rexBuilder, simplify,
                            queryBasedVEC, viewPreds, currQEC, queryPreds,
                            queryToViewTableMapping.inverse()
                        )
                        if (compensationPreds == null) {
                            // This was our last chance to use the view, skip it
                            continue
                        }
                        val compensationColumnsEquiPred: RexNode = compensationPreds.left
                        val otherCompensationPred: RexNode = compensationPreds.right
                        assert(
                            !compensationColumnsEquiPred.isAlwaysTrue()
                                    || !otherCompensationPred.isAlwaysTrue()
                        )

                        // b. Generate union branch (query).
                        val unionInputQuery: RelNode = rewriteQuery(
                            call.builder(), rexBuilder,
                            simplify, mq, compensationColumnsEquiPred, otherCompensationPred,
                            topProject, node, queryToViewTableMapping, queryBasedVEC, currQEC
                        )
                            ?: // Skip it
                            continue

                        // c. Generate union branch (view).
                        // We trigger the unifying method. This method will either create a Project
                        // or an Aggregate operator on top of the view. It will also compute the
                        // output expressions for the query.
                        val unionInputView: RelNode = rewriteView(
                            call.builder(), rexBuilder, simplify, mq,
                            matchModality, true, view, topProject, node, topViewProject, viewNode,
                            queryToViewTableMapping, currQEC
                        )
                            ?: // Skip it
                            continue

                        // d. Generate final rewriting (union).
                        val result: RelNode = createUnion(
                            call.builder(), rexBuilder,
                            topProject, unionInputQuery, unionInputView
                        )
                            ?: // Skip it
                            continue
                        call.transformTo(result)
                    } else if (compensationPreds != null) {
                        var compensationColumnsEquiPred: RexNode? = compensationPreds.left
                        var otherCompensationPred: RexNode? = compensationPreds.right

                        // a. Compute final compensation predicate.
                        if (!compensationColumnsEquiPred.isAlwaysTrue()
                            || !otherCompensationPred.isAlwaysTrue()
                        ) {
                            // All columns required by compensating predicates must be contained
                            // in the view output (condition 2).
                            val viewExprs: List<RexNode> = if (topViewProject == null) extractReferences(
                                rexBuilder,
                                view
                            ) else topViewProject.getProjects()
                            // For compensationColumnsEquiPred, we use the view equivalence classes,
                            // since we want to enforce the rest
                            if (!compensationColumnsEquiPred.isAlwaysTrue()) {
                                compensationColumnsEquiPred = rewriteExpression(
                                    rexBuilder, mq,
                                    view, viewNode, viewExprs, queryToViewTableMapping.inverse(), queryBasedVEC,
                                    false, compensationColumnsEquiPred
                                )
                                if (compensationColumnsEquiPred == null) {
                                    // Skip it
                                    continue
                                }
                            }
                            // For the rest, we use the query equivalence classes
                            if (!otherCompensationPred.isAlwaysTrue()) {
                                otherCompensationPred = rewriteExpression(
                                    rexBuilder, mq,
                                    view, viewNode, viewExprs, queryToViewTableMapping.inverse(), currQEC,
                                    true, otherCompensationPred
                                )
                                if (otherCompensationPred == null) {
                                    // Skip it
                                    continue
                                }
                            }
                        }
                        val viewCompensationPred: RexNode = RexUtil.composeConjunction(
                            rexBuilder,
                            ImmutableList.of(
                                compensationColumnsEquiPred,
                                otherCompensationPred
                            )
                        )

                        // b. Generate final rewriting if possible.
                        // First, we add the compensation predicate (if any) on top of the view.
                        // Then, we trigger the unifying method. This method will either create a
                        // Project or an Aggregate operator on top of the view. It will also compute
                        // the output expressions for the query.
                        val builder: RelBuilder = call.builder().transform { c -> c.withPruneInputOfAggregate(false) }
                        var viewWithFilter: RelNode
                        if (!viewCompensationPred.isAlwaysTrue()) {
                            val newPred: RexNode = simplify.simplifyUnknownAsFalse(viewCompensationPred)
                            viewWithFilter = builder.push(view).filter(newPred).build()
                            // No need to do anything if it's a leaf node.
                            if (viewWithFilter.getInputs().isEmpty()) {
                                call.transformTo(viewWithFilter)
                                return
                            }
                            // We add (and push) the filter to the view plan before triggering the rewriting.
                            // This is useful in case some of the columns can be folded to same value after
                            // filter is added.
                            val pushedNodes: Pair<RelNode, RelNode> =
                                pushFilterToOriginalViewPlan(builder, topViewProject, viewNode, newPred)
                            topViewProject = pushedNodes.left as Project
                            viewNode = pushedNodes.right
                        } else {
                            viewWithFilter = builder.push(view).build()
                        }
                        val result: RelNode = rewriteView(
                            builder, rexBuilder, simplify, mq, matchModality,
                            false, viewWithFilter, topProject, node, topViewProject, viewNode,
                            queryToViewTableMapping, currQEC
                        )
                            ?: // Skip it
                            continue
                        call.transformTo(result)
                    } // end else
                }
            }
        }
    }

    protected abstract fun isValidPlan(
        @Nullable topProject: Project?, node: RelNode?,
        mq: RelMetadataQuery?
    ): Boolean

    /**
     * It checks whether the query can be rewritten using the view even though the
     * query uses additional tables.
     *
     *
     * Rules implementing the method should follow different approaches depending on the
     * operators they rewrite.
     * @return ViewPartialRewriting, or null if the rewrite can't be done
     */
    @Nullable
    protected abstract fun compensateViewPartial(
        relBuilder: RelBuilder?, rexBuilder: RexBuilder?, mq: RelMetadataQuery?, input: RelNode?,
        @Nullable topProject: Project?, node: RelNode?, queryTableRefs: Set<RelTableRef?>?,
        queryEC: EquivalenceClasses?,
        @Nullable topViewProject: Project?, viewNode: RelNode?, viewTableRefs: Set<RelTableRef?>?
    ): ViewPartialRewriting?

    /**
     * If the view will be used in a union rewriting, this method is responsible for
     * rewriting the query branch of the union using the given compensation predicate.
     *
     *
     * If a rewriting can be produced, we return that rewriting. If it cannot
     * be produced, we will return null.
     */
    @Nullable
    protected abstract fun rewriteQuery(
        relBuilder: RelBuilder?, rexBuilder: RexBuilder?, simplify: RexSimplify?, mq: RelMetadataQuery?,
        compensationColumnsEquiPred: RexNode?, otherCompensationPred: RexNode?,
        @Nullable topProject: Project?, node: RelNode?,
        viewToQueryTableMapping: BiMap<RelTableRef?, RelTableRef?>?,
        viewEC: EquivalenceClasses?, queryEC: EquivalenceClasses?
    ): RelNode?

    /**
     * If the view will be used in a union rewriting, this method is responsible for
     * generating the union and any other operator needed on top of it, e.g., a Project
     * operator.
     */
    @Nullable
    protected abstract fun createUnion(
        relBuilder: RelBuilder?, rexBuilder: RexBuilder?,
        @Nullable topProject: RelNode?, unionInputQuery: RelNode?, unionInputView: RelNode?
    ): RelNode?

    /**
     * Rewrites the query using the given view query.
     *
     *
     * The input node is a Scan on the view table and possibly a compensation Filter
     * on top. If a rewriting can be produced, we return that rewriting. If it cannot
     * be produced, we will return null.
     */
    @Nullable
    protected abstract fun rewriteView(
        relBuilder: RelBuilder?, rexBuilder: RexBuilder?,
        simplify: RexSimplify?, mq: RelMetadataQuery?, matchModality: MatchModality?,
        unionRewriting: Boolean, input: RelNode?,
        @Nullable topProject: Project?, node: RelNode?,
        @Nullable topViewProject: Project?, viewNode: RelNode?,
        queryToViewTableMapping: BiMap<RelTableRef?, RelTableRef?>?,
        queryEC: EquivalenceClasses?
    ): RelNode?

    /**
     * Once we create a compensation predicate, this method is responsible for pushing
     * the resulting filter through the view nodes. This might be useful for rewritings
     * containing Aggregate operators, as some of the grouping columns might be removed,
     * which results in additional matching possibilities.
     *
     *
     * The method will return a pair of nodes: the new top project on the left and
     * the new node on the right.
     */
    protected abstract fun pushFilterToOriginalViewPlan(
        builder: RelBuilder?,
        @Nullable topViewProject: RelNode?, viewNode: RelNode?, cond: RexNode?
    ): Pair<RelNode?, RelNode?>?
    //~ Methods ----------------------------------------------------------------
    /**
     * If the node is an Aggregate, it returns a list of references to the grouping columns.
     * Otherwise, it returns a list of references to all columns in the node.
     * The returned list is immutable.
     */
    protected fun extractReferences(rexBuilder: RexBuilder, node: RelNode): List<RexNode> {
        val exprs: ImmutableList.Builder<RexNode> = ImmutableList.builder()
        if (node is Aggregate) {
            val aggregate: Aggregate = node as Aggregate
            for (i in 0 until aggregate.getGroupCount()) {
                exprs.add(rexBuilder.makeInputRef(aggregate, i))
            }
        } else {
            for (i in 0 until node.getRowType().getFieldCount()) {
                exprs.add(rexBuilder.makeInputRef(node, i))
            }
        }
        return exprs.build()
    }

    /**
     * It will flatten a multimap containing table references to table references,
     * producing all possible combinations of mappings. Each of the mappings will
     * be bi-directional.
     */
    protected fun generateTableMappings(
        multiMapTables: Multimap<RelTableRef?, RelTableRef?>
    ): List<BiMap<RelTableRef, RelTableRef>> {
        if (multiMapTables.isEmpty()) {
            return ImmutableList.of()
        }
        var result: List<BiMap<RelTableRef, RelTableRef>> = ImmutableList.of(
            HashBiMap.create()
        )
        for (e in multiMapTables.asMap().entrySet()) {
            if (e.getValue().size() === 1) {
                // Only one reference, we can just add it to every map
                val target: RelTableRef = e.getValue().iterator().next()
                for (m in result) {
                    m.put(e.getKey(), target)
                }
                continue
            }
            // Multiple references: flatten
            val newResult: ImmutableList.Builder<BiMap<RelTableRef, RelTableRef>> = ImmutableList.builder()
            for (target in e.getValue()) {
                for (m in result) {
                    if (!m.containsValue(target)) {
                        val newM: BiMap<RelTableRef, RelTableRef> = HashBiMap.create(m)
                        newM.put(e.getKey(), target)
                        newResult.add(newM)
                    }
                }
            }
            result = newResult.build()
        }
        return result
    }

    /** Returns whether a RelNode is a valid tree. Currently we only support
     * TableScan - Project - Filter - Inner Join.  */
    protected fun isValidRelNodePlan(node: RelNode?, mq: RelMetadataQuery): Boolean {
        val m: Multimap<Class<out RelNode?>, RelNode> = mq.getNodeTypes(node) ?: return false
        for (e in m.asMap().entrySet()) {
            val c: Class<out RelNode?> = e.getKey()
            if (!TableScan::class.java.isAssignableFrom(c)
                && !Project::class.java.isAssignableFrom(c)
                && !Filter::class.java.isAssignableFrom(c)
                && !Join::class.java.isAssignableFrom(c)
            ) {
                // Skip it
                return false
            }
            if (Join::class.java.isAssignableFrom(c)) {
                for (n in e.getValue()) {
                    val join: Join = n as Join
                    if (join.getJoinType() !== JoinRelType.INNER) {
                        // Skip it
                        return false
                    }
                }
            }
        }
        return true
    }

    /**
     * Classifies each of the predicates in the list into one of these two
     * categories:
     *
     *
     *  *  1-l) column equality predicates, or
     *  *  2-r) residual predicates, all the rest
     *
     *
     *
     * For each category, it creates the conjunction of the predicates. The
     * result is an pair of RexNode objects corresponding to each category.
     */
    protected fun splitPredicates(
        rexBuilder: RexBuilder?, pred: RexNode?
    ): Pair<RexNode, RexNode> {
        val equiColumnsPreds: List<RexNode> = ArrayList()
        val residualPreds: List<RexNode> = ArrayList()
        for (e in RelOptUtil.conjunctions(pred)) {
            when (e.getKind()) {
                EQUALS -> {
                    val eqCall: RexCall = e as RexCall
                    if (RexUtil.isReferenceOrAccess(eqCall.getOperands().get(0), false)
                        && RexUtil.isReferenceOrAccess(eqCall.getOperands().get(1), false)
                    ) {
                        equiColumnsPreds.add(e)
                    } else {
                        residualPreds.add(e)
                    }
                }
                else -> residualPreds.add(e)
            }
        }
        return Pair.of(
            RexUtil.composeConjunction(rexBuilder, equiColumnsPreds),
            RexUtil.composeConjunction(rexBuilder, residualPreds)
        )
    }

    /**
     * It checks whether the target can be rewritten using the source even though the
     * source uses additional tables. In order to do that, we need to double-check
     * that every join that exists in the source and is not in the target is a
     * cardinality-preserving join, i.e., it only appends columns to the row
     * without changing its multiplicity. Thus, the join needs to be:
     *
     *  *  Equi-join
     *  *  Between all columns in the keys
     *  *  Foreign-key columns do not allow NULL values
     *  *  Foreign-key
     *  *  Unique-key
     *
     *
     *
     * If it can be rewritten, it returns true. Further, it inserts the missing equi-join
     * predicates in the input `compensationEquiColumns` multimap if it is provided.
     * If it cannot be rewritten, it returns false.
     */
    protected fun compensatePartial(
        sourceTableRefs: Set<RelTableRef?>,
        sourceEC: EquivalenceClasses,
        targetTableRefs: Set<RelTableRef?>,
        @Nullable compensationEquiColumns: Multimap<RexTableInputRef?, RexTableInputRef?>?
    ): Boolean {
        // Create UK-FK graph with view tables
        val graph: DirectedGraph<RelTableRef, Edge> =
            DefaultDirectedGraph.create { source: RelTableRef?, target: RelTableRef? -> Edge(source, target) }
        val tableVNameToTableRefs: Multimap<List<String>, RelTableRef> = ArrayListMultimap.create()
        val extraTableRefs: Set<RelTableRef> = HashSet()
        for (tRef in sourceTableRefs) {
            // Add tables in view as vertices
            graph.addVertex(tRef)
            tableVNameToTableRefs.put(tRef.getQualifiedName(), tRef)
            if (!targetTableRefs.contains(tRef)) {
                // Add to extra tables if table is not part of the query
                extraTableRefs.add(tRef)
            }
        }
        for (tRef in graph.vertexSet()) {
            // Add edges between tables
            var constraints: List<RelReferentialConstraint?> = tRef.getTable().getReferentialConstraints()
            if (constraints == null) {
                constraints = ImmutableList.of()
            }
            for (constraint in constraints) {
                val parentTableRefs: Collection<RelTableRef> =
                    tableVNameToTableRefs.get(constraint.getTargetQualifiedName())
                for (parentTRef in parentTableRefs) {
                    var canBeRewritten = true
                    val equiColumns: Multimap<RexTableInputRef, RexTableInputRef> = ArrayListMultimap.create()
                    val foreignFields: List<RelDataTypeField> = tRef.getTable().getRowType().getFieldList()
                    val uniqueFields: List<RelDataTypeField> = parentTRef.getTable().getRowType().getFieldList()
                    for (pair in constraint.getColumnPairs()) {
                        val foreignKeyColumnType: RelDataType = foreignFields[pair.source].getType()
                        val foreignKeyColumnRef: RexTableInputRef =
                            RexTableInputRef.of(tRef, pair.source, foreignKeyColumnType)
                        val uniqueKeyColumnType: RelDataType = uniqueFields[pair.target].getType()
                        val uniqueKeyColumnRef: RexTableInputRef =
                            RexTableInputRef.of(parentTRef, pair.target, uniqueKeyColumnType)
                        if (!foreignKeyColumnType.isNullable()
                            && sourceEC.equivalenceClassesMap.containsKey(uniqueKeyColumnRef)
                            && castNonNull(sourceEC.equivalenceClassesMap[uniqueKeyColumnRef])
                                .contains(foreignKeyColumnRef)
                        ) {
                            equiColumns.put(foreignKeyColumnRef, uniqueKeyColumnRef)
                        } else {
                            canBeRewritten = false
                            break
                        }
                    }
                    if (canBeRewritten) {
                        // Add edge FK -> UK
                        var edge: Edge = graph.getEdge(tRef, parentTRef)
                        if (edge == null) {
                            edge = graph.addEdge(tRef, parentTRef)
                        }
                        castNonNull(edge).equiColumns.putAll(equiColumns)
                    }
                }
            }
        }

        // Try to eliminate tables from graph: if we can do it, it means extra tables in
        // view are cardinality-preserving joins
        var done = false
        do {
            val nodesToRemove: List<RelTableRef> = ArrayList()
            for (tRef in graph.vertexSet()) {
                if (graph.getInwardEdges(tRef).size() === 1
                    && graph.getOutwardEdges(tRef).isEmpty()
                ) {
                    // UK-FK join
                    nodesToRemove.add(tRef)
                    if (compensationEquiColumns != null && extraTableRefs.contains(tRef)) {
                        // We need to add to compensation columns as the table is not present in the query
                        compensationEquiColumns.putAll(graph.getInwardEdges(tRef).get(0).equiColumns)
                    }
                }
            }
            if (!nodesToRemove.isEmpty()) {
                graph.removeAllVertices(nodesToRemove)
            } else {
                done = true
            }
        } while (!done)

        // After removing them, we check whether all the remaining tables in the graph
        // are tables present in the query: if they are, we can try to rewrite
        return if (!Collections.disjoint(graph.vertexSet(), extraTableRefs)) {
            false
        } else true
    }

    /**
     * We check whether the predicates in the source are contained in the predicates
     * in the target. The method treats separately the equi-column predicates, the
     * range predicates, and the rest of predicates.
     *
     *
     * If the containment is confirmed, we produce compensation predicates that
     * need to be added to the target to produce the results in the source. Thus,
     * if source and target expressions are equivalent, those predicates will be the
     * true constant.
     *
     *
     * In turn, if containment cannot be confirmed, the method returns null.
     */
    @Nullable
    protected fun computeCompensationPredicates(
        rexBuilder: RexBuilder,
        simplify: RexSimplify?,
        sourceEC: EquivalenceClasses,
        sourcePreds: Pair<RexNode?, RexNode?>,
        targetEC: EquivalenceClasses,
        targetPreds: Pair<RexNode?, RexNode?>,
        sourceToTargetTableMapping: BiMap<RelTableRef?, RelTableRef?>
    ): Pair<RexNode, RexNode>? {
        val compensationColumnsEquiPred: RexNode?
        val compensationPred: RexNode

        // 1. Establish relationship between source and target equivalence classes.
        // If every target equivalence class is not a subset of a source
        // equivalence class, we bail out.
        compensationColumnsEquiPred = generateEquivalenceClasses(
            rexBuilder, sourceEC, targetEC
        )
        if (compensationColumnsEquiPred == null) {
            // Cannot rewrite
            return null
        }

        // 2. We check that that residual predicates of the source are satisfied within the target.
        // Compute compensating predicates.
        val queryPred: RexNode = RexUtil.swapColumnReferences(
            rexBuilder, sourcePreds.right, sourceEC.equivalenceClassesMap
        )
        val viewPred: RexNode = RexUtil.swapTableColumnReferences(
            rexBuilder, targetPreds.right, sourceToTargetTableMapping.inverse(),
            sourceEC.equivalenceClassesMap
        )
        compensationPred = SubstitutionVisitor.splitFilter(
            simplify, queryPred, viewPred
        )
        return if (compensationPred == null) {
            // Cannot rewrite
            null
        } else Pair.of(compensationColumnsEquiPred, compensationPred)
    }

    /**
     * Given the equi-column predicates of the source and the target and the
     * computed equivalence classes, it extracts possible mappings between
     * the equivalence classes.
     *
     *
     * If there is no mapping, it returns null. If there is a exact match,
     * it will return a compensation predicate that evaluates to true.
     * Finally, if a compensation predicate needs to be enforced on top of
     * the target to make the equivalences classes match, it returns that
     * compensation predicate.
     */
    @Nullable
    protected fun generateEquivalenceClasses(
        rexBuilder: RexBuilder,
        sourceEC: EquivalenceClasses, targetEC: EquivalenceClasses
    ): RexNode? {
        if (sourceEC.equivalenceClasses.isEmpty() && targetEC.equivalenceClasses.isEmpty()) {
            // No column equality predicates in query and view
            // Empty mapping and compensation predicate
            return rexBuilder.makeLiteral(true)
        }
        if (sourceEC.equivalenceClasses.isEmpty() && !targetEC.equivalenceClasses.isEmpty()) {
            // No column equality predicates in source, but column equality predicates in target
            return null
        }
        val sourceEquivalenceClasses: List<Set<RexTableInputRef>> = sourceEC.equivalenceClasses
        val targetEquivalenceClasses: List<Set<RexTableInputRef>> = targetEC.equivalenceClasses
        val mapping: Multimap<Integer, Integer> = extractPossibleMapping(
            sourceEquivalenceClasses, targetEquivalenceClasses
        )
            ?: // Did not find mapping between the equivalence classes,
            // bail out
            return null

        // Create the compensation predicate
        var compensationPredicate: RexNode = rexBuilder.makeLiteral(true)
        for (i in 0 until sourceEquivalenceClasses.size()) {
            if (!mapping.containsKey(i)) {
                // Add all predicates
                val it: Iterator<RexTableInputRef> = sourceEquivalenceClasses[i].iterator()
                val e0: RexTableInputRef = it.next()
                while (it.hasNext()) {
                    val equals: RexNode = rexBuilder.makeCall(
                        SqlStdOperatorTable.EQUALS,
                        e0, it.next()
                    )
                    compensationPredicate = rexBuilder.makeCall(
                        SqlStdOperatorTable.AND,
                        compensationPredicate, equals
                    )
                }
            } else {
                // Add only predicates that are not there
                for (j in mapping.get(i)) {
                    val difference: Set<RexTableInputRef> = HashSet(
                        sourceEquivalenceClasses[i]
                    )
                    difference.removeAll(targetEquivalenceClasses[j])
                    for (e in difference) {
                        val equals: RexNode = rexBuilder.makeCall(
                            SqlStdOperatorTable.EQUALS,
                            e, targetEquivalenceClasses[j].iterator().next()
                        )
                        compensationPredicate = rexBuilder.makeCall(
                            SqlStdOperatorTable.AND,
                            compensationPredicate, equals
                        )
                    }
                }
            }
        }
        return compensationPredicate
    }

    /**
     * Given the source and target equivalence classes, it extracts the possible mappings
     * from each source equivalence class to each target equivalence class.
     *
     *
     * If any of the source equivalence classes cannot be mapped to a target equivalence
     * class, it returns null.
     */
    @Nullable
    protected fun extractPossibleMapping(
        sourceEquivalenceClasses: List<Set<RexTableInputRef?>>,
        targetEquivalenceClasses: List<Set<RexTableInputRef?>?>
    ): Multimap<Integer, Integer>? {
        val mapping: Multimap<Integer, Integer> = ArrayListMultimap.create()
        for (i in 0 until targetEquivalenceClasses.size()) {
            var foundQueryEquivalenceClass = false
            val viewEquivalenceClass: Set<RexTableInputRef?>? = targetEquivalenceClasses[i]
            for (j in 0 until sourceEquivalenceClasses.size()) {
                val queryEquivalenceClass: Set<RexTableInputRef?> = sourceEquivalenceClasses[j]
                if (queryEquivalenceClass.containsAll(viewEquivalenceClass!!)) {
                    mapping.put(j, i)
                    foundQueryEquivalenceClass = true
                    break
                }
            } // end for
            if (!foundQueryEquivalenceClass) {
                // Target equivalence class not found in source equivalence class
                return null
            }
        } // end for
        return mapping
    }

    /**
     * First, the method takes the node expressions `nodeExprs` and swaps the table
     * and column references using the table mapping and the equivalence classes.
     * If `swapTableColumn` is true, it swaps the table reference and then the column reference,
     * otherwise it swaps the column reference and then the table reference.
     *
     *
     * Then, the method will rewrite the input expression `exprToRewrite`, replacing the
     * [RexTableInputRef] by references to the positions in `nodeExprs`.
     *
     *
     * The method will return the rewritten expression. If any of the expressions in the input
     * expression cannot be mapped, it will return null.
     */
    @Nullable
    protected fun rewriteExpression(
        rexBuilder: RexBuilder,
        mq: RelMetadataQuery,
        targetNode: RelNode?,
        node: RelNode,
        nodeExprs: List<RexNode>,
        tableMapping: BiMap<RelTableRef?, RelTableRef?>?,
        ec: EquivalenceClasses,
        swapTableColumn: Boolean,
        exprToRewrite: RexNode?
    ): RexNode? {
        val rewrittenExprs: List<Any> = rewriteExpressions(
            rexBuilder, mq, targetNode, node, nodeExprs,
            tableMapping, ec, swapTableColumn, ImmutableList.of(exprToRewrite)
        ) ?: return null
        assert(rewrittenExprs.size() === 1)
        return rewrittenExprs[0]
    }

    /**
     * First, the method takes the node expressions `nodeExprs` and swaps the table
     * and column references using the table mapping and the equivalence classes.
     * If `swapTableColumn` is true, it swaps the table reference and then the column reference,
     * otherwise it swaps the column reference and then the table reference.
     *
     *
     * Then, the method will rewrite the input expressions `exprsToRewrite`, replacing the
     * [RexTableInputRef] by references to the positions in `nodeExprs`.
     *
     *
     * The method will return the rewritten expressions. If any of the subexpressions in the input
     * expressions cannot be mapped, it will return null.
     */
    @Nullable
    protected fun rewriteExpressions(
        rexBuilder: RexBuilder,
        mq: RelMetadataQuery,
        targetNode: RelNode?,
        node: RelNode,
        nodeExprs: List<RexNode>,
        tableMapping: BiMap<RelTableRef?, RelTableRef?>?,
        ec: EquivalenceClasses,
        swapTableColumn: Boolean,
        exprsToRewrite: List<RexNode?>
    ): List<RexNode>? {
        val nodeLineage: NodeLineage
        nodeLineage = if (swapTableColumn) {
            generateSwapTableColumnReferencesLineage(
                rexBuilder, mq, node,
                tableMapping, ec, nodeExprs
            )
        } else {
            generateSwapColumnTableReferencesLineage(
                rexBuilder, mq, node,
                tableMapping, ec, nodeExprs
            )
        }
        val rewrittenExprs: List<RexNode> = ArrayList(exprsToRewrite.size())
        for (exprToRewrite in exprsToRewrite) {
            val rewrittenExpr: RexNode = replaceWithOriginalReferences(
                rexBuilder, targetNode, nodeLineage, exprToRewrite
            )
            if (RexUtil.containsTableInputRef(rewrittenExpr) != null) {
                // Some expressions were not present in view output
                return null
            }
            rewrittenExprs.add(rewrittenExpr)
        }
        return rewrittenExprs
    }

    /**
     * It swaps the table references and then the column references of the input
     * expressions using the table mapping and the equivalence classes.
     */
    protected fun generateSwapTableColumnReferencesLineage(
        rexBuilder: RexBuilder?,
        mq: RelMetadataQuery,
        node: RelNode,
        tableMapping: BiMap<RelTableRef?, RelTableRef?>?,
        ec: EquivalenceClasses,
        nodeExprs: List<RexNode>
    ): NodeLineage {
        val exprsLineage: Map<RexNode, Integer> = HashMap()
        val exprsLineageLosslessCasts: Map<RexNode, Integer> = HashMap()
        for (i in 0 until nodeExprs.size()) {
            val expr: RexNode = nodeExprs[i]
            val lineages: Set<RexNode> = mq.getExpressionLineage(node, expr)
                ?: // Next expression
                continue
            if (lineages.size() !== 1) {
                throw IllegalStateException(
                    "We only support project - filter - join, "
                            + "thus expression lineage should map to a single expression, got: '"
                            + lineages + "' for expr '" + expr + "' in node '" + node + "'"
                )
            }
            // Rewrite expr. First we swap the table references following the table
            // mapping, then we take first element from the corresponding equivalence class
            val e: RexNode = RexUtil.swapTableColumnReferences(
                rexBuilder,
                lineages.iterator().next(), tableMapping, ec.equivalenceClassesMap
            )
            exprsLineage.put(e, i)
            if (RexUtil.isLosslessCast(e)) {
                exprsLineageLosslessCasts.put((e as RexCall).getOperands().get(0), i)
            }
        }
        return NodeLineage(exprsLineage, exprsLineageLosslessCasts)
    }

    /**
     * It swaps the column references and then the table references of the input
     * expressions using the equivalence classes and the table mapping.
     */
    protected fun generateSwapColumnTableReferencesLineage(
        rexBuilder: RexBuilder?,
        mq: RelMetadataQuery,
        node: RelNode,
        tableMapping: BiMap<RelTableRef?, RelTableRef?>?,
        ec: EquivalenceClasses,
        nodeExprs: List<RexNode>
    ): NodeLineage {
        val exprsLineage: Map<RexNode, Integer> = HashMap()
        val exprsLineageLosslessCasts: Map<RexNode, Integer> = HashMap()
        for (i in 0 until nodeExprs.size()) {
            val expr: RexNode = nodeExprs[i]
            val lineages: Set<RexNode> = mq.getExpressionLineage(node, expr)
                ?: // Next expression
                continue
            if (lineages.size() !== 1) {
                throw IllegalStateException(
                    "We only support project - filter - join, "
                            + "thus expression lineage should map to a single expression, got: '"
                            + lineages + "' for expr '" + expr + "' in node '" + node + "'"
                )
            }
            // Rewrite expr. First we take first element from the corresponding equivalence class,
            // then we swap the table references following the table mapping
            val e: RexNode = RexUtil.swapColumnTableReferences(
                rexBuilder,
                lineages.iterator().next(), ec.equivalenceClassesMap, tableMapping
            )
            exprsLineage.put(e, i)
            if (RexUtil.isLosslessCast(e)) {
                exprsLineageLosslessCasts.put((e as RexCall).getOperands().get(0), i)
            }
        }
        return NodeLineage(exprsLineage, exprsLineageLosslessCasts)
    }

    /**
     * Given the input expression, it will replace (sub)expressions when possible
     * using the content of the mapping. In particular, the mapping contains the
     * digest of the expression and the index that the replacement input ref should
     * point to.
     */
    protected fun replaceWithOriginalReferences(
        rexBuilder: RexBuilder,
        node: RelNode?, nodeLineage: NodeLineage, exprToRewrite: RexNode?
    ): RexNode {
        // Currently we allow the following:
        // 1) compensation pred can be directly map to expression
        // 2) all references in compensation pred can be map to expressions
        // We support bypassing lossless casts.
        val visitor: RexShuttle = object : RexShuttle() {
            @Override
            fun visitCall(call: RexCall): RexNode {
                val rw: RexNode? = replace(call)
                return if (rw != null) rw else super.visitCall(call)
            }

            @Override
            fun visitTableInputRef(inputRef: RexTableInputRef): RexNode {
                val rw: RexNode? = replace(inputRef)
                return if (rw != null) rw else super.visitTableInputRef(inputRef)
            }

            @Nullable
            private fun replace(e: RexNode): RexNode? {
                var pos: Integer? = nodeLineage.exprsLineage[e]
                if (pos != null) {
                    // Found it
                    return rexBuilder.makeInputRef(node, pos)
                }
                pos = nodeLineage.exprsLineageLosslessCasts[e]
                return if (pos != null) {
                    // Found it
                    rexBuilder.makeCast(
                        e.getType(), rexBuilder.makeInputRef(node, pos)
                    )
                } else null
            }
        }
        return visitor.apply(exprToRewrite)
    }

    /**
     * Replaces all the input references by the position in the
     * input column set. If a reference index cannot be found in
     * the input set, then we return null.
     */
    @Nullable
    protected fun shuttleReferences(
        rexBuilder: RexBuilder,
        node: RexNode?, mapping: Mapping
    ): RexNode? {
        return try {
            val visitor: RexShuttle = object : RexShuttle() {
                @Override
                fun visitInputRef(inputRef: RexInputRef): RexNode {
                    val pos: Int = mapping.getTargetOpt(inputRef.getIndex())
                    if (pos != -1) {
                        // Found it
                        return rexBuilder.makeInputRef(inputRef.getType(), pos)
                    }
                    throw Util.FoundOne.NULL
                }
            }
            visitor.apply(node)
        } catch (ex: Util.FoundOne) {
            Util.swallow(ex, null)
            null
        }
    }

    /**
     * Replaces all the possible sub-expressions by input references
     * to the input node.
     */
    @Nullable
    protected fun shuttleReferences(
        rexBuilder: RexBuilder,
        expr: RexNode?, exprsLineage: Multimap<RexNode?, Integer?>
    ): RexNode? {
        return shuttleReferences(
            rexBuilder, expr,
            exprsLineage, null, null
        )
    }

    /**
     * Replaces all the possible sub-expressions by input references
     * to the input node. If available, it uses the rewriting mapping
     * to change the position to reference. Takes the reference type
     * from the input node.
     */
    @Nullable
    protected fun shuttleReferences(
        rexBuilder: RexBuilder,
        expr: RexNode?, exprsLineage: Multimap<RexNode?, Integer?>,
        @Nullable node: RelNode?, @Nullable rewritingMapping: Multimap<Integer?, Integer?>?
    ): RexNode? {
        return try {
            val visitor: RexShuttle = object : RexShuttle() {
                @Override
                fun visitTableInputRef(ref: RexTableInputRef): RexNode {
                    val c: Collection<Integer> = exprsLineage.get(ref)
                    if (c.isEmpty()) {
                        // Cannot map expression
                        throw Util.FoundOne.NULL
                    }
                    var pos: Int = c.iterator().next()
                    if (rewritingMapping != null) {
                        if (!rewritingMapping.containsKey(pos)) {
                            // Cannot map expression
                            throw Util.FoundOne.NULL
                        }
                        pos = rewritingMapping.get(pos).iterator().next()
                    }
                    return if (node != null) {
                        rexBuilder.makeInputRef(node, pos)
                    } else rexBuilder.makeInputRef(ref.getType(), pos)
                }

                @Override
                fun visitInputRef(inputRef: RexInputRef): RexNode {
                    val c: Collection<Integer> = exprsLineage.get(inputRef)
                    if (c.isEmpty()) {
                        // Cannot map expression
                        throw Util.FoundOne.NULL
                    }
                    var pos: Int = c.iterator().next()
                    if (rewritingMapping != null) {
                        if (!rewritingMapping.containsKey(pos)) {
                            // Cannot map expression
                            throw Util.FoundOne.NULL
                        }
                        pos = rewritingMapping.get(pos).iterator().next()
                    }
                    return if (node != null) {
                        rexBuilder.makeInputRef(node, pos)
                    } else rexBuilder.makeInputRef(inputRef.getType(), pos)
                }

                @Override
                fun visitCall(call: RexCall): RexNode {
                    val c: Collection<Integer> = exprsLineage.get(call)
                    if (c.isEmpty()) {
                        // Cannot map expression
                        return super.visitCall(call)
                    }
                    var pos: Int = c.iterator().next()
                    if (rewritingMapping != null) {
                        if (!rewritingMapping.containsKey(pos)) {
                            // Cannot map expression
                            return super.visitCall(call)
                        }
                        pos = rewritingMapping.get(pos).iterator().next()
                    }
                    return if (node != null) {
                        rexBuilder.makeInputRef(node, pos)
                    } else rexBuilder.makeInputRef(call.getType(), pos)
                }
            }
            visitor.apply(expr)
        } catch (ex: Util.FoundOne) {
            Util.swallow(ex, null)
            null
        }
    }

    /**
     * Class representing an equivalence class, i.e., a set of equivalent columns
     */
    protected class EquivalenceClasses {
        private val nodeToEquivalenceClass: Map<RexTableInputRef?, Set<RexTableInputRef>>

        @Nullable
        private var cacheEquivalenceClassesMap: Map<RexTableInputRef, Set<RexTableInputRef>>?

        @Nullable
        private var cacheEquivalenceClasses: List<Set<RexTableInputRef>>?

        init {
            nodeToEquivalenceClass = HashMap()
            cacheEquivalenceClassesMap = ImmutableMap.of()
            cacheEquivalenceClasses = ImmutableList.of()
        }

        fun addEquivalenceClass(p1: RexTableInputRef?, p2: RexTableInputRef?) {
            // Clear cache
            cacheEquivalenceClassesMap = null
            cacheEquivalenceClasses = null
            var c1: Set<RexTableInputRef>? = nodeToEquivalenceClass[p1]
            var c2: Set<RexTableInputRef>? = nodeToEquivalenceClass[p2]
            if (c1 != null && c2 != null) {
                // Both present, we need to merge
                if (c1.size() < c2.size()) {
                    // We swap them to merge
                    val c2Temp: Set<RexTableInputRef> = c2
                    c2 = c1
                    c1 = c2Temp
                }
                for (newRef in c2) {
                    c1.add(newRef)
                    nodeToEquivalenceClass.put(newRef, c1)
                }
            } else if (c1 != null) {
                // p1 present, we need to merge into it
                c1.add(p2)
                nodeToEquivalenceClass.put(p2, c1)
            } else if (c2 != null) {
                // p2 present, we need to merge into it
                c2.add(p1)
                nodeToEquivalenceClass.put(p1, c2)
            } else {
                // None are present, add to same equivalence class
                val equivalenceClass: Set<RexTableInputRef> = LinkedHashSet()
                equivalenceClass.add(p1)
                equivalenceClass.add(p2)
                nodeToEquivalenceClass.put(p1, equivalenceClass)
                nodeToEquivalenceClass.put(p2, equivalenceClass)
            }
        }

        val equivalenceClassesMap: Map<Any, Set<Any>>
            get() {
                if (cacheEquivalenceClassesMap == null) {
                    cacheEquivalenceClassesMap = ImmutableMap.copyOf(nodeToEquivalenceClass)
                }
                return cacheEquivalenceClassesMap!!
            }
        val equivalenceClasses: List<Set<Any>>
            get() {
                if (cacheEquivalenceClasses == null) {
                    val visited: Set<RexTableInputRef> = HashSet()
                    val builder: ImmutableList.Builder<Set<RexTableInputRef>> = ImmutableList.builder()
                    for (set in nodeToEquivalenceClass.values()) {
                        if (Collections.disjoint(visited, set)) {
                            builder.add(set)
                            visited.addAll(set)
                        }
                    }
                    cacheEquivalenceClasses = builder.build()
                }
                return cacheEquivalenceClasses!!
            }

        companion object {
            fun copy(ec: EquivalenceClasses): EquivalenceClasses {
                val newEc = EquivalenceClasses()
                for (e in ec.nodeToEquivalenceClass.entrySet()) {
                    newEc.nodeToEquivalenceClass.put(
                        e.getKey(), Sets.newLinkedHashSet(e.getValue())
                    )
                }
                newEc.cacheEquivalenceClassesMap = null
                newEc.cacheEquivalenceClasses = null
                return newEc
            }
        }
    }

    /** Expression lineage details.  */
    protected class NodeLineage(
        exprsLineage: Map<RexNode, Integer>,
        exprsLineageLosslessCasts: Map<RexNode, Integer>
    ) {
        val exprsLineage: Map<RexNode, Integer>
        val exprsLineageLosslessCasts: Map<RexNode, Integer>

        init {
            this.exprsLineage = ImmutableMap.copyOf(exprsLineage)
            this.exprsLineageLosslessCasts = ImmutableMap.copyOf(exprsLineageLosslessCasts)
        }
    }

    /** Edge for graph.  */
    protected class Edge internal constructor(source: RelTableRef?, target: RelTableRef?) :
        DefaultEdge(source, target) {
        val equiColumns: Multimap<RexTableInputRef, RexTableInputRef> = ArrayListMultimap.create()

        @Override
        override fun toString(): String {
            return "{" + source.toString() + " -> " + target.toString() + "}"
        }
    }

    /** View partitioning result.  */
    protected class ViewPartialRewriting private constructor(
        newView: RelNode, @Nullable newTopViewProject: Project?,
        newViewNode: RelNode
    ) {
        val newView: RelNode

        @Nullable
        val newTopViewProject: Project?
        val newViewNode: RelNode

        init {
            this.newView = newView
            this.newTopViewProject = newTopViewProject
            this.newViewNode = newViewNode
        }

        companion object {
            fun of(
                newView: RelNode, @Nullable newTopViewProject: Project?, newViewNode: RelNode
            ): ViewPartialRewriting {
                return ViewPartialRewriting(newView, newTopViewProject, newViewNode)
            }
        }
    }

    /** Complete, view partial, or query partial.  */
    protected enum class MatchModality {
        COMPLETE, VIEW_PARTIAL, QUERY_PARTIAL
    }

    /** Rule configuration.  */
    interface Config : RelRule.Config {
        /** Whether to generate rewritings containing union if the query results
         * are contained within the view results.  */
        fun generateUnionRewriting(): Boolean

        /** Sets [.generateUnionRewriting].  */
        fun withGenerateUnionRewriting(b: Boolean): Config?

        /** If we generate union rewriting, we might want to pull up projections
         * from the query itself to maximize rewriting opportunities.  */
        @Nullable
        fun unionRewritingPullProgram(): HepProgram?

        /** Sets [.unionRewritingPullProgram].  */
        fun withUnionRewritingPullProgram(@Nullable program: HepProgram?): Config

        /** Whether we should create the rewriting in the minimal subtree of plan
         * operators.  */
        fun fastBailOut(): Boolean

        /** Sets [.fastBailOut].  */
        fun withFastBailOut(b: Boolean): Config
    }
}
