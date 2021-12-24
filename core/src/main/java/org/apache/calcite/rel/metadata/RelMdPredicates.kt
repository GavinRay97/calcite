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
package org.apache.calcite.rel.metadata

import org.apache.calcite.linq4j.Ord

/**
 * Utility to infer Predicates that are applicable above a RelNode.
 *
 *
 * This is currently used by
 * [org.apache.calcite.rel.rules.JoinPushTransitivePredicatesRule] to
 * infer *Predicates* that can be inferred from one side of a Join
 * to the other.
 *
 *
 * The PullUp Strategy is sound but not complete. Here are some of the
 * limitations:
 *
 *
 *  1.  For Aggregations we only PullUp predicates that only contain
 * Grouping Keys. This can be extended to infer predicates on Aggregation
 * expressions from  expressions on the aggregated columns. For e.g.
 * <pre>
 * select a, max(b) from R1 where b &gt; 7
 *  max(b) &gt; 7 or max(b) is null
</pre> *
 *
 *  1.  For Projections we only look at columns that are projected without
 * any function applied. So:
 * <pre>
 * select a from R1 where a &gt; 7
 *  "a &gt; 7" is pulled up from the Projection.
 * select a + 1 from R1 where a + 1 &gt; 7
 *  "a + 1 gt; 7" is not pulled up
</pre> *
 *
 *  1.  There are several restrictions on Joins:
 *
 *  *  We only pullUp inferred predicates for now. Pulling up existing
 * predicates causes an explosion of duplicates. The existing predicates
 * are pushed back down as new predicates. Once we have rules to eliminate
 * duplicate Filter conditions, we should pullUp all predicates.
 *
 *  *  For Left Outer: we infer new predicates from the left and set them
 * as applicable on the Right side. No predicates are pulledUp.
 *
 *  *  Right Outer Joins are handled in an analogous manner.
 *
 *  *  For Full Outer Joins no predicates are pulledUp or inferred.
 *
 *
 */
class RelMdPredicates : MetadataHandler<BuiltInMetadata.Predicates?> {
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        @Override get() = BuiltInMetadata.Predicates.DEF

    /** Catch-all implementation for
     * [BuiltInMetadata.Predicates.getPredicates],
     * invoked using reflection.
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery.getPulledUpPredicates
     */
    fun getPredicates(rel: RelNode?, mq: RelMetadataQuery?): RelOptPredicateList {
        return RelOptPredicateList.EMPTY
    }

    /**
     * Infers predicates for a table scan.
     */
    fun getPredicates(
        table: TableScan?,
        mq: RelMetadataQuery?
    ): RelOptPredicateList {
        return RelOptPredicateList.EMPTY
    }

    /**
     * Infers predicates for a project.
     *
     *
     *  1. create a mapping from input to projection. Map only positions that
     * directly reference an input column.
     *  1. Expressions that only contain above columns are retained in the
     * Project's pullExpressions list.
     *  1. For e.g. expression 'a + e = 9' below will not be pulled up because 'e'
     * is not in the projection list.
     *
     * <blockquote><pre>
     * inputPullUpExprs:      {a &gt; 7, b + c &lt; 10, a + e = 9}
     * projectionExprs:       {a, b, c, e / 2}
     * projectionPullupExprs: {a &gt; 7, b + c &lt; 10}
    </pre></blockquote> *
     *
     *
     */
    fun getPredicates(
        project: Project,
        mq: RelMetadataQuery
    ): RelOptPredicateList {
        val input: RelNode = project.getInput()
        val rexBuilder: RexBuilder = project.getCluster().getRexBuilder()
        val inputInfo: RelOptPredicateList = mq.getPulledUpPredicates(input)
        val projectPullUpPredicates: List<RexNode> = ArrayList()
        val columnsMappedBuilder: ImmutableBitSet.Builder = ImmutableBitSet.builder()
        val m: Mapping = Mappings.create(
            MappingType.PARTIAL_FUNCTION,
            input.getRowType().getFieldCount(),
            project.getRowType().getFieldCount()
        )
        for (expr in Ord.zip(project.getProjects())) {
            if (expr.e is RexInputRef) {
                val sIdx: Int = (expr.e as RexInputRef).getIndex()
                m.set(sIdx, expr.i)
                columnsMappedBuilder.set(sIdx)
                // Project can also generate constants. We need to include them.
            } else if (RexLiteral.isNullLiteral(expr.e)) {
                projectPullUpPredicates.add(
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.IS_NULL,
                        rexBuilder.makeInputRef(project, expr.i)
                    )
                )
            } else if (RexUtil.isConstant(expr.e)) {
                val args: List<RexNode> = ImmutableList.of(rexBuilder.makeInputRef(project, expr.i), expr.e)
                val op: SqlOperator = if (args[0].getType().isNullable()
                    || args[1].getType().isNullable()
                ) SqlStdOperatorTable.IS_NOT_DISTINCT_FROM else SqlStdOperatorTable.EQUALS
                projectPullUpPredicates.add(rexBuilder.makeCall(op, args))
            }
        }

        // Go over childPullUpPredicates. If a predicate only contains columns in
        // 'columnsMapped' construct a new predicate based on mapping.
        val columnsMapped: ImmutableBitSet = columnsMappedBuilder.build()
        for (r in inputInfo.pulledUpPredicates) {
            var r2: RexNode = projectPredicate(rexBuilder, input, r, columnsMapped)
            if (!r2.isAlwaysTrue()) {
                r2 = r2.accept(RexPermuteInputsShuttle(m, input))
                projectPullUpPredicates.add(r2)
            }
        }
        return RelOptPredicateList.of(rexBuilder, projectPullUpPredicates)
    }

    /**
     * Add the Filter condition to the pulledPredicates list from the input.
     */
    fun getPredicates(filter: Filter, mq: RelMetadataQuery): RelOptPredicateList {
        val input: RelNode = filter.getInput()
        val rexBuilder: RexBuilder = filter.getCluster().getRexBuilder()
        val inputInfo: RelOptPredicateList = mq.getPulledUpPredicates(input)
        return Util.first(inputInfo, RelOptPredicateList.EMPTY)
            .union(
                rexBuilder,
                RelOptPredicateList.of(
                    rexBuilder,
                    RexUtil.retainDeterministic(
                        RelOptUtil.conjunctions(filter.getCondition())
                    )
                )
            )
    }

    /**
     * Infers predicates for a [org.apache.calcite.rel.core.Join] (including
     * `SemiJoin`).
     */
    fun getPredicates(join: Join, mq: RelMetadataQuery): RelOptPredicateList {
        val cluster: RelOptCluster = join.getCluster()
        val rexBuilder: RexBuilder = cluster.getRexBuilder()
        val executor: RexExecutor = Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR)
        val left: RelNode = join.getInput(0)
        val right: RelNode = join.getInput(1)
        val leftInfo: RelOptPredicateList = mq.getPulledUpPredicates(left)
        val rightInfo: RelOptPredicateList = mq.getPulledUpPredicates(right)
        val joinInference = JoinConditionBasedPredicateInference(
            join,
            RexUtil.composeConjunction(rexBuilder, leftInfo.pulledUpPredicates),
            RexUtil.composeConjunction(rexBuilder, rightInfo.pulledUpPredicates),
            RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, executor)
        )
        return joinInference.inferPredicates(false)
    }

    /**
     * Infers predicates for an Aggregate.
     *
     *
     * Pulls up predicates that only contains references to columns in the
     * GroupSet. For e.g.
     *
     * <blockquote><pre>
     * inputPullUpExprs : { a &gt; 7, b + c &lt; 10, a + e = 9}
     * groupSet         : { a, b}
     * pulledUpExprs    : { a &gt; 7}
    </pre></blockquote> *
     */
    fun getPredicates(agg: Aggregate, mq: RelMetadataQuery): RelOptPredicateList {
        val input: RelNode = agg.getInput()
        val rexBuilder: RexBuilder = agg.getCluster().getRexBuilder()
        val inputInfo: RelOptPredicateList = mq.getPulledUpPredicates(input)
        val aggPullUpPredicates: List<RexNode> = ArrayList()
        val groupKeys: ImmutableBitSet = agg.getGroupSet()
        if (groupKeys.isEmpty()) {
            // "GROUP BY ()" can convert an empty relation to a non-empty relation, so
            // it is not valid to pull up predicates. In particular, consider the
            // predicate "false": it is valid on all input rows (trivially - there are
            // no rows!) but not on the output (there is one row).
            return RelOptPredicateList.EMPTY
        }
        val m: Mapping = Mappings.create(
            MappingType.PARTIAL_FUNCTION,
            input.getRowType().getFieldCount(), agg.getRowType().getFieldCount()
        )
        var i = 0
        for (j in groupKeys) {
            m.set(j, i++)
        }
        for (r in inputInfo.pulledUpPredicates) {
            val rCols: ImmutableBitSet = RelOptUtil.InputFinder.bits(r)
            if (groupKeys.contains(rCols)) {
                r = r.accept(RexPermuteInputsShuttle(m, input))
                aggPullUpPredicates.add(r)
            }
        }
        return RelOptPredicateList.of(rexBuilder, aggPullUpPredicates)
    }

    /**
     * Infers predicates for a Union.
     */
    fun getPredicates(union: Union, mq: RelMetadataQuery): RelOptPredicateList {
        val rexBuilder: RexBuilder = union.getCluster().getRexBuilder()
        var finalPredicates: Set<RexNode?> = HashSet()
        val finalResidualPredicates: List<RexNode> = ArrayList()
        for (input in Ord.zip(union.getInputs())) {
            val info: RelOptPredicateList = mq.getPulledUpPredicates(input.e)
            if (info.pulledUpPredicates.isEmpty()) {
                return RelOptPredicateList.EMPTY
            }
            val predicates: Set<RexNode> = HashSet()
            val residualPredicates: List<RexNode> = ArrayList()
            for (pred in info.pulledUpPredicates) {
                if (input.i === 0) {
                    predicates.add(pred)
                    continue
                }
                if (finalPredicates.contains(pred)) {
                    predicates.add(pred)
                } else {
                    residualPredicates.add(pred)
                }
            }
            // Add new residual predicates
            finalResidualPredicates.add(RexUtil.composeConjunction(rexBuilder, residualPredicates))
            // Add those that are not part of the final set to residual
            for (e in finalPredicates) {
                if (!predicates.contains(e)) {
                    // This node was in previous union inputs, but it is not in this one
                    for (j in 0 until input.i) {
                        finalResidualPredicates.set(
                            j,
                            RexUtil.composeConjunction(
                                rexBuilder,
                                Arrays.asList(finalResidualPredicates[j], e)
                            )
                        )
                    }
                }
            }
            // Final predicates
            finalPredicates = predicates
        }
        val predicates: List<RexNode> = ArrayList(finalPredicates)
        val cluster: RelOptCluster = union.getCluster()
        val executor: RexExecutor = Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR)
        val disjunctivePredicate: RexNode = RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, executor)
            .simplifyUnknownAs(
                rexBuilder.makeCall(SqlStdOperatorTable.OR, finalResidualPredicates),
                RexUnknownAs.FALSE
            )
        if (!disjunctivePredicate.isAlwaysTrue()) {
            predicates.add(disjunctivePredicate)
        }
        return RelOptPredicateList.of(rexBuilder, predicates)
    }

    /**
     * Infers predicates for a Intersect.
     */
    fun getPredicates(intersect: Intersect, mq: RelMetadataQuery): RelOptPredicateList {
        val rexBuilder: RexBuilder = intersect.getCluster().getRexBuilder()
        val executor: RexExecutor = Util.first(intersect.getCluster().getPlanner().getExecutor(), RexUtil.EXECUTOR)
        val rexImplicationChecker = RexImplicationChecker(rexBuilder, executor, intersect.getRowType())
        var finalPredicates: Set<RexNode?> = HashSet()
        for (input in Ord.zip(intersect.getInputs())) {
            val info: RelOptPredicateList = mq.getPulledUpPredicates(input.e)
            if (info == null || info.pulledUpPredicates.isEmpty()) {
                continue
            }
            for (pred in info.pulledUpPredicates) {
                if (finalPredicates.stream().anyMatch { finalPred -> rexImplicationChecker.implies(finalPred, pred) }) {
                    // There's already a stricter predicate in finalPredicates,
                    // thus no need to count this one.
                    continue
                }
                // Remove looser predicate and add this one into finalPredicates
                finalPredicates = finalPredicates.stream()
                    .filter { finalPred -> !rexImplicationChecker.implies(pred, finalPred) }
                    .collect(Collectors.toSet())
                finalPredicates.add(pred)
            }
        }
        return RelOptPredicateList.of(rexBuilder, finalPredicates)
    }

    /**
     * Infers predicates for a Minus.
     */
    fun getPredicates(minus: Minus, mq: RelMetadataQuery): RelOptPredicateList {
        return mq.getPulledUpPredicates(minus.getInput(0))
    }

    /**
     * Infers predicates for a Sort.
     */
    fun getPredicates(sort: Sort, mq: RelMetadataQuery): RelOptPredicateList {
        val input: RelNode = sort.getInput()
        return mq.getPulledUpPredicates(input)
    }

    /**
     * Infers predicates for a TableModify.
     */
    fun getPredicates(tableModify: TableModify, mq: RelMetadataQuery): RelOptPredicateList {
        return mq.getPulledUpPredicates(tableModify.getInput())
    }

    /**
     * Infers predicates for an Exchange.
     */
    fun getPredicates(
        exchange: Exchange,
        mq: RelMetadataQuery
    ): RelOptPredicateList {
        val input: RelNode = exchange.getInput()
        return mq.getPulledUpPredicates(input)
    }
    // CHECKSTYLE: IGNORE 1
    /**
     * Returns the
     * [BuiltInMetadata.Predicates.getPredicates]
     * statistic.
     * @see RelMetadataQuery.getPulledUpPredicates
     */
    fun getPredicates(
        r: RelSubset,
        mq: RelMetadataQuery
    ): RelOptPredicateList {
        if (!Bug.CALCITE_1048_FIXED) {
            return RelOptPredicateList.EMPTY
        }
        val rexBuilder: RexBuilder = r.getCluster().getRexBuilder()
        var list: RelOptPredicateList? = null
        for (r2 in r.getRels()) {
            val list2: RelOptPredicateList = mq.getPulledUpPredicates(r2)
            if (list2 != null) {
                list = if (list == null) list2 else list.union(rexBuilder, list2)
            }
        }
        return Util.first(list, RelOptPredicateList.EMPTY)
    }

    /**
     * Utility to infer predicates from one side of the join that apply on the
     * other side.
     *
     *
     * Contract is:
     *
     *  * initialize with a [org.apache.calcite.rel.core.Join] and
     * optional predicates applicable on its left and right subtrees.
     *
     *  * you can
     * then ask it for equivalentPredicate(s) given a predicate.
     *
     *
     *
     *
     * So for:
     *
     *  1. '`R1(x) join R2(y) on x = y`' a call for
     * equivalentPredicates on '`x > 7`' will return '
     * `[y > 7]`'
     *  1. '`R1(x) join R2(y) on x = y join R3(z) on y = z`' a call for
     * equivalentPredicates on the second join '`x > 7`' will return
     *
     */
    internal class JoinConditionBasedPredicateInference @SuppressWarnings("JdkObsolete") constructor(
        joinRel: Join, @Nullable leftPredicates: RexNode?,
        @Nullable rightPredicates: RexNode?, simplify: RexSimplify
    ) {
        val joinRel: Join
        val nSysFields: Int
        val nFieldsLeft: Int
        val nFieldsRight: Int
        val leftFieldsBitSet: ImmutableBitSet
        val rightFieldsBitSet: ImmutableBitSet
        val allFieldsBitSet: ImmutableBitSet

        @SuppressWarnings("JdkObsolete")
        var equivalence: SortedMap<Integer, BitSet>
        val exprFields: Map<RexNode, ImmutableBitSet>
        val allExprs: Set<RexNode>
        val equalityPredicates: Set<RexNode>

        @Nullable
        var leftChildPredicates: RexNode? = null

        @Nullable
        var rightChildPredicates: RexNode? = null
        val simplify: RexSimplify

        init {
            this.joinRel = joinRel
            this.simplify = simplify
            nFieldsLeft = joinRel.getLeft().getRowType().getFieldList().size()
            nFieldsRight = joinRel.getRight().getRowType().getFieldList().size()
            nSysFields = joinRel.getSystemFieldList().size()
            leftFieldsBitSet = ImmutableBitSet.range(
                nSysFields,
                nSysFields + nFieldsLeft
            )
            rightFieldsBitSet = ImmutableBitSet.range(
                nSysFields + nFieldsLeft,
                nSysFields + nFieldsLeft + nFieldsRight
            )
            allFieldsBitSet = ImmutableBitSet.range(
                0,
                nSysFields + nFieldsLeft + nFieldsRight
            )
            exprFields = HashMap()
            allExprs = HashSet()
            if (leftPredicates == null) {
                leftChildPredicates = null
            } else {
                val leftMapping: Mappings.TargetMapping = Mappings.createShiftMapping(
                    nSysFields + nFieldsLeft, nSysFields, 0, nFieldsLeft
                )
                leftChildPredicates = leftPredicates.accept(
                    RexPermuteInputsShuttle(leftMapping, joinRel.getInput(0))
                )
                allExprs.add(leftChildPredicates)
                for (r in RelOptUtil.conjunctions(leftChildPredicates)) {
                    exprFields.put(r, RelOptUtil.InputFinder.bits(r))
                    allExprs.add(r)
                }
            }
            if (rightPredicates == null) {
                rightChildPredicates = null
            } else {
                val rightMapping: Mappings.TargetMapping = Mappings.createShiftMapping(
                    nSysFields + nFieldsLeft + nFieldsRight,
                    nSysFields + nFieldsLeft, 0, nFieldsRight
                )
                rightChildPredicates = rightPredicates.accept(
                    RexPermuteInputsShuttle(rightMapping, joinRel.getInput(1))
                )
                allExprs.add(rightChildPredicates)
                for (r in RelOptUtil.conjunctions(rightChildPredicates)) {
                    exprFields.put(r, RelOptUtil.InputFinder.bits(r))
                    allExprs.add(r)
                }
            }
            equivalence = TreeMap()
            equalityPredicates = HashSet()
            for (i in 0 until nSysFields + nFieldsLeft + nFieldsRight) {
                equivalence.put(i, BitSets.of(i))
            }

            // Only process equivalences found in the join conditions. Processing
            // Equivalences from the left or right side infer predicates that are
            // already present in the Tree below the join.
            val exprs: List<RexNode> = RelOptUtil.conjunctions(joinRel.getCondition())
            val eF: EquivalenceFinder = EquivalenceFinder()
            exprs.forEach { input -> input.accept(eF) }
            equivalence = BitSets.closure(equivalence)
        }

        /**
         * The PullUp Strategy is sound but not complete.
         *
         *  1. We only pullUp inferred predicates for now. Pulling up existing
         * predicates causes an explosion of duplicates. The existing predicates are
         * pushed back down as new predicates. Once we have rules to eliminate
         * duplicate Filter conditions, we should pullUp all predicates.
         *  1. For Left Outer: we infer new predicates from the left and set them as
         * applicable on the Right side. No predicates are pulledUp.
         *  1. Right Outer Joins are handled in an analogous manner.
         *  1. For Full Outer Joins no predicates are pulledUp or inferred.
         *
         */
        fun inferPredicates(
            includeEqualityInference: Boolean
        ): RelOptPredicateList {
            val inferredPredicates: List<RexNode> = ArrayList()
            val allExprs: Set<RexNode> = HashSet(allExprs)
            val joinType: JoinRelType = joinRel.getJoinType()
            when (joinType) {
                SEMI, INNER, LEFT -> infer(
                    leftChildPredicates, allExprs, inferredPredicates,
                    includeEqualityInference,
                    if (joinType === JoinRelType.LEFT) rightFieldsBitSet else allFieldsBitSet
                )
                else -> {}
            }
            when (joinType) {
                SEMI, INNER, RIGHT -> infer(
                    rightChildPredicates, allExprs, inferredPredicates,
                    includeEqualityInference,
                    if (joinType === JoinRelType.RIGHT) leftFieldsBitSet else allFieldsBitSet
                )
                else -> {}
            }
            val rightMapping: Mappings.TargetMapping = Mappings.createShiftMapping(
                nSysFields + nFieldsLeft + nFieldsRight,
                0, nSysFields + nFieldsLeft, nFieldsRight
            )
            val rightPermute = RexPermuteInputsShuttle(rightMapping, joinRel)
            val leftMapping: Mappings.TargetMapping = Mappings.createShiftMapping(
                nSysFields + nFieldsLeft, 0, nSysFields, nFieldsLeft
            )
            val leftPermute = RexPermuteInputsShuttle(leftMapping, joinRel)
            val leftInferredPredicates: List<RexNode> = ArrayList()
            val rightInferredPredicates: List<RexNode> = ArrayList()
            for (iP in inferredPredicates) {
                val iPBitSet: ImmutableBitSet = RelOptUtil.InputFinder.bits(iP)
                if (leftFieldsBitSet.contains(iPBitSet)) {
                    leftInferredPredicates.add(iP.accept(leftPermute))
                } else if (rightFieldsBitSet.contains(iPBitSet)) {
                    rightInferredPredicates.add(iP.accept(rightPermute))
                }
            }
            val rexBuilder: RexBuilder = joinRel.getCluster().getRexBuilder()
            return when (joinType) {
                SEMI -> {
                    val pulledUpPredicates: Iterable<RexNode>
                    pulledUpPredicates = Iterables.concat(
                        RelOptUtil.conjunctions(leftChildPredicates),
                        leftInferredPredicates
                    )
                    RelOptPredicateList.of(
                        rexBuilder, pulledUpPredicates,
                        leftInferredPredicates, rightInferredPredicates
                    )
                }
                INNER -> {
                    pulledUpPredicates = Iterables.concat(
                        RelOptUtil.conjunctions(leftChildPredicates),
                        RelOptUtil.conjunctions(rightChildPredicates),
                        RexUtil.retainDeterministic(
                            RelOptUtil.conjunctions(joinRel.getCondition())
                        ),
                        inferredPredicates
                    )
                    RelOptPredicateList.of(
                        rexBuilder, pulledUpPredicates,
                        leftInferredPredicates, rightInferredPredicates
                    )
                }
                LEFT -> RelOptPredicateList.of(
                    rexBuilder,
                    RelOptUtil.conjunctions(leftChildPredicates),
                    leftInferredPredicates, rightInferredPredicates
                )
                RIGHT -> RelOptPredicateList.of(
                    rexBuilder,
                    RelOptUtil.conjunctions(rightChildPredicates),
                    inferredPredicates, EMPTY_LIST
                )
                else -> {
                    assert(inferredPredicates.size() === 0)
                    RelOptPredicateList.EMPTY
                }
            }
        }

        @Nullable
        fun left(): RexNode? {
            return leftChildPredicates
        }

        @Nullable
        fun right(): RexNode? {
            return rightChildPredicates
        }

        private fun infer(
            @Nullable predicates: RexNode?, allExprs: Set<RexNode>,
            inferredPredicates: List<RexNode>, includeEqualityInference: Boolean,
            inferringFields: ImmutableBitSet
        ) {
            for (r in RelOptUtil.conjunctions(predicates)) {
                if (!includeEqualityInference
                    && equalityPredicates.contains(r)
                ) {
                    continue
                }
                for (m in mappings(r)) {
                    val tr: RexNode = r.accept(
                        RexPermuteInputsShuttle(
                            m, joinRel.getInput(0),
                            joinRel.getInput(1)
                        )
                    )
                    // Filter predicates can be already simplified, so we should work with
                    // simplified RexNode versions as well. It also allows prevent of having
                    // some duplicates in in result pulledUpPredicates
                    var simplifiedTarget: RexNode = simplify.simplifyFilterPredicates(RelOptUtil.conjunctions(tr))
                    if (simplifiedTarget == null) {
                        simplifiedTarget = joinRel.getCluster().getRexBuilder().makeLiteral(false)
                    }
                    if (checkTarget(inferringFields, allExprs, tr)
                        && checkTarget(inferringFields, allExprs, simplifiedTarget)
                    ) {
                        inferredPredicates.add(simplifiedTarget)
                        allExprs.add(simplifiedTarget)
                    }
                }
            }
        }

        fun mappings(predicate: RexNode): Iterable<Mapping> {
            val fields: ImmutableBitSet = requireNonNull(
                exprFields[predicate]
            ) { "exprFields.get(predicate) is null for $predicate" }
            return if (fields.cardinality() === 0) {
                Collections.emptyList()
            } else Iterable<Mapping> {
                ExprsItr(
                    fields
                )
            }
        }

        @SuppressWarnings("JdkObsolete")
        private fun markAsEquivalent(p1: Int, p2: Int) {
            var b: BitSet = requireNonNull(
                equivalence.get(p1)
            ) { "equivalence.get(p1) for $p1" }
            b.set(p2)
            b = requireNonNull(
                equivalence.get(p2)
            ) { "equivalence.get(p2) for $p2" }
            b.set(p1)
        }

        /**
         * Find expressions of the form 'col_x = col_y'.
         */
        internal inner class EquivalenceFinder : RexVisitorImpl<Void?>(true) {
            @Override
            fun visitCall(call: RexCall): Void? {
                if (call.getOperator().getKind() === SqlKind.EQUALS) {
                    val lPos = pos(call.getOperands().get(0))
                    val rPos = pos(call.getOperands().get(1))
                    if (lPos != -1 && rPos != -1) {
                        markAsEquivalent(lPos, rPos)
                        equalityPredicates.add(call)
                    }
                }
                return null
            }
        }

        /**
         * Given an expression returns all the possible substitutions.
         *
         *
         * For example, for an expression 'a + b + c' and the following
         * equivalences: <pre>
         * a : {a, b}
         * b : {a, b}
         * c : {c, e}
        </pre> *
         *
         *
         * The following Mappings will be returned:
         * <pre>
         * {a  a, b  a, c  c}
         * {a  a, b  a, c  e}
         * {a  a, b  b, c  c}
         * {a  a, b  b, c  e}
         * {a  b, b  a, c  c}
         * {a  b, b  a, c  e}
         * {a  b, b  b, c  c}
         * {a  b, b  b, c  e}
        </pre> *
         *
         *
         * which imply the following inferences:
         * <pre>
         * a + a + c
         * a + a + e
         * a + b + c
         * a + b + e
         * b + a + c
         * b + a + e
         * b + b + c
         * b + b + e
        </pre> *
         */
        internal inner class ExprsItr @SuppressWarnings("JdkObsolete") constructor(fields: ImmutableBitSet) :
            Iterator<Mapping?> {
            val columns: IntArray
            val columnSets: Array<BitSet?>
            val iterationIdx: IntArray

            @Nullable
            var nextMapping: Mapping? = null
            var firstCall: Boolean

            init {
                columns = IntArray(fields.cardinality())
                columnSets = arrayOfNulls<BitSet>(fields.cardinality())
                iterationIdx = IntArray(fields.cardinality())
                var j = 0
                var i: Int = fields.nextSetBit(0)
                while (i >= 0) {
                    columns[j] = i
                    val fieldIndex = i
                    columnSets[j] = requireNonNull(
                        equivalence.get(i)
                    ) { "equivalence.get(i) is null for $fieldIndex, $equivalence" }
                    iterationIdx[j] = 0
                    i = fields
                        .nextSetBit(i + 1)
                    j++
                }
                firstCall = true
            }

            @Override
            override fun hasNext(): Boolean {
                if (firstCall) {
                    initializeMapping()
                    firstCall = false
                } else {
                    computeNextMapping(iterationIdx.size - 1)
                }
                return nextMapping != null
            }

            @Override
            override fun next(): Mapping {
                if (nextMapping == null) {
                    throw NoSuchElementException()
                }
                return nextMapping
            }

            @Override
            fun remove() {
                throw UnsupportedOperationException()
            }

            private fun computeNextMapping(level: Int) {
                val t: Int = columnSets[level].nextSetBit(iterationIdx[level])
                if (t < 0) {
                    if (level == 0) {
                        nextMapping = null
                    } else {
                        val tmp: Int = columnSets[level].nextSetBit(0)
                        requireNonNull(nextMapping, "nextMapping").set(columns[level], tmp)
                        iterationIdx[level] = tmp + 1
                        computeNextMapping(level - 1)
                    }
                } else {
                    requireNonNull(nextMapping, "nextMapping").set(columns[level], t)
                    iterationIdx[level] = t + 1
                }
            }

            private fun initializeMapping() {
                nextMapping = Mappings.create(
                    MappingType.PARTIAL_FUNCTION,
                    nSysFields + nFieldsLeft + nFieldsRight,
                    nSysFields + nFieldsLeft + nFieldsRight
                )
                for (i in columnSets.indices) {
                    val c: BitSet? = columnSets[i]
                    val t: Int = c.nextSetBit(iterationIdx[i])
                    if (t < 0) {
                        nextMapping = null
                        return
                    }
                    nextMapping.set(columns[i], t)
                    iterationIdx[i] = t + 1
                }
            }
        }

        companion object {
            private fun checkTarget(
                inferringFields: ImmutableBitSet,
                allExprs: Set<RexNode?>, tr: RexNode?
            ): Boolean {
                return (inferringFields.contains(RelOptUtil.InputFinder.bits(tr))
                        && !allExprs.contains(tr)
                        && !isAlwaysTrue(tr))
            }

            private fun pos(expr: RexNode): Int {
                return if (expr is RexInputRef) {
                    (expr as RexInputRef).getIndex()
                } else -1
            }

            private fun isAlwaysTrue(predicate: RexNode?): Boolean {
                if (predicate is RexCall) {
                    val c: RexCall? = predicate as RexCall?
                    if (c.getOperator().getKind() === SqlKind.EQUALS) {
                        val lPos = pos(c.getOperands().get(0))
                        val rPos = pos(c.getOperands().get(1))
                        return lPos != -1 && lPos == rPos
                    }
                }
                return predicate.isAlwaysTrue()
            }
        }
    }

    companion object {
        val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider
            .reflectiveSource(RelMdPredicates(), BuiltInMetadata.Predicates.Handler::class.java)
        private val EMPTY_LIST: List<RexNode> = ImmutableList.of()

        /** Converts a predicate on a particular set of columns into a predicate on
         * a subset of those columns, weakening if necessary.
         *
         *
         * If not possible to simplify, returns `true`, which is the weakest
         * possible predicate.
         *
         *
         * Examples:
         *  1. The predicate `$7 = $9` on columns [7]
         * becomes `$7 is not null`
         *  1. The predicate `$7 = $9 + $11` on columns [7, 9]
         * becomes `$7 is not null or $9 is not null`
         *  1. The predicate `$7 = $9 and $9 = 5` on columns [7] becomes
         * `$7 = 5`
         *  1. The predicate
         * `$7 = $9 and ($9 = $1 or $9 = $2) and $1 > 3 and $2 > 10`
         * on columns [7] becomes `$7 > 3`
         *
         *
         *
         * We currently only handle examples 1 and 2.
         *
         * @param rexBuilder Rex builder
         * @param input Input relational expression
         * @param r Predicate expression
         * @param columnsMapped Columns which the final predicate can reference
         * @return Predicate expression narrowed to reference only certain columns
         */
        private fun projectPredicate(
            rexBuilder: RexBuilder, input: RelNode,
            r: RexNode, columnsMapped: ImmutableBitSet
        ): RexNode {
            val rCols: ImmutableBitSet = RelOptUtil.InputFinder.bits(r)
            if (columnsMapped.contains(rCols)) {
                // All required columns are present. No need to weaken.
                return r
            }
            if (columnsMapped.intersects(rCols)) {
                val list: List<RexNode> = ArrayList()
                for (c in columnsMapped.intersect(rCols)) {
                    if (input.getRowType().getFieldList().get(c).getType().isNullable()
                        && Strong.isNull(r, ImmutableBitSet.of(c))
                    ) {
                        list.add(
                            rexBuilder.makeCall(
                                SqlStdOperatorTable.IS_NOT_NULL,
                                rexBuilder.makeInputRef(input, c)
                            )
                        )
                    }
                }
                if (!list.isEmpty()) {
                    return RexUtil.composeDisjunction(rexBuilder, list)
                }
            }
            // Cannot weaken to anything non-trivial
            return rexBuilder.makeLiteral(true)
        }
    }
}
