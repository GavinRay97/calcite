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
package org.apache.calcite.adapter.enumerable

import org.apache.calcite.adapter.java.JavaTypeFactory

/** Implementation of [org.apache.calcite.rel.core.Join] in
 * [enumerable calling convention][EnumerableConvention] using
 * a merge algorithm.  */
class EnumerableMergeJoin(
    cluster: RelOptCluster?,
    traits: RelTraitSet,
    left: RelNode,
    right: RelNode,
    condition: RexNode?,
    variablesSet: Set<CorrelationId?>?,
    joinType: JoinRelType
) : Join(cluster, traits, ImmutableList.of(), left, right, condition, variablesSet, joinType), EnumerableRel {
    init {
        assert(getConvention() is EnumerableConvention)
        val leftCollations: List<RelCollation> = getCollations(left.getTraitSet())
        val rightCollations: List<RelCollation> = getCollations(right.getTraitSet())

        // If the join keys are not distinct, the sanity check doesn't apply.
        // e.g. t1.a=t2.b and t1.a=t2.c
        val isDistinct = (Util.isDistinct(joinInfo.leftKeys)
                && Util.isDistinct(joinInfo.rightKeys))
        if (!RelCollations.collationsContainKeysOrderless(leftCollations, joinInfo.leftKeys)
            || !RelCollations.collationsContainKeysOrderless(rightCollations, joinInfo.rightKeys)
        ) {
            if (isDistinct) {
                throw RuntimeException("wrong collation in left or right input")
            }
        }
        val collations: List<RelCollation> = traits.getTraits(RelCollationTraitDef.INSTANCE)
        assert(collations != null && collations.size() > 0)
        val rightKeys: ImmutableIntList = joinInfo.rightKeys
            .incr(left.getRowType().getFieldCount())
        // Currently it has very limited ability to represent the equivalent traits
        // due to the flaw of RelCompositeTrait, so the following case is totally
        // legit, but not yet supported:
        // SELECT * FROM foo JOIN bar ON foo.a = bar.c AND foo.b = bar.d;
        // MergeJoin has collation on [a, d], or [b, c]
        if (!RelCollations.collationsContainKeysOrderless(collations, joinInfo.leftKeys)
            && !RelCollations.collationsContainKeysOrderless(collations, rightKeys)
            && !RelCollations.keysContainCollationsOrderless(joinInfo.leftKeys, collations)
            && !RelCollations.keysContainCollationsOrderless(rightKeys, collations)
        ) {
            if (isDistinct) {
                throw RuntimeException("wrong collation for mergejoin")
            }
        }
        if (!isMergeJoinSupported(joinType)) {
            throw UnsupportedOperationException(
                "EnumerableMergeJoin unsupported for join type $joinType"
            )
        }
    }

    @Deprecated
    internal constructor(
        cluster: RelOptCluster?, traits: RelTraitSet, left: RelNode,
        right: RelNode, condition: RexNode?, leftKeys: ImmutableIntList?,
        rightKeys: ImmutableIntList?, variablesSet: Set<CorrelationId?>?,
        joinType: JoinRelType
    ) : this(cluster, traits, left, right, condition, variablesSet, joinType) {
    }

    @Deprecated
    internal constructor(
        cluster: RelOptCluster?, traits: RelTraitSet?, left: RelNode?,
        right: RelNode?, condition: RexNode?, leftKeys: ImmutableIntList?,
        rightKeys: ImmutableIntList?, joinType: JoinRelType?,
        variablesStopped: Set<String?>?
    ) : this(
        cluster, traits, left, right, condition, leftKeys, rightKeys,
        CorrelationId.setOf(variablesStopped), joinType
    ) {
    }

    /**
     * Pass collations through can have three cases:
     * 1. If sort keys are equal to either left join keys, or right join keys,
     * collations can be pushed to both join sides with correct mappings.
     * For example, for the query
     * select * from foo join bar on foo.a=bar.b order by foo.a desc
     * after traits pass through it will be equivalent to
     * select * from
     * (select * from foo order by foo.a desc)
     * join
     * (select * from bar order by bar.b desc)
     *
     * 2. If sort keys are sub-set of either left join keys, or right join keys,
     * collations have to be extended to cover all joins keys before passing through,
     * because merge join requires all join keys are sorted.
     * For example, for the query
     * select * from foo join bar
     * on foo.a=bar.b and foo.c=bar.d
     * order by foo.a desc
     * after traits pass through it will be equivalent to
     * select * from
     * (select * from foo order by foo.a desc, foo.c)
     * join
     * (select * from bar order by bar.b desc, bar.d)
     *
     * 3. If sort keys are super-set of either left join keys, or right join keys,
     * but not both, collations can be completely passed to the join key whose join
     * keys match the prefix of collations. Meanwhile, partial mapped collations can
     * be passed to another join side to make sure join keys are sorted.
     * For example, for the query
     * select * from foo join bar
     * on foo.a=bar.b and foo.c=bar.d
     * order by foo.a desc, foo.c desc, foo.e
     * after traits pass through it will be equivalent to
     * select * from
     * (select * from foo order by foo.a desc, foo.c desc, foo.e)
     * join
     * (select * from bar order by bar.b desc, bar.d desc)
     */
    @Override
    @Nullable
    override fun passThroughTraits(
        required: RelTraitSet
    ): Pair<RelTraitSet, List<RelTraitSet>>? {
        // Required collation keys can be subset or superset of merge join keys.
        var collation: RelCollation = getCollation(required)
        val leftInputFieldCount: Int = left.getRowType().getFieldCount()
        val reqKeys: List<Integer> = RelCollations.ordinals(collation)
        val leftKeys: List<Integer> = joinInfo.leftKeys.toIntegerList()
        val rightKeys: List<Integer> = joinInfo.rightKeys.incr(leftInputFieldCount).toIntegerList()
        val reqKeySet: ImmutableBitSet = ImmutableBitSet.of(reqKeys)
        val leftKeySet: ImmutableBitSet = ImmutableBitSet.of(joinInfo.leftKeys)
        val rightKeySet: ImmutableBitSet = ImmutableBitSet.of(joinInfo.rightKeys)
            .shift(leftInputFieldCount)
        if (reqKeySet.equals(leftKeySet)) {
            // if sort keys equal to left join keys, we can pass through all collations directly.
            val mapping: Mappings.TargetMapping = buildMapping(true)
            val rightCollation: RelCollation = collation.apply(mapping)
            return Pair.of(
                required, ImmutableList.of(
                    required,
                    required.replace(rightCollation)
                )
            )
        } else if (containsOrderless(leftKeys, collation)) {
            // if sort keys are subset of left join keys, we can extend collations to make sure all join
            // keys are sorted.
            collation = extendCollation(collation, leftKeys)
            val mapping: Mappings.TargetMapping = buildMapping(true)
            val rightCollation: RelCollation = collation.apply(mapping)
            return Pair.of(
                required, ImmutableList.of(
                    required.replace(collation),
                    required.replace(rightCollation)
                )
            )
        } else if (containsOrderless(collation, leftKeys)
            && reqKeys.stream().allMatch { i -> i < leftInputFieldCount }
        ) {
            // if sort keys are superset of left join keys, and left join keys is prefix of sort keys
            // (order not matter), also sort keys are all from left join input.
            val mapping: Mappings.TargetMapping = buildMapping(true)
            val rightCollation: RelCollation = RexUtil.apply(
                mapping,
                intersectCollationAndJoinKey(collation, joinInfo.leftKeys)
            )
            return Pair.of(
                required, ImmutableList.of(
                    required,
                    required.replace(rightCollation)
                )
            )
        } else if (reqKeySet.equals(rightKeySet)) {
            // if sort keys equal to right join keys, we can pass through all collations directly.
            val rightCollation: RelCollation = RelCollations.shift(collation, -leftInputFieldCount)
            val mapping: Mappings.TargetMapping = buildMapping(false)
            val leftCollation: RelCollation = rightCollation.apply(mapping)
            return Pair.of(
                required, ImmutableList.of(
                    required.replace(leftCollation),
                    required.replace(rightCollation)
                )
            )
        } else if (containsOrderless(rightKeys, collation)) {
            // if sort keys are subset of right join keys, we can extend collations to make sure all join
            // keys are sorted.
            collation = extendCollation(collation, rightKeys)
            val rightCollation: RelCollation = RelCollations.shift(collation, -leftInputFieldCount)
            val mapping: Mappings.TargetMapping = buildMapping(false)
            val leftCollation: RelCollation = RexUtil.apply(mapping, rightCollation)
            return Pair.of(
                required, ImmutableList.of(
                    required.replace(leftCollation),
                    required.replace(rightCollation)
                )
            )
        } else if (containsOrderless(collation, rightKeys)
            && reqKeys.stream().allMatch { i -> i >= leftInputFieldCount }
        ) {
            // if sort keys are superset of right join keys, and right join keys is prefix of sort keys
            // (order not matter), also sort keys are all from right join input.
            val rightCollation: RelCollation = RelCollations.shift(collation, -leftInputFieldCount)
            val mapping: Mappings.TargetMapping = buildMapping(false)
            val leftCollation: RelCollation = RexUtil.apply(
                mapping,
                intersectCollationAndJoinKey(rightCollation, joinInfo.rightKeys)
            )
            return Pair.of(
                required, ImmutableList.of(
                    required.replace(leftCollation),
                    required.replace(rightCollation)
                )
            )
        }
        return null
    }

    @Override
    @Nullable
    override fun deriveTraits(
        childTraits: RelTraitSet, childId: Int
    ): Pair<RelTraitSet, List<RelTraitSet>>? {
        val keyCount: Int = joinInfo.leftKeys.size()
        var collation: RelCollation = getCollation(childTraits)
        val colCount: Int = collation.getFieldCollations().size()
        if (colCount < keyCount || keyCount == 0) {
            return null
        }
        if (colCount > keyCount) {
            collation = RelCollations.of(collation.getFieldCollations().subList(0, keyCount))
        }
        val sourceKeys: ImmutableIntList = if (childId == 0) joinInfo.leftKeys else joinInfo.rightKeys
        val keySet: ImmutableBitSet = ImmutableBitSet.of(sourceKeys)
        val childCollationKeys: ImmutableBitSet = ImmutableBitSet.of(
            RelCollations.ordinals(collation)
        )
        if (!childCollationKeys.equals(keySet)) {
            return null
        }
        val mapping: Mappings.TargetMapping = buildMapping(childId == 0)
        val targetCollation: RelCollation = collation.apply(mapping)
        return if (childId == 0) {
            // traits from left child
            val joinTraits: RelTraitSet = getTraitSet().replace(collation)
            // Forget about the equiv keys for the moment
            Pair.of(
                joinTraits,
                ImmutableList.of(
                    childTraits,
                    right.getTraitSet().replace(targetCollation)
                )
            )
        } else {
            // traits from right child
            assert(childId == 1)
            val joinTraits: RelTraitSet = getTraitSet().replace(targetCollation)
            // Forget about the equiv keys for the moment
            Pair.of(
                joinTraits,
                ImmutableList.of(
                    joinTraits,
                    childTraits.replace(collation)
                )
            )
        }
    }

    @get:Override
    override val deriveMode: DeriveMode
        get() = DeriveMode.BOTH

    private fun buildMapping(left2Right: Boolean): Mappings.TargetMapping {
        val sourceKeys: ImmutableIntList = if (left2Right) joinInfo.leftKeys else joinInfo.rightKeys
        val targetKeys: ImmutableIntList = if (left2Right) joinInfo.rightKeys else joinInfo.leftKeys
        val keyMap: Map<Integer, Integer> = HashMap()
        for (i in 0 until joinInfo.leftKeys.size()) {
            keyMap.put(sourceKeys.get(i), targetKeys.get(i))
        }
        return Mappings.target(
            keyMap,
            (if (left2Right) left else right).getRowType().getFieldCount(),
            (if (left2Right) right else left).getRowType().getFieldCount()
        )
    }

    @Override
    fun copy(
        traitSet: RelTraitSet,
        condition: RexNode?, left: RelNode, right: RelNode, joinType: JoinRelType,
        semiJoinDone: Boolean
    ): EnumerableMergeJoin {
        return EnumerableMergeJoin(
            getCluster(), traitSet, left, right,
            condition, variablesSet, joinType
        )
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner,
        mq: RelMetadataQuery
    ): RelOptCost {
        // We assume that the inputs are sorted. The price of sorting them has
        // already been paid. The cost of the join is therefore proportional to the
        // input and output size.
        val rightRowCount: Double = right.estimateRowCount(mq)
        val leftRowCount: Double = left.estimateRowCount(mq)
        val rowCount: Double = mq.getRowCount(this)
        val d = leftRowCount + rightRowCount + rowCount
        return planner.getCostFactory().makeCost(d, 0, 0)
    }

    @Override
    fun implement(implementor: EnumerableRelImplementor, pref: Prefer): Result {
        val builder = BlockBuilder()
        val leftResult: Result = implementor.visitChild(this, 0, left as EnumerableRel, pref)
        val leftExpression: Expression = builder.append("left", leftResult.block)
        val left_: ParameterExpression = Expressions.parameter(leftResult.physType.getJavaRowType(), "left")
        val rightResult: Result = implementor.visitChild(this, 1, right as EnumerableRel, pref)
        val rightExpression: Expression = builder.append("right", rightResult.block)
        val right_: ParameterExpression = Expressions.parameter(rightResult.physType.getJavaRowType(), "right")
        val typeFactory: JavaTypeFactory = implementor.getTypeFactory()
        val physType: PhysType = PhysTypeImpl.of(typeFactory, getRowType(), pref.preferArray())
        val leftExpressions: List<Expression> = ArrayList()
        val rightExpressions: List<Expression> = ArrayList()
        for (pair in Pair.zip(joinInfo.leftKeys, joinInfo.rightKeys)) {
            val leftType: RelDataType = left.getRowType().getFieldList().get(pair.left).getType()
            val rightType: RelDataType = right.getRowType().getFieldList().get(pair.right).getType()
            val keyType: RelDataType = requireNonNull(
                typeFactory.leastRestrictive(ImmutableList.of(leftType, rightType))
            ) { "leastRestrictive returns null for $leftType and $rightType" }
            val keyClass: Type = typeFactory.getJavaClass(keyType)
            leftExpressions.add(
                EnumUtils.convert(
                    leftResult.physType.fieldReference(left_, pair.left), keyClass
                )
            )
            rightExpressions.add(
                EnumUtils.convert(
                    rightResult.physType.fieldReference(right_, pair.right), keyClass
                )
            )
        }
        var predicate: Expression = Expressions.constant(null)
        if (!joinInfo.nonEquiConditions.isEmpty()) {
            val nonEquiCondition: RexNode = RexUtil.composeConjunction(
                getCluster().getRexBuilder(), joinInfo.nonEquiConditions, true
            )
            if (nonEquiCondition != null) {
                predicate = EnumUtils.generatePredicate(
                    implementor, getCluster().getRexBuilder(),
                    left, right, leftResult.physType, rightResult.physType, nonEquiCondition
                )
            }
        }
        val leftKeyPhysType: PhysType = leftResult.physType.project(joinInfo.leftKeys, JavaRowFormat.LIST)
        val rightKeyPhysType: PhysType = rightResult.physType.project(joinInfo.rightKeys, JavaRowFormat.LIST)

        // Generate the appropriate key Comparator (keys must be sorted in ascending order, nulls last).
        val keysSize: Int = joinInfo.leftKeys.size()
        val fieldCollations: List<RelFieldCollation> = ArrayList(keysSize)
        for (i in 0 until keysSize) {
            fieldCollations.add(
                RelFieldCollation(
                    i, RelFieldCollation.Direction.ASCENDING,
                    RelFieldCollation.NullDirection.LAST
                )
            )
        }
        val collation: RelCollation = RelCollations.of(fieldCollations)
        val comparator: Expression = leftKeyPhysType.generateComparator(collation)
        return implementor.result(
            physType,
            builder.append(
                Expressions.call(
                    BuiltInMethod.MERGE_JOIN.method,
                    Expressions.list(
                        leftExpression,
                        rightExpression,
                        Expressions.lambda(
                            leftKeyPhysType.record(leftExpressions), left_
                        ),
                        Expressions.lambda(
                            rightKeyPhysType.record(rightExpressions), right_
                        ),
                        predicate,
                        EnumUtils.joinSelector(
                            joinType,
                            physType,
                            ImmutableList.of(
                                leftResult.physType, rightResult.physType
                            )
                        ),
                        Expressions.constant(EnumUtils.toLinq4jJoinType(joinType)),
                        comparator
                    )
                )
            ).toBlock()
        )
    }

    companion object {
        fun isMergeJoinSupported(joinType: JoinRelType?): Boolean {
            return EnumerableDefaults.isMergeJoinSupported(EnumUtils.toLinq4jJoinType(joinType))
        }

        private fun getCollation(traits: RelTraitSet): RelCollation {
            return requireNonNull(
                traits.getCollation()
            ) { "no collation trait in $traits" }
        }

        private fun getCollations(traits: RelTraitSet): List<RelCollation> {
            return requireNonNull(
                traits.getTraits(RelCollationTraitDef.INSTANCE)
            ) { "no collation trait in $traits" }
        }

        /**
         * This function extends collation by appending new collation fields defined on keys.
         */
        private fun extendCollation(collation: RelCollation, keys: List<Integer>): RelCollation {
            val fieldsForNewCollation: List<RelFieldCollation> = ArrayList(keys.size())
            fieldsForNewCollation.addAll(collation.getFieldCollations())
            val keysBitset: ImmutableBitSet = ImmutableBitSet.of(keys)
            val colKeysBitset: ImmutableBitSet = ImmutableBitSet.of(collation.getKeys())
            val exceptBitset: ImmutableBitSet = keysBitset.except(colKeysBitset)
            for (i in exceptBitset) {
                fieldsForNewCollation.add(RelFieldCollation(i))
            }
            return RelCollations.of(fieldsForNewCollation)
        }

        /**
         * This function will remove collations that are not defined on join keys.
         * For example:
         * select * from
         * foo join bar
         * on foo.a = bar.a and foo.c=bar.c
         * order by bar.a, bar.c, bar.b;
         *
         * The collation [bar.a, bar.c, bar.b] can be pushed down to bar. However, only
         * [a, c] can be pushed down to foo. This function will help create [a, c] for foo by removing
         * b from the required collation, because b is not defined on join keys.
         *
         * @param collation collation defined on the JOIN
         * @param joinKeys  the join keys
         */
        private fun intersectCollationAndJoinKey(
            collation: RelCollation, joinKeys: ImmutableIntList
        ): RelCollation {
            val fieldCollations: List<RelFieldCollation> = ArrayList()
            for (rf in collation.getFieldCollations()) {
                if (joinKeys.contains(rf.getFieldIndex())) {
                    fieldCollations.add(rf)
                }
            }
            return RelCollations.of(fieldCollations)
        }

        fun create(
            left: RelNode, right: RelNode,
            condition: RexNode?, leftKeys: ImmutableIntList?,
            rightKeys: ImmutableIntList?, joinType: JoinRelType
        ): EnumerableMergeJoin {
            val cluster: RelOptCluster = right.getCluster()
            var traitSet: RelTraitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE)
            if (traitSet.isEnabled(RelCollationTraitDef.INSTANCE)) {
                val mq: RelMetadataQuery = cluster.getMetadataQuery()
                val collations: List<RelCollation> =
                    RelMdCollation.mergeJoin(mq, left, right, leftKeys, rightKeys, joinType)
                traitSet = traitSet.replaceIfs(RelCollationTraitDef.INSTANCE) { collations }
            }
            return EnumerableMergeJoin(
                cluster, traitSet, left, right, condition,
                ImmutableSet.of(), joinType
            )
        }
    }
}
