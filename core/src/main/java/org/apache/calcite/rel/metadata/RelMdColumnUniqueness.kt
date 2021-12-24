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

import org.apache.calcite.plan.RelOptPredicateList

/**
 * RelMdColumnUniqueness supplies a default implementation of
 * [RelMetadataQuery.areColumnsUnique] for the standard logical algebra.
 */
class RelMdColumnUniqueness  //~ Constructors -----------------------------------------------------------
private constructor() : MetadataHandler<BuiltInMetadata.ColumnUniqueness?> {
    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        get() = BuiltInMetadata.ColumnUniqueness.DEF

    fun areColumnsUnique(
        rel: TableScan, mq: RelMetadataQuery?,
        columns: ImmutableBitSet?, ignoreNulls: Boolean
    ): Boolean {
        return rel.getTable().isKey(columns)
    }

    @Nullable
    fun areColumnsUnique(
        rel: Filter, mq: RelMetadataQuery,
        columns: ImmutableBitSet, ignoreNulls: Boolean
    ): Boolean {
        var columns: ImmutableBitSet = columns
        columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq)
        return mq.areColumnsUnique(rel.getInput(), columns, ignoreNulls)
    }

    /** Catch-all implementation for
     * [BuiltInMetadata.ColumnUniqueness.areColumnsUnique],
     * invoked using reflection, for any relational expression not
     * handled by a more specific method.
     *
     * @param rel Relational expression
     * @param mq Metadata query
     * @param columns column mask representing the subset of columns for which
     * uniqueness will be determined
     * @param ignoreNulls if true, ignore null values when determining column
     * uniqueness
     * @return whether the columns are unique, or
     * null if not enough information is available to make that determination
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery.areColumnsUnique
     */
    @Nullable
    fun areColumnsUnique(
        rel: RelNode?, mq: RelMetadataQuery?,
        columns: ImmutableBitSet?, ignoreNulls: Boolean
    ): Boolean? {
        // no information available
        return null
    }

    fun areColumnsUnique(
        rel: SetOp, mq: RelMetadataQuery,
        columns: ImmutableBitSet, ignoreNulls: Boolean
    ): Boolean {
        var columns: ImmutableBitSet = columns
        columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq)
        // If not ALL then the rows are distinct.
        // Therefore the set of all columns is a key.
        return (!rel.all
                && columns.nextClearBit(0) >= rel.getRowType().getFieldCount())
    }

    fun areColumnsUnique(
        rel: Intersect, mq: RelMetadataQuery,
        columns: ImmutableBitSet, ignoreNulls: Boolean
    ): Boolean {
        var columns: ImmutableBitSet = columns
        columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq)
        if (areColumnsUnique(rel as SetOp, mq, columns, ignoreNulls)) {
            return true
        }
        for (input in rel.getInputs()) {
            val b: Boolean = mq.areColumnsUnique(input, columns, ignoreNulls)
            if (b != null && b) {
                return true
            }
        }
        return false
    }

    @Nullable
    fun areColumnsUnique(
        rel: Minus, mq: RelMetadataQuery,
        columns: ImmutableBitSet, ignoreNulls: Boolean
    ): Boolean {
        var columns: ImmutableBitSet = columns
        columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq)
        return if (areColumnsUnique(rel as SetOp, mq, columns, ignoreNulls)) {
            true
        } else mq.areColumnsUnique(
            rel.getInput(0),
            columns,
            ignoreNulls
        )
    }

    @Nullable
    fun areColumnsUnique(
        rel: Sort, mq: RelMetadataQuery,
        columns: ImmutableBitSet, ignoreNulls: Boolean
    ): Boolean {
        var columns: ImmutableBitSet = columns
        columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq)
        return mq.areColumnsUnique(rel.getInput(), columns, ignoreNulls)
    }

    @Nullable
    fun areColumnsUnique(
        rel: TableModify, mq: RelMetadataQuery,
        columns: ImmutableBitSet, ignoreNulls: Boolean
    ): Boolean {
        var columns: ImmutableBitSet = columns
        columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq)
        return mq.areColumnsUnique(rel.getInput(), columns, ignoreNulls)
    }

    @Nullable
    fun areColumnsUnique(
        rel: Exchange, mq: RelMetadataQuery,
        columns: ImmutableBitSet, ignoreNulls: Boolean
    ): Boolean {
        var columns: ImmutableBitSet = columns
        columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq)
        return mq.areColumnsUnique(rel.getInput(), columns, ignoreNulls)
    }

    @Nullable
    fun areColumnsUnique(
        rel: Correlate, mq: RelMetadataQuery,
        columns: ImmutableBitSet, ignoreNulls: Boolean
    ): Boolean? {
        var columns: ImmutableBitSet = columns
        columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq)
        return when (rel.getJoinType()) {
            ANTI, SEMI -> mq.areColumnsUnique(rel.getLeft(), columns, ignoreNulls)
            LEFT, INNER -> {
                val leftAndRightColumns: Pair<ImmutableBitSet, ImmutableBitSet> =
                    splitLeftAndRightColumns(
                        rel.getLeft().getRowType().getFieldCount(),
                        columns
                    )
                val leftColumns: ImmutableBitSet = leftAndRightColumns.left
                val rightColumns: ImmutableBitSet = leftAndRightColumns.right
                val left: RelNode = rel.getLeft()
                val right: RelNode = rel.getRight()
                if (leftColumns.cardinality() > 0
                    && rightColumns.cardinality() > 0
                ) {
                    val leftUnique: Boolean = mq.areColumnsUnique(left, leftColumns, ignoreNulls)
                    val rightUnique: Boolean = mq.areColumnsUnique(right, rightColumns, ignoreNulls)
                    if (leftUnique == null || rightUnique == null) {
                        null
                    } else {
                        leftUnique && rightUnique
                    }
                } else {
                    null
                }
            }
            else -> throw IllegalStateException(
                "Unknown join type " + rel.getJoinType()
                    .toString() + " for correlate relation " + rel
            )
        }
    }

    @Nullable
    fun areColumnsUnique(
        rel: Project, mq: RelMetadataQuery,
        columns: ImmutableBitSet, ignoreNulls: Boolean
    ): Boolean? {
        var columns: ImmutableBitSet = columns
        columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq)
        // LogicalProject maps a set of rows to a different set;
        // Without knowledge of the mapping function(whether it
        // preserves uniqueness), it is only safe to derive uniqueness
        // info from the child of a project when the mapping is f(a) => a.
        //
        // Also need to map the input column set to the corresponding child
        // references
        return areProjectColumnsUnique(rel, mq, columns, ignoreNulls, rel.getProjects())
    }

    @Nullable
    fun areColumnsUnique(
        rel: Calc, mq: RelMetadataQuery,
        columns: ImmutableBitSet, ignoreNulls: Boolean
    ): Boolean? {
        var columns: ImmutableBitSet = columns
        columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq)
        val program: RexProgram = rel.getProgram()
        return areProjectColumnsUnique(
            rel, mq, columns, ignoreNulls,
            Util.transform(program.getProjectList(), program::expandLocalRef)
        )
    }

    @Nullable
    fun areColumnsUnique(
        rel: Join, mq: RelMetadataQuery,
        columns: ImmutableBitSet, ignoreNulls: Boolean
    ): Boolean? {
        var columns: ImmutableBitSet = columns
        columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq)
        if (columns.cardinality() === 0) {
            return false
        }
        val left: RelNode = rel.getLeft()
        val right: RelNode = rel.getRight()

        // Semi or anti join should ignore uniqueness of the right input.
        if (!rel.getJoinType().projectsRight()) {
            return mq.areColumnsUnique(left, columns, ignoreNulls)
        }

        // Divide up the input column mask into column masks for the left and
        // right sides of the join
        val leftAndRightColumns: Pair<ImmutableBitSet, ImmutableBitSet> = splitLeftAndRightColumns(
            rel.getLeft().getRowType().getFieldCount(),
            columns
        )
        val leftColumns: ImmutableBitSet = leftAndRightColumns.left
        val rightColumns: ImmutableBitSet = leftAndRightColumns.right

        // for FULL OUTER JOIN if columns contain column from both inputs it is not
        // guaranteed that the result will be unique
        if (!ignoreNulls && rel.getJoinType() === JoinRelType.FULL && leftColumns.cardinality() > 0 && rightColumns.cardinality() > 0) {
            return false
        }

        // If the original column mask contains columns from both the left and
        // right hand side, then the columns are unique if and only if they're
        // unique for their respective join inputs
        val leftUnique: Boolean = mq.areColumnsUnique(left, leftColumns, ignoreNulls)
        val rightUnique: Boolean = mq.areColumnsUnique(right, rightColumns, ignoreNulls)
        if (leftColumns.cardinality() > 0
            && rightColumns.cardinality() > 0
        ) {
            return if (leftUnique == null || rightUnique == null) {
                null
            } else {
                leftUnique && rightUnique
            }
        }

        // If we're only trying to determine uniqueness for columns that
        // originate from one join input, then determine if the equijoin
        // columns from the other join input are unique.  If they are, then
        // the columns are unique for the entire join if they're unique for
        // the corresponding join input, provided that input is not null
        // generating.
        val joinInfo: JoinInfo = rel.analyzeCondition()
        if (leftColumns.cardinality() > 0) {
            if (rel.getJoinType().generatesNullsOnLeft()) {
                return false
            }
            val rightJoinColsUnique: Boolean = mq.areColumnsUnique(right, joinInfo.rightSet(), ignoreNulls)
            return if (rightJoinColsUnique == null || leftUnique == null) {
                null
            } else rightJoinColsUnique && leftUnique
        } else if (rightColumns.cardinality() > 0) {
            if (rel.getJoinType().generatesNullsOnRight()) {
                return false
            }
            val leftJoinColsUnique: Boolean = mq.areColumnsUnique(left, joinInfo.leftSet(), ignoreNulls)
            return if (leftJoinColsUnique == null || rightUnique == null) {
                null
            } else leftJoinColsUnique && rightUnique
        }
        throw AssertionError()
    }

    @Nullable
    fun areColumnsUnique(
        rel: Aggregate, mq: RelMetadataQuery,
        columns: ImmutableBitSet, ignoreNulls: Boolean
    ): Boolean? {
        var columns: ImmutableBitSet = columns
        if (Aggregate.isSimple(rel) || ignoreNulls) {
            columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq)
            // group by keys form a unique key
            val groupKey: ImmutableBitSet = ImmutableBitSet.range(rel.getGroupCount())
            return columns.contains(groupKey)
        }
        return null
    }

    fun areColumnsUnique(
        rel: Values, mq: RelMetadataQuery,
        columns: ImmutableBitSet, ignoreNulls: Boolean
    ): Boolean {
        var columns: ImmutableBitSet = columns
        columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq)
        if (rel.tuples.size() < 2) {
            return true
        }
        val set: Set<List<Comparable>> = HashSet()
        val values: List<Comparable> = ArrayList(columns.cardinality())
        for (tuple in rel.tuples) {
            for (column in columns) {
                val literal: RexLiteral = tuple.get(column)
                val value: Comparable = literal.getValueAs(Comparable::class.java)
                values.add(if (value == null) NullSentinel.INSTANCE else value)
            }
            if (!set.add(ImmutableList.copyOf(values))) {
                return false
            }
            values.clear()
        }
        return true
    }

    @Nullable
    fun areColumnsUnique(
        rel: Converter, mq: RelMetadataQuery,
        columns: ImmutableBitSet, ignoreNulls: Boolean
    ): Boolean {
        var columns: ImmutableBitSet = columns
        columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq)
        return mq.areColumnsUnique(rel.getInput(), columns, ignoreNulls)
    }

    @Nullable
    fun areColumnsUnique(
        rel: RelSubset, mq: RelMetadataQuery,
        columns: ImmutableBitSet, ignoreNulls: Boolean
    ): Boolean? {
        var columns: ImmutableBitSet = columns
        columns = decorateWithConstantColumnsFromPredicates(columns, rel, mq)
        for (rel2 in rel.getRels()) {
            if (rel2 is Aggregate
                || rel2 is Filter
                || rel2 is Values
                || rel2 is Sort
                || rel2 is TableScan
                || simplyProjects(rel2, columns)
            ) {
                try {
                    val unique: Boolean = mq.areColumnsUnique(rel2, columns, ignoreNulls)
                    if (unique != null) {
                        if (unique) {
                            return true
                        }
                    } else {
                        return null
                    }
                } catch (e: CyclicMetadataException) {
                    // Ignore this relational expression; there will be non-cyclic ones
                    // in this set.
                }
            }
        }
        return false
    }

    companion object {
        val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
            RelMdColumnUniqueness(), BuiltInMetadata.ColumnUniqueness.Handler::class.java
        )

        @Nullable
        private fun areProjectColumnsUnique(
            rel: SingleRel, mq: RelMetadataQuery,
            columns: ImmutableBitSet, ignoreNulls: Boolean, projExprs: List<RexNode>
        ): Boolean? {
            val typeFactory: RelDataTypeFactory = rel.getCluster().getTypeFactory()
            val childColumns: ImmutableBitSet.Builder = ImmutableBitSet.builder()
            for (bit in columns) {
                val projExpr: RexNode = projExprs[bit]
                if (projExpr is RexInputRef) {
                    childColumns.set((projExpr as RexInputRef).getIndex())
                } else if (projExpr is RexCall && ignoreNulls) {
                    // If the expression is a cast such that the types are the same
                    // except for the nullability, then if we're ignoring nulls,
                    // it doesn't matter whether the underlying column reference
                    // is nullable.  Check that the types are the same by making a
                    // nullable copy of both types and then comparing them.
                    val call: RexCall = projExpr as RexCall
                    if (call.getOperator() !== SqlStdOperatorTable.CAST) {
                        continue
                    }
                    val castOperand: RexNode = call.getOperands().get(0) as? RexInputRef ?: continue
                    val castType: RelDataType = typeFactory.createTypeWithNullability(
                        projExpr.getType(), true
                    )
                    val origType: RelDataType = typeFactory.createTypeWithNullability(
                        castOperand.getType(),
                        true
                    )
                    if (castType.equals(origType)) {
                        childColumns.set((castOperand as RexInputRef).getIndex())
                    }
                } else {
                    // If the expression will not influence uniqueness of the
                    // projection, then skip it.
                    continue
                }
            }

            // If no columns can affect uniqueness, then return unknown
            return if (childColumns.cardinality() === 0) {
                null
            } else mq.areColumnsUnique(
                rel.getInput(), childColumns.build(),
                ignoreNulls
            )
        }

        private fun simplyProjects(rel: RelNode, columns: ImmutableBitSet): Boolean {
            if (rel !is Project) {
                return false
            }
            val project: Project = rel as Project
            val projects: List<RexNode> = project.getProjects()
            for (column in columns) {
                if (column >= projects.size()) {
                    return false
                }
                if (projects[column] !is RexInputRef) {
                    return false
                }
                val ref: RexInputRef = projects[column] as RexInputRef
                if (ref.getIndex() !== column) {
                    return false
                }
            }
            return true
        }

        /** Splits a column set between left and right sets.  */
        private fun splitLeftAndRightColumns(
            leftCount: Int,
            columns: ImmutableBitSet
        ): Pair<ImmutableBitSet, ImmutableBitSet> {
            val leftBuilder: ImmutableBitSet.Builder = ImmutableBitSet.builder()
            val rightBuilder: ImmutableBitSet.Builder = ImmutableBitSet.builder()
            for (bit in columns) {
                if (bit < leftCount) {
                    leftBuilder.set(bit)
                } else {
                    rightBuilder.set(bit - leftCount)
                }
            }
            return Pair.of(leftBuilder.build(), rightBuilder.build())
        }

        /**
         * Deduce constant columns from predicates of rel and return the union
         * bitsets of checkingColumns and the constant columns.
         */
        private fun decorateWithConstantColumnsFromPredicates(
            checkingColumns: ImmutableBitSet, rel: RelNode, mq: RelMetadataQuery
        ): ImmutableBitSet {
            val predicates: RelOptPredicateList = mq.getPulledUpPredicates(rel)
            if (!RelOptPredicateList.isEmpty(predicates)) {
                val constantIndexes: Set<Integer> = HashSet()
                predicates.constantMap.keySet().forEach { rex ->
                    if (rex is RexInputRef) {
                        constantIndexes.add((rex as RexInputRef).getIndex())
                    }
                }
                if (!constantIndexes.isEmpty()) {
                    return checkingColumns.union(ImmutableBitSet.of(constantIndexes))
                }
            }
            // If no constant columns deduced, return the original "checkingColumns".
            return checkingColumns
        }
    }
}
