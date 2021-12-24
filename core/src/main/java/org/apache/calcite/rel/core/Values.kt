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
package org.apache.calcite.rel.core

import org.apache.calcite.plan.RelOptCluster

/**
 * Relational expression whose value is a sequence of zero or more literal row
 * values.
 */
abstract class Values @SuppressWarnings("method.invocation.invalid") protected constructor(
    cluster: RelOptCluster?,
    rowType: RelDataType,
    tuples: ImmutableList<ImmutableList<RexLiteral?>?>,
    traits: RelTraitSet?
) : AbstractRelNode(cluster, traits) {
    //~ Instance fields --------------------------------------------------------
    val tuples: ImmutableList<ImmutableList<RexLiteral>>
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a new Values.
     *
     *
     * Note that tuples passed in become owned by
     * this rel (without a deep copy), so caller must not modify them after this
     * call, otherwise bad things will happen.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param rowType Row type for tuples produced by this rel
     * @param tuples  2-dimensional array of tuple values to be produced; outer
     * list contains tuples; each inner list is one tuple; all
     * tuples must be of same length, conforming to rowType
     */
    init {
        rowType = rowType
        this.tuples = tuples
        assert(assertRowType())
    }

    /**
     * Creates a Values by parsing serialized output.
     */
    protected constructor(input: RelInput) : this(
        input.getCluster(), input.getRowType("type"),
        input.getTuples("tuples"), input.getTraitSet()
    ) {
    }

    fun getTuples(input: RelInput): ImmutableList<ImmutableList<RexLiteral>> {
        return input.getTuples("tuples")
    }

    /** Returns the rows of literals represented by this Values relational
     * expression.  */
    fun getTuples(): ImmutableList<ImmutableList<RexLiteral>> {
        return tuples
    }

    /** Returns true if all tuples match rowType; otherwise, assert on
     * mismatch.  */
    private fun assertRowType(): Boolean {
        val rowType: RelDataType = getRowType()
        for (tuple in tuples) {
            assert(tuple.size() === rowType.getFieldCount())
            for (pair in Pair.zip(tuple, rowType.getFieldList())) {
                val literal: RexLiteral = pair.left
                val fieldType: RelDataType = pair.right.getType()

                // TODO jvs 19-Feb-2006: strengthen this a bit.  For example,
                // overflow, rounding, and padding/truncation must already have
                // been dealt with.
                if (!RexLiteral.isNullLiteral(literal)) {
                    assert(SqlTypeUtil.canAssignFrom(fieldType, literal.getType())) { "to $fieldType from $literal" }
                }
            }
        }
        return true
    }

    @Override
    protected fun deriveRowType(): RelDataType {
        assert(rowType != null) { "rowType must not be null for $this" }
        return rowType
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner,
        mq: RelMetadataQuery
    ): RelOptCost {
        val dRows: Double = mq.getRowCount(this)

        // Assume CPU is negligible since values are precomputed.
        val dCpu = 1.0
        val dIo = 0.0
        return planner.getCostFactory().makeCost(dRows, dCpu, dIo)
    }

    // implement RelNode
    @Override
    fun estimateRowCount(mq: RelMetadataQuery?): Double {
        return tuples.size()
    }

    // implement RelNode
    @Override
    fun explainTerms(pw: RelWriter): RelWriter {
        // A little adapter just to get the tuples to come out
        // with curly brackets instead of square brackets.  Plus
        // more whitespace for readability.
        val rowType: RelDataType = getRowType()
        val relWriter: RelWriter = super.explainTerms(pw) // For rel digest, include the row type since a rendered
            // literal may leave the type ambiguous (e.g. "null").
            .itemIf(
                "type", rowType,
                pw.getDetailLevel() === SqlExplainLevel.DIGEST_ATTRIBUTES
            )
            .itemIf("type", rowType.getFieldList(), pw.nest())
        if (pw.nest()) {
            pw.item("tuples", tuples)
        } else {
            pw.item("tuples",
                tuples.stream()
                    .map { row ->
                        row.stream()
                            .map { lit -> lit.computeDigest(RexDigestIncludeType.NO_TYPE) }
                            .collect(Collectors.joining(", ", "{ ", " }"))
                    }
                    .collect(Collectors.joining(", ", "[", "]")))
        }
        return relWriter
    }

    companion object {
        val IS_EMPTY_J: Predicate<in Values> = Predicate<in Values> { values: Values -> isEmpty(values) }

        @SuppressWarnings("Guava")
        @Deprecated // to be removed before 2.0
        val IS_EMPTY: com.google.common.base.Predicate<in Values> =
            com.google.common.base.Predicate<in Values> { values: Values -> isEmpty(values) }

        @SuppressWarnings("Guava")
        @Deprecated // to be removed before 2.0
        val IS_NOT_EMPTY: com.google.common.base.Predicate<in Values> =
            com.google.common.base.Predicate<in Values> { values: Values -> isNotEmpty(values) }
        //~ Methods ----------------------------------------------------------------
        /** Predicate, to be used when defining an operand of a [RelOptRule],
         * that returns true if a Values contains zero tuples.
         *
         *
         * This is the conventional way to represent an empty relational
         * expression. There are several rules that recognize empty relational
         * expressions and prune away that section of the tree.
         */
        fun isEmpty(values: Values): Boolean {
            return values.getTuples().isEmpty()
        }

        /** Predicate, to be used when defining an operand of a [RelOptRule],
         * that returns true if a Values contains one or more tuples.
         *
         *
         * This is the conventional way to represent an empty relational
         * expression. There are several rules that recognize empty relational
         * expressions and prune away that section of the tree.
         */
        fun isNotEmpty(values: Values): Boolean {
            return !isEmpty(values)
        }
    }
}
