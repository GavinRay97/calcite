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

import org.apache.calcite.linq4j.Ord

/**
 * Relational expression that imposes a particular sort order on its input
 * without otherwise changing its content.
 */
abstract class Sort protected constructor(
    cluster: RelOptCluster?,
    traits: RelTraitSet,
    child: RelNode?,
    collation: RelCollation,
    @Nullable offset: RexNode?,
    @Nullable fetch: RexNode?
) : SingleRel(cluster, traits, child) {
    //~ Instance fields --------------------------------------------------------
    val collation: RelCollation

    @Nullable
    val offset: RexNode?

    @Nullable
    val fetch: RexNode?
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a Sort.
     *
     * @param cluster   Cluster this relational expression belongs to
     * @param traits    Traits
     * @param child     input relational expression
     * @param collation array of sort specifications
     */
    protected constructor(
        cluster: RelOptCluster?,
        traits: RelTraitSet,
        child: RelNode?,
        collation: RelCollation
    ) : this(cluster, traits, child, collation, null, null) {
    }

    /**
     * Creates a Sort.
     *
     * @param cluster   Cluster this relational expression belongs to
     * @param traits    Traits
     * @param child     input relational expression
     * @param collation array of sort specifications
     * @param offset    Expression for number of rows to discard before returning
     * first row
     * @param fetch     Expression for number of rows to fetch
     */
    init {
        this.collation = collation
        this.offset = offset
        this.fetch = fetch
        assert(traits.containsIfApplicable(collation)) { "traits=$traits, collation=$collation" }
        assert(!(fetch == null && offset == null && collation.getFieldCollations().isEmpty())) { "trivial sort" }
    }

    /**
     * Creates a Sort by parsing serialized output.
     */
    protected constructor(input: RelInput) : this(
        input.getCluster(), input.getTraitSet().plus(input.getCollation()),
        input.getInput(),
        RelCollationTraitDef.INSTANCE.canonize(input.getCollation()),
        input.getExpression("offset"), input.getExpression("fetch")
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>?): Sort {
        return copy(traitSet, sole(inputs), collation, offset, fetch)
    }

    fun copy(
        traitSet: RelTraitSet?, newInput: RelNode?,
        newCollation: RelCollation?
    ): Sort {
        return copy(traitSet, newInput, newCollation, offset, fetch)
    }

    abstract fun copy(
        traitSet: RelTraitSet?, newInput: RelNode?,
        newCollation: RelCollation?, @Nullable offset: RexNode?, @Nullable fetch: RexNode?
    ): Sort

    /** {@inheritDoc}
     *
     *
     * The CPU cost of a Sort has three main cases:
     *
     *
     *  * If `fetch` is zero, CPU cost is zero; otherwise,
     *
     *  * if the sort keys are empty, we don't need to sort, only step over
     * the rows, and therefore the CPU cost is
     * `min(fetch + offset, inputRowCount) * bytesPerRow`; otherwise
     *
     *  * we need to read and sort `inputRowCount` rows, with at most
     * `min(fetch + offset, inputRowCount)` of them in the sort data
     * structure at a time, giving a CPU cost of `inputRowCount *
     * log(min(fetch + offset, inputRowCount)) * bytesPerRow`.
     *
     *
     *
     * The cost model factors in row width via `bytesPerRow`, because
     * sorts need to move rows around, not just compare them; by making the cost
     * higher if rows are wider, we discourage pushing a Project through a Sort.
     * We assume that each field is 4 bytes, and we add 3 'virtual fields' to
     * represent the per-row overhead. Thus a 1-field row is (3 + 1) * 4 = 16
     * bytes; a 5-field row is (3 + 5) * 4 = 32 bytes.
     *
     *
     * The cost model does not consider a 5-field sort to be more expensive
     * than, say, a 2-field sort, because both sorts will compare just one field
     * most of the time.  */
    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner,
        mq: RelMetadataQuery
    ): RelOptCost {
        val offsetValue: Double = Util.first(doubleValue(offset), 0.0)
        assert(offsetValue >= 0) { "offset should not be negative:$offsetValue" }
        val inCount: Double = mq.getRowCount(input)
        @Nullable val fetchValue = doubleValue(fetch)
        val readCount: Double
        readCount = if (fetchValue == null) {
            inCount
        } else if (fetchValue <= 0) {
            // Case 1. Read zero rows from input, therefore CPU cost is zero.
            return planner.getCostFactory().makeCost(inCount, 0, 0)
        } else {
            Math.min(inCount, offsetValue + fetchValue)
        }
        val bytesPerRow: Double = (3 + getRowType().getFieldCount()) * 4
        val cpu: Double
        cpu = if (collation.getFieldCollations().isEmpty()) {
            // Case 2. If sort keys are empty, CPU cost is cheaper because we are just
            // stepping over the first "readCount" rows, rather than sorting all
            // "inCount" them. (Presumably we are applying FETCH and/or OFFSET,
            // otherwise this Sort is a no-op.)
            readCount * bytesPerRow
        } else {
            // Case 3. Read and sort all "inCount" rows, keeping "readCount" in the
            // sort data structure at a time.
            Util.nLogM(inCount, readCount) * bytesPerRow
        }
        return planner.getCostFactory().makeCost(readCount, cpu, 0)
    }

    @Override
    fun accept(shuttle: RexShuttle): RelNode {
        val offset: RexNode = shuttle.apply(offset)
        val fetch: RexNode = shuttle.apply(fetch)
        val originalSortExps: List<RexNode> = sortExps
        val sortExps: List<RexNode> = shuttle.apply(originalSortExps)
        assert(sortExps === originalSortExps) {
            ("Sort node does not support modification of input field expressions."
                    + " Old expressions: " + originalSortExps + ", new ones: " + sortExps)
        }
        return if (offset === this.offset
            && fetch === this.fetch
        ) {
            this
        } else copy(traitSet, getInput(), collation, offset, fetch)
    }

    @get:Override
    val isEnforcer: Boolean
        get() = offset == null && fetch == null && collation.getFieldCollations().size() > 0

    /**
     * Returns the array of [RelFieldCollation]s asked for by the sort
     * specification, from most significant to least significant.
     *
     *
     * See also [RelMetadataQuery.collations],
     * which lists all known collations. For example,
     * `ORDER BY time_id` might also be sorted by
     * `the_year, the_month` because of a known monotonicity
     * constraint among the columns. `getCollation` would return
     * `[time_id]` and `collations` would return
     * `[ [time_id], [the_year, the_month] ]`.
     */
    fun getCollation(): RelCollation {
        return collation
    }

    /** Returns the sort expressions.  */
    val sortExps: List<Any>
        get() = Util.transform(collation.getFieldCollations()) { field ->
            getCluster().getRexBuilder().makeInputRef(
                input,
                Objects.requireNonNull(field, "field").getFieldIndex()
            )
        }

    @Override
    fun explainTerms(pw: RelWriter): RelWriter {
        super.explainTerms(pw)
        if (pw.nest()) {
            pw.item("collation", collation)
        } else {
            for (ord in Ord.zip(sortExps)) {
                pw.item("sort" + ord.i, ord.e)
            }
            for (ord in Ord.zip(collation.getFieldCollations())) {
                pw.item("dir" + ord.i, ord.e.shortString())
            }
        }
        pw.itemIf("offset", offset, offset != null)
        pw.itemIf("fetch", fetch, fetch != null)
        return pw
    }

    companion object {
        /** Returns the double value of a node if it is a literal, otherwise null.  */
        @Nullable
        private fun doubleValue(@Nullable r: RexNode?): Double? {
            return if (r is RexLiteral) (r as RexLiteral?).getValueAs(Double::class.java) else null
        }
    }
}
