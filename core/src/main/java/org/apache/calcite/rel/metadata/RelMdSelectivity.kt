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

import org.apache.calcite.plan.RelOptUtil

/**
 * RelMdSelectivity supplies a default implementation of
 * [RelMetadataQuery.getSelectivity] for the standard logical algebra.
 */
class RelMdSelectivity  //~ Constructors -----------------------------------------------------------
protected constructor() : MetadataHandler<BuiltInMetadata.Selectivity?> {
    //~ Methods ----------------------------------------------------------------
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        @Override get() = BuiltInMetadata.Selectivity.DEF

    @Nullable
    fun getSelectivity(
        rel: Union, mq: RelMetadataQuery,
        @Nullable predicate: RexNode?
    ): Double? {
        if (rel.getInputs().size() === 0 || predicate == null) {
            return 1.0
        }
        var sumRows = 0.0
        var sumSelectedRows = 0.0
        val adjustments = IntArray(rel.getRowType().getFieldCount())
        val rexBuilder: RexBuilder = rel.getCluster().getRexBuilder()
        for (input in rel.getInputs()) {
            val nRows: Double = mq.getRowCount(input) ?: return null

            // convert the predicate to reference the types of the union child
            val modifiedPred: RexNode = predicate.accept(
                RexInputConverter(
                    rexBuilder,
                    null,
                    input.getRowType().getFieldList(),
                    adjustments
                )
            )
            val sel: Double = mq.getSelectivity(input, modifiedPred) ?: return null
            sumRows += nRows
            sumSelectedRows += nRows * sel
        }
        if (sumRows < 1.0) {
            sumRows = 1.0
        }
        return sumSelectedRows / sumRows
    }

    @Nullable
    fun getSelectivity(
        rel: Sort, mq: RelMetadataQuery,
        @Nullable predicate: RexNode?
    ): Double {
        return mq.getSelectivity(rel.getInput(), predicate)
    }

    @Nullable
    fun getSelectivity(
        rel: TableModify, mq: RelMetadataQuery,
        @Nullable predicate: RexNode?
    ): Double {
        return mq.getSelectivity(rel.getInput(), predicate)
    }

    @Nullable
    fun getSelectivity(
        rel: Filter, mq: RelMetadataQuery,
        @Nullable predicate: RexNode?
    ): Double {
        // Take the difference between the predicate passed in and the
        // predicate in the filter's condition, so we don't apply the
        // selectivity of the filter twice.  If no predicate is passed in,
        // use the filter's condition.
        return if (predicate != null) {
            mq.getSelectivity(
                rel.getInput(),
                RelMdUtil.minusPreds(
                    rel.getCluster().getRexBuilder(),
                    predicate,
                    rel.getCondition()
                )
            )
        } else {
            mq.getSelectivity(rel.getInput(), rel.getCondition())
        }
    }

    @Nullable
    fun getSelectivity(
        rel: Calc, mq: RelMetadataQuery,
        @Nullable predicate: RexNode?
    ): Double {
        var predicate: RexNode? = predicate
        if (predicate != null) {
            predicate = RelOptUtil.pushPastCalc(predicate, rel)
        }
        val rexProgram: RexProgram = rel.getProgram()
        val programCondition: RexLocalRef = rexProgram.getCondition()
        return if (programCondition == null) {
            mq.getSelectivity(rel.getInput(), predicate)
        } else {
            mq.getSelectivity(
                rel.getInput(),
                RelMdUtil.minusPreds(
                    rel.getCluster().getRexBuilder(),
                    predicate,
                    rexProgram.expandLocalRef(programCondition)
                )
            )
        }
    }

    @Nullable
    fun getSelectivity(
        rel: Join, mq: RelMetadataQuery,
        @Nullable predicate: RexNode?
    ): Double {
        if (!rel.isSemiJoin()) {
            return getSelectivity(rel as RelNode, mq, predicate)
        }
        // create a RexNode representing the selectivity of the
        // semijoin filter and pass it to getSelectivity
        val rexBuilder: RexBuilder = rel.getCluster().getRexBuilder()
        var newPred: RexNode = RelMdUtil.makeSemiJoinSelectivityRexNode(mq, rel)
        if (predicate != null) {
            newPred = rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                newPred,
                predicate
            )
        }
        return mq.getSelectivity(rel.getLeft(), newPred)
    }

    @Nullable
    fun getSelectivity(
        rel: Aggregate, mq: RelMetadataQuery,
        @Nullable predicate: RexNode?
    ): Double? {
        val notPushable: List<RexNode> = ArrayList()
        val pushable: List<RexNode> = ArrayList()
        RelOptUtil.splitFilters(
            rel.getGroupSet(),
            predicate,
            pushable,
            notPushable
        )
        val rexBuilder: RexBuilder = rel.getCluster().getRexBuilder()
        val childPred: RexNode = RexUtil.composeConjunction(rexBuilder, pushable, true)
        val selectivity: Double = mq.getSelectivity(rel.getInput(), childPred)
        return if (selectivity == null) {
            null
        } else {
            val pred: RexNode = RexUtil.composeConjunction(rexBuilder, notPushable, true)
            selectivity * RelMdUtil.guessSelectivity(pred)
        }
    }

    @Nullable
    fun getSelectivity(
        rel: Project, mq: RelMetadataQuery,
        @Nullable predicate: RexNode?
    ): Double? {
        val notPushable: List<RexNode> = ArrayList()
        val pushable: List<RexNode> = ArrayList()
        RelOptUtil.splitFilters(
            ImmutableBitSet.range(rel.getRowType().getFieldCount()),
            predicate,
            pushable,
            notPushable
        )
        val rexBuilder: RexBuilder = rel.getCluster().getRexBuilder()
        val childPred: RexNode = RexUtil.composeConjunction(rexBuilder, pushable, true)
        val modifiedPred: RexNode?
        modifiedPred = if (childPred == null) {
            null
        } else {
            RelOptUtil.pushPastProject(childPred, rel)
        }
        val selectivity: Double = mq.getSelectivity(rel.getInput(), modifiedPred)
        return if (selectivity == null) {
            null
        } else {
            val pred: RexNode = RexUtil.composeConjunction(rexBuilder, notPushable, true)
            selectivity * RelMdUtil.guessSelectivity(pred)
        }
    }

    // Catch-all rule when none of the others apply.
    fun getSelectivity(
        rel: RelNode?, mq: RelMetadataQuery?,
        @Nullable predicate: RexNode?
    ): Double {
        return RelMdUtil.guessSelectivity(predicate)
    }

    companion object {
        val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
            RelMdSelectivity(), BuiltInMetadata.Selectivity.Handler::class.java
        )
    }
}
