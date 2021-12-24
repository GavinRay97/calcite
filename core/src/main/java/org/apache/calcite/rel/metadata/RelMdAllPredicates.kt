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
 * Utility to extract Predicates that are present in the (sub)plan
 * starting at this node.
 *
 *
 * This should be used to infer whether same filters are applied on
 * a given plan by materialized view rewriting rules.
 *
 *
 * The output predicates might contain references to columns produced
 * by TableScan operators ([RexTableInputRef]). In turn, each TableScan
 * operator is identified uniquely by its qualified name and an identifier.
 *
 *
 * If the provider cannot infer the lineage for any of the expressions
 * contain in any of the predicates, it will return null. Observe that
 * this is different from the empty list of predicates, which means that
 * there are not predicates in the (sub)plan.
 *
 */
class RelMdAllPredicates : MetadataHandler<BuiltInMetadata.AllPredicates?> {
    @get:Override
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        get() = BuiltInMetadata.AllPredicates.DEF

    /** Catch-all implementation for
     * [BuiltInMetadata.AllPredicates.getAllPredicates],
     * invoked using reflection.
     *
     * @see org.apache.calcite.rel.metadata.RelMetadataQuery.getAllPredicates
     */
    @Nullable
    fun getAllPredicates(rel: RelNode?, mq: RelMetadataQuery?): RelOptPredicateList? {
        return null
    }

    @Nullable
    fun getAllPredicates(rel: HepRelVertex, mq: RelMetadataQuery): RelOptPredicateList {
        return mq.getAllPredicates(rel.getCurrentRel())
    }

    @Nullable
    fun getAllPredicates(
        rel: RelSubset,
        mq: RelMetadataQuery
    ): RelOptPredicateList? {
        val bestOrOriginal: RelNode = Util.first(rel.getBest(), rel.getOriginal()) ?: return null
        return mq.getAllPredicates(bestOrOriginal)
    }

    /**
     * Extracts predicates for a table scan.
     */
    @Nullable
    fun getAllPredicates(scan: TableScan, mq: RelMetadataQuery?): RelOptPredicateList {
        val handler: BuiltInMetadata.AllPredicates.Handler =
            scan.getTable().unwrap(BuiltInMetadata.AllPredicates.Handler::class.java)
        return if (handler != null) {
            handler.getAllPredicates(scan, mq)
        } else RelOptPredicateList.EMPTY
    }

    /**
     * Extracts predicates for a project.
     */
    @Nullable
    fun getAllPredicates(project: Project, mq: RelMetadataQuery): RelOptPredicateList {
        return mq.getAllPredicates(project.getInput())
    }

    /**
     * Extracts predicates for a Filter.
     */
    @Nullable
    fun getAllPredicates(filter: Filter, mq: RelMetadataQuery): RelOptPredicateList? {
        return getAllFilterPredicates(filter.getInput(), mq, filter.getCondition())
    }

    /**
     * Extracts predicates for a Calc.
     */
    @Nullable
    fun getAllPredicates(calc: Calc, mq: RelMetadataQuery): RelOptPredicateList? {
        val rexProgram: RexProgram = calc.getProgram()
        return if (rexProgram.getCondition() != null) {
            val condition: RexNode = rexProgram.expandLocalRef(rexProgram.getCondition())
            getAllFilterPredicates(
                calc.getInput(),
                mq,
                condition
            )
        } else {
            mq.getAllPredicates(calc.getInput())
        }
    }

    /**
     * Add the Join condition to the list obtained from the input.
     */
    @Nullable
    fun getAllPredicates(join: Join, mq: RelMetadataQuery): RelOptPredicateList? {
        if (join.getJoinType().isOuterJoin()) {
            // We cannot map origin of this expression.
            return null
        }
        val rexBuilder: RexBuilder = join.getCluster().getRexBuilder()
        val pred: RexNode = join.getCondition()
        val qualifiedNamesToRefs: Multimap<List<String>, RelTableRef> = HashMultimap.create()
        var newPreds: RelOptPredicateList = RelOptPredicateList.EMPTY
        for (input in join.getInputs()) {
            val inputPreds: RelOptPredicateList = mq.getAllPredicates(input)
                ?: // Bail out
                return null
            // Gather table references
            val tableRefs: Set<RelTableRef> = mq.getTableReferences(input) ?: return null
            if (input === join.getLeft()) {
                // Left input references remain unchanged
                for (leftRef in tableRefs) {
                    qualifiedNamesToRefs.put(leftRef.getQualifiedName(), leftRef)
                }
                newPreds = newPreds.union(rexBuilder, inputPreds)
            } else {
                // Right input references might need to be updated if there are table name
                // clashes with left input
                val currentTablesMapping: Map<RelTableRef, RelTableRef> = HashMap()
                for (rightRef in tableRefs) {
                    var shift = 0
                    val lRefs: Collection<RelTableRef> = qualifiedNamesToRefs.get(
                        rightRef.getQualifiedName()
                    )
                    if (lRefs != null) {
                        shift = lRefs.size()
                    }
                    currentTablesMapping.put(
                        rightRef,
                        RelTableRef.of(rightRef.getTable(), shift + rightRef.getEntityNumber())
                    )
                }
                val updatedPreds: List<RexNode> = Util.transform(
                    inputPreds.pulledUpPredicates
                ) { e ->
                    RexUtil.swapTableReferences(
                        rexBuilder, e,
                        currentTablesMapping
                    )
                }
                newPreds = newPreds.union(
                    rexBuilder,
                    RelOptPredicateList.of(rexBuilder, updatedPreds)
                )
            }
        }

        // Extract input fields referenced by Join condition
        val inputExtraFields: Set<RelDataTypeField> = LinkedHashSet()
        val inputFinder: RelOptUtil.InputFinder = InputFinder(inputExtraFields)
        pred.accept(inputFinder)
        val inputFieldsUsed: ImmutableBitSet = inputFinder.build()

        // Infer column origin expressions for given references
        val mapping: Map<RexInputRef?, Set<RexNode>> = LinkedHashMap()
        val fullRowType: RelDataType = SqlValidatorUtil.createJoinType(
            rexBuilder.getTypeFactory(),
            join.getLeft().getRowType(),
            join.getRight().getRowType(),
            null,
            ImmutableList.of()
        )
        for (idx in inputFieldsUsed) {
            val inputRef: RexInputRef = RexInputRef.of(idx, fullRowType.getFieldList())
            val originalExprs: Set<RexNode> = mq.getExpressionLineage(join, inputRef)
                ?: // Bail out
                return null
            val ref: RexInputRef = RexInputRef.of(idx, fullRowType.getFieldList())
            mapping.put(ref, originalExprs)
        }

        // Replace with new expressions and return union of predicates
        val allExprs: Set<RexNode> = RelMdExpressionLineage.createAllPossibleExpressions(rexBuilder, pred, mapping)
            ?: return null
        return newPreds.union(rexBuilder, RelOptPredicateList.of(rexBuilder, allExprs))
    }

    /**
     * Extracts predicates for an Aggregate.
     */
    @Nullable
    fun getAllPredicates(agg: Aggregate, mq: RelMetadataQuery): RelOptPredicateList {
        return mq.getAllPredicates(agg.getInput())
    }

    /**
     * Extracts predicates for an TableModify.
     */
    @Nullable
    fun getAllPredicates(
        tableModify: TableModify,
        mq: RelMetadataQuery
    ): RelOptPredicateList {
        return mq.getAllPredicates(tableModify.getInput())
    }

    /**
     * Extracts predicates for a SetOp.
     */
    @Nullable
    fun getAllPredicates(setOp: SetOp, mq: RelMetadataQuery): RelOptPredicateList? {
        val rexBuilder: RexBuilder = setOp.getCluster().getRexBuilder()
        val qualifiedNamesToRefs: Multimap<List<String>, RelTableRef> = HashMultimap.create()
        var newPreds: RelOptPredicateList = RelOptPredicateList.EMPTY
        for (i in 0 until setOp.getInputs().size()) {
            val input: RelNode = setOp.getInput(i)
            val inputPreds: RelOptPredicateList = mq.getAllPredicates(input)
                ?: // Bail out
                return null
            // Gather table references
            val tableRefs: Set<RelTableRef> = mq.getTableReferences(input) ?: return null
            if (i == 0) {
                // Left input references remain unchanged
                for (leftRef in tableRefs) {
                    qualifiedNamesToRefs.put(leftRef.getQualifiedName(), leftRef)
                }
                newPreds = newPreds.union(rexBuilder, inputPreds)
            } else {
                // Right input references might need to be updated if there are table name
                // clashes with left input
                val currentTablesMapping: Map<RelTableRef, RelTableRef> = HashMap()
                for (rightRef in tableRefs) {
                    var shift = 0
                    val lRefs: Collection<RelTableRef> = qualifiedNamesToRefs.get(
                        rightRef.getQualifiedName()
                    )
                    if (lRefs != null) {
                        shift = lRefs.size()
                    }
                    currentTablesMapping.put(
                        rightRef,
                        RelTableRef.of(rightRef.getTable(), shift + rightRef.getEntityNumber())
                    )
                }
                // Add to existing qualified names
                for (newRef in currentTablesMapping.values()) {
                    qualifiedNamesToRefs.put(newRef.getQualifiedName(), newRef)
                }
                // Update preds
                val updatedPreds: List<RexNode> = Util.transform(
                    inputPreds.pulledUpPredicates
                ) { e ->
                    RexUtil.swapTableReferences(
                        rexBuilder, e,
                        currentTablesMapping
                    )
                }
                newPreds = newPreds.union(
                    rexBuilder,
                    RelOptPredicateList.of(rexBuilder, updatedPreds)
                )
            }
        }
        return newPreds
    }

    /**
     * Extracts predicates for a Sort.
     */
    @Nullable
    fun getAllPredicates(sort: Sort, mq: RelMetadataQuery): RelOptPredicateList {
        return mq.getAllPredicates(sort.getInput())
    }

    /**
     * Extracts predicates for an Exchange.
     */
    @Nullable
    fun getAllPredicates(
        exchange: Exchange,
        mq: RelMetadataQuery
    ): RelOptPredicateList {
        return mq.getAllPredicates(exchange.getInput())
    }

    companion object {
        val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
            RelMdAllPredicates(), BuiltInMetadata.AllPredicates.Handler::class.java
        )

        /**
         * Add the Filter condition to the list obtained from the input.
         * The pred comes from the parent of rel.
         */
        @Nullable
        private fun getAllFilterPredicates(
            rel: RelNode,
            mq: RelMetadataQuery, pred: RexNode
        ): RelOptPredicateList? {
            val rexBuilder: RexBuilder = rel.getCluster().getRexBuilder()
            val predsBelow: RelOptPredicateList = mq.getAllPredicates(rel)
                ?: // Safety check
                return null

            // Extract input fields referenced by Filter condition
            val inputExtraFields: Set<RelDataTypeField> = LinkedHashSet()
            val inputFinder: RelOptUtil.InputFinder = InputFinder(inputExtraFields)
            pred.accept(inputFinder)
            val inputFieldsUsed: ImmutableBitSet = inputFinder.build()

            // Infer column origin expressions for given references
            val mapping: Map<RexInputRef?, Set<RexNode>> = LinkedHashMap()
            for (idx in inputFieldsUsed) {
                val ref: RexInputRef = RexInputRef.of(idx, rel.getRowType().getFieldList())
                val originalExprs: Set<RexNode> = mq.getExpressionLineage(rel, ref)
                    ?: // Bail out
                    return null
                mapping.put(ref, originalExprs)
            }

            // Replace with new expressions and return union of predicates
            val allExprs: Set<RexNode> = RelMdExpressionLineage.createAllPossibleExpressions(rexBuilder, pred, mapping)
                ?: return null
            return predsBelow.union(rexBuilder, RelOptPredicateList.of(rexBuilder, allExprs))
        }
    }
}
