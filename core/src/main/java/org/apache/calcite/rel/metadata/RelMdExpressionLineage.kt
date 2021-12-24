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
 * Default implementation of
 * [RelMetadataQuery.getExpressionLineage] for the standard logical
 * algebra.
 *
 *
 * The goal of this provider is to infer the lineage for the given expression.
 *
 *
 * The output expressions might contain references to columns produced by
 * [TableScan] operators ([RexTableInputRef]). In turn, each
 * TableScan operator is identified uniquely by a [RelTableRef] containing
 * its qualified name and an identifier.
 *
 *
 * If the lineage cannot be inferred, we return null.
 */
class RelMdExpressionLineage  //~ Constructors -----------------------------------------------------------
protected constructor() : MetadataHandler<BuiltInMetadata.ExpressionLineage?> {
    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        get() = BuiltInMetadata.ExpressionLineage.DEF

    // Catch-all rule when none of the others apply.
    @Nullable
    fun getExpressionLineage(
        rel: RelNode?,
        mq: RelMetadataQuery?, outputExpression: RexNode?
    ): Set<RexNode>? {
        return null
    }

    @Nullable
    fun getExpressionLineage(
        rel: RelSubset,
        mq: RelMetadataQuery, outputExpression: RexNode?
    ): Set<RexNode>? {
        val bestOrOriginal: RelNode = Util.first(rel.getBest(), rel.getOriginal()) ?: return null
        return mq.getExpressionLineage(
            bestOrOriginal,
            outputExpression
        )
    }

    /**
     * Expression lineage from [TableScan].
     *
     *
     * We extract the fields referenced by the expression and we express them
     * using [RexTableInputRef].
     */
    @Nullable
    fun getExpressionLineage(
        rel: TableScan,
        mq: RelMetadataQuery?, outputExpression: RexNode
    ): Set<RexNode>? {
        val rexBuilder: RexBuilder = rel.getCluster().getRexBuilder()

        // Extract input fields referenced by expression
        val inputFieldsUsed: ImmutableBitSet = extractInputRefs(outputExpression)

        // Infer column origin expressions for given references
        val mapping: Map<RexInputRef?, Set<RexNode>> = LinkedHashMap()
        for (idx in inputFieldsUsed) {
            val inputRef: RexNode = RexTableInputRef.of(
                RelTableRef.of(rel.getTable(), 0),
                RexInputRef.of(idx, rel.getRowType().getFieldList())
            )
            val ref: RexInputRef = RexInputRef.of(idx, rel.getRowType().getFieldList())
            mapping.put(ref, ImmutableSet.of(inputRef))
        }

        // Return result
        return createAllPossibleExpressions(rexBuilder, outputExpression, mapping)
    }

    /**
     * Expression lineage from [Aggregate].
     *
     *
     * If the expression references grouping sets or aggregate function
     * results, we cannot extract the lineage and we return null.
     */
    @Nullable
    fun getExpressionLineage(
        rel: Aggregate,
        mq: RelMetadataQuery, outputExpression: RexNode
    ): Set<RexNode>? {
        val input: RelNode = rel.getInput()
        val rexBuilder: RexBuilder = rel.getCluster().getRexBuilder()

        // Extract input fields referenced by expression
        val inputFieldsUsed: ImmutableBitSet = extractInputRefs(outputExpression)
        for (idx in inputFieldsUsed) {
            if (idx >= rel.getGroupCount()) {
                // We cannot map origin of this expression.
                return null
            }
        }

        // Infer column origin expressions for given references
        val mapping: Map<RexInputRef?, Set<RexNode>> = LinkedHashMap()
        for (idx in inputFieldsUsed) {
            val inputRef: RexInputRef = RexInputRef.of(
                rel.getGroupSet().nth(idx),
                input.getRowType().getFieldList()
            )
            val originalExprs: Set<RexNode> = mq.getExpressionLineage(input, inputRef)
                ?: // Bail out
                return null
            val ref: RexInputRef = RexInputRef.of(idx, rel.getRowType().getFieldList())
            mapping.put(ref, originalExprs)
        }

        // Return result
        return createAllPossibleExpressions(rexBuilder, outputExpression, mapping)
    }

    /**
     * Expression lineage from [Join].
     *
     *
     * We only extract the lineage for INNER joins.
     */
    @Nullable
    fun getExpressionLineage(
        rel: Join, mq: RelMetadataQuery,
        outputExpression: RexNode
    ): Set<RexNode>? {
        val rexBuilder: RexBuilder = rel.getCluster().getRexBuilder()
        val leftInput: RelNode = rel.getLeft()
        val rightInput: RelNode = rel.getRight()
        val nLeftColumns: Int = leftInput.getRowType().getFieldList().size()

        // Extract input fields referenced by expression
        val inputFieldsUsed: ImmutableBitSet = extractInputRefs(outputExpression)
        if (rel.getJoinType().isOuterJoin()) {
            // If we reference the inner side, we will bail out
            if (rel.getJoinType() === JoinRelType.LEFT) {
                val rightFields: ImmutableBitSet = ImmutableBitSet.range(
                    nLeftColumns, rel.getRowType().getFieldCount()
                )
                if (inputFieldsUsed.intersects(rightFields)) {
                    // We cannot map origin of this expression.
                    return null
                }
            } else if (rel.getJoinType() === JoinRelType.RIGHT) {
                val leftFields: ImmutableBitSet = ImmutableBitSet.range(
                    0, nLeftColumns
                )
                if (inputFieldsUsed.intersects(leftFields)) {
                    // We cannot map origin of this expression.
                    return null
                }
            } else {
                // We cannot map origin of this expression.
                return null
            }
        }

        // Gather table references
        val leftTableRefs: Set<RelTableRef> = mq.getTableReferences(leftInput)
            ?: // Bail out
            return null
        val rightTableRefs: Set<RelTableRef> = mq.getTableReferences(rightInput)
            ?: // Bail out
            return null
        val qualifiedNamesToRefs: Multimap<List<String>, RelTableRef> = HashMultimap.create()
        val currentTablesMapping: Map<RelTableRef, RelTableRef> = HashMap()
        for (leftRef in leftTableRefs) {
            qualifiedNamesToRefs.put(leftRef.getQualifiedName(), leftRef)
        }
        for (rightRef in rightTableRefs) {
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

        // Infer column origin expressions for given references
        val mapping: Map<RexInputRef?, Set<RexNode>> = LinkedHashMap()
        for (idx in inputFieldsUsed) {
            if (idx < nLeftColumns) {
                val inputRef: RexInputRef = RexInputRef.of(idx, leftInput.getRowType().getFieldList())
                val originalExprs: Set<RexNode> = mq.getExpressionLineage(leftInput, inputRef)
                    ?: // Bail out
                    return null
                // Left input references remain unchanged
                mapping.put(RexInputRef.of(idx, rel.getRowType().getFieldList()), originalExprs)
            } else {
                // Right input.
                val inputRef: RexInputRef = RexInputRef.of(
                    idx - nLeftColumns,
                    rightInput.getRowType().getFieldList()
                )
                val originalExprs: Set<RexNode> = mq.getExpressionLineage(rightInput, inputRef)
                    ?: // Bail out
                    return null
                // Right input references might need to be updated if there are
                // table names clashes with left input
                val fullRowType: RelDataType = SqlValidatorUtil.createJoinType(
                    rexBuilder.getTypeFactory(),
                    rel.getLeft().getRowType(),
                    rel.getRight().getRowType(),
                    null,
                    ImmutableList.of()
                )
                val updatedExprs: Set<RexNode> = ImmutableSet.copyOf(
                    Util.transform(originalExprs) { e ->
                        RexUtil.swapTableReferences(
                            rexBuilder, e,
                            currentTablesMapping
                        )
                    })
                mapping.put(RexInputRef.of(idx, fullRowType), updatedExprs)
            }
        }

        // Return result
        return createAllPossibleExpressions(rexBuilder, outputExpression, mapping)
    }

    /**
     * Expression lineage from [Union].
     *
     *
     * For Union operator, we might be able to extract multiple origins for the
     * references in the given expression.
     */
    @Nullable
    fun getExpressionLineage(
        rel: Union, mq: RelMetadataQuery,
        outputExpression: RexNode
    ): Set<RexNode>? {
        val rexBuilder: RexBuilder = rel.getCluster().getRexBuilder()

        // Extract input fields referenced by expression
        val inputFieldsUsed: ImmutableBitSet = extractInputRefs(outputExpression)

        // Infer column origin expressions for given references
        val qualifiedNamesToRefs: Multimap<List<String>, RelTableRef> = HashMultimap.create()
        val mapping: Map<RexInputRef?, Set<RexNode>> = LinkedHashMap()
        for (input in rel.getInputs()) {
            // Gather table references
            val currentTablesMapping: Map<RelTableRef, RelTableRef> = HashMap()
            val tableRefs: Set<RelTableRef> = mq.getTableReferences(input)
                ?: // Bail out
                return null
            for (tableRef in tableRefs) {
                var shift = 0
                val lRefs: Collection<RelTableRef> = qualifiedNamesToRefs.get(
                    tableRef.getQualifiedName()
                )
                if (lRefs != null) {
                    shift = lRefs.size()
                }
                currentTablesMapping.put(
                    tableRef,
                    RelTableRef.of(tableRef.getTable(), shift + tableRef.getEntityNumber())
                )
            }
            // Map references
            for (idx in inputFieldsUsed) {
                val inputRef: RexInputRef = RexInputRef.of(idx, input.getRowType().getFieldList())
                val originalExprs: Set<RexNode> = mq.getExpressionLineage(input, inputRef)
                    ?: // Bail out
                    return null
                // References might need to be updated
                val ref: RexInputRef = RexInputRef.of(idx, rel.getRowType().getFieldList())
                val updatedExprs: Set<RexNode> = originalExprs.stream()
                    .map { e ->
                        RexUtil.swapTableReferences(
                            rexBuilder, e,
                            currentTablesMapping
                        )
                    }
                    .collect(Collectors.toSet())
                val set: Set<RexNode>? = mapping[ref]
                if (set != null) {
                    set.addAll(updatedExprs)
                } else {
                    mapping.put(ref, updatedExprs)
                }
            }
            // Add to existing qualified names
            for (newRef in currentTablesMapping.values()) {
                qualifiedNamesToRefs.put(newRef.getQualifiedName(), newRef)
            }
        }

        // Return result
        return createAllPossibleExpressions(rexBuilder, outputExpression, mapping)
    }

    /**
     * Expression lineage from Project.
     */
    @Nullable
    fun getExpressionLineage(
        rel: Project,
        mq: RelMetadataQuery, outputExpression: RexNode
    ): Set<RexNode>? {
        val input: RelNode = rel.getInput()
        val rexBuilder: RexBuilder = rel.getCluster().getRexBuilder()

        // Extract input fields referenced by expression
        val inputFieldsUsed: ImmutableBitSet = extractInputRefs(outputExpression)

        // Infer column origin expressions for given references
        val mapping: Map<RexInputRef?, Set<RexNode>> = LinkedHashMap()
        for (idx in inputFieldsUsed) {
            val inputExpr: RexNode = rel.getProjects().get(idx)
            val originalExprs: Set<RexNode> = mq.getExpressionLineage(input, inputExpr)
                ?: // Bail out
                return null
            val ref: RexInputRef = RexInputRef.of(idx, rel.getRowType().getFieldList())
            mapping.put(ref, originalExprs)
        }

        // Return result
        return createAllPossibleExpressions(rexBuilder, outputExpression, mapping)
    }

    /**
     * Expression lineage from Filter.
     */
    @Nullable
    fun getExpressionLineage(
        rel: Filter,
        mq: RelMetadataQuery, outputExpression: RexNode?
    ): Set<RexNode> {
        return mq.getExpressionLineage(rel.getInput(), outputExpression)
    }

    /**
     * Expression lineage from Sort.
     */
    @Nullable
    fun getExpressionLineage(
        rel: Sort, mq: RelMetadataQuery,
        outputExpression: RexNode?
    ): Set<RexNode> {
        return mq.getExpressionLineage(rel.getInput(), outputExpression)
    }

    /**
     * Expression lineage from TableModify.
     */
    @Nullable
    fun getExpressionLineage(
        rel: TableModify, mq: RelMetadataQuery,
        outputExpression: RexNode?
    ): Set<RexNode> {
        return mq.getExpressionLineage(rel.getInput(), outputExpression)
    }

    /**
     * Expression lineage from Exchange.
     */
    @Nullable
    fun getExpressionLineage(
        rel: Exchange,
        mq: RelMetadataQuery, outputExpression: RexNode?
    ): Set<RexNode> {
        return mq.getExpressionLineage(rel.getInput(), outputExpression)
    }

    /**
     * Expression lineage from Calc.
     */
    @Nullable
    fun getExpressionLineage(
        calc: Calc,
        mq: RelMetadataQuery, outputExpression: RexNode
    ): Set<RexNode>? {
        val input: RelNode = calc.getInput()
        val rexBuilder: RexBuilder = calc.getCluster().getRexBuilder()
        // Extract input fields referenced by expression
        val inputFieldsUsed: ImmutableBitSet = extractInputRefs(outputExpression)

        // Infer column origin expressions for given references
        val mapping: Map<RexInputRef?, Set<RexNode>> = LinkedHashMap()
        val calcProjectsAndFilter: Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> = calc.getProgram().split()
        for (idx in inputFieldsUsed) {
            val inputExpr: RexNode = calcProjectsAndFilter.getKey().get(idx)
            val originalExprs: Set<RexNode> = mq.getExpressionLineage(input, inputExpr)
                ?: // Bail out
                return null
            val ref: RexInputRef = RexInputRef.of(idx, calc.getRowType().getFieldList())
            mapping.put(ref, originalExprs)
        }

        // Return result
        return createAllPossibleExpressions(rexBuilder, outputExpression, mapping)
    }

    /**
     * Replaces expressions with their equivalences. Note that we only have to
     * look for RexInputRef.
     */
    private class RexReplacer internal constructor(replacementValues: Map<RexInputRef, RexNode>) : RexShuttle() {
        private val replacementValues: Map<RexInputRef, RexNode>

        init {
            this.replacementValues = replacementValues
        }

        @Override
        fun visitInputRef(inputRef: RexInputRef): RexNode {
            return requireNonNull(
                replacementValues[inputRef]
            ) { "no replacement found for inputRef $inputRef" }
        }
    }

    companion object {
        val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
            RelMdExpressionLineage(), BuiltInMetadata.ExpressionLineage.Handler::class.java
        )

        /**
         * Given an expression, it will create all equivalent expressions resulting
         * from replacing all possible combinations of references in the mapping by
         * the corresponding expressions.
         *
         * @param rexBuilder rexBuilder
         * @param expr expression
         * @param mapping mapping
         * @return set of resulting expressions equivalent to the input expression
         */
        @Nullable
        fun createAllPossibleExpressions(
            rexBuilder: RexBuilder,
            expr: RexNode, mapping: Map<RexInputRef?, Set<RexNode>>
        ): Set<RexNode>? {
            // Extract input fields referenced by expression
            val predFieldsUsed: ImmutableBitSet = extractInputRefs(expr)
            return if (predFieldsUsed.isEmpty()) {
                // The unique expression is the input expression
                ImmutableSet.of(expr)
            } else try {
                createAllPossibleExpressions(
                    rexBuilder, expr, predFieldsUsed, mapping,
                    HashMap()
                )
            } catch (e: UnsupportedOperationException) {
                // There may be a RexNode unsupported by RexCopier, just return null
                null
            }
        }

        private fun createAllPossibleExpressions(
            rexBuilder: RexBuilder,
            expr: RexNode, predFieldsUsed: ImmutableBitSet, mapping: Map<RexInputRef?, Set<RexNode>>,
            singleMapping: Map<RexInputRef, RexNode>
        ): Set<RexNode> {
            @KeyFor("mapping") val inputRef: RexInputRef = mapping.keySet().iterator().next()
            val replacements: Set<RexNode> = requireNonNull(
                mapping.remove(inputRef)
            ) { "mapping.remove(inputRef) is null for $inputRef" }
            val result: Set<RexNode> = HashSet()
            assert(!replacements.isEmpty())
            if (predFieldsUsed.indexOf(inputRef.getIndex()) !== -1) {
                for (replacement in replacements) {
                    singleMapping.put(inputRef, replacement)
                    createExpressions(rexBuilder, expr, predFieldsUsed, mapping, singleMapping, result)
                    singleMapping.remove(inputRef)
                }
            } else {
                createExpressions(rexBuilder, expr, predFieldsUsed, mapping, singleMapping, result)
            }
            mapping.put(inputRef, replacements)
            return result
        }

        private fun createExpressions(
            rexBuilder: RexBuilder,
            expr: RexNode, predFieldsUsed: ImmutableBitSet, mapping: Map<RexInputRef?, Set<RexNode>>,
            singleMapping: Map<RexInputRef, RexNode>, result: Set<RexNode>
        ) {
            if (mapping.isEmpty()) {
                val replacer = RexReplacer(singleMapping)
                val updatedPreds: List<RexNode> = ArrayList(1)
                updatedPreds.add(rexBuilder.copy(expr))
                replacer.mutate(updatedPreds)
                result.addAll(updatedPreds)
            } else {
                result.addAll(
                    createAllPossibleExpressions(
                        rexBuilder, expr, predFieldsUsed, mapping, singleMapping
                    )
                )
            }
        }

        private fun extractInputRefs(expr: RexNode): ImmutableBitSet {
            val inputExtraFields: Set<RelDataTypeField> = LinkedHashSet()
            val inputFinder: RelOptUtil.InputFinder = InputFinder(inputExtraFields)
            expr.accept(inputFinder)
            return inputFinder.build()
        }
    }
}
