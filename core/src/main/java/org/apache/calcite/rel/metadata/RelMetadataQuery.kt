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

import org.apache.calcite.plan.RelOptCost

/**
 * RelMetadataQuery provides a strongly-typed facade on top of
 * [RelMetadataProvider] for the set of relational expression metadata
 * queries defined as standard within Calcite. The Javadoc on these methods
 * serves as their primary specification.
 *
 *
 * To add a new standard query `Xyz` to this interface, follow
 * these steps:
 *
 *
 *  1. Add a static method `getXyz` specification to this class.
 *  1. Add unit tests to `org.apache.calcite.test.RelMetadataTest`.
 *  1. Write a new provider class `RelMdXyz` in this package. Follow
 * the pattern from an existing class such as [RelMdColumnOrigins],
 * overloading on all of the logical relational expressions to which the query
 * applies.
 *  1. Add a `SOURCE` static member, similar to
 * [RelMdColumnOrigins.SOURCE].
 *  1. Register the `SOURCE` object in [DefaultRelMetadataProvider].
 *  1. Get unit tests working.
 *
 *
 *
 * Because relational expression metadata is extensible, extension projects
 * can define similar facades in order to specify access to custom metadata.
 * Please do not add queries here (nor on [RelNode]) which lack meaning
 * outside of your extension.
 *
 *
 * Besides adding new metadata queries, extension projects may need to add
 * custom providers for the standard queries in order to handle additional
 * relational expressions (either logical or physical). In either case, the
 * process is the same: write a reflective provider and chain it on to an
 * instance of [DefaultRelMetadataProvider], pre-pending it to the default
 * providers. Then supply that instance to the planner via the appropriate
 * plugin mechanism.
 */
class RelMetadataQuery : RelMetadataQueryBase {
    private var collationHandler: BuiltInMetadata.Collation.Handler
    private var columnOriginHandler: BuiltInMetadata.ColumnOrigin.Handler
    private var expressionLineageHandler: BuiltInMetadata.ExpressionLineage.Handler
    private var tableReferencesHandler: BuiltInMetadata.TableReferences.Handler
    private var columnUniquenessHandler: BuiltInMetadata.ColumnUniqueness.Handler
    private var cumulativeCostHandler: BuiltInMetadata.CumulativeCost.Handler
    private var distinctRowCountHandler: BuiltInMetadata.DistinctRowCount.Handler
    private var distributionHandler: BuiltInMetadata.Distribution.Handler
    private var explainVisibilityHandler: BuiltInMetadata.ExplainVisibility.Handler
    private var maxRowCountHandler: BuiltInMetadata.MaxRowCount.Handler
    private var minRowCountHandler: BuiltInMetadata.MinRowCount.Handler
    private var memoryHandler: BuiltInMetadata.Memory.Handler
    private var nonCumulativeCostHandler: BuiltInMetadata.NonCumulativeCost.Handler
    private var parallelismHandler: BuiltInMetadata.Parallelism.Handler
    private var percentageOriginalRowsHandler: BuiltInMetadata.PercentageOriginalRows.Handler
    private var populationSizeHandler: BuiltInMetadata.PopulationSize.Handler
    private var predicatesHandler: BuiltInMetadata.Predicates.Handler
    private var allPredicatesHandler: BuiltInMetadata.AllPredicates.Handler
    private var nodeTypesHandler: BuiltInMetadata.NodeTypes.Handler
    private var rowCountHandler: BuiltInMetadata.RowCount.Handler
    private var selectivityHandler: BuiltInMetadata.Selectivity.Handler
    private var sizeHandler: BuiltInMetadata.Size.Handler
    private var uniqueKeysHandler: BuiltInMetadata.UniqueKeys.Handler
    private var lowerBoundCostHandler: BuiltInMetadata.LowerBoundCost.Handler

    /**
     * Creates the instance with [JaninoRelMetadataProvider] instance
     * from [.THREAD_PROVIDERS] and [.EMPTY] as a prototype.
     */
    protected constructor() : this(castNonNull(THREAD_PROVIDERS.get()), EMPTY.get()) {}

    /**
     * Create a RelMetadataQuery with a given [MetadataHandlerProvider].
     * @param provider The provider to use for construction.
     */
    constructor(provider: MetadataHandlerProvider) : super(provider) {
        collationHandler = provider.handler(BuiltInMetadata.Collation.Handler::class.java)
        columnOriginHandler = provider.handler(BuiltInMetadata.ColumnOrigin.Handler::class.java)
        expressionLineageHandler = provider.handler(BuiltInMetadata.ExpressionLineage.Handler::class.java)
        tableReferencesHandler = provider.handler(BuiltInMetadata.TableReferences.Handler::class.java)
        columnUniquenessHandler = provider.handler(BuiltInMetadata.ColumnUniqueness.Handler::class.java)
        cumulativeCostHandler = provider.handler(BuiltInMetadata.CumulativeCost.Handler::class.java)
        distinctRowCountHandler = provider.handler(BuiltInMetadata.DistinctRowCount.Handler::class.java)
        distributionHandler = provider.handler(BuiltInMetadata.Distribution.Handler::class.java)
        explainVisibilityHandler = provider.handler(BuiltInMetadata.ExplainVisibility.Handler::class.java)
        maxRowCountHandler = provider.handler(BuiltInMetadata.MaxRowCount.Handler::class.java)
        minRowCountHandler = provider.handler(BuiltInMetadata.MinRowCount.Handler::class.java)
        memoryHandler = provider.handler(BuiltInMetadata.Memory.Handler::class.java)
        nonCumulativeCostHandler = provider.handler(BuiltInMetadata.NonCumulativeCost.Handler::class.java)
        parallelismHandler = provider.handler(BuiltInMetadata.Parallelism.Handler::class.java)
        percentageOriginalRowsHandler = provider.handler(BuiltInMetadata.PercentageOriginalRows.Handler::class.java)
        populationSizeHandler = provider.handler(BuiltInMetadata.PopulationSize.Handler::class.java)
        predicatesHandler = provider.handler(BuiltInMetadata.Predicates.Handler::class.java)
        allPredicatesHandler = provider.handler(BuiltInMetadata.AllPredicates.Handler::class.java)
        nodeTypesHandler = provider.handler(BuiltInMetadata.NodeTypes.Handler::class.java)
        rowCountHandler = provider.handler(BuiltInMetadata.RowCount.Handler::class.java)
        selectivityHandler = provider.handler(BuiltInMetadata.Selectivity.Handler::class.java)
        sizeHandler = provider.handler(BuiltInMetadata.Size.Handler::class.java)
        uniqueKeysHandler = provider.handler(BuiltInMetadata.UniqueKeys.Handler::class.java)
        lowerBoundCostHandler = provider.handler(BuiltInMetadata.LowerBoundCost.Handler::class.java)
    }

    /** Creates and initializes the instance that will serve as a prototype for
     * all other instances in the Janino case.  */
    @SuppressWarnings("deprecation")
    private constructor(@SuppressWarnings("unused") dummy: Boolean) : super(null) {
        collationHandler = initialHandler(BuiltInMetadata.Collation.Handler::class.java)
        columnOriginHandler = initialHandler(BuiltInMetadata.ColumnOrigin.Handler::class.java)
        expressionLineageHandler = initialHandler(BuiltInMetadata.ExpressionLineage.Handler::class.java)
        tableReferencesHandler = initialHandler(BuiltInMetadata.TableReferences.Handler::class.java)
        columnUniquenessHandler = initialHandler(BuiltInMetadata.ColumnUniqueness.Handler::class.java)
        cumulativeCostHandler = initialHandler(BuiltInMetadata.CumulativeCost.Handler::class.java)
        distinctRowCountHandler = initialHandler(BuiltInMetadata.DistinctRowCount.Handler::class.java)
        distributionHandler = initialHandler(BuiltInMetadata.Distribution.Handler::class.java)
        explainVisibilityHandler = initialHandler(BuiltInMetadata.ExplainVisibility.Handler::class.java)
        maxRowCountHandler = initialHandler(BuiltInMetadata.MaxRowCount.Handler::class.java)
        minRowCountHandler = initialHandler(BuiltInMetadata.MinRowCount.Handler::class.java)
        memoryHandler = initialHandler(BuiltInMetadata.Memory.Handler::class.java)
        nonCumulativeCostHandler = initialHandler(BuiltInMetadata.NonCumulativeCost.Handler::class.java)
        parallelismHandler = initialHandler(BuiltInMetadata.Parallelism.Handler::class.java)
        percentageOriginalRowsHandler = initialHandler(BuiltInMetadata.PercentageOriginalRows.Handler::class.java)
        populationSizeHandler = initialHandler(BuiltInMetadata.PopulationSize.Handler::class.java)
        predicatesHandler = initialHandler(BuiltInMetadata.Predicates.Handler::class.java)
        allPredicatesHandler = initialHandler(BuiltInMetadata.AllPredicates.Handler::class.java)
        nodeTypesHandler = initialHandler(BuiltInMetadata.NodeTypes.Handler::class.java)
        rowCountHandler = initialHandler(BuiltInMetadata.RowCount.Handler::class.java)
        selectivityHandler = initialHandler(BuiltInMetadata.Selectivity.Handler::class.java)
        sizeHandler = initialHandler(BuiltInMetadata.Size.Handler::class.java)
        uniqueKeysHandler = initialHandler(BuiltInMetadata.UniqueKeys.Handler::class.java)
        lowerBoundCostHandler = initialHandler(BuiltInMetadata.LowerBoundCost.Handler::class.java)
    }

    private constructor(
        metadataHandlerProvider: MetadataHandlerProvider,
        prototype: RelMetadataQuery
    ) : super(metadataHandlerProvider) {
        collationHandler = prototype.collationHandler
        columnOriginHandler = prototype.columnOriginHandler
        expressionLineageHandler = prototype.expressionLineageHandler
        tableReferencesHandler = prototype.tableReferencesHandler
        columnUniquenessHandler = prototype.columnUniquenessHandler
        cumulativeCostHandler = prototype.cumulativeCostHandler
        distinctRowCountHandler = prototype.distinctRowCountHandler
        distributionHandler = prototype.distributionHandler
        explainVisibilityHandler = prototype.explainVisibilityHandler
        maxRowCountHandler = prototype.maxRowCountHandler
        minRowCountHandler = prototype.minRowCountHandler
        memoryHandler = prototype.memoryHandler
        nonCumulativeCostHandler = prototype.nonCumulativeCostHandler
        parallelismHandler = prototype.parallelismHandler
        percentageOriginalRowsHandler = prototype.percentageOriginalRowsHandler
        populationSizeHandler = prototype.populationSizeHandler
        predicatesHandler = prototype.predicatesHandler
        allPredicatesHandler = prototype.allPredicatesHandler
        nodeTypesHandler = prototype.nodeTypesHandler
        rowCountHandler = prototype.rowCountHandler
        selectivityHandler = prototype.selectivityHandler
        sizeHandler = prototype.sizeHandler
        uniqueKeysHandler = prototype.uniqueKeysHandler
        lowerBoundCostHandler = prototype.lowerBoundCostHandler
    }

    /**
     * Returns the
     * [BuiltInMetadata.NodeTypes.getNodeTypes]
     * statistic.
     *
     * @param rel the relational expression
     */
    @Nullable
    fun getNodeTypes(rel: RelNode?): Multimap<Class<out RelNode?>, RelNode>? {
        while (true) {
            try {
                return nodeTypesHandler.getNodeTypes(rel, this)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                nodeTypesHandler = revise(BuiltInMetadata.NodeTypes.Handler::class.java)
            } catch (e: CyclicMetadataException) {
                return null
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.RowCount.getRowCount]
     * statistic.
     *
     * @param rel the relational expression
     * @return estimated row count, or null if no reliable estimate can be
     * determined
     */
    /* @Nullable: CALCITE-4263 */   fun getRowCount(rel: RelNode?): Double {
        while (true) {
            try {
                val result: Double = rowCountHandler.getRowCount(rel, this)
                return RelMdUtil.validateResult(castNonNull(result))!!
            } catch (e: MetadataHandlerProvider.NoHandler) {
                rowCountHandler = revise(BuiltInMetadata.RowCount.Handler::class.java)
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.MaxRowCount.getMaxRowCount]
     * statistic.
     *
     * @param rel the relational expression
     * @return max row count
     */
    @Nullable
    fun getMaxRowCount(rel: RelNode?): Double {
        while (true) {
            try {
                return maxRowCountHandler.getMaxRowCount(rel, this)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                maxRowCountHandler = revise(BuiltInMetadata.MaxRowCount.Handler::class.java)
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.MinRowCount.getMinRowCount]
     * statistic.
     *
     * @param rel the relational expression
     * @return max row count
     */
    @Nullable
    fun getMinRowCount(rel: RelNode?): Double {
        while (true) {
            try {
                return minRowCountHandler.getMinRowCount(rel, this)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                minRowCountHandler = revise(BuiltInMetadata.MinRowCount.Handler::class.java)
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.CumulativeCost.getCumulativeCost]
     * statistic.
     *
     * @param rel the relational expression
     * @return estimated cost, or null if no reliable estimate can be determined
     */
    @Nullable
    fun getCumulativeCost(rel: RelNode?): RelOptCost {
        while (true) {
            try {
                return cumulativeCostHandler.getCumulativeCost(rel, this)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                cumulativeCostHandler = revise(BuiltInMetadata.CumulativeCost.Handler::class.java)
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.NonCumulativeCost.getNonCumulativeCost]
     * statistic.
     *
     * @param rel the relational expression
     * @return estimated cost, or null if no reliable estimate can be determined
     */
    @Nullable
    fun getNonCumulativeCost(rel: RelNode?): RelOptCost {
        while (true) {
            try {
                return nonCumulativeCostHandler.getNonCumulativeCost(rel, this)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                nonCumulativeCostHandler = revise(BuiltInMetadata.NonCumulativeCost.Handler::class.java)
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.PercentageOriginalRows.getPercentageOriginalRows]
     * statistic.
     *
     * @param rel the relational expression
     * @return estimated percentage (between 0.0 and 1.0), or null if no
     * reliable estimate can be determined
     */
    @Nullable
    fun getPercentageOriginalRows(rel: RelNode?): Double {
        while (true) {
            try {
                val result: Double = percentageOriginalRowsHandler.getPercentageOriginalRows(rel, this)
                return RelMdUtil.validatePercentage(result)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                percentageOriginalRowsHandler = revise(BuiltInMetadata.PercentageOriginalRows.Handler::class.java)
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.ColumnOrigin.getColumnOrigins]
     * statistic.
     *
     * @param rel           the relational expression
     * @param column 0-based ordinal for output column of interest
     * @return set of origin columns, or null if this information cannot be
     * determined (whereas empty set indicates Handler.classinitely no origin columns at
     * all)
     */
    @Nullable
    fun getColumnOrigins(rel: RelNode?, column: Int): Set<RelColumnOrigin> {
        while (true) {
            try {
                return columnOriginHandler.getColumnOrigins(rel, this, column)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                columnOriginHandler = revise(BuiltInMetadata.ColumnOrigin.Handler::class.java)
            }
        }
    }

    /**
     * Determines the origin of a column.
     *
     * @see .getColumnOrigins
     * @param rel the RelNode of the column
     * @param column the offset of the column whose origin we are trying to
     * determine
     *
     * @return the origin of a column
     */
    @Nullable
    fun getColumnOrigin(rel: RelNode?, column: Int): RelColumnOrigin? {
        val origins: Set<RelColumnOrigin> = getColumnOrigins(rel, column)
        return if (origins == null || origins.size() !== 1) {
            null
        } else Iterables.getOnlyElement(origins)
    }

    /**
     * Determines the origin of a column.
     */
    @Nullable
    fun getExpressionLineage(rel: RelNode?, expression: RexNode?): Set<RexNode> {
        while (true) {
            try {
                return expressionLineageHandler.getExpressionLineage(rel, this, expression)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                expressionLineageHandler = revise(BuiltInMetadata.ExpressionLineage.Handler::class.java)
            }
        }
    }

    /**
     * Determines the tables used by a plan.
     */
    @Nullable
    fun getTableReferences(rel: RelNode?): Set<RelTableRef> {
        while (true) {
            try {
                return tableReferencesHandler.getTableReferences(rel, this)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                tableReferencesHandler = revise(BuiltInMetadata.TableReferences.Handler::class.java)
            }
        }
    }

    /**
     * Determines the origin of a [RelNode], provided it maps to a single
     * table, optionally with filtering and projection.
     *
     * @param rel the RelNode
     *
     * @return the table, if the RelNode is a simple table; otherwise null
     */
    @Nullable
    fun getTableOrigin(rel: RelNode): RelOptTable? {
        // Determine the simple origin of the first column in the
        // RelNode.  If it's simple, then that means that the underlying
        // table is also simple, even if the column itself is derived.
        if (rel.getRowType().getFieldCount() === 0) {
            return null
        }
        val colOrigins: Set<RelColumnOrigin> = getColumnOrigins(rel, 0)
        return if (colOrigins == null || colOrigins.size() === 0) {
            null
        } else colOrigins.iterator().next().getOriginTable()
    }

    /**
     * Returns the
     * [BuiltInMetadata.Selectivity.getSelectivity]
     * statistic.
     *
     * @param rel       the relational expression
     * @param predicate predicate whose selectivity is to be estimated against
     * `rel`'s output
     * @return estimated selectivity (between 0.0 and 1.0), or null if no
     * reliable estimate can be determined
     */
    @Nullable
    fun getSelectivity(rel: RelNode?, @Nullable predicate: RexNode?): Double {
        while (true) {
            try {
                val result: Double = selectivityHandler.getSelectivity(rel, this, predicate)
                return RelMdUtil.validatePercentage(result)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                selectivityHandler = revise(BuiltInMetadata.Selectivity.Handler::class.java)
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.UniqueKeys.getUniqueKeys]
     * statistic.
     *
     * @param rel the relational expression
     * @return set of keys, or null if this information cannot be determined
     * (whereas empty set indicates Handler.classinitely no keys at all)
     */
    @Nullable
    fun getUniqueKeys(rel: RelNode?): Set<ImmutableBitSet> {
        return getUniqueKeys(rel, false)
    }

    /**
     * Returns the
     * [BuiltInMetadata.UniqueKeys.getUniqueKeys]
     * statistic.
     *
     * @param rel         the relational expression
     * @param ignoreNulls if true, ignore null values when determining
     * whether the keys are unique
     *
     * @return set of keys, or null if this information cannot be determined
     * (whereas empty set indicates definitely no keys at all)
     */
    @Nullable
    fun getUniqueKeys(
        rel: RelNode?,
        ignoreNulls: Boolean
    ): Set<ImmutableBitSet> {
        while (true) {
            try {
                return uniqueKeysHandler.getUniqueKeys(rel, this, ignoreNulls)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                uniqueKeysHandler = revise(BuiltInMetadata.UniqueKeys.Handler::class.java)
            }
        }
    }

    /**
     * Returns whether the rows of a given relational expression are distinct,
     * optionally ignoring NULL values.
     *
     *
     * This is derived by applying the
     * [BuiltInMetadata.ColumnUniqueness.areColumnsUnique]
     * statistic over all columns. If
     * [BuiltInMetadata.MaxRowCount.getMaxRowCount]
     * is less than or equal to one, we shortcut the process and declare the rows
     * unique.
     *
     * @param rel     the relational expression
     * @param ignoreNulls if true, ignore null values when determining column
     * uniqueness
     *
     * @return whether the rows are unique, or
     * null if not enough information is available to make that determination
     */
    @Nullable
    fun areRowsUnique(rel: RelNode, ignoreNulls: Boolean): Boolean {
        val maxRowCount = getMaxRowCount(rel)
        if (maxRowCount != null && maxRowCount <= 1.0) {
            return true
        }
        val columns: ImmutableBitSet = ImmutableBitSet.range(rel.getRowType().getFieldCount())
        return areColumnsUnique(rel, columns, ignoreNulls)
    }

    /**
     * Returns whether the rows of a given relational expression are distinct.
     *
     *
     * Derived by calling [.areRowsUnique].
     *
     * @param rel     the relational expression
     *
     * @return whether the rows are unique, or
     * null if not enough information is available to make that determination
     */
    @Nullable
    fun areRowsUnique(rel: RelNode): Boolean {
        return areRowsUnique(rel, false)
    }

    /**
     * Returns the
     * [BuiltInMetadata.ColumnUniqueness.areColumnsUnique]
     * statistic.
     *
     * @param rel     the relational expression
     * @param columns column mask representing the subset of columns for which
     * uniqueness will be determined
     *
     * @return true or false depending on whether the columns are unique, or
     * null if not enough information is available to make that determination
     */
    @Nullable
    fun areColumnsUnique(rel: RelNode?, columns: ImmutableBitSet?): Boolean {
        return areColumnsUnique(rel, columns, false)
    }

    /**
     * Returns the
     * [BuiltInMetadata.ColumnUniqueness.areColumnsUnique]
     * statistic.
     *
     * @param rel         the relational expression
     * @param columns     column mask representing the subset of columns for which
     * uniqueness will be determined
     * @param ignoreNulls if true, ignore null values when determining column
     * uniqueness
     * @return true or false depending on whether the columns are unique, or
     * null if not enough information is available to make that determination
     */
    @Nullable
    fun areColumnsUnique(
        rel: RelNode?, columns: ImmutableBitSet?,
        ignoreNulls: Boolean
    ): Boolean {
        while (true) {
            try {
                return columnUniquenessHandler.areColumnsUnique(
                    rel, this, columns,
                    ignoreNulls
                )
            } catch (e: MetadataHandlerProvider.NoHandler) {
                columnUniquenessHandler = revise(BuiltInMetadata.ColumnUniqueness.Handler::class.java)
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.Collation.collations]
     * statistic.
     *
     * @param rel         the relational expression
     * @return List of sorted column combinations, or
     * null if not enough information is available to make that determination
     */
    @Nullable
    fun collations(rel: RelNode?): ImmutableList<RelCollation> {
        while (true) {
            try {
                return collationHandler.collations(rel, this)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                collationHandler = revise(BuiltInMetadata.Collation.Handler::class.java)
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.Distribution.distribution]
     * statistic.
     *
     * @param rel         the relational expression
     * @return List of sorted column combinations, or
     * null if not enough information is available to make that determination
     */
    fun distribution(rel: RelNode?): RelDistribution {
        while (true) {
            try {
                return distributionHandler.distribution(rel, this) ?: return RelDistributions.ANY
            } catch (e: MetadataHandlerProvider.NoHandler) {
                distributionHandler = revise(BuiltInMetadata.Distribution.Handler::class.java)
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.PopulationSize.getPopulationSize]
     * statistic.
     *
     * @param rel      the relational expression
     * @param groupKey column mask representing the subset of columns for which
     * the row count will be determined
     * @return distinct row count for the given groupKey, or null if no reliable
     * estimate can be determined
     */
    @Nullable
    fun getPopulationSize(
        rel: RelNode?,
        groupKey: ImmutableBitSet?
    ): Double? {
        while (true) {
            try {
                val result: Double = populationSizeHandler.getPopulationSize(rel, this, groupKey)
                return RelMdUtil.validateResult(result)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                populationSizeHandler = revise(BuiltInMetadata.PopulationSize.Handler::class.java)
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.Size.averageRowSize]
     * statistic.
     *
     * @param rel      the relational expression
     * @return average size of a row, in bytes, or null if not known
     */
    @Nullable
    fun getAverageRowSize(rel: RelNode?): Double {
        while (true) {
            try {
                return sizeHandler.averageRowSize(rel, this)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                sizeHandler = revise(BuiltInMetadata.Size.Handler::class.java)
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.Size.averageColumnSizes]
     * statistic.
     *
     * @param rel      the relational expression
     * @return a list containing, for each column, the average size of a column
     * value, in bytes. Each value or the entire list may be null if the
     * metadata is not available
     */
    @Nullable
    fun getAverageColumnSizes(rel: RelNode?): List<Double> {
        while (true) {
            try {
                return sizeHandler.averageColumnSizes(rel, this)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                sizeHandler = revise(BuiltInMetadata.Size.Handler::class.java)
            }
        }
    }

    /** As [.getAverageColumnSizes] but
     * never returns a null list, only ever a list of nulls.  */
    fun getAverageColumnSizesNotNull(rel: RelNode): List<Double> {
        @Nullable val averageColumnSizes = getAverageColumnSizes(rel)
        return averageColumnSizes ?: Collections.nCopies(rel.getRowType().getFieldCount(), null)
    }

    /**
     * Returns the
     * [BuiltInMetadata.Parallelism.isPhaseTransition]
     * statistic.
     *
     * @param rel      the relational expression
     * @return whether each physical operator implementing this relational
     * expression belongs to a different process than its inputs, or null if not
     * known
     */
    @Nullable
    fun isPhaseTransition(rel: RelNode?): Boolean {
        while (true) {
            try {
                return parallelismHandler.isPhaseTransition(rel, this)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                parallelismHandler = revise(BuiltInMetadata.Parallelism.Handler::class.java)
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.Parallelism.splitCount]
     * statistic.
     *
     * @param rel      the relational expression
     * @return the number of distinct splits of the data, or null if not known
     */
    @Nullable
    fun splitCount(rel: RelNode?): Integer {
        while (true) {
            try {
                return parallelismHandler.splitCount(rel, this)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                parallelismHandler = revise(BuiltInMetadata.Parallelism.Handler::class.java)
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.Memory.memory]
     * statistic.
     *
     * @param rel      the relational expression
     * @return the expected amount of memory, in bytes, required by a physical
     * operator implementing this relational expression, across all splits,
     * or null if not known
     */
    @Nullable
    fun memory(rel: RelNode?): Double {
        while (true) {
            try {
                return memoryHandler.memory(rel, this)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                memoryHandler = revise(BuiltInMetadata.Memory.Handler::class.java)
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.Memory.cumulativeMemoryWithinPhase]
     * statistic.
     *
     * @param rel      the relational expression
     * @return the cumulative amount of memory, in bytes, required by the
     * physical operator implementing this relational expression, and all other
     * operators within the same phase, across all splits, or null if not known
     */
    @Nullable
    fun cumulativeMemoryWithinPhase(rel: RelNode?): Double {
        while (true) {
            try {
                return memoryHandler.cumulativeMemoryWithinPhase(rel, this)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                memoryHandler = revise(BuiltInMetadata.Memory.Handler::class.java)
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.Memory.cumulativeMemoryWithinPhaseSplit]
     * statistic.
     *
     * @param rel      the relational expression
     * @return the expected cumulative amount of memory, in bytes, required by
     * the physical operator implementing this relational expression, and all
     * operators within the same phase, within each split, or null if not known
     */
    @Nullable
    fun cumulativeMemoryWithinPhaseSplit(rel: RelNode?): Double {
        while (true) {
            try {
                return memoryHandler.cumulativeMemoryWithinPhaseSplit(rel, this)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                memoryHandler = revise(BuiltInMetadata.Memory.Handler::class.java)
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.DistinctRowCount.getDistinctRowCount]
     * statistic.
     *
     * @param rel       the relational expression
     * @param groupKey  column mask representing group by columns
     * @param predicate pre-filtered predicates
     * @return distinct row count for groupKey, filtered by predicate, or null
     * if no reliable estimate can be determined
     */
    @Nullable
    fun getDistinctRowCount(
        rel: RelNode?,
        groupKey: ImmutableBitSet?,
        @Nullable predicate: RexNode?
    ): Double? {
        while (true) {
            try {
                val result: Double = distinctRowCountHandler.getDistinctRowCount(
                    rel, this, groupKey,
                    predicate
                )
                return RelMdUtil.validateResult(result)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                distinctRowCountHandler = revise(BuiltInMetadata.DistinctRowCount.Handler::class.java)
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.Predicates.getPredicates]
     * statistic.
     *
     * @param rel the relational expression
     * @return Predicates that can be pulled above this RelNode
     */
    fun getPulledUpPredicates(rel: RelNode?): RelOptPredicateList {
        while (true) {
            try {
                val result: RelOptPredicateList = predicatesHandler.getPredicates(rel, this)
                return if (result != null) result else RelOptPredicateList.EMPTY
            } catch (e: MetadataHandlerProvider.NoHandler) {
                predicatesHandler = revise(BuiltInMetadata.Predicates.Handler::class.java)
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.AllPredicates.getAllPredicates]
     * statistic.
     *
     * @param rel the relational expression
     * @return All predicates within and below this RelNode
     */
    @Nullable
    fun getAllPredicates(rel: RelNode?): RelOptPredicateList {
        while (true) {
            try {
                return allPredicatesHandler.getAllPredicates(rel, this)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                allPredicatesHandler = revise(BuiltInMetadata.AllPredicates.Handler::class.java)
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.ExplainVisibility.isVisibleInExplain]
     * statistic.
     *
     * @param rel          the relational expression
     * @param explainLevel level of detail
     * @return true for visible, false for invisible; if no metadata is available,
     * defaults to true
     */
    fun isVisibleInExplain(
        rel: RelNode?,
        explainLevel: SqlExplainLevel?
    ): Boolean {
        while (true) {
            try {
                val b: Boolean = explainVisibilityHandler.isVisibleInExplain(
                    rel, this,
                    explainLevel
                )
                return b == null || b
            } catch (e: MetadataHandlerProvider.NoHandler) {
                explainVisibilityHandler = revise(BuiltInMetadata.ExplainVisibility.Handler::class.java)
            }
        }
    }

    /**
     * Returns the
     * [BuiltInMetadata.Distribution.distribution]
     * statistic.
     *
     * @param rel the relational expression
     *
     * @return description of how the rows in the relational expression are
     * physically distributed
     */
    @Nullable
    fun getDistribution(rel: RelNode?): RelDistribution? {
        while (true) {
            try {
                return distributionHandler.distribution(rel, this)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                distributionHandler = revise(BuiltInMetadata.Distribution.Handler::class.java)
            }
        }
    }

    /**
     * Returns the lower bound cost of a RelNode.
     */
    @Nullable
    fun getLowerBoundCost(rel: RelNode?, planner: VolcanoPlanner?): RelOptCost {
        while (true) {
            try {
                return lowerBoundCostHandler.getLowerBoundCost(rel, this, planner)
            } catch (e: MetadataHandlerProvider.NoHandler) {
                lowerBoundCostHandler = revise(BuiltInMetadata.LowerBoundCost.Handler::class.java)
            }
        }
    }

    companion object {
        // An empty prototype. Only initialize on first use.
        private val EMPTY: Supplier<RelMetadataQuery> = Suppliers.memoize { RelMetadataQuery(false) }
        //~ Methods ----------------------------------------------------------------
        /**
         * Returns an instance of RelMetadataQuery. It ensures that cycles do not
         * occur while computing metadata.
         */
        fun instance(): RelMetadataQuery {
            return RelMetadataQuery()
        }
    }
}
