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
 * Contains the interfaces for several common forms of metadata.
 */
abstract class BuiltInMetadata {
    /** Metadata about the selectivity of a predicate.  */
    interface Selectivity : Metadata {
        /**
         * Estimates the percentage of an expression's output rows which satisfy a
         * given predicate. Returns null to indicate that no reliable estimate can
         * be produced.
         *
         * @param predicate predicate whose selectivity is to be estimated against
         * rel's output
         * @return estimated selectivity (between 0.0 and 1.0), or null if no
         * reliable estimate can be determined
         */
        @Nullable
        fun getSelectivity(@Nullable predicate: RexNode?): Double?

        /** Handler API.  */
        @FunctionalInterface
        interface Handler : MetadataHandler<Selectivity?> {
            @Nullable
            fun getSelectivity(r: RelNode?, mq: RelMetadataQuery?, @Nullable predicate: RexNode?): Double

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<Selectivity?> = MetadataDef.of(
                Selectivity::class.java,
                Handler::class.java, BuiltInMethod.SELECTIVITY.method
            )
        }
    }

    /** Metadata about which combinations of columns are unique identifiers.  */
    interface UniqueKeys : Metadata {
        /**
         * Determines the set of unique minimal keys for this expression. A key is
         * represented as an [org.apache.calcite.util.ImmutableBitSet], where
         * each bit position represents a 0-based output column ordinal.
         *
         *
         * Nulls can be ignored if the relational expression has filtered out
         * null values.
         *
         * @param ignoreNulls if true, ignore null values when determining
         * whether the keys are unique
         * @return set of keys, or null if this information cannot be determined
         * (whereas empty set indicates definitely no keys at all)
         */
        @Nullable
        fun getUniqueKeys(ignoreNulls: Boolean): Set<ImmutableBitSet?>?

        /** Handler API.  */
        @FunctionalInterface
        interface Handler : MetadataHandler<UniqueKeys?> {
            @Nullable
            fun getUniqueKeys(
                r: RelNode?, mq: RelMetadataQuery?,
                ignoreNulls: Boolean
            ): Set<ImmutableBitSet>

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<UniqueKeys?> = MetadataDef.of(
                UniqueKeys::class.java,
                Handler::class.java, BuiltInMethod.UNIQUE_KEYS.method
            )
        }
    }

    /** Metadata about whether a set of columns uniquely identifies a row.  */
    interface ColumnUniqueness : Metadata {
        /**
         * Determines whether a specified set of columns from a specified relational
         * expression are unique.
         *
         *
         * For example, if the relational expression is a `TableScan` to
         * T(A, B, C, D) whose key is (A, B), then:
         *
         *  * `areColumnsUnique([0, 1])` yields true,
         *  * `areColumnsUnique([0])` yields false,
         *  * `areColumnsUnique([0, 2])` yields false.
         *
         *
         *
         * Nulls can be ignored if the relational expression has filtered out
         * null values.
         *
         * @param columns column mask representing the subset of columns for which
         * uniqueness will be determined
         * @param ignoreNulls if true, ignore null values when determining column
         * uniqueness
         * @return whether the columns are unique, or
         * null if not enough information is available to make that determination
         */
        fun areColumnsUnique(columns: ImmutableBitSet?, ignoreNulls: Boolean): Boolean?

        /** Handler API.  */
        @FunctionalInterface
        interface Handler : MetadataHandler<ColumnUniqueness?> {
            fun areColumnsUnique(
                r: RelNode?, mq: RelMetadataQuery?,
                columns: ImmutableBitSet?, ignoreNulls: Boolean
            ): Boolean

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<ColumnUniqueness?> = MetadataDef.of(
                ColumnUniqueness::class.java,
                Handler::class.java, BuiltInMethod.COLUMN_UNIQUENESS.method
            )
        }
    }

    /** Metadata about which columns are sorted.  */
    interface Collation : Metadata {
        /** Determines which columns are sorted.  */
        fun collations(): ImmutableList<RelCollation?>?

        /** Handler API.  */
        @FunctionalInterface
        interface Handler : MetadataHandler<Collation?> {
            fun collations(r: RelNode?, mq: RelMetadataQuery?): ImmutableList<RelCollation?>?

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<Collation?> = MetadataDef.of(
                Collation::class.java,
                Handler::class.java, BuiltInMethod.COLLATIONS.method
            )
        }
    }

    /** Metadata about how a relational expression is distributed.
     *
     *
     * If you are an operator consuming a relational expression, which subset
     * of the rows are you seeing? You might be seeing all of them (BROADCAST
     * or SINGLETON), only those whose key column values have a particular hash
     * code (HASH) or only those whose column values have particular values or
     * ranges of values (RANGE).
     *
     *
     * When a relational expression is partitioned, it is often partitioned
     * among nodes, but it may be partitioned among threads running on the same
     * node.  */
    interface Distribution : Metadata {
        /** Determines how the rows are distributed.  */
        fun distribution(): RelDistribution?

        /** Handler API.  */
        @FunctionalInterface
        interface Handler : MetadataHandler<Distribution?> {
            fun distribution(r: RelNode?, mq: RelMetadataQuery?): RelDistribution?

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<Distribution?> = MetadataDef.of(
                Distribution::class.java,
                Handler::class.java, BuiltInMethod.DISTRIBUTION.method
            )
        }
    }

    /**
     * Metadata about the node types in a relational expression.
     *
     *
     * For each relational expression, it returns a multimap from the class
     * to the nodes instantiating that class. Each node will appear in the
     * multimap only once.
     */
    interface NodeTypes : Metadata {
        /**
         * Returns a multimap from the class to the nodes instantiating that
         * class. The default implementation for a node classifies it as a
         * [RelNode].
         */
        @get:Nullable
        val nodeTypes: Multimap<Class<out RelNode?>?, RelNode?>?

        /** Handler API.  */
        @FunctionalInterface
        interface Handler : MetadataHandler<NodeTypes?> {
            @Nullable
            fun getNodeTypes(
                r: RelNode?,
                mq: RelMetadataQuery?
            ): Multimap<Class<out RelNode?>?, RelNode?>?

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<NodeTypes?> = MetadataDef.of(
                NodeTypes::class.java,
                Handler::class.java, BuiltInMethod.NODE_TYPES.method
            )
        }
    }

    /** Metadata about the number of rows returned by a relational expression.  */
    interface RowCount : Metadata {
        /**
         * Estimates the number of rows which will be returned by a relational
         * expression. The default implementation for this query asks the rel itself
         * via [RelNode.estimateRowCount], but metadata providers can override this
         * with their own cost models.
         *
         * @return estimated row count, or null if no reliable estimate can be
         * determined
         */
        @get:Nullable
        val rowCount: Double?

        /** Handler API.  */
        @FunctionalInterface
        interface Handler : MetadataHandler<RowCount?> {
            @Nullable
            fun getRowCount(r: RelNode?, mq: RelMetadataQuery?): Double

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<RowCount?> = MetadataDef.of(
                RowCount::class.java,
                Handler::class.java, BuiltInMethod.ROW_COUNT.method
            )
        }
    }

    /** Metadata about the maximum number of rows returned by a relational
     * expression.  */
    interface MaxRowCount : Metadata {
        /**
         * Estimates the max number of rows which will be returned by a relational
         * expression.
         *
         *
         * The default implementation for this query returns
         * [Double.POSITIVE_INFINITY],
         * but metadata providers can override this with their own cost models.
         *
         * @return upper bound on the number of rows returned
         */
        @get:Nullable
        val maxRowCount: Double?

        /** Handler API.  */
        @FunctionalInterface
        interface Handler : MetadataHandler<MaxRowCount?> {
            @Nullable
            fun getMaxRowCount(r: RelNode?, mq: RelMetadataQuery?): Double

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<MaxRowCount?> = MetadataDef.of(
                MaxRowCount::class.java,
                Handler::class.java, BuiltInMethod.MAX_ROW_COUNT.method
            )
        }
    }

    /** Metadata about the minimum number of rows returned by a relational
     * expression.  */
    interface MinRowCount : Metadata {
        /**
         * Estimates the minimum number of rows which will be returned by a
         * relational expression.
         *
         *
         * The default implementation for this query returns 0,
         * but metadata providers can override this with their own cost models.
         *
         * @return lower bound on the number of rows returned
         */
        @get:Nullable
        val minRowCount: Double?

        /** Handler API.  */
        @FunctionalInterface
        interface Handler : MetadataHandler<MinRowCount?> {
            @Nullable
            fun getMinRowCount(r: RelNode?, mq: RelMetadataQuery?): Double

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<MinRowCount?> = MetadataDef.of(
                MinRowCount::class.java,
                Handler::class.java, BuiltInMethod.MIN_ROW_COUNT.method
            )
        }
    }

    /** Metadata about the number of distinct rows returned by a set of columns
     * in a relational expression.  */
    interface DistinctRowCount : Metadata {
        /**
         * Estimates the number of rows which would be produced by a GROUP BY on the
         * set of columns indicated by groupKey, where the input to the GROUP BY has
         * been pre-filtered by predicate. This quantity (leaving out predicate) is
         * often referred to as cardinality (as in gender being a "low-cardinality
         * column").
         *
         * @param groupKey  column mask representing group by columns
         * @param predicate pre-filtered predicates
         * @return distinct row count for groupKey, filtered by predicate, or null
         * if no reliable estimate can be determined
         */
        @Nullable
        fun getDistinctRowCount(groupKey: ImmutableBitSet?, @Nullable predicate: RexNode?): Double?

        /** Handler API.  */
        @FunctionalInterface
        interface Handler : MetadataHandler<DistinctRowCount?> {
            @Nullable
            fun getDistinctRowCount(
                r: RelNode?, mq: RelMetadataQuery?,
                groupKey: ImmutableBitSet?, @Nullable predicate: RexNode?
            ): Double

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<DistinctRowCount?> = MetadataDef.of(
                DistinctRowCount::class.java,
                Handler::class.java, BuiltInMethod.DISTINCT_ROW_COUNT.method
            )
        }
    }

    /** Metadata about the proportion of original rows that remain in a relational
     * expression.  */
    interface PercentageOriginalRows : Metadata {
        /**
         * Estimates the percentage of the number of rows actually produced by a
         * relational expression out of the number of rows it would produce if all
         * single-table filter conditions were removed.
         *
         * @return estimated percentage (between 0.0 and 1.0), or null if no
         * reliable estimate can be determined
         */
        @get:Nullable
        val percentageOriginalRows: Double?

        /** Handler API.  */
        @FunctionalInterface
        interface Handler : MetadataHandler<PercentageOriginalRows?> {
            @Nullable
            fun getPercentageOriginalRows(r: RelNode?, mq: RelMetadataQuery?): Double

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<PercentageOriginalRows?> = MetadataDef.of(
                PercentageOriginalRows::class.java,
                Handler::class.java,
                BuiltInMethod.PERCENTAGE_ORIGINAL_ROWS.method
            )
        }
    }

    /** Metadata about the number of distinct values in the original source of a
     * column or set of columns.  */
    interface PopulationSize : Metadata {
        /**
         * Estimates the distinct row count in the original source for the given
         * `groupKey`, ignoring any filtering being applied by the expression.
         * Typically, "original source" means base table, but for derived columns,
         * the estimate may come from a non-leaf rel such as a LogicalProject.
         *
         * @param groupKey column mask representing the subset of columns for which
         * the row count will be determined
         * @return distinct row count for the given groupKey, or null if no reliable
         * estimate can be determined
         */
        @Nullable
        fun getPopulationSize(groupKey: ImmutableBitSet?): Double?

        /** Handler API.  */
        @FunctionalInterface
        interface Handler : MetadataHandler<PopulationSize?> {
            @Nullable
            fun getPopulationSize(
                r: RelNode?, mq: RelMetadataQuery?,
                groupKey: ImmutableBitSet?
            ): Double

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<PopulationSize?> = MetadataDef.of(
                PopulationSize::class.java,
                Handler::class.java, BuiltInMethod.POPULATION_SIZE.method
            )
        }
    }

    /** Metadata about the size of rows and columns.  */
    interface Size : Metadata {
        /**
         * Determines the average size (in bytes) of a row from this relational
         * expression.
         *
         * @return average size of a row, in bytes, or null if not known
         */
        @Nullable
        fun averageRowSize(): Double?

        /**
         * Determines the average size (in bytes) of a value of a column in this
         * relational expression.
         *
         *
         * Null values are included (presumably they occupy close to 0 bytes).
         *
         *
         * It is left to the caller to decide whether the size is the compressed
         * size, the uncompressed size, or memory allocation when the value is
         * wrapped in an object in the Java heap. The uncompressed size is probably
         * a good compromise.
         *
         * @return an immutable list containing, for each column, the average size
         * of a column value, in bytes. Each value or the entire list may be null if
         * the metadata is not available
         */
        fun averageColumnSizes(): List<Double?>?

        /** Handler API.  */
        interface Handler : MetadataHandler<Size?> {
            @Nullable
            fun averageRowSize(r: RelNode?, mq: RelMetadataQuery?): Double

            @Nullable
            fun averageColumnSizes(r: RelNode?, mq: RelMetadataQuery?): List<Double>

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<Size?> = MetadataDef.of(
                Size::class.java, Handler::class.java,
                BuiltInMethod.AVERAGE_ROW_SIZE.method,
                BuiltInMethod.AVERAGE_COLUMN_SIZES.method
            )
        }
    }

    /** Metadata about the origins of columns.  */
    interface ColumnOrigin : Metadata {
        /**
         * For a given output column of an expression, determines all columns of
         * underlying tables which contribute to result values. An output column may
         * have more than one origin due to expressions such as Union and
         * LogicalProject. The optimizer may use this information for catalog access
         * (e.g. index availability).
         *
         * @param outputColumn 0-based ordinal for output column of interest
         * @return set of origin columns, or null if this information cannot be
         * determined (whereas empty set indicates definitely no origin columns at
         * all)
         */
        @Nullable
        fun getColumnOrigins(outputColumn: Int): Set<RelColumnOrigin?>?

        /** Handler API.  */
        @FunctionalInterface
        interface Handler : MetadataHandler<ColumnOrigin?> {
            @Nullable
            fun getColumnOrigins(
                r: RelNode?, mq: RelMetadataQuery?,
                outputColumn: Int
            ): Set<RelColumnOrigin>

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<ColumnOrigin?> = MetadataDef.of(
                ColumnOrigin::class.java,
                Handler::class.java, BuiltInMethod.COLUMN_ORIGIN.method
            )
        }
    }

    /** Metadata about the origins of expressions.  */
    interface ExpressionLineage : Metadata {
        /**
         * Given the input expression applied on the given [RelNode], this
         * provider returns the expression with its lineage resolved.
         *
         *
         * In particular, the result will be a set of nodes which might contain
         * references to columns in TableScan operators ([RexTableInputRef]).
         * An expression can have more than one lineage expression due to Union
         * operators. However, we do not check column equality in Filter predicates.
         * Each TableScan operator below the node is identified uniquely by its
         * qualified name and its entity number.
         *
         *
         * For example, if the expression is `$0 + 2` and `$0` originated
         * from column `$3` in the `0` occurrence of table `A` in the
         * plan, result will be: `A.#0.$3 + 2`. Occurrences are generated in no
         * particular order, but it is guaranteed that if two expressions referred to the
         * same table, the qualified name + occurrence will be the same.
         *
         * @param expression expression whose lineage we want to resolve
         *
         * @return set of expressions with lineage resolved, or null if this information
         * cannot be determined (e.g. origin of an expression is an aggregation
         * in an [org.apache.calcite.rel.core.Aggregate] operator)
         */
        @Nullable
        fun getExpressionLineage(expression: RexNode?): Set<RexNode?>?

        /** Handler API.  */
        @FunctionalInterface
        interface Handler : MetadataHandler<ExpressionLineage?> {
            @Nullable
            fun getExpressionLineage(
                r: RelNode?, mq: RelMetadataQuery?,
                expression: RexNode?
            ): Set<RexNode>

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<ExpressionLineage?> = MetadataDef.of(
                ExpressionLineage::class.java,
                Handler::class.java, BuiltInMethod.EXPRESSION_LINEAGE.method
            )
        }
    }

    /** Metadata to obtain references to tables used by a given expression.  */
    interface TableReferences : Metadata {
        /**
         * This provider returns the tables used by a given plan.
         *
         *
         * In particular, the result will be a set of unique table references
         * ([RelTableRef]) corresponding to each TableScan operator in the
         * plan. These table references are composed by the table qualified name
         * and an entity number.
         *
         *
         * Importantly, the table identifiers returned by this metadata provider
         * will be consistent with the unique identifiers used by the [ExpressionLineage]
         * provider, meaning that it is guaranteed that same table will use same unique
         * identifiers in both.
         *
         * @return set of unique table identifiers, or null if this information
         * cannot be determined
         */
        val tableReferences: Set<Any?>?

        /** Handler API.  */
        @FunctionalInterface
        interface Handler : MetadataHandler<TableReferences?> {
            fun getTableReferences(r: RelNode?, mq: RelMetadataQuery?): Set<RelTableRef>

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<TableReferences?> = MetadataDef.of(
                TableReferences::class.java,
                Handler::class.java, BuiltInMethod.TABLE_REFERENCES.method
            )
        }
    }

    /** Metadata about the cost of evaluating a relational expression, including
     * all of its inputs.  */
    interface CumulativeCost : Metadata {
        /**
         * Estimates the cost of executing a relational expression, including the
         * cost of its inputs. The default implementation for this query adds
         * [NonCumulativeCost.getNonCumulativeCost] to the cumulative cost of
         * each input, but metadata providers can override this with their own cost
         * models, e.g. to take into account interactions between expressions.
         *
         * @return estimated cost, or null if no reliable estimate can be
         * determined
         */
        val cumulativeCost: RelOptCost?

        /** Handler API.  */
        @FunctionalInterface
        interface Handler : MetadataHandler<CumulativeCost?> {
            fun getCumulativeCost(r: RelNode?, mq: RelMetadataQuery?): RelOptCost

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<CumulativeCost?> = MetadataDef.of(
                CumulativeCost::class.java,
                Handler::class.java, BuiltInMethod.CUMULATIVE_COST.method
            )
        }
    }

    /** Metadata about the cost of evaluating a relational expression, not
     * including its inputs.  */
    interface NonCumulativeCost : Metadata {
        /**
         * Estimates the cost of executing a relational expression, not counting the
         * cost of its inputs. (However, the non-cumulative cost is still usually
         * dependent on the row counts of the inputs.)
         *
         *
         * The default implementation for this query asks the rel itself via
         * [RelNode.computeSelfCost],
         * but metadata providers can override this with their own cost models.
         *
         * @return estimated cost, or null if no reliable estimate can be
         * determined
         */
        val nonCumulativeCost: RelOptCost?

        /** Handler API.  */
        @FunctionalInterface
        interface Handler : MetadataHandler<NonCumulativeCost?> {
            fun getNonCumulativeCost(r: RelNode?, mq: RelMetadataQuery?): RelOptCost

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<NonCumulativeCost?> = MetadataDef.of(
                NonCumulativeCost::class.java,
                Handler::class.java,
                BuiltInMethod.NON_CUMULATIVE_COST.method
            )
        }
    }

    /** Metadata about whether a relational expression should appear in a plan.  */
    interface ExplainVisibility : Metadata {
        /**
         * Determines whether a relational expression should be visible in EXPLAIN
         * PLAN output at a particular level of detail.
         *
         * @param explainLevel level of detail
         * @return true for visible, false for invisible
         */
        fun isVisibleInExplain(explainLevel: SqlExplainLevel?): Boolean?

        /** Handler API.  */
        @FunctionalInterface
        interface Handler : MetadataHandler<ExplainVisibility?> {
            fun isVisibleInExplain(
                r: RelNode?, mq: RelMetadataQuery?,
                explainLevel: SqlExplainLevel?
            ): Boolean?

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<ExplainVisibility?> = MetadataDef.of(
                ExplainVisibility::class.java,
                Handler::class.java,
                BuiltInMethod.EXPLAIN_VISIBILITY.method
            )
        }
    }

    /** Metadata about the predicates that hold in the rows emitted from a
     * relational expression.  */
    interface Predicates : Metadata {
        /**
         * Derives the predicates that hold on rows emitted from a relational
         * expression.
         *
         * @return Predicate list
         */
        val predicates: RelOptPredicateList?

        /** Handler API.  */
        @FunctionalInterface
        interface Handler : MetadataHandler<Predicates?> {
            fun getPredicates(r: RelNode?, mq: RelMetadataQuery?): RelOptPredicateList?

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<Predicates?> = MetadataDef.of(
                Predicates::class.java,
                Handler::class.java, BuiltInMethod.PREDICATES.method
            )
        }
    }

    /** Metadata about the predicates that hold in the rows emitted from a
     * relational expression.
     *
     *
     * The difference with respect to [Predicates] provider is that
     * this provider tries to extract ALL predicates even if they are not
     * applied on the output expressions of the relational expression; we rely
     * on [RexTableInputRef] to reference origin columns in
     * [org.apache.calcite.rel.core.TableScan] for the result predicates.
     */
    interface AllPredicates : Metadata {
        /**
         * Derives the predicates that hold on rows emitted from a relational
         * expression.
         *
         * @return predicate list, or null if the provider cannot infer the
         * lineage for any of the expressions contained in any of the predicates
         */
        @get:Nullable
        val allPredicates: RelOptPredicateList?

        /** Handler API.  */
        @FunctionalInterface
        interface Handler : MetadataHandler<AllPredicates?> {
            @Nullable
            fun getAllPredicates(r: RelNode?, mq: RelMetadataQuery?): RelOptPredicateList

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<AllPredicates?> = MetadataDef.of(
                AllPredicates::class.java,
                Handler::class.java, BuiltInMethod.ALL_PREDICATES.method
            )
        }
    }

    /** Metadata about the degree of parallelism of a relational expression, and
     * how its operators are assigned to processes with independent resource
     * pools.  */
    interface Parallelism : Metadata {
        /** Returns whether each physical operator implementing this relational
         * expression belongs to a different process than its inputs.
         *
         *
         * A collection of operators processing all of the splits of a particular
         * stage in the query pipeline is called a "phase". A phase starts with
         * a leaf node such as a [org.apache.calcite.rel.core.TableScan],
         * or with a phase-change node such as an
         * [org.apache.calcite.rel.core.Exchange]. Hadoop's shuffle operator
         * (a form of sort-exchange) causes data to be sent across the network.  */
        val isPhaseTransition: Boolean?

        /** Returns the number of distinct splits of the data.
         *
         *
         * Note that splits must be distinct. For broadcast, where each copy is
         * the same, returns 1.
         *
         *
         * Thus the split count is the *proportion* of the data seen by
         * each operator instance.
         */
        fun splitCount(): Integer?

        /** Handler API.  */
        interface Handler : MetadataHandler<Parallelism?> {
            fun isPhaseTransition(r: RelNode?, mq: RelMetadataQuery?): Boolean
            fun splitCount(r: RelNode?, mq: RelMetadataQuery?): Integer

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<Parallelism?> = MetadataDef.of(
                Parallelism::class.java,
                Handler::class.java, BuiltInMethod.IS_PHASE_TRANSITION.method,
                BuiltInMethod.SPLIT_COUNT.method
            )
        }
    }

    /** Metadata to get the lower bound cost of a RelNode.  */
    interface LowerBoundCost : Metadata {
        /** Returns the lower bound cost of a RelNode.  */
        fun getLowerBoundCost(planner: VolcanoPlanner?): RelOptCost?

        /** Handler API.  */
        @FunctionalInterface
        interface Handler : MetadataHandler<LowerBoundCost?> {
            fun getLowerBoundCost(
                r: RelNode?, mq: RelMetadataQuery?, planner: VolcanoPlanner?
            ): RelOptCost

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<LowerBoundCost?> = MetadataDef.of(
                LowerBoundCost::class.java,
                Handler::class.java, BuiltInMethod.LOWER_BOUND_COST.method
            )
        }
    }

    /** Metadata about the memory use of an operator.  */
    interface Memory : Metadata {
        /** Returns the expected amount of memory, in bytes, required by a physical
         * operator implementing this relational expression, across all splits.
         *
         *
         * How much memory is used depends very much on the algorithm; for
         * example, an implementation of
         * [org.apache.calcite.rel.core.Aggregate] that loads all data into a
         * hash table requires approximately `rowCount * averageRowSize`
         * bytes, whereas an implementation that assumes that the input is sorted
         * requires only `averageRowSize` bytes to maintain a single
         * accumulator for each aggregate function.
         */
        @Nullable
        fun memory(): Double?

        /** Returns the cumulative amount of memory, in bytes, required by the
         * physical operator implementing this relational expression, and all other
         * operators within the same phase, across all splits.
         *
         * @see Parallelism.splitCount
         */
        @Nullable
        fun cumulativeMemoryWithinPhase(): Double?

        /** Returns the expected cumulative amount of memory, in bytes, required by
         * the physical operator implementing this relational expression, and all
         * operators within the same phase, within each split.
         *
         *
         * Basic formula:
         *
         * <blockquote>cumulativeMemoryWithinPhaseSplit
         * = cumulativeMemoryWithinPhase / Parallelism.splitCount</blockquote>
         */
        @Nullable
        fun cumulativeMemoryWithinPhaseSplit(): Double?

        /** Handler API.  */
        interface Handler : MetadataHandler<Memory?> {
            @Nullable
            fun memory(r: RelNode?, mq: RelMetadataQuery?): Double

            @Nullable
            fun cumulativeMemoryWithinPhase(r: RelNode?, mq: RelMetadataQuery?): Double

            @Nullable
            fun cumulativeMemoryWithinPhaseSplit(r: RelNode?, mq: RelMetadataQuery?): Double

            @get:Override
            override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
                get() = DEF
        }

        companion object {
            val DEF: MetadataDef<Memory?> = MetadataDef.of(
                Memory::class.java,
                Handler::class.java, BuiltInMethod.MEMORY.method,
                BuiltInMethod.CUMULATIVE_MEMORY_WITHIN_PHASE.method,
                BuiltInMethod.CUMULATIVE_MEMORY_WITHIN_PHASE_SPLIT.method
            )
        }
    }

    /** The built-in forms of metadata.  */
    internal interface All : Selectivity, UniqueKeys, RowCount, DistinctRowCount, PercentageOriginalRows,
        ColumnUniqueness, ColumnOrigin, Predicates, Collation, Distribution, Size, Parallelism, Memory, AllPredicates,
        ExpressionLineage, TableReferences, NodeTypes
}
