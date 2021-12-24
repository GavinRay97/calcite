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
package org.apache.calcite.tools

import org.apache.calcite.linq4j.Ord

/**
 * Extension to [RelBuilder] for Pig relational operators.
 */
class PigRelBuilder protected constructor(
    context: Context?,
    cluster: RelOptCluster,
    @Nullable relOptSchema: RelOptSchema
) : RelBuilder(context, cluster, relOptSchema) {
    @Nullable
    private var lastAlias: String? = null

    @Override
    override fun scan(vararg tableNames: String?): PigRelBuilder {
        lastAlias = null
        return super.scan(tableNames) as PigRelBuilder
    }

    @Override
    override fun scan(tableNames: Iterable<String?>?): PigRelBuilder {
        lastAlias = null
        return super.scan(tableNames) as PigRelBuilder
    }

    /** Loads a data set.
     *
     *
     * Equivalent to Pig Latin:
     * <pre>`LOAD 'path' USING loadFunction AS rowType`</pre>
     *
     *
     * `loadFunction` and `rowType` are optional.
     *
     * @param path File path
     * @param loadFunction Load function
     * @param rowType Row type (what Pig calls 'schema')
     *
     * @return This builder
     */
    fun load(
        path: String, loadFunction: RexNode?,
        rowType: RelDataType?
    ): PigRelBuilder {
        scan(path.replace(".csv", "")) // TODO: use a UDT
        return this
    }

    /** Removes duplicate tuples in a relation.
     *
     *
     * Equivalent Pig Latin:
     * <blockquote>
     * <pre>alias = DISTINCT alias [PARTITION BY partitioner] [PARALLEL n];</pre>
    </blockquote> *
     *
     * @param partitioner Partitioner; null means no partitioner
     * @param parallel Degree of parallelism; negative means unspecified
     *
     * @return This builder
     */
    fun distinct(partitioner: Partitioner?, parallel: Int): PigRelBuilder {
        // TODO: Use partitioner and parallel
        distinct()
        return this
    }

    /** Groups the data in one or more relations.
     *
     *
     * Pig Latin syntax:
     * <blockquote>
     * alias = GROUP alias { ALL | BY expression }
     * [, alias ALL | BY expression ...]
     * [USING 'collected' | 'merge'] [PARTITION BY partitioner] [PARALLEL n];
    </blockquote> *
     *
     * @param groupKeys One of more group keys; use [.groupKey] for ALL
     * @param option Whether to use an optimized method combining the data
     * (COLLECTED for one input or MERGE for two or more inputs)
     * @param partitioner Partitioner; null means no partitioner
     * @param parallel Degree of parallelism; negative means unspecified
     *
     * @return This builder
     */
    fun group(
        option: GroupOption?, partitioner: Partitioner?,
        parallel: Int, vararg groupKeys: GroupKey?
    ): PigRelBuilder {
        return group(option, partitioner, parallel, ImmutableList.copyOf(groupKeys))
    }

    fun group(
        option: GroupOption?, partitioner: Partitioner?,
        parallel: Int, groupKeys: Iterable<GroupKey?>?
    ): PigRelBuilder {
        val groupKeyList: List<GroupKey> = ImmutableList.copyOf(groupKeys)
        validateGroupList(groupKeyList)
        val groupCount: Int = groupKeyList[0].groupKeyCount()
        val n: Int = groupKeyList.size()
        for (groupKey in Ord.reverse(groupKeyList)) {
            var r: RelNode? = null
            if (groupKey.i < n - 1) {
                r = build()
            }
            // Create a ROW to pass to COLLECT. Interestingly, this is not allowed
            // by standard SQL; see [CALCITE-877] Allow ROW as argument to COLLECT.
            val row: RexNode = cluster.getRexBuilder().makeCall(
                peek(1, 0).getRowType(),
                SqlStdOperatorTable.ROW, fields()
            )
            aggregate(
                groupKey.e,
                aggregateCall(SqlStdOperatorTable.COLLECT, row).`as`(alias)
            )
            if (groupKey.i < n - 1) {
                push(requireNonNull(r, "r"))
                val predicates: List<RexNode> = ArrayList()
                for (key in Util.range(groupCount)) {
                    predicates.add(equals(field(2, 0, key), field(2, 1, key)))
                }
                join(JoinRelType.INNER, and(predicates))
            }
        }
        return this
    }

    protected fun validateGroupList(groupKeyList: List<GroupKey>) {
        if (groupKeyList.isEmpty()) {
            throw IllegalArgumentException("must have at least one group")
        }
        val groupCount: Int = groupKeyList[0].groupKeyCount()
        for (groupKey in groupKeyList) {
            if (groupKey.groupKeyCount() !== groupCount) {
                throw IllegalArgumentException("group key size mismatch")
            }
        }
    }

    @get:Nullable
    val alias: String?
        get() = if (lastAlias != null) {
            lastAlias
        } else {
            val top: RelNode = peek()
            if (top is TableScan) {
                val scan: TableScan = top as TableScan
                Util.last(scan.getTable().getQualifiedName())
            } else {
                null
            }
        }

    /** As super-class method, but also retains alias for naming of aggregates.  */
    @Override
    override fun `as`(alias: String?): RelBuilder {
        lastAlias = alias
        return super.`as`(alias)
    }

    /** Partitioner for group and join.  */
    interface Partitioner

    /** Option for performing group efficiently if data set is already sorted.  */
    enum class GroupOption {
        MERGE, COLLECTED
    }

    companion object {
        /** Creates a PigRelBuilder.  */
        fun create(config: FrameworkConfig): PigRelBuilder {
            val relBuilder: RelBuilder = RelBuilder.create(config)
            return PigRelBuilder(
                config.getContext(), relBuilder.cluster,
                relBuilder.relOptSchema
            )
        }
    }
}
