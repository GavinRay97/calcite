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
package org.apache.calcite.materialize

import org.apache.calcite.plan.RelOptTable

/** Edge in the join graph.
 *
 *
 * It is directed: the "parent" must be the "many" side containing the
 * foreign key, and the "target" is the "one" side containing the primary
 * key. For example, EMP  DEPT.
 *
 *
 * When created via
 * [LatticeSpace.addEdge]
 * it is unique within the [LatticeSpace].  */
internal class Step private constructor(
    source: LatticeTable?, target: LatticeTable?,
    keys: List<IntPair>, keyString: String
) : DefaultEdge(source, target) {
    val keys: List<IntPair>

    /** String representation of [.keys]. Computing the string requires a
     * [LatticeSpace], so we pre-compute it before construction.  */
    val keyString: String

    init {
        this.keys = ImmutableList.copyOf(keys)
        this.keyString = Objects.requireNonNull(keyString, "keyString")
        assert(
            IntPair.ORDERING.isStrictlyOrdered(keys) // ordered and unique
        )
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(source, target, keys)
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (this === obj
                || (obj is Step
                && (obj as Step).source.equals(source)
                && (obj as Step).target.equals(target)
                && (obj as Step).keys.equals(keys)))
    }

    @Override
    override fun toString(): String {
        return "Step(" + source.toString() + ", " + target.toString() + "," + keyString + ")"
    }

    fun source(): LatticeTable {
        return source as LatticeTable
    }

    fun target(): LatticeTable {
        return target as LatticeTable
    }

    fun isBackwards(statisticProvider: SqlStatisticProvider): Boolean {
        val sourceTable: RelOptTable = source().t
        val sourceColumns: List<Integer> = IntPair.left(keys)
        val targetTable: RelOptTable = target().t
        val targetColumns: List<Integer> = IntPair.right(keys)
        val noDerivedSourceColumns: Boolean =
            sourceColumns.stream().allMatch { i -> i < sourceTable.getRowType().getFieldCount() }
        val noDerivedTargetColumns: Boolean =
            targetColumns.stream().allMatch { i -> i < targetTable.getRowType().getFieldCount() }
        val forwardForeignKey = (noDerivedSourceColumns
                && noDerivedTargetColumns
                && statisticProvider.isForeignKey(
            sourceTable, sourceColumns,
            targetTable, targetColumns
        )
                && statisticProvider.isKey(targetTable, targetColumns))
        val backwardForeignKey = (noDerivedSourceColumns
                && noDerivedTargetColumns
                && statisticProvider.isForeignKey(
            targetTable, targetColumns,
            sourceTable, sourceColumns
        )
                && statisticProvider.isKey(sourceTable, sourceColumns))
        return if (backwardForeignKey != forwardForeignKey) {
            backwardForeignKey
        } else compare(
            sourceTable,
            sourceColumns,
            targetTable,
            targetColumns
        ) < 0
        // Tie-break if it's a foreign key in neither or both directions
    }

    /** Creates [Step] instances.  */
    internal class Factory @SuppressWarnings("type.argument.type.incompatible") constructor(@UnderInitialization space: LatticeSpace?) :
        AttributedDirectedGraph.AttributedEdgeFactory<LatticeTable?, Step?> {
        @NotOnlyInitialized
        private val space: LatticeSpace

        init {
            this.space = Objects.requireNonNull(space, "space")
        }

        @Override
        fun createEdge(source: LatticeTable?, target: LatticeTable?): Step {
            throw UnsupportedOperationException()
        }

        @Override
        fun createEdge(
            source: LatticeTable?, target: LatticeTable?,
            vararg attributes: Object
        ): Step {
            return create(source, target, attributes[0], space)
        }
    }

    companion object {
        /** Creates a Step.  */
        fun create(
            source: LatticeTable?, target: LatticeTable?,
            keys: List<IntPair>, space: LatticeSpace
        ): Step {
            val b = StringBuilder()
            for (key in keys) {
                b.append(' ')
                    .append(space.fieldName(source, key.source))
                    .append(':')
                    .append(space.fieldName(target, key.target))
            }
            return Step(source, target, keys, b.toString())
        }

        /** Arbitrarily compares (table, columns).  */
        private fun compare(
            table1: RelOptTable, columns1: List<Integer>,
            table2: RelOptTable, columns2: List<Integer>
        ): Int {
            var c: Int = Ordering.natural().< String > lexicographical < String ? > ()
                .compare(table1.getQualifiedName(), table2.getQualifiedName())
            if (c == 0) {
                c = Ordering.natural().< Integer > lexicographical < Integer ? > ()
                    .compare(columns1, columns2)
            }
            return c
        }

        /** Temporary method. We should use (inferred) primary keys to figure out
         * the direction of steps.  */
        @SuppressWarnings("unused")
        private fun cardinality(
            statisticProvider: SqlStatisticProvider,
            table: LatticeTable
        ): Double {
            return statisticProvider.tableCardinality(table.t)
        }
    }
}
