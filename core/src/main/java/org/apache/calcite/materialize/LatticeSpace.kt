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

/** Space within which lattices exist.  */
class LatticeSpace(statisticProvider: SqlStatisticProvider?) {
    val statisticProvider: SqlStatisticProvider
    private val tableMap: Map<List<String>, LatticeTable> = HashMap()

    @SuppressWarnings("assignment.type.incompatible")
    @NotOnlyInitialized
    val g: AttributedDirectedGraph<LatticeTable, Step> = AttributedDirectedGraph(Factory(this))
    private val simpleTableNames: Map<List<String>, String> = HashMap()
    private val simpleNames: Set<String> = HashSet()

    /** Root nodes, indexed by digest.  */
    val nodeMap: Map<String, LatticeRootNode> = HashMap()
    val pathMap: Map<ImmutableList<Step>, Path> = HashMap()
    val tableExpressions: Map<LatticeTable, List<RexNode>> = HashMap()

    init {
        this.statisticProvider = Objects.requireNonNull(statisticProvider, "statisticProvider")
    }

    /** Derives a unique name for a table, qualifying with schema name only if
     * necessary.  */
    fun simpleName(table: LatticeTable): String {
        return simpleName(table.t.getQualifiedName())
    }

    fun simpleName(table: RelOptTable): String {
        return simpleName(table.getQualifiedName())
    }

    fun simpleName(table: List<String>): String {
        val name = simpleTableNames[table]
        if (name != null) {
            return name
        }
        val name2: String = Util.last(table)
        if (simpleNames.add(name2)) {
            simpleTableNames.put(ImmutableList.copyOf(table), name2)
            return name2
        }
        val name3 = table.toString()
        simpleTableNames.put(ImmutableList.copyOf(table), name3)
        return name3
    }

    fun register(t: RelOptTable): LatticeTable {
        val table: LatticeTable? = tableMap[t.getQualifiedName()]
        if (table != null) {
            return table
        }
        val table2 = LatticeTable(t)
        tableMap.put(t.getQualifiedName(), table2)
        g.addVertex(table2)
        return table2
    }

    fun addEdge(source: LatticeTable?, target: LatticeTable?, keys: List<IntPair?>): Step? {
        var keys: List<IntPair?> = keys
        keys = sortUnique(keys)
        val step: Step = g.addEdge(source, target, keys)
        if (step != null) {
            return step
        }
        for (step2 in g.getEdges(source, target)) {
            if (step2.keys.equals(keys)) {
                return step2
            }
        }
        throw AssertionError("addEdge failed, yet no edge present")
    }

    fun addPath(steps: List<Step?>?): Path {
        val key: ImmutableList<Step> = ImmutableList.copyOf(steps)
        val path: Path? = pathMap[key]
        if (path != null) {
            return path
        }
        val path2 = Path(key, pathMap.size())
        pathMap.put(key, path2)
        return path2
    }

    /** Registers an expression as a derived column of a given table.
     *
     *
     * Its ordinal is the number of fields in the row type plus the ordinal
     * of the extended expression. For example, if a table has 10 fields then its
     * derived columns will have ordinals 10, 11, 12 etc.  */
    fun registerExpression(table: LatticeTable, e: RexNode): Int {
        val expressions: List<RexNode> = tableExpressions.computeIfAbsent(table) { t -> ArrayList() }
        val fieldCount: Int = table.t.getRowType().getFieldCount()
        for (i in 0 until expressions.size()) {
            if (expressions[i].toString().equals(e.toString())) {
                return fieldCount + i
            }
        }
        val result: Int = fieldCount + expressions.size()
        expressions.add(e)
        return result
    }

    /** Returns the name of field `field` of `table`.
     *
     *
     * If the field is derived (see
     * [.registerExpression]) its name is its
     * [RexNode.toString].  */
    fun fieldName(table: LatticeTable, field: Int): String {
        val fieldList: List<RelDataTypeField> = table.t.getRowType().getFieldList()
        val fieldCount: Int = fieldList.size()
        return if (field < fieldCount) {
            fieldList[field].getName()
        } else {
            val rexNodes: List<RexNode>? = tableExpressions[table]
            assert(rexNodes != null) { "no expressions found for table $table" }
            rexNodes!![field - fieldCount].toString()
        }
    }

    companion object {
        /** Returns a list of [IntPair] that is sorted and unique.  */
        fun sortUnique(keys: List<IntPair?>): List<IntPair?> {
            var keys: List<IntPair?> = keys
            if (keys.size() > 1) {
                // list may not be sorted; sort it
                keys = IntPair.ORDERING.immutableSortedCopy(keys)
                if (!IntPair.ORDERING.isStrictlyOrdered(keys)) {
                    // list may contain duplicates; sort and eliminate duplicates
                    val set: Set<IntPair> = TreeSet(IntPair.ORDERING)
                    set.addAll(keys)
                    keys = ImmutableList.copyOf(set)
                }
            }
            return keys
        }

        /** Returns a list of [IntPair], transposing source and target fields,
         * and ensuring the result is sorted and unique.  */
        fun swap(keys: List<IntPair?>?): List<IntPair?> {
            return sortUnique(Util.transform(keys) { x -> IntPair.of(x.target, x.source) })
        }
    }
}
