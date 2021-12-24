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

import org.apache.calcite.jdbc.CalciteSchema

/**
 * Algorithm that suggests a set of lattices.
 */
class LatticeSuggester(config: FrameworkConfig) {
    val space: LatticeSpace

    /** Lattices, indexed by digest. Uses LinkedHashMap for determinacy.  */
    val latticeMap: Map<String, Lattice> = LinkedHashMap()

    /** Lattices that have been made obsolete. Key is the obsolete lattice, value
     * is the lattice that superseded it.  */
    private val obsoleteLatticeMap: Map<Lattice, Lattice> = HashMap()

    /** Whether to try to extend an existing lattice when adding a lattice.  */
    private val evolve: Boolean

    /** Creates a LatticeSuggester.  */
    init {
        evolve = config.isEvolveLattice()
        space = LatticeSpace(config.getStatisticProvider())
    }

    /** Returns the minimal set of lattices necessary to cover all of the queries
     * seen. Any lattices that are subsumed by other lattices are not included.  */
    val latticeSet: Set<org.apache.calcite.materialize.Lattice>
        get() {
            val set: Set<Lattice> = LinkedHashSet(latticeMap.values())
            set.removeAll(obsoleteLatticeMap.keySet())
            return ImmutableSet.copyOf(set)
        }

    /** Converts a column reference to an expression.  */
    fun toRex(table: LatticeTable, column: Int): RexNode {
        val fieldList: List<RelDataTypeField> = table.t.getRowType().getFieldList()
        return if (column < fieldList.size()) {
            RexInputRef(column, fieldList[column].getType())
        } else {
            requireNonNull(
                space.tableExpressions.get(table)
            ) { "space.tableExpressions.get(table) is null for $table" }
                .get(column - fieldList.size())
        }
    }

    /** Adds a query.
     *
     *
     * It may fit within an existing lattice (or lattices). Or it may need a
     * new lattice, or an extension to an existing lattice.
     *
     * @param r Relational expression for a query
     *
     * @return A list of join graphs: usually 1; more if the query contains a
     * cartesian product; zero if the query graph is cyclic
     */
    fun addQuery(r: RelNode?): List<Lattice> {
        // Push filters into joins and towards leaves
        val planner = HepPlanner(PROGRAM, null, true, null, RelOptCostImpl.FACTORY)
        planner.setRoot(r)
        val r2: RelNode = planner.findBestExp()
        val q = Query(space)
        val frameList: List<Frame> = ArrayList()
        frames(frameList, q, r2)
        val lattices: List<Lattice> = ArrayList()
        frameList.forEach { frame -> addFrame(q, frame, lattices) }
        return ImmutableList.copyOf(lattices)
    }

    private fun addFrame(q: Query, frame: Frame, lattices: List<Lattice>) {
        val g: AttributedDirectedGraph<TableRef, StepRef> = AttributedDirectedGraph.create(StepRef.Factory())
        val map: Multimap<Pair<TableRef, TableRef>, IntPair> = LinkedListMultimap.create()
        for (tableRef in frame.tableRefs) {
            g.addVertex(tableRef)
        }
        for (hop in frame.hops) {
            map.put(
                Pair.of(hop.source.tableRef(), hop.target.tableRef()),
                IntPair.of(hop.source.col(space), hop.target.col(space))
            )
        }
        for (e in map.asMap().entrySet()) {
            val source: TableRef = e.getKey().left
            val target: TableRef = e.getKey().right
            val stepRef = q.stepRef(source, target, ImmutableList.copyOf(e.getValue()))
            g.addVertex(stepRef.source())
            g.addVertex(stepRef.target())
            g.addEdge(
                stepRef.source(), stepRef.target(), stepRef.step,
                stepRef.ordinalInQuery
            )
        }

        // If the join graph is cyclic, we can't use it.
        val cycles: Set<TableRef> = CycleDetector(g).findCycles()
        if (!cycles.isEmpty()) {
            return
        }

        // Translate the query graph to mutable nodes
        val nodes: IdentityHashMap<TableRef, MutableNode> = IdentityHashMap()
        val nodesByParent: Map<List, MutableNode> = HashMap()
        val rootNodes: List<MutableNode> = ArrayList()
        for (tableRef in TopologicalOrderIterator.of(g)) {
            val edges: List<StepRef> = g.getInwardEdges(tableRef)
            val node: MutableNode?
            when (edges.size()) {
                0 -> {
                    node = MutableNode(tableRef.table)
                    rootNodes.add(node)
                }
                1 -> {
                    val edge = edges[0]
                    val parent: MutableNode = nodes.get(edge.source())
                    val key: List = FlatLists.of(parent, tableRef.table, edge.step.keys)
                    val existingNode: MutableNode? = nodesByParent[key]
                    if (existingNode == null) {
                        node = MutableNode(tableRef.table, parent, edge.step)
                        nodesByParent.put(key, node)
                    } else {
                        node = existingNode
                    }
                }
                else -> {
                    for (edge2 in edges) {
                        val parent2: MutableNode = nodes.get(edge2.source())
                        requireNonNull(
                            parent2
                        ) { "parent for " + edge2.source() }
                        val node2 = MutableNode(tableRef.table, parent2, edge2.step)
                        parent2.children.add(node2)
                    }
                    node = null
                }
            }
            nodes.put(tableRef, node)
        }

        // Transcribe the hierarchy of mutable nodes to immutable nodes
        for (rootNode in rootNodes) {
            if (rootNode.isCyclic()) {
                continue
            }
            val rootSchema: CalciteSchema = CalciteSchema.createRootSchema(false)
            val latticeBuilder: Lattice.Builder = Builder(space, rootSchema, rootNode)
            val flatNodes: List<MutableNode> = ArrayList()
            rootNode.flatten(flatNodes)
            for (measure in frame.measures) {
                for (arg in measure.arguments) {
                    if (arg == null) {
                        // Cannot handle expressions, e.g. "sum(x + 1)" yet
                        return
                    }
                }
                latticeBuilder.addMeasure(
                    Measure(measure.aggregate, measure.distinct,
                        measure.name,
                        Util.transform(measure.arguments) { colRef ->
                            val column: Lattice.Column
                            column = if (colRef is BaseColRef) {
                                val baseColRef = colRef as BaseColRef
                                val node: MutableNode = nodes.get(baseColRef.t)
                                val table = flatNodes.indexOf(node)
                                latticeBuilder.column(table, baseColRef.c)
                            } else if (colRef is DerivedColRef) {
                                val derivedColRef = colRef as DerivedColRef
                                val alias = deriveAlias(measure, derivedColRef)
                                latticeBuilder.expression(
                                    derivedColRef.e, alias,
                                    derivedColRef.tableAliases()
                                )
                            } else {
                                throw AssertionError("expression in measure")
                            }
                            latticeBuilder.use(column, true)
                            column
                        })
                )
            }
            for (i in 0 until frame.columnCount) {
                val c = frame.column(i)
                if (c is DerivedColRef) {
                    val derivedColRef = c
                    val expression: Lattice.Column = latticeBuilder.expression(
                        derivedColRef.e,
                        derivedColRef.alias, derivedColRef.tableAliases()
                    )
                    latticeBuilder.use(expression, false)
                }
            }
            val lattice0: Lattice = latticeBuilder.build()
            val lattice1: Lattice = findMatch(lattice0, rootNode)
            lattices.add(lattice1)
        }
    }

    /** Returns the best match for a lattice. If no match, registers the lattice
     * and returns it. Never returns null.  */
    private fun findMatch(lattice: Lattice, mutableNode: MutableNode): Lattice {
        val lattice1: Lattice? = latticeMap[lattice.toString()]
        if (lattice1 != null) {
            // Exact match for an existing lattice
            return lattice1
        }
        if (evolve) {
            // No exact match. Scan existing lattices for a sub-set.
            var bestMatchQuality = 0
            var bestMatch: Lattice? = null
            for (lattice2 in latticeMap.values()) {
                val q = matchQuality(lattice2, lattice)
                if (q > bestMatchQuality) {
                    bestMatch = lattice2
                    bestMatchQuality = q
                } else if (q == bestMatchQuality && bestMatch != null && !lattice2.rootNode.paths.equals(bestMatch.rootNode.paths)
                    && lattice2.rootNode.paths.containsAll(bestMatch.rootNode.paths)
                ) {
                    bestMatch = lattice2
                }
            }
            if (bestMatch != null) {
                // Fix up the best batch
                for (path in minus<Any>(bestMatch.rootNode.paths, lattice.rootNode.paths)) {
                    // TODO: assign alias based on node in bestMatch
                    mutableNode.addPath(path, null)
                }
                val rootSchema: CalciteSchema = CalciteSchema.createRootSchema(false)
                val builder: Lattice.Builder = Builder(space, rootSchema, mutableNode)
                copyMeasures(builder, bestMatch)
                copyMeasures(builder, lattice)
                val lattice2: Lattice = builder.build()
                latticeMap.remove(bestMatch.toString())
                obsoleteLatticeMap.put(bestMatch, lattice2)
                latticeMap.put(lattice2.toString(), lattice2)
                return lattice2
            }
        }

        // No suitable existing lattice. Register this one.
        latticeMap.put(lattice.toString(), lattice)
        return lattice
    }

    /** Holds state for a particular query graph. In particular table and step
     * references count from zero each query.  */
    private class Query internal constructor(space: LatticeSpace) {
        val space: LatticeSpace
        val tableRefs: Map<Integer, TableRef> = HashMap()
        var stepRefCount = 0

        init {
            this.space = space
        }

        fun tableRef(scan: TableScan): TableRef {
            val r = tableRefs[scan.getId()]
            if (r != null) {
                return r
            }
            val t: LatticeTable = space.register(scan.getTable())
            val r2 = TableRef(t, tableRefs.size())
            tableRefs.put(scan.getId(), r2)
            return r2
        }

        fun stepRef(source: TableRef, target: TableRef, keys: List<IntPair?>): StepRef {
            var keys: List<IntPair?> = keys
            keys = LatticeSpace.sortUnique(keys)
            val h: Step = Step.create(source.table, target.table, keys, space)
            return if (h.isBackwards(space.statisticProvider)) {
                val keys1: List<IntPair?> = LatticeSpace.swap(h.keys)
                val h2: Step = space.addEdge(h.target(), h.source(), keys1)
                StepRef(target, source, h2, stepRefCount++)
            } else {
                val h2: Step = space.addEdge(h.source(), h.target(), h.keys)
                StepRef(source, target, h2, stepRefCount++)
            }
        }
    }

    /** Information about the parent of fields from a relational expression.  */
    internal abstract class Frame(
        columnCount: Int, hops: List<Hop?>?, measures: List<MutableMeasure?>?,
        tableRefs: Collection<TableRef?>?
    ) {
        val hops: List<Hop>
        val measures: List<MutableMeasure>
        val tableRefs: Set<TableRef>
        val columnCount: Int

        init {
            this.hops = ImmutableList.copyOf(hops)
            this.measures = ImmutableList.copyOf(measures)
            this.tableRefs = ImmutableSet.copyOf(tableRefs)
            this.columnCount = columnCount
        }

        constructor(
            columnCount: Int, hops: List<Hop>, measures: List<MutableMeasure?>?,
            inputs: List<Frame>
        ) : this(columnCount, hops, measures, collectTableRefs(inputs, hops)) {
        }

        @Nullable
        abstract fun column(offset: Int): ColRef

        @Override
        override fun toString(): String {
            return "Frame($hops)"
        }

        companion object {
            fun collectTableRefs(inputs: List<Frame>, hops: List<Hop>): Set<TableRef> {
                val set: LinkedHashSet<TableRef> = LinkedHashSet()
                for (hop in hops) {
                    set.add(hop.source.tableRef())
                    set.add(hop.target.tableRef())
                }
                for (frame in inputs) {
                    set.addAll(frame.tableRefs)
                }
                return set
            }
        }
    }

    /** Use of a table within a query. A table can be used more than once.  */
    private class TableRef(table: LatticeTable, ordinalInQuery: Int) {
        val table: LatticeTable
        private val ordinalInQuery: Int

        init {
            this.table = requireNonNull(table, "table")
            this.ordinalInQuery = ordinalInQuery
        }

        @Override
        override fun hashCode(): Int {
            return ordinalInQuery
        }

        @Override
        override fun equals(@Nullable obj: Object): Boolean {
            return (this === obj
                    || obj is TableRef
                    && ordinalInQuery == (obj as TableRef).ordinalInQuery)
        }

        @Override
        override fun toString(): String {
            return table.toString() + ":" + ordinalInQuery
        }
    }

    /** Use of a step within a query. A step can be used more than once.  */
    private class StepRef internal constructor(source: TableRef?, target: TableRef?, step: Step?, ordinalInQuery: Int) :
        DefaultEdge(source, target) {
        val step: Step
        val ordinalInQuery: Int

        init {
            this.step = requireNonNull(step, "step")
            this.ordinalInQuery = ordinalInQuery
        }

        @Override
        override fun hashCode(): Int {
            return ordinalInQuery
        }

        @Override
        override fun equals(@Nullable obj: Object): Boolean {
            return (this === obj
                    || obj is StepRef
                    && (obj as StepRef).ordinalInQuery == ordinalInQuery)
        }

        @Override
        override fun toString(): String {
            return ("StepRef(" + source.toString() + ", " + target.toString() + "," + step.keyString.toString() + "):"
                    + ordinalInQuery)
        }

        fun source(): TableRef {
            return source
        }

        fun target(): TableRef {
            return target
        }

        /** Creates [StepRef] instances.  */
        class Factory : AttributedDirectedGraph.AttributedEdgeFactory<TableRef?, StepRef?> {
            @Override
            fun createEdge(source: TableRef?, target: TableRef?): StepRef {
                throw UnsupportedOperationException()
            }

            @Override
            fun createEdge(
                source: TableRef?, target: TableRef?,
                vararg attributes: Object
            ): StepRef {
                val step: Step = attributes[0] as Step
                val ordinalInQuery: Integer = attributes[1] as Integer
                return StepRef(source, target, step, ordinalInQuery)
            }
        }
    }

    /** A hop is a join condition. One or more hops between the same source and
     * target combine to form a [Step].
     *
     *
     * The tables are registered but the step is not. After we have gathered
     * several join conditions we may discover that the keys are composite: e.g.
     *
     * <blockquote>
     * <pre>
     * x.a = y.a
     * AND x.b = z.b
     * AND x.c = y.c
    </pre> *
    </blockquote> *
     *
     *
     * has 3 semi-hops:
     *
     *
     *  * x.a = y.a
     *  * x.b = z.b
     *  * x.c = y.c
     *
     *
     *
     * which turn into 2 steps, the first of which is composite:
     *
     *
     *  * x.[a, c] = y.[a, c]
     *  * x.b = z.b
     *
     */
    private class Hop(val source: SingleTableColRef, val target: SingleTableColRef)

    /** Column reference.  */
    private abstract class ColRef

    /** Column reference that is within a single table.  */
    private interface SingleTableColRef {
        fun tableRef(): TableRef?
        fun col(space: LatticeSpace?): Int
    }

    /** Reference to a base column.  */
    private class BaseColRef(val t: TableRef, val c: Int) : ColRef(), SingleTableColRef {
        @Override
        override fun tableRef(): TableRef {
            return t
        }

        @Override
        override fun col(space: LatticeSpace?): Int {
            return c
        }
    }

    /** Reference to a derived column (that is, an expression).  */
    private class DerivedColRef internal constructor(tableRefs: Iterable<TableRef?>?, e: RexNode, alias: String) :
        ColRef() {
        val tableRefs: List<TableRef>
        val e: RexNode
        val alias: String

        init {
            this.tableRefs = ImmutableList.copyOf(tableRefs)
            this.e = e
            this.alias = alias
        }

        fun tableAliases(): List<String> {
            return Util.transform(tableRefs) { tableRef -> tableRef.table.alias }
        }
    }

    /** Variant of [DerivedColRef] where all referenced expressions are in
     * the same table.  */
    private class SingleTableDerivedColRef internal constructor(tableRef: TableRef?, e: RexNode, alias: String) :
        DerivedColRef(ImmutableList.of(tableRef), e, alias), SingleTableColRef {
        @Override
        override fun tableRef(): TableRef {
            return tableRefs.get(0)
        }

        @Override
        override fun col(space: LatticeSpace): Int {
            return space.registerExpression(tableRef().table, e)
        }
    }

    /** An aggregate call. Becomes a measure in the final lattice.  */
    private class MutableMeasure(
        aggregate: SqlAggFunction, distinct: Boolean,
        arguments: List<ColRef>, @Nullable name: String
    ) {
        val aggregate: SqlAggFunction
        val distinct: Boolean
        val arguments: List<ColRef>

        @Nullable
        val name: String

        init {
            this.aggregate = aggregate
            this.arguments = arguments
            this.distinct = distinct
            this.name = name
        }
    }

    companion object {
        private val PROGRAM: HepProgram = HepProgramBuilder()
            .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
            .addRuleInstance(CoreRules.JOIN_CONDITION_PUSH)
            .build()

        /** Derives the alias of an expression that is the argument to a measure.
         *
         *
         * For example, if the measure is called "sum_profit" and the aggregate
         * function is "sum", returns "profit".
         */
        private fun deriveAlias(
            measure: MutableMeasure,
            derivedColRef: DerivedColRef
        ): String {
            if (!derivedColRef.alias.contains("$")) {
                // User specified an alias. Use that.
                return derivedColRef.alias
            }
            val alias: String = requireNonNull(measure.name, "measure.name")
            if (alias.contains("$")) {
                // User did not specify an alias for the aggregate function, and it got a
                // system-generated name like 'EXPR$2'. Don't try to derive anything from
                // it.
                return derivedColRef.alias
            }
            val aggUpper: String = measure.aggregate.getName().toUpperCase(Locale.ROOT)
            val aliasUpper: String = alias.toUpperCase(Locale.ROOT)
            return if (aliasUpper.startsWith(aggUpper + "_")) {
                // Convert "sum_profit" to "profit"
                alias.substring((aggUpper + "_").length())
            } else if (aliasUpper.startsWith(aggUpper)) {
                // Convert "sumprofit" to "profit"
                alias.substring(aggUpper.length())
            } else if (aliasUpper.endsWith("_$aggUpper")) {
                // Convert "profit_sum" to "profit"
                alias.substring(0, alias.length() - "_$aggUpper".length())
            } else if (aliasUpper.endsWith(aggUpper)) {
                // Convert "profitsum" to "profit"
                alias.substring(0, alias.length() - aggUpper.length())
            } else {
                alias
            }
        }

        /** Copies measures and column usages from an existing lattice into a builder,
         * using a mapper to translate old-to-new columns, so that the new lattice can
         * inherit from the old.  */
        private fun copyMeasures(builder: Lattice.Builder, lattice: Lattice) {
            val mapper: Function<Lattice.Column?, Lattice.Column?> =
                label@ Function<Lattice.Column, Lattice.Column> { c: Lattice.Column ->
                    if (c is Lattice.BaseColumn) {
                        val p: Pair<Path, Integer> = lattice.columnToPathOffset(c)
                        return@label builder.pathOffsetToColumn(p.left, p.right)
                    } else {
                        val derivedColumn: Lattice.DerivedColumn = c as Lattice.DerivedColumn
                        return@label builder.expression(
                            derivedColumn.e, derivedColumn.alias,
                            derivedColumn.tables
                        )
                    }
                }
            for (measure in lattice.defaultMeasures) {
                builder.addMeasure(measure.copy(mapper))
            }
            for (entry in lattice.columnUses.entries()) {
                val column: Lattice.Column = lattice.columns.get(entry.getKey())
                builder.use(mapper.apply(column), entry.getValue())
            }
        }

        private fun matchQuality(lattice: Lattice, target: Lattice): Int {
            if (!lattice.rootNode.table.equals(target.rootNode.table)) {
                return 0
            }
            if (lattice.rootNode.paths.equals(target.rootNode.paths)) {
                return 3
            }
            return if (lattice.rootNode.paths.containsAll(target.rootNode.paths)) {
                2
            } else 1
        }

        private fun <E> minus(c: Collection<E>, c2: Collection<E>): Set<E> {
            val c3: LinkedHashSet<E> = LinkedHashSet(c)
            c3.removeAll(c2)
            return c3
        }

        private fun frames(frames: List<Frame>, q: Query, r: RelNode) {
            if (r is SetOp) {
                r.getInputs().forEach { input -> frames(frames, q, input) }
            } else {
                val frame = frame(q, r)
                if (frame != null) {
                    frames.add(frame)
                }
            }
        }

        @Nullable
        private fun frame(q: Query, r: RelNode): Frame? {
            return if (r is Sort) {
                val sort: Sort = r as Sort
                frame(q, sort.getInput())
            } else if (r is Filter) {
                val filter: Filter = r as Filter
                frame(q, filter.getInput())
            } else if (r is Aggregate) {
                val aggregate: Aggregate = r as Aggregate
                val h = frame(q, aggregate.getInput()) ?: return null
                val measures: List<MutableMeasure> = ArrayList()
                for (call in aggregate.getAggCallList()) {
                    measures.add(
                        MutableMeasure(
                            call.getAggregation(),
                            call.isDistinct(),
                            Util.< Integer,
                            ColRef > transform<Integer, ColRef>(call.getArgList()) { offset: Int -> h.column(offset) },
                            call.name
                        )
                    )
                }
                val fieldCount: Int = r.getRowType().getFieldCount()
                object : Frame(fieldCount, h.hops, measures, ImmutableList.of(h)) {
                    @Override
                    @Nullable
                    override fun column(offset: Int): ColRef? {
                        return if (offset < aggregate.getGroupSet().cardinality()) {
                            h.column(aggregate.getGroupSet().nth(offset))
                        } else null
                        // an aggregate function; no direct mapping
                    }
                }
            } else if (r is Project) {
                val project: Project = r as Project
                val h = frame(q, project.getInput()) ?: return null
                val fieldCount: Int = r.getRowType().getFieldCount()
                object : Frame(fieldCount, h.hops, h.measures, ImmutableList.of(h)) {
                    var columns: List<ColRef>? = null

                    init {
                        val columnBuilder: ImmutableNullableList.Builder<ColRef> = ImmutableNullableList.builder()
                        for (p in project.getNamedProjects()) {
                            @SuppressWarnings("method.invocation.invalid") val colRef =
                                toColRef(org.apache.calcite.materialize.p.left, org.apache.calcite.materialize.p.right)
                            org.apache.calcite.materialize.columnBuilder.add(org.apache.calcite.materialize.colRef)
                        }
                        columns = org.apache.calcite.materialize.columnBuilder.build()
                    }

                    @Override
                    @Nullable
                    override fun column(offset: Int): ColRef {
                        return columns!![offset]
                    }

                    /** Converts an expression to a base or derived column reference.
                     * The alias is optional, but if the derived column reference becomes
                     * a dimension or measure, the alias will be used to choose a name.  */
                    @Nullable
                    private fun toColRef(e: RexNode, alias: String): ColRef {
                        if (e is RexInputRef) {
                            return h.column((e as RexInputRef).getIndex())
                        }
                        val bits: ImmutableBitSet = RelOptUtil.InputFinder.bits(e)
                        val tableRefs: ImmutableList.Builder<TableRef> = ImmutableList.builder()
                        var c = 0 // offset within lattice of first column in a table
                        for (tableRef in h.tableRefs) {
                            val prev = c
                            c += tableRef.table.t.getRowType().getFieldCount()
                            if (bits.intersects(ImmutableBitSet.range(prev, c))) {
                                tableRefs.add(tableRef)
                            }
                        }
                        val tableRefList: List<TableRef> = tableRefs.build()
                        return when (tableRefList.size()) {
                            1 -> SingleTableDerivedColRef(
                                tableRefList[0], e, alias
                            )
                            else -> DerivedColRef(tableRefList, e, alias)
                        }
                    }
                }
            } else if (r is Join) {
                val join: Join = r as Join
                val leftCount: Int = join.getLeft().getRowType().getFieldCount()
                val left = frame(q, join.getLeft())
                val right = frame(q, join.getRight())
                if (left == null || right == null) {
                    return null
                }
                val builder: ImmutableList.Builder<Hop> = ImmutableList.builder()
                builder.addAll(left.hops)
                for (p in join.analyzeCondition().pairs()) {
                    val source = left.column(p.source)
                    val target = right.column(p.target)
                    assert(source is SingleTableColRef)
                    assert(target is SingleTableColRef)
                    builder.add(
                        Hop(
                            source as SingleTableColRef,
                            target as SingleTableColRef
                        )
                    )
                }
                builder.addAll(right.hops)
                val fieldCount: Int = r.getRowType().getFieldCount()
                object : Frame(
                    fieldCount, builder.build(),
                    CompositeList.of(left.measures, right.measures),
                    ImmutableList.of(left, right)
                ) {
                    @Override
                    @Nullable
                    override fun column(offset: Int): ColRef {
                        return if (offset < leftCount) {
                            left.column(offset)
                        } else {
                            right.column(offset - leftCount)
                        }
                    }
                }
            } else if (r is TableScan) {
                val scan: TableScan = r as TableScan
                val tableRef = q.tableRef(scan)
                val fieldCount: Int = r.getRowType().getFieldCount()
                object : Frame(
                    fieldCount, ImmutableList.of(),
                    ImmutableList.of(), ImmutableSet.of(tableRef)
                ) {
                    @Override
                    override fun column(offset: Int): ColRef {
                        if (offset >= scan.getTable().getRowType().getFieldCount()) {
                            throw IndexOutOfBoundsException(
                                "field " + offset
                                        + " out of range in " + scan.getTable().getRowType()
                            )
                        }
                        return BaseColRef(tableRef, offset)
                    }
                }
            } else {
                null
            }
        }
    }
}
