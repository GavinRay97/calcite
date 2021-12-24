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

import org.apache.calcite.avatica.AvaticaUtils

/**
 * Structure that allows materialized views based upon a star schema to be
 * recognized and recommended.
 */
class Lattice private constructor(
    rootSchema: CalciteSchema, rootNode: LatticeRootNode,
    auto: Boolean, algorithm: Boolean, algorithmMaxMillis: Long,
    statisticProviderFactory: LatticeStatisticProvider.Factory,
    @Nullable rowCountEstimate: Double, columns: ImmutableList<Column>,
    defaultMeasures: ImmutableSortedSet<Measure>, tiles: ImmutableList<Tile>,
    columnUses: ImmutableListMultimap<Integer, Boolean>
) {
    val rootSchema: CalciteSchema
    val rootNode: LatticeRootNode
    val columns: ImmutableList<Column>
    val auto: Boolean
    val algorithm: Boolean
    val algorithmMaxMillis: Long

    /** Returns an estimate of the number of rows in the un-aggregated star.  */
    val factRowCount: Double
    val defaultMeasures: ImmutableList<Measure>
    val tiles: ImmutableList<Tile>
    val columnUses: ImmutableListMultimap<Integer, Boolean>
    val statisticProvider: LatticeStatisticProvider

    init {
        var rowCountEstimate = rowCountEstimate
        this.rootSchema = rootSchema
        this.rootNode = requireNonNull(rootNode, "rootNode")
        this.columns = requireNonNull(columns, "columns")
        this.auto = auto
        this.algorithm = algorithm
        this.algorithmMaxMillis = algorithmMaxMillis
        this.defaultMeasures = defaultMeasures.asList() // unique and sorted
        this.tiles = requireNonNull(tiles, "tiles")
        this.columnUses = columnUses
        assert(isValid(Litmus.THROW))
        if (rowCountEstimate == null) {
            // We could improve this when we fix
            // [CALCITE-429] Add statistics SPI for lattice optimization algorithm
            rowCountEstimate = 1000.0
        }
        Preconditions.checkArgument(rowCountEstimate > 0.0)
        factRowCount = rowCountEstimate
        @SuppressWarnings("argument.type.incompatible") val statisticProvider: LatticeStatisticProvider =
            requireNonNull(statisticProviderFactory.apply(this))
        this.statisticProvider = statisticProvider
    }

    @RequiresNonNull(["rootNode", "defaultMeasures", "columns"])
    private fun isValid(
        litmus: Litmus
    ): Boolean {
        if (!rootNode.isValid(litmus)) {
            return false
        }
        for (measure in defaultMeasures) {
            for (arg in measure.args) {
                if (columns.get(arg.ordinal) !== arg) {
                    return litmus.fail(
                        "measure argument must be a column registered in"
                                + " this lattice: {}", measure
                    )
                }
            }
        }
        return litmus.succeed()
    }

    @Override
    override fun toString(): String {
        return rootNode.toString() + ":" + defaultMeasures
    }

    /** Generates a SQL query to populate a tile of the lattice specified by a
     * given set of columns and measures.  */
    fun sql(groupSet: ImmutableBitSet?, aggCallList: List<Measure>): String {
        return sql(groupSet, true, aggCallList)
    }

    /** Generates a SQL query to populate a tile of the lattice specified by a
     * given set of columns and measures, optionally grouping.  */
    fun sql(
        groupSet: ImmutableBitSet?, group: Boolean,
        aggCallList: List<Measure>
    ): String {
        val usedNodes: List<LatticeNode> = ArrayList()
        if (group) {
            val columnSetBuilder: ImmutableBitSet.Builder = groupSet.rebuild()
            for (call in aggCallList) {
                for (arg in call.args) {
                    columnSetBuilder.set(arg.ordinal)
                }
            }
            val columnSet: ImmutableBitSet = columnSetBuilder.build()

            // Figure out which nodes are needed. Use a node if its columns are used
            // or if has a child whose columns are used.
            for (node in rootNode.descendants) {
                if (ImmutableBitSet.range(node.startCol, node.endCol)
                        .intersects(columnSet)
                ) {
                    node.use(usedNodes)
                }
                if (usedNodes.isEmpty()) {
                    usedNodes.add(rootNode)
                }
            }
        } else {
            usedNodes.addAll(rootNode.descendants)
        }
        val dialect: SqlDialect = SqlDialect.DatabaseProduct.CALCITE.getDialect()
        val buf = StringBuilder("SELECT ")
        val groupBuf = StringBuilder("\nGROUP BY ")
        var k = 0
        val columnNames: Set<String> = HashSet()
        val w = createSqlWriter(dialect, buf, IntFunction<SqlNode> { f -> throw UnsupportedOperationException() })
        if (groupSet != null) {
            for (i in groupSet) {
                if (k++ > 0) {
                    buf.append(", ")
                    groupBuf.append(", ")
                }
                val column: Column = columns.get(i)
                column.toSql(w)
                column.toSql(w.with(groupBuf))
                if (column is BaseColumn) {
                    columnNames.add(column.column)
                }
                if (!column.alias.equals(column.defaultAlias())) {
                    buf.append(" AS ")
                    dialect.quoteIdentifier(buf, column.alias)
                }
            }
            var m = 0
            for (measure in aggCallList) {
                if (k++ > 0) {
                    buf.append(", ")
                }
                buf.append(measure.agg.getName())
                    .append("(")
                if (measure.args.isEmpty()) {
                    buf.append("*")
                } else {
                    var z = 0
                    for (arg in measure.args) {
                        if (z++ > 0) {
                            buf.append(", ")
                        }
                        arg.toSql(w)
                    }
                }
                buf.append(") AS ")
                var measureName: String?
                while (!columnNames.add("m" + m.also { measureName = it })) {
                    ++m
                }
                dialect.quoteIdentifier(buf, measureName)
            }
        } else {
            buf.append("*")
        }
        buf.append("\nFROM ")
        for (node in usedNodes) {
            if (node is LatticeChildNode) {
                buf.append("\nJOIN ")
            }
            dialect.quoteIdentifier(buf, node.table.t.getQualifiedName())
            val alias: String = node.alias
            if (alias != null) {
                buf.append(" AS ")
                dialect.quoteIdentifier(buf, alias)
            }
            if (node is LatticeChildNode) {
                val node1: LatticeChildNode = node
                buf.append(" ON ")
                k = 0
                for (pair in node1.link) {
                    if (k++ > 0) {
                        buf.append(" AND ")
                    }
                    val left: Column = columns.get(node1.parent.startCol + pair.source)
                    left.toSql(w)
                    buf.append(" = ")
                    val right: Column = columns.get(node.startCol + pair.target)
                    right.toSql(w)
                }
            }
        }
        if (CalciteSystemProperty.DEBUG.value()) {
            System.out.println(
                """
    Lattice SQL:
    $buf
    """.trimIndent()
            )
        }
        if (group) {
            if (groupSet.isEmpty()) {
                groupBuf.append("()")
            }
            buf.append(groupBuf)
        }
        return buf.toString()
    }

    /** Creates a context to which SQL can be generated.  */
    fun createSqlWriter(
        dialect: SqlDialect, buf: StringBuilder,
        field: IntFunction<SqlNode?>?
    ): SqlWriter {
        return SqlWriter(
            this, dialect, buf,
            SimpleContext(dialect, field)
        )
    }

    /** Returns a SQL query that counts the number of distinct values of the
     * attributes given in `groupSet`.  */
    fun countSql(groupSet: ImmutableBitSet?): String {
        return ("select count(*) as c from ("
                + sql(groupSet, ImmutableList.of())
                + ")")
    }

    fun createStarTable(): StarTable {
        val tables: List<Table> = ArrayList()
        for (node in rootNode.descendants) {
            tables.add(node.table.t.unwrapOrThrow(Table::class.java))
        }
        return StarTable.of(this, tables)
    }

    fun toMeasures(aggCallList: List<AggregateCall?>?): List<Measure> {
        return Util.transform(aggCallList) { aggCall: AggregateCall -> toMeasure(aggCall) }
    }

    private fun toMeasure(aggCall: AggregateCall): Measure {
        return Measure(
            aggCall.getAggregation(), aggCall.isDistinct(),
            aggCall.name, Util.transform(aggCall.getArgList(), columns::get)
        )
    }

    fun computeTiles(): Iterable<Tile> {
        return if (!algorithm) {
            tiles
        } else TileSuggester(this).tiles()
    }

    /** Returns an estimate of the number of rows in the tile with the given
     * dimensions.  */
    fun getRowCount(columns: List<Column?>?): Double {
        return statisticProvider.cardinality(columns)
    }

    fun uniqueColumnNames(): List<String> {
        return Util.transform(columns) { column -> column.alias }
    }

    fun columnToPathOffset(c: BaseColumn): Pair<Path, Integer> {
        for (p in Pair.zip(rootNode.descendants, rootNode.paths)) {
            if (Objects.equals(p.left.alias, c.table)) {
                return Pair.of(p.right, c.ordinal - p.left.startCol)
            }
        }
        throw AssertionError("lattice column not found: $c")
    }

    /** Returns the set of tables in this lattice.  */
    fun tables(): Set<LatticeTable> {
        return rootNode.descendants.stream().map { n -> n.table }
            .collect(Collectors.toCollection { LinkedHashSet() })
    }

    /** Returns the ordinal, within all of the columns in this Lattice, of the
     * first column in the table with a given alias.
     * Returns -1 if the table is not found.  */
    fun firstColumn(tableAlias: String?): Int {
        for (column in columns) {
            if (column is BaseColumn
                && (column as BaseColumn).table.equals(tableAlias)
            ) {
                return column.ordinal
            }
        }
        return -1
    }

    /** Returns whether every use of a column is as an argument to a measure.
     *
     *
     * For example, in the query
     * `select sum(x + y), sum(a + b) from t group by x + y`
     * the expression "x + y" is used once as an argument to a measure,
     * and once as a dimension.
     *
     *
     * Therefore, in a lattice created from that one query,
     * `isAlwaysMeasure` for the derived column corresponding to "x + y"
     * returns false, and for "a + b" returns true.
     *
     * @param column Column or derived column
     * @return Whether all uses are as arguments to aggregate functions
     */
    fun isAlwaysMeasure(column: Column): Boolean {
        return !columnUses.get(column.ordinal).contains(false)
    }

    /** Edge in the temporary graph.  */
    private class Edge internal constructor(source: Vertex?, target: Vertex?) : DefaultEdge(source, target) {
        val pairs: List<IntPair> = ArrayList()
        val target: Vertex
            get() = target
        val source: Vertex
            get() = source

        companion object {
            val FACTORY: DirectedGraph.EdgeFactory<Vertex, Edge> =
                DirectedGraph.EdgeFactory<Vertex, Edge> { source: Vertex?, target: Vertex? -> Edge(source, target) }
        }
    }

    /** Vertex in the temporary graph.  */
    private class Vertex(table: LatticeTable, @Nullable alias: String) {
        val table: LatticeTable

        @Nullable
        val alias: String

        init {
            this.table = table
            this.alias = alias
        }
    }

    /** A measure within a [Lattice].
     *
     *
     * It is immutable.
     *
     *
     * Examples: SUM(products.weight), COUNT() (means "COUNT(*")),
     * COUNT(DISTINCT customer.id).
     */
    class Measure(
        agg: SqlAggFunction?, distinct: Boolean, @Nullable name: String,
        args: Iterable<Column?>?
    ) : Comparable<Measure?> {
        val agg: SqlAggFunction
        val distinct: Boolean

        @Nullable
        val name: String
        val args: ImmutableList<Column>
        val digest: String

        init {
            this.agg = requireNonNull(agg, "agg")
            this.distinct = distinct
            this.name = name
            this.args = ImmutableList.copyOf(args)
            val b: StringBuilder = StringBuilder()
                .append(agg)
                .append(if (distinct) "(DISTINCT " else "(")
            for (arg in Ord.zip(this.args)) {
                if (arg.i > 0) {
                    b.append(", ")
                }
                if (arg.e is BaseColumn) {
                    b.append((arg.e as BaseColumn).table)
                    b.append('.')
                    b.append((arg.e as BaseColumn).column)
                } else {
                    b.append(arg.e.alias)
                }
            }
            b.append(')')
            digest = b.toString()
        }

        @Override
        operator fun compareTo(measure: Measure): Int {
            var c = compare(args, measure.args)
            if (c == 0) {
                c = agg.getName().compareTo(measure.agg.getName())
                if (c == 0) {
                    c = Boolean.compare(distinct, measure.distinct)
                }
            }
            return c
        }

        @Override
        override fun toString(): String {
            return digest
        }

        @Override
        override fun hashCode(): Int {
            return Objects.hash(agg, args)
        }

        @Override
        override fun equals(@Nullable obj: Object): Boolean {
            return (obj === this
                    || (obj is Measure
                    && agg.equals((obj as Measure).agg)
                    && args.equals((obj as Measure).args)
                    && distinct == (obj as Measure).distinct))
        }

        /** Returns the set of distinct argument ordinals.  */
        fun argBitSet(): ImmutableBitSet {
            val bitSet: ImmutableBitSet.Builder = ImmutableBitSet.builder()
            for (arg in args) {
                bitSet.set(arg.ordinal)
            }
            return bitSet.build()
        }

        /** Returns a list of argument ordinals.  */
        fun argOrdinals(): List<Integer> {
            return Util.transform(args) { column -> column.ordinal }
        }

        /** Copies this measure, mapping its arguments using a given function.  */
        fun copy(mapper: Function<Column?, Column?>?): Measure {
            return Measure(agg, distinct, name, Util.transform(args, mapper))
        }

        companion object {
            private fun compare(list0: List<Column>, list1: List<Column>): Int {
                val size: Int = Math.min(list0.size(), list1.size())
                for (i in 0 until size) {
                    val o0 = list0[i].ordinal
                    val o1 = list1[i].ordinal
                    val c: Int = Utilities.compare(o0, o1)
                    if (c != 0) {
                        return c
                    }
                }
                return Utilities.compare(list0.size(), list1.size())
            }
        }
    }

    /** Column in a lattice. May be an a base column or an expression,
     * and may have an additional alias that is unique
     * within the entire lattice.  */
    abstract class Column private constructor(
        /** Ordinal of the column within the lattice.  */
        val ordinal: Int, alias: String
    ) : Comparable<Column?> {
        /** Alias of the column, unique within the lattice. Derived from the column
         * name, automatically disambiguated if necessary.  */
        val alias: String

        init {
            this.alias = requireNonNull(alias, "alias")
        }

        @Override
        operator fun compareTo(column: Column): Int {
            return Utilities.compare(ordinal, column.ordinal)
        }

        @Override
        override fun hashCode(): Int {
            return ordinal
        }

        @Override
        override fun equals(@Nullable obj: Object): Boolean {
            return (obj === this
                    || obj is Column
                    && ordinal == (obj as Column).ordinal)
        }

        abstract fun toSql(writer: SqlWriter?)

        /** The alias that SQL would give to this expression.  */
        @Nullable
        abstract fun defaultAlias(): String?

        companion object {
            /** Converts a list of columns to a bit set of their ordinals.  */
            fun toBitSet(columns: List<Column>): ImmutableBitSet {
                val builder: ImmutableBitSet.Builder = ImmutableBitSet.builder()
                for (column in columns) {
                    builder.set(column.ordinal)
                }
                return builder.build()
            }
        }
    }

    /** Column in a lattice. Columns are identified by table alias and
     * column name, and may have an additional alias that is unique
     * within the entire lattice.  */
    class BaseColumn(ordinal: Int, table: String, column: String, alias: String) : Column(ordinal, alias) {
        /** Alias of the table reference that the column belongs to.  */
        val table: String

        /** Name of the column. Unique within the table reference, but not
         * necessarily within the lattice.  */
        val column: String

        init {
            this.table = requireNonNull(table, "table")
            this.column = requireNonNull(column, "column")
        }

        @Override
        override fun toString(): String {
            return identifiers().toString()
        }

        fun identifiers(): List<String> {
            return ImmutableList.of(table, column)
        }

        @Override
        override fun toSql(writer: SqlWriter) {
            writer.dialect.quoteIdentifier(writer.buf, identifiers())
        }

        @Override
        override fun defaultAlias(): String {
            return column
        }
    }

    /** Column in a lattice that is based upon a SQL expression.  */
    class DerivedColumn(
        ordinal: Int, alias: String, e: RexNode,
        tables: List<String>
    ) : Column(ordinal, alias) {
        val e: RexNode
        val tables: List<String>

        init {
            this.e = e
            this.tables = ImmutableList.copyOf(tables)
        }

        @Override
        override fun toString(): String {
            return Arrays.toString(arrayOf(e, alias))
        }

        @Override
        override fun toSql(writer: SqlWriter) {
            writer.write(e)
        }

        @Override
        @Nullable
        override fun defaultAlias(): String? {
            // there is no default alias for an expression
            return null
        }
    }

    /** The information necessary to convert a column to SQL.  */
    class SqlWriter internal constructor(
        val lattice: Lattice, dialect: SqlDialect, buf: StringBuilder,
        context: SqlImplementor.SimpleContext
    ) {
        val buf: StringBuilder
        val dialect: SqlDialect
        private val context: SqlImplementor.SimpleContext

        init {
            this.context = context
            this.buf = buf
            this.dialect = dialect
        }

        /** Re-binds this writer to a different [StringBuilder].  */
        fun with(buf: StringBuilder): SqlWriter {
            return SqlWriter(lattice, dialect, buf, context)
        }

        /** Writes an expression.  */
        fun write(e: RexNode?): SqlWriter {
            val node: SqlNode = context.toSql(null, e)
            buf.append(node.toSqlString(dialect))
            return this
        }
    }

    /** Lattice builder.  */
    class Builder {
        private val rootNode: LatticeRootNode
        private val baseColumns: ImmutableList<BaseColumn>
        private val columnsByAlias: ImmutableListMultimap<String, Column>
        private val defaultMeasureSet: NavigableSet<Measure> = TreeSet()
        private val tileListBuilder: ImmutableList.Builder<Tile> = ImmutableList.builder()
        private val columnUses: Multimap<Integer, Boolean> = LinkedHashMultimap.create()
        private val rootSchema: CalciteSchema
        private var algorithm = false
        private var algorithmMaxMillis: Long = -1
        private var auto = true

        @MonotonicNonNull
        private var rowCountEstimate: Double? = null

        @Nullable
        private var statisticProvider: String? = null
        private val derivedColumnsByName: Map<String, DerivedColumn> = LinkedHashMap()

        constructor(space: LatticeSpace, schema: CalciteSchema, sql: String?) {
            rootSchema = requireNonNull(schema.root())
            Preconditions.checkArgument(rootSchema.isRoot(), "must be root schema")
            val parsed: CalcitePrepare.ConvertResult = Schemas.convert(
                MaterializedViewTable.MATERIALIZATION_CONNECTION,
                schema, schema.path(null), sql
            )

            // Walk the join tree.
            val relNodes: List<TableScan> = ArrayList()
            val tempLinks: List<Array<IntArray>> = ArrayList()
            populate(relNodes, tempLinks, parsed.root.rel)

            // Get aliases.
            val aliases: List<String> = ArrayList()
            val from: SqlNode = (parsed.sqlNode as SqlSelect).getFrom()
            assert(from != null) { "from must not be null" }
            populateAliases(from, aliases, null)

            // Build a graph.
            val graph: DirectedGraph<Vertex, Edge> = DefaultDirectedGraph.create(Edge.FACTORY)
            val vertices: List<Vertex> = ArrayList()
            for (p in Pair.zip(relNodes, aliases)) {
                val table: LatticeTable = space.register(p.left.getTable())
                val vertex = Vertex(table, p.right)
                graph.addVertex(vertex)
                vertices.add(vertex)
            }
            for (tempLink in tempLinks) {
                val source = vertices[tempLink[0][0]]
                val target = vertices[tempLink[1][0]]
                var edge: Edge? = graph.getEdge(source, target)
                if (edge == null) {
                    edge = castNonNull(graph.addEdge(source, target))
                }
                edge!!.pairs.add(IntPair.of(tempLink[0][1], tempLink[1][1]))
            }

            // Convert the graph into a tree of nodes, each connected to a parent and
            // with a join condition to that parent.
            var root: MutableNode? = null
            val map: IdentityHashMap<LatticeTable, MutableNode> = IdentityHashMap()
            for (vertex in TopologicalOrderIterator.of(graph)) {
                val edges: List<Edge> = graph.getInwardEdges(vertex)
                var node: MutableNode
                if (root == null) {
                    if (!edges.isEmpty()) {
                        throw RuntimeException(
                            "root node must not have relationships: "
                                    + vertex
                        )
                    }
                    node = MutableNode(vertex.table)
                    root = node
                    node.alias = vertex.alias
                } else {
                    if (edges.size() !== 1) {
                        throw RuntimeException(
                            "child node must have precisely one parent: $vertex"
                        )
                    }
                    val edge = edges[0]
                    val parent: MutableNode = map.get(edge.getSource().table)
                    val step: Step = Step.create(
                        edge.getSource().table,
                        edge.getTarget().table, edge.pairs, space
                    )
                    node = MutableNode(vertex.table, parent, step)
                    node.alias = vertex.alias
                }
                map.put(vertex.table, node)
            }
            assert(root != null)
            val fixer = Fixer()
            fixer.fixUp(root)
            baseColumns = fixer.columnList.build()
            columnsByAlias = fixer.columnAliasList.build()
            rootNode = LatticeRootNode(space, root)
        }

        /** Creates a Builder based upon a mutable node.  */
        internal constructor(
            space: LatticeSpace, schema: CalciteSchema,
            mutableNode: MutableNode?
        ) {
            rootSchema = schema
            val fixer = Fixer()
            fixer.fixUp(mutableNode)
            val node0 = LatticeRootNode(space, mutableNode)
            val node1: LatticeRootNode = space.nodeMap.get(node0.digest)
            val node: LatticeRootNode
            if (node1 != null) {
                node = node1
            } else {
                node = node0
                space.nodeMap.put(node0.digest, node0)
            }
            rootNode = node
            baseColumns = fixer.columnList.build()
            columnsByAlias = fixer.columnAliasList.build()
        }

        /** Sets the "auto" attribute (default true).  */
        fun auto(auto: Boolean): Builder {
            this.auto = auto
            return this
        }

        /** Sets the "algorithm" attribute (default false).  */
        fun algorithm(algorithm: Boolean): Builder {
            this.algorithm = algorithm
            return this
        }

        /** Sets the "algorithmMaxMillis" attribute (default -1).  */
        fun algorithmMaxMillis(algorithmMaxMillis: Long): Builder {
            this.algorithmMaxMillis = algorithmMaxMillis
            return this
        }

        /** Sets the "rowCountEstimate" attribute (default null).  */
        fun rowCountEstimate(rowCountEstimate: Double): Builder {
            this.rowCountEstimate = rowCountEstimate
            return this
        }

        /** Sets the "statisticProvider" attribute.
         *
         *
         * If not set, the lattice will use [Lattices.CACHED_SQL].  */
        fun statisticProvider(@Nullable statisticProvider: String?): Builder {
            this.statisticProvider = statisticProvider
            return this
        }

        /** Builds a lattice.  */
        fun build(): Lattice {
            val statisticProvider: LatticeStatisticProvider.Factory =
                if (statisticProvider != null) AvaticaUtils.instantiatePlugin(
                    LatticeStatisticProvider.Factory::class.java,
                    statisticProvider
                ) else Lattices.CACHED_SQL
            Preconditions.checkArgument(rootSchema.isRoot(), "must be root schema")
            val columnBuilder: ImmutableList.Builder<Column> =
                ImmutableList.< Column > builder < org . apache . calcite . materialize . Lattice . Column ? > ()
                    .addAll(baseColumns)
                    .addAll(derivedColumnsByName.values())
            return Lattice(
                rootSchema, rootNode, auto,
                algorithm, algorithmMaxMillis, statisticProvider, rowCountEstimate!!,
                columnBuilder.build(), ImmutableSortedSet.copyOf(defaultMeasureSet),
                tileListBuilder.build(), ImmutableListMultimap.copyOf(columnUses)
            )
        }

        /** Resolves the arguments of a
         * [org.apache.calcite.model.JsonMeasure]. They must either be null,
         * a string, or a list of strings. Throws if the structure is invalid, or if
         * any of the columns do not exist in the lattice.  */
        fun resolveArgs(@Nullable args: Object?): ImmutableList<Column> {
            return if (args == null) {
                ImmutableList.of()
            } else if (args is String) {
                ImmutableList.of(resolveColumnByAlias(args as String))
            } else if (args is List) {
                val builder: ImmutableList.Builder<Column> = ImmutableList.builder()
                for (o in args) {
                    if (o is String) {
                        builder.add(resolveColumnByAlias(o))
                    } else {
                        throw RuntimeException(
                            "Measure arguments must be a string or a list of strings; argument: "
                                    + o
                        )
                    }
                }
                builder.build()
            } else {
                throw RuntimeException(
                    "Measure arguments must be a string or a list of strings"
                )
            }
        }

        /** Looks up a column in this lattice by alias. The alias must be unique
         * within the lattice.
         */
        private fun resolveColumnByAlias(name: String): Column {
            val list: ImmutableList<Column> = columnsByAlias.get(name)
            return if (list == null || list.size() === 0) {
                throw RuntimeException("Unknown lattice column '$name'")
            } else if (list.size() === 1) {
                list.get(0)
            } else {
                throw RuntimeException(
                    "Lattice column alias '" + name
                            + "' is not unique"
                )
            }
        }

        fun resolveColumn(name: Object): Column {
            if (name is String) {
                return resolveColumnByAlias(name as String)
            }
            if (name is List) {
                val list: List = name
                when (list.size()) {
                    1 -> {
                        val alias: Object = list.get(0)
                        if (alias is String) {
                            return resolveColumnByAlias(alias as String)
                        }
                    }
                    2 -> {
                        val table: Object = list.get(0)
                        val column: Object = list.get(1)
                        if (table is String && column is String) {
                            return resolveQualifiedColumn(table as String, column as String)
                        }
                    }
                    else -> {}
                }
            }
            throw RuntimeException(
                "Lattice column reference must be a string or a list of 1 or 2 strings; column: "
                        + name
            )
        }

        private fun resolveQualifiedColumn(table: String, column: String): Column {
            for (column1 in baseColumns) {
                if (column1.table.equals(table)
                    && column1.column.equals(column)
                ) {
                    return column1
                }
            }
            throw RuntimeException(
                "Unknown lattice column [" + table + ", "
                        + column + "]"
            )
        }

        fun resolveMeasure(
            aggName: String, distinct: Boolean,
            @Nullable args: Object?
        ): Measure {
            val agg: SqlAggFunction = resolveAgg(aggName)
            val list: ImmutableList<Column?> = resolveArgs(args)
            return Measure(agg, distinct, aggName, list)
        }

        /** Adds a measure, if it does not already exist.
         * Returns false if an identical measure already exists.  */
        fun addMeasure(measure: Measure?): Boolean {
            return defaultMeasureSet.add(measure)
        }

        fun addTile(tile: Tile?) {
            tileListBuilder.add(tile)
        }

        fun column(table: Int, column: Int): Column {
            var table = table
            var i = 0
            for (descendant in rootNode.descendants) {
                if (table-- == 0) {
                    break
                }
                i += descendant.table.t.getRowType().getFieldCount()
            }
            return baseColumns.get(i + column)
        }

        fun pathOffsetToColumn(path: Path?, offset: Int): Column {
            val i: Int = rootNode.paths.indexOf(path)
            val node: LatticeNode = rootNode.descendants.get(i)
            val c: Int = node.startCol + offset
            if (c >= node.endCol) {
                throw AssertionError()
            }
            return baseColumns.get(c)
        }

        /** Adds a lattice column based on a SQL expression,
         * or returns a column based on the same expression seen previously.  */
        fun expression(
            e: RexNode, alias: String?,
            tableAliases: List<String>
        ): Column {
            return derivedColumnsByName.computeIfAbsent(e.toString()) { k ->
                val derivedOrdinal: Int = derivedColumnsByName.size()
                val ordinal: Int = baseColumns.size() + derivedOrdinal
                DerivedColumn(
                    ordinal,
                    Util.first(alias, "e$$derivedOrdinal"), e, tableAliases
                )
            }
        }

        /** Records a use of a column.
         *
         * @param column Column
         * @param measure Whether this use is as an argument to a measure;
         * e.g. "sum(x + y)" is a measure use of the expression
         * "x + y"; "group by x + y" is not
         */
        fun use(column: Column, measure: Boolean) {
            columnUses.put(column.ordinal, measure)
        }

        /** Work space for fixing up a tree of mutable nodes.  */
        private class Fixer {
            val aliases: Set<String> = HashSet()
            val columnAliases: Set<String> = HashSet()
            val seen: Set<MutableNode> = HashSet()
            val columnList: ImmutableList.Builder<BaseColumn> = ImmutableList.builder()
            val columnAliasList: ImmutableListMultimap.Builder<String, Column> = ImmutableListMultimap.builder()
            var c = 0
            fun fixUp(node: MutableNode?) {
                if (!seen.add(node)) {
                    throw IllegalArgumentException("cyclic query graph")
                }
                if (node.alias == null) {
                    node.alias = Util.last(node.table.t.getQualifiedName())
                }
                node.alias = SqlValidatorUtil.uniquify(
                    node.alias, aliases,
                    SqlValidatorUtil.ATTEMPT_SUGGESTER
                )
                node.startCol = c
                for (name in node.table.t.getRowType().getFieldNames()) {
                    val alias: String = SqlValidatorUtil.uniquify(
                        name,
                        columnAliases, SqlValidatorUtil.ATTEMPT_SUGGESTER
                    )
                    val column = BaseColumn(c++, castNonNull(node.alias), name, alias)
                    columnList.add(column)
                    columnAliasList.put(name, column) // name before it is made unique
                }
                node.endCol = c
                assert(MutableNode.ORDERING.isStrictlyOrdered(node.children)) { node.children }
                for (child in node.children) {
                    fixUp(child)
                }
            }
        }

        companion object {
            private fun resolveAgg(aggName: String): SqlAggFunction {
                return if (aggName.equalsIgnoreCase("count")) {
                    SqlStdOperatorTable.COUNT
                } else if (aggName.equalsIgnoreCase("sum")) {
                    SqlStdOperatorTable.SUM
                } else {
                    throw RuntimeException(
                        "Unknown lattice aggregate function "
                                + aggName
                    )
                }
            }
        }
    }

    /** Materialized aggregate within a lattice.  */
    class Tile(
        measures: ImmutableList<Measure?>?,
        dimensions: ImmutableList<Column>
    ) {
        val measures: ImmutableList<Measure>
        val dimensions: ImmutableList<Column>
        val bitSet: ImmutableBitSet

        init {
            this.measures = requireNonNull(measures, "measures")
            this.dimensions = requireNonNull(dimensions, "dimensions")
            assert(Ordering.natural().isStrictlyOrdered(dimensions))
            assert(Ordering.natural().isStrictlyOrdered(measures))
            bitSet = Column.toBitSet(dimensions)
        }

        fun bitSet(): ImmutableBitSet {
            return bitSet
        }

        companion object {
            fun builder(): TileBuilder {
                return TileBuilder()
            }
        }
    }

    /** Tile builder.  */
    class TileBuilder {
        private val measureBuilder: List<Measure> = ArrayList()
        private val dimensionListBuilder: List<Column> = ArrayList()
        fun build(): Tile {
            return Tile(
                Ordering.natural().immutableSortedCopy(measureBuilder),
                Ordering.natural().immutableSortedCopy(dimensionListBuilder)
            )
        }

        fun addMeasure(measure: Measure?) {
            measureBuilder.add(measure)
        }

        fun addDimension(column: Column?) {
            dimensionListBuilder.add(column)
        }
    }

    companion object {
        /** Creates a Lattice.  */
        fun create(schema: CalciteSchema?, sql: String?, auto: Boolean): Lattice {
            return builder(schema, sql).auto(auto).build()
        }

        private fun populateAliases(
            from: SqlNode?, aliases: List<String>,
            @Nullable current: String?
        ) {
            var current = current
            if (from is SqlJoin) {
                val join: SqlJoin? = from as SqlJoin?
                populateAliases(join.getLeft(), aliases, null)
                populateAliases(join.getRight(), aliases, null)
            } else if (from.getKind() === SqlKind.AS) {
                populateAliases(
                    SqlUtil.stripAs(from), aliases,
                    SqlValidatorUtil.getAlias(from, -1)
                )
            } else {
                if (current == null) {
                    current = SqlValidatorUtil.getAlias(from, -1)
                }
                aliases.add(current)
            }
        }

        private fun populate(
            nodes: List<TableScan>, tempLinks: List<Array<IntArray>>,
            rel: RelNode
        ): Boolean {
            if (nodes.isEmpty() && rel is LogicalProject) {
                return populate(nodes, tempLinks, (rel as LogicalProject).getInput())
            }
            if (rel is TableScan) {
                nodes.add(rel as TableScan)
                return true
            }
            if (rel is LogicalJoin) {
                val join: LogicalJoin = rel as LogicalJoin
                if (join.getJoinType().isOuterJoin()) {
                    throw RuntimeException(
                        "only non nulls-generating join allowed, but got "
                                + join.getJoinType()
                    )
                }
                populate(nodes, tempLinks, join.getLeft())
                populate(nodes, tempLinks, join.getRight())
                for (rex in RelOptUtil.conjunctions(join.getCondition())) {
                    tempLinks.add(grab(nodes, rex))
                }
                return true
            }
            throw RuntimeException(
                ("Invalid node type "
                        + rel.getClass().getSimpleName()) + " in lattice query"
            )
        }

        /** Converts an "t1.c1 = t2.c2" expression into two (input, field) pairs.  */
        private fun grab(leaves: List<TableScan>, rex: RexNode): Array<IntArray> {
            when (rex.getKind()) {
                EQUALS -> {}
                else -> throw AssertionError("only equi-join allowed")
            }
            val operands: List<RexNode> = (rex as RexCall).getOperands()
            return arrayOf(
                inputField(leaves, operands[0]),
                inputField(leaves, operands[1])
            )
        }

        /** Converts an expression into an (input, field) pair.  */
        private fun inputField(leaves: List<TableScan>, rex: RexNode): IntArray {
            if (rex !is RexInputRef) {
                throw RuntimeException("only equi-join of columns allowed: $rex")
            }
            val ref: RexInputRef = rex as RexInputRef
            var start = 0
            for (i in 0 until leaves.size()) {
                val leaf: RelNode = leaves[i]
                val end: Int = start + leaf.getRowType().getFieldCount()
                if (ref.getIndex() < end) {
                    return intArrayOf(i, ref.getIndex() - start)
                }
                start = end
            }
            throw AssertionError("input not found")
        }

        fun builder(calciteSchema: CalciteSchema?, sql: String?): Builder {
            return builder(
                LatticeSpace(MapSqlStatisticProvider.INSTANCE),
                calciteSchema, sql
            )
        }

        fun builder(
            space: LatticeSpace?, calciteSchema: CalciteSchema?,
            sql: String?
        ): Builder {
            return Builder(space, calciteSchema, sql)
        }

        /** Returns an estimate of the number of rows in the tile with the given
         * dimensions.  */
        fun getRowCount(factCount: Double, vararg columnCounts: Double): Double {
            return getRowCount(factCount, Primitive.asList(columnCounts))
        }

        /** Returns an estimate of the number of rows in the tile with the given
         * dimensions.  */
        fun getRowCount(
            factCount: Double,
            columnCounts: List<Double>
        ): Double {
            // The expected number of distinct values when choosing p values
            // with replacement from n integers is n . (1 - ((n - 1) / n) ^ p).
            //
            // If we have several uniformly distributed attributes A1 ... Am
            // with N1 ... Nm distinct values, they behave as one uniformly
            // distributed attribute with N1 * ... * Nm distinct values.
            var n = 1.0
            for (columnCount in columnCounts) {
                if (columnCount > 1.0) {
                    n *= columnCount
                }
            }
            val a = (n - 1.0) / n
            if (a == 1.0) {
                // A under-flows if nn is large.
                return factCount
            }
            val v: Double = n * (1.0 - Math.pow(a, factCount))
            // Cap at fact-row-count, because numerical artifacts can cause it
            // to go a few % over.
            return Math.min(v, factCount)
        }
    }
}
