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
package org.apache.calcite.interpreter

import org.apache.calcite.DataContext

/**
 * Interpreter.
 *
 *
 * Contains the context for interpreting relational expressions. In
 * particular it holds working state while the data flow graph is being
 * assembled.
 */
class Interpreter(dataContext: DataContext?, rootRel: RelNode) : AbstractEnumerable<Array<Object?>?>(), AutoCloseable {
    private val nodes: Map<RelNode, NodeInfo>
    private val dataContext: DataContext
    private val rootRel: RelNode

    /** Creates an Interpreter.  */
    init {
        this.dataContext = requireNonNull(dataContext, "dataContext")
        val rel: RelNode = optimize(rootRel)
        val compiler: CompilerImpl = CoreCompiler(this, rootRel.getCluster())
        @SuppressWarnings("method.invocation.invalid") val pair: Pair<RelNode, Map<RelNode, NodeInfo>> =
            compiler.visitRoot(rel)
        this.rootRel = pair.left
        nodes = ImmutableMap.copyOf(pair.right)
    }

    @Override
    fun enumerator(): Enumerator<Array<Object>> {
        start()
        val nodeInfo: NodeInfo = requireNonNull(
            nodes[rootRel]
        ) { "nodeInfo for $rootRel" }
        val rows: Enumerator<Row>
        rows = if (nodeInfo.rowEnumerable != null) {
            nodeInfo.rowEnumerable.enumerator()
        } else {
            val queue: ArrayDeque<Row> = Iterables.getOnlyElement(nodeInfo.sinks.values()).list
            Linq4j.iterableEnumerator(queue)
        }
        return object : TransformedEnumerator<Row?, Array<Object?>?>(rows) {
            @Override
            @Nullable
            protected fun transform(row: Row): Array<Object> {
                return row.getValues()
            }
        }
    }

    @SuppressWarnings("CatchAndPrintStackTrace")
    private fun start() {
        // We rely on the nodes being ordered leaves first.
        for (entry in nodes.entrySet()) {
            val nodeInfo: NodeInfo = entry.getValue()
            try {
                assert(nodeInfo.node != null) { "node must not be null for nodeInfo, rel=" + nodeInfo.rel }
                nodeInfo.node.run()
            } catch (e: InterruptedException) {
                e.printStackTrace()
            }
        }
    }

    @Override
    fun close() {
        nodes.values().forEach { obj: NodeInfo -> obj.close() }
    }

    /** Information about a node registered in the data flow graph.  */
    private class NodeInfo internal constructor(rel: RelNode, @Nullable rowEnumerable: Enumerable<Row?>) {
        val rel: RelNode
        val sinks: Map<Edge, ListSink> = LinkedHashMap()

        @Nullable
        val rowEnumerable: Enumerable<Row>

        @Nullable
        var node: Node? = null

        init {
            this.rel = rel
            this.rowEnumerable = rowEnumerable
        }

        fun close() {
            if (node != null) {
                val n: Node = node
                node = null
                n.close()
            }
        }
    }

    /**
     * A [Source] that is just backed by an [Enumerator]. The [Enumerator] is closed
     * when it is finished or by calling [.close].
     */
    private class EnumeratorSource internal constructor(enumerator: Enumerator<Row?>?) : Source {
        private val enumerator: Enumerator<Row>

        init {
            this.enumerator = requireNonNull(enumerator, "enumerator")
        }

        @Override
        @Nullable
        fun receive(): Row? {
            if (enumerator.moveNext()) {
                return enumerator.current()
            }
            // close the enumerator once we have gone through everything
            enumerator.close()
            return null
        }

        @Override
        fun close() {
            enumerator.close()
        }
    }

    /** Implementation of [Sink] using a [java.util.ArrayDeque].  */
    private class ListSink(list: ArrayDeque<Row>) : Sink {
        val list: ArrayDeque<Row>

        init {
            this.list = list
        }

        @Override
        @Throws(InterruptedException::class)
        fun send(row: Row?) {
            list.add(row)
        }

        @Override
        @Throws(InterruptedException::class)
        fun end() {
        }

        @SuppressWarnings("deprecation")
        @Override
        @Throws(InterruptedException::class)
        fun setSourceEnumerable(enumerable: Enumerable<Row?>) {
            // just copy over the source into the local list
            val enumerator: Enumerator<Row> = enumerable.enumerator()
            while (enumerator.moveNext()) {
                send(enumerator.current())
            }
            enumerator.close()
        }
    }

    /** Implementation of [Source] using a [java.util.ArrayDeque].  */
    private class ListSource internal constructor(list: ArrayDeque<Row?>) : Source {
        private val list: ArrayDeque<Row>

        @Nullable
        private var iterator: Iterator<Row>? = null

        init {
            this.list = list
        }

        @Override
        @Nullable
        fun receive(): Row? {
            return try {
                if (iterator == null) {
                    iterator = list.iterator()
                }
                iterator!!.next()
            } catch (e: NoSuchElementException) {
                iterator = null
                null
            }
        }

        @Override
        fun close() {
            // noop
        }
    }

    /** Implementation of [Sink] using a [java.util.ArrayDeque].  */
    private class DuplicatingSink(queues: List<ArrayDeque<Row>>) : Sink {
        private val queues: List<ArrayDeque<Row>>

        init {
            this.queues = ImmutableList.copyOf(queues)
        }

        @Override
        @Throws(InterruptedException::class)
        fun send(row: Row?) {
            for (queue in queues) {
                queue.add(row)
            }
        }

        @Override
        @Throws(InterruptedException::class)
        fun end() {
        }

        @SuppressWarnings("deprecation")
        @Override
        @Throws(InterruptedException::class)
        fun setSourceEnumerable(enumerable: Enumerable<Row?>) {
            // just copy over the source into the local list
            val enumerator: Enumerator<Row> = enumerable.enumerator()
            while (enumerator.moveNext()) {
                send(enumerator.current())
            }
            enumerator.close()
        }
    }

    /**
     * Walks over a tree of [org.apache.calcite.rel.RelNode] and, for each,
     * creates a [org.apache.calcite.interpreter.Node] that can be
     * executed in the interpreter.
     *
     *
     * The compiler looks for methods of the form "visit(XxxRel)".
     * A "visit" method must create an appropriate [Node] and put it into
     * the [.node] field.
     *
     *
     * If you wish to handle more kinds of relational expressions, add extra
     * "visit" methods in this or a sub-class, and they will be found and called
     * via reflection.
     */
    internal class CompilerImpl(
        @field:NotOnlyInitialized @param:UnknownInitialization protected val interpreter: Interpreter,
        cluster: RelOptCluster
    ) : RelVisitor(), Compiler, ReflectiveVisitor {
        val scalarCompiler: ScalarCompiler
        private val dispatcher: ReflectiveVisitDispatcher<CompilerImpl, RelNode> = ReflectUtil.createDispatcher(
            CompilerImpl::class.java, RelNode::class.java
        )

        @Nullable
        protected var rootRel: RelNode? = null

        @Nullable
        protected var rel: RelNode? = null

        @Nullable
        protected var node: Node? = null
        val nodes: Map<RelNode, NodeInfo> = LinkedHashMap()
        val relInputs: Map<RelNode, List<RelNode>> = HashMap()
        val outEdges: Multimap<RelNode, Edge> = LinkedHashMultimap.create()

        init {
            scalarCompiler = JaninoRexCompiler(cluster.getRexBuilder())
        }

        /** Visits the tree, starting from the root `p`.  */
        fun visitRoot(p: RelNode): Pair<RelNode, Map<RelNode, NodeInfo>> {
            rootRel = p
            visit(p, 0, null)
            return Pair.of(requireNonNull(rootRel, "rootRel"), nodes)
        }

        @Override
        fun visit(p: RelNode, ordinal: Int, @Nullable parent: RelNode?) {
            var p: RelNode = p
            while (true) {
                rel = null
                val found: Boolean = dispatcher.invokeVisitor(this, p, REWRITE_METHOD_NAME)
                if (!found) {
                    throw AssertionError(
                        "interpreter: no implementation for rewrite"
                    )
                }
                if (rel == null) {
                    break
                }
                if (CalciteSystemProperty.DEBUG.value()) {
                    System.out.println("Interpreter: rewrite $p to $rel")
                }
                p = requireNonNull(rel, "rel")
                if (parent != null) {
                    var inputs: List<RelNode?>? = relInputs[parent]
                    if (inputs == null) {
                        inputs = Lists.newArrayList(parent.getInputs())
                        relInputs.put(parent, inputs)
                    }
                    inputs.set(ordinal, p)
                } else {
                    rootRel = p
                }
            }

            // rewrite children first (from left to right)
            val inputs: List<RelNode>? = relInputs[p]
            val finalP: RelNode = p
            Ord.forEach(
                Util.first(inputs, p.getInputs())
            ) { r, i -> outEdges.put(r, Edge(finalP, i)) }
            if (inputs != null) {
                for (i in 0 until inputs.size()) {
                    val input: RelNode = inputs[i]
                    visit(input, i, p)
                }
            } else {
                p.childrenAccept(this)
            }
            node = null
            val found: Boolean = dispatcher.invokeVisitor(this, p, VISIT_METHOD_NAME)
            if (!found) {
                node = if (p is InterpretableRel) {
                    p.implement(
                        InterpreterImplementor(
                            this, null,
                            DataContexts.EMPTY
                        )
                    )
                } else {
                    // Probably need to add a visit(XxxRel) method to CoreCompiler.
                    throw AssertionError(
                        "interpreter: no implementation for "
                                + p.getClass()
                    )
                }
            }
            val nodeInfo = nodes[p]
            assert(nodeInfo != null)
            nodeInfo!!.node = node
            if (inputs != null) {
                for (i in 0 until inputs.size()) {
                    val input: RelNode = inputs[i]
                    visit(input, i, p)
                }
            }
        }

        /** Fallback rewrite method.
         *
         *
         * Overriding methods (each with a different sub-class of [RelNode]
         * as its argument type) sets the [.rel] field if intends to
         * rewrite.  */
        fun rewrite(r: RelNode?) {}

        @Override
        override fun compile(nodes: List<RexNode?>?, @Nullable inputRowType: RelDataType?): Scalar {
            var inputRowType: RelDataType? = inputRowType
            if (inputRowType == null) {
                inputRowType = typeFactory.builder()
                    .build()
            }
            return scalarCompiler.compile(nodes, inputRowType)
                .apply(interpreter.dataContext)
        }

        private val typeFactory: JavaTypeFactory
            private get() = interpreter.dataContext.getTypeFactory()

        @Override
        fun combinedRowType(inputs: List<RelNode?>): RelDataType {
            val builder: RelDataTypeFactory.Builder = typeFactory.builder()
            for (input in inputs) {
                builder.addAll(input.getRowType().getFieldList())
            }
            return builder.build()
        }

        @Override
        override fun source(rel: RelNode, ordinal: Int): Source {
            val input: RelNode = getInput(rel, ordinal)
            val edge = Edge(rel, ordinal)
            val edges: Collection<Edge> = outEdges.get(input)
            val nodeInfo = nodes[input] ?: throw AssertionError("should be registered: $rel")
            if (nodeInfo.rowEnumerable != null) {
                return EnumeratorSource(nodeInfo.rowEnumerable.enumerator())
            }
            assert(nodeInfo.sinks.size() === edges.size())
            val sink = nodeInfo.sinks[edge]
            if (sink != null) {
                return ListSource(sink.list)
            }
            throw IllegalStateException(
                "Got a sink $sink to which there is no match source type!"
            )
        }

        private fun getInput(rel: RelNode, ordinal: Int): RelNode {
            val inputs: List<RelNode>? = relInputs[rel]
            return inputs?.get(ordinal) ?: rel.getInput(ordinal)
        }

        @Override
        override fun sink(rel: RelNode): Sink {
            val edges: Collection<Edge> = outEdges.get(rel)
            val edges2 = if (edges.isEmpty()) ImmutableList.of(Edge(null, 0)) else edges
            var nodeInfo = nodes[rel]
            if (nodeInfo == null) {
                nodeInfo = NodeInfo(rel, null)
                nodes.put(rel, nodeInfo)
                for (edge in edges2) {
                    nodeInfo.sinks.put(edge, ListSink(ArrayDeque()))
                }
            } else {
                for (edge in edges2) {
                    if (nodeInfo.sinks.containsKey(edge)) {
                        continue
                    }
                    nodeInfo.sinks.put(edge, ListSink(ArrayDeque()))
                }
            }
            return if (edges.size() === 1) {
                Iterables.getOnlyElement(nodeInfo.sinks.values())
            } else {
                val queues: List<ArrayDeque<Row>> = ArrayList()
                for (sink in nodeInfo.sinks.values()) {
                    queues.add(sink.list)
                }
                DuplicatingSink(queues)
            }
        }

        @Override
        override fun enumerable(rel: RelNode, rowEnumerable: Enumerable<Row?>) {
            val nodeInfo = NodeInfo(rel, rowEnumerable)
            nodes.put(rel, nodeInfo)
        }

        @Override
        override fun createContext(): Context {
            return Context(getDataContext())
        }

        @Override
        fun getDataContext(): DataContext {
            return interpreter.dataContext
        }

        companion object {
            private const val REWRITE_METHOD_NAME = "rewrite"
            private const val VISIT_METHOD_NAME = "visit"
        }
    }

    /** Edge between a [RelNode] and one of its inputs.  */
    internal class Edge(@Nullable parent: RelNode?, ordinal: Int) : Pair<RelNode?, Integer?>(parent, ordinal)

    /** Converts a list of expressions to a scalar that can compute their
     * values.  */
    internal interface ScalarCompiler {
        fun compile(nodes: List<RexNode?>?, inputRowType: RelDataType?): Scalar.Producer
    }

    companion object {
        private fun optimize(rootRel: RelNode): RelNode {
            var rootRel: RelNode = rootRel
            val hepProgram: HepProgram = HepProgramBuilder()
                .addRuleInstance(CoreRules.CALC_SPLIT)
                .addRuleInstance(CoreRules.FILTER_SCAN)
                .addRuleInstance(CoreRules.FILTER_INTERPRETER_SCAN)
                .addRuleInstance(CoreRules.PROJECT_TABLE_SCAN)
                .addRuleInstance(CoreRules.PROJECT_INTERPRETER_TABLE_SCAN)
                .addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS)
                .build()
            val planner = HepPlanner(hepProgram)
            planner.setRoot(rootRel)
            rootRel = planner.findBestExp()
            return rootRel
        }
    }
}
