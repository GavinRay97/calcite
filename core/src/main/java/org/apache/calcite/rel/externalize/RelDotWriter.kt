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
package org.apache.calcite.rel.externalize

import org.apache.calcite.plan.hep.HepRelVertex

/**
 * Utility to dump a rel node plan in dot format.
 */
@Value.Enclosing
class RelDotWriter(
    pw: PrintWriter, detailLevel: SqlExplainLevel,
    withIdPrefix: Boolean, private val option: WriteOption
) : RelWriterImpl(pw, detailLevel, withIdPrefix) {
    //~ Instance fields --------------------------------------------------------
    /**
     * Adjacent list of the plan graph.
     */
    private val outArcTable: Map<RelNode, List<RelNode>> = LinkedHashMap()
    private val nodeLabels: Map<RelNode, String> = HashMap()
    private val nodeStyles: Multimap<RelNode, String> = HashMultimap.create()

    //~ Constructors -----------------------------------------------------------
    constructor(
        pw: PrintWriter, detailLevel: SqlExplainLevel,
        withIdPrefix: Boolean
    ) : this(pw, detailLevel, withIdPrefix, WriteOption.DEFAULT) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    protected override fun explain_(
        rel: RelNode,
        values: List<Pair<String?, Object?>?>
    ) {
        // get inputs
        val inputs: List<RelNode> = getInputs(rel)
        outArcTable.put(rel, inputs)

        // generate node label
        val label = getRelNodeLabel(rel, values)
        nodeLabels.put(rel, label)
        if (highlightNode(rel)) {
            nodeStyles.put(rel, "bold")
        }
        explainInputs(inputs)
    }

    protected fun getRelNodeLabel(
        rel: RelNode,
        values: List<Pair<String?, Object?>?>
    ): String {
        val labels: List<String> = ArrayList()
        val sb = StringBuilder()
        val mq: RelMetadataQuery = rel.getCluster().getMetadataQuery()
        if (withIdPrefix) {
            sb.append(rel.getId()).append(":")
        }
        sb.append(rel.getRelTypeName())
        labels.add(sb.toString())
        sb.setLength(0)
        if (detailLevel !== SqlExplainLevel.NO_ATTRIBUTES) {
            for (value in values) {
                if (value.right is RelNode) {
                    continue
                }
                sb.append(value.left)
                    .append(" = ")
                    .append(value.right)
                labels.add(sb.toString())
                sb.setLength(0)
            }
        }
        when (detailLevel) {
            ALL_ATTRIBUTES -> sb.append("rowcount = ")
                .append(mq.getRowCount(rel))
                .append(" cumulative cost = ")
                .append(mq.getCumulativeCost(rel))
                .append(" ")
            else -> {}
        }
        when (detailLevel) {
            NON_COST_ATTRIBUTES, ALL_ATTRIBUTES -> if (!withIdPrefix) {
                // If we didn't print the rel id at the start of the line, print
                // it at the end.
                sb.append("id = ").append(rel.getId())
            }
            else -> {}
        }
        labels.add(sb.toString().trim())
        sb.setLength(0)

        // format labels separately and then concat them
        var leftSpace = option.maxNodeLabelLength()
        val newlabels: List<String> = ArrayList()
        for (i in 0 until labels.size()) {
            if (option.maxNodeLabelLength() != -1 && leftSpace <= 0) {
                if (i < labels.size() - 1) {
                    // this is not the last label, but we have to stop here
                    newlabels.add("...")
                }
                break
            }
            val formatted = formatNodeLabel(labels[i], option.maxNodeLabelLength())
            newlabels.add(formatted)
            leftSpace -= formatted.length()
        }
        return "\"" + String.join("\\n", newlabels).toString() + "\""
    }

    private fun explainInputs(inputs: List<RelNode?>) {
        for (input in inputs) {
            if (input == null || nodeLabels.containsKey(input)) {
                continue
            }
            input.explain(this)
        }
    }

    @Override
    override fun done(node: RelNode): RelWriter {
        val numOfVisitedNodes: Int = nodeLabels.size()
        super.done(node)
        if (numOfVisitedNodes == 0) {
            // When we enter this method call, no node
            // has been visited. So the current node must be the root of the plan.
            // Now we are exiting the method, all nodes in the plan
            // have been visited, so it is time to dump the plan.
            pw.println("digraph {")

            // print nodes with styles
            for (rel in nodeStyles.keySet()) {
                val style: String = String.join(",", nodeStyles.get(rel))
                pw.println(nodeLabels[rel] + " [style=\"" + style + "\"]")
            }

            // ordinary arcs
            for (entry in outArcTable.entrySet()) {
                val src: RelNode = entry.getKey()
                val srcDesc = nodeLabels[src]
                for (i in 0 until entry.getValue().size()) {
                    val dst: RelNode = entry.getValue().get(i)

                    // label is the ordinal of the arc
                    // arc direction from child to parent, to reflect the direction of data flow
                    pw.println(nodeLabels[dst] + " -> " + srcDesc + " [label=\"" + i + "\"]")
                }
            }
            pw.println("}")
            pw.flush()
        }
        return this
    }

    /**
     * Format the label into multiple lines according to the options.
     * @param label the original label.
     * @param limit the maximal length of the formatted label.
     * -1 means no limit.
     * @return the formatted label.
     */
    private fun formatNodeLabel(label: String, limit: Int): String {
        var label = label
        label = label.trim()

        // escape quotes in the label.
        label = label.replace("\"", "\\\"")
        var trimmed = false
        if (limit != -1 && label.length() > limit) {
            label = label.substring(0, limit)
            trimmed = true
        }
        if (option.maxNodeLabelPerLine() == -1) {
            // no need to split into multiple lines.
            return label + if (trimmed) "..." else ""
        }
        val descParts: List<String> = ArrayList()
        var idx = 0
        while (idx < label.length()) {
            val endIdx =
                if (idx + option.maxNodeLabelPerLine() > label.length()) label.length() else idx + option.maxNodeLabelPerLine()
            descParts.add(label.substring(idx, endIdx))
            idx += option.maxNodeLabelPerLine()
        }
        return String.join("\\n", descParts) + if (trimmed) "..." else ""
    }

    fun highlightNode(node: RelNode?): Boolean {
        val predicate: Predicate<RelNode>? = option.nodePredicate()
        return predicate != null && predicate.test(node)
    }

    /**
     * Options for displaying the rel node plan in dot format.
     */
    @Value.Immutable
    interface WriteOption {
        /**
         * The max length of node labels.
         * If the label is too long, the visual display would be messy.
         * -1 means no limit to the label length.
         */
        @Value.Default
        fun maxNodeLabelLength(): Int {
            return 100
        }

        /**
         * The max length of node label in a line.
         * -1 means no limitation.
         */
        @Value.Default
        fun maxNodeLabelPerLine(): Int {
            return 20
        }

        /**
         * Predicate for nodes that need to be highlighted.
         */
        @Nullable
        fun nodePredicate(): Predicate<RelNode?>?

        companion object {
            /** Default configuration.  */
            val DEFAULT: WriteOption = ImmutableRelDotWriter.WriteOption.of()
        }
    }

    companion object {
        private fun getInputs(parent: RelNode): List<RelNode> {
            return Util.transform(parent.getInputs()) { child ->
                if (child is HepRelVertex) {
                    return@transform (child as HepRelVertex).getCurrentRel()
                } else if (child is RelSubset) {
                    val subset: RelSubset = child as RelSubset
                    return@transform subset.getBestOrOriginal()
                } else {
                    return@transform child
                }
            }
        }
    }
}
