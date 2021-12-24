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
package org.apache.calcite.plan.volcano

import org.apache.calcite.avatica.util.Spaces
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.RelVisitor
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.util.PartiallyOrderedSet
import org.apache.calcite.util.Util
import com.google.common.collect.Ordering
import org.apiguardian.api.API
import java.io.PrintWriter
import java.io.StringWriter
import java.util.ArrayList
import java.util.Arrays
import java.util.Comparator
import java.util.HashSet
import java.util.Iterator
import java.util.List
import java.util.Map
import java.util.Set
import java.util.Objects.requireNonNull

/**
 * Utility class to dump state of `VolcanoPlanner`.
 */
@API(since = "1.23", status = API.Status.INTERNAL)
internal object Dumpers {
    /**
     * Returns a multi-line string describing the provenance of a tree of
     * relational expressions. For each node in the tree, prints the rule that
     * created the node, if any. Recursively describes the provenance of the
     * relational expressions that are the arguments to that rule.
     *
     *
     * Thus, every relational expression and rule invocation that affected
     * the final outcome is described in the provenance. This can be useful
     * when finding the root cause of "mistakes" in a query plan.
     *
     * @param provenanceMap The provenance map
     * @param root Root relational expression in a tree
     * @return Multi-line string describing the rules that created the tree
     */
    fun provenance(
        provenanceMap: Map<RelNode, VolcanoPlanner.Provenance>, root: RelNode?
    ): String {
        val sw = StringWriter()
        val pw = PrintWriter(sw)
        val nodes: List<RelNode> = ArrayList()
        object : RelVisitor() {
            @Override
            fun visit(node: RelNode?, ordinal: Int, @Nullable parent: RelNode?) {
                nodes.add(node)
                super.visit(node, ordinal, parent)
            } // CHECKSTYLE: IGNORE 1
        }.go(root)
        val visited: Set<RelNode> = HashSet()
        for (node in nodes) {
            provenanceRecurse(provenanceMap, pw, node, 0, visited)
        }
        pw.flush()
        return sw.toString()
    }

    private fun provenanceRecurse(
        provenanceMap: Map<RelNode, VolcanoPlanner.Provenance>,
        pw: PrintWriter, node: RelNode, i: Int, visited: Set<RelNode>
    ) {
        Spaces.append(pw, i * 2)
        if (!visited.add(node)) {
            pw.println("rel#" + node.getId().toString() + " (see above)")
            return
        }
        pw.println(node)
        val o: VolcanoPlanner.Provenance? = provenanceMap[node]
        Spaces.append(pw, i * 2 + 2)
        if (o === VolcanoPlanner.Provenance.EMPTY) {
            pw.println("no parent")
        } else if (o is VolcanoPlanner.DirectProvenance) {
            val rel: RelNode = (o as VolcanoPlanner.DirectProvenance?).source
            pw.println("direct")
            provenanceRecurse(provenanceMap, pw, rel, i + 2, visited)
        } else if (o is VolcanoPlanner.RuleProvenance) {
            val rule: VolcanoPlanner.RuleProvenance? = o as VolcanoPlanner.RuleProvenance?
            pw.println("call#" + rule.callId.toString() + " rule [" + rule.rule.toString() + "]")
            for (rel in rule.rels) {
                provenanceRecurse(provenanceMap, pw, rel, i + 2, visited)
            }
        } else if (o == null && node is RelSubset) {
            // A few operands recognize subsets, not individual rels.
            // The first rel in the subset is deemed to have created it.
            val subset: RelSubset = node
            pw.println("subset $subset")
            provenanceRecurse(
                provenanceMap, pw,
                subset.getRelList().get(0), i + 2, visited
            )
        } else {
            throw AssertionError("bad type $o")
        }
    }

    fun dumpSets(planner: VolcanoPlanner, pw: PrintWriter) {
        val ordering: Ordering<RelSet> = Ordering.from(Comparator.comparingInt { o -> o.id })
        for (set in ordering.immutableSortedCopy(planner.allSets)) {
            pw.println(
                "Set#" + set.id
                    .toString() + ", type: " + set.subsets.get(0).getRowType()
            )
            var j = -1
            for (subset in set.subsets) {
                ++j
                pw.println(
                    "\t" + subset + ", best="
                            + if (subset.best == null) "null" else "rel#" + subset.best.getId()
                )
                assert(subset.set === set)
                for (k in 0 until j) {
                    assert(
                        !set.subsets.get(k).getTraitSet().equals(
                            subset.getTraitSet()
                        )
                    )
                }
                for (rel in subset.getRels()) {
                    // "\t\trel#34:JavaProject(rel#32:JavaFilter(...), ...)"
                    pw.print("\t\t" + rel)
                    for (input in rel.getInputs()) {
                        val inputSubset: RelSubset = planner.getSubset(
                            input,
                            input.getTraitSet()
                        )
                        if (inputSubset == null) {
                            pw.append("no subset found for input ").print(input.getId())
                            continue
                        }
                        val inputSet: RelSet = inputSubset.set
                        if (input is RelSubset) {
                            val rels: Iterator<RelNode> = inputSubset.getRels().iterator()
                            if (rels.hasNext()) {
                                input = rels.next()
                                assert(input.getTraitSet().satisfies(inputSubset.getTraitSet()))
                                assert(inputSet.rels.contains(input))
                                assert(inputSet.subsets.contains(inputSubset))
                            }
                        }
                    }
                    if (planner.prunedNodes.contains(rel)) {
                        pw.print(", pruned")
                    }
                    val mq: RelMetadataQuery = rel.getCluster().getMetadataQuery()
                    pw.print(", rowcount=" + mq.getRowCount(rel))
                    pw.println(", cumulative cost=" + planner.getCost(rel, mq))
                }
            }
        }
    }

    fun dumpGraphviz(planner: VolcanoPlanner, pw: PrintWriter) {
        val ordering: Ordering<RelSet> = Ordering.from(Comparator.comparingInt { o -> o.id })
        val activeRels: Set<RelNode> = HashSet()
        for (volcanoRuleCall in planner.ruleCallStack) {
            activeRels.addAll(Arrays.asList(volcanoRuleCall.rels))
        }
        pw.println("digraph G {")
        pw.println("\troot [style=filled,label=\"Root\"];")
        val subsetPoset: PartiallyOrderedSet<RelSubset> =
            PartiallyOrderedSet { e1, e2 -> e1.getTraitSet().satisfies(e2.getTraitSet()) }
        val nonEmptySubsets: Set<RelSubset> = HashSet()
        for (set in ordering.immutableSortedCopy(planner.allSets)) {
            pw.print("\tsubgraph cluster")
            pw.print(set.id)
            pw.println("{")
            pw.print("\t\tlabel=")
            Util.printJavaString(
                pw, "Set " + set.id.toString() + " "
                        + set.subsets.get(0).getRowType(), false
            )
            pw.print(";\n")
            for (rel in set.rels) {
                pw.print("\t\trel")
                pw.print(rel.getId())
                pw.print(" [label=")
                val mq: RelMetadataQuery = rel.getCluster().getMetadataQuery()

                // Note: rel traitset could be different from its subset.traitset
                // It can happen due to RelTraitset#simplify
                // If the traits are different, we want to keep them on a graph
                val relSubset: RelSubset = planner.getSubset(rel)
                if (relSubset == null) {
                    pw.append("no subset found for rel")
                    continue
                }
                val traits = "." + relSubset.getTraitSet().toString()
                var title: String = rel.toString().replace(traits, "")
                if (title.endsWith(")")) {
                    val openParen: Int = title.indexOf('(')
                    if (openParen != -1) {
                        // Title is like rel#12:LogicalJoin(left=RelSubset#4,right=RelSubset#3,
                        // condition==($2, $0),joinType=inner)
                        // so we remove the parenthesis, and wrap parameters to the second line
                        // This avoids "too wide" Graphiz boxes, and makes the graph easier to follow
                        title = (title.substring(0, openParen) + '\n'
                                + title.substring(openParen + 1, title.length() - 1))
                    }
                }
                Util.printJavaString(
                    pw,
                    """
                        $title
                        rows=${mq.getRowCount(rel)}, cost=${planner.getCost(rel, mq)}
                        """.trimIndent(), false
                )
                if (rel !is AbstractConverter) {
                    nonEmptySubsets.add(relSubset)
                }
                if (relSubset.best === rel) {
                    pw.print(",color=blue")
                }
                if (activeRels.contains(rel)) {
                    pw.print(",style=dashed")
                }
                pw.print(",shape=box")
                pw.println("]")
            }
            subsetPoset.clear()
            for (subset in set.subsets) {
                subsetPoset.add(subset)
                pw.print("\t\tsubset")
                pw.print(subset.getId())
                pw.print(" [label=")
                Util.printJavaString(pw, subset.toString(), false)
                var empty = !nonEmptySubsets.contains(subset)
                if (empty) {
                    // We don't want to iterate over rels when we know the set is not empty
                    for (rel in subset.getRels()) {
                        if (rel !is AbstractConverter) {
                            empty = false
                            break
                        }
                    }
                    if (empty) {
                        pw.print(",color=red")
                    }
                }
                if (activeRels.contains(subset)) {
                    pw.print(",style=dashed")
                }
                pw.print("]\n")
            }
            for (subset in subsetPoset) {
                val children: List<RelSubset> = subsetPoset.getChildren(subset) ?: continue
                for (parent in children) {
                    pw.print("\t\tsubset")
                    pw.print(subset.getId())
                    pw.print(" -> subset")
                    pw.print(parent.getId())
                    pw.print(";")
                }
            }
            pw.print("\t}\n")
        }
        // Note: it is important that all the links are declared AFTER declaration of the nodes
        // Otherwise Graphviz creates nodes implicitly, and puts them into a wrong cluster
        pw.print("\troot -> subset")
        pw.print(requireNonNull(planner.root, "planner.root").getId())
        pw.println(";")
        for (set in ordering.immutableSortedCopy(planner.allSets)) {
            for (rel in set.rels) {
                val relSubset: RelSubset = planner.getSubset(rel)
                if (relSubset == null) {
                    pw.append("no subset found for rel ").print(rel.getId())
                    continue
                }
                pw.print("\tsubset")
                pw.print(relSubset.getId())
                pw.print(" -> rel")
                pw.print(rel.getId())
                if (relSubset.best === rel) {
                    pw.print("[color=blue]")
                }
                pw.print(";")
                val inputs: List<RelNode> = rel.getInputs()
                for (i in 0 until inputs.size()) {
                    val input: RelNode = inputs[i]
                    pw.print(" rel")
                    pw.print(rel.getId())
                    pw.print(" -> ")
                    pw.print(if (input is RelSubset) "subset" else "rel")
                    pw.print(input.getId())
                    if (relSubset.best === rel || inputs.size() > 1) {
                        var sep = '['
                        if (relSubset.best === rel) {
                            pw.print(sep)
                            pw.print("color=blue")
                            sep = ','
                        }
                        if (inputs.size() > 1) {
                            pw.print(sep)
                            pw.print("label=\"")
                            pw.print(i)
                            pw.print("\"")
                            // sep = ',';
                        }
                        pw.print(']')
                    }
                    pw.print(";")
                }
                pw.println()
            }
        }

        // Draw lines for current rules
        for (ruleCall in planner.ruleCallStack) {
            pw.print("rule")
            pw.print(ruleCall.id)
            pw.print(" [style=dashed,label=")
            Util.printJavaString(pw, ruleCall.rule.toString(), false)
            pw.print("]")
            val rels: Array<RelNode> = ruleCall.rels
            for (i in rels.indices) {
                val rel: RelNode = rels[i]
                pw.print(" rule")
                pw.print(ruleCall.id)
                pw.print(" -> ")
                pw.print(if (rel is RelSubset) "subset" else "rel")
                pw.print(rel.getId())
                pw.print(" [style=dashed")
                if (rels.size > 1) {
                    pw.print(",label=\"")
                    pw.print(i)
                    pw.print("\"")
                }
                pw.print("]")
                pw.print(";")
            }
            pw.println()
        }
        pw.print("}")
    }
}
