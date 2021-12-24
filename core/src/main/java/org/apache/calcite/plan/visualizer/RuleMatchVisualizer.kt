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
package org.apache.calcite.plan.visualizer

import org.apache.calcite.plan.RelOptCost
import org.apache.calcite.plan.RelOptListener
import org.apache.calcite.plan.RelOptPlanner
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.commons.io.IOUtils
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Charsets
import java.io.IOException
import java.io.InputStream
import java.io.UncheckedIOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.text.DecimalFormat
import java.text.MessageFormat
import java.util.ArrayList
import java.util.Arrays
import java.util.Collection
import java.util.Collections
import java.util.HashSet
import java.util.LinkedHashMap
import java.util.List
import java.util.Locale
import java.util.Map
import java.util.Objects
import java.util.Set
import java.util.stream.Collectors

/**
 * This is a tool to visualize the rule match process of a RelOptPlanner.
 *
 * <pre>`// create the visualizer
 * RuleMatchVisualizer viz = new RuleMatchVisualizer("/path/to/output/dir", "file-name-suffix");
 * viz.attachTo(planner)
 *
 * planner.findBestExpr();
 *
 * // extra step for HepPlanner: write the output to files
 * // a VolcanoPlanner will call it automatically
 * viz.writeToFile();
`</pre> *
 */
class RuleMatchVisualizer : RelOptListener {
    // default HTML template can be edited at
    // core/src/main/resources/org/apache/calcite/plan/visualizer/viz-template.html
    private val templateDirectory = "org/apache/calcite/plan/visualizer"

    @Nullable
    private val outputDirectory: String?

    @Nullable
    private val outputSuffix: String?
    private var latestRuleID = ""
    private var latestRuleTransformCount = 1
    private var initialized = false

    @Nullable
    private var planner: RelOptPlanner? = null
    private var includeTransitiveEdges = false
    private var includeIntermediateCosts = false
    private val steps: List<StepInfo> = ArrayList()
    private val allNodes: Map<String, NodeUpdateHelper> = LinkedHashMap()

    /**
     * Use this constructor to save the result on disk at the end of the planning phase.
     *
     *
     * Note: when using HepPlanner, [.writeToFile] needs to be called manually.
     *
     */
    constructor(
        outputDirectory: String?,
        outputSuffix: String?
    ) {
        this.outputDirectory = Objects.requireNonNull(outputDirectory, "outputDirectory")
        this.outputSuffix = Objects.requireNonNull(outputSuffix, "outputSuffix")
    }

    /**
     * Use this constructor when the result shall not be written to disk.
     */
    constructor() {
        outputDirectory = null
        outputSuffix = null
    }

    /**
     * Attaches the visualizer to the planner.
     * Must be called before applying the rules.
     * Must be called exactly once.
     */
    fun attachTo(planner: RelOptPlanner) {
        assert(this.planner == null)
        planner.addListener(this)
        this.planner = planner
    }

    /**
     * Output edges from a subset to the nodes of all subsets that satisfy it.
     */
    fun setIncludeTransitiveEdges(includeTransitiveEdges: Boolean) {
        this.includeTransitiveEdges = includeTransitiveEdges
    }

    /**
     * Output intermediate costs, including all cost updates.
     */
    fun setIncludeIntermediateCosts(includeIntermediateCosts: Boolean) {
        this.includeIntermediateCosts = includeIntermediateCosts
    }

    @Override
    fun ruleAttempted(event: RuleAttemptedEvent?) {
        // HepPlanner compatibility
        if (!initialized) {
            assert(planner != null)
            val root: RelNode = planner.getRoot()
            assert(root != null)
            initialized = true
            updateInitialPlan(root)
        }
    }

    /**
     * Register initial plan.
     * (Workaround for HepPlanner)
     */
    private fun updateInitialPlan(node: RelNode?) {
        if (node is HepRelVertex) {
            val v: HepRelVertex? = node as HepRelVertex?
            updateInitialPlan(v.getCurrentRel())
            return
        }
        registerRelNode(node)
        for (input in getInputs(node)) {
            updateInitialPlan(input)
        }
    }

    /**
     * Get the inputs for a node, unwrapping [HepRelVertex] nodes.
     * (Workaround for HepPlanner)
     */
    private fun getInputs(node: RelNode?): Collection<RelNode> {
        return node.getInputs().stream().map { n ->
            if (n is HepRelVertex) {
                return@map (n as HepRelVertex).getCurrentRel()
            }
            n
        }.collect(Collectors.toList())
    }

    @Override
    fun relChosen(event: RelChosenEvent) {
        if (event.getRel() == null) {
            assert(planner != null)
            val root: RelNode = planner.getRoot()
            assert(root != null)
            updateFinalPlan(root)
            addStep(FINAL, null)
            writeToFile()
        }
    }

    /**
     * Mark nodes that are part of the final plan.
     */
    private fun updateFinalPlan(node: RelNode?) {
        val size: Int = steps.size()
        if (size > 0 && FINAL.equals(steps[size - 1].getId())) {
            return
        }
        registerRelNode(node).updateAttribute("inFinalPlan", Boolean.TRUE)
        if (node is RelSubset) {
            val best: RelNode = (node as RelSubset?).getBest() ?: return
            updateFinalPlan(best)
        } else {
            for (input in getInputs(node)) {
                updateFinalPlan(input)
            }
        }
    }

    @Override
    fun ruleProductionSucceeded(event: RuleProductionEvent) {
        // method is called once before ruleMatch, and once after ruleMatch
        if (event.isBefore()) {
            // add the initialState
            if (latestRuleID.isEmpty()) {
                addStep(INITIAL, null)
                latestRuleID = INITIAL
            }
            return
        }

        // we add the state after the rule is applied
        val ruleCall: RelOptRuleCall = event.getRuleCall()
        val ruleID: String = Integer.toString(ruleCall.id)
        var displayRuleName: String = ruleCall.id + "-" + ruleCall.getRule()

        // a rule might call transform to multiple times, handle it by modifying the rule name
        if (ruleID.equals(latestRuleID)) {
            latestRuleTransformCount++
            displayRuleName += "-$latestRuleTransformCount"
        } else {
            latestRuleTransformCount = 1
        }
        latestRuleID = ruleID
        addStep(displayRuleName, ruleCall)
    }

    @Override
    fun relDiscarded(event: RelDiscardedEvent?) {
    }

    @Override
    fun relEquivalenceFound(event: RelEquivalenceEvent) {
        val rel: RelNode = event.getRel()
        assert(rel != null)
        val eqClass: Object = event.getEquivalenceClass()
        if (eqClass is String) {
            var eqClassStr = eqClass as String
            eqClassStr = eqClassStr.replace("equivalence class ", "")
            val setId = "set-$eqClassStr"
            registerSet(setId)
            registerRelNode(rel).updateAttribute("set", setId)
        }
        // register node
        registerRelNode(rel)
    }

    /**
     * Add a set.
     */
    private fun registerSet(setID: String) {
        allNodes.computeIfAbsent(setID) { k ->
            val h = NodeUpdateHelper(setID, null)
            h.updateAttribute("label", if (DEFAULT_SET.equals(setID)) "" else setID)
            h.updateAttribute("kind", "set")
            h
        }
    }

    /**
     * Add a RelNode to track its changes.
     */
    private fun registerRelNode(rel: RelNode?): NodeUpdateHelper {
        return allNodes.computeIfAbsent(key(rel)) { k ->
            val h = NodeUpdateHelper(key(rel), rel)
            // attributes that need to be set only once
            h.updateAttribute("label", getNodeLabel(rel))
            h.updateAttribute("explanation", getNodeExplanation(rel))
            h.updateAttribute("set", DEFAULT_SET)
            if (rel is RelSubset) {
                h.updateAttribute("kind", "subset")
            }
            h
        }
    }

    /**
     * Check and store the changes of the rel node.
     */
    private fun updateNodeInfo(rel: RelNode, isLastStep: Boolean) {
        val helper: NodeUpdateHelper = registerRelNode(rel)
        if (includeIntermediateCosts || isLastStep) {
            val planner: RelOptPlanner? = planner
            assert(planner != null)
            val mq: RelMetadataQuery = rel.getCluster().getMetadataQuery()
            val cost: RelOptCost = planner.getCost(rel, mq)
            val rowCount: Double = mq.getRowCount(rel)
            helper.updateAttribute("cost", formatCost(rowCount, cost))
        }
        val inputs: List<String> = ArrayList()
        if (rel is RelSubset) {
            val relSubset: RelSubset = rel as RelSubset
            relSubset.getRels().forEach { input -> inputs.add(key(input)) }
            val transitive: Set<String> = HashSet()
            relSubset.getSubsetsSatisfyingThis()
                .filter { other -> !other.equals(relSubset) }
                .forEach { input ->
                    inputs.add(key(input))
                    if (!includeTransitiveEdges) {
                        input.getRels().forEach { r -> transitive.add(key(r)) }
                    }
                }
            inputs.removeAll(transitive)
        } else {
            getInputs(rel).forEach { input -> inputs.add(key(input)) }
        }
        helper.updateAttribute("inputs", inputs)
    }

    /**
     * Add the updates since the last step to [.steps].
     */
    private fun addStep(stepID: String, @Nullable ruleCall: RelOptRuleCall?) {
        val nextNodeUpdates: Map<String?, Object> = LinkedHashMap()

        // HepPlanner compatibility
        val usesDefaultSet: Boolean = allNodes.values()
            .stream()
            .anyMatch { h -> DEFAULT_SET.equals(h.getValue("set")) }
        if (usesDefaultSet) {
            registerSet(DEFAULT_SET)
        }
        for (h in allNodes.values()) {
            val rel: RelNode = h.getRel()
            if (rel != null) {
                updateNodeInfo(rel, FINAL.equals(stepID))
            }
            if (h.isEmptyUpdate()) {
                continue
            }
            val update: Object = h.getAndResetUpdate()
            if (update != null) {
                nextNodeUpdates.put(h.getKey(), update)
            }
        }
        val matchedRels: List<String> = if (ruleCall == null) Collections.emptyList() else Arrays.stream(ruleCall.rels)
            .map { rel: RelNode? -> key(rel) }
            .collect(Collectors.toList())
        steps.add(StepInfo(stepID, nextNodeUpdates, matchedRels))
    }

    val jsonStringResult: String
        get() = try {
            val data: LinkedHashMap<String, Object> = LinkedHashMap()
            data.put("steps", steps)
            val objectMapper = ObjectMapper()
            var printer = DefaultPrettyPrinter()
            printer = printer.withoutSpacesInObjectEntries()
            objectMapper.writer(printer).writeValueAsString(data)
        } catch (e: JsonProcessingException) {
            throw RuntimeException(e)
        }

    /**
     * Writes the HTML and JS files of the rule match visualization.
     *
     *
     * The old files with the same name will be replaced.
     */
    fun writeToFile() {
        if (outputDirectory == null || outputSuffix == null) {
            return
        }
        try {
            val templatePath: String = Paths.get(templateDirectory).resolve("viz-template.html").toString()
            val cl: ClassLoader = getClass().getClassLoader()
            assert(cl != null)
            val resourceAsStream: InputStream = cl.getResourceAsStream(templatePath)
            assert(resourceAsStream != null)
            val htmlTemplate: String = IOUtils.toString(resourceAsStream, StandardCharsets.UTF_8)
            val htmlFileName = "planner-viz$outputSuffix.html"
            val dataFileName = "planner-viz-data$outputSuffix.js"
            val replaceString = "src=\"planner-viz-data.js\""
            val replaceIndex: Int = htmlTemplate.indexOf(replaceString)
            val htmlContent: String = (htmlTemplate.substring(0, replaceIndex)
                    + "src=\"" + dataFileName + "\""
                    + htmlTemplate.substring(replaceIndex + replaceString.length()))
            val dataJsContent = "var data = $jsonStringResult;\n"
            val outputDirPath: Path = Paths.get(outputDirectory)
            val htmlOutput: Path = outputDirPath.resolve(htmlFileName)
            val dataOutput: Path = outputDirPath.resolve(dataFileName)
            if (!Files.exists(outputDirPath)) {
                Files.createDirectories(outputDirPath)
            }
            Files.write(
                htmlOutput, htmlContent.getBytes(Charsets.UTF_8), StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
            )
            Files.write(
                dataOutput, dataJsContent.getBytes(Charsets.UTF_8), StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
            )
        } catch (e: IOException) {
            throw UncheckedIOException(e)
        }
    }

    //--------------------------------------------------------------------------------
    // methods related to string representation
    //--------------------------------------------------------------------------------
    private fun key(rel: RelNode?): String {
        return "" + rel.getId()
    }

    private fun getNodeLabel(relNode: RelNode?): String {
        if (relNode is RelSubset) {
            val relSubset: RelSubset? = relNode as RelSubset?
            val setId = getSetId(relSubset)
            return """
                subset#${relSubset.getId().toString()}-set$setId-
                ${relSubset.getTraitSet()}
                """.trimIndent()
        }
        return "#" + relNode.getId().toString() + "-" + relNode.getRelTypeName()
    }

    private fun getSetId(relSubset: RelSubset?): String {
        val explanation = getNodeExplanation(relSubset)
        val start: Int = explanation.indexOf("RelSubset") + "RelSubset".length()
        if (start < 0) {
            return ""
        }
        val end: Int = explanation.indexOf(".", start)
        return if (end < 0) {
            ""
        } else explanation.substring(start, end)
    }

    private fun getNodeExplanation(relNode: RelNode?): String {
        val relWriter = InputExcludedRelWriter()
        relNode.explain(relWriter)
        return relWriter.toString()
    }

    companion object {
        private const val INITIAL = "INITIAL"
        private const val FINAL = "FINAL"
        const val DEFAULT_SET = "default"
        private fun formatCost(rowCount: Double, @Nullable cost: RelOptCost?): String {
            if (cost == null) {
                return "null"
            }
            val originalStr: String = cost.toString()
            return if (originalStr.contains("inf") || originalStr.contains("huge")
                || originalStr.contains("tiny")
            ) {
                originalStr
            } else MessageFormat(
                "\nrowCount: {0}\nrows: {1}\ncpu:  {2}\nio:   {3}",
                Locale.ROOT
            ).format(
                arrayOf(
                    formatCostScientific(rowCount),
                    formatCostScientific(cost.getRows()),
                    formatCostScientific(cost.getCpu()),
                    formatCostScientific(cost.getIo())
                )
            )
        }

        private fun formatCostScientific(costNumber: Double): String {
            val costRounded: Long = Math.round(costNumber)
            val formatter: DecimalFormat = DecimalFormat.getInstance(Locale.ROOT) as DecimalFormat
            formatter.applyPattern("#.#############################################E0")
            return formatter.format(costRounded)
        }
    }
}
