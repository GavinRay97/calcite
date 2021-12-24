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
package org.apache.calcite.sql2rel

import org.apache.calcite.linq4j.Ord

/**
 * RelDecorrelator replaces all correlated expressions (corExp) in a relational
 * expression (RelNode) tree with non-correlated expressions that are produced
 * from joining the RelNode that produces the corExp with the RelNode that
 * references it.
 *
 *
 * TODO:
 *
 *  * replace `CorelMap` constructor parameter with a RelNode
 *  * make [.currentRel] immutable (would require a fresh
 * RelDecorrelator for each node being decorrelated)
 *  * make fields of `CorelMap` immutable
 *  * make sub-class rules static, and have them create their own
 * de-correlator
 *
 *
 *
 * Note: make all the members protected scope so that they can be
 * accessed by the sub-class.
 */
class RelDecorrelator protected constructor(
    // map built during translation
    protected var cm: CorelMap,
    context: Context,
    relBuilder: RelBuilder
) : ReflectiveVisitor {
    //~ Instance fields --------------------------------------------------------
    protected val relBuilder: RelBuilder

    @SuppressWarnings("method.invocation.invalid")
    protected val dispatcher: ReflectUtil.MethodDispatcher<Frame> =
        ReflectUtil.< RelNode, Frame>createMethodDispatcher<RelNode?, org.apache.calcite.sql2rel.RelDecorrelator.Frame?>(
    org.apache.calcite.sql2rel.RelDecorrelator.Frame::
    class.java, getVisitor(), "decorrelateRel",
    RelNode::
    class.java,
    Boolean::
    class.javaPrimitiveType)
    // The rel which is being visited
    @Nullable
    protected var currentRel: RelNode? = null
    protected val context: Context

    /** Built during decorrelation, of rel to all the newly created correlated
     * variables in its output, and to map old input positions to new input
     * positions. This is from the view point of the parent rel of a new rel.  */
    protected val map: Map<RelNode?, Frame> = HashMap()
    protected val generatedCorRels: HashSet<Correlate> = HashSet()

    //~ Constructors -----------------------------------------------------------
    init {
        this.context = context
        this.relBuilder = relBuilder
    }

    private fun setCurrent(@Nullable root: RelNode, @Nullable corRel: Correlate?) {
        currentRel = corRel
        if (corRel != null) {
            cm = CorelMapBuilder().build(Util.first(root, corRel))
        }
    }

    protected fun relBuilderFactory(): RelBuilderFactory {
        return RelBuilder.proto(relBuilder)
    }

    protected fun decorrelate(root: RelNode): RelNode {
        // first adjust count() expression if any
        var root: RelNode = root
        val f: RelBuilderFactory = relBuilderFactory()
        val program: HepProgram = HepProgram.builder()
            .addRuleInstance(
                AdjustProjectForCountAggregateRule.config(false, this, f).toRule()
            )
            .addRuleInstance(
                AdjustProjectForCountAggregateRule.config(true, this, f).toRule()
            )
            .addRuleInstance(
                FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig.DEFAULT
                    .withRelBuilderFactory(f)
                    .withOperandSupplier { b0 ->
                        b0.operand(Filter::class.java).oneInput { b1 -> b1.operand(Join::class.java).anyInputs() }
                    }
                    .withDescription("FilterJoinRule:filter")
                    .`as`(FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig::class.java)
                    .withSmart(true)
                    .withPredicate { join, joinType, exp -> true }
                    .`as`(FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig::class.java)
                    .toRule())
            .addRuleInstance(
                CoreRules.FILTER_PROJECT_TRANSPOSE.config
                    .withRelBuilderFactory(f)
                    .`as`(FilterProjectTransposeRule.Config::class.java)
                    .withOperandFor(
                        Filter::class.java, { filter -> !RexUtil.containsCorrelation(filter.getCondition()) },
                        Project::class.java
                    ) { project -> true }
                    .withCopyFilter(true)
                    .withCopyProject(true)
                    .toRule())
            .addRuleInstance(
                FilterCorrelateRule.Config.DEFAULT
                    .withRelBuilderFactory(f)
                    .toRule()
            )
            .addRuleInstance(
                FilterFlattenCorrelatedConditionRule.Config.DEFAULT
                    .withRelBuilderFactory(f)
                    .toRule()
            )
            .build()
        val planner: HepPlanner = createPlanner(program)
        planner.setRoot(root)
        root = planner.findBestExp()
        if (SQL2REL_LOGGER.isDebugEnabled()) {
            SQL2REL_LOGGER.debug(
                """
                    Plan before extracting correlated computations:
                    ${RelOptUtil.toString(root)}
                    """.trimIndent()
            )
        }
        root = root.accept(CorrelateProjectExtractor(f))
        // Necessary to update cm (CorrelMap) since CorrelateProjectExtractor above may modify the plan
        cm = CorelMapBuilder().build(root)
        if (SQL2REL_LOGGER.isDebugEnabled()) {
            SQL2REL_LOGGER.debug(
                """
                    Plan after extracting correlated computations:
                    ${RelOptUtil.toString(root)}
                    """.trimIndent()
            )
        }
        // Perform decorrelation.
        map.clear()
        val frame = getInvoke(root, false, null)
        if (frame != null) {
            // has been rewritten; apply rules post-decorrelation
            val builder: HepProgramBuilder = HepProgram.builder()
                .addRuleInstance(
                    CoreRules.FILTER_INTO_JOIN.config
                        .withRelBuilderFactory(f)
                        .toRule()
                )
                .addRuleInstance(
                    CoreRules.JOIN_CONDITION_PUSH.config
                        .withRelBuilderFactory(f)
                        .toRule()
                )
            if (!postDecorrelateRules.isEmpty()) {
                builder.addRuleCollection(postDecorrelateRules)
            }
            val program2: HepProgram = builder.build()
            val planner2: HepPlanner = createPlanner(program2)
            val newRoot: RelNode = frame.r
            planner2.setRoot(newRoot)
            return planner2.findBestExp()
        }
        return root
    }

    private fun createCopyHook(): Function2<RelNode, RelNode, Void> {
        return Function2<RelNode, RelNode, Void> { oldNode, newNode ->
            if (cm.mapRefRelToCorRef.containsKey(oldNode)) {
                cm.mapRefRelToCorRef.putAll(
                    newNode,
                    cm.mapRefRelToCorRef.get(oldNode)
                )
            }
            if (oldNode is Correlate
                && newNode is Correlate
            ) {
                val oldCor: Correlate = oldNode as Correlate
                val c: CorrelationId = oldCor.getCorrelationId()
                if (cm.mapCorToCorRel.get(c) === oldNode) {
                    cm.mapCorToCorRel.put(c, newNode)
                }
                if (generatedCorRels.contains(oldNode)) {
                    generatedCorRels.add(newNode as Correlate)
                }
            }
            null
        }
    }

    private fun createPlanner(program: HepProgram): HepPlanner {
        // Create a planner with a hook to update the mapping tables when a
        // node is copied when it is registered.
        return HepPlanner(
            program,
            context,
            true,
            createCopyHook(),
            RelOptCostImpl.FACTORY
        )
    }

    fun removeCorrelationViaRule(root: RelNode?): RelNode {
        val f: RelBuilderFactory = relBuilderFactory()
        val program: HepProgram = HepProgram.builder()
            .addRuleInstance(RemoveSingleAggregateRule.config(f).toRule())
            .addRuleInstance(
                RemoveCorrelationForScalarProjectRule.config(this, f).toRule()
            )
            .addRuleInstance(
                RemoveCorrelationForScalarAggregateRule.config(this, f).toRule()
            )
            .build()
        val planner: HepPlanner = createPlanner(program)
        planner.setRoot(root)
        return planner.findBestExp()
    }

    protected fun decorrelateExpr(
        currentRel: RelNode,
        map: Map<RelNode?, Frame>, cm: CorelMap, exp: RexNode
    ): RexNode {
        val shuttle = DecorrelateRexShuttle(currentRel, map, cm)
        return exp.accept(shuttle)
    }

    protected fun removeCorrelationExpr(
        exp: RexNode,
        projectPulledAboveLeftCorrelator: Boolean
    ): RexNode {
        val shuttle: RemoveCorrelationRexShuttle = RemoveCorrelationRexShuttle(
            relBuilder.getRexBuilder(),
            projectPulledAboveLeftCorrelator, null, ImmutableSet.of()
        )
        return exp.accept(shuttle)
    }

    protected fun removeCorrelationExpr(
        exp: RexNode,
        projectPulledAboveLeftCorrelator: Boolean,
        nullIndicator: RexInputRef?
    ): RexNode {
        val shuttle: RemoveCorrelationRexShuttle = RemoveCorrelationRexShuttle(
            relBuilder.getRexBuilder(),
            projectPulledAboveLeftCorrelator, nullIndicator,
            ImmutableSet.of()
        )
        return exp.accept(shuttle)
    }

    protected fun removeCorrelationExpr(
        exp: RexNode,
        projectPulledAboveLeftCorrelator: Boolean,
        isCount: Set<Integer?>?
    ): RexNode {
        val shuttle: RemoveCorrelationRexShuttle = RemoveCorrelationRexShuttle(
            relBuilder.getRexBuilder(),
            projectPulledAboveLeftCorrelator, null, isCount
        )
        return exp.accept(shuttle)
    }

    /** Fallback if none of the other `decorrelateRel` methods match.  */
    @Nullable
    fun decorrelateRel(rel: RelNode, isCorVarDefined: Boolean): Frame? {
        var newRel: RelNode = rel.copy(rel.getTraitSet(), rel.getInputs())
        if (rel.getInputs().size() > 0) {
            val oldInputs: List<RelNode> = rel.getInputs()
            val newInputs: List<RelNode> = ArrayList()
            for (i in 0 until oldInputs.size()) {
                val frame = getInvoke(oldInputs[i], isCorVarDefined, rel)
                if (frame == null || !frame.corDefOutputs.isEmpty()) {
                    // if input is not rewritten, or if it produces correlated
                    // variables, terminate rewrite
                    return null
                }
                newInputs.add(frame.r)
                newRel.replaceInput(i, frame.r)
            }
            if (!Util.equalShallow(oldInputs, newInputs)) {
                newRel = rel.copy(rel.getTraitSet(), newInputs)
            }
        }

        // the output position should not change since there are no corVars
        // coming from below.
        return register(
            rel, newRel, identityMap(rel.getRowType().getFieldCount()),
            ImmutableSortedMap.of()
        )
    }

    @Nullable
    fun decorrelateRel(rel: Sort, isCorVarDefined: Boolean): Frame? {
        //
        // Rewrite logic:
        //
        // 1. change the collations field to reference the new input.
        //

        // Sort itself should not reference corVars.
        assert(!cm.mapRefRelToCorRef.containsKey(rel))

        // Sort only references field positions in collations field.
        // The collations field in the newRel now need to refer to the
        // new output positions in its input.
        // Its output does not change the input ordering, so there's no
        // need to call propagateExpr.
        val oldInput: RelNode = rel.getInput()
        val frame = getInvoke(oldInput, isCorVarDefined, rel)
            ?: // If input has not been rewritten, do not rewrite this rel.
            return null
        val newInput: RelNode = frame.r
        val mapping: Mappings.TargetMapping = Mappings.target(
            frame.oldToNewOutputs,
            oldInput.getRowType().getFieldCount(),
            newInput.getRowType().getFieldCount()
        )
        val oldCollation: RelCollation = rel.getCollation()
        val newCollation: RelCollation = RexUtil.apply(mapping, oldCollation)
        val offset = if (rel.offset == null) -1 else RexLiteral.intValue(rel.offset)
        val fetch = if (rel.fetch == null) -1 else RexLiteral.intValue(rel.fetch)
        val newSort: RelNode = relBuilder
            .push(newInput)
            .sortLimit(offset, fetch, relBuilder.fields(newCollation))
            .build()

        // Sort does not change input ordering
        return register(rel, newSort, frame.oldToNewOutputs, frame.corDefOutputs)
    }

    @Nullable
    fun decorrelateRel(rel: Values?, isCorVarDefined: Boolean): Frame? {
        // There are no inputs, so rel does not need to be changed.
        return null
    }

    @Nullable
    fun decorrelateRel(rel: LogicalAggregate?, isCorVarDefined: Boolean): Frame {
        return decorrelateRel(rel as Aggregate?, isCorVarDefined)
    }

    @Nullable
    fun decorrelateRel(rel: Aggregate, isCorVarDefined: Boolean): Frame? {
        //
        // Rewrite logic:
        //
        // 1. Permute the group by keys to the front.
        // 2. If the input of an aggregate produces correlated variables,
        //    add them to the group list.
        // 3. Change aggCalls to reference the new project.
        //

        // Aggregate itself should not reference corVars.
        assert(!cm.mapRefRelToCorRef.containsKey(rel))
        val oldInput: RelNode = rel.getInput()
        val frame = getInvoke(oldInput, isCorVarDefined, rel)
            ?: // If input has not been rewritten, do not rewrite this rel.
            return null
        val newInput: RelNode = frame.r

        // aggregate outputs mapping: group keys and aggregates
        val outputMap: Map<Integer?, Integer> = HashMap()

        // map from newInput
        val mapNewInputToProjOutputs: Map<Integer, Integer> = HashMap()
        val oldGroupKeyCount: Int = rel.getGroupSet().cardinality()

        // Project projects the original expressions,
        // plus any correlated variables the input wants to pass along.
        val projects: List<Pair<RexNode, String>> = ArrayList()
        val newInputOutput: List<RelDataTypeField> = newInput.getRowType().getFieldList()
        var newPos = 0

        // oldInput has the original group by keys in the front.
        val omittedConstants: NavigableMap<Integer, RexLiteral> = TreeMap()
        for (i in 0 until oldGroupKeyCount) {
            val constant: RexLiteral? = projectedLiteral(newInput, i)
            if (constant != null) {
                // Exclude constants. Aggregate({true}) occurs because Aggregate({})
                // would generate 1 row even when applied to an empty table.
                omittedConstants.put(i, constant)
                continue
            }

            // add mapping of group keys.
            outputMap.put(i, newPos)
            val newInputPos: Int = frame.oldToNewOutputs.get(i)
            projects.add(RexInputRef.of2(newInputPos, newInputOutput))
            mapNewInputToProjOutputs.put(newInputPos, newPos)
            newPos++
        }
        val corDefOutputs: NavigableMap<CorDef?, Integer?> = TreeMap()
        if (!frame.corDefOutputs.isEmpty()) {
            // If input produces correlated variables, move them to the front,
            // right after any existing GROUP BY fields.

            // Now add the corVars from the input, starting from
            // position oldGroupKeyCount.
            for (entry in frame.corDefOutputs.entrySet()) {
                projects.add(RexInputRef.of2(entry.getValue(), newInputOutput))
                corDefOutputs.put(entry.getKey(), newPos)
                mapNewInputToProjOutputs.put(entry.getValue(), newPos)
                newPos++
            }
        }

        // add the remaining fields
        val newGroupKeyCount = newPos
        for (i in 0 until newInputOutput.size()) {
            if (!mapNewInputToProjOutputs.containsKey(i)) {
                projects.add(RexInputRef.of2(i, newInputOutput))
                mapNewInputToProjOutputs.put(i, newPos)
                newPos++
            }
        }
        assert(newPos == newInputOutput.size())

        // This Project will be what the old input maps to,
        // replacing any previous mapping from old input).
        var newProject: RelNode = relBuilder.push(newInput)
            .projectNamed(Pair.left(projects), Pair.right(projects), true)
            .build()
        newProject = RelOptUtil.copyRelHints(newInput, newProject)

        // update mappings:
        // oldInput ----> newInput
        //
        //                newProject
        //                   |
        // oldInput ----> newInput
        //
        // is transformed to
        //
        // oldInput ----> newProject
        //                   |
        //                newInput
        val combinedMap: Map<Integer?, Integer> = HashMap()
        for (entry in frame.oldToNewOutputs.entrySet()) {
            combinedMap.put(entry.getKey(),
                requireNonNull(
                    mapNewInputToProjOutputs[entry.getValue()]
                ) { "mapNewInputToProjOutputs.get(" + entry.getValue().toString() + ")" })
        }
        register(oldInput, newProject, combinedMap, corDefOutputs)

        // now it's time to rewrite the Aggregate
        val newGroupSet: ImmutableBitSet = ImmutableBitSet.range(newGroupKeyCount)
        val newAggCalls: List<AggregateCall> = ArrayList()
        val oldAggCalls: List<AggregateCall> = rel.getAggCallList()
        val newGroupSets: Iterable<ImmutableBitSet>?
        newGroupSets = if (rel.getGroupType() === Aggregate.Group.SIMPLE) {
            null
        } else {
            val addedGroupSet: ImmutableBitSet = ImmutableBitSet.range(oldGroupKeyCount, newGroupKeyCount)
            ImmutableBitSet.ORDERING.immutableSortedCopy(
                Util.transform(
                    rel.getGroupSets()
                ) { bitSet -> bitSet.union(addedGroupSet) })
        }
        val oldInputOutputFieldCount: Int = rel.getGroupSet().cardinality()
        val newInputOutputFieldCount: Int = newGroupSet.cardinality()
        var i = -1
        for (oldAggCall in oldAggCalls) {
            ++i
            val oldAggArgs: List<Integer> = oldAggCall.getArgList()
            val aggArgs: List<Integer> = ArrayList()

            // Adjust the Aggregate argument positions.
            // Note Aggregate does not change input ordering, so the input
            // output position mapping can be used to derive the new positions
            // for the argument.
            for (oldPos in oldAggArgs) {
                aggArgs.add(
                    requireNonNull(
                        combinedMap[oldPos]
                    ) { "combinedMap.get($oldPos)" })
            }
            val filterArg: Int = if (oldAggCall.filterArg < 0) oldAggCall.filterArg else requireNonNull(
                combinedMap[oldAggCall.filterArg]
            ) { "combinedMap.get(" + oldAggCall.filterArg.toString() + ")" }
            newAggCalls.add(
                oldAggCall.adaptTo(
                    newProject, aggArgs, filterArg,
                    oldGroupKeyCount, newGroupKeyCount
                )
            )

            // The old to new output position mapping will be the same as that
            // of newProject, plus any aggregates that the oldAgg produces.
            outputMap.put(
                oldInputOutputFieldCount + i,
                newInputOutputFieldCount + i
            )
        }
        relBuilder.push(newProject)
            .aggregate(
                if (newGroupSets == null) relBuilder.groupKey(newGroupSet) else relBuilder.groupKey(
                    newGroupSet,
                    newGroupSets
                ),
                newAggCalls
            )
        if (!omittedConstants.isEmpty()) {
            val postProjects: List<RexNode> = ArrayList(relBuilder.fields())
            for (entry in omittedConstants.descendingMap().entrySet()) {
                val index: Int = entry.getKey() + frame.corDefOutputs.size()
                postProjects.add(index, entry.getValue())
                // Shift the outputs whose index equals with or bigger than the added index
                // with 1 offset.
                shiftMapping(outputMap, index, 1)
                // Then add the constant key mapping.
                outputMap.put(entry.getKey(), index)
            }
            relBuilder.project(postProjects)
        }
        val newRel: RelNode = RelOptUtil.copyRelHints(rel, relBuilder.build())

        // Aggregate does not change input ordering so corVars will be
        // located at the same position as the input newProject.
        return register(rel, newRel, outputMap, corDefOutputs)
    }

    @Nullable
    fun getInvoke(r: RelNode, isCorVarDefined: Boolean, @Nullable parent: RelNode?): Frame? {
        val frame: Frame = dispatcher.invoke(r, isCorVarDefined)
        currentRel = parent
        if (frame != null && isCorVarDefined && r is Sort) {
            val sort: Sort = r as Sort
            // Can not decorrelate if the sort has per-correlate-key attributes like
            // offset or fetch limit, because these attributes scope would change to
            // global after decorrelation. They should take effect within the scope
            // of the correlation key actually.
            if (sort.offset != null || sort.fetch != null) {
                return null
            }
        }
        if (frame != null) {
            map.put(r, frame)
        }
        return frame
    }

    @Nullable
    fun decorrelateRel(rel: LogicalProject?, isCorVarDefined: Boolean): Frame {
        return decorrelateRel(rel as Project?, isCorVarDefined)
    }

    @Nullable
    fun decorrelateRel(rel: Project, isCorVarDefined: Boolean): Frame? {
        //
        // Rewrite logic:
        //
        // 1. Pass along any correlated variables coming from the input.
        //
        val oldInput: RelNode = rel.getInput()
        var frame = getInvoke(oldInput, isCorVarDefined, rel)
            ?: // If input has not been rewritten, do not rewrite this rel.
            return null
        val oldProjects: List<RexNode> = rel.getProjects()
        val relOutput: List<RelDataTypeField> = rel.getRowType().getFieldList()

        // Project projects the original expressions,
        // plus any correlated variables the input wants to pass along.
        val projects: List<Pair<RexNode, String>> = ArrayList()

        // If this Project has correlated reference, create value generator
        // and produce the correlated variables in the new output.
        if (cm.mapRefRelToCorRef.containsKey(rel)) {
            frame = decorrelateInputWithValueGenerator(rel, frame)
        }

        // Project projects the original expressions
        val mapOldToNewOutputs: Map<Integer?, Integer> = HashMap()
        var newPos: Int
        newPos = 0
        while (newPos < oldProjects.size()) {
            projects.add(
                newPos,
                Pair.of(
                    decorrelateExpr(
                        requireNonNull(currentRel, "currentRel"),
                        map, cm, oldProjects[newPos]
                    ),
                    relOutput[newPos].getName()
                )
            )
            mapOldToNewOutputs.put(newPos, newPos)
            newPos++
        }

        // Project any correlated variables the input wants to pass along.
        val corDefOutputs: NavigableMap<CorDef?, Integer?> = TreeMap()
        for (entry in frame.corDefOutputs.entrySet()) {
            projects.add(
                RexInputRef.of2(
                    entry.getValue(),
                    frame.r.getRowType().getFieldList()
                )
            )
            corDefOutputs.put(entry.getKey(), newPos)
            newPos++
        }
        var newProject: RelNode = relBuilder.push(frame.r)
            .projectNamed(Pair.left(projects), Pair.right(projects), true)
            .build()
        newProject = RelOptUtil.copyRelHints(rel, newProject)
        return register(rel, newProject, mapOldToNewOutputs, corDefOutputs)
    }

    /**
     * Create RelNode tree that produces a list of correlated variables.
     *
     * @param correlations         correlated variables to generate
     * @param valueGenFieldOffset  offset in the output that generated columns
     * will start
     * @param corDefOutputs        output positions for the correlated variables
     * generated
     * @return RelNode the root of the resultant RelNode tree
     */
    @Nullable
    private fun createValueGenerator(
        correlations: Iterable<CorRef>,
        valueGenFieldOffset: Int,
        corDefOutputs: NavigableMap<CorDef?, Integer?>
    ): RelNode? {
        val mapNewInputToOutputs: Map<RelNode?, List<Integer>> = HashMap()
        val mapNewInputToNewOffset: Map<RelNode?, Integer> = HashMap()

        // Input provides the definition of a correlated variable.
        // Add to map all the referenced positions (relative to each input rel).
        for (corVar in correlations) {
            val oldCorVarOffset = corVar.field
            val oldInput: RelNode = getCorRel(corVar)
            assert(oldInput != null)
            val frame = getOrCreateFrame(oldInput)
            assert(frame != null)
            val newInput: RelNode = frame.r
            val newLocalOutputs: List<Integer>
            if (!mapNewInputToOutputs.containsKey(newInput)) {
                newLocalOutputs = ArrayList()
            } else {
                newLocalOutputs = mapNewInputToOutputs[newInput]!!
            }
            val newCorVarOffset: Int = frame.oldToNewOutputs.get(oldCorVarOffset)

            // Add all unique positions referenced.
            if (!newLocalOutputs.contains(newCorVarOffset)) {
                newLocalOutputs.add(newCorVarOffset)
            }
            mapNewInputToOutputs.put(newInput, newLocalOutputs)
        }
        var offset = 0

        // Project only the correlated fields out of each input
        // and join the project together.
        // To make sure the plan does not change in terms of join order,
        // join these rels based on their occurrence in corVar list which
        // is sorted.
        val joinedInputs: Set<RelNode?> = HashSet()
        var r: RelNode? = null
        for (corVar in correlations) {
            val oldInput: RelNode = getCorRel(corVar)
            assert(oldInput != null)
            val newInput: RelNode = getOrCreateFrame(oldInput).r
            assert(newInput != null)
            if (!joinedInputs.contains(newInput)) {
                val positions: List<Integer> = requireNonNull(
                    mapNewInputToOutputs[newInput]
                ) { "mapNewInputToOutputs.get($newInput)" }
                val distinct: RelNode = relBuilder.push(newInput)
                    .project(relBuilder.fields(positions))
                    .distinct()
                    .build()
                val cluster: RelOptCluster = distinct.getCluster()
                joinedInputs.add(newInput)
                mapNewInputToNewOffset.put(newInput, offset)
                offset += distinct.getRowType().getFieldCount()
                r = if (r == null) {
                    distinct
                } else {
                    relBuilder.push(r).push(distinct)
                        .join(JoinRelType.INNER, cluster.getRexBuilder().makeLiteral(true)).build()
                }
            }
        }

        // Translate the positions of correlated variables to be relative to
        // the join output, leaving room for valueGenFieldOffset because
        // valueGenerators are joined with the original left input of the rel
        // referencing correlated variables.
        for (corRef in correlations) {
            // The first input of a Correlate is always the rel defining
            // the correlated variables.
            val oldInput: RelNode = getCorRel(corRef)
            assert(oldInput != null)
            val frame = getOrCreateFrame(oldInput)
            val newInput: RelNode = frame.r
            assert(newInput != null)
            val newLocalOutputs: List<Integer> = requireNonNull(
                mapNewInputToOutputs[newInput]
            ) { "mapNewInputToOutputs.get($newInput)" }
            val newLocalOutput: Int = frame.oldToNewOutputs.get(corRef.field)

            // newOutput is the index of the corVar in the referenced
            // position list plus the offset of referenced position list of
            // each newInput.
            val newOutput: Int = (newLocalOutputs.indexOf(newLocalOutput)
                    + requireNonNull(
                mapNewInputToNewOffset[newInput]
            ) { "mapNewInputToNewOffset.get($newInput)" }
                    + valueGenFieldOffset)
            corDefOutputs.put(corRef.def(), newOutput)
        }
        return r
    }

    private fun getOrCreateFrame(r: RelNode?): Frame {
        return getFrame(r)
            ?: return Frame(
                r, r, ImmutableSortedMap.of(),
                identityMap(r.getRowType().getFieldCount())
            )
    }

    @Nullable
    private fun getFrame(r: RelNode?): Frame? {
        return map[r]
    }

    private fun getCorRel(corVar: CorRef): RelNode {
        val r: RelNode = requireNonNull(
            cm.mapCorToCorRel.get(corVar.corr)
        ) { "cm.mapCorToCorRel.get(" + corVar.corr + ")" }
        return requireNonNull(
            r.getInput(0)
        ) { "r.getInput(0) is null for $r" }
    }

    /** Adds a value generator to satisfy the correlating variables used by
     * a relational expression, if those variables are not already provided by
     * its input.  */
    private fun maybeAddValueGenerator(rel: RelNode, frame: Frame): Frame {
        val cm1 = CorelMapBuilder().build(frame.r, rel)
        if (!cm1.mapRefRelToCorRef.containsKey(rel)) {
            return frame
        }
        val needs: Collection<CorRef> = cm1.mapRefRelToCorRef.get(rel)
        val haves: ImmutableSortedSet<CorDef> = frame.corDefOutputs.keySet()
        return if (hasAll(needs, haves)) {
            frame
        } else decorrelateInputWithValueGenerator(
            rel,
            frame
        )
    }

    private fun decorrelateInputWithValueGenerator(rel: RelNode, frame: Frame): Frame {
        // currently only handles one input
        assert(rel.getInputs().size() === 1)
        val oldInput: RelNode = frame.r
        val corDefOutputs: NavigableMap<CorDef?, Integer?> = TreeMap(frame.corDefOutputs)
        val corVarList: Collection<CorRef> = cm.mapRefRelToCorRef.get(rel)

        // Try to populate correlation variables using local fields.
        // This means that we do not need a value generator.
        if (rel is Filter) {
            val map: NavigableMap<CorDef?, Integer?> = TreeMap()
            val projects: List<RexNode> = ArrayList()
            for (correlation in corVarList) {
                val def = correlation.def()
                if (corDefOutputs.containsKey(def) || map.containsKey(def)) {
                    continue
                }
                try {
                    findCorrelationEquivalent(correlation, (rel as Filter).getCondition())
                } catch (e: Util.FoundOne) {
                    val node: Object = requireNonNull(e.getNode(), "e.getNode()")
                    if (node is RexInputRef) {
                        map.put(def, (node as RexInputRef).getIndex())
                    } else {
                        map.put(
                            def,
                            frame.r.getRowType().getFieldCount() + projects.size()
                        )
                        projects.add(node as RexNode)
                    }
                }
            }
            // If all correlation variables are now satisfied, skip creating a value
            // generator.
            if (map.size() === corVarList.size()) {
                map.putAll(frame.corDefOutputs)
                val r: RelNode
                r = if (!projects.isEmpty()) {
                    relBuilder.push(oldInput)
                        .project(Iterables.concat(relBuilder.fields(), projects))
                    relBuilder.build()
                } else {
                    oldInput
                }
                return register(
                    rel.getInput(0), r,
                    frame.oldToNewOutputs, map
                )
            }
        }
        val leftInputOutputCount: Int = frame.r.getRowType().getFieldCount()

        // can directly add positions into corDefOutputs since join
        // does not change the output ordering from the inputs.
        val valueGen: RelNode = requireNonNull(
            createValueGenerator(corVarList, leftInputOutputCount, corDefOutputs),
            "createValueGenerator(...) is null"
        )
        val join: RelNode = relBuilder.push(frame.r).push(valueGen)
            .join(
                JoinRelType.INNER, relBuilder.literal(true),
                ImmutableSet.of()
            ).build()

        // Join or Filter does not change the old input ordering. All
        // input fields from newLeftInput (i.e. the original input to the old
        // Filter) are in the output and in the same position.
        return register(
            rel.getInput(0), join, frame.oldToNewOutputs,
            corDefOutputs
        )
    }

    @Nullable
    fun decorrelateRel(rel: LogicalSnapshot, isCorVarDefined: Boolean): Frame? {
        return if (RexUtil.containsCorrelation(rel.getPeriod())) {
            null
        } else decorrelateRel(rel as RelNode, isCorVarDefined)
    }

    @Nullable
    fun decorrelateRel(rel: LogicalTableFunctionScan, isCorVarDefined: Boolean): Frame? {
        return if (RexUtil.containsCorrelation(rel.getCall())) {
            null
        } else decorrelateRel(rel as RelNode, isCorVarDefined)
    }

    @Nullable
    fun decorrelateRel(rel: LogicalFilter?, isCorVarDefined: Boolean): Frame {
        return decorrelateRel(rel as Filter?, isCorVarDefined)
    }

    @Nullable
    fun decorrelateRel(rel: Filter, isCorVarDefined: Boolean): Frame? {
        //
        // Rewrite logic:
        //
        // 1. If a Filter references a correlated field in its filter
        // condition, rewrite the Filter to be
        //   Filter
        //     Join(cross product)
        //       originalFilterInput
        //       ValueGenerator(produces distinct sets of correlated variables)
        // and rewrite the correlated fieldAccess in the filter condition to
        // reference the Join output.
        //
        // 2. If Filter does not reference correlated variables, simply
        // rewrite the filter condition using new input.
        //
        val oldInput: RelNode = rel.getInput()
        var frame = getInvoke(oldInput, isCorVarDefined, rel)
            ?: // If input has not been rewritten, do not rewrite this rel.
            return null

        // If this Filter has correlated reference, create value generator
        // and produce the correlated variables in the new output.
        if (false) {
            if (cm.mapRefRelToCorRef.containsKey(rel)) {
                frame = decorrelateInputWithValueGenerator(rel, frame)
            }
        } else {
            frame = maybeAddValueGenerator(rel, frame)
        }
        val cm2 = CorelMapBuilder().build(rel)

        // Replace the filter expression to reference output of the join
        // Map filter to the new filter over join
        relBuilder.push(frame.r)
            .filter(decorrelateExpr(castNonNull(currentRel), map, cm2, rel.getCondition()))

        // Filter does not change the input ordering.
        // Filter rel does not permute the input.
        // All corVars produced by filter will have the same output positions in the
        // input rel.
        return register(
            rel, relBuilder.build(), frame.oldToNewOutputs,
            frame.corDefOutputs
        )
    }

    @Nullable
    fun decorrelateRel(rel: LogicalCorrelate?, isCorVarDefined: Boolean): Frame {
        return decorrelateRel(rel as Correlate?, isCorVarDefined)
    }

    @Nullable
    fun decorrelateRel(rel: Correlate, isCorVarDefined: Boolean): Frame? {
        //
        // Rewrite logic:
        //
        // The original left input will be joined with the new right input that
        // has generated correlated variables propagated up. For any generated
        // corVars that are not used in the join key, pass them along to be
        // joined later with the Correlates that produce them.
        //

        // the right input to Correlate should produce correlated variables
        val oldLeft: RelNode = rel.getInput(0)
        val oldRight: RelNode = rel.getInput(1)
        val leftFrame = getInvoke(oldLeft, isCorVarDefined, rel)
        val rightFrame = getInvoke(oldRight, true, rel)
        if (leftFrame == null || rightFrame == null) {
            // If any input has not been rewritten, do not rewrite this rel.
            return null
        }
        if (rightFrame.corDefOutputs.isEmpty()) {
            return null
        }
        assert(
            rel.getRequiredColumns().cardinality()
                    <= rightFrame.corDefOutputs.keySet().size()
        )

        // Change correlator rel into a join.
        // Join all the correlated variables produced by this correlator rel
        // with the values generated and propagated from the right input
        val corDefOutputs: NavigableMap<CorDef?, Integer?> = TreeMap(rightFrame.corDefOutputs)
        val conditions: List<RexNode> = ArrayList()
        val newLeftOutput: List<RelDataTypeField> = leftFrame.r.getRowType().getFieldList()
        val newLeftFieldCount: Int = newLeftOutput.size()
        val newRightOutput: List<RelDataTypeField> = rightFrame.r.getRowType().getFieldList()
        for (rightOutput in ArrayList(corDefOutputs.entrySet())) {
            val corDef: CorDef = rightOutput.getKey()
            if (!corDef.corr.equals(rel.getCorrelationId())) {
                continue
            }
            val newLeftPos: Int = leftFrame.oldToNewOutputs.get(corDef.field)
            val newRightPos: Int = rightOutput.getValue()
            conditions.add(
                relBuilder.equals(
                    RexInputRef.of(newLeftPos, newLeftOutput),
                    RexInputRef(
                        newLeftFieldCount + newRightPos,
                        newRightOutput[newRightPos].getType()
                    )
                )
            )

            // remove this corVar from output position mapping
            corDefOutputs.remove(corDef)
        }

        // Update the output position for the corVars: only pass on the cor
        // vars that are not used in the join key.
        for (entry in corDefOutputs.entrySet()) {
            entry.setValue(entry.getValue() + newLeftFieldCount)
        }

        // then add any corVar from the left input. Do not need to change
        // output positions.
        corDefOutputs.putAll(leftFrame.corDefOutputs)

        // Create the mapping between the output of the old correlation rel
        // and the new join rel
        val mapOldToNewOutputs: Map<Integer?, Integer> = HashMap()
        val oldLeftFieldCount: Int = oldLeft.getRowType().getFieldCount()
        val oldRightFieldCount: Int = oldRight.getRowType().getFieldCount()
        assert(
            rel.getRowType().getFieldCount()
                    === oldLeftFieldCount + oldRightFieldCount
        )

        // Left input positions are not changed.
        mapOldToNewOutputs.putAll(leftFrame.oldToNewOutputs)

        // Right input positions are shifted by newLeftFieldCount.
        for (i in 0 until oldRightFieldCount) {
            mapOldToNewOutputs.put(
                i + oldLeftFieldCount,
                rightFrame.oldToNewOutputs.get(i) + newLeftFieldCount
            )
        }
        val condition: RexNode = RexUtil.composeConjunction(relBuilder.getRexBuilder(), conditions)
        val newJoin: RelNode = relBuilder.push(leftFrame.r).push(rightFrame.r)
            .join(rel.getJoinType(), condition).build()
        return register(rel, newJoin, mapOldToNewOutputs, corDefOutputs)
    }

    @Nullable
    fun decorrelateRel(rel: LogicalJoin?, isCorVarDefined: Boolean): Frame {
        return decorrelateRel(rel as Join?, isCorVarDefined)
    }

    @Nullable
    fun decorrelateRel(rel: Join, isCorVarDefined: Boolean): Frame? {
        // For SEMI/ANTI join decorrelate it's input directly,
        // because the correlate variables can only be propagated from
        // the left side, which is not supported yet.
        if (!rel.getJoinType().projectsRight()) {
            return decorrelateRel(rel as RelNode, isCorVarDefined)
        }
        //
        // Rewrite logic:
        //
        // 1. rewrite join condition.
        // 2. map output positions and produce corVars if any.
        //
        val oldLeft: RelNode = rel.getInput(0)
        val oldRight: RelNode = rel.getInput(1)
        val leftFrame = getInvoke(oldLeft, isCorVarDefined, rel)
        val rightFrame = getInvoke(oldRight, isCorVarDefined, rel)
        if (leftFrame == null || rightFrame == null) {
            // If any input has not been rewritten, do not rewrite this rel.
            return null
        }
        val newJoin: RelNode = relBuilder
            .push(leftFrame.r)
            .push(rightFrame.r)
            .join(
                rel.getJoinType(),
                decorrelateExpr(castNonNull(currentRel), map, cm, rel.getCondition()),
                ImmutableSet.of()
            )
            .hints(rel.getHints())
            .build()

        // Create the mapping between the output of the old correlation rel
        // and the new join rel
        val mapOldToNewOutputs: Map<Integer?, Integer> = HashMap()
        val oldLeftFieldCount: Int = oldLeft.getRowType().getFieldCount()
        val newLeftFieldCount: Int = leftFrame.r.getRowType().getFieldCount()
        val oldRightFieldCount: Int = oldRight.getRowType().getFieldCount()
        assert(
            rel.getRowType().getFieldCount()
                    === oldLeftFieldCount + oldRightFieldCount
        )

        // Left input positions are not changed.
        mapOldToNewOutputs.putAll(leftFrame.oldToNewOutputs)

        // Right input positions are shifted by newLeftFieldCount.
        for (i in 0 until oldRightFieldCount) {
            mapOldToNewOutputs.put(
                i + oldLeftFieldCount,
                rightFrame.oldToNewOutputs.get(i) + newLeftFieldCount
            )
        }
        val corDefOutputs: NavigableMap<CorDef?, Integer?> = TreeMap(leftFrame.corDefOutputs)

        // Right input positions are shifted by newLeftFieldCount.
        for (entry in rightFrame.corDefOutputs.entrySet()) {
            corDefOutputs.put(
                entry.getKey(),
                entry.getValue() + newLeftFieldCount
            )
        }
        return register(rel, newJoin, mapOldToNewOutputs, corDefOutputs)
    }

    /**
     * Pulls project above the join from its RHS input. Enforces nullability
     * for join output.
     *
     * @param join          Join
     * @param project       Original project as the right-hand input of the join
     * @param nullIndicatorPos Position of null indicator
     * @return the subtree with the new Project at the root
     */
    private fun projectJoinOutputWithNullability(
        join: Join,
        project: Project,
        nullIndicatorPos: Int
    ): RelNode {
        val typeFactory: RelDataTypeFactory = join.getCluster().getTypeFactory()
        val left: RelNode = join.getLeft()
        val joinType: JoinRelType = join.getJoinType()
        val nullIndicator = RexInputRef(
            nullIndicatorPos,
            typeFactory.createTypeWithNullability(
                join.getRowType().getFieldList().get(nullIndicatorPos)
                    .getType(),
                true
            )
        )

        // now create the new project
        val newProjExprs: List<Pair<RexNode, String>> = ArrayList()

        // project everything from the LHS and then those from the original
        // projRel
        val leftInputFields: List<RelDataTypeField> = left.getRowType().getFieldList()
        for (i in 0 until leftInputFields.size()) {
            newProjExprs.add(RexInputRef.of2(i, leftInputFields))
        }

        // Marked where the projected expr is coming from so that the types will
        // become nullable for the original projections which are now coming out
        // of the nullable side of the OJ.
        val projectPulledAboveLeftCorrelator: Boolean = joinType.generatesNullsOnRight()
        for (pair in project.getNamedProjects()) {
            val newProjExpr: RexNode = removeCorrelationExpr(
                pair.left,
                projectPulledAboveLeftCorrelator,
                nullIndicator
            )
            newProjExprs.add(Pair.of(newProjExpr, pair.right))
        }
        return relBuilder.push(join)
            .projectNamed(Pair.left(newProjExprs), Pair.right(newProjExprs), true)
            .build()
    }

    /**
     * Pulls a [Project] above a [Correlate] from its RHS input.
     * Enforces nullability for join output.
     *
     * @param correlate  Correlate
     * @param project the original project as the RHS input of the join
     * @param isCount Positions which are calls to the `COUNT`
     * aggregation function
     * @return the subtree with the new Project at the root
     */
    private fun aggregateCorrelatorOutput(
        correlate: Correlate,
        project: Project,
        isCount: Set<Integer>
    ): RelNode {
        val left: RelNode = correlate.getLeft()
        val joinType: JoinRelType = correlate.getJoinType()

        // now create the new project
        val newProjects: List<Pair<RexNode, String>> = ArrayList()

        // Project everything from the LHS and then those from the original
        // project
        val leftInputFields: List<RelDataTypeField> = left.getRowType().getFieldList()
        for (i in 0 until leftInputFields.size()) {
            newProjects.add(RexInputRef.of2(i, leftInputFields))
        }

        // Marked where the projected expr is coming from so that the types will
        // become nullable for the original projections which are now coming out
        // of the nullable side of the OJ.
        val projectPulledAboveLeftCorrelator: Boolean = joinType.generatesNullsOnRight()
        for (pair in project.getNamedProjects()) {
            val newProjExpr: RexNode = removeCorrelationExpr(
                pair.left,
                projectPulledAboveLeftCorrelator,
                isCount
            )
            newProjects.add(Pair.of(newProjExpr, pair.right))
        }
        return relBuilder.push(correlate)
            .projectNamed(Pair.left(newProjects), Pair.right(newProjects), true)
            .build()
    }

    /**
     * Checks whether the correlations in projRel and filter are related to
     * the correlated variables provided by corRel.
     *
     * @param correlate    Correlate
     * @param project   The original Project as the RHS input of the join
     * @param filter    Filter
     * @param correlatedJoinKeys Correlated join keys
     * @return true if filter and proj only references corVar provided by corRel
     */
    private fun checkCorVars(
        correlate: Correlate,
        @Nullable project: Project?,
        @Nullable filter: Filter?,
        @Nullable correlatedJoinKeys: List<RexFieldAccess>?
    ): Boolean {
        if (filter != null) {
            assert(correlatedJoinKeys != null)

            // check that all correlated refs in the filter condition are
            // used in the join(as field access).
            val corVarInFilter: Set<CorRef> = Sets.newHashSet(cm.mapRefRelToCorRef.get(filter))
            for (correlatedJoinKey in correlatedJoinKeys) {
                corVarInFilter.remove(cm.mapFieldAccessToCorRef[correlatedJoinKey])
            }
            if (!corVarInFilter.isEmpty()) {
                return false
            }

            // Check that the correlated variables referenced in these
            // comparisons do come from the Correlate.
            corVarInFilter.addAll(cm.mapRefRelToCorRef.get(filter))
            for (corVar in corVarInFilter) {
                if (cm.mapCorToCorRel.get(corVar.corr) !== correlate) {
                    return false
                }
            }
        }

        // if project has any correlated reference, make sure they are also
        // provided by the current correlate. They will be projected out of the LHS
        // of the correlate.
        if (project != null && cm.mapRefRelToCorRef.containsKey(project)) {
            for (corVar in cm.mapRefRelToCorRef.get(project)) {
                if (cm.mapCorToCorRel.get(corVar.corr) !== correlate) {
                    return false
                }
            }
        }
        return true
    }

    /**
     * Removes correlated variables from the tree at root corRel.
     *
     * @param correlate Correlate
     */
    private fun removeCorVarFromTree(correlate: Correlate) {
        cm.mapCorToCorRel.remove(correlate.getCorrelationId(), correlate)
    }

    /**
     * Projects all `input` output fields plus the additional expressions.
     *
     * @param input        Input relational expression
     * @param additionalExprs Additional expressions and names
     * @return the new Project
     */
    private fun createProjectWithAdditionalExprs(
        input: RelNode,
        additionalExprs: List<Pair<RexNode, String>>
    ): RelNode {
        val fieldList: List<RelDataTypeField> = input.getRowType().getFieldList()
        val projects: List<Pair<RexNode, String>> = ArrayList()
        Ord.forEach(fieldList) { field, i ->
            projects.add(
                Pair.of(
                    relBuilder.getRexBuilder().makeInputRef(field.getType(), i),
                    field.getName()
                )
            )
        }
        projects.addAll(additionalExprs)
        return relBuilder.push(input)
            .projectNamed(Pair.left(projects), Pair.right(projects), true)
            .build()
    }

    /** Registers a relational expression and the relational expression it became
     * after decorrelation.  */
    fun register(
        rel: RelNode?, newRel: RelNode?,
        oldToNewOutputs: Map<Integer?, Integer?>?,
        corDefOutputs: NavigableMap<CorDef?, Integer?>?
    ): Frame {
        val frame = Frame(rel, newRel, corDefOutputs, oldToNewOutputs)
        map.put(rel, frame)
        return frame
    }
    //~ Inner Classes ----------------------------------------------------------
    /** Shuttle that decorrelates.  */
    private class DecorrelateRexShuttle(
        currentRel: RelNode,
        map: Map<RelNode?, Frame>, cm: CorelMap
    ) : RexShuttle() {
        private val currentRel: RelNode
        private val map: Map<RelNode?, Frame>
        private val cm: CorelMap

        init {
            this.currentRel = requireNonNull(currentRel, "currentRel")
            this.map = requireNonNull(map, "map")
            this.cm = requireNonNull(cm, "cm")
        }

        @Override
        fun visitFieldAccess(fieldAccess: RexFieldAccess): RexNode {
            var newInputOutputOffset = 0
            for (input in currentRel.getInputs()) {
                val frame = map[input]
                if (frame != null) {
                    // try to find in this input rel the position of corVar
                    val corRef = cm.mapFieldAccessToCorRef[fieldAccess]
                    if (corRef != null) {
                        val newInputPos: Integer = frame.corDefOutputs.get(corRef.def())
                        if (newInputPos != null) {
                            // This input does produce the corVar referenced.
                            return RexInputRef(
                                newInputPos + newInputOutputOffset,
                                frame.r.getRowType().getFieldList().get(newInputPos)
                                    .getType()
                            )
                        }
                    }

                    // this input does not produce the corVar needed
                    newInputOutputOffset += frame.r.getRowType().getFieldCount()
                } else {
                    // this input is not rewritten
                    newInputOutputOffset += input.getRowType().getFieldCount()
                }
            }
            return fieldAccess
        }

        @Override
        fun visitInputRef(inputRef: RexInputRef): RexNode {
            val ref: RexInputRef = getNewForOldInputRef(currentRel, map, inputRef)
            return if (ref.getIndex() === inputRef.getIndex()
                && ref.getType() === inputRef.getType()
            ) {
                inputRef // re-use old object, to prevent needless expr cloning
            } else ref
        }
    }

    /** Shuttle that removes correlations.  */
    private inner class RemoveCorrelationRexShuttle internal constructor(
        rexBuilder: RexBuilder,
        val projectPulledAboveLeftCorrelator: Boolean,
        @Nullable nullIndicator: RexInputRef?,
        isCount: Set<Integer?>?
    ) : RexShuttle() {
        val rexBuilder: RexBuilder
        val typeFactory: RelDataTypeFactory

        @Nullable
        val nullIndicator: RexInputRef?
        val isCount: ImmutableSet<Integer>

        init {
            this.nullIndicator = nullIndicator // may be null
            this.isCount = ImmutableSet.copyOf(isCount)
            this.rexBuilder = rexBuilder
            typeFactory = rexBuilder.getTypeFactory()
        }

        private fun createCaseExpression(
            nullInputRef: RexInputRef,
            @Nullable lit: RexLiteral?,
            rexNode: RexNode
        ): RexNode {
            val caseOperands: Array<RexNode?> = arrayOfNulls<RexNode>(3)

            // Construct a CASE expression to handle the null indicator.
            //
            // This also covers the case where a left correlated sub-query
            // projects fields from outer relation. Since LOJ cannot produce
            // nulls on the LHS, the projection now need to make a nullable LHS
            // reference using a nullability indicator. If this this indicator
            // is null, it means the sub-query does not produce any value. As a
            // result, any RHS ref by this sub-query needs to produce null value.

            // WHEN indicator IS NULL
            caseOperands[0] = rexBuilder.makeCall(
                SqlStdOperatorTable.IS_NULL,
                RexInputRef(
                    nullInputRef.getIndex(),
                    typeFactory.createTypeWithNullability(
                        nullInputRef.getType(),
                        true
                    )
                )
            )

            // THEN CAST(NULL AS newInputTypeNullable)
            caseOperands[1] = if (lit == null) rexBuilder.makeNullLiteral(rexNode.getType()) else rexBuilder.makeCast(
                rexNode.getType(),
                lit
            )

            // ELSE cast (newInput AS newInputTypeNullable) END
            caseOperands[2] = rexBuilder.makeCast(
                typeFactory.createTypeWithNullability(
                    rexNode.getType(),
                    true
                ),
                rexNode
            )
            return rexBuilder.makeCall(
                SqlStdOperatorTable.CASE,
                caseOperands
            )
        }

        @Override
        fun visitFieldAccess(fieldAccess: RexFieldAccess): RexNode {
            if (cm.mapFieldAccessToCorRef.containsKey(fieldAccess)) {
                // if it is a corVar, change it to be input ref.
                val corVar = cm.mapFieldAccessToCorRef[fieldAccess]

                // corVar offset should point to the leftInput of currentRel,
                // which is the Correlate.
                var newRexNode: RexNode = RexInputRef(corVar!!.field, fieldAccess.getType())
                if (projectPulledAboveLeftCorrelator
                    && nullIndicator != null
                ) {
                    // need to enforce nullability by applying an additional
                    // cast operator over the transformed expression.
                    newRexNode = createCaseExpression(nullIndicator, null, newRexNode)
                }
                return newRexNode
            }
            return fieldAccess
        }

        @Override
        fun visitInputRef(inputRef: RexInputRef): RexNode {
            if (currentRel is Correlate) {
                // if this rel references corVar
                // and now it needs to be rewritten
                // it must have been pulled above the Correlate
                // replace the input ref to account for the LHS of the
                // Correlate
                val leftInputFieldCount: Int = (currentRel as Correlate?).getLeft().getRowType()
                    .getFieldCount()
                var newType: RelDataType = inputRef.getType()
                if (projectPulledAboveLeftCorrelator) {
                    newType = typeFactory.createTypeWithNullability(newType, true)
                }
                val pos: Int = inputRef.getIndex()
                val newInputRef = RexInputRef(leftInputFieldCount + pos, newType)
                return if (isCount.contains(pos)) {
                    createCaseExpression(
                        newInputRef,
                        rexBuilder.makeExactLiteral(BigDecimal.ZERO),
                        newInputRef
                    )
                } else {
                    newInputRef
                }
            }
            return inputRef
        }

        @Override
        fun visitLiteral(literal: RexLiteral): RexNode {
            // Use nullIndicator to decide whether to project null.
            // Do nothing if the literal is null.
            return if (!RexUtil.isNull(literal)
                && projectPulledAboveLeftCorrelator
                && nullIndicator != null
            ) {
                createCaseExpression(nullIndicator, null, literal)
            } else literal
        }

        @Override
        fun visitCall(call: RexCall): RexNode {
            val newCall: RexNode
            val update = booleanArrayOf(false)
            val clonedOperands: List<RexNode> = visitList(call.operands, update)
            if (update[0]) {
                val operator: SqlOperator = call.getOperator()
                var isSpecialCast = false
                if (operator is SqlFunction) {
                    val function: SqlFunction = operator as SqlFunction
                    if (function.getKind() === SqlKind.CAST) {
                        if (call.operands.size() < 2) {
                            isSpecialCast = true
                        }
                    }
                }
                val newType: RelDataType
                newType = if (!isSpecialCast) {
                    // TODO: ideally this only needs to be called if the result
                    // type will also change. However, since that requires
                    // support from type inference rules to tell whether a rule
                    // decides return type based on input types, for now all
                    // operators will be recreated with new type if any operand
                    // changed, unless the operator has "built-in" type.
                    rexBuilder.deriveReturnType(operator, clonedOperands)
                } else {
                    // Use the current return type when creating a new call, for
                    // operators with return type built into the operator
                    // definition, and with no type inference rules, such as
                    // cast function with less than 2 operands.

                    // TODO: Comments in RexShuttle.visitCall() mention other
                    // types in this category. Need to resolve those together
                    // and preferably in the base class RexShuttle.
                    call.getType()
                }
                newCall = rexBuilder.makeCall(
                    newType,
                    operator,
                    clonedOperands
                )
            } else {
                newCall = call
            }
            return if (projectPulledAboveLeftCorrelator && nullIndicator != null) {
                createCaseExpression(nullIndicator, null, newCall)
            } else newCall
        }
    }

    /**
     * Rule to remove an Aggregate with SINGLE_VALUE. For cases like:
     *
     * Aggregate(SINGLE_VALUE)
     * Project(single expression)
     * Aggregate
     *
     * For instance (subtree taken from TPCH query 17):
     *
     * LogicalAggregate(group=[{}], agg#0=[SINGLE_VALUE($0)])
     * LogicalProject(EXPR$0=[*(0.2:DECIMAL(2, 1), $0)])
     * LogicalAggregate(group=[{}], agg#0=[AVG($0)])
     * LogicalProject(L_QUANTITY=[$4])
     * LogicalFilter(condition=[=($1, $cor0.P_PARTKEY)])
     * LogicalTableScan(table=[[TPCH_01, LINEITEM]])
     *
     * Will be converted into:
     *
     * LogicalProject($f0=[*(0.2:DECIMAL(2, 1), $0)])
     * LogicalAggregate(group=[{}], agg#0=[AVG($0)])
     * LogicalProject(L_QUANTITY=[$4])
     * LogicalFilter(condition=[=($1, $cor0.P_PARTKEY)])
     * LogicalTableScan(table=[[TPCH_01, LINEITEM]])
     */
    class RemoveSingleAggregateRule
    /** Creates a RemoveSingleAggregateRule.  */
    internal constructor(config: RemoveSingleAggregateRuleConfig?) :
        RelRule<RemoveSingleAggregateRule.RemoveSingleAggregateRuleConfig?>(config) {
        @Override
        fun onMatch(call: RelOptRuleCall) {
            val singleAggregate: Aggregate = call.rel(0)
            val project: Project = call.rel(1)
            val aggregate: Aggregate = call.rel(2)

            // check the top aggregate is a single value agg function
            if (!singleAggregate.getGroupSet().isEmpty()
                || singleAggregate.getAggCallList().size() !== 1
                || singleAggregate.getAggCallList().get(0).getAggregation() !is SqlSingleValueAggFunction
            ) {
                return
            }

            // check the project only projects one expression, i.e. scalar sub-queries.
            val projExprs: List<RexNode> = project.getProjects()
            if (projExprs.size() !== 1) {
                return
            }

            // check the input to project is an aggregate on the entire input
            if (!aggregate.getGroupSet().isEmpty()) {
                return
            }

            // ensure we keep the same type after removing the SINGLE_VALUE Aggregate
            val relBuilder: RelBuilder = call.builder()
            val singleAggType: RelDataType = singleAggregate.getRowType().getFieldList().get(0).getType()
            val oldProjectExp: RexNode = projExprs[0]
            val newProjectExp: RexNode =
                if (singleAggType.equals(oldProjectExp.getType())) oldProjectExp else relBuilder.getRexBuilder()
                    .makeCast(singleAggType, oldProjectExp)
            relBuilder.push(aggregate).project(newProjectExp)
            call.transformTo(relBuilder.build())
        }

        /** Rule configuration.  */
        @Value.Immutable(singleton = false)
        interface RemoveSingleAggregateRuleConfig : RelRule.Config {
            @Override
            fun toRule(): RemoveSingleAggregateRule? {
                return RemoveSingleAggregateRule(this)
            }
        }

        companion object {
            fun config(f: RelBuilderFactory?): RemoveSingleAggregateRuleConfig {
                return ImmutableRemoveSingleAggregateRuleConfig.builder().withRelBuilderFactory(f)
                    .withOperandSupplier { b0 ->
                        b0.operand(Aggregate::class.java).oneInput { b1 ->
                            b1.operand(Project::class.java).oneInput { b2 ->
                                b2.operand(
                                    Aggregate::class.java
                                ).anyInputs()
                            }
                        }
                    }
                    .build()
            }
        }
    }

    /** Planner rule that removes correlations for scalar projects.  */
    class RemoveCorrelationForScalarProjectRule internal constructor(config: RemoveCorrelationForScalarProjectRuleConfig) :
        RelRule<RemoveCorrelationForScalarProjectRule.RemoveCorrelationForScalarProjectRuleConfig?>(config) {
        private val d: RelDecorrelator

        /** Creates a RemoveCorrelationForScalarProjectRule.  */
        init {
            d = requireNonNull(config.decorrelator())
        }

        @Override
        fun onMatch(call: RelOptRuleCall) {
            val correlate: Correlate = call.rel(0)
            val left: RelNode = call.rel(1)
            val aggregate: Aggregate = call.rel(2)
            val project: Project = call.rel(3)
            var right: RelNode = call.rel(4)
            val cluster: RelOptCluster = correlate.getCluster()
            d.setCurrent(call.getPlanner().getRoot(), correlate)

            // Check for this pattern.
            // The pattern matching could be simplified if rules can be applied
            // during decorrelation.
            //
            // Correlate(left correlation, condition = true)
            //   leftInput
            //   Aggregate (groupby (0) single_value())
            //     Project-A (may reference corVar)
            //       rightInput
            val joinType: JoinRelType = correlate.getJoinType()

            // corRel.getCondition was here, however Correlate was updated so it
            // never includes a join condition. The code was not modified for brevity.
            var joinCond: RexNode = d.relBuilder.literal(true)
            if (joinType !== JoinRelType.LEFT
                || joinCond !== d.relBuilder.literal(true)
            ) {
                return
            }

            // check that the agg is of the following type:
            // doing a single_value() on the entire input
            if (!aggregate.getGroupSet().isEmpty()
                || aggregate.getAggCallList().size() !== 1
                || aggregate.getAggCallList().get(0).getAggregation() !is SqlSingleValueAggFunction
            ) {
                return
            }

            // check this project only projects one expression, i.e. scalar
            // sub-queries.
            if (project.getProjects().size() !== 1) {
                return
            }
            val nullIndicatorPos: Int
            if (right is Filter
                && d.cm.mapRefRelToCorRef.containsKey(right)
            ) {
                // rightInput has this shape:
                //
                //       Filter (references corVar)
                //         filterInput

                // If rightInput is a filter and contains correlated
                // reference, make sure the correlated keys in the filter
                // condition forms a unique key of the RHS.
                val filter: Filter = right as Filter
                right = filter.getInput()
                assert(right is HepRelVertex)
                right = (right as HepRelVertex).getCurrentRel()

                // check filter input contains no correlation
                if (RelOptUtil.getVariablesUsed(right).size() > 0) {
                    return
                }

                // extract the correlation out of the filter

                // First breaking up the filter conditions into equality
                // comparisons between rightJoinKeys (from the original
                // filterInput) and correlatedJoinKeys. correlatedJoinKeys
                // can be expressions, while rightJoinKeys need to be input
                // refs. These comparisons are AND'ed together.
                val tmpRightJoinKeys: List<RexNode> = ArrayList()
                val correlatedJoinKeys: List<RexNode> = ArrayList()
                RelOptUtil.splitCorrelatedFilterCondition(
                    filter,
                    tmpRightJoinKeys,
                    correlatedJoinKeys,
                    false
                )

                // check that the columns referenced in these comparisons form
                // an unique key of the filterInput
                val rightJoinKeys: List<RexInputRef> = ArrayList()
                for (key in tmpRightJoinKeys) {
                    assert(key is RexInputRef)
                    rightJoinKeys.add(key as RexInputRef)
                }

                // check that the columns referenced in rightJoinKeys form an
                // unique key of the filterInput
                if (rightJoinKeys.isEmpty()) {
                    return
                }

                // The join filters out the nulls.  So, it's ok if there are
                // nulls in the join keys.
                val mq: RelMetadataQuery = call.getMetadataQuery()
                if (!RelMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered(
                        mq, right,
                        rightJoinKeys
                    )
                ) {
                    SQL2REL_LOGGER.debug(
                        "{} are not unique keys for {}",
                        rightJoinKeys, right
                    )
                    return
                }
                val visitor: RexUtil.FieldAccessFinder = FieldAccessFinder()
                RexUtil.apply(visitor, correlatedJoinKeys, null)
                val correlatedKeyList: List<RexFieldAccess> = visitor.getFieldAccessList()
                if (!d.checkCorVars(correlate, project, filter, correlatedKeyList)) {
                    return
                }

                // Change the plan to this structure.
                // Note that the Aggregate is removed.
                //
                // Project-A' (replace corVar to input ref from the Join)
                //   Join (replace corVar to input ref from leftInput)
                //     leftInput
                //     rightInput (previously filterInput)

                // Change the filter condition into a join condition
                joinCond = d.removeCorrelationExpr(filter.getCondition(), false)
                nullIndicatorPos = (left.getRowType().getFieldCount()
                        + rightJoinKeys[0].getIndex())
            } else if (d.cm.mapRefRelToCorRef.containsKey(project)) {
                // check filter input contains no correlation
                if (RelOptUtil.getVariablesUsed(right).size() > 0) {
                    return
                }
                if (!d.checkCorVars(correlate, project, null, null)) {
                    return
                }

                // Change the plan to this structure.
                //
                // Project-A' (replace corVar to input ref from Join)
                //   Join (left, condition = true)
                //     leftInput
                //     Aggregate(groupby(0), single_value(0), s_v(1)....)
                //       Project-B (everything from input plus literal true)
                //         projectInput

                // make the new Project to provide a null indicator
                right = d.createProjectWithAdditionalExprs(
                    right,
                    ImmutableList.of(
                        Pair.of(d.relBuilder.literal(true), "nullIndicator")
                    )
                )

                // make the new aggRel
                right = RelOptUtil.createSingleValueAggRel(cluster, right)

                // The last field:
                //     single_value(true)
                // is the nullIndicator
                nullIndicatorPos = left.getRowType().getFieldCount()
                +right.getRowType().getFieldCount() - 1
            } else {
                return
            }

            // make the new join rel
            val join: Join = d.relBuilder.push(left).push(right)
                .join(joinType, joinCond).build() as Join
            val newProject: RelNode = d.projectJoinOutputWithNullability(join, project, nullIndicatorPos)
            call.transformTo(newProject)
            d.removeCorVarFromTree(correlate)
        }

        /** Rule configuration.
         *
         *
         * Extends [RelDecorrelator.Config] because rule needs a
         * decorrelator instance.  */
        @Value.Immutable(singleton = false)
        interface RemoveCorrelationForScalarProjectRuleConfig : Config {
            @Override
            fun toRule(): RemoveCorrelationForScalarProjectRule? {
                return RemoveCorrelationForScalarProjectRule(this)
            }
        }

        companion object {
            fun config(
                decorrelator: RelDecorrelator?,
                relBuilderFactory: RelBuilderFactory?
            ): RemoveCorrelationForScalarProjectRuleConfig {
                return ImmutableRemoveCorrelationForScalarProjectRuleConfig.builder()
                    .withRelBuilderFactory(relBuilderFactory)
                    .withOperandSupplier { b0 ->
                        b0.operand(Correlate::class.java).inputs(
                            { b1 -> b1.operand(RelNode::class.java).anyInputs() }
                        ) { b2 ->
                            b2.operand(Aggregate::class.java).oneInput { b3 ->
                                b3.operand(Project::class.java).oneInput { b4 ->
                                    b4.operand(
                                        RelNode::class.java
                                    ).anyInputs()
                                }
                            }
                        }
                    }
                    .withDecorrelator(decorrelator)
                    .build()
            }
        }
    }

    /** Planner rule that removes correlations for scalar aggregates.  */
    class RemoveCorrelationForScalarAggregateRule internal constructor(config: RemoveCorrelationForScalarAggregateRuleConfig) :
        RelRule<RemoveCorrelationForScalarAggregateRule.RemoveCorrelationForScalarAggregateRuleConfig?>(config) {
        private val d: RelDecorrelator

        /** Creates a RemoveCorrelationForScalarAggregateRule.  */
        init {
            d = requireNonNull(config.decorrelator())
        }

        @Override
        fun onMatch(call: RelOptRuleCall) {
            val correlate: Correlate = call.rel(0)
            val left: RelNode = call.rel(1)
            val aggOutputProject: Project = call.rel(2)
            val aggregate: Aggregate = call.rel(3)
            val aggInputProject: Project = call.rel(4)
            var right: RelNode = call.rel(5)
            val builder: RelBuilder = call.builder()
            val rexBuilder: RexBuilder = builder.getRexBuilder()
            val cluster: RelOptCluster = correlate.getCluster()
            d.setCurrent(call.getPlanner().getRoot(), correlate)

            // check for this pattern
            // The pattern matching could be simplified if rules can be applied
            // during decorrelation,
            //
            // CorrelateRel(left correlation, condition = true)
            //   leftInput
            //   Project-A (a RexNode)
            //     Aggregate (groupby (0), agg0(), agg1()...)
            //       Project-B (references coVar)
            //         rightInput

            // check aggOutputProject projects only one expression
            val aggOutputProjects: List<RexNode> = aggOutputProject.getProjects()
            if (aggOutputProjects.size() !== 1) {
                return
            }
            val joinType: JoinRelType = correlate.getJoinType()
            // corRel.getCondition was here, however Correlate was updated so it
            // never includes a join condition. The code was not modified for brevity.
            var joinCond: RexNode = rexBuilder.makeLiteral(true)
            if (joinType !== JoinRelType.LEFT
                || joinCond !== rexBuilder.makeLiteral(true)
            ) {
                return
            }

            // check that the agg is on the entire input
            if (!aggregate.getGroupSet().isEmpty()) {
                return
            }
            val aggInputProjects: List<RexNode> = aggInputProject.getProjects()
            val aggCalls: List<AggregateCall> = aggregate.getAggCallList()
            val isCountStar: Set<Integer> = HashSet()

            // mark if agg produces count(*) which needs to reference the
            // nullIndicator after the transformation.
            var k = -1
            for (aggCall in aggCalls) {
                ++k
                if (aggCall.getAggregation() is SqlCountAggFunction
                    && aggCall.getArgList().size() === 0
                ) {
                    isCountStar.add(k)
                }
            }
            if (right is Filter
                && d.cm.mapRefRelToCorRef.containsKey(right)
            ) {
                // rightInput has this shape:
                //
                //       Filter (references corVar)
                //         filterInput
                val filter: Filter = right as Filter
                right = filter.getInput()
                assert(right is HepRelVertex)
                right = (right as HepRelVertex).getCurrentRel()

                // check filter input contains no correlation
                if (RelOptUtil.getVariablesUsed(right).size() > 0) {
                    return
                }

                // check filter condition type First extract the correlation out
                // of the filter

                // First breaking up the filter conditions into equality
                // comparisons between rightJoinKeys(from the original
                // filterInput) and correlatedJoinKeys. correlatedJoinKeys
                // can only be RexFieldAccess, while rightJoinKeys can be
                // expressions. These comparisons are AND'ed together.
                val rightJoinKeys: List<RexNode> = ArrayList()
                val tmpCorrelatedJoinKeys: List<RexNode> = ArrayList()
                RelOptUtil.splitCorrelatedFilterCondition(
                    filter,
                    rightJoinKeys,
                    tmpCorrelatedJoinKeys,
                    true
                )

                // make sure the correlated reference forms a unique key check
                // that the columns referenced in these comparisons form an
                // unique key of the leftInput
                val correlatedJoinKeys: List<RexFieldAccess> = ArrayList()
                val correlatedInputRefJoinKeys: List<RexInputRef> = ArrayList()
                for (joinKey in tmpCorrelatedJoinKeys) {
                    assert(joinKey is RexFieldAccess)
                    correlatedJoinKeys.add(joinKey as RexFieldAccess)
                    val correlatedInputRef: RexNode = d.removeCorrelationExpr(joinKey, false)
                    assert(correlatedInputRef is RexInputRef)
                    correlatedInputRefJoinKeys.add(
                        correlatedInputRef as RexInputRef
                    )
                }

                // check that the columns referenced in rightJoinKeys form an
                // unique key of the filterInput
                if (correlatedInputRefJoinKeys.isEmpty()) {
                    return
                }

                // The join filters out the nulls.  So, it's ok if there are
                // nulls in the join keys.
                val mq: RelMetadataQuery = call.getMetadataQuery()
                if (!RelMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered(
                        mq, left,
                        correlatedInputRefJoinKeys
                    )
                ) {
                    SQL2REL_LOGGER.debug(
                        "{} are not unique keys for {}",
                        correlatedJoinKeys, left
                    )
                    return
                }

                // check corVar references are valid
                if (!d.checkCorVars(
                        correlate, aggInputProject, filter,
                        correlatedJoinKeys
                    )
                ) {
                    return
                }

                // Rewrite the above plan:
                //
                // Correlate(left correlation, condition = true)
                //   leftInput
                //   Project-A (a RexNode)
                //     Aggregate (groupby(0), agg0(),agg1()...)
                //       Project-B (may reference corVar)
                //         Filter (references corVar)
                //           rightInput (no correlated reference)
                //

                // to this plan:
                //
                // Project-A' (all gby keys + rewritten nullable ProjExpr)
                //   Aggregate (groupby(all left input refs)
                //                 agg0(rewritten expression),
                //                 agg1()...)
                //     Project-B' (rewritten original projected exprs)
                //       Join(replace corVar w/ input ref from leftInput)
                //         leftInput
                //         rightInput
                //

                // In the case where agg is count(*) or count($corVar), it is
                // changed to count(nullIndicator).
                // Note:  any non-nullable field from the RHS can be used as
                // the indicator however a "true" field is added to the
                // projection list from the RHS for simplicity to avoid
                // searching for non-null fields.
                //
                // Project-A' (all gby keys + rewritten nullable ProjExpr)
                //   Aggregate (groupby(all left input refs),
                //                 count(nullIndicator), other aggs...)
                //     Project-B' (all left input refs plus
                //                    the rewritten original projected exprs)
                //       Join(replace corVar to input ref from leftInput)
                //         leftInput
                //         Project (everything from rightInput plus
                //                     the nullIndicator "true")
                //           rightInput
                //

                // first change the filter condition into a join condition
                joinCond = d.removeCorrelationExpr(filter.getCondition(), false)
            } else if (d.cm.mapRefRelToCorRef.containsKey(aggInputProject)) {
                // check rightInput contains no correlation
                if (RelOptUtil.getVariablesUsed(right).size() > 0) {
                    return
                }

                // check corVar references are valid
                if (!d.checkCorVars(correlate, aggInputProject, null, null)) {
                    return
                }
                val nFields: Int = left.getRowType().getFieldCount()
                val allCols: ImmutableBitSet = ImmutableBitSet.range(nFields)

                // leftInput contains unique keys
                // i.e. each row is distinct and can group by on all the left
                // fields
                val mq: RelMetadataQuery = call.getMetadataQuery()
                if (!RelMdUtil.areColumnsDefinitelyUnique(mq, left, allCols)) {
                    SQL2REL_LOGGER.debug("There are no unique keys for {}", left)
                    return
                }
                //
                // Rewrite the above plan:
                //
                // CorrelateRel(left correlation, condition = true)
                //   leftInput
                //   Project-A (a RexNode)
                //     Aggregate (groupby(0), agg0(), agg1()...)
                //       Project-B (references coVar)
                //         rightInput (no correlated reference)
                //

                // to this plan:
                //
                // Project-A' (all gby keys + rewritten nullable ProjExpr)
                //   Aggregate (groupby(all left input refs)
                //                 agg0(rewritten expression),
                //                 agg1()...)
                //     Project-B' (rewritten original projected exprs)
                //       Join (LOJ cond = true)
                //         leftInput
                //         rightInput
                //

                // In the case where agg is count($corVar), it is changed to
                // count(nullIndicator).
                // Note:  any non-nullable field from the RHS can be used as
                // the indicator however a "true" field is added to the
                // projection list from the RHS for simplicity to avoid
                // searching for non-null fields.
                //
                // Project-A' (all gby keys + rewritten nullable ProjExpr)
                //   Aggregate (groupby(all left input refs),
                //                 count(nullIndicator), other aggs...)
                //     Project-B' (all left input refs plus
                //                    the rewritten original projected exprs)
                //       Join (replace corVar to input ref from leftInput)
                //         leftInput
                //         Project (everything from rightInput plus
                //                     the nullIndicator "true")
                //           rightInput
            } else {
                return
            }
            val leftInputFieldType: RelDataType = left.getRowType()
            val leftInputFieldCount: Int = leftInputFieldType.getFieldCount()
            val joinOutputProjExprCount: Int = leftInputFieldCount + aggInputProjects.size() + 1
            right = d.createProjectWithAdditionalExprs(
                right,
                ImmutableList.of(
                    Pair.of(rexBuilder.makeLiteral(true), "nullIndicator")
                )
            )
            val join: Join = d.relBuilder.push(left).push(right)
                .join(joinType, joinCond).build() as Join

            // To the consumer of joinOutputProjRel, nullIndicator is located
            // at the end
            var nullIndicatorPos: Int = join.getRowType().getFieldCount() - 1
            val nullIndicator = RexInputRef(
                nullIndicatorPos,
                cluster.getTypeFactory().createTypeWithNullability(
                    join.getRowType().getFieldList()
                        .get(nullIndicatorPos).getType(),
                    true
                )
            )

            // first project all group-by keys plus the transformed agg input
            val joinOutputProjects: List<RexNode> = ArrayList()

            // LOJ Join preserves LHS types
            for (i in 0 until leftInputFieldCount) {
                joinOutputProjects.add(
                    rexBuilder.makeInputRef(
                        leftInputFieldType.getFieldList().get(i).getType(), i
                    )
                )
            }
            for (aggInputProjExpr in aggInputProjects) {
                joinOutputProjects.add(
                    d.removeCorrelationExpr(
                        aggInputProjExpr,
                        joinType.generatesNullsOnRight(),
                        nullIndicator
                    )
                )
            }
            joinOutputProjects.add(
                rexBuilder.makeInputRef(join, nullIndicatorPos)
            )
            val joinOutputProject: RelNode = builder.push(join)
                .project(joinOutputProjects)
                .build()

            // nullIndicator is now at a different location in the output of
            // the join
            nullIndicatorPos = joinOutputProjExprCount - 1
            val newAggCalls: List<AggregateCall> = ArrayList()
            k = -1
            for (aggCall in aggCalls) {
                ++k
                val argList: List<Integer>
                if (isCountStar.contains(k)) {
                    // this is a count(*), transform it to count(nullIndicator)
                    // the null indicator is located at the end
                    argList = Collections.singletonList(nullIndicatorPos)
                } else {
                    argList = ArrayList()
                    for (aggArg in aggCall.getArgList()) {
                        argList.add(aggArg + leftInputFieldCount)
                    }
                }
                val filterArg: Int =
                    if (aggCall.filterArg < 0) aggCall.filterArg else aggCall.filterArg + leftInputFieldCount
                newAggCalls.add(
                    aggCall.adaptTo(
                        joinOutputProject, argList, filterArg,
                        aggregate.getGroupCount(), leftInputFieldCount
                    )
                )
            }
            val groupSet: ImmutableBitSet = ImmutableBitSet.range(leftInputFieldCount)
            builder.push(joinOutputProject)
                .aggregate(builder.groupKey(groupSet), newAggCalls)
            val newAggOutputProjectList: List<RexNode> = ArrayList()
            for (i in groupSet) {
                newAggOutputProjectList.add(
                    rexBuilder.makeInputRef(builder.peek(), i)
                )
            }
            val newAggOutputProjects: RexNode = d.removeCorrelationExpr(aggOutputProjects[0], false)
            newAggOutputProjectList.add(
                rexBuilder.makeCast(
                    cluster.getTypeFactory().createTypeWithNullability(
                        newAggOutputProjects.getType(),
                        true
                    ),
                    newAggOutputProjects
                )
            )
            builder.project(newAggOutputProjectList)
            call.transformTo(builder.build())
            d.removeCorVarFromTree(correlate)
        }

        /** Rule configuration.
         *
         *
         * Extends [RelDecorrelator.Config] because rule needs a
         * decorrelator instance.  */
        @Value.Immutable(singleton = false)
        interface RemoveCorrelationForScalarAggregateRuleConfig : Config {
            @Override
            fun toRule(): RemoveCorrelationForScalarAggregateRule? {
                return RemoveCorrelationForScalarAggregateRule(this)
            }
        }

        companion object {
            fun config(
                d: RelDecorrelator?,
                relBuilderFactory: RelBuilderFactory?
            ): RemoveCorrelationForScalarAggregateRuleConfig {
                return ImmutableRemoveCorrelationForScalarAggregateRuleConfig.builder()
                    .withRelBuilderFactory(relBuilderFactory)
                    .withOperandSupplier { b0 ->
                        b0.operand(Correlate::class.java).inputs(
                            { b1 -> b1.operand(RelNode::class.java).anyInputs() }
                        ) { b2 ->
                            b2.operand(Project::class.java).oneInput { b3 ->
                                b3.operand(Aggregate::class.java)
                                    .predicate(Aggregate::isSimple).oneInput { b4 ->
                                        b4.operand(Project::class.java).oneInput { b5 ->
                                            b5.operand(
                                                RelNode::class.java
                                            ).anyInputs()
                                        }
                                    }
                            }
                        }
                    }
                    .withDecorrelator(d)
                    .build()
            }
        }
    }
    // REVIEW jhyde 29-Oct-2007: This rule is non-static, depends on the state
    // of members in RelDecorrelator, and has side-effects in the decorrelator.
    // This breaks the contract of a planner rule, and the rule will not be
    // reusable in other planners.
    // REVIEW jvs 29-Oct-2007:  Shouldn't it also be incorporating
    // the flavor attribute into the description?
    /** Planner rule that adjusts projects when counts are added.  */
    class AdjustProjectForCountAggregateRule internal constructor(config: AdjustProjectForCountAggregateRuleConfig) :
        RelRule<AdjustProjectForCountAggregateRule.AdjustProjectForCountAggregateRuleConfig?>(config) {
        val d: RelDecorrelator

        /** Creates an AdjustProjectForCountAggregateRule.  */
        init {
            d = requireNonNull(config.decorrelator())
        }

        @Override
        fun onMatch(call: RelOptRuleCall) {
            val correlate: Correlate = call.rel(0)
            val left: RelNode = call.rel(1)
            val aggOutputProject: Project
            val aggregate: Aggregate
            if (config.flavor()) {
                aggOutputProject = call.rel(2)
                aggregate = call.rel(3)
            } else {
                aggregate = call.rel(2)

                // Create identity projection
                val projects: List<Pair<RexNode, String>> = ArrayList()
                val fields: List<RelDataTypeField> = aggregate.getRowType().getFieldList()
                for (i in 0 until fields.size()) {
                    projects.add(RexInputRef.of2(projects.size(), fields))
                }
                val relBuilder: RelBuilder = call.builder()
                relBuilder.push(aggregate)
                    .projectNamed(Pair.left(projects), Pair.right(projects), true)
                aggOutputProject = relBuilder.build() as Project
            }
            onMatch2(call, correlate, left, aggOutputProject, aggregate)
        }

        private fun onMatch2(
            call: RelOptRuleCall,
            correlate: Correlate,
            leftInput: RelNode,
            aggOutputProject: Project,
            aggregate: Aggregate
        ) {
            if (d.generatedCorRels.contains(correlate)) {
                // This Correlate was generated by a previous invocation of
                // this rule. No further work to do.
                return
            }
            d.setCurrent(call.getPlanner().getRoot(), correlate)

            // check for this pattern
            // The pattern matching could be simplified if rules can be applied
            // during decorrelation,
            //
            // CorrelateRel(left correlation, condition = true)
            //   leftInput
            //   Project-A (a RexNode)
            //     Aggregate (groupby (0), agg0(), agg1()...)

            // check aggOutputProj projects only one expression
            val aggOutputProjExprs: List<RexNode> = aggOutputProject.getProjects()
            if (aggOutputProjExprs.size() !== 1) {
                return
            }
            val joinType: JoinRelType = correlate.getJoinType()
            // corRel.getCondition was here, however Correlate was updated so it
            // never includes a join condition. The code was not modified for brevity.
            val joinCond: RexNode = d.relBuilder.literal(true)
            if (joinType !== JoinRelType.LEFT
                || joinCond !== d.relBuilder.literal(true)
            ) {
                return
            }

            // check that the agg is on the entire input
            if (!aggregate.getGroupSet().isEmpty()) {
                return
            }
            val aggCalls: List<AggregateCall> = aggregate.getAggCallList()
            val isCount: Set<Integer> = HashSet()

            // remember the count() positions
            var i = -1
            for (aggCall in aggCalls) {
                ++i
                if (aggCall.getAggregation() is SqlCountAggFunction) {
                    isCount.add(i)
                }
            }

            // now rewrite the plan to
            //
            // Project-A' (all LHS plus transformed original projections,
            //             replacing references to count() with case statement)
            //   Correlate(left correlation, condition = true)
            //     leftInput
            //     Aggregate(groupby (0), agg0(), agg1()...)
            //
            val rexBuilder: RexBuilder = d.relBuilder.getRexBuilder()
            val requiredNodes: List<RexNode> = correlate.getRequiredColumns().asList().stream()
                .map { ord -> rexBuilder.makeInputRef(correlate, ord) }
                .collect(Collectors.toList())
            val newCorrelate: Correlate = d.relBuilder.push(leftInput)
                .push(aggregate).correlate(
                    correlate.getJoinType(),
                    correlate.getCorrelationId(),
                    requiredNodes
                ).build() as Correlate


            // remember this rel so we don't fire rule on it again
            // REVIEW jhyde 29-Oct-2007: rules should not save state; rule
            // should recognize patterns where it does or does not need to do
            // work
            d.generatedCorRels.add(newCorrelate)

            // need to update the mapCorToCorRel Update the output position
            // for the corVars: only pass on the corVars that are not used in
            // the join key.
            if (d.cm.mapCorToCorRel.get(correlate.getCorrelationId()) === correlate) {
                d.cm.mapCorToCorRel.put(correlate.getCorrelationId(), newCorrelate)
            }
            val newOutput: RelNode = d.aggregateCorrelatorOutput(newCorrelate, aggOutputProject, isCount)
            call.transformTo(newOutput)
        }

        /** Rule configuration.  */
        @Value.Immutable(singleton = false)
        interface AdjustProjectForCountAggregateRuleConfig : Config {
            @Override
            fun toRule(): AdjustProjectForCountAggregateRule? {
                return AdjustProjectForCountAggregateRule(this)
            }

            /** Returns the flavor of the rule (true for 4 operands, false for 3
             * operands).  */
            fun flavor(): Boolean

            /** Sets [.flavor].  */
            fun withFlavor(flavor: Boolean): AdjustProjectForCountAggregateRuleConfig?
        }

        companion object {
            fun config(
                flavor: Boolean, decorrelator: RelDecorrelator?, relBuilderFactory: RelBuilderFactory?
            ): AdjustProjectForCountAggregateRuleConfig {
                return ImmutableAdjustProjectForCountAggregateRuleConfig.builder()
                    .withRelBuilderFactory(relBuilderFactory)
                    .withOperandSupplier { b0 ->
                        b0.operand(Correlate::class.java).inputs(
                            { b1 -> b1.operand(RelNode::class.java).anyInputs() }
                        ) { b2 ->
                            if (flavor) b2.operand(Project::class.java)
                                .oneInput { b3 -> b3.operand(Aggregate::class.java).anyInputs() } else b2.operand(
                                Aggregate::class.java
                            ).anyInputs()
                        }
                    }
                    .withFlavor(flavor)
                    .withDecorrelator(decorrelator)
                    .build()
            }
        }
    }

    /**
     * A unique reference to a correlation field.
     *
     *
     * For instance, if a RelNode references emp.name multiple times, it would
     * result in multiple `CorRef` objects that differ just in
     * [CorRef.uniqueKey].
     */
    class CorRef(corr: CorrelationId, field: Int, uniqueKey: Int) : Comparable<CorRef?> {
        val uniqueKey: Int
        val corr: CorrelationId
        val field: Int

        init {
            this.corr = corr
            this.field = field
            this.uniqueKey = uniqueKey
        }

        @Override
        override fun toString(): String {
            return corr.getName() + '.' + field
        }

        @Override
        override fun hashCode(): Int {
            return Objects.hash(uniqueKey, corr, field)
        }

        @Override
        override fun equals(@Nullable o: Object): Boolean {
            return (this === o
                    || (o is CorRef
                    && uniqueKey == (o as CorRef).uniqueKey && corr === (o as CorRef).corr && field == (o as CorRef).field))
        }

        @Override
        operator fun compareTo(o: CorRef): Int {
            var c: Int = corr.compareTo(o.corr)
            if (c != 0) {
                return c
            }
            c = Integer.compare(field, o.field)
            return if (c != 0) {
                c
            } else Integer.compare(uniqueKey, o.uniqueKey)
        }

        fun def(): CorDef {
            return CorDef(corr, field)
        }
    }

    /** A correlation and a field.  */
    class CorDef(corr: CorrelationId, field: Int) : Comparable<CorDef?> {
        val corr: CorrelationId
        val field: Int

        init {
            this.corr = corr
            this.field = field
        }

        @Override
        override fun toString(): String {
            return corr.getName() + '.' + field
        }

        @Override
        override fun hashCode(): Int {
            return Objects.hash(corr, field)
        }

        @Override
        override fun equals(@Nullable o: Object): Boolean {
            return (this === o
                    || (o is CorDef
                    && corr === (o as CorDef).corr && field == (o as CorDef).field))
        }

        @Override
        operator fun compareTo(o: CorDef): Int {
            val c: Int = corr.compareTo(o.corr)
            return if (c != 0) {
                c
            } else Integer.compare(field, o.field)
        }
    }

    /** A map of the locations of
     * [org.apache.calcite.rel.core.Correlate]
     * in a tree of [RelNode]s.
     *
     *
     * It is used to drive the decorrelation process.
     * Treat it as immutable; rebuild if you modify the tree.
     *
     *
     * There are three maps:
     *
     *  1. [.mapRefRelToCorRef] maps a [RelNode] to the correlated
     * variables it references;
     *
     *  1. [.mapCorToCorRel] maps a correlated variable to the
     * [Correlate] providing it;
     *
     *  1. [.mapFieldAccessToCorRef] maps a rex field access to
     * the corVar it represents. Because typeFlattener does not clone or
     * modify a correlated field access this map does not need to be
     * updated.
     *
     *   */
    protected class CorelMap private constructor(
        mapRefRelToCorRef: Multimap<RelNode, CorRef>,
        mapCorToCorRel: NavigableMap<CorrelationId, RelNode>,
        mapFieldAccessToCorRef: Map<RexFieldAccess, CorRef>
    ) {
        private val mapRefRelToCorRef: Multimap<RelNode, CorRef>
        val mapCorToCorRel: NavigableMap<CorrelationId, RelNode>
        private val mapFieldAccessToCorRef: Map<RexFieldAccess, CorRef>

        // TODO: create immutable copies of all maps
        init {
            this.mapRefRelToCorRef = mapRefRelToCorRef
            this.mapCorToCorRel = mapCorToCorRel
            this.mapFieldAccessToCorRef = ImmutableMap.copyOf(mapFieldAccessToCorRef)
        }

        @Override
        override fun toString(): String {
            return """
                mapRefRelToCorRef=$mapRefRelToCorRef
                mapCorToCorRel=$mapCorToCorRel
                mapFieldAccessToCorRef=$mapFieldAccessToCorRef

                """.trimIndent()
        }

        @SuppressWarnings("UndefinedEquals")
        @Override
        override fun equals(@Nullable obj: Object): Boolean {
            return (obj === this
                    || (obj is CorelMap // TODO: Multimap does not have well-defined equals behavior
                    && mapRefRelToCorRef.equals((obj as CorelMap).mapRefRelToCorRef)
                    && mapCorToCorRel.equals((obj as CorelMap).mapCorToCorRel)
                    && mapFieldAccessToCorRef.equals(
                (obj as CorelMap).mapFieldAccessToCorRef
            )))
        }

        @Override
        override fun hashCode(): Int {
            return Objects.hash(
                mapRefRelToCorRef, mapCorToCorRel,
                mapFieldAccessToCorRef
            )
        }

        fun getMapCorToCorRel(): NavigableMap<CorrelationId, RelNode> {
            return mapCorToCorRel
        }

        /**
         * Returns whether there are any correlating variables in this statement.
         *
         * @return whether there are any correlating variables
         */
        fun hasCorrelation(): Boolean {
            return !mapCorToCorRel.isEmpty()
        }

        companion object {
            /** Creates a CorelMap with given contents.  */
            fun of(
                mapRefRelToCorVar: SortedSetMultimap<RelNode, CorRef>,
                mapCorToCorRel: NavigableMap<CorrelationId, RelNode>,
                mapFieldAccessToCorVar: Map<RexFieldAccess, CorRef>
            ): CorelMap {
                return CorelMap(
                    mapRefRelToCorVar, mapCorToCorRel,
                    mapFieldAccessToCorVar
                )
            }
        }
    }

    /** Builds a [org.apache.calcite.sql2rel.RelDecorrelator.CorelMap].  */
    class CorelMapBuilder : RelHomogeneousShuttle() {
        val mapCorToCorRel: NavigableMap<CorrelationId, RelNode> = TreeMap()
        val mapRefRelToCorRef: SortedSetMultimap<RelNode, CorRef> = MultimapBuilder.SortedSetMultimapBuilder.hashKeys()
            .treeSetValues()
            .build()
        val mapFieldAccessToCorVar: Map<RexFieldAccess, CorRef> = HashMap()
        val offset: Holder<Integer> = Holder.of(0)
        var corrIdGenerator = 0

        /** Creates a CorelMap by iterating over a [RelNode] tree.  */
        fun build(vararg rels: RelNode?): CorelMap {
            for (rel in rels) {
                stripHep(rel).accept(this)
            }
            return CorelMap(
                mapRefRelToCorRef, mapCorToCorRel,
                mapFieldAccessToCorVar
            )
        }

        @Override
        fun visit(other: RelNode): RelNode {
            if (other is Join) {
                val join: Join = other as Join
                try {
                    stack.push(join)
                    join.getCondition().accept(rexVisitor(join))
                } finally {
                    stack.pop()
                }
                return visitJoin(join)
            } else if (other is Correlate) {
                val correlate: Correlate = other as Correlate
                mapCorToCorRel.put(correlate.getCorrelationId(), correlate)
                return visitJoin(correlate)
            } else if (other is Filter) {
                val filter: Filter = other as Filter
                try {
                    stack.push(filter)
                    filter.getCondition().accept(rexVisitor(filter))
                } finally {
                    stack.pop()
                }
            } else if (other is Project) {
                val project: Project = other as Project
                try {
                    stack.push(project)
                    for (node in project.getProjects()) {
                        node.accept(rexVisitor(project))
                    }
                } finally {
                    stack.pop()
                }
            }
            return super.visit(other)
        }

        @Override
        protected fun visitChild(
            parent: RelNode?, i: Int,
            input: RelNode
        ): RelNode {
            return super.visitChild(parent, i, stripHep(input))
        }

        private fun visitJoin(join: BiRel): RelNode {
            val x: Int = offset.get()
            visitChild(join, 0, join.getLeft())
            offset.set(x + join.getLeft().getRowType().getFieldCount())
            visitChild(join, 1, join.getRight())
            offset.set(x)
            return join
        }

        private fun rexVisitor(rel: RelNode): RexVisitorImpl<Void> {
            return object : RexVisitorImpl<Void?>(true) {
                @Override
                fun visitFieldAccess(fieldAccess: RexFieldAccess): Void {
                    val ref: RexNode = fieldAccess.getReferenceExpr()
                    if (ref is RexCorrelVariable) {
                        val `var`: RexCorrelVariable = ref as RexCorrelVariable
                        if (mapFieldAccessToCorVar.containsKey(fieldAccess)) {
                            // for cases where different Rel nodes are referring to
                            // same correlation var (e.g. in case of NOT IN)
                            // avoid generating another correlation var
                            // and record the 'rel' is using the same correlation
                            mapRefRelToCorRef.put(
                                rel,
                                mapFieldAccessToCorVar[fieldAccess]
                            )
                        } else {
                            val correlation = CorRef(
                                `var`.id, fieldAccess.getField().getIndex(),
                                corrIdGenerator++
                            )
                            mapFieldAccessToCorVar.put(fieldAccess, correlation)
                            mapRefRelToCorRef.put(rel, correlation)
                        }
                    }
                    return super.visitFieldAccess(fieldAccess)
                }

                @Override
                fun visitSubQuery(subQuery: RexSubQuery): Void {
                    subQuery.rel.accept(this@CorelMapBuilder)
                    return super.visitSubQuery(subQuery)
                }
            }
        }
    }

    /** Frame describing the relational expression after decorrelation
     * and where to find the output fields and correlation variables
     * among its output fields.  */
    class Frame(
        oldRel: RelNode?, r: RelNode?, corDefOutputs: NavigableMap<CorDef?, Integer?>?,
        oldToNewOutputs: Map<Integer?, Integer?>?
    ) {
        val r: RelNode
        val corDefOutputs: ImmutableSortedMap<CorDef, Integer>
        val oldToNewOutputs: ImmutableSortedMap<Integer, Integer>

        init {
            this.r = requireNonNull(r, "r")
            this.corDefOutputs = ImmutableSortedMap.copyOf(corDefOutputs)
            this.oldToNewOutputs = ImmutableSortedMap.copyOf(oldToNewOutputs)
            assert(
                allLessThan(
                    this.corDefOutputs.values(),
                    r.getRowType().getFieldCount(), Litmus.THROW
                )
            )
            assert(
                allLessThan(
                    this.oldToNewOutputs.keySet(),
                    oldRel.getRowType().getFieldCount(), Litmus.THROW
                )
            )
            assert(
                allLessThan(
                    this.oldToNewOutputs.values(),
                    r.getRowType().getFieldCount(), Litmus.THROW
                )
            )
        }
    }

    /** Base configuration for rules that are non-static in a RelDecorrelator.  */
    interface Config : RelRule.Config {
        /** Returns the RelDecorrelator that will be context for the created
         * rule instance.  */
        fun decorrelator(): RelDecorrelator?

        /** Sets [.decorrelator].  */
        fun withDecorrelator(decorrelator: RelDecorrelator?): Config?
    }
    // -------------------------------------------------------------------------
    //  Getter/Setter
    // -------------------------------------------------------------------------
    /**
     * Returns the `visitor` on which the `MethodDispatcher` dispatches
     * each `decorrelateRel` method, the default implementation returns this instance,
     * if you got a sub-class, override this method to replace the `visitor` as the
     * sub-class instance.
     */
    protected val visitor: RelDecorrelator
        protected get() = this

    /** Returns the rules applied on the rel after decorrelation, never null.  */
    protected val postDecorrelateRules: Collection<Any>
        protected get() = Collections.emptyList()

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        private val SQL2REL_LOGGER: Logger = CalciteTrace.getSqlToRelTracer()

        //~ Methods ----------------------------------------------------------------
        @Deprecated // to be removed before 2.0
        fun decorrelateQuery(rootRel: RelNode): RelNode {
            val relBuilder: RelBuilder = RelFactories.LOGICAL_BUILDER.create(rootRel.getCluster(), null)
            return decorrelateQuery(rootRel, relBuilder)
        }

        /** Decorrelates a query.
         *
         *
         * This is the main entry point to `RelDecorrelator`.
         *
         * @param rootRel           Root node of the query
         * @param relBuilder        Builder for relational expressions
         *
         * @return Equivalent query with all
         * [org.apache.calcite.rel.core.Correlate] instances removed
         */
        fun decorrelateQuery(
            rootRel: RelNode,
            relBuilder: RelBuilder
        ): RelNode {
            val corelMap = CorelMapBuilder().build(rootRel)
            if (!corelMap.hasCorrelation()) {
                return rootRel
            }
            val cluster: RelOptCluster = rootRel.getCluster()
            val decorrelator = RelDecorrelator(
                corelMap,
                cluster.getPlanner().getContext(), relBuilder
            )
            var newRootRel: RelNode = decorrelator.removeCorrelationViaRule(rootRel)
            if (SQL2REL_LOGGER.isDebugEnabled()) {
                SQL2REL_LOGGER.debug(
                    RelOptUtil.dumpPlan(
                        "Plan after removing Correlator", newRootRel,
                        SqlExplainFormat.TEXT, SqlExplainLevel.EXPPLAN_ATTRIBUTES
                    )
                )
            }
            if (!decorrelator.cm.mapCorToCorRel.isEmpty()) {
                newRootRel = decorrelator.decorrelate(newRootRel)
            }

            // Re-propagate the hints.
            newRootRel = RelOptUtil.propagateRelHints(newRootRel, true)
            return newRootRel
        }

        /**
         * Shift the mapping to fixed offset from the `startIndex`.
         *
         * @param mapping    The original mapping
         * @param startIndex Any output whose index equals with or bigger than the starting index
         * would be shift
         * @param offset     Shift offset
         */
        private fun shiftMapping(mapping: Map<Integer?, Integer>, startIndex: Int, offset: Int) {
            for (entry in mapping.entrySet()) {
                if (entry.getValue() >= startIndex) {
                    entry.setValue(entry.getValue() + offset)
                }
            }
        }

        /** Returns a literal output field, or null if it is not literal.  */
        @Nullable
        private fun projectedLiteral(rel: RelNode, i: Int): RexLiteral? {
            if (rel is Project) {
                val project: Project = rel as Project
                val node: RexNode = project.getProjects().get(i)
                if (node is RexLiteral) {
                    return node as RexLiteral
                }
            }
            return null
        }

        /** Returns whether all of a collection of [CorRef]s are satisfied
         * by at least one of a collection of [CorDef]s.  */
        private fun hasAll(
            corRefs: Collection<CorRef>,
            corDefs: Collection<CorDef>
        ): Boolean {
            for (corRef in corRefs) {
                if (!has(corDefs, corRef)) {
                    return false
                }
            }
            return true
        }

        /** Returns whether a [CorrelationId] is satisfied by at least one of a
         * collection of [CorDef]s.  */
        private fun has(corDefs: Collection<CorDef>, corr: CorRef): Boolean {
            for (corDef in corDefs) {
                if (corDef.corr.equals(corr.corr) && corDef.field == corr.field) {
                    return true
                }
            }
            return false
        }

        /** Finds a [RexInputRef] that is equivalent to a [CorRef],
         * and if found, throws a [org.apache.calcite.util.Util.FoundOne].  */
        @Throws(Util.FoundOne::class)
        private fun findCorrelationEquivalent(correlation: CorRef, e: RexNode) {
            when (e.getKind()) {
                EQUALS -> {
                    val call: RexCall = e as RexCall
                    val operands: List<RexNode> = call.getOperands()
                    if (references(operands[0], correlation)) {
                        throw FoundOne(operands[1])
                    }
                    if (references(operands[1], correlation)) {
                        throw FoundOne(operands[0])
                    }
                }
                AND -> for (operand in (e as RexCall).getOperands()) {
                    findCorrelationEquivalent(correlation, operand)
                }
                else -> {}
            }
        }

        private fun references(e: RexNode, correlation: CorRef): Boolean {
            return when (e.getKind()) {
                CAST -> {
                    val operand: RexNode = (e as RexCall).getOperands().get(0)
                    if (isWidening(e.getType(), operand.getType())) {
                        references(operand, correlation)
                    } else false
                }
                FIELD_ACCESS -> {
                    val f: RexFieldAccess = e as RexFieldAccess
                    if (f.getField().getIndex() === correlation.field
                        && f.getReferenceExpr() is RexCorrelVariable
                    ) {
                        if ((f.getReferenceExpr() as RexCorrelVariable).id === correlation.corr) {
                            return true
                        }
                    }
                    false
                }
                else -> false
            }
        }

        /** Returns whether one type is just a widening of another.
         *
         *
         * For example:
         *  * `VARCHAR(10)` is a widening of `VARCHAR(5)`.
         *  * `VARCHAR(10)` is a widening of `VARCHAR(10) NOT NULL`.
         *
         */
        private fun isWidening(type: RelDataType, type1: RelDataType): Boolean {
            return (type.getSqlTypeName() === type1.getSqlTypeName()
                    && type.getPrecision() >= type1.getPrecision())
        }

        private fun getNewForOldInputRef(
            currentRel: RelNode?,
            map: Map<RelNode?, Frame>, oldInputRef: RexInputRef
        ): RexInputRef {
            assert(currentRel != null)
            var oldOrdinal: Int = oldInputRef.getIndex()
            var newOrdinal = 0

            // determine which input rel oldOrdinal references, and adjust
            // oldOrdinal to be relative to that input rel
            var oldInput: RelNode? = null
            for (oldInput0 in currentRel.getInputs()) {
                val oldInputType: RelDataType = oldInput0.getRowType()
                val n: Int = oldInputType.getFieldCount()
                if (oldOrdinal < n) {
                    oldInput = oldInput0
                    break
                }
                val newInput: RelNode = requireNonNull(
                    map[oldInput0]
                ) { "map.get(oldInput0) for $oldInput0" }.r
                newOrdinal += newInput.getRowType().getFieldCount()
                oldOrdinal -= n
            }
            assert(oldInput != null)
            val frame = map[oldInput]
            assert(frame != null)

            // now oldOrdinal is relative to oldInput
            val oldLocalOrdinal = oldOrdinal

            // figure out the newLocalOrdinal, relative to the newInput.
            var newLocalOrdinal = oldLocalOrdinal
            if (!frame!!.oldToNewOutputs.isEmpty()) {
                newLocalOrdinal = frame.oldToNewOutputs.get(oldLocalOrdinal)
            }
            newOrdinal += newLocalOrdinal
            return RexInputRef(
                newOrdinal,
                frame.r.getRowType().getFieldList().get(newLocalOrdinal).getType()
            )
        }

        /* Returns an immutable map with the identity [0: 0, .., count-1: count-1]. */
        fun identityMap(count: Int): Map<Integer?, Integer> {
            val builder: ImmutableMap.Builder<Integer, Integer> = ImmutableMap.builder()
            for (i in 0 until count) {
                builder.put(i, i)
            }
            return builder.build()
        }

        fun allLessThan(
            integers: Collection<Integer?>, limit: Int,
            ret: Litmus
        ): Boolean {
            for (value in integers) {
                if (value >= limit) {
                    return ret.fail("out of range; value: {}, limit: {}", value, limit)
                }
            }
            return ret.succeed()
        }

        private fun stripHep(rel: RelNode): RelNode {
            var rel: RelNode = rel
            if (rel is HepRelVertex) {
                val hepRelVertex: HepRelVertex = rel as HepRelVertex
                rel = hepRelVertex.getCurrentRel()
            }
            return rel
        }
    }
}
