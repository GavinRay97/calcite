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
package org.apache.calcite.rel.rules

import org.apache.calcite.plan.RelOptCluster

/**
 * CalcRelSplitter operates on a
 * [org.apache.calcite.rel.core.Calc] with multiple [RexCall]
 * sub-expressions that cannot all be implemented by a single concrete
 * [RelNode].
 *
 *
 * For example, the Java and Fennel calculator do not implement an identical
 * set of operators. The Calc can be used to split a single Calc with
 * mixed Java- and Fennel-only operators into a tree of Calc object that can
 * each be individually implemented by either Java or Fennel.and splits it into
 * several Calc instances.
 *
 *
 * Currently the splitter is only capable of handling two "rel types". That
 * is, it can deal with Java vs. Fennel Calcs, but not Java vs. Fennel vs.
 * some other type of Calc.
 *
 *
 * See [ProjectToWindowRule]
 * for an example of how this class is used.
 */
abstract class CalcRelSplitter internal constructor(calc: Calc, relBuilder: RelBuilder, relTypes: Array<RelType?>) {
    //~ Instance fields --------------------------------------------------------
    protected val program: RexProgram
    private val typeFactory: RelDataTypeFactory
    private val relTypes: Array<RelType?>
    private val cluster: RelOptCluster
    private val traits: RelTraitSet
    private val child: RelNode
    protected val relBuilder: RelBuilder
    //~ Constructors -----------------------------------------------------------
    /**
     * Constructs a CalcRelSplitter.
     *
     * @param calc     Calc to split
     * @param relTypes Array of rel types, e.g. {Java, Fennel}. Must be
     * distinct.
     */
    init {
        this.relBuilder = relBuilder
        for (i in relTypes.indices) {
            assert(relTypes[i] != null)
            for (j in 0 until i) {
                assert(relTypes[i] !== relTypes[j]) { "Rel types must be distinct" }
            }
        }
        program = calc.getProgram()
        cluster = calc.getCluster()
        traits = calc.getTraitSet()
        typeFactory = calc.getCluster().getTypeFactory()
        child = calc.getInput()
        this.relTypes = relTypes
    }

    //~ Methods ----------------------------------------------------------------
    fun execute(): RelNode {
        // Check that program is valid. In particular, this means that every
        // expression is trivial (either an atom, or a function applied to
        // references to atoms) and every expression depends only on
        // expressions to the left.
        assert(program.isValid(Litmus.THROW, null))
        val exprList: List<RexNode> = program.getExprList()
        val exprs: Array<RexNode> = exprList.toArray(arrayOfNulls<RexNode>(0))
        assert(!RexUtil.containComplexExprs(exprList))

        // Figure out what level each expression belongs to.
        val exprLevels = IntArray(exprs.size)

        // The type of a level is given by
        // relTypes[levelTypeOrdinals[level]].
        val levelTypeOrdinals = IntArray(exprs.size)
        val levelCount = chooseLevels(exprs, -1, exprLevels, levelTypeOrdinals)

        // For each expression, figure out which is the highest level where it
        // is used.
        val exprMaxUsingLevelOrdinals: IntArray = HighestUsageFinder(exprs, exprLevels)
            .getMaxUsingLevelOrdinals()

        // If expressions are used as outputs, mark them as higher than that.
        val projectRefList: List<RexLocalRef> = program.getProjectList()
        val conditionRef: RexLocalRef = program.getCondition()
        for (projectRef in projectRefList) {
            exprMaxUsingLevelOrdinals[projectRef.getIndex()] = levelCount
        }
        if (conditionRef != null) {
            exprMaxUsingLevelOrdinals[conditionRef.getIndex()] = levelCount
        }

        // Print out what we've got.
        if (RULE_LOGGER.isTraceEnabled()) {
            traceLevelExpressions(
                exprs,
                exprLevels,
                levelTypeOrdinals,
                levelCount
            )
        }

        // Now build the calcs.
        var rel: RelNode = child
        val inputFieldCount: Int = program.getInputRowType().getFieldCount()
        var inputExprOrdinals = identityArray(inputFieldCount)
        var doneCondition = false
        for (level in 0 until levelCount) {
            val projectExprOrdinals: IntArray
            val outputRowType: RelDataType?
            if (level == levelCount - 1) {
                outputRowType = program.getOutputRowType()
                projectExprOrdinals = IntArray(projectRefList.size())
                for (i in projectExprOrdinals.indices) {
                    projectExprOrdinals[i] = projectRefList[i].getIndex()
                }
            } else {
                outputRowType = null

                // Project the expressions which are computed at this level or
                // before, and will be used at later levels.
                val projectExprOrdinalList: List<Integer> = ArrayList()
                for (i in exprs.indices) {
                    val expr: RexNode = exprs[i]
                    if (expr is RexLiteral) {
                        // Don't project literals. They are always created in
                        // the level where they are used.
                        exprLevels[i] = -1
                        continue
                    }
                    if (exprLevels[i] <= level
                        && exprMaxUsingLevelOrdinals[i] > level
                    ) {
                        projectExprOrdinalList.add(i)
                    }
                }
                projectExprOrdinals = Ints.toArray(projectExprOrdinalList)
            }
            val relType = relTypes[levelTypeOrdinals[level]]

            // Can we do the condition this level?
            var conditionExprOrdinal = -1
            if (conditionRef != null && !doneCondition) {
                conditionExprOrdinal = conditionRef.getIndex()
                if (exprLevels[conditionExprOrdinal] > level
                    || !relType!!.supportsCondition()
                ) {
                    // stand down -- we're not ready to do the condition yet
                    conditionExprOrdinal = -1
                } else {
                    doneCondition = true
                }
            }
            val program1: RexProgram = createProgramForLevel(
                level,
                levelCount,
                rel.getRowType(),
                exprs,
                exprLevels,
                inputExprOrdinals,
                projectExprOrdinals,
                conditionExprOrdinal,
                outputRowType
            )
            rel = relType!!.makeRel(cluster, traits, relBuilder, rel, program1)

            // Sometimes a level's program merely projects its inputs. We don't
            // want these. They cause an explosion in the search space.
            if (rel is LogicalCalc
                && (rel as LogicalCalc).getProgram().isTrivial()
            ) {
                rel = rel.getInput(0)
            }
            rel = handle(rel)

            // The outputs of this level will be the inputs to the next level.
            inputExprOrdinals = projectExprOrdinals
        }
        Preconditions.checkArgument(
            doneCondition || conditionRef == null,
            "unhandled condition"
        )
        return rel
    }

    /**
     * Opportunity to further refine the relational expression created for a
     * given level. The default implementation returns the relational expression
     * unchanged.
     */
    protected fun handle(rel: RelNode): RelNode {
        return rel
    }

    /**
     * Figures out which expressions to calculate at which level.
     *
     * @param exprs             Array of expressions
     * @param conditionOrdinal  Ordinal of the condition expression, or -1 if no
     * condition
     * @param exprLevels        Level ordinal for each expression (output)
     * @param levelTypeOrdinals The type of each level (output)
     * @return Number of levels required
     */
    private fun chooseLevels(
        exprs: Array<RexNode>,
        conditionOrdinal: Int,
        exprLevels: IntArray,
        levelTypeOrdinals: IntArray
    ): Int {
        val inputFieldCount: Int = program.getInputRowType().getFieldCount()
        var levelCount = 0
        val maxInputFinder = MaxInputFinder(exprLevels)
        val relTypesPossibleForTopLevel = BooleanArray(relTypes.size)
        Arrays.fill(relTypesPossibleForTopLevel, true)

        // Compute the order in which to visit expressions.
        val cohorts: List<Set<Integer>> = cohorts
        val permutation: List<Integer> = computeTopologicalOrdering(exprs, cohorts)
        for (i in permutation) {
            val expr: RexNode = exprs[i]
            val condition = i == conditionOrdinal
            if (i < inputFieldCount) {
                assert(expr is RexInputRef)
                exprLevels[i] = -1
                continue
            }

            // Deduce the minimum level of the expression. An expression must
            // be at a level greater than or equal to all of its inputs.
            var level = maxInputFinder.maxInputFor(expr)

            // If the expression is in a cohort, it can occur no lower than the
            // levels of other expressions in the same cohort.
            val cohort: Set<Integer>? = findCohort(cohorts, i)
            if (cohort != null) {
                for (exprOrdinal in cohort) {
                    if (exprOrdinal == i) {
                        // Already did this member of the cohort. It's a waste
                        // of effort to repeat.
                        continue
                    }
                    val cohortExpr: RexNode = exprs[exprOrdinal]
                    val cohortLevel = maxInputFinder.maxInputFor(cohortExpr)
                    if (cohortLevel > level) {
                        level = cohortLevel
                    }
                }
            }

            // Try to implement this expression at this level.
            // If that is not possible, try to implement it at higher levels.
            levelLoop@ while (true) {
                if (level >= levelCount) {
                    // This is a new level. We can use any type we like.
                    for (relTypeOrdinal in relTypes.indices) {
                        if (!relTypesPossibleForTopLevel[relTypeOrdinal]) {
                            continue
                        }
                        if (relTypes[relTypeOrdinal]!!.canImplement(
                                expr,
                                condition
                            )
                        ) {
                            // Success. We have found a type where we can
                            // implement this expression.
                            exprLevels[i] = level
                            levelTypeOrdinals[level] = relTypeOrdinal
                            assert(
                                level == 0
                                        || (levelTypeOrdinals[level - 1]
                                        != levelTypeOrdinals[level])
                            ) { "successive levels of same type" }

                            // Figure out which of the other reltypes are
                            // still possible for this level.
                            // Previous reltypes are not possible.
                            for (j in 0 until relTypeOrdinal) {
                                relTypesPossibleForTopLevel[j] = false
                            }

                            // Successive reltypes may be possible.
                            for (j in relTypeOrdinal + 1 until relTypes.size) {
                                if (relTypesPossibleForTopLevel[j]) {
                                    relTypesPossibleForTopLevel[j] = relTypes[j]!!.canImplement(
                                        expr,
                                        condition
                                    )
                                }
                            }

                            // Move to next level.
                            levelTypeOrdinals[levelCount] = firstSet(relTypesPossibleForTopLevel)
                            ++levelCount
                            Arrays.fill(relTypesPossibleForTopLevel, true)
                            break@levelLoop
                        }
                    }

                    // None of the reltypes still active for this level could
                    // implement expr. But maybe we could succeed with a new
                    // level, with all options open?
                    if (count(relTypesPossibleForTopLevel) >= relTypes.size) {
                        // Cannot implement for any type.
                        throw AssertionError("cannot implement $expr")
                    }
                    levelTypeOrdinals[levelCount] = firstSet(relTypesPossibleForTopLevel)
                    ++levelCount
                    Arrays.fill(relTypesPossibleForTopLevel, true)
                } else {
                    val levelTypeOrdinal = levelTypeOrdinals[level]
                    if (!relTypes[levelTypeOrdinal]!!.canImplement(
                            expr,
                            condition
                        )
                    ) {
                        // Cannot implement this expression in this type;
                        // continue to next level.
                        ++level
                        continue
                    }
                    exprLevels[i] = level
                    break
                }
                ++level
            }
        }
        if (levelCount > 0) {
            // The latest level should be CalcRelType otherwise literals cannot be
            // implemented.
            assert("CalcRelType".equals(relTypes[0]!!.name)) {
                ("The first RelType should be CalcRelType for proper RexLiteral"
                        + " implementation at the last level, got " + relTypes[0]!!.name)
            }
            if (levelTypeOrdinals[levelCount - 1] != 0) {
                levelCount++
            }
        }
        return levelCount
    }

    /**
     * Creates a program containing the expressions for a given level.
     *
     *
     * The expression list of the program will consist of all entries in the
     * expression list `allExprs[i]` for which the corresponding
     * level ordinal `exprLevels[i]` is equal to `level`.
     * Expressions are mapped according to `inputExprOrdinals`.
     *
     * @param level                Level ordinal
     * @param levelCount           Number of levels
     * @param inputRowType         Input row type
     * @param allExprs             Array of all expressions
     * @param exprLevels           Array of the level ordinal of each expression
     * @param inputExprOrdinals    Ordinals in the expression list of input
     * expressions. Input expression `i`
     * will be found at position
     * `inputExprOrdinals[i]`.
     * @param projectExprOrdinals  Ordinals of the expressions to be output this
     * level.
     * @param conditionExprOrdinal Ordinal of the expression to form the
     * condition for this level, or -1 if there is no
     * condition.
     * @param outputRowType        Output row type
     * @return Relational expression
     */
    private fun createProgramForLevel(
        level: Int,
        levelCount: Int,
        inputRowType: RelDataType,
        allExprs: Array<RexNode>,
        exprLevels: IntArray,
        inputExprOrdinals: IntArray,
        projectExprOrdinals: IntArray,
        conditionExprOrdinal: Int,
        @Nullable outputRowType: RelDataType?
    ): RexProgram {
        // Build a list of expressions to form the calc.
        var outputRowType: RelDataType? = outputRowType
        val exprs: List<RexNode> = ArrayList()

        // exprInverseOrdinals describes where an expression in allExprs comes
        // from -- from an input, from a calculated expression, or -1 if not
        // available at this level.
        val exprInverseOrdinals = IntArray(allExprs.size)
        Arrays.fill(exprInverseOrdinals, -1)
        var j = 0

        // First populate the inputs. They were computed at some previous level
        // and are used here.
        for (i in inputExprOrdinals.indices) {
            val inputExprOrdinal = inputExprOrdinals[i]
            exprs.add(
                RexInputRef(
                    i,
                    allExprs[inputExprOrdinal].getType()
                )
            )
            exprInverseOrdinals[inputExprOrdinal] = j
            ++j
        }

        // Next populate the computed expressions.
        val shuttle: RexShuttle = InputToCommonExprConverter(
            exprInverseOrdinals,
            exprLevels,
            level,
            inputExprOrdinals,
            allExprs
        )
        for (i in allExprs.indices) {
            if (exprLevels[i] == level
                || exprLevels[i] == -1 && level == levelCount - 1 && allExprs[i] is RexLiteral
            ) {
                val expr: RexNode = allExprs[i]
                val translatedExpr: RexNode = expr.accept(shuttle)
                exprs.add(translatedExpr)
                assert(exprInverseOrdinals[i] == -1)
                exprInverseOrdinals[i] = j
                ++j
            }
        }

        // Form the projection and condition list. Project and condition
        // ordinals are offsets into allExprs, so we need to map them into
        // exprs.
        val projectRefs: List<RexLocalRef> = ArrayList(projectExprOrdinals.size)
        val fieldNames: List<String> = ArrayList(projectExprOrdinals.size)
        for (i in projectExprOrdinals.indices) {
            val projectExprOrdinal = projectExprOrdinals[i]
            val index = exprInverseOrdinals[projectExprOrdinal]
            assert(index >= 0)
            val expr: RexNode = allExprs[projectExprOrdinal]
            projectRefs.add(RexLocalRef(index, expr.getType()))

            // Inherit meaningful field name if possible.
            fieldNames.add(deriveFieldName(expr, i))
        }
        val conditionRef: RexLocalRef?
        if (conditionExprOrdinal >= 0) {
            val index = exprInverseOrdinals[conditionExprOrdinal]
            conditionRef = RexLocalRef(
                index,
                allExprs[conditionExprOrdinal].getType()
            )
        } else {
            conditionRef = null
        }
        if (outputRowType == null) {
            outputRowType = RexUtil.createStructType(typeFactory, projectRefs, fieldNames, null)
        }
        // Program is NOT normalized here (e.g. can contain literals in
        // call operands), since literals should be inlined.
        return RexProgram(
            inputRowType, exprs, projectRefs, conditionRef, outputRowType
        )
    }

    private fun deriveFieldName(expr: RexNode, ordinal: Int): String {
        if (expr is RexInputRef) {
            val inputIndex: Int = (expr as RexInputRef).getIndex()
            val fieldName: String = child.getRowType().getFieldList().get(inputIndex).getName()
            // Don't inherit field names like '$3' from child: that's
            // confusing.
            if (!fieldName.startsWith("$") || fieldName.startsWith("\$EXPR")) {
                return fieldName
            }
        }
        return "$$ordinal"
    }

    /**
     * Traces the given array of level expression lists at the finer level.
     *
     * @param exprs             Array expressions
     * @param exprLevels        For each expression, the ordinal of its level
     * @param levelTypeOrdinals For each level, the ordinal of its type in
     * the [.relTypes] array
     * @param levelCount        The number of levels
     */
    private fun traceLevelExpressions(
        exprs: Array<RexNode>,
        exprLevels: IntArray,
        levelTypeOrdinals: IntArray,
        levelCount: Int
    ) {
        val traceMsg = StringWriter()
        val traceWriter = PrintWriter(traceMsg)
        traceWriter.println("FarragoAutoCalcRule result expressions for: ")
        traceWriter.println(program.toString())
        for (level in 0 until levelCount) {
            traceWriter.println(
                "Rel Level " + level
                        + ", type " + relTypes[levelTypeOrdinals[level]]
            )
            for (i in exprs.indices) {
                val expr: RexNode = exprs[i]
                assert(exprLevels[i] >= -1 && exprLevels[i] < levelCount) { "expression's level is out of range" }
                if (exprLevels[i] == level) {
                    traceWriter.println("\t$i: $expr")
                }
            }
            traceWriter.println()
        }
        val msg: String = traceMsg.toString()
        RULE_LOGGER.trace(msg)
    }

    /**
     * Returns whether a relational expression can be implemented solely in a
     * given [RelType].
     *
     * @param rel         Calculation relational expression
     * @param relTypeName Name of a [RelType]
     * @return Whether relational expression can be implemented
     */
    protected fun canImplement(rel: LogicalCalc, relTypeName: String): Boolean {
        for (relType in relTypes) {
            if (relType!!.name.equals(relTypeName)) {
                return relType.canImplement(rel.getProgram())
            }
        }
        throw AssertionError("unknown type $relTypeName")
    }

    /**
     * Returns a list of sets of expressions that should be on the same level.
     *
     *
     * For example, if this method returns { {3, 5}, {4, 7} }, it means that
     * expressions 3 and 5, should be on the same level, and expressions 4 and 7
     * should be on the same level. The two cohorts do not need to be on the
     * same level.
     *
     *
     * The list is best effort. If it is not possible to arrange that the
     * expressions in a cohort are on the same level, the [.execute]
     * method will still succeed.
     *
     *
     * The default implementation of this method returns the empty list;
     * expressions will be put on the most suitable level. This is generally
     * the lowest possible level, except for literals, which are placed at the
     * level where they are used.
     *
     * @return List of cohorts, that is sets of expressions, that the splitting
     * algorithm should attempt to place on the same level
     */
    protected val cohorts: List<Set<Any>>
        protected get() = Collections.emptyList()
    //~ Inner Classes ----------------------------------------------------------
    /** Type of relational expression. Determines which kinds of
     * expressions it can handle.  */
    abstract class RelType protected constructor(val name: String) {
        @Override
        override fun toString(): String {
            return name
        }

        abstract fun canImplement(field: RexFieldAccess?): Boolean
        abstract fun canImplement(param: RexDynamicParam?): Boolean
        abstract fun canImplement(literal: RexLiteral?): Boolean
        abstract fun canImplement(call: RexCall?): Boolean
        fun supportsCondition(): Boolean {
            return true
        }

        fun makeRel(
            cluster: RelOptCluster?,
            traitSet: RelTraitSet?, relBuilder: RelBuilder?, input: RelNode?,
            program: RexProgram?
        ): RelNode {
            return LogicalCalc.create(input, program)
        }

        /**
         * Returns whether this `RelType` can implement a given
         * expression.
         *
         * @param expr      Expression
         * @param condition Whether expression is a condition
         * @return Whether this `RelType` can implement a given
         * expression.
         */
        fun canImplement(expr: RexNode, condition: Boolean): Boolean {
            return if (condition && !supportsCondition()) {
                false
            } else try {
                expr.accept(ImplementTester(this))
                true
            } catch (e: CannotImplement) {
                Util.swallow(e, null)
                false
            }
        }

        /**
         * Returns whether this tester's `RelType` can implement a
         * given program.
         *
         * @param program Program
         * @return Whether this tester's `RelType` can implement a
         * given program.
         */
        fun canImplement(program: RexProgram): Boolean {
            if (program.getCondition() != null
                && !canImplement(program.getCondition(), true)
            ) {
                return false
            }
            for (expr in program.getExprList()) {
                if (!canImplement(expr, false)) {
                    return false
                }
            }
            return true
        }
    }

    /**
     * Visitor which returns whether an expression can be implemented in a given
     * type of relational expression.
     */
    private class ImplementTester internal constructor(private val relType: RelType) : RexVisitorImpl<Void?>(false) {
        @Override
        fun visitCall(call: RexCall?): Void? {
            if (!relType.canImplement(call)) {
                throw CannotImplement.INSTANCE
            }
            return null
        }

        @Override
        fun visitDynamicParam(dynamicParam: RexDynamicParam?): Void? {
            if (!relType.canImplement(dynamicParam)) {
                throw CannotImplement.INSTANCE
            }
            return null
        }

        @Override
        fun visitFieldAccess(fieldAccess: RexFieldAccess?): Void? {
            if (!relType.canImplement(fieldAccess)) {
                throw CannotImplement.INSTANCE
            }
            return null
        }

        @Override
        fun visitLiteral(literal: RexLiteral?): Void? {
            if (!relType.canImplement(literal)) {
                throw CannotImplement.INSTANCE
            }
            return null
        }
    }

    /**
     * Control exception for [ImplementTester].
     */
    private object CannotImplement : RuntimeException() {
        @SuppressWarnings("ThrowableInstanceNeverThrown")
        val INSTANCE: CannotImplement = CannotImplement()
    }

    /**
     * Shuttle which converts every reference to an input field in an expression
     * to a reference to a common sub-expression.
     */
    private class InputToCommonExprConverter internal constructor(
        private val exprInverseOrdinals: IntArray,
        private val exprLevels: IntArray,
        private val level: Int,
        private val inputExprOrdinals: IntArray,
        allExprs: Array<RexNode>
    ) : RexShuttle() {
        private val allExprs: Array<RexNode>

        init {
            this.allExprs = allExprs
        }

        @Override
        fun visitInputRef(input: RexInputRef): RexNode {
            val index = exprInverseOrdinals[input.getIndex()]
            assert(index >= 0)
            return RexLocalRef(
                index,
                input.getType()
            )
        }

        @Override
        fun visitLocalRef(local: RexLocalRef): RexNode {
            // A reference to a local variable becomes a reference to an input
            // if the local was computed at a previous level.
            val localIndex: Int = local.getIndex()
            val exprLevel = exprLevels[localIndex]
            return if (exprLevel < level) {
                if (allExprs[localIndex] is RexLiteral) {
                    // Expression is to be inlined. Use the original expression.
                    return allExprs[localIndex]
                }
                val inputIndex = indexOf(localIndex, inputExprOrdinals)
                assert(inputIndex >= 0)
                RexLocalRef(
                    inputIndex,
                    local.getType()
                )
            } else {
                // It's a reference to what was a local expression at the
                // previous level, and was then projected.
                val exprIndex = exprInverseOrdinals[localIndex]
                RexLocalRef(
                    exprIndex,
                    local.getType()
                )
            }
        }
    }

    /**
     * Finds the highest level used by any of the inputs of a given expression.
     */
    private class MaxInputFinder internal constructor(private val exprLevels: IntArray) : RexVisitorImpl<Void?>(true) {
        var level = 0
        @Override
        fun visitLocalRef(localRef: RexLocalRef): Void? {
            val inputLevel = exprLevels[localRef.getIndex()]
            level = Math.max(level, inputLevel)
            return null
        }

        /**
         * Returns the highest level of any of the inputs of an expression.
         */
        fun maxInputFor(expr: RexNode): Int {
            level = 0
            expr.accept(this)
            return level
        }
    }

    /**
     * Builds an array of the highest level which contains an expression which
     * uses each expression as an input.
     */
    private class HighestUsageFinder internal constructor(exprs: Array<RexNode>, exprLevels: IntArray) :
        RexVisitorImpl<Void?>(true) {
        val maxUsingLevelOrdinals: IntArray
        private var currentLevel = 0

        init {
            maxUsingLevelOrdinals = IntArray(exprs.size)
            Arrays.fill(maxUsingLevelOrdinals, -1)
            for (i in exprs.indices) {
                if (exprs[i] is RexLiteral) {
                    // Literals are always used directly. It never makes sense
                    // to compute them at a lower level and project them to
                    // where they are used.
                    maxUsingLevelOrdinals[i] = -1
                    continue
                }
                currentLevel = exprLevels[i]
                @SuppressWarnings("argument.type.incompatible") val unused: Void = exprs[i].accept(this)
            }
        }

        @Override
        fun visitLocalRef(ref: RexLocalRef): Void? {
            val index: Int = ref.getIndex()
            maxUsingLevelOrdinals[index] = Math.max(maxUsingLevelOrdinals[index], currentLevel)
            return null
        }
    }

    companion object {
        //~ Static fields/initializers ---------------------------------------------
        private val RULE_LOGGER: Logger = RelOptPlanner.LOGGER

        /**
         * Computes the order in which to visit expressions, so that we decide the
         * level of an expression only after the levels of lower expressions have
         * been decided.
         *
         *
         * First, we need to ensure that an expression is visited after all of
         * its inputs.
         *
         *
         * Further, if the expression is a member of a cohort, we need to visit
         * it after the inputs of all other expressions in that cohort. With this
         * condition, expressions in the same cohort will very likely end up in the
         * same level.
         *
         *
         * Note that if there are no cohorts, the expressions from the
         * [RexProgram] are already in a suitable order. We perform the
         * topological sort just to ensure that the code path is well-trodden.
         *
         * @param exprs   Expressions
         * @param cohorts List of cohorts, each of which is a set of expr ordinals
         * @return Expression ordinals in topological order
         */
        private fun computeTopologicalOrdering(
            exprs: Array<RexNode>,
            cohorts: List<Set<Integer>>
        ): List<Integer> {
            val graph: DirectedGraph<Integer, DefaultEdge> = DefaultDirectedGraph.create()
            for (i in exprs.indices) {
                graph.addVertex(i)
            }
            for (i in exprs.indices) {
                val expr: RexNode = exprs[i]
                val cohort: Set<Integer>? = findCohort(cohorts, i)
                val targets: Set<Integer>
                targets = cohort ?: Collections.singleton(i)
                expr.accept(
                    object : RexVisitorImpl<Void?>(true) {
                        @Override
                        fun visitLocalRef(localRef: RexLocalRef): Void? {
                            for (target in targets) {
                                graph.addEdge(localRef.getIndex(), target)
                            }
                            return null
                        }
                    })
            }
            val iter: TopologicalOrderIterator<Integer, DefaultEdge> = TopologicalOrderIterator(graph)
            val permutation: List<Integer> = ArrayList()
            while (iter.hasNext()) {
                permutation.add(iter.next())
            }
            return permutation
        }

        /**
         * Finds the cohort that contains the given integer, or returns null.
         *
         * @param cohorts List of cohorts, each a set of integers
         * @param ordinal Integer to search for
         * @return Cohort that contains the integer, or null if not found
         */
        @Nullable
        private fun findCohort(
            cohorts: List<Set<Integer>>,
            ordinal: Int
        ): Set<Integer>? {
            for (cohort in cohorts) {
                if (cohort.contains(ordinal)) {
                    return cohort
                }
            }
            return null
        }

        private fun identityArray(length: Int): IntArray {
            val ints = IntArray(length)
            for (i in ints.indices) {
                ints[i] = i
            }
            return ints
        }

        /**
         * Returns the number of bits set in an array.
         */
        private fun count(booleans: BooleanArray): Int {
            var count = 0
            for (b in booleans) {
                if (b) {
                    ++count
                }
            }
            return count
        }

        /**
         * Returns the index of the first set bit in an array.
         */
        private fun firstSet(booleans: BooleanArray): Int {
            for (i in booleans.indices) {
                if (booleans[i]) {
                    return i
                }
            }
            return -1
        }

        /**
         * Searches for a value in a map, and returns the position where it was
         * found, or -1.
         *
         * @param value Value to search for
         * @param map   Map to search in
         * @return Ordinal of value in map, or -1 if not found
         */
        private fun indexOf(value: Int, map: IntArray): Int {
            for (i in map.indices) {
                if (value == map[i]) {
                    return i
                }
            }
            return -1
        }
    }
}
