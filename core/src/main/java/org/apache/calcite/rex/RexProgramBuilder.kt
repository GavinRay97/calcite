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
package org.apache.calcite.rex

import org.apache.calcite.plan.RelOptPredicateList
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.util.Litmus
import org.apache.calcite.util.Pair
import java.util.ArrayList
import java.util.HashMap
import java.util.List
import java.util.Map
import java.util.Objects.requireNonNull

/**
 * Workspace for constructing a [RexProgram].
 *
 *
 * RexProgramBuilder is necessary because a [RexProgram] is immutable.
 * (The [String] class has the same problem: it is immutable, so they
 * introduced [StringBuilder].)
 */
class RexProgramBuilder @SuppressWarnings("method.invocation.invalid") private constructor(
    inputRowType: RelDataType, rexBuilder: RexBuilder?,
    @Nullable simplify: RexSimplify?
) {
    //~ Instance fields --------------------------------------------------------
    private val rexBuilder: RexBuilder
    private val inputRowType: RelDataType
    private val exprList: List<RexNode> = ArrayList()
    private val exprMap: Map<Pair<RexNode, String>?, RexLocalRef> = HashMap()
    private val localRefList: List<RexLocalRef> = ArrayList()
    private val projectRefList: List<RexLocalRef> = ArrayList()
    private val projectNameList: List<String> = ArrayList()

    @SuppressWarnings("unused")
    @Nullable
    private val simplify: RexSimplify?

    @Nullable
    private var conditionRef: RexLocalRef? = null
    private val validating: Boolean
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a program-builder that will not simplify.
     */
    constructor(inputRowType: RelDataType, rexBuilder: RexBuilder?) : this(inputRowType, rexBuilder, null) {}

    /**
     * Creates a program-builder.
     */
    init {
        this.inputRowType = requireNonNull(inputRowType, "inputRowType")
        this.rexBuilder = requireNonNull(rexBuilder, "rexBuilder")
        this.simplify = simplify // may be null
        validating = assertionsAreEnabled()

        // Pre-create an expression for each input field.
        if (inputRowType.isStruct()) {
            val fields: List<RelDataTypeField> = inputRowType.getFieldList()
            for (i in 0 until fields.size()) {
                registerInternal(RexInputRef.of(i, fields), false)
            }
        }
    }

    /**
     * Creates a program builder with the same contents as a program.
     *
     * @param rexBuilder     Rex builder
     * @param inputRowType   Input row type
     * @param exprList       Common expressions
     * @param projectList    Projections
     * @param condition      Condition, or null
     * @param outputRowType  Output row type
     * @param normalize      Whether to normalize
     * @param simplify       Simplifier, or null to not simplify
     */
    @SuppressWarnings("method.invocation.invalid")
    private constructor(
        rexBuilder: RexBuilder,
        inputRowType: RelDataType,
        exprList: List<RexNode>,
        projectList: Iterable<RexNode?>,
        @Nullable condition: RexNode?,
        outputRowType: RelDataType,
        normalize: Boolean,
        @Nullable simplify: RexSimplify?
    ) : this(inputRowType, rexBuilder, simplify) {
        var condition: RexNode? = condition

        // Create a shuttle for registering input expressions.
        val shuttle: RexShuttle = RegisterMidputShuttle(true, exprList)

        // If we are not normalizing, register all internal expressions. If we
        // are normalizing, expressions will be registered if and when they are
        // first used.
        if (!normalize) {
            shuttle.visitEach(exprList)
        }
        val expander: RexShuttle = ExpansionShuttle(exprList)

        // Register project expressions
        // and create a named project item.
        val fieldList: List<RelDataTypeField> = outputRowType.getFieldList()
        for (pair in Pair.zip(projectList, fieldList)) {
            val project: RexNode
            project = if (simplify != null) {
                simplify.simplify(pair.left.accept(expander))
            } else {
                pair.left
            }
            val name: String = pair.right.getName()
            val ref: RexLocalRef = project.accept(shuttle) as RexLocalRef
            addProject(ref.getIndex(), name)
        }

        // Register the condition, if there is one.
        if (condition != null) {
            if (simplify != null) {
                condition = simplify.simplify(
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.IS_TRUE,
                        condition.accept(expander)
                    )
                )
                if (condition.isAlwaysTrue()) {
                    condition = null
                }
            }
            if (condition != null) {
                val ref: RexLocalRef = condition.accept(shuttle) as RexLocalRef
                addCondition(ref)
            }
        }
    }

    private fun validate(expr: RexNode, fieldOrdinal: Int) {
        val validator: RexVisitor<Void> = object : RexVisitorImpl<Void?>(true) {
            @Override
            fun visitInputRef(input: RexInputRef): Void? {
                val index: Int = input.getIndex()
                val fields: List<RelDataTypeField> = inputRowType.getFieldList()
                if (index < fields.size()) {
                    val inputField: RelDataTypeField = fields[index]
                    if (input.getType() !== inputField.getType()) {
                        throw AssertionError(
                            "in expression " + expr
                                    + ", field reference " + input + " has inconsistent type"
                        )
                    }
                } else {
                    if (index >= fieldOrdinal) {
                        throw AssertionError(
                            "in expression " + expr
                                    + ", field reference " + input + " is out of bounds"
                        )
                    }
                    val refExpr: RexNode = exprList[index]
                    if (refExpr.getType() !== input.getType()) {
                        throw AssertionError(
                            "in expression " + expr
                                    + ", field reference " + input + " has inconsistent type"
                        )
                    }
                }
                return null
            }
        }
        expr.accept(validator)
    }

    /**
     * Adds a project expression to the program.
     *
     *
     * The expression specified in terms of the input fields. If not, call
     * [.registerOutput] first.
     *
     * @param expr Expression to add
     * @param name Name of field in output row type; if null, a unique name will
     * be generated when the program is created
     * @return the ref created
     */
    fun addProject(expr: RexNode?, @Nullable name: String?): RexLocalRef {
        val ref: RexLocalRef = registerInput(expr)
        return addProject(ref.getIndex(), name)
    }

    /**
     * Adds a projection based upon the `index`th expression.
     *
     * @param ordinal Index of expression to project
     * @param name    Name of field in output row type; if null, a unique name
     * will be generated when the program is created
     * @return the ref created
     */
    fun addProject(ordinal: Int, @Nullable name: String?): RexLocalRef {
        val ref: RexLocalRef = localRefList[ordinal]
        projectRefList.add(ref)
        projectNameList.add(name)
        return ref
    }

    /**
     * Adds a project expression to the program at a given position.
     *
     *
     * The expression specified in terms of the input fields. If not, call
     * [.registerOutput] first.
     *
     * @param at   Position in project list to add expression
     * @param expr Expression to add
     * @param name Name of field in output row type; if null, a unique name will
     * be generated when the program is created
     * @return the ref created
     */
    fun addProject(at: Int, expr: RexNode?, name: String?): RexLocalRef {
        val ref: RexLocalRef = registerInput(expr)
        projectRefList.add(at, ref)
        projectNameList.add(at, name)
        return ref
    }

    /**
     * Adds a projection based upon the `index`th expression at a
     * given position.
     *
     * @param at      Position in project list to add expression
     * @param ordinal Index of expression to project
     * @param name    Name of field in output row type; if null, a unique name
     * will be generated when the program is created
     * @return the ref created
     */
    fun addProject(at: Int, ordinal: Int, name: String?): RexLocalRef {
        return addProject(
            at,
            localRefList[ordinal],
            name
        )
    }

    /**
     * Sets the condition of the program.
     *
     *
     * The expression must be specified in terms of the input fields. If
     * not, call [.registerOutput] first.
     */
    fun addCondition(expr: RexNode?) {
        assert(expr != null)
        var conditionRef: RexLocalRef? = conditionRef
        if (conditionRef == null) {
            conditionRef = registerInput(expr)
            this.conditionRef = conditionRef
        } else {
            // AND the new condition with the existing condition.
            // If the new condition is identical to the existing condition, skip it.
            val ref: RexLocalRef = registerInput(expr)
            if (!ref.equals(conditionRef)) {
                this.conditionRef = registerInput(
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.AND,
                        conditionRef,
                        ref
                    )
                )
            }
        }
    }

    /**
     * Registers an expression in the list of common sub-expressions, and
     * returns a reference to that expression.
     *
     *
     * The expression must be expressed in terms of the *inputs* of
     * this program.
     */
    fun registerInput(expr: RexNode?): RexLocalRef {
        val shuttle: RexShuttle = RegisterInputShuttle(true)
        val ref: RexNode = expr!!.accept(shuttle)
        return ref as RexLocalRef
    }

    /**
     * Converts an expression expressed in terms of the *outputs* of this
     * program into an expression expressed in terms of the *inputs*,
     * registers it in the list of common sub-expressions, and returns a
     * reference to that expression.
     *
     * @param expr Expression to register
     */
    fun registerOutput(expr: RexNode): RexLocalRef {
        val shuttle: RexShuttle = RegisterOutputShuttle(exprList)
        val ref: RexNode = expr.accept(shuttle)
        return ref as RexLocalRef
    }

    /**
     * Registers an expression in the list of common sub-expressions, and
     * returns a reference to that expression.
     *
     *
     * If an equivalent sub-expression already exists, creates another
     * expression only if `force` is true.
     *
     * @param expr  Expression to register
     * @param force Whether to create a new sub-expression if an equivalent
     * sub-expression exists.
     */
    private fun registerInternal(expr: RexNode, force: Boolean): RexLocalRef? {
        var expr: RexNode = expr
        val simplify = RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, RexUtil.EXECUTOR)
        expr = simplify.simplifyPreservingType(expr)
        var ref: RexLocalRef?
        val key: Pair<RexNode, String>?
        if (expr is RexLocalRef) {
            key = null
            ref = expr as RexLocalRef
        } else {
            key = RexUtil.makeKey(expr)
            ref = exprMap[key]
        }
        if (ref == null) {
            if (validating) {
                validate(
                    expr,
                    exprList.size()
                )
            }

            // Add expression to list, and return a new reference to it.
            ref = addExpr(expr)
            exprMap.put(requireNonNull(key, "key"), ref)
        } else {
            if (force) {
                // Add expression to list, but return the previous ref.
                addExpr(expr)
            }
        }
        while (true) {
            val index: Int = ref!!.index
            val expr2: RexNode = exprList[index]
            ref = if (expr2 is RexLocalRef) {
                expr2 as RexLocalRef
            } else {
                return ref
            }
        }
    }

    /**
     * Adds an expression to the list of common expressions, and returns a
     * reference to the expression. **DOES NOT CHECK WHETHER THE EXPRESSION
     * ALREADY EXISTS**.
     *
     * @param expr Expression
     * @return Reference to expression
     */
    fun addExpr(expr: RexNode): RexLocalRef {
        val ref: RexLocalRef
        val index: Int = exprList.size()
        exprList.add(expr)
        ref = RexLocalRef(
            index,
            expr.getType()
        )
        localRefList.add(ref)
        return ref
    }

    /**
     * Converts the state of the program builder to an immutable program,
     * normalizing in the process.
     *
     *
     * It is OK to call this method, modify the program specification (by
     * adding projections, and so forth), and call this method again.
     */
    val program: org.apache.calcite.rex.RexProgram
        get() = getProgram(true)

    /**
     * Converts the state of the program builder to an immutable program.
     *
     *
     * It is OK to call this method, modify the program specification (by
     * adding projections, and so forth), and call this method again.
     *
     * @param normalize Whether to normalize
     */
    fun getProgram(normalize: Boolean): RexProgram {
        assert(projectRefList.size() === projectNameList.size())

        // Make sure all fields have a name.
        generateMissingNames()
        val outputRowType: RelDataType = computeOutputRowType()
        return if (normalize) {
            create(
                rexBuilder,
                inputRowType,
                exprList,
                projectRefList,
                conditionRef,
                outputRowType,
                true
            )
                .getProgram(false)
        } else RexProgram(
            inputRowType,
            exprList,
            projectRefList,
            conditionRef,
            outputRowType
        )
    }

    private fun computeOutputRowType(): RelDataType {
        return RexUtil.createStructType(
            rexBuilder.typeFactory, projectRefList,
            projectNameList, null
        )
    }

    private fun generateMissingNames() {
        var i = -1
        var j = 0
        for (projectName in projectNameList) {
            ++i
            if (projectName == null) {
                while (true) {
                    val candidateName = "$" + j++
                    if (!projectNameList.contains(candidateName)) {
                        projectNameList.set(i, candidateName)
                        break
                    }
                }
            }
        }
    }

    /**
     * Adds a set of expressions, projections and filters, applying a shuttle
     * first.
     *
     * @param exprList       Common expressions
     * @param projectRefList Projections
     * @param conditionRef   Condition, or null
     * @param outputRowType  Output row type
     * @param shuttle        Shuttle to apply to each expression before adding it
     * to the program builder
     * @param updateRefs     Whether to update references that changes as a result
     * of rewrites made by the shuttle
     */
    private fun add(
        exprList: List<RexNode>,
        projectRefList: List<RexLocalRef>,
        @Nullable conditionRef: RexLocalRef,
        outputRowType: RelDataType,
        shuttle: RexShuttle,
        updateRefs: Boolean
    ) {
        var conditionRef: RexLocalRef? = conditionRef
        val outFields: List<RelDataTypeField> = outputRowType.getFieldList()
        val registerInputShuttle: RexShuttle = RegisterInputShuttle(false)

        // For each common expression, first apply the user's shuttle, then
        // register the result.
        // REVIEW jpham 28-Apr-2006: if the user shuttle rewrites an input
        // expression, then input references may change
        val newRefs: List<RexLocalRef> = ArrayList(exprList.size())
        val refShuttle: RexShuttle = UpdateRefShuttle(newRefs)
        var i = 0
        for (expr in exprList) {
            var newExpr: RexNode = expr
            if (updateRefs) {
                newExpr = expr.accept(refShuttle)
            }
            newExpr = newExpr.accept(shuttle)
            newRefs.add(
                i++,
                newExpr.accept(registerInputShuttle) as RexLocalRef
            )
        }
        i = -1
        for (oldRef in projectRefList) {
            ++i
            var ref: RexLocalRef = oldRef
            if (updateRefs) {
                ref = oldRef.accept(refShuttle) as RexLocalRef
            }
            ref = ref.accept(shuttle) as RexLocalRef
            this.projectRefList.add(ref)
            val name: String = outFields[i].getName()
            assert(name != null)
            projectNameList.add(name)
        }
        if (conditionRef != null) {
            if (updateRefs) {
                conditionRef = conditionRef.accept(refShuttle) as RexLocalRef
            }
            conditionRef = conditionRef.accept(shuttle) as RexLocalRef
            addCondition(conditionRef)
        }
    }

    private fun registerProjectsAndCondition(program: RexProgram): List<RexLocalRef> {
        val exprList: List<RexNode> = program.getExprList()
        val projectRefList: List<RexLocalRef> = ArrayList()
        val shuttle: RexShuttle = RegisterOutputShuttle(exprList)

        // For each project, lookup the expr and expand it so it is in terms of
        // bottomCalc's input fields
        for (topProject in program.getProjectList()) {
            val topExpr: RexNode = exprList[topProject.getIndex()]
            val expanded: RexLocalRef = topExpr.accept(shuttle) as RexLocalRef

            // Remember the expr, but don't add to the project list yet.
            projectRefList.add(expanded)
        }

        // Similarly for the condition.
        val topCondition: RexLocalRef = program.getCondition()
        if (topCondition != null) {
            val topExpr: RexNode = exprList[topCondition.getIndex()]
            val expanded: RexLocalRef = topExpr.accept(shuttle) as RexLocalRef
            addCondition(registerInput(expanded))
        }
        return projectRefList
    }

    /**
     * Removes all project items.
     *
     *
     * After calling this method, you may need to re-normalize.
     */
    fun clearProjects() {
        projectRefList.clear()
        projectNameList.clear()
    }

    /**
     * Clears the condition.
     *
     *
     * After calling this method, you may need to re-normalize.
     */
    fun clearCondition() {
        conditionRef = null
    }

    /**
     * Adds a project item for every input field.
     *
     *
     * You cannot call this method if there are other project items.
     */
    fun addIdentity() {
        assert(projectRefList.isEmpty())
        for (field in inputRowType.getFieldList()) {
            addProject(
                RexInputRef(
                    field.getIndex(),
                    field.getType()
                ),
                field.getName()
            )
        }
    }

    /**
     * Creates a reference to a given input field.
     *
     * @param index Ordinal of input field, must be less than the number of
     * fields in the input type
     * @return Reference to input field
     */
    fun makeInputRef(index: Int): RexLocalRef {
        val fields: List<RelDataTypeField> = inputRowType.getFieldList()
        assert(index < fields.size())
        val field: RelDataTypeField = fields[index]
        return RexLocalRef(
            index,
            field.getType()
        )
    }

    /**
     * Returns the row type of the input to the program.
     */
    fun getInputRowType(): RelDataType {
        return inputRowType
    }

    /**
     * Returns the list of project expressions.
     */
    val projectList: List<org.apache.calcite.rex.RexLocalRef>
        get() = projectRefList
    //~ Inner Classes ----------------------------------------------------------
    /** Shuttle that visits a tree of [RexNode] and registers them
     * in a program.  */
    private abstract inner class RegisterShuttle : RexShuttle() {
        @Override
        override fun visitCall(call: RexCall): RexNode? {
            val expr: RexNode = super.visitCall(call)
            return registerInternal(expr, false)
        }

        @Override
        override fun visitOver(over: RexOver): RexNode? {
            val expr: RexNode = super.visitOver(over)
            return registerInternal(expr, false)
        }

        @Override
        override fun visitLiteral(literal: RexLiteral): RexNode? {
            val expr: RexNode = super.visitLiteral(literal)
            return registerInternal(expr, false)
        }

        @Override
        override fun visitFieldAccess(fieldAccess: RexFieldAccess): RexNode? {
            val expr: RexNode = super.visitFieldAccess(fieldAccess)
            return registerInternal(expr, false)
        }

        @Override
        override fun visitDynamicParam(dynamicParam: RexDynamicParam): RexNode? {
            val expr: RexNode = super.visitDynamicParam(dynamicParam)
            return registerInternal(expr, false)
        }

        @Override
        override fun visitCorrelVariable(variable: RexCorrelVariable): RexNode? {
            val expr: RexNode = super.visitCorrelVariable(variable)
            return registerInternal(expr, false)
        }
    }

    /**
     * Shuttle which walks over an expression, registering each sub-expression.
     * Each [RexInputRef] is assumed to refer to an *input* of the
     * program.
     */
    private inner class RegisterInputShuttle(private val valid: Boolean) : RegisterShuttle() {
        @Override
        override fun visitInputRef(input: RexInputRef): RexNode {
            val index: Int = input.getIndex()
            if (valid) {
                // The expression should already be valid. Check that its
                // index is within bounds.
                if (index < 0 || index >= inputRowType.getFieldCount()) {
                    assert(false) {
                        ("RexInputRef index " + index + " out of range 0.."
                                + (inputRowType.getFieldCount() - 1))
                    }
                }
                assert(
                    input.getType().isStruct()
                            || RelOptUtil.eq(
                        "type1", input.getType(),
                        "type2", inputRowType.getFieldList().get(index).getType(),
                        Litmus.THROW
                    )
                )
            }

            // Return a reference to the N'th expression, which should be
            // equivalent.
            return localRefList[index]
        }

        @Override
        override fun visitLocalRef(local: RexLocalRef): RexNode? {
            var local: RexLocalRef = local
            if (valid) {
                // The expression should already be valid.
                val index: Int = local.getIndex()
                assert(index >= 0) { index }
                assert(index < exprList.size()) { "index=$index, exprList=$exprList" }
                assert(
                    RelOptUtil.eq(
                        "expr type",
                        exprList[index].getType(),
                        "ref type",
                        local.getType(),
                        Litmus.THROW
                    )
                )
            }

            // Resolve the expression to an input.
            while (true) {
                val index: Int = local.getIndex()
                val expr: RexNode = exprList[index]
                if (expr is RexLocalRef) {
                    local = expr as RexLocalRef
                    if (local.index >= index) {
                        throw AssertionError(
                            "expr " + local + " references later expr " + local.index
                        )
                    }
                } else {
                    // Add expression to the list, just so that subsequent
                    // expressions don't get screwed up. This expression is
                    // unused, so will be eliminated soon.
                    return registerInternal(local, false)
                }
            }
        }
    }

    /**
     * Extension to [RegisterInputShuttle] which allows expressions to be
     * in terms of inputs or previous common sub-expressions.
     */
    private inner class RegisterMidputShuttle(
        valid: Boolean,
        localExprList: List<RexNode>
    ) : RegisterInputShuttle(valid) {
        private val localExprList: List<RexNode>

        init {
            this.localExprList = localExprList
        }

        @Override
        override fun visitLocalRef(local: RexLocalRef): RexNode {
            // Convert a local ref into the common-subexpression it references.
            val index: Int = local.getIndex()
            return localExprList[index].accept(this)
        }
    }

    /**
     * Shuttle which walks over an expression, registering each sub-expression.
     * Each [RexInputRef] is assumed to refer to an *output* of the
     * program.
     */
    private inner class RegisterOutputShuttle internal constructor(localExprList: List<RexNode>) : RegisterShuttle() {
        private val localExprList: List<RexNode>

        init {
            this.localExprList = localExprList
        }

        @Override
        override fun visitInputRef(input: RexInputRef): RexNode {
            // This expression refers to the Nth project column. Lookup that
            // column and find out what common sub-expression IT refers to.
            val index: Int = input.getIndex()
            val local: RexLocalRef = projectRefList[index]
            assert(
                RelOptUtil.eq(
                    "type1",
                    local.getType(),
                    "type2",
                    input.getType(),
                    Litmus.THROW
                )
            )
            return local
        }

        @Override
        override fun visitLocalRef(local: RexLocalRef): RexNode {
            // Convert a local ref into the common-subexpression it references.
            val index: Int = local.getIndex()
            return localExprList[index].accept(this)
        }
    }

    /**
     * Shuttle that rewires [RexLocalRef] using a list of updated
     * references.
     */
    private class UpdateRefShuttle(newRefs: List<RexLocalRef>) : RexShuttle() {
        private val newRefs: List<RexLocalRef>

        init {
            this.newRefs = newRefs
        }

        @Override
        override fun visitLocalRef(localRef: RexLocalRef): RexNode {
            return newRefs[localRef.getIndex()]
        }
    }

    companion object {
        //~ Methods ----------------------------------------------------------------
        /**
         * Returns whether assertions are enabled in this class.
         */
        private fun assertionsAreEnabled(): Boolean {
            var assertionsEnabled = false
            assert(true.also { assertionsEnabled = it })
            return assertionsEnabled
        }

        /**
         * Creates a program builder and initializes it from an existing program.
         *
         *
         * Calling [.getProgram] immediately after creation will return a
         * program equivalent (in terms of external behavior) to the existing
         * program.
         *
         *
         * The existing program will not be changed. (It cannot: programs are
         * immutable.)
         *
         * @param program    Existing program
         * @param rexBuilder Rex builder
         * @param normalize  Whether to normalize
         * @return A program builder initialized with an equivalent program
         */
        fun forProgram(
            program: RexProgram,
            rexBuilder: RexBuilder?,
            normalize: Boolean
        ): RexProgramBuilder {
            assert(program.isValid(Litmus.THROW, null))
            val inputRowType: RelDataType = program.getInputRowType()
            val projectRefs: List<RexLocalRef> = program.getProjectList()
            val conditionRef: RexLocalRef = program.getCondition()
            val exprs: List<RexNode> = program.getExprList()
            val outputRowType: RelDataType = program.getOutputRowType()
            return create(
                rexBuilder,
                inputRowType,
                exprs,
                projectRefs,
                conditionRef,
                outputRowType,
                normalize,
                false
            )
        }

        /**
         * Creates a program builder with the same contents as a program.
         *
         *
         * If `normalize`, converts the program to canonical form. In
         * canonical form, in addition to the usual constraints:
         *
         *
         *  * The first N internal expressions are [RexInputRef]s to the N
         * input fields;
         *  * Subsequent internal expressions reference only preceding expressions;
         *  * Arguments to [RexCall]s must be [RexLocalRef]s (that is,
         * expressions must have maximum depth 1)
         *
         *
         *
         * there are additional constraints:
         *
         *
         *  * Expressions appear in the left-deep order they are needed by
         * the projections and (if present) the condition. Thus, expression N+1
         * is the leftmost argument (literal or or call) in the expansion of
         * projection #0.
         *  * There are no duplicate expressions
         *  * There are no unused expressions
         *
         *
         * @param rexBuilder     Rex builder
         * @param inputRowType   Input row type
         * @param exprList       Common expressions
         * @param projectList    Projections
         * @param condition      Condition, or null
         * @param outputRowType  Output row type
         * @param normalize      Whether to normalize
         * @param simplify       Whether to simplify expressions
         * @return A program builder
         */
        fun create(
            rexBuilder: RexBuilder,
            inputRowType: RelDataType,
            exprList: List<RexNode>,
            projectList: List<RexNode?>,
            @Nullable condition: RexNode?,
            outputRowType: RelDataType,
            normalize: Boolean,
            @Nullable simplify: RexSimplify?
        ): RexProgramBuilder {
            return RexProgramBuilder(
                rexBuilder, inputRowType, exprList,
                projectList, condition, outputRowType, normalize, simplify
            )
        }

        @Deprecated // to be removed before 2.0
        fun create(
            rexBuilder: RexBuilder,
            inputRowType: RelDataType,
            exprList: List<RexNode>,
            projectList: List<RexNode?>,
            @Nullable condition: RexNode?,
            outputRowType: RelDataType,
            normalize: Boolean,
            simplify_: Boolean
        ): RexProgramBuilder {
            var simplify: RexSimplify? = null
            if (simplify_) {
                simplify = RexSimplify(
                    rexBuilder, RelOptPredicateList.EMPTY,
                    RexUtil.EXECUTOR
                )
            }
            return RexProgramBuilder(
                rexBuilder, inputRowType, exprList,
                projectList, condition, outputRowType, normalize, simplify
            )
        }

        @Deprecated // to be removed before 2.0
        fun create(
            rexBuilder: RexBuilder,
            inputRowType: RelDataType,
            exprList: List<RexNode>,
            projectList: List<RexNode?>,
            @Nullable condition: RexNode?,
            outputRowType: RelDataType,
            normalize: Boolean
        ): RexProgramBuilder {
            return create(
                rexBuilder, inputRowType, exprList, projectList, condition,
                outputRowType, normalize, null
            )
        }

        /**
         * Creates a program builder with the same contents as a program, applying a
         * shuttle first.
         *
         *
         * TODO: Refactor the above create method in terms of this one.
         *
         * @param rexBuilder     Rex builder
         * @param inputRowType   Input row type
         * @param exprList       Common expressions
         * @param projectRefList Projections
         * @param conditionRef   Condition, or null
         * @param outputRowType  Output row type
         * @param shuttle        Shuttle to apply to each expression before adding it
         * to the program builder
         * @param updateRefs     Whether to update references that changes as a result
         * of rewrites made by the shuttle
         * @return A program builder
         */
        fun create(
            rexBuilder: RexBuilder?,
            inputRowType: RelDataType,
            exprList: List<RexNode>,
            projectRefList: List<RexLocalRef>,
            @Nullable conditionRef: RexLocalRef,
            outputRowType: RelDataType,
            shuttle: RexShuttle,
            updateRefs: Boolean
        ): RexProgramBuilder {
            val progBuilder = RexProgramBuilder(inputRowType, rexBuilder)
            progBuilder.add(
                exprList,
                projectRefList,
                conditionRef,
                outputRowType,
                shuttle,
                updateRefs
            )
            return progBuilder
        }

        @Deprecated // to be removed before 2.0
        fun normalize(
            rexBuilder: RexBuilder?,
            program: RexProgram
        ): RexProgram {
            return program.normalize(rexBuilder, null)
        }

        /**
         * Merges two programs together, and normalizes the result.
         *
         * @param topProgram    Top program. Its expressions are in terms of the
         * outputs of the bottom program.
         * @param bottomProgram Bottom program. Its expressions are in terms of the
         * result fields of the relational expression's input
         * @param rexBuilder    Rex builder
         * @return Merged program
         * @see .mergePrograms
         */
        fun mergePrograms(
            topProgram: RexProgram,
            bottomProgram: RexProgram,
            rexBuilder: RexBuilder?
        ): RexProgram {
            return mergePrograms(topProgram, bottomProgram, rexBuilder, true)
        }

        /**
         * Merges two programs together.
         *
         *
         * All expressions become common sub-expressions. For example, the query
         *
         * <blockquote><pre>SELECT x + 1 AS p, x + y AS q FROM (
         * SELECT a + b AS x, c AS y
         * FROM t
         * WHERE c = 6)}</pre></blockquote>
         *
         *
         * would be represented as the programs
         *
         * <blockquote><pre>
         * Calc:
         * Projects={$2, $3},
         * Condition=null,
         * Exprs={$0, $1, $0 + 1, $0 + $1})
         * Calc(
         * Projects={$3, $2},
         * Condition={$4}
         * Exprs={$0, $1, $2, $0 + $1, $2 = 6}
        </pre></blockquote> *
         *
         *
         * The merged program is
         *
         * <blockquote><pre>
         * Calc(
         * Projects={$4, $5}
         * Condition=$6
         * Exprs={0: $0       // a
         * 1: $1        // b
         * 2: $2        // c
         * 3: ($0 + $1) // x = a + b
         * 4: ($3 + 1)  // p = x + 1
         * 5: ($3 + $2) // q = x + y
         * 6: ($2 = 6)  // c = 6
        </pre></blockquote> *
         *
         *
         * Another example:
         *
         * <blockquote>
         * <pre>SELECT *
         * FROM (
         * SELECT a + b AS x, c AS y
         * FROM t
         * WHERE c = 6)
         * WHERE x = 5</pre>
        </blockquote> *
         *
         *
         * becomes
         *
         * <blockquote>
         * <pre>SELECT a + b AS x, c AS y
         * FROM t
         * WHERE c = 6 AND (a + b) = 5</pre>
        </blockquote> *
         *
         * @param topProgram    Top program. Its expressions are in terms of the
         * outputs of the bottom program.
         * @param bottomProgram Bottom program. Its expressions are in terms of the
         * result fields of the relational expression's input
         * @param rexBuilder    Rex builder
         * @param normalize     Whether to convert program to canonical form
         * @return Merged program
         */
        fun mergePrograms(
            topProgram: RexProgram,
            bottomProgram: RexProgram,
            rexBuilder: RexBuilder?,
            normalize: Boolean
        ): RexProgram {
            // Initialize a program builder with the same expressions, outputs
            // and condition as the bottom program.
            assert(bottomProgram.isValid(Litmus.THROW, null))
            assert(topProgram.isValid(Litmus.THROW, null))
            val progBuilder = forProgram(bottomProgram, rexBuilder, false)

            // Drive from the outputs of the top program. Register each expression
            // used as an output.
            val projectRefList: List<RexLocalRef> = progBuilder.registerProjectsAndCondition(topProgram)

            // Switch to the projects needed by the top program. The original
            // projects of the bottom program are no longer needed.
            progBuilder.clearProjects()
            val outputRowType: RelDataType = topProgram.getOutputRowType()
            for (pair in Pair.zip(projectRefList, outputRowType.getFieldNames(), true)) {
                progBuilder.addProject(pair.left, pair.right)
            }
            val mergedProg: RexProgram = progBuilder.getProgram(normalize)
            assert(mergedProg.isValid(Litmus.THROW, null))
            assert(mergedProg.getOutputRowType() === topProgram.getOutputRowType())
            return mergedProg
        }
    }
}
