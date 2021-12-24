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

import org.apache.calcite.linq4j.Ord
import org.apache.calcite.plan.RelOptPredicateList
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rel.RelCollations
import org.apache.calcite.rel.RelFieldCollation
import org.apache.calcite.rel.RelInput
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.externalize.RelJsonWriter
import org.apache.calcite.rel.externalize.RelWriterImpl
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.type.SqlTypeUtil
import org.apache.calcite.util.Litmus
import org.apache.calcite.util.Pair
import org.apache.calcite.util.Permutation
import org.apache.calcite.util.mapping.MappingType
import org.apache.calcite.util.mapping.Mappings
import com.google.common.collect.ImmutableList
import com.google.common.collect.Ordering
import com.google.errorprone.annotations.CheckReturnValue
import org.checkerframework.checker.initialization.qual.UnknownInitialization
import org.checkerframework.checker.nullness.qual.MonotonicNonNull
import org.checkerframework.dataflow.qual.Pure
import java.io.PrintWriter
import java.io.StringWriter
import java.util.AbstractList
import java.util.ArrayList
import java.util.Arrays
import java.util.Collections
import java.util.HashSet
import java.util.List
import java.util.Set
import org.apache.calcite.linq4j.Nullness.castNonNull
import java.util.Objects.requireNonNull

/**
 * A collection of expressions which read inputs, compute output expressions,
 * and optionally use a condition to filter rows.
 *
 *
 * Programs are immutable. It may help to use a [RexProgramBuilder],
 * which has the same relationship to [RexProgram] as [StringBuilder]
 * has to [String].
 *
 *
 * A program can contain aggregate functions. If it does, the arguments to
 * each aggregate function must be an [RexInputRef].
 *
 * @see RexProgramBuilder
 */
class RexProgram(
    inputRowType: RelDataType?,
    exprs: List<RexNode?>?,
    projects: List<RexLocalRef?>?,
    @Nullable condition: RexLocalRef?,
    outputRowType: RelDataType?
) {
    //~ Instance fields --------------------------------------------------------
    /**
     * First stage of expression evaluation. The expressions in this array can
     * refer to inputs (using input ordinal #0) or previous expressions in the
     * array (using input ordinal #1).
     */
    private val exprs: List<RexNode>?

    /**
     * With [.condition], the second stage of expression evaluation.
     */
    private val projects: List<RexLocalRef>?

    /**
     * The optional condition. If null, the calculator does not filter rows.
     */
    @Nullable
    private val condition: RexLocalRef?
    private val inputRowType: RelDataType?
    private val outputRowType: RelDataType?

    /**
     * Reference counts for each expression, computed on demand.
     */
    private var refCounts: @MonotonicNonNull IntArray?
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a program.
     *
     *
     * The expressions must be valid: they must not contain common expressions,
     * forward references, or non-trivial aggregates.
     *
     * @param inputRowType  Input row type
     * @param exprs         Common expressions
     * @param projects      Projection expressions
     * @param condition     Condition expression. If null, calculator does not
     * filter rows
     * @param outputRowType Description of the row produced by the program
     */
    init {
        this.inputRowType = inputRowType
        this.exprs = ImmutableList.copyOf(exprs)
        this.projects = ImmutableList.copyOf(projects)
        this.condition = condition
        this.outputRowType = outputRowType
        assert(isValid(Litmus.THROW, null))
    }
    //~ Methods ----------------------------------------------------------------
    // REVIEW jvs 16-Oct-2006:  The description below is confusing.  I
    // think it means "none of the entries are null, there may be none,
    // and there is no further reduction into smaller common sub-expressions
    // possible"?
    /**
     * Returns the common sub-expressions of this program.
     *
     *
     * The list is never null but may be empty; each the expression in the
     * list is not null; and no further reduction into smaller common
     * sub-expressions is possible.
     */
    val exprList: List<org.apache.calcite.rex.RexNode>?
        get() = exprs

    /**
     * Returns an array of references to the expressions which this program is
     * to project. Never null, may be empty.
     */
    val projectList: List<org.apache.calcite.rex.RexLocalRef>?
        get() = projects

    /**
     * Returns a list of project expressions and their field names.
     */
    val namedProjects: List<Any>
        get() = object : AbstractList<Pair<RexLocalRef?, String?>?>() {
            @Override
            fun size(): Int {
                return projects!!.size()
            }

            @Override
            operator fun get(index: Int): Pair<RexLocalRef, String> {
                return Pair.of(
                    projects!![index],
                    outputRowType.getFieldList().get(index).getName()
                )
            }
        }

    /**
     * Returns the field reference of this program's filter condition, or null
     * if there is no condition.
     */
    @Pure
    @Nullable
    fun getCondition(): RexLocalRef? {
        return condition
    }

    // description of this calc, chiefly intended for debugging
    @Override
    override fun toString(): String {
        // Intended to produce similar output to explainCalc,
        // but without requiring a RelNode or RelOptPlanWriter.
        val pw = RelWriterImpl(PrintWriter(StringWriter()))
        collectExplainTerms("", pw)
        return pw.simple()
    }

    /**
     * Writes an explanation of the expressions in this program to a plan
     * writer.
     *
     * @param pw Plan writer
     */
    fun explainCalc(pw: RelWriter): RelWriter {
        return if (pw is RelJsonWriter) {
            pw
                .item("exprs", exprs)
                .item("projects", projects)
                .item("condition", condition)
                .item("inputRowType", inputRowType)
                .item("outputRowType", outputRowType)
        } else {
            collectExplainTerms("", pw, pw.getDetailLevel())
        }
    }

    fun collectExplainTerms(
        prefix: String,
        pw: RelWriter
    ): RelWriter {
        return collectExplainTerms(
            prefix,
            pw,
            SqlExplainLevel.EXPPLAN_ATTRIBUTES
        )
    }

    /**
     * Collects the expressions in this program into a list of terms and values.
     *
     * @param prefix Prefix for term names, usually the empty string, but useful
     * if a relational expression contains more than one program
     * @param pw     Plan writer
     */
    fun collectExplainTerms(
        prefix: String,
        pw: RelWriter,
        level: SqlExplainLevel
    ): RelWriter {
        val inFields: List<RelDataTypeField> = inputRowType.getFieldList()
        val outFields: List<RelDataTypeField> = outputRowType.getFieldList()
        assert(outFields.size() === projects.size()) {
            "outFields.length=" + outFields.size()
                .toString() + ", projects.length=" + projects!!.size()
        }
        pw.item(
            prefix + "expr#0"
                    + if (inFields.size() > 1) ".." + (inFields.size() - 1) else "",
            "{inputs}"
        )
        for (i in inFields.size() until exprs!!.size()) {
            pw.item(prefix + "expr#" + i, exprs!![i])
        }

        // If a lot of the fields are simply projections of the underlying
        // expression, try to be a bit less verbose.
        var trivialCount = countTrivial(projects)
        when (trivialCount) {
            0 -> {}
            1 -> trivialCount = 0
            else -> pw.item(prefix + "proj#0.." + (trivialCount - 1), "{exprs}")
        }
        val withFieldNames = level !== SqlExplainLevel.DIGEST_ATTRIBUTES
        // Print the non-trivial fields with their names as they appear in the
        // output row type.
        for (i in trivialCount until projects!!.size()) {
            val fieldName = if (withFieldNames) prefix + outFields[i].getName() else prefix + i
            pw.item(fieldName, projects!![i])
        }
        if (condition != null) {
            pw.item("$prefix\$condition", condition)
        }
        return pw
    }

    /**
     * Returns the number of expressions in this program.
     */
    val exprCount: Int
        get() = (exprs!!.size()
                + projects!!.size()
                + if (condition == null) 0 else 1)

    /**
     * Returns the type of the input row to the program.
     *
     * @return input row type
     */
    fun getInputRowType(): RelDataType? {
        return inputRowType
    }

    /**
     * Returns whether this program contains windowed aggregate functions.
     *
     * @return whether this program contains windowed aggregate functions
     */
    fun containsAggs(): Boolean {
        return RexOver.containsOver(this)
    }

    /**
     * Returns the type of the output row from this program.
     *
     * @return output row type
     */
    fun getOutputRowType(): RelDataType? {
        return outputRowType
    }

    /**
     * Checks that this program is valid.
     *
     *
     * If `fail` is true, executes `assert false`, so
     * will throw an [AssertionError] if assertions are enabled. If `
     * fail` is false, merely returns whether the program is valid.
     *
     * @param litmus What to do if an error is detected
     * @param context Context of enclosing [RelNode], for validity checking,
     * or null if not known
     * @return Whether the program is valid
     */
    fun isValid(
        litmus: Litmus, context: @Nullable RelNode.Context?
    ): Boolean {
        if (inputRowType == null) {
            return litmus.fail(null)
        }
        if (exprs == null) {
            return litmus.fail(null)
        }
        if (projects == null) {
            return litmus.fail(null)
        }
        if (outputRowType == null) {
            return litmus.fail(null)
        }

        // If the input row type is a struct (contains fields) then the leading
        // expressions must be references to those fields. But we don't require
        // this if the input row type is, say, a java class.
        if (inputRowType.isStruct()) {
            if (!RexUtil.containIdentity(exprs, inputRowType, litmus)) {
                return litmus.fail(null)
            }

            // None of the other fields should be inputRefs.
            for (i in inputRowType.getFieldCount() until exprs.size()) {
                val expr: RexNode = exprs[i]
                if (expr is RexInputRef) {
                    return litmus.fail(null)
                }
            }
        }
        // todo: enable
        // CHECKSTYLE: IGNORE 1
        if (false && RexUtil.containNoCommonExprs(exprs, litmus)) {
            return litmus.fail(null)
        }
        if (!RexUtil.containNoForwardRefs(exprs, inputRowType, litmus)) {
            return litmus.fail(null)
        }
        if (!RexUtil.containNoNonTrivialAggs(exprs, litmus)) {
            return litmus.fail(null)
        }
        val checker = Checker(inputRowType, RexUtil.types(exprs), null, litmus)
        if (condition != null) {
            if (!SqlTypeUtil.inBooleanFamily(condition.getType())) {
                return litmus.fail("condition must be boolean")
            }
            condition.accept(checker)
            if (checker.failCount > 0) {
                return litmus.fail(null)
            }
        }
        for (project in projects) {
            project.accept(checker)
            if (checker.failCount > 0) {
                return litmus.fail(null)
            }
        }
        for (expr in exprs) {
            expr.accept(checker)
            if (checker.failCount > 0) {
                return litmus.fail(null)
            }
        }
        return litmus.succeed()
    }

    /**
     * Returns whether an expression always evaluates to null.
     *
     *
     * Like [RexUtil.isNull], null literals are null, and
     * casts of null literals are null. But this method also regards references
     * to null expressions as null.
     *
     * @param expr Expression
     * @return Whether expression always evaluates to null
     */
    fun isNull(expr: RexNode): Boolean {
        return when (expr.getKind()) {
            LITERAL -> (expr as RexLiteral).getValue2() == null
            LOCAL_REF -> {
                val inputRef: RexLocalRef = expr as RexLocalRef
                isNull(exprs!![inputRef.index])
            }
            CAST -> isNull((expr as RexCall).operands.get(0))
            else -> false
        }
    }

    /**
     * Fully expands a RexLocalRef back into a pure RexNode tree containing no
     * RexLocalRefs (reversing the effect of common subexpression elimination).
     * For example, `program.expandLocalRef(program.getCondition())`
     * will return the expansion of a program's condition.
     *
     * @param ref a RexLocalRef from this program
     * @return expanded form
     */
    fun expandLocalRef(ref: RexLocalRef): RexNode {
        return ref.accept(ExpansionShuttle(exprs))
    }

    /** Expands a list of expressions that may contain [RexLocalRef]s.  */
    fun expandList(nodes: List<RexNode?>?): List<RexNode> {
        return ExpansionShuttle(exprs).visitList(nodes)
    }

    /** Splits this program into a list of project expressions and a list of
     * filter expressions.
     *
     *
     * Neither list is null.
     * The filters are evaluated first.  */
    fun split(): Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> {
        val filters: List<RexNode> = ArrayList()
        if (condition != null) {
            RelOptUtil.decomposeConjunction(expandLocalRef(condition), filters)
        }
        val projects: ImmutableList.Builder<RexNode> = ImmutableList.builder()
        for (project in this.projects) {
            projects.add(expandLocalRef(project))
        }
        return Pair.of(projects.build(), ImmutableList.copyOf(filters))
    }

    /**
     * Given a list of collations which hold for the input to this program,
     * returns a list of collations which hold for its output. The result is
     * mutable and sorted.
     */
    fun getCollations(inputCollations: List<RelCollation?>): List<RelCollation> {
        val outputCollations: List<RelCollation> = ArrayList()
        deduceCollations(
            outputCollations,
            inputRowType.getFieldCount(), projects,
            inputCollations
        )
        return outputCollations
    }

    /**
     * Returns whether the fields on the leading edge of the project list are
     * the input fields.
     *
     * @param fail Whether to throw an assert failure if does not project
     * identity
     */
    fun projectsIdentity(fail: Boolean): Boolean {
        val fieldCount: Int = inputRowType.getFieldCount()
        if (projects!!.size() < fieldCount) {
            assert(!fail) {
                ("program '" + toString()
                        + "' does not project identity for input row type '"
                        + inputRowType + "'")
            }
            return false
        }
        for (i in 0 until fieldCount) {
            val project: RexLocalRef = projects[i]
            if (project.index !== i) {
                assert(!fail) {
                    ("program " + toString()
                            + "' does not project identity for input row type '"
                            + inputRowType + "', field #" + i)
                }
                return false
            }
        }
        return true
    }

    /**
     * Returns whether this program projects precisely its input fields. It may
     * or may not apply a condition.
     */
    fun projectsOnlyIdentity(): Boolean {
        if (projects!!.size() !== inputRowType.getFieldCount()) {
            return false
        }
        for (i in 0 until projects!!.size()) {
            val project: RexLocalRef = projects!![i]
            if (project.index !== i) {
                return false
            }
        }
        return true
    }

    /**
     * Returns whether this program returns its input exactly.
     *
     *
     * This is a stronger condition than [.projectsIdentity].
     */
    val isTrivial: Boolean
        get() = getCondition() == null && projectsOnlyIdentity()

    /**
     * Gets reference counts for each expression in the program, where the
     * references are detected from later expressions in the same program, as
     * well as the project list and condition. Expressions with references
     * counts greater than 1 are true common sub-expressions.
     *
     * @return array of reference counts; the ith element in the returned array
     * is the number of references to getExprList()[i]
     */
    val referenceCounts: IntArray
        get() {
            if (refCounts != null) {
                return refCounts
            }
            refCounts = IntArray(exprs!!.size())
            val refCounter = ReferenceCounter(
                refCounts!!
            )
            RexUtil.apply(refCounter, exprs, null)
            if (condition != null) {
                refCounter.visitLocalRef(condition)
            }
            for (project in projects) {
                refCounter.visitLocalRef(project)
            }
            return refCounts
        }

    /**
     * Returns whether an expression is constant.
     */
    fun isConstant(ref: RexNode): Boolean {
        return ref.accept(ConstantFinder())
    }

    @Nullable
    fun gatherExpr(expr: RexNode): RexNode {
        return expr.accept(Marshaller())
    }

    /**
     * Returns the input field that an output field is populated from, or -1 if
     * it is populated from an expression.
     */
    fun getSourceField(outputOrdinal: Int): Int {
        assert(outputOrdinal >= 0 && outputOrdinal < projects!!.size())
        val project: RexLocalRef = projects!![outputOrdinal]
        var index: Int = project.index
        while (true) {
            var expr: RexNode = exprs!![index]
            if (expr is RexCall
                && (expr as RexCall).getOperator()
                === SqlStdOperatorTable.IN_FENNEL
            ) {
                // drill through identity function
                expr = (expr as RexCall).getOperands().get(0)
            }
            index = if (expr is RexLocalRef) {
                (expr as RexLocalRef).index
            } else return if (expr is RexInputRef) {
                (expr as RexInputRef).index
            } else {
                -1
            }
        }
    }

    /**
     * Returns whether this program is a permutation of its inputs.
     */
    val isPermutation: Boolean
        get() {
            if (projects!!.size() !== inputRowType.getFieldList().size()) {
                return false
            }
            for (i in 0 until projects!!.size()) {
                if (getSourceField(i) < 0) {
                    return false
                }
            }
            return true
        }

    /**
     * Returns a permutation, if this program is a permutation, otherwise null.
     */
    @CheckReturnValue
    @Nullable
    fun getPermutation(): Permutation? {
        val permutation = Permutation(projects!!.size())
        if (projects!!.size() !== inputRowType.getFieldList().size()) {
            return null
        }
        for (i in 0 until projects!!.size()) {
            val sourceField = getSourceField(i)
            if (sourceField < 0) {
                return null
            }
            permutation.set(i, sourceField)
        }
        return permutation
    }

    /**
     * Returns the set of correlation variables used (read) by this program.
     *
     * @return set of correlation variable names
     */
    val correlVariableNames: Set<String>
        get() {
            val paramIdSet: Set<String> = HashSet()
            RexUtil.apply(
                object : RexVisitorImpl<Void?>(true) {
                    @Override
                    fun visitCorrelVariable(
                        correlVariable: RexCorrelVariable
                    ): Void? {
                        paramIdSet.add(correlVariable.getName())
                        return null
                    }
                },
                exprs,
                null
            )
            return paramIdSet
        }

    /**
     * Returns whether this program is in canonical form.
     *
     * @param litmus     What to do if an error is detected (program is not in
     * canonical form)
     * @param rexBuilder Rex builder
     * @return whether in canonical form
     */
    fun isNormalized(litmus: Litmus, rexBuilder: RexBuilder?): Boolean {
        val normalizedProgram = normalize(rexBuilder, null)
        val normalized = normalizedProgram.toString()
        val string = toString()
        if (!normalized.equals(string)) {
            val message = """
                Program is not normalized:
                program:    {}
                normalized: {}

                """.trimIndent()
            return litmus.fail(message, string, normalized)
        }
        return litmus.succeed()
    }

    /**
     * Creates a simplified/normalized copy of this program.
     *
     * @param rexBuilder Rex builder
     * @param simplify Simplifier to simplify (in addition to normalizing),
     * or null to not simplify
     * @return Normalized program
     */
    fun normalize(rexBuilder: RexBuilder?, @Nullable simplify: RexSimplify?): RexProgram {
        // Normalize program by creating program builder from the program, then
        // converting to a program. getProgram does not need to normalize
        // because the builder was normalized on creation.
        assert(isValid(Litmus.THROW, null))
        val builder: RexProgramBuilder = RexProgramBuilder.create(
            rexBuilder, inputRowType, exprs, projects,
            condition, outputRowType, true, simplify
        )
        return builder.getProgram(false)
    }

    @Deprecated // to be removed before 2.0
    fun normalize(rexBuilder: RexBuilder?, simplify: Boolean): RexProgram {
        val predicates: RelOptPredicateList = RelOptPredicateList.EMPTY
        return normalize(rexBuilder, if (simplify) RexSimplify(rexBuilder, predicates, RexUtil.EXECUTOR) else null)
    }

    /**
     * Returns a partial mapping of a set of project expressions.
     *
     *
     * The mapping is an inverse function.
     * Every target has a source field, but
     * a source might have 0, 1 or more targets.
     * Project expressions that do not consist of
     * a mapping are ignored.
     *
     * @param inputFieldCount Number of input fields
     * @return Mapping of a set of project expressions, never null
     */
    fun getPartialMapping(inputFieldCount: Int): Mappings.TargetMapping {
        val mapping: Mappings.TargetMapping = Mappings.create(
            MappingType.INVERSE_FUNCTION,
            inputFieldCount, projects!!.size()
        )
        for (exp in Ord.zip(projects)) {
            val rexNode: RexNode = expandLocalRef(exp.e)
            if (rexNode is RexInputRef) {
                mapping.set((rexNode as RexInputRef).getIndex(), exp.i)
            }
        }
        return mapping
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Visitor which walks over a program and checks validity.
     */
    internal class Checker(
        inputRowType: RelDataType?,
        internalExprTypeList: List<RelDataType>, context: @Nullable RelNode.Context?,
        litmus: Litmus?
    ) : RexChecker(inputRowType, context, litmus) {
        private val internalExprTypeList: List<RelDataType>

        /**
         * Creates a Checker.
         *
         * @param inputRowType         Types of the input fields
         * @param internalExprTypeList Types of the internal expressions
         * @param context              Context of the enclosing [RelNode],
         * or null
         * @param litmus               Whether to fail
         */
        init {
            this.internalExprTypeList = internalExprTypeList
        }

        /** Overrides [RexChecker] method, because [RexLocalRef] is
         * is illegal in most rex expressions, but legal in a program.  */
        @Override
        fun visitLocalRef(localRef: RexLocalRef): Boolean {
            val index: Int = localRef.getIndex()
            if (index < 0 || index >= internalExprTypeList.size()) {
                ++failCount
                return litmus.fail(null)
            }
            if (!RelOptUtil.eq(
                    "type1",
                    localRef.getType(),
                    "type2",
                    internalExprTypeList[index], litmus
                )
            ) {
                ++failCount
                return litmus.fail(null)
            }
            return litmus.succeed()
        }
    }

    /**
     * A RexShuttle used in the implementation of
     * [RexProgram.expandLocalRef].
     */
    internal class ExpansionShuttle(exprs: List<RexNode>?) : RexShuttle() {
        private val exprs: List<RexNode>?

        init {
            this.exprs = exprs
        }

        @Override
        override fun visitLocalRef(localRef: RexLocalRef): RexNode {
            val tree: RexNode = exprs!![localRef.getIndex()]
            return tree.accept(this)
        }
    }

    /**
     * Walks over an expression and determines whether it is constant.
     */
    private inner class ConstantFinder : RexUtil.ConstantFinder() {
        @Override
        fun visitLocalRef(localRef: RexLocalRef): Boolean {
            val expr: RexNode = exprs!![localRef.index]
            return expr.accept(this)
        }

        @Override
        fun visitOver(over: RexOver?): Boolean {
            return false
        }

        @Override
        fun visitCorrelVariable(correlVariable: RexCorrelVariable?): Boolean {
            // Correlating variables are constant WITHIN A RESTART, so that's
            // good enough.
            return true
        }
    }

    /**
     * Given an expression in a program, creates a clone of the expression with
     * sub-expressions (represented by [RexLocalRef]s) fully expanded.
     */
    private inner class Marshaller internal constructor() : RexVisitorImpl<RexNode?>(false) {
        @Override
        fun visitInputRef(inputRef: RexInputRef): RexNode {
            return inputRef
        }

        @Override
        @Nullable
        fun visitLocalRef(localRef: RexLocalRef): RexNode {
            val expr: RexNode = exprs!![localRef.index]
            return expr.accept(this)
        }

        @Override
        fun visitLiteral(literal: RexLiteral): RexNode {
            return literal
        }

        @Override
        fun visitCall(call: RexCall): RexNode {
            val newOperands: List<RexNode> = ArrayList()
            for (operand in call.getOperands()) {
                newOperands.add(castNonNull(operand.accept(this)))
            }
            return call.clone(call.getType(), newOperands)
        }

        @Override
        fun visitOver(over: RexOver): RexNode {
            return visitCall(over)
        }

        @Override
        fun visitCorrelVariable(correlVariable: RexCorrelVariable): RexNode {
            return correlVariable
        }

        @Override
        fun visitDynamicParam(dynamicParam: RexDynamicParam): RexNode {
            return dynamicParam
        }

        @Override
        fun visitRangeRef(rangeRef: RexRangeRef): RexNode {
            return rangeRef
        }

        @Override
        fun visitFieldAccess(fieldAccess: RexFieldAccess): RexNode {
            val referenceExpr: RexNode = fieldAccess.getReferenceExpr().accept(this)
            return RexFieldAccess(
                requireNonNull(referenceExpr, "referenceExpr must not be null"),
                fieldAccess.getField()
            )
        }
    }

    /**
     * Visitor which marks which expressions are used.
     */
    private class ReferenceCounter internal constructor(private val refCounts: IntArray) : RexVisitorImpl<Void?>(true) {
        @Override
        fun visitLocalRef(localRef: RexLocalRef): Void? {
            val index: Int = localRef.getIndex()
            refCounts[index]++
            return null
        }
    }

    companion object {
        /**
         * Creates a program which calculates projections and filters rows based
         * upon a condition. Does not attempt to eliminate common sub-expressions.
         *
         * @param projectExprs  Project expressions
         * @param conditionExpr Condition on which to filter rows, or null if rows
         * are not to be filtered
         * @param outputRowType Output row type
         * @param rexBuilder    Builder of rex expressions
         * @return A program
         */
        fun create(
            inputRowType: RelDataType?,
            projectExprs: List<RexNode?>?,
            @Nullable conditionExpr: RexNode?,
            outputRowType: RelDataType,
            rexBuilder: RexBuilder?
        ): RexProgram {
            return create(
                inputRowType, projectExprs, conditionExpr,
                outputRowType.getFieldNames(), rexBuilder
            )
        }

        /**
         * Creates a program which calculates projections and filters rows based
         * upon a condition. Does not attempt to eliminate common sub-expressions.
         *
         * @param projectExprs  Project expressions
         * @param conditionExpr Condition on which to filter rows, or null if rows
         * are not to be filtered
         * @param fieldNames    Names of projected fields
         * @param rexBuilder    Builder of rex expressions
         * @return A program
         */
        fun create(
            inputRowType: RelDataType,
            projectExprs: List<RexNode?>,
            @Nullable conditionExpr: RexNode?,
            @Nullable fieldNames: List<String?>?,
            rexBuilder: RexBuilder?
        ): RexProgram {
            var fieldNames = fieldNames
            if (fieldNames == null) {
                fieldNames = Collections.nCopies(projectExprs.size(), null)
            } else {
                assert(fieldNames.size() === projectExprs.size()) {
                    ("fieldNames=" + fieldNames
                            + ", exprs=" + projectExprs)
                }
            }
            val programBuilder = RexProgramBuilder(inputRowType, rexBuilder)
            for (i in 0 until projectExprs.size()) {
                programBuilder.addProject(projectExprs[i], fieldNames[i])
            }
            if (conditionExpr != null) {
                programBuilder.addCondition(conditionExpr)
            }
            return programBuilder.getProgram()
        }

        /**
         * Create a program from serialized output.
         * In this case, the input is mainly from the output json string of [RelJsonWriter]
         */
        fun create(input: RelInput): RexProgram {
            val exprs: List<RexNode> = requireNonNull(input.getExpressionList("exprs"), "exprs")
            val projectRexNodes: List<RexNode> = requireNonNull(
                input.getExpressionList("projects"),
                "projects"
            )
            val projects: List<RexLocalRef> = ArrayList(projectRexNodes.size())
            for (rexNode in projectRexNodes) {
                projects.add(rexNode as RexLocalRef)
            }
            val inputType: RelDataType = input.getRowType("inputRowType")
            val outputType: RelDataType = input.getRowType("outputRowType")
            return RexProgram(inputType, exprs, projects, input.getExpression("condition"), outputType)
        }

        /**
         * Returns the number of expressions at the front of an array which are
         * simply projections of the same field.
         *
         * @param refs References
         */
        private fun countTrivial(refs: List<RexLocalRef>?): Int {
            for (i in 0 until refs!!.size()) {
                val ref: RexLocalRef = refs!![i]
                if (ref.getIndex() !== i) {
                    return i
                }
            }
            return refs!!.size()
        }

        /**
         * Creates the identity program.
         */
        fun createIdentity(rowType: RelDataType): RexProgram {
            return createIdentity(rowType, rowType)
        }

        /**
         * Creates a program that projects its input fields but with possibly
         * different names for the output fields.
         */
        fun createIdentity(
            rowType: RelDataType,
            outputRowType: RelDataType
        ): RexProgram {
            if (rowType !== outputRowType
                && !Pair.right(rowType.getFieldList()).equals(
                    Pair.right(outputRowType.getFieldList())
                )
            ) {
                throw IllegalArgumentException(
                    "field type mismatch: $rowType vs. $outputRowType"
                )
            }
            val fields: List<RelDataTypeField> = rowType.getFieldList()
            val projectRefs: List<RexLocalRef> = ArrayList()
            val refs: List<RexInputRef> = ArrayList()
            for (i in 0 until fields.size()) {
                val ref: RexInputRef = RexInputRef.of(i, fields)
                refs.add(ref)
                projectRefs.add(RexLocalRef(i, ref.getType()))
            }
            return RexProgram(rowType, refs, projectRefs, null, outputRowType)
        }

        /**
         * Given a list of expressions and a description of which are ordered,
         * populates a list of collations, sorted in natural order.
         */
        fun deduceCollations(
            outputCollations: List<RelCollation?>,
            sourceCount: Int,
            refs: List<RexLocalRef>?,
            inputCollations: List<RelCollation?>
        ) {
            val targets = IntArray(sourceCount)
            Arrays.fill(targets, -1)
            for (i in 0 until refs!!.size()) {
                val ref: RexLocalRef = refs!![i]
                val source: Int = ref.getIndex()
                if (source < sourceCount && targets[source] == -1) {
                    targets[source] = i
                }
            }
            loop@ for (collation in inputCollations) {
                val fieldCollations: List<RelFieldCollation> = ArrayList(0)
                for (fieldCollation in collation.getFieldCollations()) {
                    val source: Int = fieldCollation.getFieldIndex()
                    val target = targets[source]
                    if (target < 0) {
                        continue@loop
                    }
                    fieldCollations.add(fieldCollation.withFieldIndex(target))
                }

                // Success -- all of the source fields of this key are mapped
                // to the output.
                outputCollations.add(RelCollations.of(fieldCollations))
            }
            outputCollations.sort(Ordering.natural())
        }
    }
}
