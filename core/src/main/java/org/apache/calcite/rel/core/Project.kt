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
package org.apache.calcite.rel.core

import org.apache.calcite.linq4j.Ord

/**
 * Relational expression that computes a set of
 * 'select expressions' from its input relational expression.
 *
 * @see org.apache.calcite.rel.logical.LogicalProject
 */
abstract class Project @SuppressWarnings("method.invocation.invalid") protected constructor(
    cluster: RelOptCluster?,
    traits: RelTraitSet?,
    hints: List<RelHint?>?,
    input: RelNode?,
    projects: List<RexNode?>?,
    rowType: RelDataType
) : SingleRel(cluster, traits, input), Hintable {
    //~ Instance fields --------------------------------------------------------
    protected val exps: ImmutableList<RexNode?>
    protected val hints: ImmutableList<RelHint>
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a Project.
     *
     * @param cluster  Cluster that this relational expression belongs to
     * @param traits   Traits of this relational expression
     * @param hints    Hints of this relation expression
     * @param input    Input relational expression
     * @param projects List of expressions for the input columns
     * @param rowType  Output row type
     */
    init {
        assert(rowType != null)
        exps = ImmutableList.copyOf(projects)
        this.hints = ImmutableList.copyOf(hints)
        rowType = rowType
        assert(isValid(Litmus.THROW, null))
    }

    @Deprecated // to be removed before 2.0
    protected constructor(
        cluster: RelOptCluster?, traits: RelTraitSet?,
        input: RelNode?, projects: List<RexNode?>?, rowType: RelDataType?
    ) : this(cluster, traits, ImmutableList.of(), input, projects, rowType) {
    }

    @Deprecated // to be removed before 2.0
    protected constructor(
        cluster: RelOptCluster?, traitSet: RelTraitSet?, input: RelNode?,
        projects: List<RexNode?>?, rowType: RelDataType?, flags: Int
    ) : this(cluster, traitSet, ImmutableList.of(), input, projects, rowType) {
        Util.discard(flags)
    }

    /**
     * Creates a Project by parsing serialized output.
     */
    protected constructor(input: RelInput) : this(
        input.getCluster(),
        input.getTraitSet(),
        ImmutableList.of(),
        input.getInput(),
        requireNonNull(input.getExpressionList("exprs"), "exprs"),
        input.getRowType("exprs", "fields")
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun copy(
        traitSet: RelTraitSet?,
        inputs: List<RelNode?>?
    ): RelNode {
        return copy(traitSet, sole(inputs), exps, getRowType())
    }

    /**
     * Copies a project.
     *
     * @param traitSet Traits
     * @param input Input
     * @param projects Project expressions
     * @param rowType Output row type
     * @return New `Project` if any parameter differs from the value of this
     * `Project`, or just `this` if all the parameters are
     * the same
     *
     * @see .copy
     */
    abstract fun copy(
        traitSet: RelTraitSet?, input: RelNode?,
        projects: List<RexNode?>?, rowType: RelDataType?
    ): Project

    @Deprecated // to be removed before 2.0
    fun copy(
        traitSet: RelTraitSet?, input: RelNode?,
        projects: List<RexNode?>?, rowType: RelDataType?, flags: Int
    ): Project {
        Util.discard(flags)
        return copy(traitSet, input, projects, rowType)
    }

    // to be removed before 2.0
    @get:Deprecated
    val isBoxed: Boolean
        get() = true

    @Override
    fun accept(shuttle: RexShuttle): RelNode {
        val exps: List<RexNode> = shuttle.apply(exps)
        if (this.exps === exps) {
            return this
        }
        val rowType: RelDataType = RexUtil.createStructType(
            getInput().getCluster().getTypeFactory(),
            exps,
            getRowType().getFieldNames(),
            null
        )
        return copy(traitSet, getInput(), exps, rowType)
    }

    /**
     * Returns the project expressions.
     *
     * @return Project expressions
     */
    val projects: List<Any?>
        get() = exps

    /**
     * Returns a list of (expression, name) pairs. Convenient for various
     * transformations.
     *
     * @return List of (expression, name) pairs
     */
    val namedProjects: List<Any>
        get() = Pair.zip(projects, getRowType().getFieldNames())

    @Override
    fun getHints(): ImmutableList<RelHint> {
        return hints
    }

    // to be removed before 2.0
    @get:Deprecated
    val flags: Int
        get() = 1

    /** Returns whether this Project contains any windowed-aggregate functions.  */
    fun containsOver(): Boolean {
        return RexOver.containsOver(projects, null)
    }

    @Override
    fun isValid(litmus: Litmus, @Nullable context: Context?): Boolean {
        if (!super.isValid(litmus, context)) {
            return litmus.fail(null)
        }
        if (!RexUtil.compatibleTypes(exps, getRowType(), litmus)) {
            return litmus.fail("incompatible types")
        }
        val checker = RexChecker(
            getInput().getRowType(), context, litmus
        )
        for (exp in exps) {
            exp.accept(checker)
            if (checker.getFailureCount() > 0) {
                return litmus.fail(
                    "{} failures in expression {}",
                    checker.getFailureCount(), exp
                )
            }
        }
        if (!Util.isDistinct(getRowType().getFieldNames())) {
            return litmus.fail("field names not distinct: {}", rowType)
        }
        //CHECKSTYLE: IGNORE 1
        return if (false && !Util.isDistinct(Util.transform(exps, RexNode::toString))) {
            // Projecting the same expression twice is usually a bad idea,
            // because it may create expressions downstream which are equivalent
            // but which look different. We can't ban duplicate projects,
            // because we need to allow
            //
            //  SELECT a, b FROM c UNION SELECT x, x FROM z
            litmus.fail("duplicate expressions: {}", exps)
        } else litmus.succeed()
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner,
        mq: RelMetadataQuery
    ): RelOptCost {
        val dRows: Double = mq.getRowCount(getInput())
        val dCpu: Double = dRows * exps.size()
        val dIo = 0.0
        return planner.getCostFactory().makeCost(dRows, dCpu, dIo)
    }

    @Override
    fun explainTerms(pw: RelWriter): RelWriter {
        super.explainTerms(pw)
        // Skip writing field names so the optimizer can reuse the projects that differ in
        // field names only
        if (pw.getDetailLevel() === SqlExplainLevel.DIGEST_ATTRIBUTES) {
            val firstNonTrivial = countTrivial(exps)
            if (firstNonTrivial == 1) {
                pw.item("inputs", "0")
            } else if (firstNonTrivial != 0) {
                pw.item("inputs", "0.." + (firstNonTrivial - 1))
            }
            if (firstNonTrivial != exps.size()) {
                pw.item("exprs", exps.subList(firstNonTrivial, exps.size()))
            }
            return pw
        }
        if (pw.nest()) {
            pw.item("fields", getRowType().getFieldNames())
            pw.item("exprs", exps)
        } else {
            for (field in Ord.zip(getRowType().getFieldList())) {
                var fieldName: String = field.e.getName()
                if (fieldName == null) {
                    fieldName = "field#" + field.i
                }
                pw.item(fieldName, exps.get(field.i))
            }
        }
        return pw
    }

    @API(since = "1.24", status = API.Status.INTERNAL)
    @EnsuresNonNullIf(expression = "#1", result = true)
    protected fun deepEquals0(@Nullable obj: Object?): Boolean {
        if (this === obj) {
            return true
        }
        if (obj == null || getClass() !== obj.getClass()) {
            return false
        }
        val o = obj as Project
        return (traitSet.equals(o.traitSet)
                && input.deepEquals(o.input)
                && exps.equals(o.exps)
                && hints.equals(o.hints)
                && getRowType().equalsSansFieldNames(o.getRowType()))
    }

    @API(since = "1.24", status = API.Status.INTERNAL)
    protected fun deepHashCode0(): Int {
        return Objects.hash(traitSet, input.deepHashCode(), exps, hints)
    }

    /**
     * Returns a mapping, or null if this projection is not a mapping.
     *
     * @return Mapping, or null if this projection is not a mapping
     */
    val mapping: @Nullable Mappings.TargetMapping?
        get() = getMapping(getInput().getRowType().getFieldCount(), exps)

    /**
     * Returns a permutation, if this projection is merely a permutation of its
     * input fields; otherwise null.
     *
     * @return Permutation, if this projection is merely a permutation of its
     * input fields; otherwise null
     */
    @get:Nullable
    val permutation: Permutation?
        get() = getPermutation(getInput().getRowType().getFieldCount(), exps)

    /**
     * Checks whether this is a functional mapping.
     * Every output is a source field, but
     * a source field may appear as zero, one, or more output fields.
     */
    fun isMapping(): Boolean {
        for (exp in exps) {
            if (exp !is RexInputRef) {
                return false
            }
        }
        return true
    }
    //~ Inner Classes ----------------------------------------------------------
    /** No longer used.  */
    @Deprecated // to be removed before 2.0
    object Flags {
        const val ANON_FIELDS = 2
        const val BOXED = 1
        const val NONE = 0
    }

    companion object {
        /**
         * Returns the number of expressions at the front of an array which are
         * simply projections of the same field.
         *
         * @param refs References
         * @return the index of the first non-trivial expression, or list.size otherwise
         */
        private fun countTrivial(refs: List<RexNode?>): Int {
            for (i in 0 until refs.size()) {
                val ref: RexNode? = refs[i]
                if (ref !is RexInputRef
                    || (ref as RexInputRef?).getIndex() !== i
                ) {
                    return i
                }
            }
            return refs.size()
        }

        /**
         * Returns a mapping of a set of project expressions.
         *
         *
         * The mapping is an inverse surjection.
         * Every target has a source field, but no
         * source has more than one target.
         * Thus you can safely call
         * [org.apache.calcite.util.mapping.Mappings.TargetMapping.getSourceOpt].
         *
         * @param inputFieldCount Number of input fields
         * @param projects Project expressions
         * @return Mapping of a set of project expressions, or null if projection is
         * not a mapping
         */
        fun getMapping(
            inputFieldCount: Int,
            projects: List<RexNode?>
        ): @Nullable Mappings.TargetMapping? {
            if (inputFieldCount < projects.size()) {
                return null // surjection is not possible
            }
            val mapping: Mappings.TargetMapping = Mappings.create(
                MappingType.INVERSE_SURJECTION,
                inputFieldCount, projects.size()
            )
            for (exp in Ord.< RexNode > zip < RexNode ? > projects) {
                if (exp.e !is RexInputRef) {
                    return null
                }
                val source: Int = (exp.e as RexInputRef).getIndex()
                if (mapping.getTargetOpt(source) !== -1) {
                    return null
                }
                mapping.set(source, exp.i)
            }
            return mapping
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
         * @param projects Project expressions
         * @return Mapping of a set of project expressions, never null
         */
        fun getPartialMapping(
            inputFieldCount: Int,
            projects: List<RexNode?>
        ): Mappings.TargetMapping {
            val mapping: Mappings.TargetMapping = Mappings.create(
                MappingType.INVERSE_FUNCTION,
                inputFieldCount, projects.size()
            )
            for (exp in Ord.< RexNode > zip < RexNode ? > projects) {
                if (exp.e is RexInputRef) {
                    mapping.set((exp.e as RexInputRef).getIndex(), exp.i)
                }
            }
            return mapping
        }

        /**
         * Returns a permutation, if this projection is merely a permutation of its
         * input fields; otherwise null.
         */
        @Nullable
        fun getPermutation(
            inputFieldCount: Int,
            projects: List<RexNode?>
        ): Permutation? {
            val fieldCount: Int = projects.size()
            if (fieldCount != inputFieldCount) {
                return null
            }
            val permutation = Permutation(fieldCount)
            val alreadyProjected: Set<Integer> = HashSet(fieldCount)
            for (i in 0 until fieldCount) {
                val exp: RexNode? = projects[i]
                if (exp is RexInputRef) {
                    val index: Int = (exp as RexInputRef?).getIndex()
                    if (!alreadyProjected.add(index)) {
                        return null
                    }
                    permutation.set(i, index)
                } else {
                    return null
                }
            }
            return permutation
        }
    }
}
