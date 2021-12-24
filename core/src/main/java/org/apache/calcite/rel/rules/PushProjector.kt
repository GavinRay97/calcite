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

import org.apache.calcite.linq4j.Ord
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.Strong
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Correlate
import org.apache.calcite.rel.core.Join
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.core.SetOp
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexUtil
import org.apache.calcite.rex.RexVisitorImpl
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.SqlUtil
import org.apache.calcite.tools.RelBuilder
import org.apache.calcite.util.BitSets
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.util.Pair
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableSet
import com.google.common.collect.Lists
import java.util.ArrayList
import java.util.BitSet
import java.util.List
import java.util.Set
import java.util.function.Predicate
import java.util.stream.Collectors
import java.util.stream.IntStream
import java.util.Objects.requireNonNull

/**
 * PushProjector is a utility class used to perform operations used in push
 * projection rules.
 *
 *
 * Pushing is particularly interesting in the case of join, because there
 * are multiple inputs. Generally an expression can be pushed down to a
 * particular input if it depends upon no other inputs. If it can be pushed
 * down to both sides, it is pushed down to the left.
 *
 *
 * Sometimes an expression needs to be split before it can be pushed down.
 * To flag that an expression cannot be split, specify a rule that it must be
 * <dfn>preserved</dfn>. Such an expression will be pushed down intact to one
 * of the inputs, or not pushed down at all.
 */
class PushProjector(
    @Nullable origProj: Project?,
    @Nullable origFilter: RexNode?,
    childRel: RelNode,
    preserveExprCondition: ExprCondition,
    relBuilder: RelBuilder?
) {
    //~ Instance fields --------------------------------------------------------
    @Nullable
    private val origProj: Project?

    @Nullable
    private val origFilter: RexNode?
    private val childRel: RelNode
    private val preserveExprCondition: ExprCondition
    private val relBuilder: RelBuilder

    /**
     * Original projection expressions.
     */
    var origProjExprs: List<RexNode>? = null

    /**
     * Fields from the RelNode that the projection is being pushed past.
     */
    var childFields: List<RelDataTypeField>? = null

    /**
     * Number of fields in the RelNode that the projection is being pushed past.
     */
    val nChildFields: Int

    /**
     * Bitmap containing the references in the original projection.
     */
    val projRefs: BitSet

    /**
     * Bitmap containing the fields in the RelNode that the projection is being
     * pushed past, if the RelNode is not a join. If the RelNode is a join, then
     * the fields correspond to the left hand side of the join.
     */
    var childBitmap: ImmutableBitSet? = null

    /**
     * Bitmap containing the fields in the right hand side of a join, in the
     * case where the projection is being pushed past a join. Not used
     * otherwise.
     */
    @Nullable
    var rightBitmap: ImmutableBitSet? = null

    /**
     * Bitmap containing the fields that should be strong, i.e. when preserving expressions
     * we can only preserve them if the expressions if it is null when these fields are null.
     */
    @Nullable
    var strongBitmap: ImmutableBitSet? = null

    /**
     * Number of fields in the RelNode that the projection is being pushed past,
     * if the RelNode is not a join. If the RelNode is a join, then this is the
     * number of fields in the left hand side of the join.
     *
     *
     * The identity
     * `nChildFields == nSysFields + nFields + nFieldsRight`
     * holds. `nFields` does not include `nSysFields`.
     * The output of a join looks like this:
     *
     * <blockquote><pre>
     * | nSysFields | nFields | nFieldsRight |
    </pre></blockquote> *
     *
     *
     * The output of a single-input rel looks like this:
     *
     * <blockquote><pre>
     * | nSysFields | nFields |
    </pre></blockquote> *
     */
    var nFields = 0

    /**
     * Number of fields in the right hand side of a join, in the case where the
     * projection is being pushed past a join. Always 0 otherwise.
     */
    var nFieldsRight = 0

    /**
     * Number of system fields. System fields appear at the start of a join,
     * before the first field from the left input.
     */
    private var nSysFields = 0

    /**
     * Expressions referenced in the projection/filter that should be preserved.
     * In the case where the projection is being pushed past a join, then the
     * list only contains the expressions corresponding to the left hand side of
     * the join.
     */
    val childPreserveExprs: List<RexNode>

    /**
     * Expressions referenced in the projection/filter that should be preserved,
     * corresponding to expressions on the right hand side of the join, if the
     * projection is being pushed past a join. Empty list otherwise.
     */
    val rightPreserveExprs: List<RexNode>

    /**
     * Number of system fields being projected.
     */
    var nSystemProject = 0

    /**
     * Number of fields being projected. In the case where the projection is
     * being pushed past a join, the number of fields being projected from the
     * left hand side of the join.
     */
    var nProject = 0

    /**
     * Number of fields being projected from the right hand side of a join, in
     * the case where the projection is being pushed past a join. 0 otherwise.
     */
    var nRightProject = 0

    /**
     * Rex builder used to create new expressions.
     */
    val rexBuilder: RexBuilder
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a PushProjector object for pushing projects past a RelNode.
     *
     * @param origProj              the original projection that is being pushed;
     * may be null if the projection is implied as a
     * result of a projection having been trivially
     * removed
     * @param origFilter            the filter that the projection must also be
     * pushed past, if applicable
     * @param childRel              the RelNode that the projection is being
     * pushed past
     * @param preserveExprCondition condition for whether an expression should
     * be preserved in the projection
     */
    init {
        this.origProj = origProj
        this.origFilter = origFilter
        this.childRel = childRel
        this.preserveExprCondition = preserveExprCondition
        this.relBuilder = requireNonNull(relBuilder, "relBuilder")
        origProjExprs = if (origProj == null) {
            ImmutableList.of()
        } else {
            origProj.getProjects()
        }
        if (childRel is Join) {
            val join: Join = childRel as Join
            childFields = Lists.newArrayList(join.getLeft().getRowType().getFieldList())
            childFields.addAll(join.getRight().getRowType().getFieldList())
        } else {
            childFields = childRel.getRowType().getFieldList()
        }
        nChildFields = childFields.size()
        projRefs = BitSet(nChildFields)
        if (childRel is Join) {
            val joinRel: Join = childRel as Join
            val leftFields: List<RelDataTypeField> = joinRel.getLeft().getRowType().getFieldList()
            val rightFields: List<RelDataTypeField> = joinRel.getRight().getRowType().getFieldList()
            nFields = leftFields.size()
            nFieldsRight = rightFields.size()
            nSysFields = joinRel.getSystemFieldList().size()
            childBitmap = ImmutableBitSet.range(nSysFields, nFields + nSysFields)
            rightBitmap = ImmutableBitSet.range(nFields + nSysFields, nChildFields)
            strongBitmap = when (joinRel.getJoinType()) {
                INNER -> ImmutableBitSet.of()
                RIGHT -> ImmutableBitSet.range(nSysFields, nFields + nSysFields)
                LEFT -> ImmutableBitSet.range(nFields + nSysFields, nChildFields)
                FULL -> ImmutableBitSet.range(nSysFields, nChildFields)
                else -> ImmutableBitSet.range(nSysFields, nChildFields)
            }
        } else if (childRel is Correlate) {
            val corrRel: Correlate = childRel as Correlate
            val leftFields: List<RelDataTypeField> = corrRel.getLeft().getRowType().getFieldList()
            val rightFields: List<RelDataTypeField> = corrRel.getRight().getRowType().getFieldList()
            nFields = leftFields.size()
            val joinType: JoinRelType = corrRel.getJoinType()
            when (joinType) {
                SEMI, ANTI -> nFieldsRight = 0
                else -> nFieldsRight = rightFields.size()
            }
            nSysFields = 0
            childBitmap = ImmutableBitSet.range(0, nFields)
            rightBitmap = ImmutableBitSet.range(nFields, nChildFields)

            // Required columns need to be included in project
            projRefs.or(BitSets.of(corrRel.getRequiredColumns()))
            strongBitmap = when (joinType) {
                INNER -> ImmutableBitSet.of()
                ANTI, SEMI -> ImmutableBitSet.range(0, nFields)
                LEFT -> ImmutableBitSet.range(nFields, nChildFields)
                else -> ImmutableBitSet.range(0, nChildFields)
            }
        } else {
            nFields = nChildFields
            nFieldsRight = 0
            childBitmap = ImmutableBitSet.range(nChildFields)
            rightBitmap = null
            nSysFields = 0
            strongBitmap = ImmutableBitSet.of()
        }
        assert(nChildFields == nSysFields + nFields + nFieldsRight)
        childPreserveExprs = ArrayList()
        rightPreserveExprs = ArrayList()
        rexBuilder = childRel.getCluster().getRexBuilder()
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Decomposes a projection to the input references referenced by a
     * projection and a filter, either of which is optional. If both are
     * provided, the filter is underneath the project.
     *
     *
     * Creates a projection containing all input references as well as
     * preserving any special expressions. Converts the original projection
     * and/or filter to reference the new projection. Then, finally puts on top,
     * a final projection corresponding to the original projection.
     *
     * @param defaultExpr expression to be used in the projection if no fields
     * or special columns are selected
     * @return the converted projection if it makes sense to push elements of
     * the projection; otherwise returns null
     */
    @Nullable
    fun convertProject(@Nullable defaultExpr: RexNode?): RelNode? {
        // locate all fields referenced in the projection and filter
        locateAllRefs()

        // if all columns are being selected (either explicitly in the
        // projection) or via a "select *", then there needs to be some
        // special expressions to preserve in the projection; otherwise,
        // there's no point in proceeding any further
        if (origProj == null) {
            if (childPreserveExprs.size() === 0) {
                return null
            }

            // even though there is no projection, this is the same as
            // selecting all fields
            if (nChildFields > 0) {
                // Calling with nChildFields == 0 should be safe but hits
                // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6222207
                projRefs.set(0, nChildFields)
            }
            nProject = nChildFields
        } else if (projRefs.cardinality() === nChildFields
            && childPreserveExprs.size() === 0
        ) {
            return null
        }

        // if nothing is being selected from the underlying rel, just
        // project the default expression passed in as a parameter or the
        // first column if there is no default expression
        if (projRefs.cardinality() === 0 && childPreserveExprs.size() === 0) {
            if (defaultExpr != null) {
                childPreserveExprs.add(defaultExpr)
            } else if (nChildFields == 1) {
                return null
            } else {
                projRefs.set(0)
                nProject = 1
            }
        }

        // create a new projection referencing all fields referenced in
        // either the project or the filter
        val newProject: RelNode = createProjectRefsAndExprs(childRel, false, false)
        val adjustments = adjustments

        // if a filter was passed in, convert it to reference the projected
        // columns, placing it on top of the project just created
        val projChild: RelNode
        projChild = if (origFilter != null) {
            val newFilter: RexNode = convertRefsAndExprs(
                origFilter,
                newProject.getRowType().getFieldList(),
                adjustments
            )
            relBuilder.push(newProject)
            relBuilder.filter(newFilter)
            relBuilder.build()
        } else {
            newProject
        }

        // put the original project on top of the filter/project, converting
        // it to reference the modified projection list; otherwise, create
        // a projection that essentially selects all fields
        return createNewProject(projChild, adjustments)
    }

    /**
     * Locates all references found in either the projection expressions a
     * filter, as well as references to expressions that should be preserved.
     * Based on that, determines whether pushing the projection makes sense.
     *
     * @return true if all inputs from the child that the projection is being
     * pushed past are referenced in the projection/filter and no special
     * preserve expressions are referenced; in that case, it does not make sense
     * to push the projection
     */
    fun locateAllRefs(): Boolean {
        RexUtil.apply(
            InputSpecialOpFinder(
                projRefs,
                childBitmap,
                rightBitmap,
                requireNonNull(strongBitmap, "strongBitmap"),
                preserveExprCondition,
                childPreserveExprs,
                rightPreserveExprs
            ),
            origProjExprs,
            origFilter
        )

        // The system fields of each child are always used by the join, even if
        // they are not projected out of it.
        projRefs.set(
            nSysFields,
            nSysFields + nSysFields,
            true
        )
        projRefs.set(
            nSysFields + nFields,
            nSysFields + nFields + nSysFields,
            true
        )

        // Count how many fields are projected.
        nSystemProject = 0
        nProject = 0
        nRightProject = 0
        for (bit in BitSets.toIter(projRefs)) {
            if (bit < nSysFields) {
                nSystemProject++
            } else if (bit < nSysFields + nFields) {
                nProject++
            } else {
                nRightProject++
            }
        }
        assert(
            nSystemProject + nProject + nRightProject
                    == projRefs.cardinality()
        )
        if (childRel is Join
            || childRel is SetOp
        ) {
            // if nothing is projected from the children, arbitrarily project
            // the first columns; this is necessary since Fennel doesn't
            // handle 0-column projections
            if (nProject == 0 && childPreserveExprs.size() === 0) {
                projRefs.set(0)
                nProject = 1
            }
            if (childRel is Join) {
                if (nRightProject == 0 && rightPreserveExprs.size() === 0) {
                    projRefs.set(nFields)
                    nRightProject = 1
                }
            }
        }

        // no need to push projections if all children fields are being
        // referenced and there are no special preserve expressions; note
        // that we need to do this check after we've handled the 0-column
        // project cases
        val allFieldsReferenced: Boolean = IntStream.range(0, nChildFields).allMatch { i -> projRefs.get(i) }
        return if (allFieldsReferenced
            && childPreserveExprs.size() === 0 && rightPreserveExprs.size() === 0
        ) {
            true
        } else false
    }

    /**
     * Creates a projection based on the inputs specified in a bitmap and the
     * expressions that need to be preserved. The expressions are appended after
     * the input references.
     *
     * @param projChild child that the projection will be created on top of
     * @param adjust    if true, need to create new projection expressions;
     * otherwise, the existing ones are reused
     * @param rightSide if true, creating a projection for the right hand side
     * of a join
     * @return created projection
     */
    fun createProjectRefsAndExprs(
        projChild: RelNode,
        adjust: Boolean,
        rightSide: Boolean
    ): Project {
        val preserveExprs: List<RexNode>
        val nInputRefs: Int
        val offset: Int
        if (rightSide) {
            preserveExprs = rightPreserveExprs
            nInputRefs = nRightProject
            offset = nSysFields + nFields
        } else {
            preserveExprs = childPreserveExprs
            nInputRefs = nProject
            offset = nSysFields
        }
        var refIdx = offset - 1
        val newProjects: List<Pair<RexNode, String>> = ArrayList()
        val destFields: List<RelDataTypeField> = projChild.getRowType().getFieldList()

        // add on the input references
        for (i in 0 until nInputRefs) {
            refIdx = projRefs.nextSetBit(refIdx + 1)
            assert(refIdx >= 0)
            val destField: RelDataTypeField = destFields[refIdx - offset]
            newProjects.add(
                Pair.of(
                    rexBuilder.makeInputRef(
                        destField.getType(), refIdx - offset
                    ),
                    destField.getName()
                )
            )
        }

        // add on the expressions that need to be preserved, converting the
        // arguments to reference the projected columns (if necessary)
        var adjustments = intArrayOf()
        if (preserveExprs.size() > 0 && adjust) {
            adjustments = IntArray(childFields!!.size())
            for (idx in offset until childFields!!.size()) {
                adjustments[idx] = -offset
            }
        }
        var preserveExpOrdinal = 0
        for (projExpr in preserveExprs) {
            var newExpr: RexNode
            newExpr = if (adjust) {
                projExpr.accept(
                    RexInputConverter(
                        rexBuilder,
                        childFields,
                        destFields,
                        adjustments
                    )
                )
            } else {
                projExpr
            }
            val typeList: List<RelDataType> = projChild.getRowType().getFieldList()
                .stream().map { field -> field.getType() }.collect(Collectors.toList())
            val fixer: RexUtil.FixNullabilityShuttle = FixNullabilityShuttle(
                projChild.getCluster().getRexBuilder(), typeList
            )
            newExpr = newExpr.accept(fixer)
            val originalFieldName = findOriginalFieldName(projExpr)
            val newAlias: String
            newAlias = originalFieldName ?: SqlUtil.deriveAliasFromOrdinal(preserveExpOrdinal)
            newProjects.add(Pair.of(newExpr, newAlias))
            preserveExpOrdinal++
        }
        return relBuilder.push(projChild)
            .projectNamed(Pair.left(newProjects), Pair.right(newProjects), true)
            .build() as Project
    }

    @Nullable
    private fun findOriginalFieldName(originRexNode: RexNode): String? {
        if (origProj == null) {
            return null
        }
        val idx: Int = origProj.getProjects().indexOf(originRexNode)
        return if (idx < 0) {
            null
        } else origProj.getRowType().getFieldList().get(idx).getName()
    }

    /**
     * Determines how much each input reference needs to be adjusted as a result
     * of projection.
     *
     * @return array indicating how much each input needs to be adjusted by
     */
    val adjustments: IntArray
        get() {
            val adjustments = IntArray(nChildFields)
            var newIdx = 0
            val rightOffset: Int = childPreserveExprs.size()
            for (pos in BitSets.toIter(projRefs)) {
                adjustments[pos] = -(pos - newIdx)
                if (pos >= nSysFields + nFields) {
                    adjustments[pos] += rightOffset
                }
                newIdx++
            }
            return adjustments
        }

    /**
     * Clones an expression tree and walks through it, adjusting each
     * RexInputRef index by some amount, and converting expressions that need to
     * be preserved to field references.
     *
     * @param rex         the expression
     * @param destFields  fields that the new expressions will be referencing
     * @param adjustments the amount each input reference index needs to be
     * adjusted by
     * @return modified expression tree
     */
    fun convertRefsAndExprs(
        rex: RexNode,
        destFields: List<RelDataTypeField?>?,
        adjustments: IntArray?
    ): RexNode {
        return rex.accept(
            RefAndExprConverter(
                rexBuilder,
                childFields,
                destFields,
                adjustments,
                childPreserveExprs,
                nProject,
                rightPreserveExprs,
                nProject + childPreserveExprs.size() + nRightProject
            )
        )
    }

    /**
     * Creates a new projection based on the original projection, adjusting all
     * input refs using an adjustment array passed in. If there was no original
     * projection, create a new one that selects every field from the underlying
     * rel.
     *
     *
     * If the resulting projection would be trivial, return the child.
     *
     * @param projChild   child of the new project
     * @param adjustments array indicating how much each input reference should
     * be adjusted by
     * @return the created projection
     */
    fun createNewProject(projChild: RelNode, adjustments: IntArray?): RelNode {
        val projects: List<Pair<RexNode, String>> = ArrayList()
        if (origProj != null) {
            for (p in origProj.getNamedProjects()) {
                projects.add(
                    Pair.of(
                        convertRefsAndExprs(
                            p.left,
                            projChild.getRowType().getFieldList(),
                            adjustments
                        ),
                        p.right
                    )
                )
            }
        } else {
            for (field in Ord.zip(childFields)) {
                projects.add(
                    Pair.of(
                        rexBuilder.makeInputRef(
                            field.e.getType(), field.i
                        ), field.e.getName()
                    )
                )
            }
        }
        return relBuilder.push(projChild)
            .project(Pair.left(projects), Pair.right(projects))
            .build()
    }
    //~ Inner Classes ----------------------------------------------------------
    /**
     * Visitor which builds a bitmap of the inputs used by an expressions, as
     * well as locating expressions corresponding to special operators.
     */
    private class InputSpecialOpFinder internal constructor(
        rexRefs: BitSet,
        leftFields: ImmutableBitSet?,
        @Nullable rightFields: ImmutableBitSet?,
        strongFields: ImmutableBitSet,
        preserveExprCondition: ExprCondition,
        preserveLeft: List<RexNode>,
        preserveRight: List<RexNode>?
    ) : RexVisitorImpl<Void?>(true) {
        private val rexRefs: BitSet
        private val leftFields: ImmutableBitSet?

        @Nullable
        private val rightFields: ImmutableBitSet?
        private val strongFields: ImmutableBitSet
        private val preserveExprCondition: ExprCondition
        private val preserveLeft: List<RexNode>
        private val preserveRight: List<RexNode>?
        private val strong: Strong

        init {
            this.rexRefs = rexRefs
            this.leftFields = leftFields
            this.rightFields = rightFields
            this.preserveExprCondition = preserveExprCondition
            this.preserveLeft = preserveLeft
            this.preserveRight = preserveRight
            this.strongFields = strongFields
            strong = Strong.of(strongFields)
        }

        @Override
        fun visitCall(call: RexCall): Void? {
            if (preserve(call)) {
                return null
            }
            super.visitCall(call)
            return null
        }

        private fun isStrong(exprArgs: ImmutableBitSet, call: RexNode): Boolean {
            // If the expressions do not use any of the inputs that require output to be null,
            // no need to check.  Otherwise, check that the expression is null.
            // For example, in an "left outer join", we don't require that expressions
            // pushed down into the left input to be strong.  On the other hand,
            // expressions pushed into the right input must be.  In that case,
            // strongFields == right input fields.
            return !strongFields.intersects(exprArgs) || strong.isNull(call)
        }

        private fun preserve(call: RexNode): Boolean {
            if (preserveExprCondition.test(call)) {
                // if the arguments of the expression only reference the
                // left hand side, preserve it on the left; similarly, if
                // it only references expressions on the right
                val exprArgs: ImmutableBitSet = RelOptUtil.InputFinder.bits(call)
                if (exprArgs.cardinality() > 0) {
                    if (leftFields.contains(exprArgs) && isStrong(exprArgs, call)) {
                        if (!preserveLeft.contains(call)) {
                            preserveLeft.add(call)
                        }
                        return true
                    } else if (requireNonNull(rightFields, "rightFields").contains(exprArgs)
                        && isStrong(exprArgs, call)
                    ) {
                        assert(preserveRight != null)
                        if (!preserveRight!!.contains(call)) {
                            preserveRight.add(call)
                        }
                        return true
                    }
                }
                // if the expression arguments reference both the left and
                // right, fall through and don't attempt to preserve the
                // expression, but instead locate references and special
                // ops in the call operands
            }
            return false
        }

        @Override
        fun visitInputRef(inputRef: RexInputRef): Void? {
            rexRefs.set(inputRef.getIndex())
            return null
        }
    }

    /**
     * Walks an expression tree, replacing input refs with new values to reflect
     * projection and converting special expressions to field references.
     */
    private class RefAndExprConverter internal constructor(
        rexBuilder: RexBuilder?,
        srcFields: List<RelDataTypeField?>?,
        destFields: List<RelDataTypeField?>?,
        adjustments: IntArray?,
        preserveLeft: List<RexNode>,
        firstLeftRef: Int,
        preserveRight: List<RexNode>,
        firstRightRef: Int
    ) : RelOptUtil.RexInputConverter(rexBuilder, srcFields, destFields, adjustments) {
        private val preserveLeft: List<RexNode>
        private val firstLeftRef: Int
        private val preserveRight: List<RexNode>
        private val firstRightRef: Int

        init {
            this.preserveLeft = preserveLeft
            this.firstLeftRef = firstLeftRef
            this.preserveRight = preserveRight
            this.firstRightRef = firstRightRef
        }

        @Override
        fun visitCall(call: RexCall): RexNode {
            // if the expression corresponds to one that needs to be preserved,
            // convert it to a field reference; otherwise, convert the entire
            // expression
            val match = findExprInLists(
                call,
                preserveLeft,
                firstLeftRef,
                preserveRight,
                firstRightRef
            )
            return if (match >= 0) {
                rexBuilder.makeInputRef(
                    requireNonNull(destFields, "destFields").get(match).getType(),
                    match
                )
            } else super.visitCall(call)
        }

        companion object {
            /**
             * Looks for a matching RexNode from among two lists of RexNodes and
             * returns the offset into the list corresponding to the match, adjusted
             * by an amount, depending on whether the match was from the first or
             * second list.
             *
             * @param rex      RexNode that is being matched against
             * @param rexList1 first list of RexNodes
             * @param adjust1  adjustment if match occurred in first list
             * @param rexList2 second list of RexNodes
             * @param adjust2  adjustment if match occurred in the second list
             * @return index in the list corresponding to the matching RexNode; -1
             * if no match
             */
            private fun findExprInLists(
                rex: RexNode,
                rexList1: List<RexNode>,
                adjust1: Int,
                rexList2: List<RexNode>?,
                adjust2: Int
            ): Int {
                var match = rexList1.indexOf(rex)
                if (match >= 0) {
                    return match + adjust1
                }
                if (rexList2 != null) {
                    match = rexList2.indexOf(rex)
                    if (match >= 0) {
                        return match + adjust2
                    }
                }
                return -1
            }
        }
    }

    /**
     * A functor that replies true or false for a given expression.
     *
     * @see org.apache.calcite.rel.rules.PushProjector.OperatorExprCondition
     */
    interface ExprCondition : Predicate<RexNode?> {
        /**
         * Evaluates a condition for a given expression.
         *
         * @param expr Expression
         * @return result of evaluating the condition
         */
        @Override
        fun test(expr: RexNode?): Boolean

        companion object {
            /**
             * Constant condition that replies `false` for all expressions.
             */
            val FALSE: ExprCondition = ExprCondition { expr: RexNode? -> false }

            /**
             * Constant condition that replies `true` for all expressions.
             */
            val TRUE: ExprCondition = ExprCondition { expr: RexNode? -> true }
        }
    }

    /**
     * An expression condition that evaluates to true if the expression is
     * a call to one of a set of operators.
     */
    internal class OperatorExprCondition(operatorSet: Iterable<SqlOperator?>?) : ExprCondition {
        private val operatorSet: Set<SqlOperator>

        /**
         * Creates an OperatorExprCondition.
         *
         * @param operatorSet Set of operators
         */
        init {
            this.operatorSet = ImmutableSet.copyOf(operatorSet)
        }

        @Override
        override fun test(expr: RexNode): Boolean {
            return (expr is RexCall
                    && operatorSet.contains((expr as RexCall).getOperator()))
        }
    }
}
