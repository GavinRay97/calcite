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
package org.apache.calcite.plan

import org.apache.calcite.rel.RelNode
import com.google.common.collect.ImmutableList
import org.checkerframework.checker.initialization.qual.NotOnlyInitialized
import org.checkerframework.checker.initialization.qual.UnknownInitialization
import org.checkerframework.checker.nullness.qual.MonotonicNonNull
import java.util.List
import java.util.Objects
import java.util.function.Predicate

/**
 * Operand that determines whether a [RelOptRule]
 * can be applied to a particular expression.
 *
 *
 * For example, the rule to pull a filter up from the left side of a join
 * takes operands: `Join(Filter, Any)`.
 *
 *
 * Note that `children` means different things if it is empty or
 * it is `null`: `Join(Filter **()**, Any)` means
 * that, to match the rule, `Filter` must have no operands.
 */
class RelOptRuleOperand @SuppressWarnings(["initialization.fields.uninitialized", "initialization.invalid.field.write.initialized"]) internal constructor(
    clazz: Class<R?>?,
    @Nullable trait: RelTrait?,
    predicate: Predicate<in R?>?,
    childPolicy: RelOptRuleOperandChildPolicy,
    children: ImmutableList<RelOptRuleOperand?>
) {
    //~ Instance fields --------------------------------------------------------
    @Nullable
    private var parent: RelOptRuleOperand? = null

    @NotOnlyInitialized
    private var rule: RelOptRule? = null
    private val predicate: Predicate<RelNode>

    // REVIEW jvs 29-Aug-2004: some of these are Volcano-specific and should be
    // factored out
    var solveOrder: @MonotonicNonNull IntArray?
    var ordinalInParent = 0
    var ordinalInRule = 0

    @Nullable
    val trait: RelTrait?
    private val clazz: Class<out RelNode?>
    private val children: ImmutableList<RelOptRuleOperand>?

    /**
     * Whether child operands can be matched in any order.
     */
    val childPolicy: RelOptRuleOperandChildPolicy
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates an operand.
     *
     *
     * The `childOperands` argument is often populated by calling one
     * of the following methods:
     * [RelOptRule.some],
     * [RelOptRule.none],
     * [RelOptRule.any],
     * [RelOptRule.unordered],
     * See [org.apache.calcite.plan.RelOptRuleOperandChildren] for more
     * details.
     *
     * @param clazz    Class of relational expression to match (must not be null)
     * @param trait    Trait to match, or null to match any trait
     * @param predicate Predicate to apply to relational expression
     * @param children Child operands
     *
     */
    @Deprecated // to be removed before 2.0; see [CALCITE-1166]
    @Deprecated(
        """Use
    {@link RelOptRule#operand(Class, RelOptRuleOperandChildren)} or one of its
    overloaded methods."""
    )
    protected constructor(
        clazz: Class<R?>?,
        trait: RelTrait?,
        predicate: Predicate<in R?>?,
        children: RelOptRuleOperandChildren
    ) : this(clazz, trait, predicate, children.policy, children.operands) {
    }

    /** Private constructor.
     *
     *
     * Do not call from outside package, and do not create a sub-class.
     *
     *
     * The other constructor is deprecated; when it is removed, make fields
     * [.parent], [.ordinalInParent] and [.solveOrder] final,
     * and add constructor parameters for them. See
     * [[CALCITE-1166]
 * Disallow sub-classes of RelOptRuleOperand](https://issues.apache.org/jira/browse/CALCITE-1166).  */
    init {
        assert(clazz != null)
        when (childPolicy) {
            ANY -> {}
            LEAF -> assert(children.size() === 0)
            UNORDERED -> assert(children.size() === 1)
            else -> assert(children.size() > 0)
        }
        this.childPolicy = childPolicy
        this.clazz = Objects.requireNonNull(clazz, "clazz")
        this.trait = trait
        this.predicate = Objects.requireNonNull(predicate as Predicate?)
        this.children = children
        for (child in this.children) {
            assert(child.parent == null) { "cannot re-use operands" }
            child.parent = this
        }
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Returns the parent operand.
     *
     * @return parent operand
     */
    @Nullable
    fun getParent(): RelOptRuleOperand? {
        return parent
    }

    /**
     * Sets the parent operand.
     *
     * @param parent Parent operand
     */
    fun setParent(@Nullable parent: RelOptRuleOperand?) {
        this.parent = parent
    }

    /**
     * Returns the rule this operand belongs to.
     *
     * @return containing rule
     */
    fun getRule(): RelOptRule? {
        return rule
    }

    /**
     * Sets the rule this operand belongs to.
     *
     * @param rule containing rule
     */
    @SuppressWarnings("initialization.invalid.field.write.initialized")
    fun setRule(@UnknownInitialization rule: RelOptRule?) {
        this.rule = rule
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(clazz, trait, children)
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        if (this === obj) {
            return true
        }
        if (obj !is RelOptRuleOperand) {
            return false
        }
        val that = obj as RelOptRuleOperand
        return (clazz === that.clazz
                && Objects.equals(trait, that.trait)
                && children.equals(that.children))
    }

    /**
     * **FOR DEBUG ONLY.**
     *
     *
     * To facilitate IDE shows the operand description in the debugger,
     * returns the root operand description, but highlight current
     * operand's matched class with '*' in the description.
     *
     *
     * e.g. The following are examples of rule operand description for
     * the operands that match with `LogicalFilter`.
     *
     *
     *  * SemiJoinRule:project: Project(Join(*RelNode*, Aggregate))
     *  * ProjectFilterTransposeRule: LogicalProject(*LogicalFilter*)
     *  * FilterProjectTransposeRule: *Filter*(Project)
     *  * ReduceExpressionsRule(Filter): *LogicalFilter*
     *  * PruneEmptyJoin(right): Join(*RelNode*, Values)
     *
     *
     * @see .describeIt
     */
    @Override
    override fun toString(): String {
        var root: RelOptRuleOperand? = this
        while (root!!.parent != null) {
            root = root.parent
        }
        val s: StringBuilder = root.describeIt(this)
        return s.toString()
    }

    /**
     * Returns this rule operand description, and highlight the operand's
     * class name with '*' if `that` operand equals current operand.
     *
     * @param that The rule operand that needs to be highlighted
     * @return The string builder that describes this rule operand
     * @see .toString
     */
    private fun describeIt(that: RelOptRuleOperand): StringBuilder {
        val s = StringBuilder()
        if (parent == null) {
            s.append(rule).append(": ")
        }
        if (this === that) {
            s.append('*')
        }
        s.append(clazz.getSimpleName())
        if (this === that) {
            s.append('*')
        }
        if (children != null && !children.isEmpty()) {
            s.append('(')
            var first = true
            for (child in children) {
                if (!first) {
                    s.append(", ")
                }
                s.append(child.describeIt(that))
                first = false
            }
            s.append(')')
        }
        return s
    }

    /**
     * Returns relational expression class matched by this operand.
     */
    val matchedClass: Class<out RelNode?>
        get() = clazz

    /**
     * Returns the child operands.
     *
     * @return child operands
     */
    val childOperands: List<RelOptRuleOperand>?
        get() = children

    /**
     * Returns whether a relational expression matches this operand. It must be
     * of the right class and trait.
     */
    fun matches(rel: RelNode): Boolean {
        if (!clazz.isInstance(rel)) {
            return false
        }
        return if (trait != null && !rel.getTraitSet().contains(trait)) {
            false
        } else predicate.test(rel)
    }
}
