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

import org.apache.calcite.avatica.util.TimeUnit
import org.apache.calcite.avatica.util.TimeUnitRange
import org.apache.calcite.plan.RelOptPredicateList
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.plan.Strong
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.metadata.NullSentinel
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.type.SqlTypeCoercionRule
import org.apache.calcite.sql.type.SqlTypeFamily
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.sql.type.SqlTypeUtil
import org.apache.calcite.util.Bug
import org.apache.calcite.util.Pair
import org.apache.calcite.util.RangeSets
import org.apache.calcite.util.Sarg
import org.apache.calcite.util.Util
import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.BoundType
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableRangeSet
import com.google.common.collect.Iterables
import com.google.common.collect.Multimap
import com.google.common.collect.Range
import com.google.common.collect.RangeSet
import com.google.common.collect.Sets
import com.google.common.collect.TreeRangeSet
import java.math.BigDecimal
import java.util.ArrayList
import java.util.BitSet
import java.util.Collection
import java.util.Collections
import java.util.EnumSet
import java.util.HashMap
import java.util.HashSet
import java.util.LinkedHashSet
import java.util.List
import java.util.Map
import java.util.Set
import org.apache.calcite.linq4j.Nullness.castNonNull
import org.apache.calcite.rex.RexUnknownAs.FALSE
import org.apache.calcite.rex.RexUnknownAs.TRUE
import org.apache.calcite.rex.RexUnknownAs.UNKNOWN
import java.util.Objects.requireNonNull

/**
 * Context required to simplify a row-expression.
 */
class RexSimplify private constructor(
    rexBuilder: RexBuilder, predicates: RelOptPredicateList,
    defaultUnknownAs: RexUnknownAs, predicateElimination: Boolean,
    paranoid: Boolean, executor: RexExecutor
) {
    private val paranoid: Boolean
    val rexBuilder: RexBuilder
    private val predicates: RelOptPredicateList

    /** How to treat UNKNOWN values, if one of the deprecated `simplify` methods without an `unknownAs` argument is called.  */
    val defaultUnknownAs: RexUnknownAs
    val predicateElimination: Boolean
    private val executor: RexExecutor
    private val strong: Strong

    /**
     * Creates a RexSimplify.
     *
     * @param rexBuilder Rex builder
     * @param predicates Predicates known to hold on input fields
     * @param executor Executor for constant reduction, not null
     */
    constructor(
        rexBuilder: RexBuilder, predicates: RelOptPredicateList,
        executor: RexExecutor
    ) : this(rexBuilder, predicates, UNKNOWN, true, false, executor) {
    }

    /** Internal constructor.  */
    init {
        this.rexBuilder = requireNonNull(rexBuilder, "rexBuilder")
        this.predicates = requireNonNull(predicates, "predicates")
        this.defaultUnknownAs = requireNonNull(defaultUnknownAs, "defaultUnknownAs")
        this.predicateElimination = predicateElimination
        this.paranoid = paranoid
        this.executor = requireNonNull(executor, "executor")
        strong = Strong()
    }

    @Deprecated // to be removed before 2.0
    constructor(
        rexBuilder: RexBuilder, unknownAsFalse: Boolean,
        executor: RexExecutor
    ) : this(
        rexBuilder, RelOptPredicateList.EMPTY,
        RexUnknownAs.falseIf(unknownAsFalse), true, false, executor
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        rexBuilder: RexBuilder, predicates: RelOptPredicateList,
        unknownAsFalse: Boolean, executor: RexExecutor
    ) : this(
        rexBuilder, predicates, RexUnknownAs.falseIf(unknownAsFalse), true,
        false, executor
    ) {
    }
    //~ Methods ----------------------------------------------------------------
    /** Returns a RexSimplify the same as this but with a specified
     * [.defaultUnknownAs] value.
     *
     */
    @Deprecated // to be removed before 2.0
    @Deprecated(
        """Use methods with a {@link RexUnknownAs} argument, such as
    {@link #simplify(RexNode, RexUnknownAs)}. """
    )
    fun withUnknownAsFalse(unknownAsFalse: Boolean): RexSimplify {
        val defaultUnknownAs: RexUnknownAs = RexUnknownAs.falseIf(unknownAsFalse)
        return if (defaultUnknownAs === this.defaultUnknownAs) this else RexSimplify(
            rexBuilder, predicates, defaultUnknownAs,
            predicateElimination, paranoid, executor
        )
    }

    /** Returns a RexSimplify the same as this but with a specified
     * [.predicates] value.  */
    fun withPredicates(predicates: RelOptPredicateList): RexSimplify {
        return if (predicates === this.predicates) this else RexSimplify(
            rexBuilder, predicates, defaultUnknownAs,
            predicateElimination, paranoid, executor
        )
    }

    /** Returns a RexSimplify the same as this but which verifies that
     * the expression before and after simplification are equivalent.
     *
     * @see .verify
     */
    fun withParanoid(paranoid: Boolean): RexSimplify {
        return if (paranoid == this.paranoid) this else RexSimplify(
            rexBuilder, predicates, defaultUnknownAs,
            predicateElimination, paranoid, executor
        )
    }

    /** Returns a RexSimplify the same as this but with a specified
     * [.predicateElimination] value.
     *
     *
     * This is introduced temporarily, until
     * [[CALCITE-2401] is fixed][Bug.CALCITE_2401_FIXED].
     */
    private fun withPredicateElimination(predicateElimination: Boolean): RexSimplify {
        return if (predicateElimination == this.predicateElimination) this else RexSimplify(
            rexBuilder, predicates, defaultUnknownAs,
            predicateElimination, paranoid, executor
        )
    }

    /** Simplifies a boolean expression, always preserving its type and its
     * nullability.
     *
     *
     * This is useful if you are simplifying expressions in a
     * [Project].  */
    fun simplifyPreservingType(e: RexNode): RexNode {
        return simplifyPreservingType(e, defaultUnknownAs, true)
    }

    fun simplifyPreservingType(
        e: RexNode, unknownAs: RexUnknownAs,
        matchNullability: Boolean
    ): RexNode {
        val e2: RexNode = simplifyUnknownAs(e, unknownAs)
        if (e2.getType() === e.getType()) {
            return e2
        }
        if (!matchNullability
            && SqlTypeUtil.equalSansNullability(rexBuilder.typeFactory, e2.getType(), e.getType())
        ) {
            return e2
        }
        val e3: RexNode = rexBuilder.makeCast(e.getType(), e2, matchNullability)
        return if (e3.equals(e)) {
            e
        } else e3
    }

    /**
     * Simplifies a boolean expression.
     *
     *
     * In particular:
     *
     *  * `simplify(x = 1 AND y = 2 AND NOT x = 1)`
     * returns `y = 2`
     *  * `simplify(x = 1 AND FALSE)`
     * returns `FALSE`
     *
     *
     *
     * Handles UNKNOWN values using the policy specified when you created this
     * `RexSimplify`. Unless you used a deprecated constructor, that policy
     * is [RexUnknownAs.UNKNOWN].
     *
     *
     * If the expression is a predicate in a WHERE clause, consider instead
     * using [.simplifyUnknownAsFalse].
     *
     * @param e Expression to simplify
     */
    fun simplify(e: RexNode): RexNode {
        return simplifyUnknownAs(e, defaultUnknownAs)
    }

    /** As [.simplify], but for a boolean expression
     * for which a result of UNKNOWN will be treated as FALSE.
     *
     *
     * Use this form for expressions on a WHERE, ON, HAVING or FILTER(WHERE)
     * clause.
     *
     *
     * This may allow certain additional simplifications. A result of UNKNOWN
     * may yield FALSE, however it may still yield UNKNOWN. (If the simplified
     * expression has type BOOLEAN NOT NULL, then of course it can only return
     * FALSE.)  */
    fun simplifyUnknownAsFalse(e: RexNode): RexNode {
        return simplifyUnknownAs(e, FALSE)
    }

    /** As [.simplify], but specifying how UNKNOWN values are to be
     * treated.
     *
     *
     * If UNKNOWN is treated as FALSE, this may allow certain additional
     * simplifications. A result of UNKNOWN may yield FALSE, however it may still
     * yield UNKNOWN. (If the simplified expression has type BOOLEAN NOT NULL,
     * then of course it can only return FALSE.)  */
    fun simplifyUnknownAs(e: RexNode, unknownAs: RexUnknownAs): RexNode {
        val simplified: RexNode = withParanoid(false).simplify(e, unknownAs)
        if (paranoid) {
            verify(e, simplified, unknownAs)
        }
        return simplified
    }

    /** Internal method to simplify an expression.
     *
     *
     * Unlike the public [.simplify]
     * and [.simplifyUnknownAsFalse] methods,
     * never calls [.verify].
     * Verify adds an overhead that is only acceptable for a top-level call.
     */
    fun simplify(e: RexNode, unknownAs: RexUnknownAs): RexNode {
        if (strong.isNull(e)) {
            // Only boolean NULL (aka UNKNOWN) can be converted to FALSE. Even in
            // unknownAs=FALSE mode, we must not convert a NULL integer (say) to FALSE
            if (e.getType().getSqlTypeName() === SqlTypeName.BOOLEAN) {
                when (unknownAs) {
                    FALSE, TRUE -> return rexBuilder.makeLiteral(unknownAs.toBoolean())
                    else -> {}
                }
            }
            return rexBuilder.makeNullLiteral(e.getType())
        }
        return when (e.getKind()) {
            AND -> simplifyAnd(e as RexCall, unknownAs)
            OR -> simplifyOr(e as RexCall, unknownAs)
            NOT -> simplifyNot(e as RexCall, unknownAs)
            CASE -> simplifyCase(e as RexCall, unknownAs)
            COALESCE -> simplifyCoalesce(e as RexCall)
            CAST -> simplifyCast(e as RexCall)
            CEIL, FLOOR -> simplifyCeilFloor(e as RexCall)
            IS_NULL, IS_NOT_NULL, IS_TRUE, IS_NOT_TRUE, IS_FALSE, IS_NOT_FALSE -> {
                assert(e is RexCall)
                simplifyIs(e as RexCall, unknownAs)
            }
            EQUALS, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, NOT_EQUALS -> simplifyComparison(
                e as RexCall,
                unknownAs
            )
            SEARCH -> simplifySearch(e as RexCall, unknownAs)
            LIKE -> simplifyLike(e as RexCall, unknownAs)
            MINUS_PREFIX -> simplifyUnaryMinus(e as RexCall, unknownAs)
            PLUS_PREFIX -> simplifyUnaryPlus(e as RexCall, unknownAs)
            PLUS, MINUS, TIMES, DIVIDE -> simplifyArithmetic(e as RexCall)
            else -> if (e.getClass() === RexCall::class.java) {
                simplifyGenericNode(e as RexCall)
            } else {
                e
            }
        }
    }

    /** Applies NOT to an expression.  */
    fun not(e: RexNode?): RexNode {
        return RexUtil.not(rexBuilder, e)
    }

    /** Applies IS NOT FALSE to an expression.  */
    fun isNotFalse(e: RexNode?): RexNode {
        return if (e!!.isAlwaysTrue()) rexBuilder.makeLiteral(true) else (if (e!!.isAlwaysFalse()) rexBuilder.makeLiteral(
            false
        ) else if (e.getKind() === SqlKind.NOT) isNotTrue((e as RexCall?).operands.get(0)) else if (predicates.isEffectivelyNotNull(
                e
            )
        ) e // would "CAST(e AS BOOLEAN NOT NULL)" better?
        else rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_FALSE, e))
    }

    /** Applies IS NOT TRUE to an expression.  */
    fun isNotTrue(e: RexNode): RexNode {
        return if (e.isAlwaysTrue()) rexBuilder.makeLiteral(false) else if (e.isAlwaysFalse()) rexBuilder.makeLiteral(
            true
        ) else if (e.getKind() === SqlKind.NOT) isNotFalse((e as RexCall).operands.get(0)) else if (predicates.isEffectivelyNotNull(
                e
            )
        ) not(e) else rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_TRUE, e)
    }

    /** Applies IS TRUE to an expression.  */
    fun isTrue(e: RexNode?): RexNode {
        return if (e!!.isAlwaysTrue()) rexBuilder.makeLiteral(true) else (if (e!!.isAlwaysFalse()) rexBuilder.makeLiteral(
            false
        ) else if (e.getKind() === SqlKind.NOT) isFalse((e as RexCall?).operands.get(0)) else if (predicates.isEffectivelyNotNull(
                e
            )
        ) e // would "CAST(e AS BOOLEAN NOT NULL)" better?
        else rexBuilder.makeCall(SqlStdOperatorTable.IS_TRUE, e))
    }

    /** Applies IS FALSE to an expression.  */
    fun isFalse(e: RexNode): RexNode {
        return if (e.isAlwaysTrue()) rexBuilder.makeLiteral(false) else if (e.isAlwaysFalse()) rexBuilder.makeLiteral(
            true
        ) else if (e.getKind() === SqlKind.NOT) isTrue((e as RexCall).operands.get(0)) else if (predicates.isEffectivelyNotNull(
                e
            )
        ) not(e) else rexBuilder.makeCall(SqlStdOperatorTable.IS_FALSE, e)
    }

    /**
     * Runs simplification inside a non-specialized node.
     */
    private fun simplifyGenericNode(e: RexCall): RexNode {
        val operands: List<RexNode> = ArrayList(e.operands)
        simplifyList(operands, UNKNOWN)
        return if (e.operands.equals(operands)) {
            e
        } else rexBuilder.makeCall(e.getType(), e.getOperator(), operands)
    }

    private fun simplifyArithmetic(e: RexCall): RexNode {
        if (e.getType().getSqlTypeName().getFamily() !== SqlTypeFamily.NUMERIC
            || e.getOperands().stream()
                .anyMatch { o -> e.getType().getSqlTypeName().getFamily() !== SqlTypeFamily.NUMERIC }
        ) {
            // we only support simplifying numeric types.
            return simplifyGenericNode(e)
        }
        assert(e.getOperands().size() === 2)
        return when (e.getKind()) {
            PLUS -> simplifyPlus(e)
            MINUS -> simplifyMinus(e)
            TIMES -> simplifyMultiply(e)
            DIVIDE -> simplifyDivide(e)
            else -> throw IllegalArgumentException("Unsupported arithmeitc operation " + e.getKind())
        }
    }

    private fun simplifyPlus(e: RexCall): RexNode {
        val zeroIndex = findLiteralIndex(e.operands, BigDecimal.ZERO)
        if (zeroIndex >= 0) {
            // return the other operand.
            val other: RexNode = e.getOperands().get((zeroIndex + 1) % 2)
            return if (other.getType().equals(e.getType())) other else rexBuilder.makeCast(e.getType(), other)
        }
        return simplifyGenericNode(e)
    }

    private fun simplifyMinus(e: RexCall): RexNode {
        val zeroIndex = findLiteralIndex(e.operands, BigDecimal.ZERO)
        if (zeroIndex == 1) {
            val leftOperand: RexNode = e.getOperands().get(0)
            return if (leftOperand.getType().equals(e.getType())) leftOperand else rexBuilder.makeCast(
                e.getType(),
                leftOperand
            )
        }
        return simplifyGenericNode(e)
    }

    private fun simplifyMultiply(e: RexCall): RexNode {
        val oneIndex = findLiteralIndex(e.operands, BigDecimal.ONE)
        if (oneIndex >= 0) {
            // return the other operand.
            val other: RexNode = e.getOperands().get((oneIndex + 1) % 2)
            return if (other.getType().equals(e.getType())) other else rexBuilder.makeCast(e.getType(), other)
        }
        return simplifyGenericNode(e)
    }

    private fun simplifyDivide(e: RexCall): RexNode {
        val oneIndex = findLiteralIndex(e.operands, BigDecimal.ONE)
        if (oneIndex == 1) {
            val leftOperand: RexNode = e.getOperands().get(0)
            return if (leftOperand.getType().equals(e.getType())) leftOperand else rexBuilder.makeCast(
                e.getType(),
                leftOperand
            )
        }
        return simplifyGenericNode(e)
    }

    private fun simplifyLike(e: RexCall, unknownAs: RexUnknownAs): RexNode {
        if (e.operands.get(1) is RexLiteral) {
            if ("%".equals(e.operands.get(1).getValueAs(String::class.java))) {
                // "x LIKE '%'" simplifies to "x = x"
                val x: RexNode = e.operands.get(0)
                return simplify(
                    rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, x, x),
                    unknownAs
                )
            }
        }
        return simplifyGenericNode(e)
    }

    // e must be a comparison (=, >, >=, <, <=, !=)
    private fun simplifyComparison(e: RexCall, unknownAs: RexUnknownAs): RexNode {
        return simplifyComparison(e, unknownAs, Comparable::class.java)
    }

    // e must be a comparison (=, >, >=, <, <=, !=)
    private fun <C : Comparable<C>?> simplifyComparison(
        e: RexCall,
        unknownAs: RexUnknownAs, clazz: Class<C>
    ): RexNode {
        val operands: List<RexNode> = ArrayList(e.operands)
        // UNKNOWN mode is warranted: false = null
        simplifyList(operands, UNKNOWN)

        // Simplify "x <op> x"
        val o0: RexNode = operands[0]
        val o1: RexNode = operands[1]
        if (o0.equals(o1) && RexUtil.isDeterministic(o0)) {
            val newExpr: RexNode
            when (e.getKind()) {
                EQUALS, GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL -> {
                    // "x = x" simplifies to "null or x is not null" (similarly <= and >=)
                    newExpr = rexBuilder.makeCall(
                        SqlStdOperatorTable.OR,
                        rexBuilder.makeNullLiteral(e.getType()),
                        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, o0)
                    )
                    return simplify(newExpr, unknownAs)
                }
                NOT_EQUALS, LESS_THAN, GREATER_THAN -> {
                    // "x != x" simplifies to "null and x is null" (similarly < and >)
                    newExpr = rexBuilder.makeCall(
                        SqlStdOperatorTable.AND,
                        rexBuilder.makeNullLiteral(e.getType()),
                        rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, o0)
                    )
                    return simplify(newExpr, unknownAs)
                }
                else -> {}
            }
        }
        if (o0.getType().getSqlTypeName() === SqlTypeName.BOOLEAN) {
            val cmp = Comparison.of(
                rexBuilder.makeCall(e.getOperator(), o0, o1),
                java.util.function.Predicate<RexNode> { node -> true })
            if (cmp != null) {
                if (cmp.literal.isAlwaysTrue()) {
                    when (cmp.kind) {
                        GREATER_THAN_OR_EQUAL, EQUALS -> return cmp.ref
                        LESS_THAN, NOT_EQUALS -> return simplify(not(cmp.ref), unknownAs)
                        GREATER_THAN ->             /* this is false, but could be null if x is null */if (!cmp.ref.getType()
                                .isNullable()
                        ) {
                            return rexBuilder.makeLiteral(false)
                        }
                        LESS_THAN_OR_EQUAL ->             /* this is true, but could be null if x is null */if (!cmp.ref.getType()
                                .isNullable()
                        ) {
                            return rexBuilder.makeLiteral(true)
                        }
                        else -> {}
                    }
                }
                if (cmp.literal.isAlwaysFalse()) {
                    when (cmp.kind) {
                        EQUALS, LESS_THAN_OR_EQUAL -> return simplify(not(cmp.ref), unknownAs)
                        NOT_EQUALS, GREATER_THAN -> return cmp.ref
                        GREATER_THAN_OR_EQUAL ->             /* this is true, but could be null if x is null */if (!cmp.ref.getType()
                                .isNullable()
                        ) {
                            return rexBuilder.makeLiteral(true)
                        }
                        LESS_THAN ->             /* this is false, but could be null if x is null */if (!cmp.ref.getType()
                                .isNullable()
                        ) {
                            return rexBuilder.makeLiteral(false)
                        }
                        else -> {}
                    }
                }
            }
        }


        // Simplify "<literal1> <op> <literal2>"
        // For example, "1 = 2" becomes FALSE;
        // "1 != 1" becomes FALSE;
        // "1 != NULL" becomes UNKNOWN (or FALSE if unknownAsFalse);
        // "1 != '1'" is unchanged because the types are not the same.
        if (o0.isA(SqlKind.LITERAL)
            && o1.isA(SqlKind.LITERAL)
            && SqlTypeUtil.equalSansNullability(
                rexBuilder.getTypeFactory(),
                o0.getType(), o1.getType()
            )
        ) {
            val v0: C = (o0 as RexLiteral).getValueAs(clazz)
            val v1: C = (o1 as RexLiteral).getValueAs(clazz)
            if (v0 == null || v1 == null) {
                return if (unknownAs === FALSE) rexBuilder.makeLiteral(false) else rexBuilder.makeNullLiteral(e.getType())
            }
            val comparisonResult = v0.compareTo(v1)
            return when (e.getKind()) {
                EQUALS -> rexBuilder.makeLiteral(comparisonResult == 0)
                GREATER_THAN -> rexBuilder.makeLiteral(comparisonResult > 0)
                GREATER_THAN_OR_EQUAL -> rexBuilder.makeLiteral(comparisonResult >= 0)
                LESS_THAN -> rexBuilder.makeLiteral(comparisonResult < 0)
                LESS_THAN_OR_EQUAL -> rexBuilder.makeLiteral(comparisonResult <= 0)
                NOT_EQUALS -> rexBuilder.makeLiteral(comparisonResult != 0)
                else -> throw AssertionError()
            }
        }

        // If none of the arguments were simplified, return the call unchanged.
        val e2: RexNode
        e2 = if (operands.equals(e.operands)) {
            e
        } else {
            rexBuilder.makeCall(e.op, operands)
        }
        return simplifyUsingPredicates<Comparable<C>>(e2, clazz)
    }

    /**
     * Simplifies a conjunction of boolean expressions.
     */
    @Deprecated // to be removed before 2.0
    fun simplifyAnds(nodes: Iterable<RexNode?>): RexNode {
        ensureParanoidOff()
        return simplifyAnds(nodes, defaultUnknownAs)
    }

    // package-protected only for a deprecated method; treat as private
    fun simplifyAnds(
        nodes: Iterable<RexNode?>,
        unknownAs: RexUnknownAs
    ): RexNode {
        val terms: List<RexNode> = ArrayList()
        val notTerms: List<RexNode> = ArrayList()
        for (e in nodes) {
            RelOptUtil.decomposeConjunction(e, terms, notTerms)
        }
        simplifyList(terms, UNKNOWN)
        simplifyList(notTerms, UNKNOWN)
        return if (unknownAs === FALSE) {
            simplifyAnd2ForUnknownAsFalse(terms, notTerms)
        } else simplifyAnd2(terms, notTerms)
    }

    private fun simplifyList(terms: List<RexNode>, unknownAs: RexUnknownAs) {
        for (i in 0 until terms.size()) {
            terms.set(i, simplify(terms[i], unknownAs))
        }
    }

    private fun simplifyAndTerms(terms: List<RexNode>, unknownAs: RexUnknownAs) {
        var simplify = this
        for (i in 0 until terms.size()) {
            val t: RexNode = terms[i]
            if (Predicate.of(t) == null) {
                continue
            }
            terms.set(i, simplify.simplify(t, unknownAs))
            val newPredicates: RelOptPredicateList = simplify.predicates.union(
                rexBuilder,
                RelOptPredicateList.of(rexBuilder, terms.subList(i, i + 1))
            )
            simplify = simplify.withPredicates(newPredicates)
        }
        for (i in 0 until terms.size()) {
            val t: RexNode = terms[i]
            if (Predicate.of(t) != null) {
                continue
            }
            terms.set(i, simplify.simplify(t, unknownAs))
        }
    }

    private fun simplifyOrTerms(terms: List<RexNode>, unknownAs: RexUnknownAs) {
        // Suppose we are processing "e1(x) OR e2(x) OR e3(x)". When we are
        // visiting "e3(x)" we know both "e1(x)" and "e2(x)" are not true (they
        // may be unknown), because if either of them were true we would have
        // stopped.
        var simplify = this

        // 'doneTerms' prevents us from visiting a term in both first and second
        // loops. If we did this, the second visit would have a predicate saying
        // that 'term' is false. Effectively, we sort terms: visiting
        // 'allowedAsPredicate' terms in the first loop, and
        // non-'allowedAsPredicate' in the second. Each term is visited once.
        val doneTerms = BitSet()
        for (i in 0 until terms.size()) {
            val t: RexNode = terms[i]
            if (!simplify.allowedAsPredicateDuringOrSimplification(t)) {
                continue
            }
            doneTerms.set(i)
            val t2: RexNode = simplify.simplify(t, unknownAs)
            terms.set(i, t2)
            val inverse: RexNode = simplify.simplify(isNotTrue(t2), RexUnknownAs.UNKNOWN)
            val newPredicates: RelOptPredicateList = simplify.predicates.union(
                rexBuilder,
                RelOptPredicateList.of(rexBuilder, ImmutableList.of(inverse))
            )
            simplify = simplify.withPredicates(newPredicates)
        }
        for (i in 0 until terms.size()) {
            val t: RexNode = terms[i]
            if (doneTerms.get(i)) {
                continue  // we visited this term in the first loop
            }
            terms.set(i, simplify.simplify(t, unknownAs))
        }
    }

    /**
     * Decides whether the given node could be used as a predicate during the simplification
     * of other OR operands.
     */
    private fun allowedAsPredicateDuringOrSimplification(t: RexNode): Boolean {
        val predicate = Predicate.of(t)
        return predicate != null && predicate.allowedInOr(predicates)
    }

    private fun simplifyNot(call: RexCall, unknownAs: RexUnknownAs): RexNode {
        val a: RexNode = call.getOperands().get(0)
        val newOperands: List<RexNode>
        when (a.getKind()) {
            NOT ->       // NOT NOT x ==> x
                return simplify((a as RexCall).getOperands().get(0), unknownAs)
            SEARCH -> {
                // NOT SEARCH(x, Sarg[(-inf, 10) OR NULL) ==> SEARCH(x, Sarg[[10, +inf)])
                val call2: RexCall = a as RexCall
                val ref: RexNode = call2.operands.get(0)
                val literal: RexLiteral = call2.operands.get(1)
                val sarg: Sarg = literal.getValueAs(Sarg::class.java)
                return simplifySearch(
                    call2.clone(
                        call2.type,
                        ImmutableList.of(
                            ref,
                            rexBuilder.makeLiteral(
                                requireNonNull(sarg, "sarg").negate(), literal.getType(),
                                literal.getTypeName()
                            )
                        )
                    ),
                    unknownAs.negate()
                )
            }
            LITERAL -> if (a.getType().getSqlTypeName() === SqlTypeName.BOOLEAN
                && !RexLiteral.isNullLiteral(a)
            ) {
                return rexBuilder.makeLiteral(!RexLiteral.booleanValue(a))
            }
            AND -> {
                // NOT distributivity for AND
                newOperands = ArrayList()
                for (operand in (a as RexCall).getOperands()) {
                    newOperands.add(simplify(not(operand), unknownAs))
                }
                return simplify(
                    rexBuilder.makeCall(SqlStdOperatorTable.OR, newOperands), unknownAs
                )
            }
            OR -> {
                // NOT distributivity for OR
                newOperands = ArrayList()
                for (operand in (a as RexCall).getOperands()) {
                    newOperands.add(simplify(not(operand), unknownAs))
                }
                return simplify(
                    rexBuilder.makeCall(SqlStdOperatorTable.AND, newOperands), unknownAs
                )
            }
            CASE -> {
                newOperands = ArrayList()
                val operands: List<RexNode> = (a as RexCall).getOperands()
                var i = 0
                while (i < operands.size()) {
                    if (i + 1 == operands.size()) {
                        newOperands.add(not(operands[i]))
                    } else {
                        newOperands.add(operands[i])
                        newOperands.add(not(operands[i + 1]))
                    }
                    i += 2
                }
                return simplify(
                    rexBuilder.makeCall(SqlStdOperatorTable.CASE, newOperands), unknownAs
                )
            }
            IN, NOT_IN -> {}
            else -> {
                val negateKind: SqlKind = a.getKind().negate()
                if (a.getKind() !== negateKind) {
                    return simplify(
                        rexBuilder.makeCall(
                            RexUtil.op(negateKind),
                            (a as RexCall).getOperands()
                        ), unknownAs
                    )
                }
                val negateKind2: SqlKind = a.getKind().negateNullSafe()
                if (a.getKind() !== negateKind2) {
                    return simplify(
                        rexBuilder.makeCall(
                            RexUtil.op(negateKind2),
                            (a as RexCall).getOperands()
                        ), unknownAs
                    )
                }
            }
        }
        val a2: RexNode = simplify(a, unknownAs.negate())
        return if (a === a2) {
            call
        } else not(a2)
    }

    private fun simplifyUnaryMinus(call: RexCall, unknownAs: RexUnknownAs): RexNode {
        val a: RexNode = call.getOperands().get(0)
        return if (a.getKind() === SqlKind.MINUS_PREFIX) {
            // -(-(x)) ==> x
            simplify((a as RexCall).getOperands().get(0), unknownAs)
        } else simplifyGenericNode(call)
    }

    private fun simplifyUnaryPlus(call: RexCall, unknownAs: RexUnknownAs): RexNode {
        return simplify(call.getOperands().get(0), unknownAs)
    }

    private fun simplifyIs(call: RexCall, unknownAs: RexUnknownAs): RexNode {
        val kind: SqlKind = call.getKind()
        val a: RexNode = call.getOperands().get(0)
        val simplified: RexNode? = simplifyIs1(kind, a, unknownAs)
        return if (simplified == null) call else simplified
    }

    @Nullable
    private fun simplifyIs1(kind: SqlKind, a: RexNode, unknownAs: RexUnknownAs): RexNode? {
        // UnknownAs.FALSE corresponds to x IS TRUE evaluation
        // UnknownAs.TRUE to x IS NOT FALSE
        // Note that both UnknownAs.TRUE and UnknownAs.FALSE only changes the meaning of Unknown
        // (1) if we are already in UnknownAs.FALSE mode; x IS TRUE can be simplified to x
        // (2) similarly in UnknownAs.TRUE mode; x IS NOT FALSE can be simplified to x
        // (3) x IS FALSE could be rewritten to (NOT x) IS TRUE and from there the 1. rule applies
        // (4) x IS NOT TRUE can be rewritten to (NOT x) IS NOT FALSE and from there the 2. rule applies
        if (kind === SqlKind.IS_TRUE && unknownAs === RexUnknownAs.FALSE) {
            return simplify(a, unknownAs)
        }
        if (kind === SqlKind.IS_FALSE && unknownAs === RexUnknownAs.FALSE) {
            return simplify(not(a), unknownAs)
        }
        if (kind === SqlKind.IS_NOT_FALSE && unknownAs === RexUnknownAs.TRUE) {
            return simplify(a, unknownAs)
        }
        if (kind === SqlKind.IS_NOT_TRUE && unknownAs === RexUnknownAs.TRUE) {
            return simplify(not(a), unknownAs)
        }
        val pred: RexNode? = simplifyIsPredicate(kind, a)
        return if (pred != null) {
            pred
        } else simplifyIs2(kind, a, unknownAs)
    }

    @Nullable
    private fun simplifyIsPredicate(kind: SqlKind, a: RexNode): RexNode? {
        if (!RexUtil.isReferenceOrAccess(a, true)) {
            return null
        }
        for (p in predicates.pulledUpPredicates) {
            val pred = IsPredicate.of(p)
            if (pred == null || !a.equals(pred.ref)) {
                continue
            }
            if (kind === pred.kind) {
                return rexBuilder.makeLiteral(true)
            }
        }
        return null
    }

    @Nullable
    private fun simplifyIs2(kind: SqlKind, a: RexNode, unknownAs: RexUnknownAs): RexNode? {
        val simplified: RexNode?
        when (kind) {
            IS_NULL -> {
                // x IS NULL ==> FALSE (if x is not nullable)
                validateStrongPolicy(a)
                simplified = simplifyIsNull(a)
                if (simplified != null) {
                    return simplified
                }
            }
            IS_NOT_NULL -> {
                // x IS NOT NULL ==> TRUE (if x is not nullable)
                validateStrongPolicy(a)
                simplified = simplifyIsNotNull(a)
                if (simplified != null) {
                    return simplified
                }
            }
            IS_TRUE -> {
                // x IS TRUE ==> x (if x is not nullable)
                if (predicates.isEffectivelyNotNull(a)) {
                    return simplify(a, unknownAs)
                }
                simplified = simplify(a, RexUnknownAs.FALSE)
                return if (simplified === a) {
                    null
                } else isTrue(simplified)
            }
            IS_NOT_FALSE -> {
                // x IS NOT FALSE ==> x (if x is not nullable)
                if (predicates.isEffectivelyNotNull(a)) {
                    return simplify(a, unknownAs)
                }
                simplified = simplify(a, RexUnknownAs.TRUE)
                return if (simplified === a) {
                    null
                } else isNotFalse(simplified)
            }
            IS_FALSE, IS_NOT_TRUE ->       // x IS NOT TRUE ==> NOT x (if x is not nullable)
                // x IS FALSE ==> NOT x (if x is not nullable)
                if (predicates.isEffectivelyNotNull(a)) {
                    return simplify(not(a), unknownAs)
                }
            else -> {}
        }
        when (a.getKind()) {
            NOT -> {
                // (NOT x) IS TRUE ==> x IS FALSE
                // Similarly for IS NOT TRUE, IS FALSE, etc.
                //
                // Note that
                //   (NOT x) IS TRUE !=> x IS FALSE
                // because of null values.
                val notKind: SqlOperator = RexUtil.op(kind.negateNullSafe())
                val arg: RexNode = (a as RexCall).operands.get(0)
                return simplify(rexBuilder.makeCall(notKind, arg), UNKNOWN)
            }
            else -> {}
        }
        val a2: RexNode = simplify(a, UNKNOWN)
        return if (a !== a2) {
            rexBuilder.makeCall(RexUtil.op(kind), ImmutableList.of(a2))
        } else null
        // cannot be simplified
    }

    @Nullable
    private fun simplifyIsNotNull(a: RexNode): RexNode? {
        // Simplify the argument first,
        // call ourselves recursively to see whether we can make more progress.
        // For example, given
        // "(CASE WHEN FALSE THEN 1 ELSE 2) IS NOT NULL" we first simplify the
        // argument to "2", and only then we can simplify "2 IS NOT NULL" to "TRUE".
        var a: RexNode = a
        a = simplify(a, UNKNOWN)
        if (!a.getType().isNullable() && isSafeExpression(a)) {
            return rexBuilder.makeLiteral(true)
        }
        if (predicates.pulledUpPredicates.contains(a)) {
            return rexBuilder.makeLiteral(true)
        }
        return if (hasCustomNullabilityRules(a.getKind())) {
            null
        } else when (Strong.policy(a)) {
            NOT_NULL -> rexBuilder.makeLiteral(true)
            ANY -> {
                // "f" is a strong operator, so "f(operand0, operand1) IS NOT NULL"
                // simplifies to "operand0 IS NOT NULL AND operand1 IS NOT NULL"
                val operands: List<RexNode> = ArrayList()
                for (operand in (a as RexCall).getOperands()) {
                    val simplified: RexNode? = simplifyIsNotNull(operand)
                    if (simplified == null) {
                        operands.add(
                            rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, operand)
                        )
                    } else if (simplified.isAlwaysFalse()) {
                        return rexBuilder.makeLiteral(false)
                    } else {
                        operands.add(simplified)
                    }
                }
                RexUtil.composeConjunction(rexBuilder, operands)
            }
            CUSTOM -> when (a.getKind()) {
                LITERAL -> rexBuilder.makeLiteral(!(a as RexLiteral).isNull())
                else -> throw AssertionError(
                    "every CUSTOM policy needs a handler, "
                            + a.getKind()
                )
            }
            AS_IS -> null
            else -> null
        }
    }

    @Nullable
    private fun simplifyIsNull(a: RexNode): RexNode? {
        // Simplify the argument first,
        // call ourselves recursively to see whether we can make more progress.
        // For example, given
        // "(CASE WHEN FALSE THEN 1 ELSE 2) IS NULL" we first simplify the
        // argument to "2", and only then we can simplify "2 IS NULL" to "FALSE".
        var a: RexNode = a
        a = simplify(a, UNKNOWN)
        if (!a.getType().isNullable() && isSafeExpression(a)) {
            return rexBuilder.makeLiteral(false)
        }
        if (RexUtil.isNull(a)) {
            return rexBuilder.makeLiteral(true)
        }
        return if (hasCustomNullabilityRules(a.getKind())) {
            null
        } else when (Strong.policy(a)) {
            NOT_NULL -> rexBuilder.makeLiteral(false)
            ANY -> {
                // "f" is a strong operator, so "f(operand0, operand1) IS NULL" simplifies
                // to "operand0 IS NULL OR operand1 IS NULL"
                val operands: List<RexNode> = ArrayList()
                for (operand in (a as RexCall).getOperands()) {
                    val simplified: RexNode? = simplifyIsNull(operand)
                    if (simplified == null) {
                        operands.add(
                            rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, operand)
                        )
                    } else {
                        operands.add(simplified)
                    }
                }
                RexUtil.composeDisjunction(rexBuilder, operands, false)
            }
            AS_IS -> null
            else -> null
        }
    }

    private fun simplifyCoalesce(call: RexCall): RexNode {
        val operandSet: Set<RexNode> = HashSet()
        val operands: List<RexNode> = ArrayList()
        for (operand in call.getOperands()) {
            operand = simplify(operand, UNKNOWN)
            if (!RexUtil.isNull(operand)
                && operandSet.add(operand)
            ) {
                operands.add(operand)
            }
            if (!operand.getType().isNullable()) {
                break
            }
        }
        return when (operands.size()) {
            0 -> rexBuilder.makeNullLiteral(call.type)
            1 -> operands[0]
            else -> {
                if (operands.equals(call.operands)) {
                    call
                } else call.clone(call.type, operands)
            }
        }
    }

    private fun simplifyCase(call: RexCall, unknownAs: RexUnknownAs): RexNode {
        val inputBranches = CaseBranch.fromCaseOperands(rexBuilder, ArrayList(call.getOperands()))

        // run simplification on all operands
        val condSimplifier = withPredicates(RelOptPredicateList.EMPTY)
        val valueSimplifier = this
        val caseType: RelDataType = call.getType()
        var conditionNeedsSimplify = false
        var lastBranch: CaseBranch? = null
        val branches: List<CaseBranch> = ArrayList()
        for (inputBranch in inputBranches) {
            // simplify the condition
            var newCond: RexNode = condSimplifier.simplify(inputBranch.cond, RexUnknownAs.FALSE)
            if (newCond.isAlwaysFalse()) {
                // If the condition is false, we do not need to add it
                continue
            }

            // simplify the value
            val newValue: RexNode = valueSimplifier.simplify(inputBranch.value, unknownAs)

            // create new branch
            if (lastBranch != null) {
                if (lastBranch.value.equals(newValue)
                    && isSafeExpression(newCond)
                ) {
                    // in this case, last branch and new branch have the same conclusion,
                    // hence we create a new composite condition and we do not add it to
                    // the final branches for the time being
                    newCond = rexBuilder.makeCall(SqlStdOperatorTable.OR, lastBranch.cond, newCond)
                    conditionNeedsSimplify = true
                } else {
                    // if we reach here, the new branch is not mergeable with the last one,
                    // hence we are going to add the last branch to the final branches.
                    // if the last branch was merged, then we will simplify it first.
                    // otherwise, we just add it
                    val branch = generateBranch(conditionNeedsSimplify, condSimplifier, lastBranch)
                    if (!branch.cond.isAlwaysFalse()) {
                        // If the condition is not false, we add it to the final result
                        branches.add(branch)
                        if (branch.cond.isAlwaysTrue()) {
                            // If the condition is always true, we are done
                            lastBranch = null
                            break
                        }
                    }
                    conditionNeedsSimplify = false
                }
            }
            lastBranch = CaseBranch(newCond, newValue)
            if (newCond.isAlwaysTrue()) {
                // If the condition is always true, we are done (useful in first loop iteration)
                break
            }
        }
        if (lastBranch != null) {
            // we need to add the last pending branch once we have finished
            // with the for loop
            val branch = generateBranch(conditionNeedsSimplify, condSimplifier, lastBranch)
            if (!branch.cond.isAlwaysFalse()) {
                branches.add(branch)
            }
        }
        if (branches.size() === 1) {
            // we can just return the value in this case (matching the case type)
            val value: RexNode = branches[0].value
            return if (sameTypeOrNarrowsNullability(caseType, value.getType())) {
                value
            } else {
                rexBuilder.makeAbstractCast(caseType, value)
            }
        }
        if (call.getType().getSqlTypeName() === SqlTypeName.BOOLEAN) {
            val result: RexNode? = simplifyBooleanCase(rexBuilder, branches, unknownAs, caseType)
            if (result != null) {
                return if (sameTypeOrNarrowsNullability(caseType, result.getType())) {
                    simplify(result, unknownAs)
                } else {
                    // If the simplification would widen the nullability
                    val simplified: RexNode = simplify(result, UNKNOWN)
                    if (!simplified.getType().isNullable()) {
                        simplified
                    } else {
                        rexBuilder.makeCast(call.getType(), simplified)
                    }
                }
            }
        }
        val newOperands: List<RexNode> = CaseBranch.toCaseOperands(branches)
        return if (newOperands.equals(call.getOperands())) {
            call
        } else rexBuilder.makeCall(SqlStdOperatorTable.CASE, newOperands)
    }

    /**
     * Return if the new type is the same and at most narrows the nullability.
     */
    private fun sameTypeOrNarrowsNullability(oldType: RelDataType, newType: RelDataType): Boolean {
        return (oldType.equals(newType)
                || (SqlTypeUtil.equalSansNullability(rexBuilder.typeFactory, oldType, newType)
                && oldType.isNullable()))
    }

    /** Object to describe a CASE branch.  */
    internal class CaseBranch(cond: RexNode, value: RexNode) {
        val cond: RexNode
        val value: RexNode

        init {
            this.cond = cond
            this.value = value
        }

        @Override
        override fun toString(): String {
            return cond.toString() + " => " + value
        }

        companion object {
            /** Given "CASE WHEN p1 THEN v1 ... ELSE e END"
             * returns [(p1, v1), ..., (true, e)].  */
            fun fromCaseOperands(
                rexBuilder: RexBuilder,
                operands: List<RexNode>
            ): List<CaseBranch> {
                val ret: List<CaseBranch> = ArrayList()
                var i = 0
                while (i < operands.size() - 1) {
                    ret.add(CaseBranch(operands[i], operands[i + 1]))
                    i += 2
                }
                ret.add(CaseBranch(rexBuilder.makeLiteral(true), Util.last(operands)))
                return ret
            }

            fun toCaseOperands(branches: List<CaseBranch>): List<RexNode> {
                val ret: List<RexNode> = ArrayList()
                for (i in 0 until branches.size() - 1) {
                    val branch = branches[i]
                    ret.add(branch.cond)
                    ret.add(branch.value)
                }
                val lastBranch: CaseBranch = Util.last(branches)
                assert(lastBranch.cond.isAlwaysTrue())
                ret.add(lastBranch.value)
                return ret
            }
        }
    }

    /**
     * Decides whether it is safe to flatten the given CASE part into ANDs/ORs.
     */
    internal enum class SafeRexVisitor : RexVisitor<Boolean?> {
        INSTANCE;

        @SuppressWarnings("ImmutableEnumChecker")
        private val safeOps: Set<SqlKind>

        init {
            val safeOps: Set<SqlKind> = EnumSet.noneOf(SqlKind::class.java)
            safeOps.addAll(SqlKind.COMPARISON)
            safeOps.add(SqlKind.PLUS_PREFIX)
            safeOps.add(SqlKind.MINUS_PREFIX)
            safeOps.add(SqlKind.PLUS)
            safeOps.add(SqlKind.MINUS)
            safeOps.add(SqlKind.TIMES)
            safeOps.add(SqlKind.IS_FALSE)
            safeOps.add(SqlKind.IS_NOT_FALSE)
            safeOps.add(SqlKind.IS_TRUE)
            safeOps.add(SqlKind.IS_NOT_TRUE)
            safeOps.add(SqlKind.IS_NULL)
            safeOps.add(SqlKind.IS_NOT_NULL)
            safeOps.add(SqlKind.IS_DISTINCT_FROM)
            safeOps.add(SqlKind.IS_NOT_DISTINCT_FROM)
            safeOps.add(SqlKind.IN)
            safeOps.add(SqlKind.SEARCH)
            safeOps.add(SqlKind.OR)
            safeOps.add(SqlKind.AND)
            safeOps.add(SqlKind.NOT)
            safeOps.add(SqlKind.CASE)
            safeOps.add(SqlKind.LIKE)
            safeOps.add(SqlKind.COALESCE)
            safeOps.add(SqlKind.TRIM)
            safeOps.add(SqlKind.LTRIM)
            safeOps.add(SqlKind.RTRIM)
            safeOps.add(SqlKind.BETWEEN)
            safeOps.add(SqlKind.CEIL)
            safeOps.add(SqlKind.FLOOR)
            safeOps.add(SqlKind.REVERSE)
            safeOps.add(SqlKind.TIMESTAMP_ADD)
            safeOps.add(SqlKind.TIMESTAMP_DIFF)
            this.safeOps = Sets.immutableEnumSet(safeOps)
        }

        @Override
        fun visitInputRef(inputRef: RexInputRef?): Boolean {
            return true
        }

        @Override
        fun visitLocalRef(localRef: RexLocalRef?): Boolean {
            return false
        }

        @Override
        fun visitLiteral(literal: RexLiteral?): Boolean {
            return true
        }

        @Override
        fun visitCall(call: RexCall): Boolean {
            return if (!safeOps.contains(call.getKind())) {
                false
            } else RexVisitorImpl.visitArrayAnd(this, call.operands)
        }

        @Override
        fun visitOver(over: RexOver?): Boolean {
            return false
        }

        @Override
        fun visitCorrelVariable(correlVariable: RexCorrelVariable?): Boolean {
            return false
        }

        @Override
        fun visitDynamicParam(dynamicParam: RexDynamicParam?): Boolean {
            return false
        }

        @Override
        fun visitRangeRef(rangeRef: RexRangeRef?): Boolean {
            return false
        }

        @Override
        fun visitFieldAccess(fieldAccess: RexFieldAccess?): Boolean {
            return true
        }

        @Override
        fun visitSubQuery(subQuery: RexSubQuery?): Boolean {
            return false
        }

        @Override
        fun visitTableInputRef(fieldRef: RexTableInputRef?): Boolean {
            return false
        }

        @Override
        fun visitPatternFieldRef(fieldRef: RexPatternFieldRef?): Boolean {
            return false
        }
    }

    @Nullable
    private fun simplifyBooleanCase(
        rexBuilder: RexBuilder,
        inputBranches: List<CaseBranch>, @SuppressWarnings("unused") unknownAs: RexUnknownAs,
        branchType: RelDataType
    ): RexNode? {
        val result: RexNode

        // prepare all condition/branches for boolean interpretation
        // It's done here make these interpretation changes available to case2or simplifications
        // but not interfere with the normal simplification recursion
        val branches: List<CaseBranch> = ArrayList()
        for (branch in inputBranches) {
            if (branches.size() > 0 && !isSafeExpression(branch.cond)
                || !isSafeExpression(branch.value)
            ) {
                return null
            }
            val cond: RexNode = isTrue(branch.cond)
            val value: RexNode
            value = if (!branchType.equals(branch.value.getType())) {
                rexBuilder.makeAbstractCast(branchType, branch.value)
            } else {
                branch.value
            }
            branches.add(CaseBranch(cond, value))
        }
        result = simplifyBooleanCaseGeneric(rexBuilder, branches)
        return result
    }

    @Deprecated // to be removed before 2.0
    fun simplifyAnd(e: RexCall?): RexNode {
        ensureParanoidOff()
        return simplifyAnd(e, defaultUnknownAs)
    }

    fun simplifyAnd(e: RexCall?, unknownAs: RexUnknownAs): RexNode {
        val operands: List<RexNode> = RelOptUtil.conjunctions(e)
        if (unknownAs === FALSE && predicateElimination) {
            simplifyAndTerms(operands, FALSE)
        } else {
            simplifyList(operands, unknownAs)
        }
        val terms: List<RexNode> = ArrayList()
        val notTerms: List<RexNode> = ArrayList()
        val sargCollector = SargCollector(rexBuilder, true)
        operands.forEach { t -> sargCollector.accept(t, terms) }
        if (sargCollector.needToFix()) {
            operands.clear()
            terms.forEach { t -> operands.add(SargCollector.fix(rexBuilder, t, unknownAs)) }
        }
        terms.clear()
        for (o in operands) {
            RelOptUtil.decomposeConjunction(o, terms, notTerms)
        }
        when (unknownAs) {
            FALSE -> return simplifyAnd2ForUnknownAsFalse(terms, notTerms, Comparable::class.java)
            else -> {}
        }
        return simplifyAnd2(terms, notTerms)
    }

    // package-protected only to support a deprecated method; treat as private
    fun simplifyAnd2(terms: List<RexNode>, notTerms: List<RexNode>): RexNode {
        for (term in terms) {
            if (term.isAlwaysFalse()) {
                return rexBuilder.makeLiteral(false)
            }
        }
        if (terms.isEmpty() && notTerms.isEmpty()) {
            return rexBuilder.makeLiteral(true)
        }
        // If one of the not-disjunctions is a disjunction that is wholly
        // contained in the disjunctions list, the expression is not
        // satisfiable.
        //
        // Example #1. x AND y AND z AND NOT (x AND y)  - not satisfiable
        // Example #2. x AND y AND NOT (x AND y)        - not satisfiable
        // Example #3. x AND y AND NOT (x AND y AND z)  - may be satisfiable
        var notSatisfiableNullables: List<RexNode>? = null
        for (notDisjunction in notTerms) {
            val terms2: List<RexNode> = RelOptUtil.conjunctions(notDisjunction)
            if (!terms.containsAll(terms2)) {
                // may be satisfiable ==> check other terms
                continue
            }
            if (!notDisjunction.getType().isNullable()) {
                // x is NOT nullable, then x AND NOT(x) ==> FALSE
                return rexBuilder.makeLiteral(false)
            }
            // x AND NOT(x) is UNKNOWN for NULL input
            // So we search for the shortest notDisjunction then convert
            // original expression to NULL and x IS NULL
            if (notSatisfiableNullables == null) {
                notSatisfiableNullables = ArrayList()
            }
            notSatisfiableNullables.add(notDisjunction)
        }
        if (notSatisfiableNullables != null) {
            // Remove the intersection of "terms" and "notTerms"
            terms.removeAll(notSatisfiableNullables)
            notTerms.removeAll(notSatisfiableNullables)

            // The intersection simplify to "null and x1 is null and x2 is null..."
            terms.add(rexBuilder.makeNullLiteral(notSatisfiableNullables[0].getType()))
            for (notSatisfiableNullable in notSatisfiableNullables) {
                terms.add(
                    simplifyIs(
                        rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, notSatisfiableNullable) as RexCall,
                        UNKNOWN
                    )
                )
            }
        }
        // Add the NOT disjunctions back in.
        for (notDisjunction in notTerms) {
            terms.add(simplify(not(notDisjunction), UNKNOWN))
        }
        return RexUtil.composeConjunction(rexBuilder, terms)
    }

    /** As [.simplifyAnd2] but we assume that if the expression
     * returns UNKNOWN it will be interpreted as FALSE.  */
    fun simplifyAnd2ForUnknownAsFalse(
        terms: List<RexNode>,
        notTerms: List<RexNode>
    ): RexNode {
        return simplifyAnd2ForUnknownAsFalse(terms, notTerms, Comparable::class.java)
    }

    private fun <C : Comparable<C>?> simplifyAnd2ForUnknownAsFalse(
        terms: List<RexNode>, notTerms: List<RexNode>, clazz: Class<C>
    ): RexNode {
        for (term in terms) {
            if (term.isAlwaysFalse() || RexLiteral.isNullLiteral(term)) {
                return rexBuilder.makeLiteral(false)
            }
        }
        if (terms.isEmpty() && notTerms.isEmpty()) {
            return rexBuilder.makeLiteral(true)
        }
        if (terms.size() === 1 && notTerms.isEmpty()) {
            // Make sure "x OR y OR x" (a single-term conjunction) gets simplified.
            return simplify(terms[0], FALSE)
        }
        // Try to simplify the expression
        val equalityTerms: Multimap<RexNode, Pair<RexNode, RexNode>> = ArrayListMultimap.create()
        val rangeTerms: Map<RexNode, Pair<Range<C>, List<RexNode>>> = HashMap()
        val equalityConstantTerms: Map<RexNode, RexLiteral> = HashMap()
        val negatedTerms: Set<RexNode> = HashSet()
        val nullOperands: Set<RexNode> = HashSet()
        val notNullOperands: Set<RexNode> = LinkedHashSet()
        val comparedOperands: Set<RexNode> = HashSet()

        // Add the predicates from the source to the range terms.
        for (predicate in predicates.pulledUpPredicates) {
            val comparison = Comparison.of(predicate)
            if (comparison != null
                && comparison.kind !== SqlKind.NOT_EQUALS
            ) { // not supported yet
                val v0: C = comparison.literal.getValueAs(clazz)
                if (v0 != null) {
                    val result: RexNode? = processRange(
                        rexBuilder, terms, rangeTerms,
                        predicate, comparison.ref, v0, comparison.kind
                    )
                    if (result != null) {
                        // Not satisfiable
                        return result
                    }
                }
            }
        }
        var i = 0
        while (i < terms.size()) {
            var term: RexNode = terms[i]
            if (!RexUtil.isDeterministic(term)) {
                i++
                continue
            }
            // Simplify BOOLEAN expressions if possible
            while (term.getKind() === SqlKind.EQUALS) {
                val call: RexCall = term as RexCall
                if (call.getOperands().get(0).isAlwaysTrue()) {
                    term = call.getOperands().get(1)
                    terms.set(i, term)
                    continue
                } else if (call.getOperands().get(1).isAlwaysTrue()) {
                    term = call.getOperands().get(0)
                    terms.set(i, term)
                    continue
                }
                break
            }
            when (term.getKind()) {
                EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN_OR_EQUAL -> {
                    val call: RexCall = term as RexCall
                    val left: RexNode = call.getOperands().get(0)
                    comparedOperands.add(left)
                    // if it is a cast, we include the inner reference
                    if (left.getKind() === SqlKind.CAST) {
                        val leftCast: RexCall = left as RexCall
                        comparedOperands.add(leftCast.getOperands().get(0))
                    }
                    val right: RexNode = call.getOperands().get(1)
                    comparedOperands.add(right)
                    // if it is a cast, we include the inner reference
                    if (right.getKind() === SqlKind.CAST) {
                        val rightCast: RexCall = right as RexCall
                        comparedOperands.add(rightCast.getOperands().get(0))
                    }
                    val comparison = Comparison.of(term)
                    // Check for comparison with null values
                    if (comparison != null
                        && comparison.literal.getValue() == null
                    ) {
                        return rexBuilder.makeLiteral(false)
                    }
                    // Check for equality on different constants. If the same ref or CAST(ref)
                    // is equal to different constants, this condition cannot be satisfied,
                    // and hence it can be evaluated to FALSE
                    if (term.getKind() === SqlKind.EQUALS) {
                        if (comparison != null) {
                            val literal: RexLiteral = comparison.literal
                            val prevLiteral: RexLiteral = equalityConstantTerms.put(comparison.ref, literal)
                            if (prevLiteral != null && !literal.equals(prevLiteral)) {
                                return rexBuilder.makeLiteral(false)
                            }
                        } else if (RexUtil.isReferenceOrAccess(left, true)
                            && RexUtil.isReferenceOrAccess(right, true)
                        ) {
                            equalityTerms.put(left, Pair.of(right, term))
                        }
                    }
                    // Assume the expression a > 5 is part of a Filter condition.
                    // Then we can derive the negated term: a <= 5.
                    // But as the comparison is string based and thus operands order dependent,
                    // we should also add the inverted negated term: 5 >= a.
                    // Observe that for creating the inverted term we invert the list of operands.
                    val negatedTerm: RexNode = RexUtil.negate(rexBuilder, call)
                    if (negatedTerm != null) {
                        negatedTerms.add(negatedTerm)
                        val invertNegatedTerm: RexNode = RexUtil.invert(rexBuilder, negatedTerm as RexCall)
                        if (invertNegatedTerm != null) {
                            negatedTerms.add(invertNegatedTerm)
                        }
                    }
                    // Remove terms that are implied by predicates on the input,
                    // or weaken terms that are partially implied.
                    // E.g. given predicate "x >= 5" and term "x between 3 and 10"
                    // we weaken to term to "x between 5 and 10".
                    val term2: RexNode = simplifyUsingPredicates<Comparable<C>>(term, clazz)
                    if (term2 !== term) {
                        terms.set(i, term2.also { term = it })
                    }
                    // Range
                    if (comparison != null
                        && comparison.kind !== SqlKind.NOT_EQUALS
                    ) { // not supported yet
                        val constant: C = comparison.literal.getValueAs(clazz) ?: break
                        val result: RexNode? = processRange(
                            rexBuilder, terms, rangeTerms,
                            term, comparison.ref, constant, comparison.kind
                        )
                        if (result != null) {
                            // Not satisfiable
                            return result
                        }
                    }
                }
                IN -> comparedOperands.add((term as RexCall).operands.get(0))
                BETWEEN -> comparedOperands.add((term as RexCall).operands.get(1))
                IS_NOT_NULL -> {
                    notNullOperands.add((term as RexCall).getOperands().get(0))
                    terms.remove(i)
                    --i
                }
                IS_NULL -> nullOperands.add((term as RexCall).getOperands().get(0))
                else -> {}
            }
            i++
        }
        // If one column should be null and is in a comparison predicate,
        // it is not satisfiable.
        // Example. IS NULL(x) AND x < 5  - not satisfiable
        if (!Collections.disjoint(nullOperands, comparedOperands)) {
            return rexBuilder.makeLiteral(false)
        }
        // Check for equality of two refs wrt equality with constants
        // Example #1. x=5 AND y=5 AND x=y : x=5 AND y=5
        // Example #2. x=5 AND y=6 AND x=y - not satisfiable
        for (ref1 in equalityTerms.keySet()) {
            val literal1: RexLiteral = equalityConstantTerms[ref1] ?: continue
            val references: Collection<Pair<RexNode, RexNode>> = equalityTerms.get(ref1)
            for (ref2 in references) {
                val literal2: RexLiteral = equalityConstantTerms[ref2.left] ?: continue
                if (!literal1.equals(literal2)) {
                    // If an expression is equal to two different constants,
                    // it is not satisfiable
                    return rexBuilder.makeLiteral(false)
                }
                // Otherwise we can remove the term, as we already know that
                // the expression is equal to two constants
                terms.remove(ref2.right)
            }
        }
        // Remove not necessary IS NOT NULL expressions.
        //
        // Example. IS NOT NULL(x) AND x < 5  : x < 5
        for (operand in notNullOperands) {
            if (!comparedOperands.contains(operand)) {
                terms.add(
                    rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, operand)
                )
            }
        }
        // If one of the not-disjunctions is a disjunction that is wholly
        // contained in the disjunctions list, the expression is not
        // satisfiable.
        //
        // Example #1. x AND y AND z AND NOT (x AND y)  - not satisfiable
        // Example #2. x AND y AND NOT (x AND y)        - not satisfiable
        // Example #3. x AND y AND NOT (x AND y AND z)  - may be satisfiable
        val termsSet: Set<RexNode> = HashSet(terms)
        for (notDisjunction in notTerms) {
            if (!RexUtil.isDeterministic(notDisjunction)) {
                continue
            }
            val terms2Set: List<RexNode> = RelOptUtil.conjunctions(notDisjunction)
            if (termsSet.containsAll(terms2Set)) {
                return rexBuilder.makeLiteral(false)
            }
        }
        // Add the NOT disjunctions back in.
        for (notDisjunction in notTerms) {
            terms.add(not(notDisjunction))
        }
        // The negated terms: only deterministic expressions
        for (negatedTerm in negatedTerms) {
            if (termsSet.contains(negatedTerm)) {
                return rexBuilder.makeLiteral(false)
            }
        }
        return RexUtil.composeConjunction(rexBuilder, terms)
    }

    @SuppressWarnings("BetaApi")
    private fun <C : Comparable<C>?> simplifyUsingPredicates(
        e: RexNode,
        clazz: Class<C>
    ): RexNode {
        if (predicates.pulledUpPredicates.isEmpty()) {
            return e
        }
        val comparison = Comparison.of(e)
        // Check for comparison with null values
        if (comparison == null
            || comparison.literal.getValue() == null
        ) {
            return e
        }
        val v0: C = comparison.literal.getValueAs(clazz) ?: return e
        val rangeSet: RangeSet<C> = rangeSet(comparison.kind, v0)
        val rangeSet2: RangeSet<C> = residue<Comparable<C>>(
            comparison.ref, rangeSet, predicates.pulledUpPredicates,
            clazz
        )
        return if (rangeSet2.isEmpty()) {
            // Term is impossible to satisfy given these predicates
            rexBuilder.makeLiteral(false)
        } else if (rangeSet2.equals(rangeSet)) {
            // no change
            e
        } else if (rangeSet2.equals(RangeSets.rangeSetAll())) {
            // Range is always satisfied given these predicates; but nullability might
            // be problematic
            simplify(
                rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, comparison.ref),
                RexUnknownAs.UNKNOWN
            )
        } else if (rangeSet2.asRanges().size() === 1 && Iterables.getOnlyElement(rangeSet2.asRanges()).hasLowerBound()
            && Iterables.getOnlyElement(rangeSet2.asRanges()).hasUpperBound()
            && Iterables.getOnlyElement(rangeSet2.asRanges()).lowerEndpoint()
                .equals(Iterables.getOnlyElement(rangeSet2.asRanges()).upperEndpoint())
        ) {
            val r: Range<C> = Iterables.getOnlyElement(rangeSet2.asRanges())
            // range is now a point; it's worth simplifying
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS, comparison.ref,
                rexBuilder.makeLiteral(
                    r.lowerEndpoint(),
                    comparison.literal.getType(), comparison.literal.getTypeName()
                )
            )
        } else {
            // range has been reduced but it's not worth simplifying
            e
        }
    }

    /** Simplifies OR(x, x) into x, and similar.
     * The simplified expression returns UNKNOWN values as is (not as FALSE).  */
    @Deprecated // to be removed before 2.0
    fun simplifyOr(call: RexCall): RexNode {
        ensureParanoidOff()
        return simplifyOr(call, UNKNOWN)
    }

    private fun simplifyOr(call: RexCall, unknownAs: RexUnknownAs): RexNode {
        assert(call.getKind() === SqlKind.OR)
        val terms0: List<RexNode> = RelOptUtil.disjunctions(call)
        val terms: List<RexNode>
        if (predicateElimination) {
            terms = Util.moveToHead(terms0) { e -> e.getKind() === SqlKind.IS_NULL }
            simplifyOrTerms(terms, unknownAs)
        } else {
            terms = terms0
            simplifyList(terms, unknownAs)
        }
        return simplifyOrs(terms, unknownAs)
    }

    /** Simplifies a list of terms and combines them into an OR.
     * Modifies the list in place.
     * The simplified expression returns UNKNOWN values as is (not as FALSE).  */
    @Deprecated // to be removed before 2.0
    fun simplifyOrs(terms: List<RexNode>): RexNode {
        ensureParanoidOff()
        return simplifyOrs(terms, UNKNOWN)
    }

    private fun ensureParanoidOff() {
        if (paranoid) {
            throw UnsupportedOperationException("Paranoid is not supported for this method")
        }
    }

    /** Simplifies a list of terms and combines them into an OR.
     * Modifies the list in place.  */
    private fun simplifyOrs(terms: List<RexNode>, unknownAs: RexUnknownAs): RexNode {
        val sargCollector = SargCollector(rexBuilder, false)
        val newTerms: List<RexNode> = ArrayList()
        terms.forEach { t -> sargCollector.accept(t, newTerms) }
        if (sargCollector.needToFix()) {
            terms.clear()
            newTerms.forEach { t -> terms.add(SargCollector.fix(rexBuilder, t, unknownAs)) }
        }

        // CALCITE-3198 Auxiliary map to simplify cases like:
        //   X <> A OR X <> B => X IS NOT NULL or NULL
        // The map key will be the 'X'; and the value the first call 'X<>A' that is found,
        // or 'X IS NOT NULL' if a simplification takes place (because another 'X<>B' is found)
        val notEqualsComparisonMap: Map<RexNode, RexNode> = HashMap()
        val trueLiteral: RexLiteral = rexBuilder.makeLiteral(true)
        var i = 0
        while (i < terms.size()) {
            val term: RexNode = terms[i]
            when (term.getKind()) {
                LITERAL -> if (RexLiteral.isNullLiteral(term)) {
                    if (unknownAs === FALSE) {
                        terms.remove(i)
                        --i
                        i++
                        continue
                    } else if (unknownAs === TRUE) {
                        return trueLiteral
                    }
                } else {
                    return if (RexLiteral.booleanValue(term)) {
                        term // true
                    } else {
                        terms.remove(i)
                        --i
                        i++
                        continue
                    }
                }
                NOT_EQUALS -> {
                    val notEqualsComparison = Comparison.of(term)
                    if (notEqualsComparison != null) {
                        // We are dealing with a X<>A term, check if we saw before another NOT_EQUALS involving X
                        val prevNotEquals: RexNode? = notEqualsComparisonMap[notEqualsComparison.ref]
                        if (prevNotEquals == null) {
                            // This is the first NOT_EQUALS involving X, put it in the map
                            notEqualsComparisonMap.put(notEqualsComparison.ref, term)
                        } else {
                            // There is already in the map another NOT_EQUALS involving X:
                            //   - if it is already an IS_NOT_NULL: it was already simplified, ignore this term
                            //   - if it is not an IS_NOT_NULL (i.e. it is a NOT_EQUALS): check comparison values
                            if (prevNotEquals.getKind() !== SqlKind.IS_NOT_NULL) {
                                val comparable1: Comparable = notEqualsComparison.literal.getValue()
                                val comparable2: Comparable =
                                    castNonNull(Comparison.of(prevNotEquals)).literal.getValue()
                                if (comparable1 != null && comparable2 != null && comparable1.compareTo(comparable2) !== 0) {
                                    // X <> A OR X <> B => X IS NOT NULL OR NULL
                                    val isNotNull: RexNode =
                                        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, notEqualsComparison.ref)
                                    val constantNull: RexNode = rexBuilder.makeNullLiteral(trueLiteral.getType())
                                    val newCondition: RexNode = simplify(
                                        rexBuilder.makeCall(SqlStdOperatorTable.OR, isNotNull, constantNull),
                                        unknownAs
                                    )
                                    if (newCondition.isAlwaysTrue()) {
                                        return trueLiteral
                                    }
                                    notEqualsComparisonMap.put(notEqualsComparison.ref, isNotNull)
                                    val pos = terms.indexOf(prevNotEquals)
                                    terms.set(pos, newCondition)
                                }
                            }
                            terms.remove(i)
                            --i
                            i++
                            continue
                        }
                    }
                }
                else -> {}
            }
            i++
        }
        return RexUtil.composeDisjunction(rexBuilder, terms)
    }

    private fun verify(before: RexNode, simplified: RexNode, unknownAs: RexUnknownAs) {
        if (simplified.isAlwaysFalse()
            && before.isAlwaysTrue()
        ) {
            throw AssertionError(
                "always true [" + before
                        + "] simplified to always false [" + simplified + "]"
            )
        }
        if (simplified.isAlwaysTrue()
            && before.isAlwaysFalse()
        ) {
            throw AssertionError(
                "always false [" + before
                        + "] simplified to always true [" + simplified + "]"
            )
        }
        val foo0 = RexAnalyzer(before, predicates)
        val foo1 = RexAnalyzer(simplified, predicates)
        if (foo0.unsupportedCount > 0 || foo1.unsupportedCount > 0) {
            // Analyzer cannot handle this expression currently
            return
        }
        if (!foo0.variables.containsAll(foo1.variables)) {
            throw AssertionError(
                "variable mismatch: "
                        + before + " has " + foo0.variables + ", "
                        + simplified + " has " + foo1.variables
            )
        }
        assignment_loop@ for (map in foo0.assignments()) {
            for (predicate in predicates.pulledUpPredicates) {
                val v: Comparable = RexInterpreter.evaluate(predicate, map)
                if (!Boolean.TRUE.equals(v)) {
                    continue@assignment_loop
                }
            }
            var v0: Comparable = RexInterpreter.evaluate(foo0.e, map)
                ?: throw AssertionError("interpreter returned null for " + foo0.e)
            var v1: Comparable = RexInterpreter.evaluate(foo1.e, map)
                ?: throw AssertionError("interpreter returned null for " + foo1.e)
            if (before.getType().getSqlTypeName() === SqlTypeName.BOOLEAN) {
                when (unknownAs) {
                    FALSE, TRUE -> {
                        if (v0 === NullSentinel.INSTANCE) {
                            v0 = unknownAs.toBoolean()
                        }
                        if (v1 === NullSentinel.INSTANCE) {
                            v1 = unknownAs.toBoolean()
                        }
                    }
                    else -> {}
                }
            }
            if (!v0.equals(v1)) {
                throw AssertionError(
                    """result mismatch (unknown as $unknownAs): when applied to $map,
$before yielded $v0;
$simplified yielded $v1"""
                )
            }
        }
    }

    private fun simplifySearch(call: RexCall, unknownAs: RexUnknownAs): RexNode {
        assert(call.getKind() === SqlKind.SEARCH)
        val a: RexNode = call.getOperands().get(0)
        if (call.getOperands().get(1) is RexLiteral) {
            val literal: RexLiteral = call.getOperands().get(1)
            val sarg: Sarg = castNonNull(literal.getValueAs(Sarg::class.java))
            if (sarg.isAll() || sarg.isNone()) {
                return RexUtil.simpleSarg(rexBuilder, a, sarg, unknownAs)
            }
            // Remove null from sarg if the left-hand side is never null
            if (sarg.nullAs !== UNKNOWN) {
                val simplified: RexNode? = simplifyIs1(SqlKind.IS_NULL, a, unknownAs)
                if (simplified != null
                    && simplified.isAlwaysFalse()
                ) {
                    val sarg2: Sarg = Sarg.of(UNKNOWN, sarg.rangeSet)
                    val literal2: RexLiteral = rexBuilder.makeLiteral(
                        sarg2, literal.getType(),
                        literal.getTypeName()
                    )
                    // Now we've strengthened the Sarg, try to simplify again
                    return simplifySearch(
                        call.clone(call.type, ImmutableList.of(a, literal2)),
                        unknownAs
                    )
                }
            } else if (sarg.isPoints() && sarg.pointCount <= 1) {
                // Expand "SEARCH(x, Sarg([point])" to "x = point"
                // and "SEARCH(x, Sarg([])" to "false"
                return RexUtil.expandSearch(rexBuilder, null, call)
            }
        }
        return call
    }

    private fun simplifyCast(e: RexCall): RexNode {
        var operand: RexNode = e.getOperands().get(0)
        operand = simplify(operand, UNKNOWN)
        if (sameTypeOrNarrowsNullability(e.getType(), operand.getType())) {
            return operand
        }
        if (RexUtil.isLosslessCast(operand)) {
            // x :: y below means cast(x as y) (which is PostgreSQL-specifiic cast by the way)
            // A) Remove lossless casts:
            // A.1) intExpr :: bigint :: int => intExpr
            // A.2) char2Expr :: char(5) :: char(2) => char2Expr
            // B) There are cases when we can't remove two casts, but we could probably remove inner one
            // B.1) char2expression :: char(4) :: char(5) -> char2expression :: char(5)
            // B.2) char2expression :: char(10) :: char(5) -> char2expression :: char(5)
            // B.3) char2expression :: varchar(10) :: char(5) -> char2expression :: char(5)
            // B.4) char6expression :: varchar(10) :: char(5) -> char6expression :: char(5)
            // C) Simplification is not possible:
            // C.1) char6expression :: char(3) :: char(5) -> must not be changed
            //      the input is truncated to 3 chars, so we can't use char6expression :: char(5)
            // C.2) varchar2Expr :: char(5) :: varchar(2) -> must not be changed
            //      the input have to be padded with spaces (up to 2 chars)
            // C.3) char2expression :: char(4) :: varchar(5) -> must not be changed
            //      would not have the padding

            // The approach seems to be:
            // 1) Ensure inner cast is lossless (see if above)
            // 2) If operand of the inner cast has the same type as the outer cast,
            //    remove two casts except C.2 or C.3-like pattern (== inner cast is CHAR)
            // 3) If outer cast is lossless, remove inner cast (B-like cases)

            // Here we try to remove two casts in one go (A-like cases)
            val intExpr: RexNode = (operand as RexCall).operands.get(0)
            // intExpr == CHAR detects A.1
            // operand != CHAR detects C.2
            if ((intExpr.getType().getSqlTypeName() === SqlTypeName.CHAR
                        || operand.getType().getSqlTypeName() !== SqlTypeName.CHAR)
                && sameTypeOrNarrowsNullability(e.getType(), intExpr.getType())
            ) {
                return intExpr
            }
            // Here we try to remove inner cast (B-like cases)
            if (RexUtil.isLosslessCast(intExpr.getType(), operand.getType())
                && (e.getType().getSqlTypeName() === operand.getType().getSqlTypeName() || e.getType()
                    .getSqlTypeName() === SqlTypeName.CHAR || operand.getType().getSqlTypeName() !== SqlTypeName.CHAR)
                && SqlTypeCoercionRule.instance()
                    .canApplyFrom(intExpr.getType().getSqlTypeName(), e.getType().getSqlTypeName())
            ) {
                return rexBuilder.makeCast(e.getType(), intExpr)
            }
        }
        return when (operand.getKind()) {
            LITERAL -> {
                val literal: RexLiteral = operand as RexLiteral
                val value: Comparable = literal.getValueAs(Comparable::class.java)
                val typeName: SqlTypeName = literal.getTypeName()

                // First, try to remove the cast without changing the value.
                // makeCast and canRemoveCastFromLiteral have the same logic, so we are
                // sure to be able to remove the cast.
                if (rexBuilder.canRemoveCastFromLiteral(e.getType(), value, typeName)) {
                    return rexBuilder.makeCast(e.getType(), operand)
                }
                when (literal.getTypeName()) {
                    TIME -> when (e.getType().getSqlTypeName()) {
                        TIMESTAMP -> return e
                        else -> {}
                    }
                    else -> {}
                }
                val reducedValues: List<RexNode> = ArrayList()
                val simplifiedExpr: RexNode = rexBuilder.makeCast(e.getType(), operand)
                executor.reduce(rexBuilder, ImmutableList.of(simplifiedExpr), reducedValues)
                requireNonNull(
                    Iterables.getOnlyElement(reducedValues)
                )
            }
            else -> if (operand === e.getOperands().get(0)) {
                e
            } else {
                rexBuilder.makeCast(e.getType(), operand)
            }
        }
    }

    /** Tries to simplify CEIL/FLOOR function on top of CEIL/FLOOR.
     *
     *
     * Examples:
     *
     *
     *  * `floor(floor($0, flag(hour)), flag(day))` returns `floor($0, flag(day))`
     *
     *  * `ceil(ceil($0, flag(second)), flag(day))` returns `ceil($0, flag(day))`
     *
     *  * `floor(floor($0, flag(day)), flag(second))` does not change
     *
     *
     */
    private fun simplifyCeilFloor(e: RexCall): RexNode {
        if (e.getOperands().size() !== 2) {
            // Bail out since we only simplify floor <date>
            return e
        }
        val operand: RexNode = simplify(e.getOperands().get(0), UNKNOWN)
        if (e.getKind() === operand.getKind()) {
            assert(e.getKind() === SqlKind.CEIL || e.getKind() === SqlKind.FLOOR)
            // CEIL/FLOOR on top of CEIL/FLOOR
            val child: RexCall = operand as RexCall
            if (child.getOperands().size() !== 2) {
                // Bail out since we only simplify ceil/floor <date>
                return e
            }
            val parentFlag: RexLiteral = e.operands.get(1)
            val parentFlagValue: TimeUnitRange = parentFlag.getValue() as TimeUnitRange
            val childFlagValue: TimeUnitRange = child.operands.get(1).getValue() as TimeUnitRange
            if (parentFlagValue != null && childFlagValue != null) {
                if (canRollUp(parentFlagValue.startUnit, childFlagValue.startUnit)) {
                    return e.clone(
                        e.getType(),
                        ImmutableList.of(child.getOperands().get(0), parentFlag)
                    )
                }
            }
        }
        return e.clone(
            e.getType(),
            ImmutableList.of(operand, e.getOperands().get(1))
        )
    }

    /** Removes any casts that change nullability but not type.
     *
     *
     * For example, `CAST(1 = 0 AS BOOLEAN)` becomes `1 = 0`.  */
    fun removeNullabilityCast(e: RexNode?): RexNode {
        return RexUtil.removeNullabilityCast(rexBuilder.getTypeFactory(), e)
    }

    /** Marker interface for predicates (expressions that evaluate to BOOLEAN).  */
    private interface Predicate {
        /** Returns whether this predicate can be used while simplifying other OR
         * operands.  */
        fun allowedInOr(predicates: RelOptPredicateList?): Boolean {
            return true
        }

        companion object {
            /** Wraps an expression in a Predicate or returns null.  */
            @Nullable
            fun of(t: RexNode): Predicate? {
                val p: Predicate? = Comparison.of(t)
                return p ?: IsPredicate.of(t)
            }
        }
    }

    /** Represents a simple Comparison.
     *
     *
     * Left hand side is a [RexNode], right hand side is a literal.
     */
    private class Comparison private constructor(ref: RexNode, kind: SqlKind, literal: RexLiteral) : Predicate {
        val ref: RexNode
        val kind: SqlKind
        val literal: RexLiteral

        init {
            this.ref = requireNonNull(ref, "ref")
            this.kind = requireNonNull(kind, "kind")
            this.literal = requireNonNull(literal, "literal")
        }

        @Override
        override fun allowedInOr(predicates: RelOptPredicateList): Boolean {
            return (!ref.getType().isNullable()
                    || predicates.isEffectivelyNotNull(ref))
        }

        companion object {
            /** Creates a comparison, between a [RexInputRef] or [RexFieldAccess] or
             * deterministic [RexCall] and a literal.  */
            @Nullable
            fun of(e: RexNode): Comparison? {
                return of(e, java.util.function.Predicate<RexNode> { node ->
                    (RexUtil.isReferenceOrAccess(node, true)
                            || RexUtil.isDeterministic(node))
                })
            }

            /** Creates a comparison, or returns null.  */
            @Nullable
            fun of(e: RexNode, nodePredicate: java.util.function.Predicate<RexNode?>): Comparison? {
                when (e.getKind()) {
                    EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN_OR_EQUAL -> {
                        val call: RexCall = e as RexCall
                        val left: RexNode = call.getOperands().get(0)
                        val right: RexNode = call.getOperands().get(1)
                        when (right.getKind()) {
                            LITERAL -> if (nodePredicate.test(left)) {
                                return Comparison(left, e.getKind(), right as RexLiteral)
                            }
                            else -> {}
                        }
                        when (left.getKind()) {
                            LITERAL -> if (nodePredicate.test(right)) {
                                return Comparison(right, e.getKind().reverse(), left as RexLiteral)
                            }
                            else -> {}
                        }
                    }
                    else -> {}
                }
                return null
            }
        }
    }

    /** Represents an IS Predicate.  */
    private class IsPredicate private constructor(ref: RexNode, kind: SqlKind) : Predicate {
        val ref: RexNode
        val kind: SqlKind

        init {
            this.ref = requireNonNull(ref, "ref")
            this.kind = requireNonNull(kind, "kind")
        }

        companion object {
            /** Creates an IS predicate, or returns null.  */
            @Nullable
            fun of(e: RexNode): IsPredicate? {
                when (e.getKind()) {
                    IS_NULL, IS_NOT_NULL -> {
                        val pA: RexNode = (e as RexCall).getOperands().get(0)
                        return if (!RexUtil.isReferenceOrAccess(pA, true)) {
                            null
                        } else IsPredicate(pA, e.getKind())
                    }
                    else -> {}
                }
                return null
            }
        }
    }

    /**
     * Combines predicates AND, optimizes, and returns null if the result is
     * always false.
     *
     *
     * The expression is simplified on the assumption that an UNKNOWN value
     * is always treated as FALSE. Therefore the simplified expression may
     * sometimes evaluate to FALSE where the original expression would evaluate to
     * UNKNOWN.
     *
     * @param predicates Filter condition predicates
     * @return simplified conjunction of predicates for the filter, null if always false
     */
    @Nullable
    fun simplifyFilterPredicates(predicates: Iterable<RexNode?>?): RexNode? {
        val simplifiedAnds: RexNode = withPredicateElimination(Bug.CALCITE_2401_FIXED)
            .simplifyUnknownAsFalse(
                RexUtil.composeConjunction(rexBuilder, predicates)
            )
        return if (simplifiedAnds.isAlwaysFalse()) {
            null
        } else removeNullabilityCast(simplifiedAnds)

        // Remove cast of BOOLEAN NOT NULL to BOOLEAN or vice versa. Filter accepts
        // nullable and not-nullable conditions, but a CAST might get in the way of
        // other rewrites.
    }

    /** Gathers expressions that can be converted into
     * [search arguments][Sarg].  */
    internal class SargCollector(rexBuilder: RexBuilder, negate: Boolean) {
        val map: Map<RexNode, RexSargBuilder> = HashMap()
        private val rexBuilder: RexBuilder
        private val negate: Boolean

        /**
         * Count of the new terms after converting all the operands to
         * `SEARCH` on a [Sarg]. It is used to decide whether
         * the new terms are simpler.
         */
        private var newTermsCount = 0

        init {
            this.rexBuilder = rexBuilder
            this.negate = negate
        }

        fun accept(term: RexNode, newTerms: List<RexNode>) {
            if (!accept_(term, newTerms)) {
                newTerms.add(term)
            }
            newTermsCount = newTerms.size()
        }

        private fun accept_(e: RexNode, newTerms: List<RexNode>): Boolean {
            return when (e.getKind()) {
                LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, EQUALS, NOT_EQUALS, SEARCH -> accept2(
                    (e as RexCall).operands.get(0),
                    (e as RexCall).operands.get(1), e.getKind(), newTerms
                )
                IS_NULL, IS_NOT_NULL -> {
                    val arg: RexNode = (e as RexCall).operands.get(0)
                    accept1(arg, e.getKind(), newTerms)
                }
                else -> false
            }
        }

        private fun accept2(
            left: RexNode, right: RexNode, kind: SqlKind,
            newTerms: List<RexNode>
        ): Boolean {
            when (left.getKind()) {
                INPUT_REF, FIELD_ACCESS -> {
                    when (right.getKind()) {
                        LITERAL -> return accept2b(left, kind, right as RexLiteral, newTerms)
                        else -> {}
                    }
                    return false
                }
                LITERAL -> {
                    when (right.getKind()) {
                        INPUT_REF, FIELD_ACCESS -> return accept2b(right, kind.reverse(), left as RexLiteral, newTerms)
                        else -> {}
                    }
                    return false
                }
                else -> {}
            }
            return false
        }

        // always returns true
        private fun accept1(e: RexNode, kind: SqlKind, newTerms: List<RexNode>): Boolean {
            val b: RexSargBuilder =
                map.computeIfAbsent(e) { e2 -> addFluent(newTerms, RexSargBuilder(e2, rexBuilder, negate)) }
            return when (if (negate) kind.negate() else kind) {
                IS_NULL -> {
                    b.nullAs = b.nullAs.or(TRUE)
                    true
                }
                IS_NOT_NULL -> {
                    b.nullAs = b.nullAs.or(FALSE)
                    b.addAll()
                    true
                }
                else -> throw AssertionError("unexpected $kind")
            }
        }

        private fun accept2b(
            e: RexNode, kind: SqlKind,
            literal: RexLiteral, newTerms: List<RexNode>
        ): Boolean {
            var kind: SqlKind = kind
            if (literal.getValue() == null) {
                // Cannot include expressions 'x > NULL' in a Sarg. Comparing to a NULL
                // literal is a daft thing to do, because it always returns UNKNOWN. It
                // is better handled by other simplifications.
                return false
            }
            val b: RexSargBuilder =
                map.computeIfAbsent(e) { e2 -> addFluent(newTerms, RexSargBuilder(e2, rexBuilder, negate)) }
            if (negate) {
                kind = kind.negateNullSafe()
            }
            val value: Comparable = requireNonNull(literal.getValueAs(Comparable::class.java), "value")
            return when (kind) {
                LESS_THAN -> {
                    b.addRange(Range.lessThan(value), literal.getType())
                    true
                }
                LESS_THAN_OR_EQUAL -> {
                    b.addRange(Range.atMost(value), literal.getType())
                    true
                }
                GREATER_THAN -> {
                    b.addRange(Range.greaterThan(value), literal.getType())
                    true
                }
                GREATER_THAN_OR_EQUAL -> {
                    b.addRange(Range.atLeast(value), literal.getType())
                    true
                }
                EQUALS -> {
                    b.addRange(Range.singleton(value), literal.getType())
                    true
                }
                NOT_EQUALS -> {
                    b.addRange(Range.lessThan(value), literal.getType())
                    b.addRange(Range.greaterThan(value), literal.getType())
                    true
                }
                SEARCH -> {
                    val sarg: Sarg = value as Sarg
                    b.addSarg(sarg, negate, literal.getType())
                    true
                }
                else -> throw AssertionError("unexpected $kind")
            }
        }

        /** Returns whether it is worth to fix and convert to `SEARCH` calls.  */
        fun needToFix(): Boolean {
            // Fix and converts to SEARCH if:
            // 1. A Sarg has complexity greater than 1;
            // 2. The terms are reduced as simpler Sarg points;
            // 3. The terms are reduced as simpler Sarg comparison.

            // Ignore 'negate' just to be compatible with previous versions of this
            // method. "build().complexity()" would be a better estimate, if we could
            // switch to it breaking lots of plans.
            val builders: Collection<RexSargBuilder> = map.values()
            return (builders.stream().anyMatch { b -> b.build(false).complexity() > 1 }
                    || newTermsCount == 1
                    && builders.stream().allMatch { b -> simpleSarg(b.build()) })
        }

        companion object {
            private fun <E> addFluent(list: List<in E>, e: E): E {
                list.add(e)
                return e
            }

            /**
             * Returns whether this Sarg can be expanded to more simple form, e.g.
             * the IN call or single comparison.
             */
            private fun simpleSarg(sarg: Sarg): Boolean {
                return sarg.isPoints() || RangeSets.isOpenInterval(sarg.rangeSet)
            }

            /** If a term is a call to `SEARCH` on a [RexSargBuilder],
             * converts it to a `SEARCH` on a [Sarg].  */
            fun fix(
                rexBuilder: RexBuilder, term: RexNode,
                unknownAs: RexUnknownAs?
            ): RexNode {
                if (term is RexSargBuilder) {
                    val sargBuilder = term
                    val sarg: Sarg = sargBuilder.build<Comparable<C>>()
                    return if (sarg.complexity() <= 1 && simpleSarg(sarg)) {
                        // Expand small sargs into comparisons in order to avoid plan changes
                        // and better readability.
                        RexUtil.sargRef(
                            rexBuilder, sargBuilder.ref, sarg,
                            term.getType(), unknownAs
                        )
                    } else rexBuilder.makeCall(
                        SqlStdOperatorTable.SEARCH, sargBuilder.ref,
                        rexBuilder.makeSearchArgumentLiteral(sarg, term.getType())
                    )
                }
                return term
            }
        }
    }

    /** Equivalent to a [RexLiteral] whose value is a [Sarg],
     * but mutable, so that the Sarg can be expanded as [SargCollector]
     * traverses a list of OR or AND terms.
     *
     *
     * The [SargCollector.fix] method converts it to an immutable
     * literal.
     *
     *
     * The [.nullAs] field will become [Sarg.nullAs], as follows:
     *
     *
     *  * If there is at least one term that returns TRUE when the argument
     * is NULL, then the overall value will be TRUE; failing that,
     *  * if there is at least one term that returns UNKNOWN when the argument
     * is NULL, then the overall value will be UNKNOWN; failing that,
     *  * the value will be FALSE.
     *
     *
     *
     * This is analogous to the behavior of OR in three-valued logic:
     * `TRUE OR UNKNOWN OR FALSE` returns `TRUE`;
     * `UNKNOWN OR FALSE OR UNKNOWN` returns `UNKNOWN`;
     * `FALSE OR FALSE` returns `FALSE`.  */
    @SuppressWarnings("BetaApi")
    private class RexSargBuilder internal constructor(ref: RexNode?, rexBuilder: RexBuilder?, negate: Boolean) :
        RexNode() {
        val ref: RexNode
        val rexBuilder: RexBuilder
        val negate: Boolean
        val types: List<RelDataType> = ArrayList()
        val rangeSet: RangeSet<Comparable> = TreeRangeSet.create()
        var nullAs: RexUnknownAs = FALSE

        init {
            this.ref = requireNonNull(ref, "ref")
            this.rexBuilder = requireNonNull(rexBuilder, "rexBuilder")
            this.negate = negate
        }

        @Override
        override fun toString(): String {
            return ("SEARCH(" + ref + ", " + (if (negate) "NOT " else "") + rangeSet
                    + "; NULL AS " + nullAs + ")")
        }

        fun <C : Comparable<C>?> build(): Sarg<C> {
            return build<Comparable<C>>(negate)
        }

        @SuppressWarnings(["rawtypes", "unchecked", "UnstableApiUsage"])
        fun <C : Comparable<C>?> build(negate: Boolean): Sarg<C> {
            val r: RangeSet<C> = rangeSet as RangeSet
            return if (negate) {
                Sarg.of(nullAs.negate(), r.complement())
            } else {
                Sarg.of(nullAs, r)
            }
        }

        // Expression is "x IS NULL"
        @get:Override
        override val type: RelDataType
            get() {
                if (types.isEmpty()) {
                    // Expression is "x IS NULL"
                    return ref.getType()
                }
                val distinctTypes: List<RelDataType> = Util.distinctList(types)
                return requireNonNull(
                    rexBuilder.typeFactory.leastRestrictive(distinctTypes)
                ) { "Can't find leastRestrictive type among $distinctTypes" }
            }

        @Override
        override fun <R> accept(visitor: RexVisitor<R>?): R {
            throw UnsupportedOperationException()
        }

        @Override
        override fun <R, P> accept(visitor: RexBiVisitor<R, P>?, arg: P): R {
            throw UnsupportedOperationException()
        }

        @Override
        override fun equals(@Nullable obj: Object?): Boolean {
            throw UnsupportedOperationException()
        }

        @Override
        override fun hashCode(): Int {
            throw UnsupportedOperationException()
        }

        fun addAll() {
            rangeSet.add(Range.all())
        }

        fun addRange(range: Range<Comparable?>?, type: RelDataType?) {
            types.add(type)
            rangeSet.add(range)
            nullAs = nullAs.or(UNKNOWN)
        }

        @SuppressWarnings(["UnstableApiUsage", "rawtypes", "unchecked"])
        fun addSarg(sarg: Sarg, negate: Boolean, type: RelDataType?) {
            val r: RangeSet
            val nullAs: RexUnknownAs
            if (negate) {
                r = sarg.rangeSet.complement()
                nullAs = sarg.nullAs.negate()
            } else {
                r = sarg.rangeSet
                nullAs = sarg.nullAs
            }
            types.add(type)
            rangeSet.addAll(r)
            when (nullAs) {
                TRUE -> this.nullAs = this.nullAs.or(TRUE)
                FALSE -> this.nullAs = this.nullAs.or(FALSE)
                UNKNOWN -> this.nullAs = this.nullAs.or(UNKNOWN)
            }
        }
    }

    companion object {
        /**
         * Try to find a literal with the given value in the input list.
         * The type of the literal must be one of the numeric types.
         */
        private fun findLiteralIndex(operands: List<RexNode>, value: BigDecimal): Int {
            for (i in 0 until operands.size()) {
                if (operands[i].isA(SqlKind.LITERAL)) {
                    val comparable: Comparable = (operands[i] as RexLiteral).getValue()
                    if (comparable is BigDecimal
                        && value.compareTo(comparable as BigDecimal) === 0
                    ) {
                        return i
                    }
                }
            }
            return -1
        }

        /**
         * Validates strong policy for specified [RexNode].
         *
         * @param rexNode Rex node to validate the strong policy
         * @throws AssertionError If the validation fails
         */
        private fun validateStrongPolicy(rexNode: RexNode) {
            if (hasCustomNullabilityRules(rexNode.getKind())) {
                return
            }
            when (Strong.policy(rexNode)) {
                NOT_NULL -> assert(!rexNode.getType().isNullable())
                ANY -> {
                    val operands: List<RexNode> = (rexNode as RexCall).getOperands()
                    if (rexNode.getType().isNullable()) {
                        assert(
                            operands.stream()
                                .map(RexNode::getType)
                                .anyMatch(RelDataType::isNullable)
                        )
                    } else {
                        assert(
                            operands.stream()
                                .map(RexNode::getType)
                                .noneMatch(RelDataType::isNullable)
                        )
                    }
                }
                else -> {}
            }
        }

        /**
         * Returns `true` if specified [SqlKind] has custom nullability rules which
         * depend not only on the nullability of input operands.
         *
         *
         * For example, CAST may be used to change the nullability of its operand type,
         * so it may be nullable, though the argument type was non-nullable.
         *
         * @param sqlKind Sql kind to check
         * @return `true` if specified [SqlKind] has custom nullability rules
         */
        private fun hasCustomNullabilityRules(sqlKind: SqlKind): Boolean {
            return when (sqlKind) {
                CAST, ITEM -> true
                else -> false
            }
        }

        /**
         * If boolean is true, simplify cond in input branch and return new branch.
         * Otherwise, simply return input branch.
         */
        private fun generateBranch(
            simplifyCond: Boolean, simplifier: RexSimplify,
            branch: CaseBranch
        ): CaseBranch {
            return if (simplifyCond) {
                // the previous branch was merged, time to simplify it and
                // add it to the final result
                CaseBranch(
                    simplifier.simplify(branch.cond, RexUnknownAs.FALSE), branch.value
                )
            } else branch
        }

        /** Analyzes a given [RexNode] and decides whenever it is safe to
         * unwind.
         *
         *
         * "Safe" means that it only contains a combination of known good operators.
         *
         *
         * Division is an unsafe operator; consider the following:
         * <pre>case when a &gt; 0 then 1 / a else null end</pre>
         */
        fun isSafeExpression(r: RexNode): Boolean {
            return r.accept(SafeRexVisitor.INSTANCE)
        }

        /**
         * Generic boolean case simplification.
         *
         *
         * Rewrites:
         * <pre>
         * CASE
         * WHEN p1 THEN x
         * WHEN p2 THEN y
         * ELSE z
         * END
        </pre> *
         * to
         * <pre>(p1 and x) or (p2 and y and not(p1)) or (true and z and not(p1) and not(p2))</pre>
         */
        private fun simplifyBooleanCaseGeneric(
            rexBuilder: RexBuilder,
            branches: List<CaseBranch>
        ): RexNode {
            val booleanBranches: Boolean = branches.stream()
                .allMatch { branch -> branch.value.isAlwaysTrue() || branch.value.isAlwaysFalse() }
            val terms: List<RexNode> = ArrayList()
            val notTerms: List<RexNode> = ArrayList()
            for (branch in branches) {
                val useBranch: Boolean = !branch.value.isAlwaysFalse()
                if (useBranch) {
                    val branchTerm: RexNode
                    branchTerm = if (branch.value.isAlwaysTrue()) {
                        branch.cond
                    } else {
                        rexBuilder.makeCall(SqlStdOperatorTable.AND, branch.cond, branch.value)
                    }
                    terms.add(RexUtil.andNot(rexBuilder, branchTerm, notTerms))
                }
                if (booleanBranches && useBranch) {
                    // we are safe to ignore this branch because for boolean true branches:
                    // a || (b && !a) === a || b
                } else {
                    notTerms.add(branch.cond)
                }
            }
            return RexUtil.composeDisjunction(rexBuilder, terms)
        }

        /** Weakens a term so that it checks only what is not implied by predicates.
         *
         *
         * The term is broken into "ref comparison constant",
         * for example "$0 &lt; 5".
         *
         *
         * Examples:
         *
         *
         *  * `residue($0 < 10, [$0 < 5])` returns `true`
         *
         *  * `residue($0 < 10, [$0 < 20, $0 > 0])` returns `$0 < 10`
         *
         */
        @SuppressWarnings("BetaApi")
        private fun <C : Comparable<C>?> residue(
            ref: RexNode,
            r0: RangeSet<C>, predicates: List<RexNode>, clazz: Class<C>
        ): RangeSet<C> {
            var result: RangeSet<C> = r0
            for (predicate in predicates) {
                when (predicate.getKind()) {
                    EQUALS, NOT_EQUALS, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL -> {
                        val call: RexCall = predicate as RexCall
                        if (call.operands.get(0).equals(ref)
                            && call.operands.get(1) is RexLiteral
                        ) {
                            val literal: RexLiteral = call.operands.get(1)
                            val c1: C = literal.getValueAs(clazz)
                            assert(c1 != null) { "value must not be null in $literal" }
                            when (predicate.getKind()) {
                                NOT_EQUALS -> {
                                    // We want to intersect result with the range set of everything but
                                    // c1. We subtract the point c1 from result, which is equivalent.
                                    val pointRange: Range<C> = range(SqlKind.EQUALS, c1)
                                    val notEqualsRangeSet: RangeSet<C> = ImmutableRangeSet.of(pointRange).complement()
                                    if (result.enclosesAll(notEqualsRangeSet)) {
                                        result = RangeSets.rangeSetAll()
                                        continue
                                    }
                                    result = RangeSets.minus(result, pointRange)
                                }
                                else -> {
                                    val r1: Range<C> = range(predicate.getKind(), c1)
                                    if (result.encloses(r1)) {
                                        // Given these predicates, term is always satisfied.
                                        // e.g. r0 is "$0 < 10", r1 is "$0 < 5"
                                        result = RangeSets.rangeSetAll()
                                        continue
                                    }
                                    result = result.subRangeSet(r1)
                                }
                            }
                            if (result.isEmpty()) {
                                break // short-cut
                            }
                        }
                    }
                    else -> {}
                }
            }
            return result
        }

        /** Method that returns whether we can rollup from inner time unit
         * to outer time unit.  */
        private fun canRollUp(outer: TimeUnit, inner: TimeUnit): Boolean {
            // Special handling for QUARTER as it is not in the expected
            // order in TimeUnit
            when (outer) {
                YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND -> when (inner) {
                    YEAR, QUARTER, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND -> {
                        return if (inner === TimeUnit.QUARTER) {
                            outer === TimeUnit.YEAR
                        } else outer.ordinal() <= inner.ordinal()
                    }
                    else -> {}
                }
                QUARTER -> when (inner) {
                    QUARTER, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND -> return true
                    else -> {}
                }
                else -> {}
            }
            return false
        }

        private fun <C : Comparable<C>?> processRange(
            rexBuilder: RexBuilder, terms: List<RexNode>,
            rangeTerms: Map<RexNode, Pair<Range<C>, List<RexNode>>>, term: RexNode,
            ref: RexNode, v0: C, comparison: SqlKind
        ): @Nullable RexNode? {
            val p: Pair<Range<C>, List<RexNode>>? = rangeTerms[ref]
            if (p == null) {
                rangeTerms.put(
                    ref,
                    Pair.of(
                        range(comparison, v0),
                        ImmutableList.of(term)
                    )
                )
            } else {
                // Exists
                var removeUpperBound = false
                var removeLowerBound = false
                var r: Range<C> = p.left
                val trueLiteral: RexLiteral = rexBuilder.makeLiteral(true)
                when (comparison) {
                    EQUALS -> {
                        if (!r.contains(v0)) {
                            // Range is empty, not satisfiable
                            return rexBuilder.makeLiteral(false)
                        }
                        rangeTerms.put(
                            ref,
                            Pair.of(
                                Range.singleton(v0),
                                ImmutableList.of(term)
                            )
                        )
                        // remove
                        for (e in p.right) {
                            replaceLast<Any>(terms, e, trueLiteral)
                        }
                    }
                    LESS_THAN -> {
                        var comparisonResult = 0
                        if (r.hasUpperBound()) {
                            comparisonResult = v0!!.compareTo(r.upperEndpoint())
                        }
                        if (comparisonResult <= 0) {
                            // 1) No upper bound, or
                            // 2) We need to open the upper bound, or
                            // 3) New upper bound is lower than old upper bound
                            r = if (r.hasLowerBound()) {
                                if (v0!!.compareTo(r.lowerEndpoint()) <= 0) {
                                    // Range is empty, not satisfiable
                                    return rexBuilder.makeLiteral(false)
                                }
                                // a <= x < b OR a < x < b
                                Range.range(
                                    r.lowerEndpoint(), r.lowerBoundType(),
                                    v0, BoundType.OPEN
                                )
                            } else {
                                // x < b
                                Range.lessThan(v0)
                            }
                            if (r.isEmpty()) {
                                // Range is empty, not satisfiable
                                return rexBuilder.makeLiteral(false)
                            }

                            // remove prev upper bound
                            removeUpperBound = true
                        } else {
                            // Remove this term as it is contained in current upper bound
                            replaceLast<Any>(terms, term, trueLiteral)
                        }
                    }
                    LESS_THAN_OR_EQUAL -> {
                        var comparisonResult = -1
                        if (r.hasUpperBound()) {
                            comparisonResult = v0!!.compareTo(r.upperEndpoint())
                        }
                        if (comparisonResult < 0) {
                            // 1) No upper bound, or
                            // 2) New upper bound is lower than old upper bound
                            r = if (r.hasLowerBound()) {
                                if (v0!!.compareTo(r.lowerEndpoint()) < 0) {
                                    // Range is empty, not satisfiable
                                    return rexBuilder.makeLiteral(false)
                                }
                                // a <= x <= b OR a < x <= b
                                Range.range(
                                    r.lowerEndpoint(), r.lowerBoundType(),
                                    v0, BoundType.CLOSED
                                )
                            } else {
                                // x <= b
                                Range.atMost(v0)
                            }
                            if (r.isEmpty()) {
                                // Range is empty, not satisfiable
                                return rexBuilder.makeLiteral(false)
                            }

                            // remove prev upper bound
                            removeUpperBound = true
                        } else {
                            // Remove this term as it is contained in current upper bound
                            replaceLast<Any>(terms, term, trueLiteral)
                        }
                    }
                    GREATER_THAN -> {
                        var comparisonResult = 0
                        if (r.hasLowerBound()) {
                            comparisonResult = v0!!.compareTo(r.lowerEndpoint())
                        }
                        if (comparisonResult >= 0) {
                            // 1) No lower bound, or
                            // 2) We need to open the lower bound, or
                            // 3) New lower bound is greater than old lower bound
                            r = if (r.hasUpperBound()) {
                                if (v0!!.compareTo(r.upperEndpoint()) >= 0) {
                                    // Range is empty, not satisfiable
                                    return rexBuilder.makeLiteral(false)
                                }
                                // a < x <= b OR a < x < b
                                Range.range(
                                    v0, BoundType.OPEN,
                                    r.upperEndpoint(), r.upperBoundType()
                                )
                            } else {
                                // x > a
                                Range.greaterThan(v0)
                            }
                            if (r.isEmpty()) {
                                // Range is empty, not satisfiable
                                return rexBuilder.makeLiteral(false)
                            }

                            // remove prev lower bound
                            removeLowerBound = true
                        } else {
                            // Remove this term as it is contained in current lower bound
                            replaceLast<Any>(terms, term, trueLiteral)
                        }
                    }
                    GREATER_THAN_OR_EQUAL -> {
                        var comparisonResult = 1
                        if (r.hasLowerBound()) {
                            comparisonResult = v0!!.compareTo(r.lowerEndpoint())
                        }
                        if (comparisonResult > 0) {
                            // 1) No lower bound, or
                            // 2) New lower bound is greater than old lower bound
                            r = if (r.hasUpperBound()) {
                                if (v0!!.compareTo(r.upperEndpoint()) > 0) {
                                    // Range is empty, not satisfiable
                                    return rexBuilder.makeLiteral(false)
                                }
                                // a <= x <= b OR a <= x < b
                                Range.range(
                                    v0, BoundType.CLOSED,
                                    r.upperEndpoint(), r.upperBoundType()
                                )
                            } else {
                                // x >= a
                                Range.atLeast(v0)
                            }
                            if (r.isEmpty()) {
                                // Range is empty, not satisfiable
                                return rexBuilder.makeLiteral(false)
                            }

                            // remove prev lower bound
                            removeLowerBound = true
                        } else {
                            // Remove this term as it is contained in current lower bound
                            replaceLast<Any>(terms, term, trueLiteral)
                        }
                    }
                    else -> throw AssertionError()
                }
                if (removeUpperBound) {
                    val newBounds: ImmutableList.Builder<RexNode> = ImmutableList.builder()
                    for (e in p.right) {
                        if (isUpperBound(e)) {
                            replaceLast<Any>(terms, e, trueLiteral)
                        } else {
                            newBounds.add(e)
                        }
                    }
                    newBounds.add(term)
                    rangeTerms.put(
                        ref,
                        Pair.of(r, newBounds.build())
                    )
                } else if (removeLowerBound) {
                    val newBounds: ImmutableList.Builder<RexNode> = ImmutableList.builder()
                    for (e in p.right) {
                        if (isLowerBound(e)) {
                            replaceLast<Any>(terms, e, trueLiteral)
                        } else {
                            newBounds.add(e)
                        }
                    }
                    newBounds.add(term)
                    rangeTerms.put(
                        ref,
                        Pair.of(r, newBounds.build())
                    )
                }
            }
            // Default
            return null
        }

        private fun <C : Comparable<C>?> range(
            comparison: SqlKind,
            c: C?
        ): Range<C> {
            return when (comparison) {
                EQUALS -> Range.singleton(c)
                LESS_THAN -> Range.lessThan(c)
                LESS_THAN_OR_EQUAL -> Range.atMost(c)
                GREATER_THAN -> Range.greaterThan(c)
                GREATER_THAN_OR_EQUAL -> Range.atLeast(c)
                else -> throw AssertionError()
            }
        }

        @SuppressWarnings("BetaApi")
        private fun <C : Comparable<C>?> rangeSet(
            comparison: SqlKind,
            c: C
        ): RangeSet<C> {
            return when (comparison) {
                EQUALS, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL -> ImmutableRangeSet.of(
                    range(comparison, c)
                )
                NOT_EQUALS -> ImmutableRangeSet.< C > builder < C ? > ()
                    .add(range(SqlKind.LESS_THAN, c))
                    .add(range(SqlKind.GREATER_THAN, c))
                    .build()
                else -> throw AssertionError()
            }
        }

        private fun isUpperBound(e: RexNode): Boolean {
            val operands: List<RexNode>
            return when (e.getKind()) {
                LESS_THAN, LESS_THAN_OR_EQUAL -> {
                    operands = (e as RexCall).getOperands()
                    (RexUtil.isReferenceOrAccess(operands[0], true)
                            && operands[1].isA(SqlKind.LITERAL))
                }
                GREATER_THAN, GREATER_THAN_OR_EQUAL -> {
                    operands = (e as RexCall).getOperands()
                    (RexUtil.isReferenceOrAccess(operands[1], true)
                            && operands[0].isA(SqlKind.LITERAL))
                }
                else -> false
            }
        }

        private fun isLowerBound(e: RexNode): Boolean {
            val operands: List<RexNode>
            return when (e.getKind()) {
                LESS_THAN, LESS_THAN_OR_EQUAL -> {
                    operands = (e as RexCall).getOperands()
                    (RexUtil.isReferenceOrAccess(operands[1], true)
                            && operands[0].isA(SqlKind.LITERAL))
                }
                GREATER_THAN, GREATER_THAN_OR_EQUAL -> {
                    operands = (e as RexCall).getOperands()
                    (RexUtil.isReferenceOrAccess(operands[0], true)
                            && operands[1].isA(SqlKind.LITERAL))
                }
                else -> false
            }
        }

        /**
         * Replaces the last occurrence of one specified value in a list with
         * another.
         *
         *
         * Does not change the size of the list.
         *
         *
         * Returns whether the value was found.
         */
        private fun <E> replaceLast(list: List<E>, oldVal: E, newVal: E): Boolean {
            @SuppressWarnings("argument.type.incompatible") val index = list.lastIndexOf(oldVal)
            if (index < 0) {
                return false
            }
            list.set(index, newVal)
            return true
        }
    }
}
