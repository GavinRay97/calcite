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

import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexUtil
import org.apache.calcite.sql.SqlKind
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import java.util.Collection
import java.util.Objects

/**
 * Predicates that are known to hold in the output of a particular relational
 * expression.
 *
 *
 * **Pulled up predicates** (field [.pulledUpPredicates] are
 * predicates that apply to every row output by the relational expression. They
 * are inferred from the input relational expression(s) and the relational
 * operator.
 *
 *
 * For example, if you apply `Filter(x > 1)` to a relational
 * expression that has a predicate `y < 10` then the pulled up predicates
 * for the Filter are `[y < 10, x > 1]`.
 *
 *
 * **Inferred predicates** only apply to joins. If there there is a
 * predicate on the left input to a join, and that predicate is over columns
 * used in the join condition, then a predicate can be inferred on the right
 * input to the join. (And vice versa.)
 *
 *
 * For example, in the query
 * <blockquote>SELECT *<br></br>
 * FROM emp<br></br>
 * JOIN dept ON emp.deptno = dept.deptno
 * WHERE emp.gender = 'F' AND emp.deptno &lt; 10</blockquote>
 * we have
 *
 *  * left: `Filter(Scan(EMP), deptno < 10`,
 * predicates: `[deptno < 10]`
 *  * right: `Scan(DEPT)`, predicates: `[]`
 *  * join: `Join(left, right, emp.deptno = dept.deptno`,
 * leftInferredPredicates: [],
 * rightInferredPredicates: [deptno &lt; 10],
 * pulledUpPredicates: [emp.gender = 'F', emp.deptno &lt; 10,
 * emp.deptno = dept.deptno, dept.deptno &lt; 10]
 *
 *
 *
 * Note that the predicate from the left input appears in
 * `rightInferredPredicates`. Predicates from several sources appear in
 * `pulledUpPredicates`.
 */
class RelOptPredicateList private constructor(
    pulledUpPredicates: ImmutableList<RexNode>,
    leftInferredPredicates: ImmutableList<RexNode>,
    rightInferredPredicates: ImmutableList<RexNode>,
    constantMap: ImmutableMap<RexNode, RexNode>
) {
    /** Predicates that can be pulled up from the relational expression and its
     * inputs.  */
    val pulledUpPredicates: ImmutableList<RexNode>

    /** Predicates that were inferred from the right input.
     * Empty if the relational expression is not a join.  */
    val leftInferredPredicates: ImmutableList<RexNode>

    /** Predicates that were inferred from the left input.
     * Empty if the relational expression is not a join.  */
    val rightInferredPredicates: ImmutableList<RexNode>

    /** A map of each (e, constant) pair that occurs within
     * [.pulledUpPredicates].  */
    val constantMap: ImmutableMap<RexNode, RexNode>

    init {
        this.pulledUpPredicates = Objects.requireNonNull(pulledUpPredicates, "pulledUpPredicates")
        this.leftInferredPredicates = Objects.requireNonNull(leftInferredPredicates, "leftInferredPredicates")
        this.rightInferredPredicates = Objects.requireNonNull(rightInferredPredicates, "rightInferredPredicates")
        this.constantMap = Objects.requireNonNull(constantMap, "constantMap")
    }

    @Override
    override fun toString(): String {
        val b = StringBuilder("{")
        append(b, "pulled", pulledUpPredicates)
        append(b, "left", leftInferredPredicates)
        append(b, "right", rightInferredPredicates)
        append(b, "constants", constantMap.entrySet())
        return b.append("}").toString()
    }

    fun union(
        rexBuilder: RexBuilder?,
        list: RelOptPredicateList
    ): RelOptPredicateList {
        return if (this === EMPTY) {
            list
        } else if (list === EMPTY) {
            this
        } else {
            of(
                rexBuilder,
                concat<Any>(
                    pulledUpPredicates,
                    list.pulledUpPredicates
                ),
                concat<Any>(
                    leftInferredPredicates,
                    list.leftInferredPredicates
                ),
                concat<Any>(
                    rightInferredPredicates,
                    list.rightInferredPredicates
                )
            )
        }
    }

    fun shift(rexBuilder: RexBuilder?, offset: Int): RelOptPredicateList {
        return of(
            rexBuilder,
            RexUtil.shift(pulledUpPredicates, offset),
            RexUtil.shift(leftInferredPredicates, offset),
            RexUtil.shift(rightInferredPredicates, offset)
        )
    }

    /** Returns whether an expression is effectively NOT NULL due to an
     * `e IS NOT NULL` condition in this predicate list.  */
    fun isEffectivelyNotNull(e: RexNode): Boolean {
        if (!e.getType().isNullable()) {
            return true
        }
        for (p in pulledUpPredicates) {
            if (p.getKind() === SqlKind.IS_NOT_NULL
                && (p as RexCall).getOperands().get(0).equals(e)
            ) {
                return true
            }
        }
        if (SqlKind.COMPARISON.contains(e.getKind())) {
            // A comparison with a (non-null) literal, such as 'ref < 10', is not null if 'ref'
            // is not null.
            val call: RexCall = e as RexCall
            if (call.getOperands().get(1) is RexLiteral
                && !(call.getOperands().get(1) as RexLiteral).isNull()
            ) {
                return isEffectivelyNotNull(call.getOperands().get(0))
            }
        }
        return false
    }

    companion object {
        private val EMPTY_LIST: ImmutableList<RexNode> = ImmutableList.of()
        val EMPTY = RelOptPredicateList(
            EMPTY_LIST, EMPTY_LIST, EMPTY_LIST,
            ImmutableMap.of()
        )

        /** Creates a RelOptPredicateList with only pulled-up predicates, no inferred
         * predicates.
         *
         *
         * Use this for relational expressions other than joins.
         *
         * @param pulledUpPredicates Predicates that apply to the rows returned by the
         * relational expression
         */
        fun of(
            rexBuilder: RexBuilder?,
            pulledUpPredicates: Iterable<RexNode?>?
        ): RelOptPredicateList {
            val pulledUpPredicatesList: ImmutableList<RexNode?> = ImmutableList.copyOf(pulledUpPredicates)
            return if (pulledUpPredicatesList.isEmpty()) {
                EMPTY
            } else of(
                rexBuilder,
                pulledUpPredicatesList,
                EMPTY_LIST,
                EMPTY_LIST
            )
        }

        /**
         * Returns true if given predicate list is empty.
         * @param value input predicate list
         * @return true if all the predicates are empty or if the argument is null
         */
        fun isEmpty(@Nullable value: RelOptPredicateList?): Boolean {
            return if (value == null || value === EMPTY) {
                true
            } else value.constantMap.isEmpty()
                    && value.leftInferredPredicates.isEmpty()
                    && value.rightInferredPredicates.isEmpty()
                    && value.pulledUpPredicates.isEmpty()
        }

        /** Creates a RelOptPredicateList for a join.
         *
         * @param rexBuilder Rex builder
         * @param pulledUpPredicates Predicates that apply to the rows returned by the
         * relational expression
         * @param leftInferredPredicates Predicates that were inferred from the right
         * input
         * @param rightInferredPredicates Predicates that were inferred from the left
         * input
         */
        fun of(
            rexBuilder: RexBuilder?,
            pulledUpPredicates: Iterable<RexNode?>?,
            leftInferredPredicates: Iterable<RexNode?>?,
            rightInferredPredicates: Iterable<RexNode?>?
        ): RelOptPredicateList {
            val pulledUpPredicatesList: ImmutableList<RexNode> = ImmutableList.copyOf(pulledUpPredicates)
            val leftInferredPredicateList: ImmutableList<RexNode> = ImmutableList.copyOf(leftInferredPredicates)
            val rightInferredPredicatesList: ImmutableList<RexNode> = ImmutableList.copyOf(rightInferredPredicates)
            if (pulledUpPredicatesList.isEmpty()
                && leftInferredPredicateList.isEmpty()
                && rightInferredPredicatesList.isEmpty()
            ) {
                return EMPTY
            }
            val constantMap: ImmutableMap<RexNode, RexNode> = RexUtil.predicateConstants(
                RexNode::class.java, rexBuilder,
                pulledUpPredicatesList
            )
            return RelOptPredicateList(
                pulledUpPredicatesList,
                leftInferredPredicateList, rightInferredPredicatesList, constantMap
            )
        }

        private fun append(b: StringBuilder, key: String, value: Collection<*>) {
            if (!value.isEmpty()) {
                if (b.length() > 1) {
                    b.append(", ")
                }
                b.append(key)
                b.append(value)
            }
        }

        /** Concatenates two immutable lists, avoiding a copy it possible.  */
        private fun <E> concat(
            list1: ImmutableList<E>,
            list2: ImmutableList<E>
        ): ImmutableList<E> {
            return if (list1.isEmpty()) {
                list2
            } else if (list2.isEmpty()) {
                list1
            } else {
                ImmutableList.< E > builder < E ? > ().addAll(list1).addAll(list2).build()
            }
        }
    }
}
