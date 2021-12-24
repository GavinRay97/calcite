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

import org.apache.calcite.plan.RelOptRuleCall

/**
 * Transform that converts IN, EXISTS and scalar sub-queries into joins.
 *
 *
 * Sub-queries are represented by [RexSubQuery] expressions.
 *
 *
 * A sub-query may or may not be correlated. If a sub-query is correlated,
 * the wrapped [RelNode] will contain a [RexCorrelVariable] before
 * the rewrite, and the product of the rewrite will be a [Correlate].
 * The Correlate can be removed using [RelDecorrelator].
 *
 * @see CoreRules.FILTER_SUB_QUERY_TO_CORRELATE
 *
 * @see CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE
 *
 * @see CoreRules.JOIN_SUB_QUERY_TO_CORRELATE
 */
@Value.Enclosing
class SubQueryRemoveRule protected constructor(config: Config) : RelRule<SubQueryRemoveRule.Config?>(config),
    TransformationRule {
    /** Creates a SubQueryRemoveRule.  */
    init {
        Objects.requireNonNull(config.matchHandler())
    }

    @Override
    fun onMatch(call: RelOptRuleCall?) {
        config.matchHandler().accept(this, call)
    }

    protected fun apply(
        e: RexSubQuery?, variablesSet: Set<CorrelationId>,
        logic: RelOptUtil.Logic,
        builder: RelBuilder, inputCount: Int, offset: Int
    ): RexNode {
        return when (e.getKind()) {
            SCALAR_QUERY -> rewriteScalarQuery(
                e,
                variablesSet,
                builder,
                inputCount,
                offset
            )
            ARRAY_QUERY_CONSTRUCTOR -> rewriteCollection(
                e, SqlTypeName.ARRAY, variablesSet, builder,
                inputCount, offset
            )
            MAP_QUERY_CONSTRUCTOR -> rewriteCollection(
                e, SqlTypeName.MAP, variablesSet, builder,
                inputCount, offset
            )
            MULTISET_QUERY_CONSTRUCTOR -> rewriteCollection(
                e, SqlTypeName.MULTISET, variablesSet, builder,
                inputCount, offset
            )
            SOME -> rewriteSome(e, variablesSet, builder)
            IN -> rewriteIn(
                e,
                variablesSet,
                logic,
                builder,
                offset
            )
            EXISTS -> rewriteExists(
                e,
                variablesSet,
                logic,
                builder
            )
            UNIQUE -> rewriteUnique(e, builder)
            else -> throw AssertionError(e.getKind())
        }
    }

    /** Shuttle that replaces occurrences of a given
     * [org.apache.calcite.rex.RexSubQuery] with a replacement
     * expression.  */
    private class ReplaceSubQueryShuttle internal constructor(subQuery: RexSubQuery?, replacement: RexNode) :
        RexShuttle() {
        private val subQuery: RexSubQuery?
        private val replacement: RexNode

        init {
            this.subQuery = subQuery
            this.replacement = replacement
        }

        @Override
        fun visitSubQuery(subQuery: RexSubQuery): RexNode {
            return if (subQuery.equals(this.subQuery)) replacement else subQuery
        }
    }

    /** Rule configuration.  */
    @Value.Immutable(singleton = false)
    interface Config : RelRule.Config {
        @Override
        fun toRule(): SubQueryRemoveRule? {
            return SubQueryRemoveRule(this)
        }

        /** Forwards a call to [.onMatch].  */
        fun matchHandler(): MatchHandler<SubQueryRemoveRule?>?

        /** Sets [.matchHandler].  */
        fun withMatchHandler(matchHandler: MatchHandler<SubQueryRemoveRule?>?): Config?

        companion object {
            val PROJECT: Config = ImmutableSubQueryRemoveRule.Config.builder()
                .withMatchHandler { rule: SubQueryRemoveRule, call: RelOptRuleCall -> matchProject(rule, call) }
                .build()
                .withOperandSupplier { b ->
                    b.operand(Project::class.java)
                        .predicate(RexUtil.SubQueryFinder::containsSubQuery).anyInputs()
                }
                .withDescription("SubQueryRemoveRule:Project")
            val FILTER: Config = ImmutableSubQueryRemoveRule.Config.builder()
                .withMatchHandler { rule: SubQueryRemoveRule, call: RelOptRuleCall -> matchFilter(rule, call) }
                .build()
                .withOperandSupplier { b ->
                    b.operand(Filter::class.java)
                        .predicate(RexUtil.SubQueryFinder::containsSubQuery).anyInputs()
                }
                .withDescription("SubQueryRemoveRule:Filter")
            val JOIN: Config = ImmutableSubQueryRemoveRule.Config.builder()
                .withMatchHandler { rule: SubQueryRemoveRule, call: RelOptRuleCall -> matchJoin(rule, call) }
                .build()
                .withOperandSupplier { b ->
                    b.operand(Join::class.java)
                        .predicate(RexUtil.SubQueryFinder::containsSubQuery)
                        .anyInputs()
                }
                .withDescription("SubQueryRemoveRule:Join")
        }
    }

    companion object {
        /**
         * Rewrites a scalar sub-query into an
         * [org.apache.calcite.rel.core.Aggregate].
         *
         * @param e            Scalar sub-query to rewrite
         * @param variablesSet A set of variables used by a relational
         * expression of the specified RexSubQuery
         * @param builder      Builder
         * @param offset       Offset to shift [RexInputRef]
         *
         * @return Expression that may be used to replace the RexSubQuery
         */
        private fun rewriteScalarQuery(
            e: RexSubQuery?, variablesSet: Set<CorrelationId>,
            builder: RelBuilder, inputCount: Int, offset: Int
        ): RexNode {
            builder.push(e.rel)
            val mq: RelMetadataQuery = e.rel.getCluster().getMetadataQuery()
            val unique: Boolean = mq.areColumnsUnique(
                builder.peek(),
                ImmutableBitSet.of()
            )
            if (unique == null || !unique) {
                builder.aggregate(
                    builder.groupKey(),
                    builder.aggregateCall(
                        SqlStdOperatorTable.SINGLE_VALUE,
                        builder.field(0)
                    )
                )
            }
            builder.join(JoinRelType.LEFT, builder.literal(true), variablesSet)
            return field(builder, inputCount, offset)
        }

        /**
         * Rewrites a sub-query into a
         * [org.apache.calcite.rel.core.Collect].
         *
         * @param e            Sub-query to rewrite
         * @param collectionType Collection type (ARRAY, MAP, MULTISET)
         * @param variablesSet A set of variables used by a relational
         * expression of the specified RexSubQuery
         * @param builder      Builder
         * @param offset       Offset to shift [RexInputRef]
         * @return Expression that may be used to replace the RexSubQuery
         */
        private fun rewriteCollection(
            e: RexSubQuery?,
            collectionType: SqlTypeName, variablesSet: Set<CorrelationId>,
            builder: RelBuilder, inputCount: Int, offset: Int
        ): RexNode {
            builder.push(e.rel)
            builder.push(
                Collect.create(builder.build(), collectionType, "x")
            )
            builder.join(JoinRelType.INNER, builder.literal(true), variablesSet)
            return field(builder, inputCount, offset)
        }

        /**
         * Rewrites a SOME sub-query into a [Join].
         *
         * @param e            SOME sub-query to rewrite
         * @param builder      Builder
         *
         * @return Expression that may be used to replace the RexSubQuery
         */
        private fun rewriteSome(
            e: RexSubQuery?, variablesSet: Set<CorrelationId>,
            builder: RelBuilder
        ): RexNode {
            // Most general case, where the left and right keys might have nulls, and
            // caller requires 3-valued logic return.
            //
            // select e.deptno, e.deptno < some (select deptno from emp) as v
            // from emp as e
            //
            // becomes
            //
            // select e.deptno,
            //   case
            //   when q.c = 0 then false // sub-query is empty
            //   when (e.deptno < q.m) is true then true
            //   when q.c > q.d then unknown // sub-query has at least one null
            //   else e.deptno < q.m
            //   end as v
            // from emp as e
            // cross join (
            //   select max(deptno) as m, count(*) as c, count(deptno) as d
            //   from emp) as q
            //
            val op: SqlQuantifyOperator = e.op as SqlQuantifyOperator
            when (op.comparisonKind) {
                GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL, LESS_THAN, GREATER_THAN, NOT_EQUALS -> {}
                else -> throw AssertionError("unexpected $op")
            }
            val caseRexNode: RexNode
            val literalFalse: RexNode = builder.literal(false)
            val literalTrue: RexNode = builder.literal(true)
            val literalUnknown: RexLiteral = builder.getRexBuilder().makeNullLiteral(literalFalse.getType())
            val minMax: SqlAggFunction = if (op.comparisonKind === SqlKind.GREATER_THAN
                || op.comparisonKind === SqlKind.GREATER_THAN_OR_EQUAL
            ) SqlStdOperatorTable.MIN else SqlStdOperatorTable.MAX
            caseRexNode = if (variablesSet.isEmpty()) {
                when (op.comparisonKind) {
                    GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL, LESS_THAN, GREATER_THAN -> {
                        // for non-correlated case queries such as
                        // select e.deptno, e.deptno < some (select deptno from emp) as v
                        // from emp as e
                        //
                        // becomes
                        //
                        // select e.deptno,
                        //   case
                        //   when q.c = 0 then false // sub-query is empty
                        //   when (e.deptno < q.m) is true then true
                        //   when q.c > q.d then unknown // sub-query has at least one null
                        //   else e.deptno < q.m
                        //   end as v
                        // from emp as e
                        // cross join (
                        //   select max(deptno) as m, count(*) as c, count(deptno) as d
                        //   from emp) as q
                        builder.push(e.rel)
                            .aggregate(
                                builder.groupKey(),
                                builder.aggregateCall(minMax, builder.field(0)).`as`("m"),
                                builder.count(false, "c"),
                                builder.count(false, "d", builder.field(0))
                            )
                            .`as`("q")
                            .join(JoinRelType.INNER)
                        builder.call(
                            SqlStdOperatorTable.CASE,
                            builder.equals(builder.field("q", "c"), builder.literal(0)),
                            literalFalse,
                            builder.call(
                                SqlStdOperatorTable.IS_TRUE,
                                builder.call(
                                    RexUtil.op(op.comparisonKind),
                                    e.operands.get(0), builder.field("q", "m")
                                )
                            ),
                            literalTrue,
                            builder.greaterThan(
                                builder.field("q", "c"),
                                builder.field("q", "d")
                            ),
                            literalUnknown,
                            builder.call(
                                RexUtil.op(op.comparisonKind),
                                e.operands.get(0), builder.field("q", "m")
                            )
                        )
                    }
                    NOT_EQUALS -> {
                        // for non-correlated case queries such as
                        // select e.deptno, e.deptno <> some (select deptno from emp) as v
                        // from emp as e
                        //
                        // becomes
                        //
                        // select e.deptno,
                        //   case
                        //   when q.c = 0 then false // sub-query is empty
                        //   when e.deptno is null then unknown
                        //   when q.c <> q.d && q.d <= 1 then e.deptno != m || unknown
                        //   when q.d = 1
                        //     then e.deptno != m // sub-query has the distinct result
                        //   else true
                        //   end as v
                        // from emp as e
                        // cross join (
                        //   select count(*) as c, count(deptno) as d, max(deptno) as m
                        //   from (select distinct deptno from emp)) as q
                        builder.push(e.rel)
                        builder.distinct()
                            .aggregate(
                                builder.groupKey(),
                                builder.count(false, "c"),
                                builder.count(false, "d", builder.field(0)),
                                builder.max(builder.field(0)).`as`("m")
                            )
                            .`as`("q")
                            .join(JoinRelType.INNER)
                        builder.call(
                            SqlStdOperatorTable.CASE,
                            builder.equals(builder.field("c"), builder.literal(0)),
                            literalFalse,
                            builder.isNull(e.getOperands().get(0)),
                            literalUnknown,
                            builder.and(
                                builder.notEquals(builder.field("d"), builder.field("c")),
                                builder.lessThanOrEqual(
                                    builder.field("d"),
                                    builder.literal(1)
                                )
                            ),
                            builder.or(
                                builder.notEquals(e.operands.get(0), builder.field("q", "m")),
                                literalUnknown
                            ),
                            builder.equals(builder.field("d"), builder.literal(1)),
                            builder.notEquals(e.operands.get(0), builder.field("q", "m")),
                            literalTrue
                        )
                    }
                    else -> throw AssertionError("not possible - per above check")
                }
            } else {
                val indicator = "trueLiteral"
                val parentQueryFields: List<RexNode> = ArrayList()
                when (op.comparisonKind) {
                    GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL, LESS_THAN, GREATER_THAN -> {
                        // for correlated case queries such as
                        //
                        // select e.deptno, e.deptno < some (
                        //   select deptno from emp where emp.name = e.name) as v
                        // from emp as e
                        //
                        // becomes
                        //
                        // select e.deptno,
                        //   case
                        //   when indicator is null then false // sub-query is empty for corresponding corr value
                        //   when q.c = 0 then false // sub-query is empty
                        //   when (e.deptno < q.m) is true then true
                        //   when q.c > q.d then unknown // sub-query has at least one null
                        //   else e.deptno < q.m
                        //   end as v
                        // from emp as e
                        // left outer join (
                        //   select name, max(deptno) as m, count(*) as c, count(deptno) as d,
                        //       "alwaysTrue" as indicator
                        //   from emp group by name) as q on e.name = q.name
                        builder.push(e.rel)
                            .aggregate(
                                builder.groupKey(),
                                builder.aggregateCall(minMax, builder.field(0)).`as`("m"),
                                builder.count(false, "c"),
                                builder.count(false, "d", builder.field(0))
                            )
                        parentQueryFields.addAll(builder.fields())
                        parentQueryFields.add(builder.alias(literalTrue, indicator))
                        builder.project(parentQueryFields).`as`("q")
                        builder.join(JoinRelType.LEFT, literalTrue, variablesSet)
                        builder.call(
                            SqlStdOperatorTable.CASE,
                            builder.isNull(builder.field("q", indicator)),
                            literalFalse,
                            builder.equals(builder.field("q", "c"), builder.literal(0)),
                            literalFalse,
                            builder.call(
                                SqlStdOperatorTable.IS_TRUE,
                                builder.call(
                                    RexUtil.op(op.comparisonKind),
                                    e.operands.get(0), builder.field("q", "m")
                                )
                            ),
                            literalTrue,
                            builder.greaterThan(
                                builder.field("q", "c"),
                                builder.field("q", "d")
                            ),
                            literalUnknown,
                            builder.call(
                                RexUtil.op(op.comparisonKind),
                                e.operands.get(0), builder.field("q", "m")
                            )
                        )
                    }
                    NOT_EQUALS -> {
                        // for correlated case queries such as
                        //
                        // select e.deptno, e.deptno <> some (
                        //   select deptno from emp where emp.name = e.name) as v
                        // from emp as e
                        //
                        // becomes
                        //
                        // select e.deptno,
                        //   case
                        //   when indicator is null
                        //     then false // sub-query is empty for corresponding corr value
                        //   when q.c = 0 then false // sub-query is empty
                        //   when e.deptno is null then unknown
                        //   when q.c <> q.d && q.d <= 1
                        //     then e.deptno != m || unknown
                        //   when q.d = 1
                        //     then e.deptno != m // sub-query has the distinct result
                        //   else true
                        //   end as v
                        // from emp as e
                        // left outer join (
                        //   select name, count(distinct *) as c, count(distinct deptno) as d,
                        //       max(deptno) as m, "alwaysTrue" as indicator
                        //   from emp group by name) as q on e.name = q.name
                        builder.push(e.rel)
                            .aggregate(
                                builder.groupKey(),
                                builder.count(true, "c"),
                                builder.count(true, "d", builder.field(0)),
                                builder.max(builder.field(0)).`as`("m")
                            )
                        parentQueryFields.addAll(builder.fields())
                        parentQueryFields.add(builder.alias(literalTrue, indicator))
                        builder.project(parentQueryFields).`as`("q") // TODO use projectPlus
                        builder.join(JoinRelType.LEFT, literalTrue, variablesSet)
                        builder.call(
                            SqlStdOperatorTable.CASE,
                            builder.isNull(builder.field("q", indicator)),
                            literalFalse,
                            builder.equals(builder.field("c"), builder.literal(0)),
                            literalFalse,
                            builder.isNull(e.getOperands().get(0)),
                            literalUnknown,
                            builder.and(
                                builder.notEquals(builder.field("d"), builder.field("c")),
                                builder.lessThanOrEqual(
                                    builder.field("d"),
                                    builder.literal(1)
                                )
                            ),
                            builder.or(
                                builder.notEquals(e.operands.get(0), builder.field("q", "m")),
                                literalUnknown
                            ),
                            builder.equals(builder.field("d"), builder.literal(1)),
                            builder.notEquals(e.operands.get(0), builder.field("q", "m")),
                            literalTrue
                        )
                    }
                    else -> throw AssertionError("not possible - per above check")
                }
            }

            // CASE statement above is created with nullable boolean type, but it might
            // not be correct.  If the original sub-query node's type is not nullable it
            // is guaranteed for case statement to not produce NULLs. Therefore to avoid
            // planner complaining we need to add cast.  Note that nullable type is
            // created due to the MIN aggregate call, since there is no GROUP BY.
            return if (!e.getType().isNullable()) {
                builder.cast(caseRexNode, e.getType().getSqlTypeName())
            } else caseRexNode
        }

        /**
         * Rewrites an EXISTS RexSubQuery into a [Join].
         *
         * @param e            EXISTS sub-query to rewrite
         * @param variablesSet A set of variables used by a relational
         * expression of the specified RexSubQuery
         * @param logic        Logic for evaluating
         * @param builder      Builder
         *
         * @return Expression that may be used to replace the RexSubQuery
         */
        private fun rewriteExists(
            e: RexSubQuery?, variablesSet: Set<CorrelationId>,
            logic: RelOptUtil.Logic, builder: RelBuilder
        ): RexNode {
            builder.push(e.rel)
            builder.project(builder.alias(builder.literal(true), "i"))
            when (logic) {
                TRUE -> {
                    // Handles queries with single EXISTS in filter condition:
                    // select e.deptno from emp as e
                    // where exists (select deptno from emp)
                    builder.aggregate(builder.groupKey(0))
                    builder.`as`("dt")
                    builder.join(JoinRelType.INNER, builder.literal(true), variablesSet)
                    return builder.literal(true)
                }
                else -> builder.distinct()
            }
            builder.`as`("dt")
            builder.join(JoinRelType.LEFT, builder.literal(true), variablesSet)
            return builder.isNotNull(last(builder.fields()))
        }

        /**
         * Rewrites a UNIQUE RexSubQuery into an EXISTS RexSubQuery.
         *
         *
         * For example, rewrites the UNIQUE sub-query:
         *
         * <pre>`UNIQUE (SELECT PUBLISHED_IN
         * FROM BOOK
         * WHERE AUTHOR_ID = 3)
        `</pre> *
         *
         *
         * to the following EXISTS sub-query:
         *
         * <pre>`NOT EXISTS (
         * SELECT * FROM (
         * SELECT PUBLISHED_IN
         * FROM BOOK
         * WHERE AUTHOR_ID = 3
         * ) T
         * WHERE (T.PUBLISHED_IN) IS NOT NULL
         * GROUP BY T.PUBLISHED_IN
         * HAVING COUNT(*) > 1
         * )
        `</pre> *
         *
         * @param e            UNIQUE sub-query to rewrite
         * @param builder      Builder
         *
         * @return Expression that may be used to replace the RexSubQuery
         */
        private fun rewriteUnique(e: RexSubQuery?, builder: RelBuilder): RexNode {
            // if sub-query always return unique value.
            val mq: RelMetadataQuery = e.rel.getCluster().getMetadataQuery()
            val isUnique: Boolean = mq.areRowsUnique(e.rel, true)
            if (isUnique != null && isUnique) {
                return builder.getRexBuilder().makeLiteral(true)
            }
            builder.push(e.rel)
            val notNullCondition: List<RexNode> = builder.fields().stream()
                .map(builder::isNotNull)
                .collect(Collectors.toList())
            builder
                .filter(notNullCondition)
                .aggregate(builder.groupKey(builder.fields()), builder.countStar("c"))
                .filter(
                    builder.greaterThan(last(builder.fields()), builder.literal(1))
                )
            val relNode: RelNode = builder.build()
            return builder.call(SqlStdOperatorTable.NOT, RexSubQuery.exists(relNode))
        }

        /**
         * Rewrites an IN RexSubQuery into a [Join].
         *
         * @param e            IN sub-query to rewrite
         * @param variablesSet A set of variables used by a relational
         * expression of the specified RexSubQuery
         * @param logic        Logic for evaluating
         * @param builder      Builder
         * @param offset       Offset to shift [RexInputRef]
         *
         * @return Expression that may be used to replace the RexSubQuery
         */
        private fun rewriteIn(
            e: RexSubQuery?, variablesSet: Set<CorrelationId>,
            logic: RelOptUtil.Logic, builder: RelBuilder, offset: Int
        ): RexNode {
            // Most general case, where the left and right keys might have nulls, and
            // caller requires 3-valued logic return.
            //
            // select e.deptno, e.deptno in (select deptno from emp)
            // from emp as e
            //
            // becomes
            //
            // select e.deptno,
            //   case
            //   when ct.c = 0 then false
            //   when e.deptno is null then null
            //   when dt.i is not null then true
            //   when ct.ck < ct.c then null
            //   else false
            //   end
            // from emp as e
            // left join (
            //   (select count(*) as c, count(deptno) as ck from emp) as ct
            //   cross join (select distinct deptno, true as i from emp)) as dt
            //   on e.deptno = dt.deptno
            //
            // If keys are not null we can remove "ct" and simplify to
            //
            // select e.deptno,
            //   case
            //   when dt.i is not null then true
            //   else false
            //   end
            // from emp as e
            // left join (select distinct deptno, true as i from emp) as dt
            //   on e.deptno = dt.deptno
            //
            // We could further simplify to
            //
            // select e.deptno,
            //   dt.i is not null
            // from emp as e
            // left join (select distinct deptno, true as i from emp) as dt
            //   on e.deptno = dt.deptno
            //
            // but have not yet.
            //
            // If the logic is TRUE we can just kill the record if the condition
            // evaluates to FALSE or UNKNOWN. Thus the query simplifies to an inner
            // join:
            //
            // select e.deptno,
            //   true
            // from emp as e
            // inner join (select distinct deptno from emp) as dt
            //   on e.deptno = dt.deptno
            //
            var offset = offset
            builder.push(e.rel)
            val fields: List<RexNode> = ArrayList(builder.fields())

            // for the case when IN has only literal operands, it may be handled
            // in the simpler way:
            //
            // select e.deptno, 123456 in (select deptno from emp)
            // from emp as e
            //
            // becomes
            //
            // select e.deptno,
            //   case
            //   when dt.c IS NULL THEN FALSE
            //   when e.deptno IS NULL THEN NULL
            //   when dt.cs IS FALSE THEN NULL
            //   when dt.cs IS NOT NULL THEN TRUE
            //   else false
            //   end
            // from emp AS e
            // cross join (
            //   select distinct deptno is not null as cs, count(*) as c
            //   from emp
            //   where deptno = 123456 or deptno is null or e.deptno is null
            //   order by cs desc limit 1) as dt
            //
            val allLiterals: Boolean = RexUtil.allLiterals(e.getOperands())
            val expressionOperands: List<RexNode> = ArrayList(e.getOperands())
            val keyIsNulls: List<RexNode> = e.getOperands().stream()
                .filter { operand -> operand.getType().isNullable() }
                .map(builder::isNull)
                .collect(Collectors.toList())
            val trueLiteral: RexLiteral = builder.literal(true)
            val falseLiteral: RexLiteral = builder.literal(false)
            val unknownLiteral: RexLiteral = builder.getRexBuilder().makeNullLiteral(trueLiteral.getType())
            if (allLiterals) {
                val conditions: List<RexNode> = Pair.zip(expressionOperands, fields).stream()
                    .map { pair -> builder.equals(pair.left, pair.right) }
                    .collect(Collectors.toList())
                when (logic) {
                    TRUE, TRUE_FALSE -> {
                        builder.filter(conditions)
                        builder.project(builder.alias(trueLiteral, "cs"))
                        builder.distinct()
                    }
                    else -> {
                        val isNullOperands: List<RexNode> = fields.stream()
                            .map(builder::isNull)
                            .collect(Collectors.toList())
                        // uses keyIsNulls conditions in the filter to avoid empty results
                        isNullOperands.addAll(keyIsNulls)
                        builder.filter(
                            builder.or(
                                builder.and(conditions),
                                builder.or(isNullOperands)
                            )
                        )
                        val project: RexNode = builder.and(
                            fields.stream()
                                .map(builder::isNotNull)
                                .collect(Collectors.toList())
                        )
                        builder.project(builder.alias(project, "cs"))
                        if (variablesSet.isEmpty()) {
                            builder.aggregate(
                                builder.groupKey(builder.field("cs")),
                                builder.count(false, "c")
                            )

                            // sorts input with desc order since we are interested
                            // only in the case when one of the values is true.
                            // When true value is absent then we are interested
                            // only in false value.
                            builder.sortLimit(
                                0, 1,
                                ImmutableList.of(builder.desc(builder.field("cs")))
                            )
                        } else {
                            builder.distinct()
                        }
                    }
                }
                // clears expressionOperands and fields lists since
                // all expressions were used in the filter
                expressionOperands.clear()
                fields.clear()
            } else {
                when (logic) {
                    TRUE -> builder.aggregate(builder.groupKey(fields))
                    TRUE_FALSE_UNKNOWN, UNKNOWN_AS_TRUE -> {
                        // Builds the cross join
                        builder.aggregate(
                            builder.groupKey(),
                            builder.count(false, "c"),
                            builder.count(builder.fields()).`as`("ck")
                        )
                        builder.`as`("ct")
                        if (!variablesSet.isEmpty()) {
                            builder.join(JoinRelType.LEFT, trueLiteral, variablesSet)
                        } else {
                            builder.join(JoinRelType.INNER, trueLiteral, variablesSet)
                        }
                        offset += 2
                        builder.push(e.rel)
                        fields.add(builder.alias(trueLiteral, "i"))
                        builder.project(fields)
                        builder.distinct()
                    }
                    else -> {
                        fields.add(builder.alias(trueLiteral, "i"))
                        builder.project(fields)
                        builder.distinct()
                    }
                }
            }
            builder.`as`("dt")
            val refOffset = offset
            val conditions: List<RexNode> = Pair.zip(expressionOperands, builder.fields()).stream()
                .map { pair -> builder.equals(pair.left, RexUtil.shift(pair.right, refOffset)) }
                .collect(Collectors.toList())
            when (logic) {
                TRUE -> {
                    builder.join(JoinRelType.INNER, builder.and(conditions), variablesSet)
                    return trueLiteral
                }
                else -> {}
            }
            // Now the left join
            builder.join(JoinRelType.LEFT, builder.and(conditions), variablesSet)
            val operands: ImmutableList.Builder<RexNode> = ImmutableList.builder()
            var b: RexLiteral = trueLiteral
            when (logic) {
                TRUE_FALSE_UNKNOWN -> {
                    b = unknownLiteral
                    if (allLiterals) {
                        // Considers case when right side of IN is empty
                        // for the case of non-correlated sub-queries
                        if (variablesSet.isEmpty()) {
                            operands.add(
                                builder.isNull(builder.field("c")),
                                falseLiteral
                            )
                        }
                        operands.add(
                            builder.equals(builder.field("cs"), falseLiteral),
                            b
                        )
                    } else {
                        operands.add(
                            builder.equals(builder.field("ct", "c"), builder.literal(0)),
                            falseLiteral
                        )
                    }
                }
                UNKNOWN_AS_TRUE -> if (allLiterals) {
                    if (variablesSet.isEmpty()) {
                        operands.add(
                            builder.isNull(builder.field("c")),
                            falseLiteral
                        )
                    }
                    operands.add(
                        builder.equals(builder.field("cs"), falseLiteral),
                        b
                    )
                } else {
                    operands.add(
                        builder.equals(builder.field("ct", "c"), builder.literal(0)),
                        falseLiteral
                    )
                }
                else -> {}
            }
            if (!keyIsNulls.isEmpty()) {
                operands.add(builder.or(keyIsNulls), unknownLiteral)
            }
            if (allLiterals) {
                operands.add(
                    builder.isNotNull(builder.field("cs")),
                    trueLiteral
                )
            } else {
                operands.add(
                    builder.isNotNull(last(builder.fields())),
                    trueLiteral
                )
            }
            if (!allLiterals) {
                when (logic) {
                    TRUE_FALSE_UNKNOWN, UNKNOWN_AS_TRUE -> operands.add(
                        builder.lessThan(
                            builder.field("ct", "ck"),
                            builder.field("ct", "c")
                        ),
                        b
                    )
                    else -> {}
                }
            }
            operands.add(falseLiteral)
            return builder.call(SqlStdOperatorTable.CASE, operands.build())
        }

        /** Returns a reference to a particular field, by offset, across several
         * inputs on a [RelBuilder]'s stack.  */
        private fun field(builder: RelBuilder, inputCount: Int, offset: Int): RexInputRef {
            var offset = offset
            var inputOrdinal = 0
            while (true) {
                val r: RelNode = builder.peek(inputCount, inputOrdinal)
                if (offset < r.getRowType().getFieldCount()) {
                    return builder.field(inputCount, inputOrdinal, offset)
                }
                ++inputOrdinal
                offset -= r.getRowType().getFieldCount()
            }
        }

        /** Returns a list of expressions that project the first `fieldCount`
         * fields of the top input on a [RelBuilder]'s stack.  */
        private fun fields(builder: RelBuilder, fieldCount: Int): List<RexNode> {
            val projects: List<RexNode> = ArrayList()
            for (i in 0 until fieldCount) {
                projects.add(builder.field(i))
            }
            return projects
        }

        private fun matchProject(
            rule: SubQueryRemoveRule,
            call: RelOptRuleCall
        ) {
            val project: Project = call.rel(0)
            val builder: RelBuilder = call.builder()
            val e: RexSubQuery = RexUtil.SubQueryFinder.find(project.getProjects())
            assert(e != null)
            val logic: RelOptUtil.Logic = LogicVisitor.find(
                RelOptUtil.Logic.TRUE_FALSE_UNKNOWN,
                project.getProjects(), e
            )
            builder.push(project.getInput())
            val fieldCount: Int = builder.peek().getRowType().getFieldCount()
            val variablesSet: Set<CorrelationId> = RelOptUtil.getVariablesUsed(e.rel)
            val target: RexNode = rule.apply(
                e, variablesSet,
                logic, builder, 1, fieldCount
            )
            val shuttle: RexShuttle = ReplaceSubQueryShuttle(e, target)
            builder.project(
                shuttle.apply(project.getProjects()),
                project.getRowType().getFieldNames()
            )
            call.transformTo(builder.build())
        }

        private fun matchFilter(
            rule: SubQueryRemoveRule,
            call: RelOptRuleCall
        ) {
            val filter: Filter = call.rel(0)
            val builder: RelBuilder = call.builder()
            builder.push(filter.getInput())
            var count = 0
            var c: RexNode = filter.getCondition()
            while (true) {
                val e: RexSubQuery = RexUtil.SubQueryFinder.find(c)
                if (e == null) {
                    assert(count > 0)
                    break
                }
                ++count
                val logic: RelOptUtil.Logic = LogicVisitor.find(RelOptUtil.Logic.TRUE, ImmutableList.of(c), e)
                val variablesSet: Set<CorrelationId> = RelOptUtil.getVariablesUsed(e.rel)
                val target: RexNode = rule.apply(
                    e, variablesSet, logic,
                    builder, 1, builder.peek().getRowType().getFieldCount()
                )
                val shuttle: RexShuttle = ReplaceSubQueryShuttle(e, target)
                c = c.accept(shuttle)
            }
            builder.filter(c)
            builder.project(fields(builder, filter.getRowType().getFieldCount()))
            call.transformTo(builder.build())
        }

        private fun matchJoin(rule: SubQueryRemoveRule, call: RelOptRuleCall) {
            val join: Join = call.rel(0)
            val builder: RelBuilder = call.builder()
            val e: RexSubQuery = RexUtil.SubQueryFinder.find(join.getCondition())
            assert(e != null)
            val logic: RelOptUtil.Logic = LogicVisitor.find(
                RelOptUtil.Logic.TRUE,
                ImmutableList.of(join.getCondition()), e
            )
            builder.push(join.getLeft())
            builder.push(join.getRight())
            val fieldCount: Int = join.getRowType().getFieldCount()
            val variablesSet: Set<CorrelationId> = RelOptUtil.getVariablesUsed(e.rel)
            val target: RexNode = rule.apply(
                e, variablesSet,
                logic, builder, 2, fieldCount
            )
            val shuttle: RexShuttle = ReplaceSubQueryShuttle(e, target)
            builder.join(join.getJoinType(), shuttle.apply(join.getCondition()))
            builder.project(fields(builder, join.getRowType().getFieldCount()))
            call.transformTo(builder.build())
        }
    }
}
