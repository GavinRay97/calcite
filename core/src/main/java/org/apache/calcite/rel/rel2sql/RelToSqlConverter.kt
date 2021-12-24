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
package org.apache.calcite.rel.rel2sql

import org.apache.calcite.adapter.jdbc.JdbcTable
import org.apache.calcite.linq4j.Ord
import org.apache.calcite.linq4j.tree.Expressions
import org.apache.calcite.plan.RelOptTable
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rel.RelCollations
import org.apache.calcite.rel.RelFieldCollation
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rel.core.Correlate
import org.apache.calcite.rel.core.CorrelationId
import org.apache.calcite.rel.core.Filter
import org.apache.calcite.rel.core.Intersect
import org.apache.calcite.rel.core.Join
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.core.Match
import org.apache.calcite.rel.core.Minus
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.core.TableFunctionScan
import org.apache.calcite.rel.core.TableModify
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.core.Uncollect
import org.apache.calcite.rel.core.Union
import org.apache.calcite.rel.core.Values
import org.apache.calcite.rel.core.Window
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.logical.LogicalProject
import org.apache.calcite.rel.logical.LogicalSort
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.rex.RexLocalRef
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexProgram
import org.apache.calcite.sql.JoinConditionType
import org.apache.calcite.sql.JoinType
import org.apache.calcite.sql.SqlBasicCall
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlDelete
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.SqlHint
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlInsert
import org.apache.calcite.sql.SqlIntervalLiteral
import org.apache.calcite.sql.SqlJoin
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlMatchRecognize
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlNodeList
import org.apache.calcite.sql.SqlSelect
import org.apache.calcite.sql.SqlTableRef
import org.apache.calcite.sql.SqlUpdate
import org.apache.calcite.sql.SqlUtil
import org.apache.calcite.sql.`fun`.SqlInternalOperators
import org.apache.calcite.sql.`fun`.SqlSingleValueAggFunction
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.util.SqlShuttle
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.util.ImmutableBitSet
import org.apache.calcite.util.Pair
import org.apache.calcite.util.Permutation
import org.apache.calcite.util.ReflectUtil
import org.apache.calcite.util.ReflectiveVisitor
import org.apache.calcite.util.Util
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.ImmutableSet
import com.google.common.collect.Iterables
import com.google.common.collect.Ordering
import java.util.ArrayDeque
import java.util.ArrayList
import java.util.Arrays
import java.util.Collection
import java.util.Collections
import java.util.Deque
import java.util.LinkedHashSet
import java.util.List
import java.util.Map
import java.util.Set
import java.util.SortedSet
import java.util.TreeSet
import java.util.stream.Collectors
import java.util.stream.Stream
import org.apache.calcite.rex.RexLiteral.stringValue
import java.util.Objects.requireNonNull

/**
 * Utility to convert relational expressions to SQL abstract syntax tree.
 */
class RelToSqlConverter @SuppressWarnings("argument.type.incompatible") constructor(dialect: SqlDialect?) :
    SqlImplementor(dialect), ReflectiveVisitor {
    private val dispatcher: ReflectUtil.MethodDispatcher<Result>
    private val stack: Deque<Frame> = ArrayDeque()

    /** Creates a RelToSqlConverter.  */
    init {
        dispatcher = ReflectUtil.createMethodDispatcher(
            Result::class.java, this, "visit",
            RelNode::class.java
        )
    }

    /** Dispatches a call to the `visit(Xxx e)` method where `Xxx`
     * most closely matches the runtime type of the argument.  */
    protected fun dispatch(e: RelNode?): Result {
        return dispatcher.invoke(e)
    }

    @Override
    override fun visitInput(
        parent: RelNode, i: Int, anon: Boolean,
        ignoreClauses: Boolean, expectedClauses: Set<Clause?>?
    ): Result {
        return try {
            val e: RelNode = parent.getInput(i)
            stack.push(
                Frame(
                    parent,
                    i,
                    e,
                    anon,
                    ignoreClauses,
                    expectedClauses
                )
            )
            dispatch(e)
        } finally {
            stack.pop()
        }
    }

    @get:Override
    protected override val isAnon: Boolean
        protected get() {
            val peek: Frame = stack.peek()
            return peek == null || peek.anon
        }

    @Override
    protected override fun result(
        node: SqlNode?, clauses: Collection<Clause?>?,
        @Nullable neededAlias: String?, @Nullable neededType: RelDataType?,
        aliases: Map<String?, RelDataType?>?
    ): Result {
        val frame: Frame = requireNonNull(stack.peek())
        return super.result(node, clauses, neededAlias, neededType, aliases)
            .withAnon(isAnon)
            .withExpectedClauses(
                frame.ignoreClauses, frame.expectedClauses,
                frame.parent
            )
    }

    /** Visits a RelNode; called by [.dispatch] via reflection.  */
    fun visit(e: RelNode): Result {
        throw AssertionError("Need to implement " + e.getClass().getName())
    }

    /**
     * A SqlShuttle to replace references to a column of a table alias with the expression
     * from the select item that is the source of that column.
     * ANTI- and SEMI-joins generate an alias for right hand side relation which
     * is used in the ON condition. But that alias is never created, so we have to inline references.
     */
    private class AliasReplacementShuttle internal constructor(
        private val tableAlias: String?, tableType: RelDataType,
        replaceSource: SqlNodeList
    ) : SqlShuttle() {
        private val tableType: RelDataType
        private val replaceSource: SqlNodeList

        init {
            this.tableType = tableType
            this.replaceSource = replaceSource
        }

        @Override
        fun visit(id: SqlIdentifier): SqlNode {
            if (tableAlias!!.equals(id.names.get(0))) {
                val index: Int = requireNonNull(
                    tableType.getField(id.names.get(1), false, false)
                ) { "field " + id.names.get(1).toString() + " is not found in " + tableType }
                    .getIndex()
                var selectItem: SqlNode = requireNonNull(replaceSource, "replaceSource").get(index)
                if (selectItem.getKind() === SqlKind.AS) {
                    selectItem = (selectItem as SqlCall).operand(0)
                }
                return selectItem.clone(id.getParserPosition())
            }
            return id
        }
    }

    /** Visits a Join; called by [.dispatch] via reflection.  */
    fun visit(e: Join): Result {
        when (e.getJoinType()) {
            ANTI, SEMI -> return visitAntiOrSemiJoin(e)
            else -> {}
        }
        val leftResult: Result = visitInput(e, 0).resetAlias()
        val rightResult: Result = visitInput(e, 1).resetAlias()
        val leftContext: Context = leftResult.qualifiedContext()
        val rightContext: Context = rightResult.qualifiedContext()
        val sqlCondition: SqlNode?
        val condType: JoinConditionType
        var joinType: JoinType = joinType(e.getJoinType())
        if (joinType === JoinType.INNER
            && e.getCondition().isAlwaysTrue()
        ) {
            joinType = if (isCommaJoin(e)) {
                JoinType.COMMA
            } else {
                JoinType.CROSS
            }
            sqlCondition = null
            condType = JoinConditionType.NONE
        } else {
            sqlCondition = convertConditionToSqlNode(
                e.getCondition(), leftContext,
                rightContext
            )
            condType = JoinConditionType.ON
        }
        val join: SqlNode = SqlJoin(
            POS,
            leftResult.asFrom(),
            SqlLiteral.createBoolean(false, POS),
            joinType.symbol(POS),
            rightResult.asFrom(),
            condType.symbol(POS),
            sqlCondition
        )
        return result(join, leftResult, rightResult)
    }

    protected fun visitAntiOrSemiJoin(e: Join): Result {
        val leftResult: Result = visitInput(e, 0).resetAlias()
        val rightResult: Result = visitInput(e, 1).resetAlias()
        val leftContext: Context = leftResult.qualifiedContext()
        val rightContext: Context = rightResult.qualifiedContext()
        val sqlSelect: SqlSelect = leftResult.asSelect()
        var sqlCondition: SqlNode = convertConditionToSqlNode(e.getCondition(), leftContext, rightContext)
        if (leftResult.neededAlias != null) {
            val visitor: SqlShuttle = AliasReplacementShuttle(
                leftResult.neededAlias,
                e.getLeft().getRowType(), sqlSelect.getSelectList()
            )
            sqlCondition = sqlCondition.accept(visitor)
        }
        val fromPart: SqlNode = rightResult.asFrom()
        val existsSqlSelect: SqlSelect
        if (fromPart.getKind() === SqlKind.SELECT) {
            existsSqlSelect = fromPart as SqlSelect
            existsSqlSelect.setSelectList(
                SqlNodeList(ImmutableList.of(ONE), POS)
            )
            if (existsSqlSelect.getWhere() != null) {
                sqlCondition = SqlStdOperatorTable.AND.createCall(
                    POS,
                    existsSqlSelect.getWhere(),
                    sqlCondition
                )
            }
            existsSqlSelect.setWhere(sqlCondition)
        } else {
            existsSqlSelect = SqlSelect(
                POS, null,
                SqlNodeList(
                    ImmutableList.of(ONE), POS
                ),
                fromPart, sqlCondition, null,
                null, null, null, null, null, null
            )
        }
        sqlCondition = SqlStdOperatorTable.EXISTS.createCall(POS, existsSqlSelect)
        if (e.getJoinType() === JoinRelType.ANTI) {
            sqlCondition = SqlStdOperatorTable.NOT.createCall(POS, sqlCondition)
        }
        if (sqlSelect.getWhere() != null) {
            sqlCondition = SqlStdOperatorTable.AND.createCall(
                POS,
                sqlSelect.getWhere(),
                sqlCondition
            )
        }
        sqlSelect.setWhere(sqlCondition)
        val resultNode: SqlNode =
            if (leftResult.neededAlias == null) sqlSelect else `as`(sqlSelect, leftResult.neededAlias)
        return result(resultNode, leftResult, rightResult)
    }

    /** Returns whether this join should be unparsed as a [JoinType.COMMA].
     *
     *
     * Comma-join is one possible syntax for `CROSS JOIN ... ON TRUE`,
     * supported on most but not all databases
     * (see [SqlDialect.emulateJoinTypeForCrossJoin]).
     *
     *
     * For example, the following queries are equivalent:
     *
     * <pre>`// Comma join
     * SELECT *
     * FROM Emp, Dept
     *
     * // Cross join
     * SELECT *
     * FROM Emp CROSS JOIN Dept
     *
     * // Inner join
     * SELECT *
     * FROM Emp INNER JOIN Dept ON TRUE
    `</pre> *
     *
     *
     * Examples:
     *
     *  * `FROM (x CROSS JOIN y ON TRUE) CROSS JOIN z ON TRUE`
     * is a comma join, because all joins are comma joins;
     *  * `FROM (x CROSS JOIN y ON TRUE) CROSS JOIN z ON TRUE`
     * would not be a comma join when run on Apache Spark, because Spark does
     * not support comma join;
     *  * `FROM (x CROSS JOIN y ON TRUE) LEFT JOIN z ON TRUE`
     * is not comma join because one of the joins is not INNER;
     *  * `FROM (x CROSS JOIN y ON c) CROSS JOIN z ON TRUE`
     * is not a comma join because one of the joins is ON TRUE.
     *
     */
    private fun isCommaJoin(join: Join): Boolean {
        if (!join.getCondition().isAlwaysTrue()) {
            return false
        }
        if (dialect.emulateJoinTypeForCrossJoin() !== JoinType.COMMA) {
            return false
        }
        assert(!stack.isEmpty())
        assert(stack.element().r === join)
        var j: Join? = null
        for (frame in stack) {
            j = frame.r as Join
            if (frame.parent !is Join) {
                break
            }
        }
        val topJoin: Join = requireNonNull(j, "top join")

        // Flatten the join tree, using a breadth-first algorithm.
        // After flattening, the list contains all of the joins that will make up
        // the FROM clause.
        val flatJoins: List<Join> = ArrayList()
        flatJoins.add(topJoin)
        var i = 0
        while (i < flatJoins.size()) {
            val j2: Join = flatJoins[i++]
            if (j2.getLeft() is Join) {
                flatJoins.add(j2.getLeft() as Join)
            }
            if (j2.getRight() is Join) {
                flatJoins.add(j2.getRight() as Join)
            }
        }

        // If all joins are cross-joins (INNER JOIN ON TRUE), we can use
        // we can use comma syntax "FROM a, b, c, d, e".
        for (j2 in flatJoins) {
            if (j2.getJoinType() !== JoinRelType.INNER
                || !j2.getCondition().isAlwaysTrue()
            ) {
                return false
            }
        }
        return true
    }

    /** Visits a Correlate; called by [.dispatch] via reflection.  */
    fun visit(e: Correlate): Result {
        val leftResult: Result = visitInput(e, 0)
            .resetAlias(e.getCorrelVariable(), e.getRowType())
        parseCorrelTable(e, leftResult)
        val rightResult: Result = visitInput(e, 1)
        val rightLateral: SqlNode = SqlStdOperatorTable.LATERAL.createCall(POS, rightResult.node)
        val rightLateralAs: SqlNode = SqlStdOperatorTable.AS.createCall(POS, rightLateral,
            SqlIdentifier(
                requireNonNull(
                    rightResult.neededAlias
                ) { "rightResult.neededAlias is null, node is " + rightResult.node }, POS
            )
        )
        val join: SqlNode = SqlJoin(
            POS,
            leftResult.asFrom(),
            SqlLiteral.createBoolean(false, POS),
            JoinType.COMMA.symbol(POS),
            rightLateralAs,
            JoinConditionType.NONE.symbol(POS),
            null
        )
        return result(join, leftResult, rightResult)
    }

    /** Visits a Filter; called by [.dispatch] via reflection.  */
    fun visit(e: Filter): Result {
        val input: RelNode = e.getInput()
        return if (input is Aggregate) {
            val aggregate: Aggregate = input as Aggregate
            val ignoreClauses = aggregate.getInput() is Project
            val x: Result = visitInput(
                e, 0, isAnon, ignoreClauses,
                ImmutableSet.of(Clause.HAVING)
            )
            parseCorrelTable(e, x)
            val builder: Builder = x.builder(e)
            x.asSelect().setHaving(
                SqlUtil.andExpressions(
                    x.asSelect().getHaving(),
                    builder.context.toSql(null, e.getCondition())
                )
            )
            builder.result()
        } else {
            val x: Result = visitInput(e, 0, Clause.WHERE)
            parseCorrelTable(e, x)
            val builder: Builder = x.builder(e)
            builder.setWhere(builder.context.toSql(null, e.getCondition()))
            builder.result()
        }
    }

    /** Visits a Project; called by [.dispatch] via reflection.  */
    fun visit(e: Project): Result {
        val x: Result = visitInput(e, 0, Clause.SELECT)
        parseCorrelTable(e, x)
        val builder: Builder = x.builder(e)
        if (!isStar(e.getProjects(), e.getInput().getRowType(), e.getRowType())) {
            val selectList: List<SqlNode> = ArrayList()
            for (ref in e.getProjects()) {
                var sqlExpr: SqlNode = builder.context.toSql(null, ref)
                if (SqlUtil.isNullLiteral(sqlExpr, false)) {
                    val field: RelDataTypeField = e.getRowType().getFieldList().get(selectList.size())
                    sqlExpr = castNullType(sqlExpr, field.getType())
                }
                addSelect(selectList, sqlExpr, e.getRowType())
            }
            builder.setSelect(SqlNodeList(selectList, POS))
        }
        return builder.result()
    }

    /** Wraps a NULL literal in a CAST operator to a target type.
     *
     * @param nullLiteral NULL literal
     * @param type Target type
     *
     * @return null literal wrapped in CAST call
     */
    private fun castNullType(nullLiteral: SqlNode, type: RelDataType): SqlNode {
        val typeNode: SqlNode = dialect.getCastSpec(type) ?: return nullLiteral
        return SqlStdOperatorTable.CAST.createCall(POS, nullLiteral, typeNode)
    }

    /** Visits a Window; called by [.dispatch] via reflection.  */
    fun visit(e: Window): Result {
        val x: Result = visitInput(e, 0)
        val builder: Builder = x.builder(e)
        val input: RelNode = e.getInput()
        val inputFieldCount: Int = input.getRowType().getFieldCount()
        val rexOvers: List<SqlNode> = ArrayList()
        for (group in e.groups) {
            rexOvers.addAll(builder.context.toSql(group, e.constants, inputFieldCount))
        }
        val selectList: List<SqlNode> = ArrayList()
        for (field in input.getRowType().getFieldList()) {
            addSelect(selectList, builder.context.field(field.getIndex()), e.getRowType())
        }
        for (rexOver in rexOvers) {
            addSelect(selectList, rexOver, e.getRowType())
        }
        builder.setSelect(SqlNodeList(selectList, POS))
        return builder.result()
    }

    /** Visits an Aggregate; called by [.dispatch] via reflection.  */
    fun visit(e: Aggregate): Result {
        val builder: Builder = visitAggregate(e, e.getGroupSet().toList(), Clause.GROUP_BY)
        return builder.result()
    }

    private fun visitAggregate(
        e: Aggregate, groupKeyList: List<Integer>,
        vararg clauses: Clause
    ): Builder {
        // groupSet contains at least one column that is not in any groupSet.
        // Set of clauses that we expect the builder need to add extra Clause.HAVING
        // then can add Having filter condition in buildAggregate.
        val clauseSet: Set<Clause> = TreeSet(Arrays.asList(clauses))
        if (!e.getGroupSet().equals(ImmutableBitSet.union(e.getGroupSets()))) {
            clauseSet.add(Clause.HAVING)
        }
        // "select a, b, sum(x) from ( ... ) group by a, b"
        val ignoreClauses = e.getInput() is Project
        val x: Result = visitInput(e, 0, isAnon, ignoreClauses, clauseSet)
        val builder: Builder = x.builder(e)
        val selectList: List<SqlNode> = ArrayList()
        val groupByList: List<SqlNode> = generateGroupList(builder, selectList, e, groupKeyList)
        return buildAggregate(e, builder, selectList, groupByList)
    }

    /**
     * Builds the group list for an Aggregate node.
     *
     * @param e The Aggregate node
     * @param builder The SQL builder
     * @param groupByList output group list
     * @param selectList output select list
     */
    protected fun buildAggGroupList(
        e: Aggregate, builder: Builder,
        groupByList: List<SqlNode?>, selectList: List<SqlNode?>
    ) {
        for (group in e.getGroupSet()) {
            val field: SqlNode = builder.context.field(group)
            addSelect(selectList, field, e.getRowType())
            groupByList.add(field)
        }
    }

    /**
     * Builds an aggregate query.
     *
     * @param e The Aggregate node
     * @param builder The SQL builder
     * @param selectList The precomputed group list
     * @param groupByList The precomputed select list
     * @return The aggregate query result
     */
    protected fun buildAggregate(
        e: Aggregate, builder: Builder,
        selectList: List<SqlNode?>, groupByList: List<SqlNode?>
    ): Builder {
        for (aggCall in e.getAggCallList()) {
            var aggCallSqlNode: SqlNode = builder.context.toSql(aggCall)
            if (aggCall.getAggregation() is SqlSingleValueAggFunction) {
                aggCallSqlNode = dialect.rewriteSingleValueExpr(aggCallSqlNode)
            }
            addSelect(selectList, aggCallSqlNode, e.getRowType())
        }
        builder.setSelect(SqlNodeList(selectList, POS))
        if (!groupByList.isEmpty() || e.getAggCallList().isEmpty()) {
            // Some databases don't support "GROUP BY ()". We can omit it as long
            // as there is at least one aggregate function.
            builder.setGroupBy(SqlNodeList(groupByList, POS))
        }
        if (builder.clauses.contains(Clause.HAVING) && !e.getGroupSet()
                .equals(ImmutableBitSet.union(e.getGroupSets()))
        ) {
            // groupSet contains at least one column that is not in any groupSets.
            // To make such columns must appear in the output (their value will
            // always be NULL), we generate an extra grouping set, then filter
            // it out using a "HAVING GROUPING(groupSets) <> 0".
            // We want to generate the
            val groupingList = SqlNodeList(POS)
            e.getGroupSet().forEach { g -> groupingList.add(builder.context.field(g)) }
            builder.setHaving(
                SqlStdOperatorTable.NOT_EQUALS.createCall(
                    POS,
                    SqlStdOperatorTable.GROUPING.createCall(groupingList), ZERO
                )
            )
        }
        return builder
    }

    /** Generates the GROUP BY items, for example `GROUP BY x, y`,
     * `GROUP BY CUBE (x, y)` or `GROUP BY ROLLUP (x, y)`.
     *
     *
     * Also populates the SELECT clause. If the GROUP BY list is simple, the
     * SELECT will be identical; if the GROUP BY list contains GROUPING SETS,
     * CUBE or ROLLUP, the SELECT clause will contain the distinct leaf
     * expressions.  */
    private fun generateGroupList(
        builder: Builder,
        selectList: List<SqlNode>, aggregate: Aggregate, groupList: List<Integer>
    ): List<SqlNode> {
        val sortedGroupList: List<Integer> = Ordering.natural().sortedCopy(groupList)
        assert(aggregate.getGroupSet().asList().equals(sortedGroupList)) {
            ("groupList " + groupList + " must be equal to groupSet "
                    + aggregate.getGroupSet() + ", just possibly a different order")
        }
        val groupKeys: List<SqlNode> = ArrayList()
        for (key in groupList) {
            val field: SqlNode = builder.context.field(key)
            groupKeys.add(field)
        }
        for (key in sortedGroupList) {
            val field: SqlNode = builder.context.field(key)
            addSelect(selectList, field, aggregate.getRowType())
        }
        return when (aggregate.getGroupType()) {
            SIMPLE -> ImmutableList.copyOf(groupKeys)
            CUBE -> {
                if (aggregate.getGroupSet().cardinality() > 1) {
                    ImmutableList.of(
                        SqlStdOperatorTable.CUBE.createCall(SqlParserPos.ZERO, groupKeys)
                    )
                } else ImmutableList.of(
                    SqlStdOperatorTable.ROLLUP.createCall(SqlParserPos.ZERO, groupKeys)
                )
            }
            ROLLUP -> ImmutableList.of(
                SqlStdOperatorTable.ROLLUP.createCall(SqlParserPos.ZERO, groupKeys)
            )
            OTHER -> {
                // Make sure that the group sets contains all bits.
                val groupSets: List<ImmutableBitSet>
                if (aggregate.getGroupSet()
                        .equals(ImmutableBitSet.union(aggregate.groupSets))
                ) {
                    groupSets = aggregate.getGroupSets()
                } else {
                    groupSets = ArrayList(aggregate.getGroupSets().size() + 1)
                    groupSets.add(aggregate.getGroupSet())
                    groupSets.addAll(aggregate.getGroupSets())
                }
                ImmutableList.of(
                    SqlStdOperatorTable.GROUPING_SETS.createCall(SqlParserPos.ZERO,
                        groupSets.stream()
                            .map { groupSet -> groupItem(groupKeys, groupSet, aggregate.getGroupSet()) }
                            .collect(Collectors.toList())))
            }
            else -> {
                val groupSets: List<ImmutableBitSet>
                if (aggregate.getGroupSet()
                        .equals(ImmutableBitSet.union(aggregate.groupSets))
                ) {
                    groupSets = aggregate.getGroupSets()
                } else {
                    groupSets = ArrayList(aggregate.getGroupSets().size() + 1)
                    groupSets.add(aggregate.getGroupSet())
                    groupSets.addAll(aggregate.getGroupSets())
                }
                ImmutableList.of(
                    SqlStdOperatorTable.GROUPING_SETS.createCall(SqlParserPos.ZERO,
                        groupSets.stream()
                            .map { groupSet -> groupItem(groupKeys, groupSet, aggregate.getGroupSet()) }
                            .collect(Collectors.toList())))
            }
        }
    }

    /** Visits a TableScan; called by [.dispatch] via reflection.  */
    fun visit(e: TableScan): Result {
        val identifier: SqlIdentifier = getSqlTargetTable(e)
        val node: SqlNode
        val hints: ImmutableList<RelHint> = e.getHints()
        if (!hints.isEmpty()) {
            val pos: SqlParserPos = identifier.getParserPosition()
            node = SqlTableRef(pos, identifier,
                SqlNodeList.of(pos, hints.stream().map { h -> toSqlHint(h, pos) }
                    .collect(Collectors.toList())))
        } else {
            node = identifier
        }
        return result(node, ImmutableList.of(Clause.FROM), e, null)
    }

    /** Visits a Union; called by [.dispatch] via reflection.  */
    fun visit(e: Union): Result {
        return setOpToSql(if (e.all) SqlStdOperatorTable.UNION_ALL else SqlStdOperatorTable.UNION, e)
    }

    /** Visits an Intersect; called by [.dispatch] via reflection.  */
    fun visit(e: Intersect): Result {
        return setOpToSql(if (e.all) SqlStdOperatorTable.INTERSECT_ALL else SqlStdOperatorTable.INTERSECT, e)
    }

    /** Visits a Minus; called by [.dispatch] via reflection.  */
    fun visit(e: Minus): Result {
        return setOpToSql(if (e.all) SqlStdOperatorTable.EXCEPT_ALL else SqlStdOperatorTable.EXCEPT, e)
    }

    /** Visits a Calc; called by [.dispatch] via reflection.  */
    fun visit(e: Calc): Result {
        val program: RexProgram = e.getProgram()
        val expectedClauses: ImmutableSet<Clause> =
            if (program.getCondition() != null) ImmutableSet.of(Clause.WHERE) else ImmutableSet.of()
        val x: Result = visitInput(e, 0, expectedClauses)
        parseCorrelTable(e, x)
        val builder: Builder = x.builder(e)
        if (!isStar(program)) {
            val selectList: List<SqlNode> = ArrayList(program.getProjectList().size())
            for (ref in program.getProjectList()) {
                val sqlExpr: SqlNode = builder.context.toSql(program, ref)
                addSelect(selectList, sqlExpr, e.getRowType())
            }
            builder.setSelect(SqlNodeList(selectList, POS))
        }
        if (program.getCondition() != null) {
            builder.setWhere(
                builder.context.toSql(program, program.getCondition())
            )
        }
        return builder.result()
    }

    /** Visits a Values; called by [.dispatch] via reflection.  */
    fun visit(e: Values): Result {
        val clauses: List<Clause> = ImmutableList.of(Clause.SELECT)
        val pairs: Map<String, RelDataType> = ImmutableMap.of()
        val context: Context = aliasContext(pairs, false)
        var query: SqlNode
        val rename = (stack.size() <= 1
                || Iterables.get(stack, 1).r !is TableModify)
        val fieldNames: List<String> = e.getRowType().getFieldNames()
        if (!dialect.supportsAliasedValues() && rename) {
            // Some dialects (such as Oracle and BigQuery) don't support
            // "AS t (c1, c2)". So instead of
            //   (VALUES (v0, v1), (v2, v3)) AS t (c0, c1)
            // we generate
            //   SELECT v0 AS c0, v1 AS c1 FROM DUAL
            //   UNION ALL
            //   SELECT v2 AS c0, v3 AS c1 FROM DUAL
            // for Oracle and
            //   SELECT v0 AS c0, v1 AS c1
            //   UNION ALL
            //   SELECT v2 AS c0, v3 AS c1
            // for dialects that support SELECT-without-FROM.
            val list: List<SqlSelect> = ArrayList()
            for (tuple in e.getTuples()) {
                val values2: List<SqlNode> = ArrayList()
                val exprList: SqlNodeList = exprList(context, tuple)
                for (value in Pair.zip(exprList, fieldNames)) {
                    values2.add(`as`(value.left, value.right))
                }
                list.add(
                    SqlSelect(
                        POS, null,
                        SqlNodeList(values2, POS),
                        dual, null, null,
                        null, null, null, null, null, null
                    )
                )
            }
            if (list.isEmpty()) {
                // In this case we need to construct the following query:
                // SELECT NULL as C0, NULL as C1, NULL as C2 ... FROM DUAL WHERE FALSE
                // This would return an empty result set with the same number of columns as the field names.
                val nullColumnNames: List<SqlNode> = ArrayList(fieldNames.size())
                for (fieldName in fieldNames) {
                    val nullColumnName: SqlCall = `as`(SqlLiteral.createNull(POS), fieldName)
                    nullColumnNames.add(nullColumnName)
                }
                val dual: SqlIdentifier? = dual
                if (dual == null) {
                    query = SqlSelect(
                        POS, null,
                        SqlNodeList(nullColumnNames, POS), null, null, null, null,
                        null, null, null, null, null
                    )

                    // Wrap "SELECT 1 AS x"
                    // as "SELECT * FROM (SELECT 1 AS x) AS t WHERE false"
                    query = SqlSelect(
                        POS, null, SqlNodeList.SINGLETON_STAR,
                        `as`(query, "t"), createAlwaysFalseCondition(), null, null,
                        null, null, null, null, null
                    )
                } else {
                    query = SqlSelect(
                        POS, null,
                        SqlNodeList(nullColumnNames, POS),
                        dual, createAlwaysFalseCondition(), null,
                        null, null, null, null, null, null
                    )
                }
            } else if (list.size() === 1) {
                query = list[0]
            } else {
                query = SqlStdOperatorTable.UNION_ALL.createCall(
                    SqlNodeList(list, POS)
                )
            }
        } else {
            // Generate ANSI syntax
            //   (VALUES (v0, v1), (v2, v3))
            // or, if rename is required
            //   (VALUES (v0, v1), (v2, v3)) AS t (c0, c1)
            val selects = SqlNodeList(POS)
            val isEmpty: Boolean = Values.isEmpty(e)
            if (isEmpty) {
                // In case of empty values, we need to build:
                //   SELECT *
                //   FROM (VALUES (NULL, NULL ...)) AS T (C1, C2 ...)
                //   WHERE 1 = 0
                selects.add(
                    SqlInternalOperators.ANONYMOUS_ROW.createCall(
                        POS,
                        Collections.nCopies(
                            fieldNames.size(),
                            SqlLiteral.createNull(POS)
                        )
                    )
                )
            } else {
                for (tuple in e.getTuples()) {
                    selects.add(
                        SqlInternalOperators.ANONYMOUS_ROW.createCall(
                            exprList(context, tuple)
                        )
                    )
                }
            }
            query = SqlStdOperatorTable.VALUES.createCall(selects)
            if (rename) {
                query = `as`(query, "t", fieldNames.toArray(arrayOfNulls<String>(0)))
            }
            if (isEmpty) {
                if (!rename) {
                    query = `as`(query, "t")
                }
                query = SqlSelect(
                    POS, null, SqlNodeList.SINGLETON_STAR, query,
                    createAlwaysFalseCondition(), null, null, null,
                    null, null, null, null
                )
            }
        }
        return result(query, clauses, e, null)
    }

    @get:Nullable
    private val dual: SqlIdentifier?
        private get() {
            val names: List<String> = dialect.getSingleRowTableName() ?: return null
            return SqlIdentifier(names, POS)
        }

    /** Visits a Sort; called by [.dispatch] via reflection.  */
    fun visit(e: Sort): Result {
        if (e.getInput() is Aggregate) {
            val aggregate: Aggregate = e.getInput() as Aggregate
            if (hasTrickyRollup(e, aggregate)) {
                // MySQL 5 does not support standard "GROUP BY ROLLUP(x, y)", only
                // the non-standard "GROUP BY x, y WITH ROLLUP".
                // It does not allow "WITH ROLLUP" in combination with "ORDER BY",
                // but "GROUP BY x, y WITH ROLLUP" implicitly sorts by x, y,
                // so skip the ORDER BY.
                val groupList: Set<Integer> = LinkedHashSet()
                for (fc in e.collation.getFieldCollations()) {
                    groupList.add(aggregate.getGroupSet().nth(fc.getFieldIndex()))
                }
                groupList.addAll(Aggregate.Group.getRollup(aggregate.getGroupSets()))
                val builder: Builder = visitAggregate(
                    aggregate, ImmutableList.copyOf(groupList),
                    Clause.GROUP_BY, Clause.OFFSET, Clause.FETCH
                )
                offsetFetch(e, builder)
                return builder.result()
            }
        }
        if (e.getInput() is Project) {
            // Deal with the case Sort(Project(Aggregate ...))
            // by converting it to Project(Sort(Aggregate ...)).
            val project: Project = e.getInput() as Project
            val permutation: Permutation = project.getPermutation()
            if (permutation != null
                && project.getInput() is Aggregate
            ) {
                val aggregate: Aggregate = project.getInput() as Aggregate
                if (hasTrickyRollup(e, aggregate)) {
                    val collation: RelCollation = RelCollations.permute(e.collation, permutation)
                    val sort2: Sort = LogicalSort.create(aggregate, collation, e.offset, e.fetch)
                    val project2: Project = LogicalProject.create(
                        sort2,
                        ImmutableList.of(),
                        project.getProjects(),
                        project.getRowType()
                    )
                    return visit(project2)
                }
            }
        }
        val x: Result = visitInput(
            e, 0, Clause.ORDER_BY, Clause.OFFSET,
            Clause.FETCH
        )
        val builder: Builder = x.builder(e)
        if (stack.size() !== 1
            && builder.select.getSelectList().equals(SqlNodeList.SINGLETON_STAR)
        ) {
            // Generates explicit column names instead of start(*) for
            // non-root order by to avoid ambiguity.
            val selectList: List<SqlNode> = Expressions.list()
            for (field in e.getRowType().getFieldList()) {
                addSelect(selectList, builder.context.field(field.getIndex()), e.getRowType())
            }
            builder.select.setSelectList(SqlNodeList(selectList, POS))
        }
        val orderByList: List<SqlNode> = Expressions.list()
        for (field in e.getCollation().getFieldCollations()) {
            builder.addOrderItem(orderByList, field)
        }
        if (!orderByList.isEmpty()) {
            builder.setOrderBy(SqlNodeList(orderByList, POS))
        }
        offsetFetch(e, builder)
        return builder.result()
    }

    /** Adds OFFSET and FETCH to a builder, if applicable.
     * The builder must have been created with OFFSET and FETCH clauses.  */
    fun offsetFetch(e: Sort, builder: Builder) {
        if (e.fetch != null) {
            builder.setFetch(builder.context.toSql(null, e.fetch))
        }
        if (e.offset != null) {
            builder.setOffset(builder.context.toSql(null, e.offset))
        }
    }

    fun hasTrickyRollup(e: Sort, aggregate: Aggregate): Boolean {
        return (!dialect.supportsAggregateFunction(SqlKind.ROLLUP)
                && dialect.supportsGroupByWithRollup()
                && (aggregate.getGroupType() === Aggregate.Group.ROLLUP
                || aggregate.getGroupType() === Aggregate.Group.CUBE
                && aggregate.getGroupSet().cardinality() === 1)
                && e.collation.getFieldCollations().stream()
            .allMatch { fc -> fc.getFieldIndex() < aggregate.getGroupSet().cardinality() })
    }

    /** Visits a TableModify; called by [.dispatch] via reflection.  */
    fun visit(modify: TableModify): Result {
        val pairs: Map<String, RelDataType> = ImmutableMap.of()
        val context: Context = aliasContext(pairs, false)

        // Target Table Name
        val sqlTargetTable: SqlIdentifier = getSqlTargetTable(modify)
        return when (modify.getOperation()) {
            INSERT -> {

                // Convert the input to a SELECT query or keep as VALUES. Not all
                // dialects support naked VALUES, but all support VALUES inside INSERT.
                val sqlSource: SqlNode = visitInput(modify, 0).asQueryOrValues()
                val sqlInsert = SqlInsert(
                    POS, SqlNodeList.EMPTY, sqlTargetTable, sqlSource,
                    identifierList(
                        modify.getTable().getRowType().getFieldNames()
                    )
                )
                result(sqlInsert, ImmutableList.of(), modify, null)
            }
            UPDATE -> {
                val input: Result = visitInput(modify, 0)
                val sqlUpdate = SqlUpdate(
                    POS, sqlTargetTable,
                    identifierList(
                        requireNonNull(
                            modify.getUpdateColumnList()
                        ) { "modify.getUpdateColumnList() is null for $modify" }),
                    exprList(context,
                        requireNonNull(
                            modify.getSourceExpressionList()
                        ) { "modify.getSourceExpressionList() is null for $modify" }),
                    (input.node as SqlSelect).getWhere(), input.asSelect(),
                    null
                )
                result(sqlUpdate, input.clauses, modify, null)
            }
            DELETE -> {
                val input: Result = visitInput(modify, 0)
                val sqlDelete = SqlDelete(
                    POS, sqlTargetTable,
                    input.asSelect().getWhere(), input.asSelect(), null
                )
                result(sqlDelete, input.clauses, modify, null)
            }
            MERGE -> throw AssertionError("not implemented: $modify")
            else -> throw AssertionError("not implemented: $modify")
        }
    }

    /** Visits a Match; called by [.dispatch] via reflection.  */
    fun visit(e: Match): Result {
        val input: RelNode = e.getInput()
        val x: Result = visitInput(e, 0)
        val context: Context = matchRecognizeContext(x.qualifiedContext())
        val tableRef: SqlNode = x.asQueryOrValues()
        val rexBuilder: RexBuilder = input.getCluster().getRexBuilder()
        val partitionSqlList: List<SqlNode> = ArrayList()
        for (key in e.getPartitionKeys()) {
            val ref: RexInputRef = rexBuilder.makeInputRef(input, key)
            val sqlNode: SqlNode = context.toSql(null, ref)
            partitionSqlList.add(sqlNode)
        }
        val partitionList = SqlNodeList(partitionSqlList, POS)
        val orderBySqlList: List<SqlNode> = ArrayList()
        if (e.getOrderKeys() != null) {
            for (fc in e.getOrderKeys().getFieldCollations()) {
                if (fc.nullDirection !== RelFieldCollation.NullDirection.UNSPECIFIED) {
                    val first = fc.nullDirection === RelFieldCollation.NullDirection.FIRST
                    val nullDirectionNode: SqlNode = dialect.emulateNullDirection(
                        context.field(fc.getFieldIndex()),
                        first, fc.direction.isDescending()
                    )
                    if (nullDirectionNode != null) {
                        orderBySqlList.add(nullDirectionNode)
                        fc = RelFieldCollation(
                            fc.getFieldIndex(), fc.getDirection(),
                            RelFieldCollation.NullDirection.UNSPECIFIED
                        )
                    }
                }
                orderBySqlList.add(context.toSql(fc))
            }
        }
        val orderByList = SqlNodeList(orderBySqlList, SqlParserPos.ZERO)
        val rowsPerMatch: SqlLiteral =
            if (e.isAllRows()) SqlMatchRecognize.RowsPerMatchOption.ALL_ROWS.symbol(POS) else SqlMatchRecognize.RowsPerMatchOption.ONE_ROW.symbol(
                POS
            )
        val after: SqlNode
        after = if (e.getAfter() is RexLiteral) {
            val value: SqlMatchRecognize.AfterOption =
                (e.getAfter() as RexLiteral).getValue2() as SqlMatchRecognize.AfterOption
            SqlLiteral.createSymbol(value, POS)
        } else {
            val call: RexCall = e.getAfter() as RexCall
            val operand: String = requireNonNull(
                stringValue(call.getOperands().get(0))
            ) { "non-null string value expected for 0th operand of AFTER call $call" }
            call.getOperator().createCall(POS, SqlIdentifier(operand, POS))
        }
        val rexPattern: RexNode = e.getPattern()
        val pattern: SqlNode = context.toSql(null, rexPattern)
        val strictStart: SqlLiteral = SqlLiteral.createBoolean(e.isStrictStart(), POS)
        val strictEnd: SqlLiteral = SqlLiteral.createBoolean(e.isStrictEnd(), POS)
        val rexInterval: RexLiteral = e.getInterval() as RexLiteral
        var interval: SqlIntervalLiteral? = null
        if (rexInterval != null) {
            interval = context.toSql(null, rexInterval) as SqlIntervalLiteral
        }
        val subsetList = SqlNodeList(POS)
        for (entry in e.getSubsets().entrySet()) {
            val left: SqlNode = SqlIdentifier(entry.getKey(), POS)
            val rhl: List<SqlNode> = ArrayList()
            for (right in entry.getValue()) {
                rhl.add(SqlIdentifier(right, POS))
            }
            subsetList.add(
                SqlStdOperatorTable.EQUALS.createCall(
                    POS, left,
                    SqlNodeList(rhl, POS)
                )
            )
        }
        val measureList = SqlNodeList(POS)
        for (entry in e.getMeasures().entrySet()) {
            val alias: String = entry.getKey()
            val sqlNode: SqlNode = context.toSql(null, entry.getValue())
            measureList.add(`as`(sqlNode, alias))
        }
        val patternDefList = SqlNodeList(POS)
        for (entry in e.getPatternDefinitions().entrySet()) {
            val alias: String = entry.getKey()
            val sqlNode: SqlNode = context.toSql(null, entry.getValue())
            patternDefList.add(`as`(sqlNode, alias))
        }
        val matchRecognize: SqlNode = SqlMatchRecognize(
            POS, tableRef,
            pattern, strictStart, strictEnd, patternDefList, measureList, after,
            subsetList, rowsPerMatch, partitionList, orderByList, interval
        )
        return result(matchRecognize, Expressions.list(Clause.FROM), e, null)
    }

    fun visit(e: Uncollect): Result {
        val x: Result = visitInput(e, 0)
        val unnestNode: SqlNode = SqlStdOperatorTable.UNNEST.createCall(POS, x.asStatement())
        val operands: List<SqlNode> = createAsFullOperands(e.getRowType(), unnestNode,
            requireNonNull(x.neededAlias) { "x.neededAlias is null, node is " + x.node })
        val asNode: SqlNode = SqlStdOperatorTable.AS.createCall(POS, operands)
        return result(asNode, ImmutableList.of(Clause.FROM), e, null)
    }

    fun visit(e: TableFunctionScan): Result {
        val inputSqlNodes: List<SqlNode> = ArrayList()
        val inputSize: Int = e.getInputs().size()
        for (i in 0 until inputSize) {
            val x: Result = visitInput(e, i)
            inputSqlNodes.add(x.asStatement())
        }
        val context: Context = tableFunctionScanContext(inputSqlNodes)
        val callNode: SqlNode = context.toSql(null, e.getCall())
        // Convert to table function call, "TABLE($function_name(xxx))"
        val tableCall: SqlNode = SqlBasicCall(
            SqlStdOperatorTable.COLLECTION_TABLE,
            ImmutableList.of(callNode), SqlParserPos.ZERO
        )
        val select: SqlNode = SqlSelect(
            SqlParserPos.ZERO, null, SqlNodeList.SINGLETON_STAR,
            tableCall, null, null, null, null, null, null, null,
            SqlNodeList.EMPTY
        )
        return result(select, ImmutableList.of(Clause.SELECT), e, null)
    }

    /**
     * Creates operands for a full AS operator. Format SqlNode AS alias(col_1, col_2,... ,col_n).
     *
     * @param rowType Row type of the SqlNode
     * @param leftOperand SqlNode
     * @param alias alias
     */
    fun createAsFullOperands(
        rowType: RelDataType, leftOperand: SqlNode?,
        alias: String?
    ): List<SqlNode> {
        val result: List<SqlNode> = ArrayList()
        result.add(leftOperand)
        result.add(SqlIdentifier(alias, POS))
        Ord.forEach(rowType.getFieldNames()) { fieldName, i ->
            if (SqlUtil.isGeneratedAlias(fieldName)) {
                fieldName = "col_$i"
            }
            result.add(SqlIdentifier(fieldName, POS))
        }
        return result
    }

    @Override
    override fun addSelect(
        selectList: List<SqlNode?>, node: SqlNode,
        rowType: RelDataType
    ) {
        var node: SqlNode = node
        val name: String = rowType.getFieldNames().get(selectList.size())
        val alias: String = SqlValidatorUtil.getAlias(node, -1)
        if (alias == null || !alias.equals(name)) {
            node = `as`(node, name)
        }
        selectList.add(node)
    }

    private fun parseCorrelTable(relNode: RelNode, x: Result) {
        for (id in relNode.getVariablesSet()) {
            correlTableMap.put(id, x.qualifiedContext())
        }
    }

    /** Stack frame.  */
    private class Frame internal constructor(
        parent: RelNode?, ordinalInParent: Int, r: RelNode?, anon: Boolean,
        ignoreClauses: Boolean, expectedClauses: Iterable<Clause?>?
    ) {
        val parent: RelNode

        @SuppressWarnings("unused")
        private val ordinalInParent: Int
        val r: RelNode
        val anon: Boolean
        val ignoreClauses: Boolean
        val expectedClauses: ImmutableSet<out Clause?>

        init {
            this.parent = requireNonNull(parent, "parent")
            this.ordinalInParent = ordinalInParent
            this.r = requireNonNull(r, "r")
            this.anon = anon
            this.ignoreClauses = ignoreClauses
            this.expectedClauses = ImmutableSet.copyOf(expectedClauses)
        }
    }

    companion object {
        private fun groupItem(
            groupKeys: List<SqlNode>,
            groupSet: ImmutableBitSet, wholeGroupSet: ImmutableBitSet
        ): SqlNode {
            val nodes: List<SqlNode> = groupSet.asList().stream()
                .map { key -> groupKeys[wholeGroupSet.indexOf(key)] }
                .collect(Collectors.toList())
            return when (nodes.size()) {
                1 -> nodes[0]
                else -> SqlStdOperatorTable.ROW.createCall(SqlParserPos.ZERO, nodes)
            }
        }

        private fun toSqlHint(hint: RelHint, pos: SqlParserPos): SqlHint {
            if (hint.kvOptions != null) {
                return SqlHint(pos, SqlIdentifier(hint.hintName, pos),
                    SqlNodeList.of(pos, hint.kvOptions.entrySet().stream()
                        .flatMap { e ->
                            Stream.of(
                                SqlIdentifier(e.getKey(), pos),
                                SqlLiteral.createCharString(e.getValue(), pos)
                            )
                        }
                        .collect(Collectors.toList())),
                    SqlHint.HintOptionFormat.KV_LIST)
            } else if (hint.listOptions != null) {
                return SqlHint(pos, SqlIdentifier(hint.hintName, pos),
                    SqlNodeList.of(pos, hint.listOptions.stream()
                        .map { e -> SqlLiteral.createCharString(e, pos) }
                        .collect(Collectors.toList())),
                    SqlHint.HintOptionFormat.LITERAL_LIST)
            }
            return SqlHint(
                pos, SqlIdentifier(hint.hintName, pos),
                SqlNodeList.EMPTY, SqlHint.HintOptionFormat.EMPTY
            )
        }

        private fun createAlwaysFalseCondition(): SqlNode {
            // Building the select query in the form:
            // select * from VALUES(NULL,NULL ...) where 1=0
            // Use condition 1=0 since "where false" does not seem to be supported
            // on some DB vendors.
            return SqlStdOperatorTable.EQUALS.createCall(
                POS,
                ImmutableList.of(ONE, ZERO)
            )
        }

        private fun getSqlTargetTable(e: RelNode): SqlIdentifier {
            // Use the foreign catalog, schema and table names, if they exist,
            // rather than the qualified name of the shadow table in Calcite.
            val table: RelOptTable = requireNonNull(e.getTable())
            return table.maybeUnwrap(JdbcTable::class.java)
                .map(JdbcTable::tableName)
                .orElseGet { SqlIdentifier(table.getQualifiedName(), SqlParserPos.ZERO) }
        }

        /** Converts a list of [RexNode] expressions to [SqlNode]
         * expressions.  */
        private fun exprList(
            context: Context,
            exprs: List<RexNode?>
        ): SqlNodeList {
            return SqlNodeList(
                Util.transform(exprs) { e -> context.toSql(null, e) }, POS
            )
        }

        /** Converts a list of names expressions to a list of single-part
         * [SqlIdentifier]s.  */
        private fun identifierList(names: List<String>): SqlNodeList {
            return SqlNodeList(
                Util.transform(names) { name -> SqlIdentifier(name, POS) }, POS
            )
        }

        private fun `as`(e: SqlNode, alias: String?): SqlCall {
            return SqlStdOperatorTable.AS.createCall(
                POS, e,
                SqlIdentifier(alias, POS)
            )
        }
    }
}
