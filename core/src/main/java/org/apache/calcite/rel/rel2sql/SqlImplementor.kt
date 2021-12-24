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

import org.apache.calcite.linq4j.Ord
import org.apache.calcite.linq4j.tree.Expressions
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rel.RelFieldCollation
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.SingleRel
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.core.CorrelationId
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rel.core.Project
import org.apache.calcite.rel.core.Window
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.rel.type.RelDataTypeSystemImpl
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexCall
import org.apache.calcite.rex.RexCorrelVariable
import org.apache.calcite.rex.RexDynamicParam
import org.apache.calcite.rex.RexFieldAccess
import org.apache.calcite.rex.RexFieldCollation
import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.rex.RexLiteral
import org.apache.calcite.rex.RexLocalRef
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rex.RexOver
import org.apache.calcite.rex.RexPatternFieldRef
import org.apache.calcite.rex.RexProgram
import org.apache.calcite.rex.RexShuttle
import org.apache.calcite.rex.RexSubQuery
import org.apache.calcite.rex.RexUnknownAs
import org.apache.calcite.rex.RexUtil
import org.apache.calcite.rex.RexWindow
import org.apache.calcite.rex.RexWindowBound
import org.apache.calcite.sql.JoinType
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.SqlBasicCall
import org.apache.calcite.sql.SqlBinaryOperator
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.SqlDynamicParam
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlJoin
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlMatchRecognize
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlNodeList
import org.apache.calcite.sql.SqlNumericLiteral
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.SqlOverOperator
import org.apache.calcite.sql.SqlSelect
import org.apache.calcite.sql.SqlSelectKeyword
import org.apache.calcite.sql.SqlSetOperator
import org.apache.calcite.sql.SqlTableRef
import org.apache.calcite.sql.SqlUtil
import org.apache.calcite.sql.SqlWindow
import org.apache.calcite.sql.`fun`.SqlCase
import org.apache.calcite.sql.`fun`.SqlCountAggFunction
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.`fun`.SqlSumEmptyIsZeroAggFunction
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeFactoryImpl
import org.apache.calcite.sql.type.SqlTypeFamily
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.sql.validate.SqlValidatorUtil
import org.apache.calcite.util.DateString
import org.apache.calcite.util.Pair
import org.apache.calcite.util.RangeSets
import org.apache.calcite.util.Sarg
import org.apache.calcite.util.TimeString
import org.apache.calcite.util.TimestampString
import org.apache.calcite.util.Util
import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import com.google.common.collect.ImmutableSet
import com.google.common.collect.Iterables
import com.google.common.collect.Lists
import com.google.common.collect.Range
import com.google.common.collect.RangeSet
import org.checkerframework.checker.initialization.qual.UnknownInitialization
import java.math.BigDecimal
import java.util.AbstractList
import java.util.ArrayDeque
import java.util.ArrayList
import java.util.Collection
import java.util.Collections
import java.util.Deque
import java.util.HashMap
import java.util.HashSet
import java.util.Iterator
import java.util.LinkedHashMap
import java.util.LinkedHashSet
import java.util.List
import java.util.Map
import java.util.Set
import java.util.function.Function
import java.util.function.IntFunction
import java.util.function.Predicate
import org.apache.calcite.linq4j.Nullness.castNonNull
import java.util.Objects.requireNonNull

/**
 * State for generating a SQL statement.
 */
abstract class SqlImplementor protected constructor(dialect: SqlDialect?) {
    val dialect: SqlDialect
    protected val aliasSet: Set<String> = LinkedHashSet()
    protected val correlTableMap: Map<CorrelationId, Context> = HashMap()

    /** Private RexBuilder for short-lived expressions. It has its own
     * dedicated type factory, so don't trust the types to be canonized.  */
    val rexBuilder: RexBuilder = RexBuilder(SqlTypeFactoryImpl(RelDataTypeSystemImpl.DEFAULT))

    init {
        this.dialect = requireNonNull(dialect, "dialect")
    }

    /** Visits a relational expression that has no parent.  */
    fun visitRoot(r: RelNode): Result {
        return try {
            visitInput(holder(r), 0)
        } catch (e: Error) {
            throw Util.throwAsRuntime(
                """
    Error while converting RelNode to SqlNode:
    ${RelOptUtil.toString(r)}
    """.trimIndent(), e
            )
        } catch (e: RuntimeException) {
            throw Util.throwAsRuntime(
                """
    Error while converting RelNode to SqlNode:
    ${RelOptUtil.toString(r)}
    """.trimIndent(), e
            )
        }
    }
    // CHECKSTYLE: IGNORE 1

    @Deprecated // to be removed before 2.0
    @Deprecated(
        """Use either {@link #visitRoot(RelNode)} or
    {@link #visitInput(RelNode, int)}. """
    )
    fun visitChild(i: Int, e: RelNode?): Result {
        throw UnsupportedOperationException()
    }

    /** Visits an input of the current relational expression,
     * deducing `anon` using [.isAnon].  */
    fun visitInput(e: RelNode?, i: Int): Result {
        return visitInput(e, i, ImmutableSet.of())
    }

    /** Visits an input of the current relational expression,
     * with the given expected clauses.  */
    fun visitInput(e: RelNode?, i: Int, vararg clauses: Clause?): Result {
        return visitInput(e, i, ImmutableSet.copyOf(clauses))
    }

    /** Visits an input of the current relational expression,
     * deducing `anon` using [.isAnon].  */
    fun visitInput(e: RelNode?, i: Int, clauses: Set<Clause?>?): Result {
        return visitInput(e, i, isAnon, false, clauses)
    }

    /** Visits the `i`th input of `e`, the current relational
     * expression.
     *
     * @param e Current relational expression
     * @param i Ordinal of input within `e`
     * @param anon Whether to remove trivial aliases such as "EXPR$0"
     * @param ignoreClauses Whether to ignore the expected clauses when deciding
     * whether a sub-query is required
     * @param expectedClauses Set of clauses that we expect the builder that
     * consumes this result will create
     * @return Result
     *
     * @see .isAnon
     */
    abstract fun visitInput(
        e: RelNode?, i: Int, anon: Boolean,
        ignoreClauses: Boolean, expectedClauses: Set<Clause?>?
    ): Result

    fun addSelect(
        selectList: List<SqlNode?>, node: SqlNode?,
        rowType: RelDataType
    ) {
        var node: SqlNode? = node
        val name: String = rowType.getFieldNames().get(selectList.size())
        val alias: String = SqlValidatorUtil.getAlias(node, -1)
        if (alias == null || !alias.equals(name)) {
            node = `as`(node, name)
        }
        selectList.add(node)
    }

    /** Convenience method for creating column and table aliases.
     *
     *
     * `AS(e, "c")` creates "e AS c";
     * `AS(e, "t", "c1", "c2"` creates "e AS t (c1, c2)".  */
    protected fun `as`(e: SqlNode?, alias: String?, vararg fieldNames: String?): SqlCall {
        val operandList: List<SqlNode> = ArrayList()
        operandList.add(e)
        operandList.add(SqlIdentifier(alias, POS))
        for (fieldName in fieldNames) {
            operandList.add(SqlIdentifier(fieldName, POS))
        }
        return SqlStdOperatorTable.AS.createCall(POS, operandList)
    }

    fun setOpToSql(operator: SqlSetOperator, rel: RelNode): Result {
        var node: SqlNode? = null
        for (input in Ord.zip(rel.getInputs())) {
            val result = visitInput(rel, input.i)
            node = if (node == null) {
                result.asSelect()
            } else {
                operator.createCall(POS, node, result.asSelect())
            }
        }
        assert(node != null) {
            ("set op must have at least one input, operator = " + operator
                    + ", rel = " + rel)
        }
        val clauses: List<Clause> = Expressions.list(Clause.SET_OP)
        return result(node, clauses, rel, null)
    }

    /** Creates a result based on a single relational expression.  */
    fun result(
        node: SqlNode?, clauses: Collection<Clause?>?,
        rel: RelNode, @Nullable aliases: Map<String?, RelDataType?>?
    ): Result {
        assert(
            aliases == null || aliases.size() < 2 || aliases is LinkedHashMap
                    || aliases is ImmutableMap
        ) { "must use a Map implementation that preserves order" }
        val alias2: String = SqlValidatorUtil.getAlias(node, -1)
        val alias3 = alias2 ?: "t"
        val alias4: String = SqlValidatorUtil.uniquify(
            alias3, aliasSet, SqlValidatorUtil.EXPR_SUGGESTER
        )
        val rowType: RelDataType = adjustedRowType(rel, node)
        if (aliases != null && !aliases.isEmpty()
            && (!dialect.hasImplicitTableAlias()
                    || aliases.size() > 1)
        ) {
            return result(node, clauses, alias4, rowType, aliases)
        }
        val alias5: String?
        alias5 = if (alias2 == null || !alias2.equals(alias4)
            || !dialect.hasImplicitTableAlias()
        ) {
            alias4
        } else {
            null
        }
        return result(
            node, clauses, alias5, rowType,
            ImmutableMap.of(alias4, rowType)
        )
    }

    /** Factory method for [Result].
     *
     *
     * Call this method rather than creating a `Result` directly,
     * because sub-classes may override.  */
    protected fun result(
        node: SqlNode?, clauses: Collection<Clause?>?,
        @Nullable neededAlias: String?, @Nullable neededType: RelDataType?,
        aliases: Map<String?, RelDataType?>?
    ): Result {
        return Result(node, clauses, neededAlias, neededType, aliases)
    }

    /** Creates a result based on a join. (Each join could contain one or more
     * relational expressions.)  */
    fun result(join: SqlNode, leftResult: Result, rightResult: Result): Result {
        val aliases: Map<String?, RelDataType>
        aliases = if (join.getKind() === SqlKind.JOIN) {
            val builder: ImmutableMap.Builder<String, RelDataType> = ImmutableMap.builder()
            collectAliases(
                builder, join,
                Iterables.concat(
                    leftResult.aliases.values(),
                    rightResult.aliases.values()
                ).iterator()
            )
            builder.build()
        } else {
            leftResult.aliases
        }
        return result(join, ImmutableList.of(Clause.FROM), null, null, aliases)
    }

    /** Returns whether to remove trivial aliases such as "EXPR$0"
     * when converting the current relational expression into a SELECT.
     *
     *
     * For example, INSERT does not care about field names;
     * we would prefer to generate without the "EXPR$0" alias:
     *
     * <pre>`INSERT INTO t1 SELECT x, y + 1 FROM t2`</pre>
     *
     * rather than with it:
     *
     * <pre>`INSERT INTO t1 SELECT x, y + 1 AS EXPR$0 FROM t2`</pre>
     *
     *
     * But JOIN does care about field names; we have to generate the "EXPR$0"
     * alias:
     *
     * <pre>`SELECT *
     * FROM emp AS e
     * JOIN (SELECT x, y + 1 AS EXPR$0) AS d
     * ON e.deptno = d.EXPR$0`
    </pre> *
     *
     *
     * because if we omit "AS EXPR$0" we do not know the field we are joining
     * to, and the following is invalid:
     *
     * <pre>`SELECT *
     * FROM emp AS e
     * JOIN (SELECT x, y + 1) AS d
     * ON e.deptno = d.EXPR$0`
    </pre> *
     */
    protected val isAnon: Boolean
        protected get() = false

    /** Wraps a node in a SELECT statement that has no clauses:
     * "SELECT ... FROM (node)".  */
    fun wrapSelect(node: SqlNode): SqlSelect {
        var node: SqlNode = node
        assert(
            node is SqlJoin
                    || node is SqlIdentifier
                    || node is SqlMatchRecognize
                    || node is SqlTableRef
                    || (node is SqlCall
                    && ((node as SqlCall).getOperator() is SqlSetOperator
                    || (node as SqlCall).getOperator() === SqlStdOperatorTable.AS || (node as SqlCall).getOperator() === SqlStdOperatorTable.VALUES))
        ) { node }
        if (requiresAlias(node)) {
            node = `as`(node, "t")
        }
        return SqlSelect(
            POS, SqlNodeList.EMPTY, SqlNodeList.SINGLETON_STAR,
            node, null, null, null, SqlNodeList.EMPTY, null, null, null, null
        )
    }

    /** Returns whether we need to add an alias if this node is to be the FROM
     * clause of a SELECT.  */
    private fun requiresAlias(node: SqlNode): Boolean {
        return if (!dialect.requiresAliasForFromItems()) {
            false
        } else when (node.getKind()) {
            IDENTIFIER -> !dialect.hasImplicitTableAlias()
            AS, JOIN, EXPLICIT_TABLE -> false
            else -> true
        }
    }

    /** Context for translating a [RexNode] expression (within a
     * [RelNode]) into a [SqlNode] expression (within a SQL parse
     * tree).  */
    abstract class Context protected constructor(dialect: SqlDialect, fieldCount: Int, ignoreCast: Boolean) {
        val dialect: SqlDialect
        val fieldCount: Int
        private val ignoreCast: Boolean

        protected constructor(dialect: SqlDialect, fieldCount: Int) : this(dialect, fieldCount, false) {}

        init {
            this.dialect = dialect
            this.fieldCount = fieldCount
            this.ignoreCast = ignoreCast
        }

        abstract fun field(ordinal: Int): SqlNode

        /** Creates a reference to a field to be used in an ORDER BY clause.
         *
         *
         * By default, it returns the same result as [.field].
         *
         *
         * If the field has an alias, uses the alias.
         * If the field is an unqualified column reference which is the same an
         * alias, switches to a qualified column reference.
         */
        fun orderField(ordinal: Int): SqlNode {
            return field(ordinal)
        }

        /** Converts an expression from [RexNode] to [SqlNode]
         * format.
         *
         * @param program Required only if `rex` contains [RexLocalRef]
         * @param rex Expression to convert
         */
        fun toSql(@Nullable program: RexProgram?, rex: RexNode): SqlNode {
            val subQuery: RexSubQuery
            val sqlSubQuery: SqlNode
            val literal: RexLiteral
            return when (rex.getKind()) {
                LOCAL_REF -> {
                    val index: Int = (rex as RexLocalRef).getIndex()
                    toSql(program, requireNonNull(program, "program").getExprList().get(index))
                }
                INPUT_REF -> field((rex as RexInputRef).getIndex())
                FIELD_ACCESS -> {
                    val accesses: Deque<RexFieldAccess> = ArrayDeque()
                    var referencedExpr: RexNode = rex
                    while (referencedExpr.getKind() === SqlKind.FIELD_ACCESS) {
                        accesses.offerLast(referencedExpr as RexFieldAccess)
                        referencedExpr = (referencedExpr as RexFieldAccess).getReferenceExpr()
                    }
                    var sqlIdentifier: SqlIdentifier
                    when (referencedExpr.getKind()) {
                        CORREL_VARIABLE -> {
                            val variable: RexCorrelVariable = referencedExpr as RexCorrelVariable
                            val correlAliasContext = getAliasContext(variable)
                            val lastAccess: RexFieldAccess = accesses.pollLast()
                            assert(lastAccess != null)
                            sqlIdentifier = correlAliasContext
                                .field(lastAccess.getField().getIndex()) as SqlIdentifier
                        }
                        ROW -> {
                            val expr: SqlNode = toSql(program, referencedExpr)
                            sqlIdentifier =
                                SqlIdentifier(expr.toString(), POS)
                        }
                        else -> sqlIdentifier = toSql(program, referencedExpr) as SqlIdentifier
                    }
                    var nameIndex: Int = sqlIdentifier.names.size()
                    var access: RexFieldAccess
                    while (accesses.pollLast().also { access = it } != null) {
                        sqlIdentifier = sqlIdentifier.add(
                            nameIndex++,
                            access.getField().getName(),
                            POS
                        )
                    }
                    sqlIdentifier
                }
                PATTERN_INPUT_REF -> {
                    val ref: RexPatternFieldRef = rex as RexPatternFieldRef
                    val pv: String = ref.getAlpha()
                    val refNode: SqlNode = field(ref.getIndex())
                    val id: SqlIdentifier = refNode as SqlIdentifier
                    if (id.names.size() > 1) {
                        id.setName(0, pv)
                    } else {
                        SqlIdentifier(
                            ImmutableList.of(pv, id.names.get(0)),
                            POS
                        )
                    }
                }
                LITERAL -> toSql(program, rex as RexLiteral)
                CASE -> {
                    val caseCall: RexCall = rex as RexCall
                    val caseNodeList: List<SqlNode> = toSql(program, caseCall.getOperands())
                    val valueNode: SqlNode?
                    val whenList: List<SqlNode> = Expressions.list()
                    val thenList: List<SqlNode> = Expressions.list()
                    val elseNode: SqlNode
                    if (caseNodeList.size() % 2 === 0) {
                        // switched:
                        //   "case x when v1 then t1 when v2 then t2 ... else e end"
                        valueNode = caseNodeList[0]
                        var i = 1
                        while (i < caseNodeList.size() - 1) {
                            whenList.add(caseNodeList[i])
                            thenList.add(caseNodeList[i + 1])
                            i += 2
                        }
                    } else {
                        // other: "case when w1 then t1 when w2 then t2 ... else e end"
                        valueNode = null
                        var i = 0
                        while (i < caseNodeList.size() - 1) {
                            whenList.add(caseNodeList[i])
                            thenList.add(caseNodeList[i + 1])
                            i += 2
                        }
                    }
                    elseNode = caseNodeList[caseNodeList.size() - 1]
                    SqlCase(
                        POS,
                        valueNode,
                        SqlNodeList(whenList, POS),
                        SqlNodeList(thenList, POS),
                        elseNode
                    )
                }
                DYNAMIC_PARAM -> {
                    val caseParam: RexDynamicParam = rex as RexDynamicParam
                    SqlDynamicParam(caseParam.getIndex(), POS)
                }
                IN -> {
                    subQuery = rex as RexSubQuery
                    sqlSubQuery = implementor().visitRoot(subQuery.rel).asQueryOrValues()
                    val operands: List<RexNode> = subQuery.operands
                    val op0: SqlNode
                    if (operands.size() === 1) {
                        op0 = toSql(program, operands[0])
                    } else {
                        val cols: List<SqlNode> = toSql(program, operands)
                        op0 = SqlNodeList(cols, POS)
                    }
                    subQuery.getOperator()
                        .createCall(POS, op0, sqlSubQuery)
                }
                SEARCH -> {
                    val search: RexCall = rex as RexCall
                    if (search.operands.get(1).getKind() === SqlKind.LITERAL) {
                        literal = search.operands.get(1) as RexLiteral
                        val sarg: Sarg = castNonNull(literal.getValueAs(Sarg::class.java))
                        return toSql(program, search.operands.get(0), literal.getType(), sarg)
                    }
                    toSql(program, RexUtil.expandSearch(implementor().rexBuilder, program, search))
                }
                EXISTS, UNIQUE, SCALAR_QUERY -> {
                    subQuery = rex as RexSubQuery
                    sqlSubQuery = implementor().visitRoot(subQuery.rel).asQueryOrValues()
                    subQuery.getOperator().createCall(POS, sqlSubQuery)
                }
                NOT -> {
                    val operand: RexNode = (rex as RexCall).operands.get(0)
                    val node: SqlNode = toSql(program, operand)
                    val inverseOperator: SqlOperator? =
                        getInverseOperator(operand)
                    if (inverseOperator != null) {
                        when (operand.getKind()) {
                            IN -> assert(operand is RexSubQuery) { "scalar IN is no longer allowed in RexCall: $rex" }
                            else -> {}
                        }
                        inverseOperator.createCall(
                            POS,
                            (node as SqlCall).getOperandList()
                        )
                    } else {
                        SqlStdOperatorTable.NOT.createCall(POS, node)
                    }
                }
                else -> {
                    if (rex is RexOver) {
                        toSql(program, rex as RexOver)
                    } else callToSql(program, rex as RexCall, false)
                }
            }
        }

        private fun callToSql(
            @Nullable program: RexProgram?, call0: RexCall,
            not: Boolean
        ): SqlNode {
            val call1: RexCall = reverseCall(call0)
            val call: RexCall = stripCastFromString(call1, dialect) as RexCall
            var op: SqlOperator = call.getOperator()
            when (op.getKind()) {
                SUM0 -> op = SqlStdOperatorTable.SUM
                NOT -> {
                    val operand: RexNode = call.operands.get(0)
                    if (getInverseOperator(operand) != null) {
                        return callToSql(program, operand as RexCall, !not)
                    }
                }
                else -> {}
            }
            if (not) {
                op = requireNonNull(
                    getInverseOperator(call)
                ) { "unable to negate " + call.getKind() }
            }
            val nodeList: List<SqlNode> = toSql(program, call.getOperands())
            when (call.getKind()) {
                CAST -> {
                    // CURSOR is used inside CAST, like 'CAST ($0): CURSOR NOT NULL',
                    // convert it to sql call of {@link SqlStdOperatorTable#CURSOR}.
                    val dataType: RelDataType = call.getType()
                    if (dataType.getSqlTypeName() === SqlTypeName.CURSOR) {
                        val operand0: RexNode = call.operands.get(0)
                        assert(operand0 is RexInputRef)
                        val ordinal: Int = (operand0 as RexInputRef).getIndex()
                        val fieldOperand: SqlNode = field(ordinal)
                        return SqlStdOperatorTable.CURSOR.createCall(SqlParserPos.ZERO, fieldOperand)
                    }
                    if (ignoreCast) {
                        assert(nodeList.size() === 1)
                        return nodeList[0]
                    } else {
                        nodeList.add(castNonNull(dialect.getCastSpec(call.getType())))
                    }
                }
                else -> {}
            }
            return SqlUtil.createCall(op, POS, nodeList)
        }

        /** Reverses the order of a call, while preserving semantics, if it improves
         * readability.
         *
         *
         * In the base implementation, this method does nothing;
         * in a join context, reverses a call such as
         * "e.deptno = d.deptno" to
         * "d.deptno = e.deptno"
         * if "d" is the left input to the join
         * and "e" is the right.  */
        protected fun reverseCall(call: RexCall): RexCall {
            return call
        }

        /** Converts a Sarg to SQL, generating "operand IN (c1, c2, ...)" if the
         * ranges are all points.  */
        @SuppressWarnings(["BetaApi", "UnstableApiUsage"])
        private fun <C : Comparable<C>?> toSql(
            @Nullable program: RexProgram?,
            operand: RexNode, type: RelDataType, sarg: Sarg<C>
        ): SqlNode {
            val orList: List<SqlNode> = ArrayList()
            val operandSql: SqlNode = toSql(program, operand)
            if (sarg.nullAs === RexUnknownAs.TRUE) {
                orList.add(SqlStdOperatorTable.IS_NULL.createCall(POS, operandSql))
            }
            if (sarg.isPoints()) {
                // generate 'x = 10' or 'x IN (10, 20, 30)'
                orList.add(
                    toIn<Comparable<C>>(
                        operandSql, SqlStdOperatorTable.EQUALS,
                        SqlStdOperatorTable.IN, program, type, sarg.rangeSet
                    )
                )
            } else if (sarg.isComplementedPoints()) {
                // generate 'x <> 10' or 'x NOT IN (10, 20, 30)'
                orList.add(
                    toIn(
                        operandSql, SqlStdOperatorTable.NOT_EQUALS,
                        SqlStdOperatorTable.NOT_IN, program, type,
                        sarg.rangeSet.complement()
                    )
                )
            } else {
                val consumer: RangeSets.Consumer<C> =
                    RangeToSql<Comparable<C>>(operandSql, orList, Function<C, SqlNode> { v ->
                        toSql(
                            program,
                            implementor().rexBuilder.makeLiteral(v, type)
                        )
                    })
                RangeSets.forEach(sarg.rangeSet, consumer)
            }
            return SqlUtil.createCall(SqlStdOperatorTable.OR, POS, orList)
        }

        @SuppressWarnings("BetaApi")
        private fun <C : Comparable<C>?> toIn(
            operandSql: SqlNode,
            eqOp: SqlBinaryOperator, inOp: SqlBinaryOperator,
            @Nullable program: RexProgram?, type: RelDataType, rangeSet: RangeSet<C>
        ): SqlNode {
            val list: SqlNodeList = rangeSet.asRanges().stream()
                .map { range ->
                    toSql(
                        program,
                        implementor().rexBuilder.makeLiteral(
                            range.lowerEndpoint(),
                            type, true, true
                        )
                    )
                }
                .collect(SqlNode.toList())
            return when (list.size()) {
                1 -> eqOp.createCall(POS, operandSql, list.get(0))
                else -> inOp.createCall(POS, operandSql, list)
            }
        }

        /** Converts an expression from [RexWindowBound] to [SqlNode]
         * format.
         *
         * @param rexWindowBound Expression to convert
         */
        fun toSql(rexWindowBound: RexWindowBound): SqlNode {
            val offsetLiteral: SqlNode? = if (rexWindowBound.getOffset() == null) null else SqlLiteral.createCharString(
                rexWindowBound.getOffset().toString(),
                SqlParserPos.ZERO
            )
            return if (rexWindowBound.isPreceding()) {
                if (offsetLiteral == null) SqlWindow.createUnboundedPreceding(POS) else SqlWindow.createPreceding(
                    offsetLiteral,
                    POS
                )
            } else if (rexWindowBound.isFollowing()) {
                if (offsetLiteral == null) SqlWindow.createUnboundedFollowing(POS) else SqlWindow.createFollowing(
                    offsetLiteral,
                    POS
                )
            } else {
                assert(rexWindowBound.isCurrentRow())
                SqlWindow.createCurrentRow(POS)
            }
        }

        fun toSql(
            group: Window.Group, constants: ImmutableList<RexLiteral?>,
            inputFieldCount: Int
        ): List<SqlNode> {
            val rexOvers: List<SqlNode> = ArrayList()
            val partitionKeys: List<SqlNode> = ArrayList()
            val orderByKeys: List<SqlNode> = ArrayList()
            for (partition in group.keys) {
                partitionKeys.add(field(partition))
            }
            for (collation in group.orderKeys.getFieldCollations()) {
                this.addOrderItem(orderByKeys, collation)
            }
            val isRows: SqlLiteral = SqlLiteral.createBoolean(group.isRows, POS)
            var lowerBound: SqlNode? = null
            var upperBound: SqlNode? = null
            val allowPartial: SqlLiteral? = null
            for (winAggCall in group.aggCalls) {
                val aggFunction: SqlAggFunction = winAggCall.getOperator() as SqlAggFunction
                val sqlWindow: SqlWindow = SqlWindow.create(
                    null, null,
                    SqlNodeList(partitionKeys, POS), SqlNodeList(orderByKeys, POS),
                    isRows, lowerBound, upperBound, allowPartial, POS
                )
                if (aggFunction.allowsFraming()) {
                    lowerBound = createSqlWindowBound(group.lowerBound)
                    upperBound = createSqlWindowBound(group.upperBound)
                    sqlWindow.setLowerBound(lowerBound)
                    sqlWindow.setUpperBound(upperBound)
                }
                val replaceConstants: RexShuttle = object : RexShuttle() {
                    @Override
                    fun visitInputRef(inputRef: RexInputRef): RexNode {
                        val index: Int = inputRef.getIndex()
                        val ref: RexNode
                        ref = if (index > inputFieldCount - 1) {
                            constants.get(index - inputFieldCount)
                        } else {
                            inputRef
                        }
                        return ref
                    }
                }
                val aggCall: RexCall = winAggCall.accept(replaceConstants) as RexCall
                val operands: List<SqlNode> = toSql(null, aggCall.operands)
                rexOvers.add(createOverCall(aggFunction, operands, sqlWindow, winAggCall.distinct))
            }
            return rexOvers
        }

        protected fun getAliasContext(variable: RexCorrelVariable?): Context {
            throw UnsupportedOperationException()
        }

        private fun toSql(@Nullable program: RexProgram, rexOver: RexOver): SqlCall {
            val rexWindow: RexWindow = rexOver.getWindow()
            val partitionList = SqlNodeList(
                toSql(program, rexWindow.partitionKeys), POS
            )
            val orderNodes: List<SqlNode> = Expressions.list()
            if (rexWindow.orderKeys != null) {
                for (rfc in rexWindow.orderKeys) {
                    addOrderItem(orderNodes, program, rfc)
                }
            }
            val orderList = SqlNodeList(orderNodes, POS)
            val isRows: SqlLiteral = SqlLiteral.createBoolean(rexWindow.isRows(), POS)

            // null defaults to true.
            // During parsing the allowPartial == false (e.g. disallow partial)
            // is expand into CASE expression and is handled as a such.
            // Not sure if we can collapse this CASE expression back into
            // "disallow partial" and set the allowPartial = false.
            val allowPartial: SqlLiteral? = null
            val sqlAggregateFunction: SqlAggFunction = rexOver.getAggOperator()
            var lowerBound: SqlNode? = null
            var upperBound: SqlNode? = null
            if (sqlAggregateFunction.allowsFraming()) {
                lowerBound = createSqlWindowBound(rexWindow.getLowerBound())
                upperBound = createSqlWindowBound(rexWindow.getUpperBound())
            }
            val sqlWindow: SqlWindow = SqlWindow.create(
                null, null, partitionList,
                orderList, isRows, lowerBound, upperBound, allowPartial, POS
            )
            val nodeList: List<SqlNode> = toSql(program, rexOver.getOperands())
            return createOverCall(sqlAggregateFunction, nodeList, sqlWindow, rexOver.isDistinct())
        }

        private fun toSql(@Nullable program: RexProgram, rfc: RexFieldCollation): SqlNode {
            var node: SqlNode = toSql(program, rfc.left)
            when (rfc.getDirection()) {
                DESCENDING, STRICTLY_DESCENDING -> node = SqlStdOperatorTable.DESC.createCall(POS, node)
                else -> {}
            }
            if (rfc.getNullDirection()
                !== dialect.defaultNullDirection(rfc.getDirection())
            ) {
                when (rfc.getNullDirection()) {
                    FIRST -> node = SqlStdOperatorTable.NULLS_FIRST.createCall(POS, node)
                    LAST -> node = SqlStdOperatorTable.NULLS_LAST.createCall(POS, node)
                    else -> {}
                }
            }
            return node
        }

        private fun createSqlWindowBound(rexWindowBound: RexWindowBound): SqlNode {
            if (rexWindowBound.isCurrentRow()) {
                return SqlWindow.createCurrentRow(POS)
            }
            if (rexWindowBound.isPreceding()) {
                return if (rexWindowBound.isUnbounded()) {
                    SqlWindow.createUnboundedPreceding(POS)
                } else {
                    val literal: SqlNode = toSql(null, rexWindowBound.getOffset())
                    SqlWindow.createPreceding(literal, POS)
                }
            }
            if (rexWindowBound.isFollowing()) {
                return if (rexWindowBound.isUnbounded()) {
                    SqlWindow.createUnboundedFollowing(POS)
                } else {
                    val literal: SqlNode = toSql(null, rexWindowBound.getOffset())
                    SqlWindow.createFollowing(literal, POS)
                }
            }
            throw AssertionError(
                "Unsupported Window bound: "
                        + rexWindowBound
            )
        }

        private fun toSql(@Nullable program: RexProgram, operandList: List<RexNode>): List<SqlNode> {
            val list: List<SqlNode> = ArrayList()
            for (rex in operandList) {
                list.add(toSql(program, rex))
            }
            return list
        }

        fun fieldList(): List<SqlNode> {
            return object : AbstractList<SqlNode?>() {
                @Override
                operator fun get(index: Int): SqlNode {
                    return field(index)
                }

                @Override
                fun size(): Int {
                    return fieldCount
                }
            }
        }

        fun addOrderItem(orderByList: List<SqlNode?>, field: RelFieldCollation) {
            var field: RelFieldCollation = field
            if (field.nullDirection !== RelFieldCollation.NullDirection.UNSPECIFIED) {
                val first = field.nullDirection === RelFieldCollation.NullDirection.FIRST
                val nullDirectionNode: SqlNode = dialect.emulateNullDirection(
                    field(field.getFieldIndex()),
                    first, field.direction.isDescending()
                )
                if (nullDirectionNode != null) {
                    orderByList.add(nullDirectionNode)
                    field = RelFieldCollation(
                        field.getFieldIndex(),
                        field.getDirection(),
                        RelFieldCollation.NullDirection.UNSPECIFIED
                    )
                }
            }
            orderByList.add(toSql(field))
        }

        /** Converts a RexFieldCollation to an ORDER BY item.  */
        private fun addOrderItem(
            orderByList: List<SqlNode>,
            @Nullable program: RexProgram, field: RexFieldCollation
        ) {
            var node: SqlNode = toSql(program, field.left)
            var nullDirectionNode: SqlNode? = null
            if (field.getNullDirection() !== RelFieldCollation.NullDirection.UNSPECIFIED) {
                val first = field.getNullDirection() === RelFieldCollation.NullDirection.FIRST
                nullDirectionNode = dialect.emulateNullDirection(
                    node, first, field.getDirection().isDescending()
                )
            }
            if (nullDirectionNode != null) {
                orderByList.add(nullDirectionNode)
                when (field.getDirection()) {
                    DESCENDING, STRICTLY_DESCENDING -> node = SqlStdOperatorTable.DESC.createCall(
                        POS, node
                    )
                    else -> {}
                }
                orderByList.add(node)
            } else {
                orderByList.add(toSql(program, field))
            }
        }

        /** Converts a call to an aggregate function to an expression.  */
        fun toSql(aggCall: AggregateCall): SqlNode {
            return toSql(
                aggCall.getAggregation(), aggCall.isDistinct(),
                Util.transform(aggCall.getArgList()) { ordinal: Int -> field(ordinal) },
                aggCall.filterArg, aggCall.collation
            )
        }

        /** Converts a call to an aggregate function, with a given list of operands,
         * to an expression.  */
        private fun toSql(
            op: SqlOperator, distinct: Boolean,
            operandList: List<SqlNode>, filterArg: Int, collation: RelCollation
        ): SqlCall {
            var operandList: List<SqlNode> = operandList
            val qualifier: SqlLiteral? = if (distinct) SqlSelectKeyword.DISTINCT.symbol(POS) else null
            if (op is SqlSumEmptyIsZeroAggFunction) {
                val node: SqlNode = toSql(
                    SqlStdOperatorTable.SUM, distinct,
                    operandList, filterArg, collation
                )
                return SqlStdOperatorTable.COALESCE.createCall(POS, node, ZERO)
            }

            // Handle filter on dialects that do support FILTER by generating CASE.
            if (filterArg >= 0 && !dialect.supportsAggregateFunctionFilter()) {
                // SUM(x) FILTER(WHERE b)  ==>  SUM(CASE WHEN b THEN x END)
                // COUNT(*) FILTER(WHERE b)  ==>  COUNT(CASE WHEN b THEN 1 END)
                // COUNT(x) FILTER(WHERE b)  ==>  COUNT(CASE WHEN b THEN x END)
                // COUNT(x, y) FILTER(WHERE b)  ==>  COUNT(CASE WHEN b THEN x END, y)
                val whenList: SqlNodeList = SqlNodeList.of(field(filterArg))
                val thenList: SqlNodeList = SqlNodeList.of(if (operandList.isEmpty()) ONE else operandList[0])
                val elseList: SqlNode = SqlLiteral.createNull(POS)
                val caseCall: SqlCall = SqlStdOperatorTable.CASE.createCall(
                    null, POS, null, whenList,
                    thenList, elseList
                )
                val newOperandList: List<SqlNode> = ArrayList()
                newOperandList.add(caseCall)
                if (operandList.size() > 1) {
                    newOperandList.addAll(Util.skip(operandList))
                }
                return toSql(op, distinct, newOperandList, -1, collation)
            }
            if (op is SqlCountAggFunction && operandList.isEmpty()) {
                // If there is no parameter in "count" function, add a star identifier
                // to it.
                operandList = ImmutableList.of(SqlIdentifier.STAR)
            }
            val call: SqlCall = op.createCall(qualifier, POS, operandList)

            // Handle filter by generating FILTER (WHERE ...)
            val call2: SqlCall
            call2 = if (filterArg < 0) {
                call
            } else {
                assert(
                    dialect.supportsAggregateFunctionFilter() // we checked above
                )
                SqlStdOperatorTable.FILTER.createCall(
                    POS, call,
                    field(filterArg)
                )
            }

            // Handle collation
            return withOrder(call2, collation)
        }

        /** Wraps a call in a [SqlKind.WITHIN_GROUP] call, if
         * `collation` is non-empty.  */
        private fun withOrder(call: SqlCall, collation: RelCollation): SqlCall {
            if (collation.getFieldCollations().isEmpty()) {
                return call
            }
            val orderByList: List<SqlNode> = ArrayList()
            for (field in collation.getFieldCollations()) {
                addOrderItem(orderByList, field)
            }
            return SqlStdOperatorTable.WITHIN_GROUP.createCall(
                POS, call,
                SqlNodeList(orderByList, POS)
            )
        }

        /** Converts a collation to an ORDER BY item.  */
        fun toSql(collation: RelFieldCollation): SqlNode {
            var node: SqlNode = orderField(collation.getFieldIndex())
            when (collation.getDirection()) {
                DESCENDING, STRICTLY_DESCENDING -> node = SqlStdOperatorTable.DESC.createCall(
                    POS, node
                )
                else -> {}
            }
            if (collation.nullDirection !== dialect.defaultNullDirection(collation.direction)) {
                when (collation.nullDirection) {
                    FIRST -> node = SqlStdOperatorTable.NULLS_FIRST.createCall(POS, node)
                    LAST -> node = SqlStdOperatorTable.NULLS_LAST.createCall(POS, node)
                    else -> {}
                }
            }
            return node
        }

        abstract fun implementor(): SqlImplementor

        /** Converts a [Range] to a SQL expression.
         *
         * @param <C> Value type
        </C> */
        private class RangeToSql<C : Comparable<C>?> internal constructor(
            arg: SqlNode, list: List<SqlNode>,
            literalFactory: Function<C, SqlNode?>
        ) : RangeSets.Consumer<C> {
            private val list: List<SqlNode>
            private val literalFactory: Function<C, SqlNode>
            private val arg: SqlNode

            init {
                this.arg = arg
                this.list = list
                this.literalFactory = literalFactory
            }

            private fun addAnd(vararg nodes: SqlNode) {
                list.add(
                    SqlUtil.createCall(
                        SqlStdOperatorTable.AND, POS,
                        ImmutableList.copyOf(nodes)
                    )
                )
            }

            private fun op(op: SqlOperator, value: C): SqlNode {
                return op.createCall(POS, arg, literalFactory.apply(value))
            }

            @Override
            fun all() {
                list.add(SqlLiteral.createBoolean(true, POS))
            }

            @Override
            fun atLeast(lower: C) {
                list.add(op(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, lower))
            }

            @Override
            fun atMost(upper: C) {
                list.add(op(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, upper))
            }

            @Override
            fun greaterThan(lower: C) {
                list.add(op(SqlStdOperatorTable.GREATER_THAN, lower))
            }

            @Override
            fun lessThan(upper: C) {
                list.add(op(SqlStdOperatorTable.LESS_THAN, upper))
            }

            @Override
            fun singleton(value: C) {
                list.add(op(SqlStdOperatorTable.EQUALS, value))
            }

            @Override
            fun closed(lower: C, upper: C) {
                addAnd(
                    op(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, lower),
                    op(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, upper)
                )
            }

            @Override
            fun closedOpen(lower: C, upper: C) {
                addAnd(
                    op(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, lower),
                    op(SqlStdOperatorTable.LESS_THAN, upper)
                )
            }

            @Override
            fun openClosed(lower: C, upper: C) {
                addAnd(
                    op(SqlStdOperatorTable.GREATER_THAN, lower),
                    op(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, upper)
                )
            }

            @Override
            fun open(lower: C, upper: C) {
                addAnd(
                    op(SqlStdOperatorTable.GREATER_THAN, lower),
                    op(SqlStdOperatorTable.LESS_THAN, upper)
                )
            }
        }

        companion object {
            /** If `node` is a [RexCall], extracts the operator and
             * finds the corresponding inverse operator using [SqlOperator.not].
             * Returns null if `node` is not a [RexCall],
             * or if the operator has no logical inverse.  */
            @Nullable
            private fun getInverseOperator(node: RexNode): SqlOperator? {
                return if (node is RexCall) {
                    (node as RexCall).getOperator().not()
                } else {
                    null
                }
            }

            private fun createOverCall(
                op: SqlAggFunction, operands: List<SqlNode>,
                window: SqlWindow, isDistinct: Boolean
            ): SqlCall {
                if (op is SqlSumEmptyIsZeroAggFunction) {
                    // Rewrite "SUM0(x) OVER w" to "COALESCE(SUM(x) OVER w, 0)"
                    val node: SqlCall = createOverCall(SqlStdOperatorTable.SUM, operands, window, isDistinct)
                    return SqlStdOperatorTable.COALESCE.createCall(POS, node, ZERO)
                }
                val aggFunctionCall: SqlCall
                aggFunctionCall = if (isDistinct) {
                    op.createCall(
                        SqlSelectKeyword.DISTINCT.symbol(POS),
                        POS,
                        operands
                    )
                } else {
                    op.createCall(POS, operands)
                }
                return SqlStdOperatorTable.OVER.createCall(
                    POS, aggFunctionCall,
                    window
                )
            }
        }
    }

    /** Simple implementation of [Context] that cannot handle sub-queries
     * or correlations. Because it is so simple, you do not need to create a
     * [SqlImplementor] or [org.apache.calcite.tools.RelBuilder]
     * to use it. It is a good way to convert a [RexNode] to SQL text.  */
    class SimpleContext(dialect: SqlDialect, field: IntFunction<SqlNode?>) : Context(dialect, 0, false) {
        private val field: IntFunction<SqlNode>

        init {
            this.field = field
        }

        @Override
        override fun implementor(): SqlImplementor {
            throw UnsupportedOperationException()
        }

        @Override
        override fun field(ordinal: Int): SqlNode {
            return field.apply(ordinal)
        }
    }

    /** Implementation of [Context] that has an enclosing
     * [SqlImplementor] and can therefore do non-trivial expressions.  */
    protected abstract inner class BaseContext internal constructor(dialect: SqlDialect, fieldCount: Int) :
        Context(dialect, fieldCount) {
        @Override
        override fun getAliasContext(variable: RexCorrelVariable): Context {
            return requireNonNull(
                correlTableMap[variable.id]
            ) { "variable " + variable.id.toString() + " is not found" }
        }

        @Override
        override fun implementor(): SqlImplementor {
            return this@SqlImplementor
        }
    }

    fun aliasContext(
        aliases: Map<String?, RelDataType?>?,
        qualified: Boolean
    ): Context {
        return AliasContext(dialect, aliases, qualified)
    }

    fun joinContext(leftContext: Context?, rightContext: Context?): Context {
        return JoinContext(dialect, leftContext, rightContext)
    }

    fun matchRecognizeContext(context: Context): Context {
        return MatchRecognizeContext(dialect, (context as AliasContext).aliases)
    }

    fun tableFunctionScanContext(inputSqlNodes: List<SqlNode?>?): Context {
        return TableFunctionScanContext(dialect, inputSqlNodes)
    }

    /** Context for translating MATCH_RECOGNIZE clause.  */
    inner class MatchRecognizeContext(
        dialect: SqlDialect,
        aliases: Map<String?, RelDataType?>
    ) : AliasContext(dialect, aliases, false) {
        @Override
        override fun toSql(@Nullable program: RexProgram?, rex: RexNode): SqlNode {
            if (rex.getKind() === SqlKind.LITERAL) {
                val literal: RexLiteral = rex as RexLiteral
                if (literal.getTypeName().getFamily() === SqlTypeFamily.CHARACTER) {
                    return SqlIdentifier(castNonNull(RexLiteral.stringValue(literal)), POS)
                }
            }
            return super.toSql(program, rex)
        }
    }

    /** Implementation of Context that precedes field references with their
     * "table alias" based on the current sub-query's FROM clause.  */
    inner class AliasContext(
        dialect: SqlDialect,
        aliases: Map<String?, RelDataType?>, qualified: Boolean
    ) : BaseContext(dialect, computeFieldCount(aliases)) {
        private val qualified: Boolean
        val aliases: Map<String?, RelDataType?>

        /** Creates an AliasContext; use [.aliasContext].  */
        init {
            this.aliases = aliases
            this.qualified = qualified
        }

        @Override
        override fun field(ordinal: Int): SqlNode {
            var ordinal = ordinal
            for (alias in aliases.entrySet()) {
                val fields: List<RelDataTypeField> = alias.getValue().getFieldList()
                if (ordinal < fields.size()) {
                    val field: RelDataTypeField = fields[ordinal]
                    return SqlIdentifier(
                        if (!qualified) ImmutableList.of(field.getName()) else ImmutableList.of(
                            alias.getKey(),
                            field.getName()
                        ),
                        POS
                    )
                }
                ordinal -= fields.size()
            }
            throw AssertionError(
                "field ordinal $ordinal out of range $aliases"
            )
        }
    }

    /** Context for translating ON clause of a JOIN from [RexNode] to
     * [SqlNode].  */
    internal inner class JoinContext
    /** Creates a JoinContext; use [.joinContext].  */ private constructor(
        dialect: SqlDialect,
        private val leftContext: Context,
        private val rightContext: Context
    ) : BaseContext(dialect, leftContext.fieldCount + rightContext.fieldCount) {
        @Override
        override fun field(ordinal: Int): SqlNode {
            return if (ordinal < leftContext.fieldCount) {
                leftContext.field(ordinal)
            } else {
                rightContext.field(ordinal - leftContext.fieldCount)
            }
        }

        @Override
        override fun reverseCall(call: RexCall): RexCall {
            return when (call.getKind()) {
                EQUALS, IS_DISTINCT_FROM, IS_NOT_DISTINCT_FROM, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL -> {
                    assert(call.operands.size() === 2)
                    val op0: RexNode = call.operands.get(0)
                    val op1: RexNode = call.operands.get(1)
                    if (op0 is RexInputRef
                        && op1 is RexInputRef
                        && (op1 as RexInputRef).getIndex() < leftContext.fieldCount && (op0 as RexInputRef).getIndex() >= leftContext.fieldCount
                    ) {
                        // Arguments were of form 'op1 = op0'
                        val op2: SqlOperator = requireNonNull(call.getOperator().reverse())
                        return rexBuilder.makeCall(op2, op1, op0) as RexCall
                    }
                    call
                }
                else -> call
            }
        }
    }

    /** Context for translating call of a TableFunctionScan from [RexNode] to
     * [SqlNode].  */
    internal inner class TableFunctionScanContext(dialect: SqlDialect, inputSqlNodes: List<SqlNode>) :
        BaseContext(dialect, inputSqlNodes.size()) {
        private val inputSqlNodes: List<SqlNode>

        init {
            this.inputSqlNodes = inputSqlNodes
        }

        @Override
        override fun field(ordinal: Int): SqlNode {
            return inputSqlNodes[ordinal]
        }
    }

    /** Result of implementing a node.  */
    inner class Result private constructor(
        node: SqlNode, clauses: Collection<Clause>, @Nullable neededAlias: String,
        @Nullable neededType: RelDataType, aliases: Map<String?, RelDataType>, anon: Boolean,
        ignoreClauses: Boolean, expectedClauses: Set<Clause>,
        @Nullable expectedRel: RelNode?
    ) {
        val node: SqlNode

        @Nullable
        val neededAlias: String?

        @Nullable
        private val neededType: RelDataType
        val aliases: Map<String?, RelDataType>
        val clauses: List<Clause>
        private val anon: Boolean

        /** Whether to treat [.expectedClauses] as empty for the
         * purposes of figuring out whether we need a new sub-query.  */
        private val ignoreClauses: Boolean

        /** Clauses that will be generated to implement current relational
         * expression.  */
        private val expectedClauses: ImmutableSet<Clause>

        @Nullable
        private val expectedRel: RelNode?
        private val needNew: Boolean

        constructor(
            node: SqlNode, clauses: Collection<Clause>, @Nullable neededAlias: String,
            @Nullable neededType: RelDataType, aliases: Map<String?, RelDataType>
        ) : this(
            node, clauses, neededAlias, neededType, aliases, false, false,
            ImmutableSet.of(), null
        ) {
        }

        init {
            this.node = node
            this.neededAlias = neededAlias
            this.neededType = neededType
            this.aliases = aliases
            this.clauses = ImmutableList.copyOf(clauses)
            this.anon = anon
            this.ignoreClauses = ignoreClauses
            this.expectedClauses = ImmutableSet.copyOf(expectedClauses)
            this.expectedRel = expectedRel
            val clauses2 = if (ignoreClauses) ImmutableSet.of() else expectedClauses
            needNew = (expectedRel != null
                    && needNewSubQuery(expectedRel, this.clauses, clauses2))
        }

        /** Creates a builder for the SQL of the given relational expression,
         * using the clauses that you declared when you called
         * [.visitInput].  */
        fun builder(rel: RelNode?): Builder {
            return builder(rel, expectedClauses)
        }
        // CHECKSTYLE: IGNORE 3

        @Deprecated // to be removed before 2.0
        @Deprecated(
            """Provide the expected clauses up-front, when you call
      {@link #visitInput(RelNode, int, Set)}, then create a builder using
      {@link #builder(RelNode)}. """
        )
        fun builder(rel: RelNode?, clause: Clause?, vararg clauses: Clause?): Builder {
            return builder(rel, ImmutableSet.copyOf(Lists.asList(clause, clauses)))
        }

        /** Once you have a Result of implementing a child relational expression,
         * call this method to create a Builder to implement the current relational
         * expression by adding additional clauses to the SQL query.
         *
         *
         * You need to declare which clauses you intend to add. If the clauses
         * are "later", you can add to the same query. For example, "GROUP BY" comes
         * after "WHERE". But if they are the same or earlier, this method will
         * start a new SELECT that wraps the previous result.
         *
         *
         * When you have called
         * [Builder.setSelect],
         * [Builder.setWhere] etc. call
         * [Builder.result]
         * to fix the new query.
         *
         * @param rel Relational expression being implemented
         * @return A builder
         */
        private fun builder(rel: RelNode, clauses: Set<Clause>): Builder {
            assert(expectedClauses.containsAll(clauses))
            assert(rel.equals(expectedRel))
            val clauses2 = if (ignoreClauses) ImmutableSet.of() else clauses
            val needNew = needNewSubQuery(rel, this.clauses, clauses2)
            assert(needNew == this.needNew)
            val select: SqlSelect
            val clauseList: Expressions.FluentList<Clause> = Expressions.list()
            if (needNew) {
                select = subSelect()
            } else {
                select = asSelect()
                clauseList.addAll(this.clauses)
            }
            clauseList.appendAll(clauses)
            val newContext: Context
            var newAliases: Map<String?, RelDataType?>? = null
            val selectList: SqlNodeList = select.getSelectList()
            if (!selectList.equals(SqlNodeList.SINGLETON_STAR)) {
                val aliasRef = (expectedClauses.contains(Clause.HAVING)
                        && dialect.getConformance().isHavingAlias())
                newContext = object : Context(dialect, selectList.size()) {
                    @Override
                    override fun implementor(): SqlImplementor {
                        return this@SqlImplementor
                    }

                    @Override
                    override fun field(ordinal: Int): SqlNode {
                        val selectItem: SqlNode = selectList.get(ordinal)
                        when (selectItem.getKind()) {
                            AS -> {
                                val asCall: SqlCall = selectItem as SqlCall
                                val alias: SqlNode = asCall.operand(1)
                                return if (aliasRef && !SqlUtil.isGeneratedAlias((alias as SqlIdentifier).getSimple())) {
                                    // For BigQuery, given the query
                                    //   SELECT SUM(x) AS x FROM t HAVING(SUM(t.x) > 0)
                                    // we can generate
                                    //   SELECT SUM(x) AS x FROM t HAVING(x > 0)
                                    // because 'x' in HAVING resolves to the 'AS x' not 't.x'.
                                    alias
                                } else asCall.operand(0)
                            }
                            else -> {}
                        }
                        return selectItem
                    }

                    @Override
                    override fun orderField(ordinal: Int): SqlNode {
                        // If the field expression is an unqualified column identifier
                        // and matches a different alias, use an ordinal.
                        // For example, given
                        //    SELECT deptno AS empno, empno AS x FROM emp ORDER BY emp.empno
                        // we generate
                        //    SELECT deptno AS empno, empno AS x FROM emp ORDER BY 2
                        // "ORDER BY empno" would give incorrect result;
                        // "ORDER BY x" is acceptable but is not preferred.
                        val node: SqlNode = field(ordinal)
                        if (node is SqlIdentifier
                            && (node as SqlIdentifier).isSimple()
                        ) {
                            val name: String = (node as SqlIdentifier).getSimple()
                            for (selectItem in Ord.zip(selectList)) {
                                if (selectItem.i !== ordinal) {
                                    val alias: String = SqlValidatorUtil.getAlias(selectItem.e, -1)
                                    if (name.equalsIgnoreCase(alias)) {
                                        return SqlLiteral.createExactNumeric(
                                            Integer.toString(ordinal + 1), SqlParserPos.ZERO
                                        )
                                    }
                                }
                            }
                        }
                        return node
                    }
                }
            } else {
                val qualified = !dialect.hasImplicitTableAlias() || aliases.size() > 1
                // basically, we did a subSelect() since needNew is set and neededAlias is not null
                // now, we need to make sure that we need to update the alias context.
                // if our aliases map has a single element:  <neededAlias, rowType>,
                // then we don't need to rewrite the alias but otherwise, it should be updated.
                if (needNew
                    && neededAlias != null && (aliases.size() !== 1 || !aliases.containsKey(neededAlias))
                ) {
                    newAliases = ImmutableMap.of(neededAlias, rel.getInput(0).getRowType())
                    newContext = aliasContext(newAliases, qualified)
                } else {
                    newContext = aliasContext(aliases, qualified)
                }
            }
            return Builder(
                rel, clauseList, select, newContext, isAnon,
                if (needNew && !aliases.containsKey(neededAlias)) newAliases else aliases
            )
        }

        /** Returns whether a new sub-query is required.  */
        private fun needNewSubQuery(
            rel: RelNode?, clauses: List<Clause>,
            expectedClauses: Set<Clause>
        ): Boolean {
            if (clauses.isEmpty()) {
                return false
            }
            val maxClause: Clause = Collections.max(clauses)
            // If old and new clause are equal and belong to below set,
            // then new SELECT wrap is not required
            val nonWrapSet: Set<Clause> = ImmutableSet.of(Clause.SELECT)
            for (clause in expectedClauses) {
                if (maxClause.ordinal() > clause.ordinal()
                    || (maxClause == clause
                            && !nonWrapSet.contains(clause))
                ) {
                    return true
                }
            }
            if (rel is Project
                && clauses.contains(Clause.HAVING)
                && dialect.getConformance().isHavingAlias()
            ) {
                return true
            }
            if (rel is Project
                && (rel as Project?).containsOver()
                && maxClause == Clause.SELECT
            ) {
                // Cannot merge a Project that contains windowed functions onto an
                // underlying Project
                return true
            }
            if (rel is Aggregate) {
                val agg: Aggregate? = rel as Aggregate?
                val hasNestedAgg = hasNested(agg, Predicate<SqlNode> { node: SqlNode -> isAggregate(node) })
                val hasNestedWindowedAgg =
                    hasNested(agg, Predicate<SqlNode> { node: SqlNode -> isWindowedAggregate(node) })
                if (!dialect.supportsNestedAggregations()
                    && (hasNestedAgg || hasNestedWindowedAgg)
                ) {
                    return true
                }
                if (clauses.contains(Clause.GROUP_BY)) {
                    // Avoid losing the distinct attribute of inner aggregate.
                    return !hasNestedAgg || Aggregate.isNotGrandTotal(agg)
                }
            }
            return false
        }

        /** Returns whether an [Aggregate] contains nested operands that
         * match the predicate.
         *
         * @param aggregate Aggregate node
         * @param operandPredicate Predicate for the nested operands
         * @return whether any nested operands matches the predicate
         */
        private fun hasNested(
            aggregate: Aggregate?,
            operandPredicate: Predicate<SqlNode>
        ): Boolean {
            if (node is SqlSelect) {
                val selectList: SqlNodeList = (node as SqlSelect).getSelectList()
                if (!selectList.equals(SqlNodeList.SINGLETON_STAR)) {
                    val aggregatesArgs: Set<Integer> = HashSet()
                    for (aggregateCall in aggregate.getAggCallList()) {
                        aggregatesArgs.addAll(aggregateCall.getArgList())
                    }
                    for (aggregatesArg in aggregatesArgs) {
                        if (selectList.get(aggregatesArg) is SqlBasicCall) {
                            val call: SqlBasicCall = selectList.get(aggregatesArg) as SqlBasicCall
                            for (operand in call.getOperandList()) {
                                if (operand != null && operandPredicate.test(operand)) {
                                    return true
                                }
                            }
                        }
                    }
                }
            }
            return false
        }

        /** Returns the highest clause that is in use.  */
        @Deprecated
        fun maxClause(): Clause {
            return Collections.max(clauses)
        }

        /** Returns a node that can be included in the FROM clause or a JOIN. It has
         * an alias that is unique within the query. The alias is implicit if it
         * can be derived using the usual rules (For example, "SELECT * FROM emp" is
         * equivalent to "SELECT * FROM emp AS emp".)  */
        fun asFrom(): SqlNode {
            return if (neededAlias != null) {
                if (node.getKind() === SqlKind.AS) {
                    // If we already have an AS node, we need to replace the alias
                    // This is especially relevant for the VALUES clause rendering
                    val sqlCall: SqlCall = node as SqlCall
                    @SuppressWarnings("assignment.type.incompatible") val operands: Array<SqlNode> =
                        sqlCall.getOperandList().toArray(arrayOfNulls<SqlNode>(0))
                    operands[1] = SqlIdentifier(neededAlias, POS)
                    SqlStdOperatorTable.AS.createCall(POS, operands)
                } else {
                    SqlStdOperatorTable.AS.createCall(
                        POS, node,
                        SqlIdentifier(neededAlias, POS)
                    )
                }
            } else node
        }

        fun subSelect(): SqlSelect {
            return wrapSelect(asFrom())
        }

        /** Converts a non-query node into a SELECT node. Set operators (UNION,
         * INTERSECT, EXCEPT) remain as is.  */
        fun asSelect(): SqlSelect {
            if (node is SqlSelect) {
                return node as SqlSelect
            }
            return if (!dialect.hasImplicitTableAlias()) {
                wrapSelect(asFrom())
            } else wrapSelect(node)
        }

        fun stripTrivialAliases(node: SqlNode) {
            when (node.getKind()) {
                SELECT -> {
                    val select: SqlSelect = node as SqlSelect
                    val nodeList: SqlNodeList = select.getSelectList()
                    if (nodeList != null) {
                        var i = 0
                        while (i < nodeList.size()) {
                            val n: SqlNode = nodeList.get(i)
                            if (n.getKind() === SqlKind.AS) {
                                val call: SqlCall = n as SqlCall
                                val identifier: SqlIdentifier = call.operand(1)
                                if (SqlUtil.isGeneratedAlias(identifier.getSimple())) {
                                    nodeList.set(i, call.operand(0))
                                }
                            }
                            i++
                        }
                    }
                }
                UNION, INTERSECT, EXCEPT, INSERT, UPDATE, DELETE, MERGE -> {
                    val call: SqlCall = node as SqlCall
                    for (operand in call.getOperandList()) {
                        if (operand != null) {
                            stripTrivialAliases(operand)
                        }
                    }
                }
                else -> {}
            }
        }

        /** Strips trivial aliases if anon.  */
        private fun maybeStrip(node: SqlNode): SqlNode {
            if (anon) {
                stripTrivialAliases(node)
            }
            return node
        }

        /** Converts a non-query node into a SELECT node. Set operators (UNION,
         * INTERSECT, EXCEPT) and DML operators (INSERT, UPDATE, DELETE, MERGE)
         * remain as is.  */
        fun asStatement(): SqlNode {
            return when (node.getKind()) {
                UNION, INTERSECT, EXCEPT, INSERT, UPDATE, DELETE, MERGE -> maybeStrip(node)
                else -> maybeStrip(asSelect())
            }
        }

        /** Converts a non-query node into a SELECT node. Set operators (UNION,
         * INTERSECT, EXCEPT) and VALUES remain as is.  */
        fun asQueryOrValues(): SqlNode {
            return when (node.getKind()) {
                UNION, INTERSECT, EXCEPT, VALUES -> maybeStrip(node)
                else -> maybeStrip(asSelect())
            }
        }

        /** Returns a context that always qualifies identifiers. Useful if the
         * Context deals with just one arm of a join, yet we wish to generate
         * a join condition that qualifies column names to disambiguate them.  */
        fun qualifiedContext(): Context {
            return aliasContext(aliases, true)
        }

        /**
         * In join, when the left and right nodes have been generated,
         * update their alias with 'neededAlias' if not null.
         */
        fun resetAlias(): Result {
            return if (neededAlias == null) {
                this
            } else {
                Result(
                    node, clauses, neededAlias, neededType,
                    ImmutableMap.of(neededAlias, castNonNull(neededType)), anon, ignoreClauses,
                    expectedClauses, expectedRel
                )
            }
        }

        /**
         * Sets the alias of the join or correlate just created.
         *
         * @param alias New alias
         * @param type type of the node associated with the alias
         */
        fun resetAlias(alias: String?, type: RelDataType?): Result {
            return Result(
                node, clauses, alias, neededType,
                ImmutableMap.of(alias, type), anon, ignoreClauses,
                expectedClauses, expectedRel
            )
        }

        /** Returns a copy of this Result, overriding the value of `anon`.  */
        fun withAnon(anon: Boolean): Result {
            return if (anon == this.anon) this else Result(
                node, clauses, neededAlias, neededType, aliases, anon,
                ignoreClauses, expectedClauses, expectedRel
            )
        }

        /** Returns a copy of this Result, overriding the value of
         * `ignoreClauses` and `expectedClauses`.  */
        fun withExpectedClauses(
            ignoreClauses: Boolean,
            expectedClauses: Set<Clause?>, expectedRel: RelNode
        ): Result {
            return if (ignoreClauses == this.ignoreClauses && expectedClauses.equals(this.expectedClauses)
                && expectedRel === this.expectedRel
            ) this else Result(
                node, clauses, neededAlias, neededType, aliases, anon,
                ignoreClauses, ImmutableSet.copyOf(expectedClauses), expectedRel
            )
        }
    }

    /** Builder.  */
    inner class Builder(
        rel: RelNode?, clauses: List<Clause?>?, select: SqlSelect?,
        context: Context?, anon: Boolean,
        @Nullable aliases: Map<String?, RelDataType>
    ) {
        private val rel: RelNode
        val clauses: List<Clause>
        val select: SqlSelect
        val context: Context
        val anon: Boolean

        @Nullable
        private val aliases: Map<String?, RelDataType>

        init {
            this.rel = requireNonNull(rel, "rel")
            this.clauses = ImmutableList.copyOf(clauses)
            this.select = requireNonNull(select, "select")
            this.context = requireNonNull(context, "context")
            this.anon = anon
            this.aliases = aliases
        }

        fun setSelect(nodeList: SqlNodeList?) {
            select.setSelectList(nodeList)
        }

        fun setWhere(node: SqlNode?) {
            assert(clauses.contains(Clause.WHERE))
            select.setWhere(node)
        }

        fun setGroupBy(nodeList: SqlNodeList?) {
            assert(clauses.contains(Clause.GROUP_BY))
            select.setGroupBy(nodeList)
        }

        fun setHaving(node: SqlNode?) {
            assert(clauses.contains(Clause.HAVING))
            select.setHaving(node)
        }

        fun setOrderBy(nodeList: SqlNodeList?) {
            assert(clauses.contains(Clause.ORDER_BY))
            select.setOrderBy(nodeList)
        }

        fun setFetch(fetch: SqlNode?) {
            assert(clauses.contains(Clause.FETCH))
            select.setFetch(fetch)
        }

        fun setOffset(offset: SqlNode?) {
            assert(clauses.contains(Clause.OFFSET))
            select.setOffset(offset)
        }

        fun addOrderItem(
            orderByList: List<SqlNode?>,
            field: RelFieldCollation
        ) {
            context.addOrderItem(orderByList, field)
        }

        fun result(): Result {
            return this@SqlImplementor.result(select, clauses, rel, aliases)
                .withAnon(anon)
        }
    }

    /** Clauses in a SQL query. Ordered by evaluation order.
     * SELECT is set only when there is a NON-TRIVIAL SELECT clause.  */
    enum class Clause {
        FROM, WHERE, GROUP_BY, HAVING, SELECT, SET_OP, ORDER_BY, FETCH, OFFSET
    }

    companion object {
        // Always use quoted position, the "isQuoted" info is only used when
        // unparsing a SqlIdentifier. For some rex nodes, saying RexInputRef, we have
        // no idea about whether it is quoted or not for the original sql statement.
        // So we just quote it.
        val POS: SqlParserPos = SqlParserPos.QUOTED_ZERO

        /** SQL numeric literal `0`.  */
        val ZERO: SqlNumericLiteral = SqlNumericLiteral.createExactNumeric("0", POS)

        /** SQL numeric literal `1`.  */
        val ONE: SqlNumericLiteral = SqlLiteral.createExactNumeric("1", POS)

        /** Creates a relational expression that has `r` as its input.  */
        private fun holder(r: RelNode): RelNode {
            return object : SingleRel(r.getCluster(), r.getTraitSet(), r) {}
        }

        /** Returns whether a list of expressions projects all fields, in order,
         * from the input, with the same names.  */
        fun isStar(
            exps: List<RexNode?>, inputRowType: RelDataType,
            projectRowType: RelDataType
        ): Boolean {
            assert(exps.size() === projectRowType.getFieldCount())
            var i = 0
            for (ref in exps) {
                if (ref !is RexInputRef) {
                    return false
                } else if ((ref as RexInputRef).getIndex() !== i++) {
                    return false
                }
            }
            return (i == inputRowType.getFieldCount()
                    && inputRowType.getFieldNames().equals(projectRowType.getFieldNames()))
        }

        fun isStar(program: RexProgram): Boolean {
            var i = 0
            for (ref in program.getProjectList()) {
                if (ref.getIndex() !== i++) {
                    return false
                }
            }
            return i == program.getInputRowType().getFieldCount()
        }

        /**
         * Converts a [RexNode] condition into a [SqlNode].
         *
         * @param node            Join condition
         * @param leftContext     Left context
         * @param rightContext    Right context
         *
         * @return SqlNode that represents the condition
         */
        fun convertConditionToSqlNode(
            node: RexNode,
            leftContext: Context,
            rightContext: Context?
        ): SqlNode {
            if (node.isAlwaysTrue()) {
                return SqlLiteral.createBoolean(true, POS)
            }
            if (node.isAlwaysFalse()) {
                return SqlLiteral.createBoolean(false, POS)
            }
            val joinContext = leftContext.implementor().joinContext(leftContext, rightContext)
            return joinContext.toSql(null, node)
        }

        /** Removes cast from string.
         *
         *
         * For example, `x > CAST('2015-01-07' AS DATE)`
         * becomes `x > '2015-01-07'`.
         */
        private fun stripCastFromString(node: RexNode, dialect: SqlDialect): RexNode {
            when (node.getKind()) {
                EQUALS, IS_NOT_DISTINCT_FROM, NOT_EQUALS, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL -> {
                    val call: RexCall = node as RexCall
                    val o0: RexNode = call.operands.get(0)
                    val o1: RexNode = call.operands.get(1)
                    if (o0.getKind() === SqlKind.CAST
                        && o1.getKind() !== SqlKind.CAST
                    ) {
                        if (!dialect.supportsImplicitTypeCoercion(o0 as RexCall)) {
                            // If the dialect does not support implicit type coercion,
                            // we definitely can not strip the cast.
                            return node
                        }
                        val o0b: RexNode = (o0 as RexCall).getOperands().get(0)
                        return call.clone(call.getType(), ImmutableList.of(o0b, o1))
                    }
                    if (o1.getKind() === SqlKind.CAST
                        && o0.getKind() !== SqlKind.CAST
                    ) {
                        if (!dialect.supportsImplicitTypeCoercion(o1 as RexCall)) {
                            return node
                        }
                        val o1b: RexNode = (o1 as RexCall).getOperands().get(0)
                        return call.clone(call.getType(), ImmutableList.of(o0, o1b))
                    }
                }
                else -> {}
            }
            return node
        }

        fun joinType(joinType: JoinRelType?): JoinType {
            return when (joinType) {
                LEFT -> JoinType.LEFT
                RIGHT -> JoinType.RIGHT
                INNER -> JoinType.INNER
                FULL -> JoinType.FULL
                else -> throw AssertionError(joinType)
            }
        }

        /** Returns the row type of `rel`, adjusting the field names if
         * `node` is "(query) as tableAlias (fieldAlias, ...)".  */
        private fun adjustedRowType(rel: RelNode, node: SqlNode?): RelDataType {
            val rowType: RelDataType = rel.getRowType()
            val builder: RelDataTypeFactory.Builder
            return when (node.getKind()) {
                UNION, INTERSECT, EXCEPT -> adjustedRowType(
                    rel,
                    (node as SqlCall?).getOperandList().get(0)
                )
                SELECT -> {
                    val selectList: SqlNodeList = (node as SqlSelect?).getSelectList()
                    if (selectList.equals(SqlNodeList.SINGLETON_STAR)) {
                        return rowType
                    }
                    builder = rel.getCluster().getTypeFactory().builder()
                    Pair.forEach(
                        selectList,
                        rowType.getFieldList()
                    ) { selectItem, field ->
                        builder.add(
                            Util.first(
                                SqlValidatorUtil.getAlias(selectItem, -1),
                                field.getName()
                            ),
                            field.getType()
                        )
                    }
                    builder.build()
                }
                AS -> {
                    val operandList: List<SqlNode> = (node as SqlCall?).getOperandList()
                    if (operandList.size() <= 2) {
                        return rowType
                    }
                    builder = rel.getCluster().getTypeFactory().builder()
                    Pair.forEach(
                        Util.skip(operandList, 2),
                        rowType.getFieldList()
                    ) { operand, field -> builder.add(operand.toString(), field.getType()) }
                    builder.build()
                }
                else -> rowType
            }
        }

        private fun collectAliases(
            builder: ImmutableMap.Builder<String, RelDataType>,
            node: SqlNode, aliases: Iterator<RelDataType>
        ) {
            if (node is SqlJoin) {
                val join: SqlJoin = node as SqlJoin
                collectAliases(builder, join.getLeft(), aliases)
                collectAliases(builder, join.getRight(), aliases)
            } else {
                val alias: String = SqlValidatorUtil.getAlias(node, -1)
                assert(alias != null)
                builder.put(alias, aliases.next())
            }
        }

        /** Returns whether a node is a call to an aggregate function.  */
        private fun isAggregate(node: SqlNode): Boolean {
            return (node is SqlCall
                    && (node as SqlCall).getOperator() is SqlAggFunction)
        }

        /** Returns whether a node is a call to a windowed aggregate function.  */
        private fun isWindowedAggregate(node: SqlNode): Boolean {
            return (node is SqlCall
                    && (node as SqlCall).getOperator() is SqlOverOperator)
        }

        /** Converts a [RexLiteral] in the context of a [RexProgram]
         * to a [SqlNode].  */
        fun toSql(@Nullable program: RexProgram?, literal: RexLiteral): SqlNode {
            return when (literal.getTypeName()) {
                SYMBOL -> {
                    SqlLiteral.createSymbol(literal.getValue(), POS)
                }
                ROW -> {
                    val list: List<RexLiteral> = castNonNull(literal.getValueAs(List::class.java))
                    SqlStdOperatorTable.ROW.createCall(
                        POS,
                        list.stream().map { e -> toSql(program, e) }
                            .collect(Util.toImmutableList()))
                }
                SARG -> {
                    val arg: Sarg = literal.getValueAs(Sarg::class.java)
                    throw AssertionError(
                        "sargs [" + arg
                                + "] should be handled as part of predicates, not as literals"
                    )
                }
                else -> toSql(literal)
            }
        }

        /** Converts a [RexLiteral] to a [SqlLiteral].  */
        fun toSql(literal: RexLiteral): SqlNode {
            val typeName: SqlTypeName = literal.getTypeName()
            when (typeName) {
                SYMBOL -> {
                    return SqlLiteral.createSymbol(literal.getValue(), POS)
                }
                ROW -> {
                    val list: List<RexLiteral> = castNonNull(literal.getValueAs(List::class.java))
                    return SqlStdOperatorTable.ROW.createCall(
                        POS,
                        list.stream().map { e -> toSql(e) }
                            .collect(Util.toImmutableList()))
                }
                SARG -> {
                    val arg: Sarg = literal.getValueAs(Sarg::class.java)
                    throw AssertionError(
                        "sargs [" + arg
                                + "] should be handled as part of predicates, not as literals"
                    )
                }
                else -> {}
            }
            val family: SqlTypeFamily = requireNonNull(
                typeName.getFamily()
            ) { "literal $literal has null SqlTypeFamily, and is SqlTypeName is $typeName" }
            return when (family) {
                CHARACTER -> SqlLiteral.createCharString(
                    castNonNull(literal.getValue2()) as String?,
                    POS
                )
                NUMERIC, EXACT_NUMERIC -> SqlLiteral.createExactNumeric(
                    castNonNull(literal.getValueAs(BigDecimal::class.java)).toPlainString(),
                    POS
                )
                APPROXIMATE_NUMERIC -> SqlLiteral.createApproxNumeric(
                    castNonNull(literal.getValueAs(BigDecimal::class.java)).toPlainString(),
                    POS
                )
                BOOLEAN -> SqlLiteral.createBoolean(
                    castNonNull(literal.getValueAs(Boolean::class.java)),
                    POS
                )
                INTERVAL_YEAR_MONTH, INTERVAL_DAY_TIME -> {
                    val negative: Boolean = castNonNull(literal.getValueAs(Boolean::class.java))
                    SqlLiteral.createInterval(
                        if (negative) -1 else 1,
                        castNonNull(literal.getValueAs(String::class.java)),
                        castNonNull(literal.getType().getIntervalQualifier()),
                        POS
                    )
                }
                DATE -> SqlLiteral.createDate(
                    castNonNull(literal.getValueAs(DateString::class.java)),
                    POS
                )
                TIME -> SqlLiteral.createTime(
                    castNonNull(literal.getValueAs(TimeString::class.java)),
                    literal.getType().getPrecision(), POS
                )
                TIMESTAMP -> SqlLiteral.createTimestamp(
                    castNonNull(literal.getValueAs(TimestampString::class.java)),
                    literal.getType().getPrecision(), POS
                )
                ANY, NULL -> {
                    when (typeName) {
                        NULL -> return SqlLiteral.createNull(POS)
                        else -> {}
                    }
                    throw AssertionError(literal.toString() + ": " + typeName)
                }
                else -> throw AssertionError(literal.toString() + ": " + typeName)
            }
        }

        private fun computeFieldCount(
            aliases: Map<String?, RelDataType?>
        ): Int {
            var x = 0
            for (type in aliases.values()) {
                x += type.getFieldCount()
            }
            return x
        }
    }
}
