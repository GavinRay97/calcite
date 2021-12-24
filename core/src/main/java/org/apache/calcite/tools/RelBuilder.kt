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
package org.apache.calcite.tools

import org.apache.calcite.linq4j.Ord

/**
 * Builder for relational expressions.
 *
 *
 * `RelBuilder` does not make possible anything that you could not
 * also accomplish by calling the factory methods of the particular relational
 * expression. But it makes common tasks more straightforward and concise.
 *
 *
 * `RelBuilder` uses factories to create relational expressions.
 * By default, it uses the default factories, which create logical relational
 * expressions ([LogicalFilter],
 * [LogicalProject] and so forth).
 * But you could override those factories so that, say, `filter` creates
 * instead a `HiveFilter`.
 *
 *
 * It is not thread-safe.
 */
@Value.Enclosing
class RelBuilder protected constructor(
    @Nullable context: Context?, cluster: RelOptCluster,
    @Nullable relOptSchema: RelOptSchema
) {
    val cluster: RelOptCluster

    @Nullable
    val relOptSchema: RelOptSchema
    private val stack: Deque<Frame> = ArrayDeque()
    private val simplifier: RexSimplify
    private val config: Config
    private val viewExpander: RelOptTable.ViewExpander
    private var struct: Struct

    init {
        var context: Context? = context
        this.cluster = cluster
        this.relOptSchema = relOptSchema
        if (context == null) {
            context = Contexts.EMPTY_CONTEXT
        }
        config = getConfig(context)
        viewExpander = getViewExpander(cluster, context)
        struct = requireNonNull(RelFactories.Struct.fromContext(context))
        val executor: RexExecutor = context.maybeUnwrap(RexExecutor::class.java)
            .orElse(
                Util.first(
                    cluster.getPlanner().getExecutor(),
                    RexUtil.EXECUTOR
                )
            )
        val predicates: RelOptPredicateList = RelOptPredicateList.EMPTY
        simplifier = RexSimplify(cluster.getRexBuilder(), predicates, executor)
    }

    /** Creates a copy of this RelBuilder, with the same state as this, applying
     * a transform to the config.  */
    fun transform(transform: UnaryOperator<Config?>): RelBuilder {
        val context: Context = Contexts.of(struct, transform.apply(config))
        return RelBuilder(context, cluster, relOptSchema)
    }

    /** Performs an action on this RelBuilder.
     *
     *
     * For example, consider the following code:
     *
     * <blockquote><pre>
     * RelNode filterAndRename(RelBuilder relBuilder, RelNode rel,
     * RexNode condition, List&lt;String&gt; fieldNames) {
     * relBuilder.push(rel)
     * .filter(condition);
     * if (fieldNames != null) {
     * relBuilder.rename(fieldNames);
     * }
     * return relBuilder
     * .build();</pre>
    </blockquote> *
     *
     *
     * The pipeline is disrupted by the 'if'. The `let` method
     * allows you to perform the flow as a single pipeline:
     *
     * <blockquote><pre>
     * RelNode filterAndRename(RelBuilder relBuilder, RelNode rel,
     * RexNode condition, List&lt;String&gt; fieldNames) {
     * return relBuilder.push(rel)
     * .filter(condition)
     * .let(r -&gt; fieldNames == null ? r : r.rename(fieldNames))
     * .build();</pre>
    </blockquote> *
     *
     *
     * In pipelined cases such as this one, the lambda must return this
     * RelBuilder. But `let` return values of other types.
     */
    fun <R> let(consumer: Function<RelBuilder?, R>): R {
        return consumer.apply(this)
    }

    /** Converts this RelBuilder to a string.
     * The string is the string representation of all of the RelNodes on the stack.  */
    @Override
    override fun toString(): String {
        return stack.stream()
            .map { frame -> RelOptUtil.toString(frame.rel) }
            .collect(Collectors.joining(""))
    }

    /** Returns the type factory.  */
    val typeFactory: RelDataTypeFactory
        get() = cluster.getTypeFactory()

    /** Returns new RelBuilder that adopts the convention provided.
     * RelNode will be created with such convention if corresponding factory is provided.  */
    fun adoptConvention(convention: Convention): RelBuilder {
        struct = convention.getRelFactories()
        return this
    }

    /** Returns the builder for [RexNode] expressions.  */
    val rexBuilder: RexBuilder
        get() = cluster.getRexBuilder()

    fun getCluster(): RelOptCluster {
        return cluster
    }

    @Nullable
    fun getRelOptSchema(): RelOptSchema {
        return relOptSchema
    }

    val scanFactory: RelFactories.TableScanFactory
        get() = struct.scanFactory
    // Methods for manipulating the stack
    /** Adds a relational expression to be the input to the next relational
     * expression constructed.
     *
     *
     * This method is usual when you want to weave in relational expressions
     * that are not supported by the builder. If, while creating such expressions,
     * you need to use previously built expressions as inputs, call
     * [.build] to pop those inputs.  */
    fun push(node: RelNode): RelBuilder {
        stack.push(Frame(node))
        return this
    }

    /** Adds a rel node to the top of the stack while preserving the field names
     * and aliases.  */
    private fun replaceTop(node: RelNode) {
        val frame: Frame = stack.pop()
        stack.push(Frame(node, frame.fields))
    }

    /** Pushes a collection of relational expressions.  */
    fun pushAll(nodes: Iterable<RelNode?>): RelBuilder {
        for (node in nodes) {
            push(node)
        }
        return this
    }

    /** Returns the size of the stack.  */
    fun size(): Int {
        return stack.size()
    }

    /** Returns the final relational expression.
     *
     *
     * Throws if the stack is empty.
     */
    fun build(): RelNode {
        return stack.pop().rel
    }

    /** Returns the relational expression at the top of the stack, but does not
     * remove it.  */
    fun peek(): RelNode {
        return castNonNull(peek_()).rel
    }

    @Nullable
    private fun peek_(): Frame {
        return stack.peek()
    }

    /** Returns the relational expression `n` positions from the top of the
     * stack, but does not remove it.  */
    fun peek(n: Int): RelNode {
        return peek_(n).rel
    }

    private fun peek_(n: Int): Frame {
        return if (n == 0) {
            // more efficient than starting an iterator
            Objects.requireNonNull(stack.peek(), "stack.peek")
        } else Iterables.get(stack, n)
    }

    /** Returns the relational expression `n` positions from the top of the
     * stack, but does not remove it.  */
    fun peek(inputCount: Int, inputOrdinal: Int): RelNode {
        return peek_(inputCount, inputOrdinal).rel
    }

    private fun peek_(inputCount: Int, inputOrdinal: Int): Frame {
        return peek_(inputCount - 1 - inputOrdinal)
    }

    /** Returns the number of fields in all inputs before (to the left of)
     * the given input.
     *
     * @param inputCount Number of inputs
     * @param inputOrdinal Input ordinal
     */
    private fun inputOffset(inputCount: Int, inputOrdinal: Int): Int {
        var offset = 0
        for (i in 0 until inputOrdinal) {
            offset += peek(inputCount, i).getRowType().getFieldCount()
        }
        return offset
    }

    /** Evaluates an expression with a relational expression temporarily on the
     * stack.  */
    fun <E> with(r: RelNode, fn: Function<RelBuilder?, E>): E {
        return try {
            push(r)
            fn.apply(this)
        } finally {
            stack.pop()
        }
    }
    // Methods that return scalar expressions
    /** Creates a literal (constant expression).  */
    fun literal(@Nullable value: Object?): RexLiteral {
        val rexBuilder: RexBuilder = cluster.getRexBuilder()
        return if (value == null) {
            val type: RelDataType = typeFactory.createSqlType(SqlTypeName.NULL)
            rexBuilder.makeNullLiteral(type)
        } else if (value is Boolean) {
            rexBuilder.makeLiteral(value as Boolean?)
        } else if (value is BigDecimal) {
            rexBuilder.makeExactLiteral(value as BigDecimal?)
        } else if (value is Float || value is Double) {
            rexBuilder.makeApproxLiteral(
                BigDecimal.valueOf((value as Number).doubleValue())
            )
        } else if (value is Number) {
            rexBuilder.makeExactLiteral(
                BigDecimal.valueOf((value as Number).longValue())
            )
        } else if (value is String) {
            rexBuilder.makeLiteral(value as String?)
        } else if (value is Enum) {
            rexBuilder.makeLiteral(
                value,
                typeFactory.createSqlType(SqlTypeName.SYMBOL)
            )
        } else {
            throw IllegalArgumentException(
                "cannot convert " + value
                        + " (" + value.getClass() + ") to a constant"
            )
        }
    }

    /** Creates a correlation variable for the current input, and writes it into
     * a Holder.  */
    fun variable(v: Holder<RexCorrelVariable?>): RelBuilder {
        v.set(
            rexBuilder.makeCorrel(
                peek().getRowType(),
                cluster.createCorrel()
            ) as RexCorrelVariable
        )
        return this
    }

    /** Creates a reference to a field by name.
     *
     *
     * Equivalent to `field(1, 0, fieldName)`.
     *
     * @param fieldName Field name
     */
    fun field(fieldName: String): RexInputRef {
        return field(1, 0, fieldName)
    }

    /** Creates a reference to a field of given input relational expression
     * by name.
     *
     * @param inputCount Number of inputs
     * @param inputOrdinal Input ordinal
     * @param fieldName Field name
     */
    fun field(inputCount: Int, inputOrdinal: Int, fieldName: String): RexInputRef {
        val frame = peek_(inputCount, inputOrdinal)
        val fieldNames: List<String> = Pair.left(frame.fields())
        val i = fieldNames.indexOf(fieldName)
        return if (i >= 0) {
            field(inputCount, inputOrdinal, i)
        } else {
            throw IllegalArgumentException(
                "field [" + fieldName
                        + "] not found; input fields are: " + fieldNames
            )
        }
    }

    /** Creates a reference to an input field by ordinal.
     *
     *
     * Equivalent to `field(1, 0, ordinal)`.
     *
     * @param fieldOrdinal Field ordinal
     */
    fun field(fieldOrdinal: Int): RexInputRef {
        return field(1, 0, fieldOrdinal, false) as RexInputRef
    }

    /** Creates a reference to a field of a given input relational expression
     * by ordinal.
     *
     * @param inputCount Number of inputs
     * @param inputOrdinal Input ordinal
     * @param fieldOrdinal Field ordinal within input
     */
    fun field(inputCount: Int, inputOrdinal: Int, fieldOrdinal: Int): RexInputRef {
        return field(inputCount, inputOrdinal, fieldOrdinal, false) as RexInputRef
    }

    /** As [.field], but if `alias` is true, the method
     * may apply an alias to make sure that the field has the same name as in the
     * input frame. If no alias is applied the expression is definitely a
     * [RexInputRef].  */
    private fun field(
        inputCount: Int, inputOrdinal: Int, fieldOrdinal: Int,
        alias: Boolean
    ): RexNode {
        val frame = peek_(inputCount, inputOrdinal)
        val input: RelNode = frame.rel
        val rowType: RelDataType = input.getRowType()
        if (fieldOrdinal < 0 || fieldOrdinal > rowType.getFieldCount()) {
            throw IllegalArgumentException(
                "field ordinal [" + fieldOrdinal
                        + "] out of range; input fields are: " + rowType.getFieldNames()
            )
        }
        val field: RelDataTypeField = rowType.getFieldList().get(fieldOrdinal)
        val offset = inputOffset(inputCount, inputOrdinal)
        val ref: RexInputRef = cluster.getRexBuilder()
            .makeInputRef(field.getType(), offset + fieldOrdinal)
        val aliasField: RelDataTypeField = frame.fields()[fieldOrdinal]
        return if (!alias || field.getName().equals(aliasField.getName())) {
            ref
        } else {
            alias(ref, aliasField.getName())
        }
    }

    /** Creates a reference to a field of the current record which originated
     * in a relation with a given alias.  */
    fun field(alias: String, fieldName: String): RexNode {
        return field(1, alias, fieldName)
    }

    /** Creates a reference to a field which originated in a relation with the
     * given alias. Searches for the relation starting at the top of the
     * stack.  */
    fun field(inputCount: Int, alias: String, fieldName: String): RexNode {
        requireNonNull(alias, "alias")
        requireNonNull(fieldName, "fieldName")
        val fields: List<String> = ArrayList()
        for (inputOrdinal in 0 until inputCount) {
            val frame = peek_(inputOrdinal)
            for (p in Ord.zip(frame.fields)) {
                // If alias and field name match, reference that field.
                if (p.e.left.contains(alias)
                    && p.e.right.getName().equals(fieldName)
                ) {
                    return field(inputCount, inputCount - 1 - inputOrdinal, p.i)
                }
                fields.add(
                    String.format(
                        Locale.ROOT, "{aliases=%s,fieldName=%s}", p.e.left,
                        p.e.right.getName()
                    )
                )
            }
        }
        throw IllegalArgumentException(
            "{alias=" + alias + ",fieldName=" + fieldName + "} "
                    + "field not found; fields are: " + fields
        )
    }

    /** Returns a reference to a given field of a record-valued expression.  */
    fun field(e: RexNode?, name: String?): RexNode {
        return rexBuilder.makeFieldAccess(e, name, false)
    }

    /** Returns references to the fields of the top input.  */
    fun fields(): ImmutableList<RexNode> {
        return fields(1, 0)
    }

    /** Returns references to the fields of a given input.  */
    fun fields(inputCount: Int, inputOrdinal: Int): ImmutableList<RexNode> {
        val input: RelNode = peek(inputCount, inputOrdinal)
        val rowType: RelDataType = input.getRowType()
        val nodes: ImmutableList.Builder<RexNode> = ImmutableList.builder()
        for (fieldOrdinal in Util.range(rowType.getFieldCount())) {
            nodes.add(field(inputCount, inputOrdinal, fieldOrdinal))
        }
        return nodes.build()
    }

    /** Returns references to fields for a given collation.  */
    fun fields(collation: RelCollation): ImmutableList<RexNode> {
        val nodes: ImmutableList.Builder<RexNode> = ImmutableList.builder()
        for (fieldCollation in collation.getFieldCollations()) {
            var node: RexNode = field(fieldCollation.getFieldIndex())
            when (fieldCollation.direction) {
                DESCENDING -> node = desc(node)
                else -> {}
            }
            when (fieldCollation.nullDirection) {
                FIRST -> node = nullsFirst(node)
                LAST -> node = nullsLast(node)
                else -> {}
            }
            nodes.add(node)
        }
        return nodes.build()
    }

    /** Returns references to fields for a given list of input ordinals.  */
    fun fields(ordinals: List<Number?>): ImmutableList<RexNode> {
        val nodes: ImmutableList.Builder<RexNode> = ImmutableList.builder()
        for (ordinal in ordinals) {
            val node: RexNode = field(1, 0, ordinal.intValue(), false)
            nodes.add(node)
        }
        return nodes.build()
    }

    /** Returns references to fields for a given bit set of input ordinals.  */
    fun fields(ordinals: ImmutableBitSet): ImmutableList<RexNode> {
        return fields(ordinals.asList())
    }

    /** Returns references to fields identified by name.  */
    fun fields(fieldNames: Iterable<String>): ImmutableList<RexNode> {
        val builder: ImmutableList.Builder<RexNode> = ImmutableList.builder()
        for (fieldName in fieldNames) {
            builder.add(field(fieldName))
        }
        return builder.build()
    }

    /** Returns references to fields identified by a mapping.  */
    fun fields(mapping: Mappings.TargetMapping?): ImmutableList<RexNode> {
        return fields(Mappings.asListNonNull(mapping))
    }

    /** Creates an access to a field by name.  */
    fun dot(node: RexNode?, fieldName: String?): RexNode {
        val builder: RexBuilder = cluster.getRexBuilder()
        return builder.makeFieldAccess(node, fieldName, true)
    }

    /** Creates an access to a field by ordinal.  */
    fun dot(node: RexNode?, fieldOrdinal: Int): RexNode {
        val builder: RexBuilder = cluster.getRexBuilder()
        return builder.makeFieldAccess(node, fieldOrdinal)
    }

    /** Creates a call to a scalar operator.  */
    fun call(operator: SqlOperator, vararg operands: RexNode?): RexNode {
        return call(operator, ImmutableList.copyOf(operands))
    }

    /** Creates a call to a scalar operator.  */
    private fun call(operator: SqlOperator, operandList: List<RexNode>): RexCall {
        when (operator.getKind()) {
            LIKE, SIMILAR -> {
                val likeOperator: SqlLikeOperator = operator as SqlLikeOperator
                if (likeOperator.isNegated()) {
                    val notLikeOperator: SqlOperator = likeOperator.not()
                    return not(call(notLikeOperator, operandList)) as RexCall
                }
            }
            BETWEEN -> {
                assert(operandList.size() === 3)
                return between(
                    operandList[0], operandList[1],
                    operandList[2]
                ) as RexCall
            }
            else -> {}
        }
        val builder: RexBuilder = cluster.getRexBuilder()
        val type: RelDataType = builder.deriveReturnType(operator, operandList)
        return builder.makeCall(type, operator, operandList) as RexCall
    }

    /** Creates a call to a scalar operator.  */
    fun call(
        operator: SqlOperator,
        operands: Iterable<RexNode?>?
    ): RexNode {
        return call(operator, ImmutableList.copyOf(operands))
    }

    /** Creates an IN predicate with a list of values.
     *
     *
     * For example,
     * <pre>`b.scan("Emp")
     * .filter(b.in(b.field("deptno"), b.literal(10), b.literal(20)))
    `</pre> *
     * is equivalent to SQL
     * <pre>`SELECT *
     * FROM Emp
     * WHERE deptno IN (10, 20)
    `</pre> *   */
    fun `in`(arg: RexNode?, vararg ranges: RexNode?): RexNode {
        return `in`(arg, ImmutableList.copyOf(ranges))
    }

    /** Creates an IN predicate with a list of values.
     *
     *
     * For example,
     *
     * <pre>`b.scan("Emps")
     * .filter(
     * b.in(b.field("deptno"),
     * Arrays.asList(b.literal(10), b.literal(20))))
    `</pre> *
     *
     *
     * is equivalent to the SQL
     *
     * <pre>`SELECT *
     * FROM Emps
     * WHERE deptno IN (10, 20)
    `</pre> *   */
    fun `in`(arg: RexNode?, ranges: Iterable<RexNode?>?): RexNode {
        return rexBuilder.makeIn(arg, ImmutableList.copyOf(ranges))
    }

    /** Creates an IN predicate with a sub-query.  */
    @Experimental
    fun `in`(rel: RelNode?, nodes: Iterable<RexNode?>?): RexSubQuery {
        return RexSubQuery.`in`(rel, ImmutableList.copyOf(nodes))
    }

    /** Creates an IN predicate with a sub-query.
     *
     *
     * For example,
     *
     * <pre>`b.scan("Emps")
     * .filter(
     * b.in(b.field("deptno"),
     * b2 -> b2.scan("Depts")
     * .filter(
     * b2.eq(b2.field("location"), b2.literal("Boston")))
     * .project(b.field("deptno"))
     * .build()))
    `</pre> *
     *
     *
     * is equivalent to the SQL
     *
     * <pre>`SELECT *
     * FROM Emps
     * WHERE deptno IN (SELECT deptno FROM Dept WHERE location = 'Boston')
    `</pre> *   */
    @Experimental
    fun `in`(arg: RexNode?, f: Function<RelBuilder?, RelNode?>): RexNode {
        val rel: RelNode = f.apply(this)
        return RexSubQuery.`in`(rel, ImmutableList.of(arg))
    }

    /** Creates a SOME (or ANY) predicate.
     *
     *
     * For example,
     *
     * <pre>`b.scan("Emps")
     * .filter(
     * b.some(b.field("commission"),
     * SqlStdOperatorTable.GREATER_THAN,
     * b2 -> b2.scan("Emps")
     * .filter(
     * b2.eq(b2.field("job"), b2.literal("Manager")))
     * .project(b2.field("sal"))
     * .build()))
    `</pre> *
     *
     *
     * is equivalent to the SQL
     *
     * <pre>`SELECT *
     * FROM Emps
     * WHERE commission > SOME (SELECT sal FROM Emps WHERE job = 'Manager')
    `</pre> *
     *
     *
     * or (since `SOME` and `ANY` are synonyms) the SQL
     *
     * <pre>`SELECT *
     * FROM Emps
     * WHERE commission > ANY (SELECT sal FROM Emps WHERE job = 'Manager')
    `</pre> *   */
    @Experimental
    fun some(
        node: RexNode, op: SqlOperator,
        f: Function<RelBuilder, RelNode>
    ): RexSubQuery {
        return some_(node, op.kind, f)
    }

    private fun some_(
        node: RexNode, kind: SqlKind,
        f: Function<RelBuilder, RelNode>
    ): RexSubQuery {
        val rel: RelNode = f.apply(this)
        val quantifyOperator: SqlQuantifyOperator = SqlStdOperatorTable.some(kind)
        return RexSubQuery.some(rel, ImmutableList.of(node), quantifyOperator)
    }

    /** Creates an ALL predicate.
     *
     *
     * For example,
     *
     * <pre>`b.scan("Emps")
     * .filter(
     * b.all(b.field("commission"),
     * SqlStdOperatorTable.GREATER_THAN,
     * b2 -> b2.scan("Emps")
     * .filter(
     * b2.eq(b2.field("job"), b2.literal("Manager")))
     * .project(b2.field("sal"))
     * .build()))
    `</pre> *
     *
     *
     * is equivalent to the SQL
     *
     * <pre>`SELECT *
     * FROM Emps
     * WHERE commission > ALL (SELECT sal FROM Emps WHERE job = 'Manager')
    `</pre> *
     *
     *
     * Calcite translates `ALL` predicates to `NOT SOME`. The
     * following SQL is equivalent to the previous:
     *
     * <pre>`SELECT *
     * FROM Emps
     * WHERE NOT (commission <= SOME (SELECT sal FROM Emps WHERE job = 'Manager'))
    `</pre> *   */
    @Experimental
    fun all(
        node: RexNode, op: SqlOperator,
        f: Function<RelBuilder, RelNode>
    ): RexNode {
        return not(some_(node, op.kind.negateNullSafe(), f))
    }

    /** Creates an EXISTS predicate.
     *
     *
     * For example,
     *
     * <pre>`b.scan("Depts")
     * .filter(
     * b.exists(b2 ->
     * b2.scan("Emps")
     * .filter(
     * b2.eq(b2.field("job"), b2.literal("Manager")))
     * .build()))
    `</pre> *
     *
     *
     * is equivalent to the SQL
     *
     * <pre>`SELECT *
     * FROM Depts
     * WHERE EXISTS (SELECT 1 FROM Emps WHERE job = 'Manager')
    `</pre> *   */
    @Experimental
    fun exists(f: Function<RelBuilder?, RelNode?>): RexSubQuery {
        val rel: RelNode = f.apply(this)
        return RexSubQuery.exists(rel)
    }

    /** Creates a UNIQUE predicate.
     *
     *
     * For example,
     *
     * <pre>`b.scan("Depts")
     * .filter(
     * b.exists(b2 ->
     * b2.scan("Emps")
     * .filter(
     * b2.eq(b2.field("job"), b2.literal("Manager")))
     * .project(b2.field("deptno")
     * .build()))
    `</pre> *
     *
     *
     * is equivalent to the SQL
     *
     * <pre>`SELECT *
     * FROM Depts
     * WHERE UNIQUE (SELECT deptno FROM Emps WHERE job = 'Manager')
    `</pre> *   */
    @Experimental
    fun unique(f: Function<RelBuilder?, RelNode?>): RexSubQuery {
        val rel: RelNode = f.apply(this)
        return RexSubQuery.unique(rel)
    }

    /** Creates a scalar sub-query.
     *
     *
     * For example,
     *
     * <pre>`b.scan("Depts")
     * .project(
     * b.field("deptno")
     * b.scalarQuery(b2 ->
     * b2.scan("Emps")
     * .aggregate(
     * b2.eq(b2.field("job"), b2.literal("Manager")))
     * .build()))
    `</pre> *
     *
     *
     * is equivalent to the SQL
     *
     * <pre>`SELECT deptno, (SELECT MAX(sal) FROM Emps)
     * FROM Depts
    `</pre> *   */
    @Experimental
    fun scalarQuery(f: Function<RelBuilder?, RelNode?>): RexSubQuery {
        return RexSubQuery.scalar(f.apply(this))
    }

    /** Creates an ARRAY sub-query.
     *
     *
     * For example,
     *
     * <pre>`b.scan("Depts")
     * .project(
     * b.field("deptno")
     * b.arrayQuery(b2 ->
     * b2.scan("Emps")
     * .build()))
    `</pre> *
     *
     *
     * is equivalent to the SQL
     *
     * <pre>`SELECT deptno, ARRAY (SELECT * FROM Emps)
     * FROM Depts
    `</pre> *   */
    @Experimental
    fun arrayQuery(f: Function<RelBuilder?, RelNode?>): RexSubQuery {
        return RexSubQuery.array(f.apply(this))
    }

    /** Creates a MULTISET sub-query.
     *
     *
     * For example,
     *
     * <pre>`b.scan("Depts")
     * .project(
     * b.field("deptno")
     * b.multisetQuery(b2 ->
     * b2.scan("Emps")
     * .build()))
    `</pre> *
     *
     *
     * is equivalent to the SQL
     *
     * <pre>`SELECT deptno, MULTISET (SELECT * FROM Emps)
     * FROM Depts
    `</pre> *   */
    @Experimental
    fun multisetQuery(f: Function<RelBuilder?, RelNode?>): RexSubQuery {
        return RexSubQuery.multiset(f.apply(this))
    }

    /** Creates a MAP sub-query.
     *
     *
     * For example,
     *
     * <pre>`b.scan("Depts")
     * .project(
     * b.field("deptno")
     * b.multisetQuery(b2 ->
     * b2.scan("Emps")
     * .project(b2.field("empno"), b2.field("job"))
     * .build()))
    `</pre> *
     *
     *
     * is equivalent to the SQL
     *
     * <pre>`SELECT deptno, MAP (SELECT empno, job FROM Emps)
     * FROM Depts
    `</pre> *   */
    @Experimental
    fun mapQuery(f: Function<RelBuilder?, RelNode?>): RexSubQuery {
        return RexSubQuery.map(f.apply(this))
    }

    /** Creates an AND.  */
    fun and(vararg operands: RexNode?): RexNode {
        return and(ImmutableList.copyOf(operands))
    }

    /** Creates an AND.
     *
     *
     * Simplifies the expression a little:
     * `e AND TRUE` becomes `e`;
     * `e AND e2 AND NOT e` becomes `e2`.  */
    fun and(operands: Iterable<RexNode?>?): RexNode {
        return RexUtil.composeConjunction(rexBuilder, operands)
    }

    /** Creates an OR.  */
    fun or(vararg operands: RexNode?): RexNode {
        return or(ImmutableList.copyOf(operands))
    }

    /** Creates an OR.  */
    fun or(operands: Iterable<RexNode?>?): RexNode {
        return RexUtil.composeDisjunction(cluster.getRexBuilder(), operands)
    }

    /** Creates a NOT.  */
    fun not(operand: RexNode): RexNode {
        return call(SqlStdOperatorTable.NOT, operand)
    }

    /** Creates an `=`.  */
    fun equals(operand0: RexNode?, operand1: RexNode?): RexNode {
        return call(SqlStdOperatorTable.EQUALS, operand0, operand1)
    }

    /** Creates a `>`.  */
    fun greaterThan(operand0: RexNode?, operand1: RexNode?): RexNode {
        return call(SqlStdOperatorTable.GREATER_THAN, operand0, operand1)
    }

    /** Creates a `>=`.  */
    fun greaterThanOrEqual(operand0: RexNode?, operand1: RexNode?): RexNode {
        return call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, operand0, operand1)
    }

    /** Creates a `<`.  */
    fun lessThan(operand0: RexNode?, operand1: RexNode?): RexNode {
        return call(SqlStdOperatorTable.LESS_THAN, operand0, operand1)
    }

    /** Creates a `<=`.  */
    fun lessThanOrEqual(operand0: RexNode?, operand1: RexNode?): RexNode {
        return call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, operand0, operand1)
    }

    /** Creates a `<>`.  */
    fun notEquals(operand0: RexNode?, operand1: RexNode?): RexNode {
        return call(SqlStdOperatorTable.NOT_EQUALS, operand0, operand1)
    }

    /** Creates an expression equivalent to "`o0 IS NOT DISTINCT FROM o1`".
     * It is also equivalent to
     * "`o0 = o1 OR (o0 IS NULL AND o1 IS NULL)`".  */
    fun isNotDistinctFrom(operand0: RexNode?, operand1: RexNode?): RexNode {
        return RelOptUtil.isDistinctFrom(rexBuilder, operand0, operand1, true)
    }

    /** Creates an expression equivalent to `o0 IS DISTINCT FROM o1`.
     * It is also equivalent to
     * "`NOT (o0 = o1 OR (o0 IS NULL AND o1 IS NULL))`.  */
    fun isDistinctFrom(operand0: RexNode?, operand1: RexNode?): RexNode {
        return RelOptUtil.isDistinctFrom(rexBuilder, operand0, operand1, false)
    }

    /** Creates a `BETWEEN`.  */
    fun between(arg: RexNode?, lower: RexNode?, upper: RexNode?): RexNode {
        return rexBuilder.makeBetween(arg, lower, upper)
    }

    /** Creates ab `IS NULL`.  */
    fun isNull(operand: RexNode): RexNode {
        return call(SqlStdOperatorTable.IS_NULL, operand)
    }

    /** Creates an `IS NOT NULL`.  */
    fun isNotNull(operand: RexNode): RexNode {
        return call(SqlStdOperatorTable.IS_NOT_NULL, operand)
    }

    /** Creates an expression that casts an expression to a given type.  */
    fun cast(expr: RexNode?, typeName: SqlTypeName?): RexNode {
        val type: RelDataType = cluster.getTypeFactory().createSqlType(typeName)
        return cluster.getRexBuilder().makeCast(type, expr)
    }

    /** Creates an expression that casts an expression to a type with a given name
     * and precision or length.  */
    fun cast(expr: RexNode?, typeName: SqlTypeName?, precision: Int): RexNode {
        val type: RelDataType = cluster.getTypeFactory().createSqlType(typeName, precision)
        return cluster.getRexBuilder().makeCast(type, expr)
    }

    /** Creates an expression that casts an expression to a type with a given
     * name, precision and scale.  */
    fun cast(
        expr: RexNode?, typeName: SqlTypeName?, precision: Int,
        scale: Int
    ): RexNode {
        val type: RelDataType = cluster.getTypeFactory().createSqlType(typeName, precision, scale)
        return cluster.getRexBuilder().makeCast(type, expr)
    }

    /**
     * Returns an expression wrapped in an alias.
     *
     *
     * This method is idempotent: If the expression is already wrapped in the
     * correct alias, does nothing; if wrapped in an incorrect alias, removes
     * the incorrect alias and applies the correct alias.
     *
     * @see .project
     */
    fun alias(expr: RexNode, @Nullable alias: String?): RexNode {
        var expr: RexNode = expr
        val aliasLiteral: RexNode = literal(alias)
        return when (expr.getKind()) {
            AS -> {
                val call: RexCall = expr as RexCall
                if (call.operands.get(1).equals(aliasLiteral)) {
                    // current alias is correct
                    return expr
                }
                expr = call.operands.get(0)
                call(SqlStdOperatorTable.AS, expr, aliasLiteral)
            }
            else -> call(SqlStdOperatorTable.AS, expr, aliasLiteral)
        }
    }

    /** Converts a sort expression to descending.  */
    fun desc(node: RexNode): RexNode {
        return call(SqlStdOperatorTable.DESC, node)
    }

    /** Converts a sort expression to nulls last.  */
    fun nullsLast(node: RexNode): RexNode {
        return call(SqlStdOperatorTable.NULLS_LAST, node)
    }

    /** Converts a sort expression to nulls first.  */
    fun nullsFirst(node: RexNode): RexNode {
        return call(SqlStdOperatorTable.NULLS_FIRST, node)
    }
    // Methods that create window bounds
    /** Creates an `UNBOUNDED PRECEDING` window bound,
     * for use in methods such as [OverCall.rowsFrom]
     * and [OverCall.rangeBetween].  */
    fun unboundedPreceding(): RexWindowBound {
        return RexWindowBounds.UNBOUNDED_PRECEDING
    }

    /** Creates a `bound PRECEDING` window bound,
     * for use in methods such as [OverCall.rowsFrom]
     * and [OverCall.rangeBetween].  */
    fun preceding(bound: RexNode?): RexWindowBound {
        return RexWindowBounds.preceding(bound)
    }

    /** Creates a `CURRENT ROW` window bound,
     * for use in methods such as [OverCall.rowsFrom]
     * and [OverCall.rangeBetween].  */
    fun currentRow(): RexWindowBound {
        return RexWindowBounds.CURRENT_ROW
    }

    /** Creates a `bound FOLLOWING` window bound,
     * for use in methods such as [OverCall.rowsFrom]
     * and [OverCall.rangeBetween].  */
    fun following(bound: RexNode?): RexWindowBound {
        return RexWindowBounds.following(bound)
    }

    /** Creates an `UNBOUNDED FOLLOWING` window bound,
     * for use in methods such as [OverCall.rowsFrom]
     * and [OverCall.rangeBetween].  */
    fun unboundedFollowing(): RexWindowBound {
        return RexWindowBounds.UNBOUNDED_FOLLOWING
    }
    // Methods that create group keys and aggregate calls
    /** Creates an empty group key.  */
    fun groupKey(): GroupKey {
        return groupKey(ImmutableList.of())
    }

    /** Creates a group key.  */
    fun groupKey(vararg nodes: RexNode?): GroupKey {
        return groupKey(ImmutableList.copyOf(nodes))
    }

    /** Creates a group key.  */
    fun groupKey(nodes: Iterable<RexNode?>?): GroupKey {
        return GroupKeyImpl(ImmutableList.copyOf(nodes), null, null)
    }

    /** Creates a group key with grouping sets.  */
    fun groupKey(
        nodes: Iterable<RexNode?>,
        nodeLists: Iterable<Iterable<RexNode?>?>
    ): GroupKey {
        return Companion.groupKey_(nodes, nodeLists)
    }
    // CHECKSTYLE: IGNORE 1

    @Deprecated // to be removed before 2.0
    @Deprecated(
        """Now that indicator is deprecated, use
    {@link #groupKey(Iterable, Iterable)}, which has the same behavior as
    calling this method with {@code indicator = false}. """
    )
    fun groupKey(
        nodes: Iterable<RexNode?>, indicator: Boolean,
        nodeLists: Iterable<Iterable<RexNode?>?>
    ): GroupKey {
        Aggregate.checkIndicator(indicator)
        return Companion.groupKey_(nodes, nodeLists)
    }

    /** Creates a group key of fields identified by ordinal.  */
    fun groupKey(vararg fieldOrdinals: Int): GroupKey {
        return groupKey(fields(ImmutableIntList.of(fieldOrdinals)))
    }

    /** Creates a group key of fields identified by name.  */
    fun groupKey(vararg fieldNames: String?): GroupKey {
        return groupKey(fields(ImmutableList.copyOf(fieldNames)))
    }

    /** Creates a group key, identified by field positions
     * in the underlying relational expression.
     *
     *
     * This method of creating a group key does not allow you to group on new
     * expressions, only column projections, but is efficient, especially when you
     * are coming from an existing [Aggregate].  */
    fun groupKey(groupSet: ImmutableBitSet): GroupKey {
        return groupKey_(groupSet, ImmutableList.of(groupSet))
    }

    /** Creates a group key with grouping sets, both identified by field positions
     * in the underlying relational expression.
     *
     *
     * This method of creating a group key does not allow you to group on new
     * expressions, only column projections, but is efficient, especially when you
     * are coming from an existing [Aggregate].
     *
     *
     * It is possible for `groupSet` to be strict superset of all
     * `groupSets`. For example, in the pseudo SQL
     *
     * <pre>`GROUP BY 0, 1, 2
     * GROUPING SETS ((0, 1), 0)
    `</pre> *
     *
     *
     * column 2 does not appear in either grouping set. This is not valid SQL.
     * We can approximate in actual SQL by adding an extra grouping set and
     * filtering out using `HAVING`, as follows:
     *
     * <pre>`GROUP BY GROUPING SETS ((0, 1, 2), (0, 1), 0)
     * HAVING GROUPING_ID(0, 1, 2) <> 0
    `</pre> *
     */
    fun groupKey(
        groupSet: ImmutableBitSet,
        groupSets: Iterable<ImmutableBitSet?>?
    ): GroupKey {
        return groupKey_(groupSet, ImmutableList.copyOf(groupSets))
    }
    // CHECKSTYLE: IGNORE 1

    @Deprecated // to be removed before 2.0
    @Deprecated("Use {@link #groupKey(ImmutableBitSet, Iterable)}. ")
    fun groupKey(
        groupSet: ImmutableBitSet, indicator: Boolean,
        @Nullable groupSets: ImmutableList<ImmutableBitSet?>?
    ): GroupKey {
        Aggregate.checkIndicator(indicator)
        return groupKey_(
            groupSet,
            if (groupSets == null) ImmutableList.of(groupSet) else ImmutableList.copyOf(groupSets)
        )
    }

    private fun groupKey_(
        groupSet: ImmutableBitSet,
        groupSets: ImmutableList<ImmutableBitSet>
    ): GroupKey {
        if (groupSet.length() > peek().getRowType().getFieldCount()) {
            throw IllegalArgumentException("out of bounds: $groupSet")
        }
        requireNonNull(groupSets, "groupSets")
        val nodes: ImmutableList<RexNode> = fields(groupSet)
        return groupKey_(nodes, Util.transform(groupSets, this::fields))
    }

    @Deprecated // to be removed before 2.0
    fun aggregateCall(
        aggFunction: SqlAggFunction?, distinct: Boolean,
        filter: RexNode?, @Nullable alias: String?, vararg operands: RexNode?
    ): AggCall {
        return aggregateCall(
            aggFunction, distinct, false, false, filter, null,
            ImmutableList.of(), alias, ImmutableList.copyOf(operands)
        )
    }

    @Deprecated // to be removed before 2.0
    fun aggregateCall(
        aggFunction: SqlAggFunction?, distinct: Boolean,
        approximate: Boolean, filter: RexNode?, @Nullable alias: String?, vararg operands: RexNode?
    ): AggCall {
        return aggregateCall(
            aggFunction, distinct, approximate, false, filter,
            null, ImmutableList.of(), alias, ImmutableList.copyOf(operands)
        )
    }

    @Deprecated // to be removed before 2.0
    fun aggregateCall(
        aggFunction: SqlAggFunction?, distinct: Boolean,
        filter: RexNode?, @Nullable alias: String?, operands: Iterable<RexNode?>?
    ): AggCall {
        return aggregateCall(
            aggFunction, distinct, false, false, filter, null,
            ImmutableList.of(), alias, ImmutableList.copyOf(operands)
        )
    }

    @Deprecated // to be removed before 2.0
    fun aggregateCall(
        aggFunction: SqlAggFunction?, distinct: Boolean,
        approximate: Boolean, filter: RexNode?, @Nullable alias: String?,
        operands: Iterable<RexNode?>?
    ): AggCall {
        return aggregateCall(
            aggFunction, distinct, approximate, false, filter,
            null, ImmutableList.of(), alias, ImmutableList.copyOf(operands)
        )
    }

    /** Creates a call to an aggregate function.
     *
     *
     * To add other operands, apply
     * [AggCall.distinct],
     * [AggCall.approximate],
     * [AggCall.filter],
     * [AggCall.sort],
     * [AggCall. as] to the result.  */
    fun aggregateCall(
        aggFunction: SqlAggFunction?,
        operands: Iterable<RexNode?>?
    ): AggCall {
        return aggregateCall(
            aggFunction, false, false, false, null, null,
            ImmutableList.of(), null, ImmutableList.copyOf(operands)
        )
    }

    /** Creates a call to an aggregate function.
     *
     *
     * To add other operands, apply
     * [AggCall.distinct],
     * [AggCall.approximate],
     * [AggCall.filter],
     * [AggCall.sort],
     * [AggCall. as] to the result.  */
    fun aggregateCall(
        aggFunction: SqlAggFunction?,
        vararg operands: RexNode?
    ): AggCall {
        return aggregateCall(
            aggFunction, false, false, false, null, null,
            ImmutableList.of(), null, ImmutableList.copyOf(operands)
        )
    }

    /** Creates a call to an aggregate function as a copy of an
     * [AggregateCall].  */
    fun aggregateCall(a: AggregateCall): AggCall {
        return aggregateCall(
            a.getAggregation(), a.isDistinct(), a.isApproximate(),
            a.ignoreNulls(), if (a.filterArg < 0) null else field(a.filterArg),
            if (a.distinctKeys == null) null else fields(a.distinctKeys),
            fields(a.collation), a.name, fields(a.getArgList())
        )
    }

    /** Creates a call to an aggregate function as a copy of an
     * [AggregateCall], applying a mapping.  */
    fun aggregateCall(a: AggregateCall, mapping: Mapping?): AggCall {
        return aggregateCall(
            a.getAggregation(), a.isDistinct(), a.isApproximate(),
            a.ignoreNulls(),
            if (a.filterArg < 0) null else field(Mappings.apply(mapping, a.filterArg)),
            if (a.distinctKeys == null) null else fields(Mappings.apply(mapping, a.distinctKeys)),
            fields(RexUtil.apply(mapping, a.collation)), a.name,
            fields(Mappings.apply2(mapping, a.getArgList()))
        )
    }

    /** Creates a call to an aggregate function with all applicable operands.  */
    protected fun aggregateCall(
        aggFunction: SqlAggFunction?, distinct: Boolean,
        approximate: Boolean, ignoreNulls: Boolean, @Nullable filter: RexNode?,
        @Nullable distinctKeys: ImmutableList<RexNode?>?,
        orderKeys: ImmutableList<RexNode?>?,
        @Nullable alias: String?, operands: ImmutableList<RexNode?>?
    ): AggCall {
        return AggCallImpl(
            aggFunction, distinct, approximate, ignoreNulls,
            filter, alias, operands, distinctKeys, orderKeys
        )
    }

    /** Creates a call to the `COUNT` aggregate function.  */
    fun count(vararg operands: RexNode?): AggCall {
        return count(false, null, *operands)
    }

    /** Creates a call to the `COUNT` aggregate function.  */
    fun count(operands: Iterable<RexNode?>?): AggCall {
        return count(false, null, operands)
    }

    /** Creates a call to the `COUNT` aggregate function,
     * optionally distinct and with an alias.  */
    fun count(distinct: Boolean, @Nullable alias: String?, vararg operands: RexNode?): AggCall {
        return aggregateCall(
            SqlStdOperatorTable.COUNT, distinct, false, false, null,
            null, ImmutableList.of(), alias, ImmutableList.copyOf(operands)
        )
    }

    /** Creates a call to the `COUNT` aggregate function,
     * optionally distinct and with an alias.  */
    fun count(
        distinct: Boolean, @Nullable alias: String?,
        operands: Iterable<RexNode?>?
    ): AggCall {
        return aggregateCall(
            SqlStdOperatorTable.COUNT, distinct, false, false, null,
            null, ImmutableList.of(), alias, ImmutableList.copyOf(operands)
        )
    }

    /** Creates a call to the `COUNT(*)` aggregate function.  */
    fun countStar(@Nullable alias: String?): AggCall {
        return count(false, alias)
    }

    /** Creates a call to the `SUM` aggregate function.  */
    fun sum(operand: RexNode?): AggCall {
        return sum(false, null, operand)
    }

    /** Creates a call to the `SUM` aggregate function,
     * optionally distinct and with an alias.  */
    fun sum(distinct: Boolean, @Nullable alias: String?, operand: RexNode?): AggCall {
        return aggregateCall(
            SqlStdOperatorTable.SUM, distinct, false, false, null,
            null, ImmutableList.of(), alias, ImmutableList.of(operand)
        )
    }

    /** Creates a call to the `AVG` aggregate function.  */
    fun avg(operand: RexNode?): AggCall {
        return avg(false, null, operand)
    }

    /** Creates a call to the `AVG` aggregate function,
     * optionally distinct and with an alias.  */
    fun avg(distinct: Boolean, @Nullable alias: String?, operand: RexNode?): AggCall {
        return aggregateCall(
            SqlStdOperatorTable.AVG, distinct, false, false, null,
            null, ImmutableList.of(), alias, ImmutableList.of(operand)
        )
    }

    /** Creates a call to the `MIN` aggregate function.  */
    fun min(operand: RexNode?): AggCall {
        return min(null, operand)
    }

    /** Creates a call to the `MIN` aggregate function,
     * optionally with an alias.  */
    fun min(@Nullable alias: String?, operand: RexNode?): AggCall {
        return aggregateCall(
            SqlStdOperatorTable.MIN, false, false, false, null,
            null, ImmutableList.of(), alias, ImmutableList.of(operand)
        )
    }

    /** Creates a call to the `MAX` aggregate function,
     * optionally with an alias.  */
    fun max(operand: RexNode?): AggCall {
        return max(null, operand)
    }

    /** Creates a call to the `MAX` aggregate function.  */
    fun max(@Nullable alias: String?, operand: RexNode?): AggCall {
        return aggregateCall(
            SqlStdOperatorTable.MAX, false, false, false, null,
            null, ImmutableList.of(), alias, ImmutableList.of(operand)
        )
    }
    // Methods for patterns
    /**
     * Creates a reference to a given field of the pattern.
     *
     * @param alpha the pattern name
     * @param type Type of field
     * @param i Ordinal of field
     * @return Reference to field of pattern
     */
    fun patternField(alpha: String?, type: RelDataType?, i: Int): RexNode {
        return rexBuilder.makePatternFieldRef(alpha, type, i)
    }

    /** Creates a call that concatenates patterns;
     * for use in [.match].  */
    fun patternConcat(nodes: Iterable<RexNode?>?): RexNode {
        val list: ImmutableList<RexNode> = ImmutableList.copyOf(nodes)
        if (list.size() > 2) {
            // Convert into binary calls
            return patternConcat(patternConcat(Util.skipLast(list)), Util.last(list))
        }
        val t: RelDataType = typeFactory.createSqlType(SqlTypeName.NULL)
        return rexBuilder.makeCall(
            t, SqlStdOperatorTable.PATTERN_CONCAT,
            list
        )
    }

    /** Creates a call that concatenates patterns;
     * for use in [.match].  */
    fun patternConcat(vararg nodes: RexNode?): RexNode {
        return patternConcat(ImmutableList.copyOf(nodes))
    }

    /** Creates a call that creates alternate patterns;
     * for use in [.match].  */
    fun patternAlter(nodes: Iterable<RexNode?>?): RexNode {
        val t: RelDataType = typeFactory.createSqlType(SqlTypeName.NULL)
        return rexBuilder.makeCall(
            t, SqlStdOperatorTable.PATTERN_ALTER,
            ImmutableList.copyOf(nodes)
        )
    }

    /** Creates a call that creates alternate patterns;
     * for use in [.match].  */
    fun patternAlter(vararg nodes: RexNode?): RexNode {
        return patternAlter(ImmutableList.copyOf(nodes))
    }

    /** Creates a call that creates quantify patterns;
     * for use in [.match].  */
    fun patternQuantify(nodes: Iterable<RexNode?>?): RexNode {
        val t: RelDataType = typeFactory.createSqlType(SqlTypeName.NULL)
        return rexBuilder.makeCall(
            t, SqlStdOperatorTable.PATTERN_QUANTIFIER,
            ImmutableList.copyOf(nodes)
        )
    }

    /** Creates a call that creates quantify patterns;
     * for use in [.match].  */
    fun patternQuantify(vararg nodes: RexNode?): RexNode {
        return patternQuantify(ImmutableList.copyOf(nodes))
    }

    /** Creates a call that creates permute patterns;
     * for use in [.match].  */
    fun patternPermute(nodes: Iterable<RexNode?>?): RexNode {
        val t: RelDataType = typeFactory.createSqlType(SqlTypeName.NULL)
        return rexBuilder.makeCall(
            t, SqlStdOperatorTable.PATTERN_PERMUTE,
            ImmutableList.copyOf(nodes)
        )
    }

    /** Creates a call that creates permute patterns;
     * for use in [.match].  */
    fun patternPermute(vararg nodes: RexNode?): RexNode {
        return patternPermute(ImmutableList.copyOf(nodes))
    }

    /** Creates a call that creates an exclude pattern;
     * for use in [.match].  */
    fun patternExclude(node: RexNode?): RexNode {
        val t: RelDataType = typeFactory.createSqlType(SqlTypeName.NULL)
        return rexBuilder.makeCall(
            t, SqlStdOperatorTable.PATTERN_EXCLUDE,
            ImmutableList.of(node)
        )
    }
    // Methods that create relational expressions
    /** Creates a [TableScan] of the table
     * with a given name.
     *
     *
     * Throws if the table does not exist.
     *
     *
     * Returns this builder.
     *
     * @param tableNames Name of table (can optionally be qualified)
     */
    fun scan(tableNames: Iterable<String?>?): RelBuilder {
        val names: List<String> = ImmutableList.copyOf(tableNames)
        requireNonNull(relOptSchema, "relOptSchema")
        val relOptTable: RelOptTable = relOptSchema.getTableForMember(names)
            ?: throw RESOURCE.tableNotFound(String.join(".", names)).ex()
        val scan: RelNode = struct.scanFactory.createScan(
            ViewExpanders.toRelContext(viewExpander, cluster),
            relOptTable
        )
        push(scan)
        rename(relOptTable.getRowType().getFieldNames())

        // When the node is not a TableScan but from expansion,
        // we need to explicitly add the alias.
        if (scan !is TableScan) {
            `as`(Util.last(ImmutableList.copyOf(tableNames)))
        }
        return this
    }

    /** Creates a [TableScan] of the table
     * with a given name.
     *
     *
     * Throws if the table does not exist.
     *
     *
     * Returns this builder.
     *
     * @param tableNames Name of table (can optionally be qualified)
     */
    fun scan(vararg tableNames: String?): RelBuilder {
        return scan(ImmutableList.copyOf(tableNames))
    }

    /** Creates a [Snapshot] of a given snapshot period.
     *
     *
     * Returns this builder.
     *
     * @param period Name of table (can optionally be qualified)
     */
    fun snapshot(period: RexNode?): RelBuilder {
        val frame: Frame = stack.pop()
        val snapshot: RelNode = struct.snapshotFactory.createSnapshot(frame.rel, period)
        stack.push(Frame(snapshot, frame.fields))
        return this
    }

    /**
     * Creates a RexCall to the `CURSOR` function by ordinal.
     *
     * @param inputCount Number of inputs
     * @param ordinal The reference to the relational input
     * @return RexCall to CURSOR function
     */
    fun cursor(inputCount: Int, ordinal: Int): RexNode {
        if (inputCount <= ordinal || ordinal < 0) {
            throw IllegalArgumentException("bad input count or ordinal")
        }
        // Refer to the "ordinal"th input as if it were a field
        // (because that's how things are laid out inside a TableFunctionScan)
        val input: RelNode = peek(inputCount, ordinal)
        return call(
            SqlStdOperatorTable.CURSOR,
            rexBuilder.makeInputRef(input.getRowType(), ordinal)
        )
    }

    /** Creates a [TableFunctionScan].  */
    fun functionScan(
        operator: SqlOperator,
        inputCount: Int, vararg operands: RexNode?
    ): RelBuilder {
        return functionScan(operator, inputCount, ImmutableList.copyOf(operands))
    }

    /** Creates a [TableFunctionScan].  */
    fun functionScan(
        operator: SqlOperator,
        inputCount: Int, operands: Iterable<RexNode?>?
    ): RelBuilder {
        if (inputCount < 0 || inputCount > stack.size()) {
            throw IllegalArgumentException("bad input count")
        }

        // Gets inputs.
        val inputs: List<RelNode> = ArrayList()
        for (i in 0 until inputCount) {
            inputs.add(0, build())
        }
        val call: RexCall = call(operator, ImmutableList.copyOf(operands))
        val functionScan: RelNode = struct.tableFunctionScanFactory.createTableFunctionScan(
            cluster,
            inputs, call, null, getColumnMappings(operator)
        )
        push(functionScan)
        return this
    }

    /** Creates a [Filter] of an array of
     * predicates.
     *
     *
     * The predicates are combined using AND,
     * and optimized in a similar way to the [.and] method.
     * If the result is TRUE no filter is created.  */
    fun filter(vararg predicates: RexNode?): RelBuilder {
        return filter(ImmutableSet.of(), ImmutableList.copyOf(predicates))
    }

    /** Creates a [Filter] of a list of
     * predicates.
     *
     *
     * The predicates are combined using AND,
     * and optimized in a similar way to the [.and] method.
     * If the result is TRUE no filter is created.  */
    fun filter(predicates: Iterable<RexNode?>?): RelBuilder {
        return filter(ImmutableSet.of(), predicates)
    }

    /** Creates a [Filter] of a list of correlation variables
     * and an array of predicates.
     *
     *
     * The predicates are combined using AND,
     * and optimized in a similar way to the [.and] method.
     * If the result is TRUE no filter is created.  */
    fun filter(
        variablesSet: Iterable<CorrelationId?>?,
        vararg predicates: RexNode?
    ): RelBuilder {
        return filter(variablesSet, ImmutableList.copyOf(predicates))
    }

    /**
     * Creates a [Filter] of a list of correlation variables
     * and a list of predicates.
     *
     *
     * The predicates are combined using AND,
     * and optimized in a similar way to the [.and] method.
     * If simplification is on and the result is TRUE, no filter is created.  */
    fun filter(
        variablesSet: Iterable<CorrelationId?>?,
        predicates: Iterable<RexNode?>?
    ): RelBuilder {
        val conjunctionPredicates: RexNode
        conjunctionPredicates = if (config.simplify()) {
            simplifier.simplifyFilterPredicates(predicates)
        } else {
            RexUtil.composeConjunction(simplifier.rexBuilder, predicates)
        }
        if (conjunctionPredicates == null || conjunctionPredicates.isAlwaysFalse()) {
            return empty()
        }
        if (conjunctionPredicates.isAlwaysTrue()) {
            return this
        }
        val frame: Frame = stack.pop()
        val filter: RelNode = struct.filterFactory.createFilter(
            frame.rel,
            conjunctionPredicates, ImmutableSet.copyOf(variablesSet)
        )
        stack.push(Frame(filter, frame.fields))
        return this
    }

    /** Creates a [Project] of the given
     * expressions.  */
    fun project(vararg nodes: RexNode?): RelBuilder {
        return project(ImmutableList.copyOf(nodes))
    }

    /** Creates a [Project] of the given list
     * of expressions.
     *
     *
     * Infers names as would [.project] if all
     * suggested names were null.
     *
     * @param nodes Expressions
     */
    fun project(nodes: Iterable<RexNode?>?): RelBuilder {
        return project(nodes, ImmutableList.of())
    }

    /** Creates a [Project] of the given list
     * of expressions and field names.
     *
     * @param nodes Expressions
     * @param fieldNames field names for expressions
     */
    fun project(
        nodes: Iterable<RexNode?>?,
        fieldNames: Iterable<String?>?
    ): RelBuilder {
        return project(nodes, fieldNames, false)
    }

    /** Creates a [Project] of the given list
     * of expressions, using the given names.
     *
     *
     * Names are deduced as follows:
     *
     *  * If the length of `fieldNames` is greater than the index of
     * the current entry in `nodes`, and the entry in
     * `fieldNames` is not null, uses it; otherwise
     *  * If an expression projects an input field,
     * or is a cast an input field,
     * uses the input field name; otherwise
     *  * If an expression is a call to
     * [SqlStdOperatorTable.AS]
     * (see [.alias]), removes the call but uses the intended alias.
     *
     *
     *
     * After the field names have been inferred, makes the
     * field names unique by appending numeric suffixes.
     *
     * @param nodes Expressions
     * @param fieldNames Suggested field names
     * @param force create project even if it is identity
     */
    fun project(
        nodes: Iterable<RexNode?>?,
        fieldNames: Iterable<String?>?, force: Boolean
    ): RelBuilder {
        return project_(nodes, fieldNames, ImmutableList.of(), force)
    }

    /** Creates a [Project] of all original fields, plus the given
     * expressions.  */
    fun projectPlus(vararg nodes: RexNode?): RelBuilder {
        return projectPlus(ImmutableList.copyOf(nodes))
    }

    /** Creates a [Project] of all original fields, plus the given list of
     * expressions.  */
    fun projectPlus(nodes: Iterable<RexNode?>?): RelBuilder {
        return project(Iterables.concat(fields(), nodes))
    }

    /** Creates a [Project] of all original fields, except the given
     * expressions.
     *
     * @throws IllegalArgumentException if the given expressions contain duplicates
     * or there is an expression that does not match an existing field
     */
    fun projectExcept(vararg expressions: RexNode?): RelBuilder {
        return projectExcept(ImmutableList.copyOf(expressions))
    }

    /** Creates a [Project] of all original fields, except the given list of
     * expressions.
     *
     * @throws IllegalArgumentException if the given expressions contain duplicates
     * or there is an expression that does not match an existing field
     */
    fun projectExcept(expressions: Iterable<RexNode?>): RelBuilder {
        val allExpressions: List<RexNode> = ArrayList(fields())
        val excludeExpressions: Set<RexNode> = HashSet()
        for (excludeExp in expressions) {
            if (!excludeExpressions.add(excludeExp)) {
                throw IllegalArgumentException(
                    "Input list contains duplicates. Expression $excludeExp exists multiple times."
                )
            }
            if (!allExpressions.remove(excludeExp)) {
                throw IllegalArgumentException("Expression " + excludeExp.toString().toString() + " not found.")
            }
        }
        return this.project(allExpressions)
    }

    /** Creates a [Project] of the given list
     * of expressions, using the given names.
     *
     *
     * Names are deduced as follows:
     *
     *  * If the length of `fieldNames` is greater than the index of
     * the current entry in `nodes`, and the entry in
     * `fieldNames` is not null, uses it; otherwise
     *  * If an expression projects an input field,
     * or is a cast an input field,
     * uses the input field name; otherwise
     *  * If an expression is a call to
     * [SqlStdOperatorTable.AS]
     * (see [.alias]), removes the call but uses the intended alias.
     *
     *
     *
     * After the field names have been inferred, makes the
     * field names unique by appending numeric suffixes.
     *
     * @param nodes Expressions
     * @param fieldNames Suggested field names
     * @param hints Hints
     * @param force create project even if it is identity
     */
    private fun project_(
        nodes: Iterable<RexNode?>?,
        fieldNames: Iterable<String?>?,
        hints: Iterable<RelHint>,
        force: Boolean
    ): RelBuilder {
        val frame: Frame = requireNonNull(peek_(), "frame stack is empty")
        val inputRowType: RelDataType = frame.rel.getRowType()
        val nodeList: List<RexNode> = Lists.newArrayList(nodes)

        // Perform a quick check for identity. We'll do a deeper check
        // later when we've derived column names.
        if (!force && Iterables.isEmpty(fieldNames)
            && RexUtil.isIdentity(nodeList, inputRowType)
        ) {
            return this
        }
        val fieldNameList: List<String?> = Lists.newArrayList(fieldNames)
        while (fieldNameList.size() < nodeList.size()) {
            fieldNameList.add(null)
        }
        bloat@ if (frame.rel is Project
            && config.bloat() >= 0
        ) {
            val project: Project = frame.rel as Project
            // Populate field names. If the upper expression is an input ref and does
            // not have a recommended name, use the name of the underlying field.
            for (i in 0 until fieldNameList.size()) {
                if (fieldNameList[i] == null) {
                    val node: RexNode = nodeList[i]
                    if (node is RexInputRef) {
                        val ref: RexInputRef = node as RexInputRef
                        fieldNameList.set(
                            i,
                            project.getRowType().getFieldNames().get(ref.getIndex())
                        )
                    }
                }
            }
            val newNodes: List<RexNode> = RelOptUtil.pushPastProjectUnlessBloat(
                nodeList, project,
                config.bloat()
            )
                ?: // The merged expression is more complex than the input expressions.
                // Do not merge.
                break@bloat

            // Carefully build a list of fields, so that table aliases from the input
            // can be seen for fields that are based on a RexInputRef.
            val frame1: Frame = stack.pop()
            val fields: List<Field> = ArrayList()
            for (f in project.getInput().getRowType().getFieldList()) {
                fields.add(Field(ImmutableSet.of(), f))
            }
            for (pair in Pair.zip(project.getProjects(), frame1.fields)) {
                when (pair.left.getKind()) {
                    INPUT_REF -> {
                        val i: Int = (pair.left as RexInputRef).getIndex()
                        val field = fields[i]
                        val aliases: ImmutableSet<String?> = pair.right.left
                        fields.set(i, Field(aliases, field.right))
                    }
                    else -> {}
                }
            }
            stack.push(Frame(project.getInput(), ImmutableList.copyOf(fields)))
            val mergedHints: ImmutableSet.Builder<RelHint> = ImmutableSet.builder()
            mergedHints.addAll(project.getHints())
            mergedHints.addAll(hints)
            return project_(newNodes, fieldNameList, mergedHints.build(), force)
        }

        // Simplify expressions.
        if (config.simplify()) {
            val shuttle: RexShuttle = RexUtil.searchShuttle(rexBuilder, null, 2)
            for (i in 0 until nodeList.size()) {
                val node0: RexNode = nodeList[i]
                val node1: RexNode = simplifier.simplifyPreservingType(node0)
                val node2: RexNode = node1.accept(shuttle)
                nodeList.set(i, node2)
            }
        }

        // Replace null names with generated aliases.
        for (i in 0 until fieldNameList.size()) {
            if (fieldNameList[i] == null) {
                fieldNameList.set(i, inferAlias(nodeList, nodeList[i], i))
            }
        }
        val fields: ImmutableList.Builder<Field> = ImmutableList.builder()
        val uniqueNameList: Set<String?> = if (typeFactory.getTypeSystem()
                .isSchemaCaseSensitive()
        ) HashSet() else TreeSet(String.CASE_INSENSITIVE_ORDER)
        // calculate final names and build field list
        for (i in 0 until fieldNameList.size()) {
            val node: RexNode = nodeList[i]
            var name = fieldNameList[i]
            val originalName = name
            var field: Field
            if (name == null || uniqueNameList.contains(name)) {
                var j = 0
                if (name == null) {
                    j = i
                }
                do {
                    name = SqlValidatorUtil.F_SUGGESTER.apply(originalName, j, j++)
                } while (uniqueNameList.contains(name))
                fieldNameList.set(i, name)
            }
            val fieldType: RelDataTypeField = RelDataTypeFieldImpl(name, i, node.getType())
            field = when (node.getKind()) {
                INPUT_REF -> {
                    // preserve rel aliases for INPUT_REF fields
                    val index: Int = (node as RexInputRef).getIndex()
                    Field(frame.fields.get(index).left, fieldType)
                }
                else -> Field(ImmutableSet.of(), fieldType)
            }
            uniqueNameList.add(name)
            fields.add(field)
        }
        if (!force && RexUtil.isIdentity(nodeList, inputRowType)) {
            if (fieldNameList.equals(inputRowType.getFieldNames())) {
                // Do not create an identity project if it does not rename any fields
                return this
            } else {
                // create "virtual" row type for project only rename fields
                stack.pop()
                // Ignore the hints.
                stack.push(Frame(frame.rel, fields.build()))
            }
            return this
        }

        // If the expressions are all literals, and the input is a Values with N
        // rows, replace with a Values with same tuple N times.
        if (config.simplifyValues()
            && frame.rel is Values
            && nodeList.stream().allMatch { e -> e is RexLiteral }
        ) {
            val values: Values = build() as Values
            val typeBuilder: RelDataTypeFactory.Builder = typeFactory.builder()
            Pair.forEach(fieldNameList, nodeList) { name, expr ->
                typeBuilder.add(
                    requireNonNull(name, "name"),
                    expr.getType()
                )
            }
            return values(
                Collections.nCopies(values.tuples.size(), nodeList),
                typeBuilder.build()
            )
        }
        val project: RelNode = struct.projectFactory.createProject(
            frame.rel,
            ImmutableList.copyOf(hints),
            ImmutableList.copyOf(nodeList),
            fieldNameList
        )
        stack.pop()
        stack.push(Frame(project, fields.build()))
        return this
    }

    /** Creates a [Project] of the given
     * expressions and field names, and optionally optimizing.
     *
     *
     * If `fieldNames` is null, or if a particular entry in
     * `fieldNames` is null, derives field names from the input
     * expressions.
     *
     *
     * If `force` is false,
     * and the input is a `Project`,
     * and the expressions  make the trivial projection ($0, $1, ...),
     * modifies the input.
     *
     * @param nodes       Expressions
     * @param fieldNames  Suggested field names, or null to generate
     * @param force       Whether to create a renaming Project if the
     * projections are trivial
     */
    fun projectNamed(
        nodes: Iterable<RexNode?>?,
        @Nullable fieldNames: Iterable<String?>?, force: Boolean
    ): RelBuilder {
        @SuppressWarnings("unchecked") val nodeList: List<RexNode?> =
            if (nodes is List) nodes else ImmutableList.copyOf(nodes)
        val fieldNameList: List<String>? =
            if (fieldNames == null) null else if (fieldNames is List) fieldNames else ImmutableNullableList.copyOf(
                fieldNames
            )
        val input: RelNode = peek()
        val rowType: RelDataType = RexUtil.createStructType(
            cluster.getTypeFactory(), nodeList,
            fieldNameList, SqlValidatorUtil.F_SUGGESTER
        )
        if (!force
            && RexUtil.isIdentity(nodeList, input.getRowType())
        ) {
            if (input is Project && fieldNames != null) {
                // Rename columns of child projection if desired field names are given.
                val frame: Frame = stack.pop()
                val childProject: Project = frame.rel as Project
                val newInput: Project = childProject.copy(
                    childProject.getTraitSet(),
                    childProject.getInput(), childProject.getProjects(), rowType
                )
                stack.push(Frame(newInput.attachHints(childProject.getHints()), frame.fields))
            }
            if (input is Values && fieldNameList != null) {
                // Rename columns of child values if desired field names are given.
                val frame: Frame = stack.pop()
                val values: Values = frame.rel as Values
                val typeBuilder: RelDataTypeFactory.Builder = typeFactory.builder()
                Pair.forEach(
                    fieldNameList,
                    rowType.getFieldList()
                ) { name, field -> typeBuilder.add(requireNonNull(name, "name"), field.getType()) }
                val newRowType: RelDataType = typeBuilder.build()
                val newValues: RelNode = struct.valuesFactory.createValues(
                    cluster, newRowType,
                    values.tuples
                )
                stack.push(Frame(newValues, frame.fields))
            }
        } else {
            project(nodeList, rowType.getFieldNames(), force)
        }
        return this
    }

    /**
     * Creates an [Uncollect] with given item aliases.
     *
     * @param itemAliases   Operand item aliases, never null
     * @param withOrdinality If `withOrdinality`, the output contains an extra
     * `ORDINALITY` column
     */
    fun uncollect(itemAliases: List<String?>?, withOrdinality: Boolean): RelBuilder {
        val frame: Frame = stack.pop()
        stack.push(
            Frame(
                Uncollect(
                    cluster,
                    cluster.traitSetOf(Convention.NONE),
                    frame.rel,
                    withOrdinality,
                    requireNonNull(itemAliases, "itemAliases")
                )
            )
        )
        return this
    }

    /** Ensures that the field names match those given.
     *
     *
     * If all fields have the same name, adds nothing;
     * if any fields do not have the same name, adds a [Project].
     *
     *
     * Note that the names can be short-lived. Other `RelBuilder`
     * operations make no guarantees about the field names of the rows they
     * produce.
     *
     * @param fieldNames List of desired field names; may contain null values or
     * have fewer fields than the current row type
     */
    fun rename(fieldNames: List<String?>): RelBuilder {
        val oldFieldNames: List<String> = peek().getRowType().getFieldNames()
        Preconditions.checkArgument(
            fieldNames.size() <= oldFieldNames.size(),
            "More names than fields"
        )
        val newFieldNames: List<String> = ArrayList(oldFieldNames)
        for (i in 0 until fieldNames.size()) {
            val s = fieldNames[i]
            if (s != null) {
                newFieldNames.set(i, s)
            }
        }
        if (oldFieldNames.equals(newFieldNames)) {
            return this
        }
        if (peek() is Values) {
            // Special treatment for VALUES. Re-build it rather than add a project.
            val v: Values = build() as Values
            val b: RelDataTypeFactory.Builder = typeFactory.builder()
            for (p in Pair.zip(newFieldNames, v.getRowType().getFieldList())) {
                b.add(p.left, p.right.getType())
            }
            return values(v.tuples, b.build())
        }
        return project(fields(), newFieldNames, true)
    }

    /** Infers the alias of an expression.
     *
     *
     * If the expression was created by [.alias], replaces the expression
     * in the project list.
     */
    @Nullable
    private fun inferAlias(exprList: List<RexNode>, expr: RexNode, i: Int): String? {
        return when (expr.getKind()) {
            INPUT_REF -> {
                val ref: RexInputRef = expr as RexInputRef
                requireNonNull(stack.peek(), "empty frame stack").fields.get(ref.getIndex()).getValue().getName()
            }
            CAST -> inferAlias(exprList, (expr as RexCall).getOperands().get(0), -1)
            AS -> {
                val call: RexCall = expr as RexCall
                if (i >= 0) {
                    exprList.set(i, call.getOperands().get(0))
                }
                val value: NlsString = (call.getOperands().get(1) as RexLiteral).getValue() as NlsString
                castNonNull(value)
                    .getValue()
            }
            else -> null
        }
    }

    /** Creates an [Aggregate] that makes the
     * relational expression distinct on all fields.  */
    fun distinct(): RelBuilder {
        return aggregate(groupKey(fields()))
    }

    /** Creates an [Aggregate] with an array of
     * calls.  */
    fun aggregate(groupKey: GroupKey?, vararg aggCalls: AggCall?): RelBuilder {
        return aggregate(groupKey, ImmutableList.copyOf(aggCalls))
    }

    /** Creates an [Aggregate] with an array of
     * [AggregateCall]s.  */
    fun aggregate(groupKey: GroupKey?, aggregateCalls: List<AggregateCall?>): RelBuilder {
        return aggregate(groupKey,
            aggregateCalls.stream()
                .map { aggregateCall ->
                    AggCallImpl2(
                        aggregateCall,
                        aggregateCall.getArgList().stream()
                            .map(this::field)
                            .collect(Util.toImmutableList())
                    )
                }
                .collect(Collectors.toList()))
    }

    /** Creates an [Aggregate] with multiple calls.  */
    fun aggregate(groupKey: GroupKey, aggCalls: Iterable<AggCall>): RelBuilder {
        val registrar = Registrar(fields(), peek().getRowType().getFieldNames())
        val groupKey_ = groupKey as GroupKeyImpl
        var groupSet: ImmutableBitSet = ImmutableBitSet.of(registrar.registerExpressions(groupKey_.nodes))
        label@ if (Iterables.isEmpty(aggCalls)) {
            val mq: RelMetadataQuery = peek().getCluster().getMetadataQuery()
            if (groupSet.isEmpty()) {
                val minRowCount: Double = mq.getMinRowCount(peek())
                if (minRowCount == null || minRowCount < 1.0) {
                    // We can't remove "GROUP BY ()" if there's a chance the rel could be
                    // empty.
                    break@label
                }
            }
            if (registrar.extraNodes.size() === fields().size()) {
                val unique: Boolean = mq.areColumnsUnique(peek(), groupSet)
                if (unique != null && unique
                    && !config.aggregateUnique()
                    && groupKey_.isSimple
                ) {
                    // Rel is already unique.
                    return project(fields(groupSet))
                }
            }
            val maxRowCount: Double = mq.getMaxRowCount(peek())
            if (maxRowCount != null && maxRowCount <= 1.0 && !config.aggregateUnique()
                && groupKey_.isSimple
            ) {
                // If there is at most one row, rel is already unique.
                return project(fields(groupSet))
            }
        }
        var groupSets: ImmutableList<ImmutableBitSet>
        if (groupKey_.nodeLists != null) {
            val sizeBefore: Int = registrar.extraNodes.size()
            val groupSetList: List<ImmutableBitSet> = ArrayList()
            for (nodeList in groupKey_.nodeLists) {
                val groupSet2: ImmutableBitSet = ImmutableBitSet.of(registrar.registerExpressions(nodeList))
                if (!groupSet.contains(groupSet2)) {
                    throw IllegalArgumentException(
                        "group set element " + nodeList
                                + " must be a subset of group key"
                    )
                }
                groupSetList.add(groupSet2)
            }
            val groupSetMultiset: ImmutableSortedMultiset<ImmutableBitSet> = ImmutableSortedMultiset.copyOf(
                ImmutableBitSet.COMPARATOR,
                groupSetList
            )
            if (Iterables.any(aggCalls) { c: AggCall -> isGroupId(c) }
                || !ImmutableBitSet.ORDERING.isStrictlyOrdered(groupSetMultiset)) {
                return rewriteAggregateWithDuplicateGroupSets(
                    groupSet, groupSetMultiset,
                    ImmutableList.copyOf(aggCalls)
                )
            }
            groupSets = ImmutableList.copyOf(groupSetMultiset.elementSet())
            if (registrar.extraNodes.size() > sizeBefore) {
                throw IllegalArgumentException(
                    "group sets contained expressions "
                            + "not in group key: "
                            + Util.skip(registrar.extraNodes, sizeBefore)
                )
            }
        } else {
            groupSets = ImmutableList.of(groupSet)
        }
        for (aggCall in aggCalls) {
            (aggCall as AggCallPlus).register(registrar)
        }
        project(registrar.extraNodes)
        rename(registrar.names)
        val frame: Frame = stack.pop()
        var r: RelNode = frame.rel
        val aggregateCalls: List<AggregateCall> = ArrayList()
        for (aggCall in aggCalls) {
            aggregateCalls.add(
                (aggCall as AggCallPlus).aggregateCall(registrar, groupSet, r)
            )
        }
        assert(ImmutableBitSet.ORDERING.isStrictlyOrdered(groupSets)) { groupSets }
        for (set in groupSets) {
            assert(groupSet.contains(set))
        }
        var inFields: List<Field> = frame.fields
        if (config.pruneInputOfAggregate()
            && r is Project
        ) {
            val fieldsUsed: Set<Integer> = RelOptUtil.getAllFields2(groupSet, aggregateCalls)
            // Some parts of the system can't handle rows with zero fields, so
            // pretend that one field is used.
            if (fieldsUsed.isEmpty()) {
                r = (r as Project).getInput()
            } else if (fieldsUsed.size() < r.getRowType().getFieldCount()) {
                // Some fields are computed but not used. Prune them.
                val map: Map<Integer, Integer> = HashMap()
                for (source in fieldsUsed) {
                    map.put(source, map.size())
                }
                groupSet = groupSet.permute(map)
                groupSets = ImmutableBitSet.ORDERING.immutableSortedCopy(
                    ImmutableBitSet.permute(groupSets, map)
                )
                val targetMapping: Mappings.TargetMapping = Mappings.target(
                    map, r.getRowType().getFieldCount(),
                    fieldsUsed.size()
                )
                val oldAggregateCalls: List<AggregateCall> = ArrayList(aggregateCalls)
                aggregateCalls.clear()
                for (aggregateCall in oldAggregateCalls) {
                    aggregateCalls.add(aggregateCall.transform(targetMapping))
                }
                inFields = Mappings.permute(inFields, targetMapping.inverse())
                val project: Project = r as Project
                val newProjects: List<RexNode> = ArrayList()
                val builder: RelDataTypeFactory.Builder = cluster.getTypeFactory().builder()
                for (i in fieldsUsed) {
                    newProjects.add(project.getProjects().get(i))
                    builder.add(project.getRowType().getFieldList().get(i))
                }
                r = project.copy(
                    cluster.traitSet(), project.getInput(), newProjects,
                    builder.build()
                )
            }
        }
        if (!config.dedupAggregateCalls() || Util.isDistinct(aggregateCalls)) {
            return aggregate_(
                groupSet, groupSets, r, aggregateCalls,
                registrar.extraNodes, inFields
            )
        }

        // There are duplicate aggregate calls. Rebuild the list to eliminate
        // duplicates, then add a Project.
        val callSet: Set<AggregateCall> = HashSet()
        val projects: List<Pair<Integer, String>> = ArrayList()
        Util.range(groupSet.cardinality())
            .forEach { i -> projects.add(Pair.of(i, null)) }
        val distinctAggregateCalls: List<AggregateCall> = ArrayList()
        for (aggregateCall in aggregateCalls) {
            val i: Int
            if (callSet.add(aggregateCall)) {
                i = distinctAggregateCalls.size()
                distinctAggregateCalls.add(aggregateCall)
            } else {
                i = distinctAggregateCalls.indexOf(aggregateCall)
                assert(i >= 0)
            }
            projects.add(Pair.of(groupSet.cardinality() + i, aggregateCall.name))
        }
        aggregate_(
            groupSet, groupSets, r, distinctAggregateCalls,
            registrar.extraNodes, inFields
        )
        val fields: List<RexNode> = projects.stream()
            .map { p -> if (p.right == null) field(p.left) else alias(field(p.left), p.right) }
            .collect(Collectors.toList())
        return project(fields)
    }

    /** Finishes the implementation of [.aggregate] by creating an
     * [Aggregate] and pushing it onto the stack.  */
    private fun aggregate_(
        groupSet: ImmutableBitSet,
        groupSets: ImmutableList<ImmutableBitSet>, input: RelNode,
        aggregateCalls: List<AggregateCall>, extraNodes: List<RexNode?>,
        inFields: List<Field>
    ): RelBuilder {
        val aggregate: RelNode = struct.aggregateFactory.createAggregate(
            input,
            ImmutableList.of(), groupSet, groupSets, aggregateCalls
        )

        // build field list
        val fields: ImmutableList.Builder<Field> = ImmutableList.builder()
        val aggregateFields: List<RelDataTypeField> = aggregate.getRowType().getFieldList()
        var i = 0
        // first, group fields
        for (groupField in groupSet.asList()) {
            val node: RexNode? = extraNodes[groupField]
            val kind: SqlKind = node.getKind()
            when (kind) {
                INPUT_REF -> fields.add(inFields[(node as RexInputRef?).getIndex()])
                else -> {
                    val name: String = aggregateFields[i].getName()
                    val fieldType: RelDataTypeField = RelDataTypeFieldImpl(name, i, node.getType())
                    fields.add(Field(ImmutableSet.of(), fieldType))
                }
            }
            i++
        }
        // second, aggregate fields. retain `i' as field index
        for (j in 0 until aggregateCalls.size()) {
            val call: AggregateCall = aggregateCalls[j]
            val fieldType: RelDataTypeField = RelDataTypeFieldImpl(
                aggregateFields[i + j].getName(), i + j,
                call.getType()
            )
            fields.add(Field(ImmutableSet.of(), fieldType))
        }
        stack.push(Frame(aggregate, fields.build()))
        return this
    }

    /**
     * The `GROUP_ID()` function is used to distinguish duplicate groups.
     * However, as Aggregate normalizes group sets to canonical form (i.e.,
     * flatten, sorting, redundancy removal), this information is lost in RelNode.
     * Therefore, it is impossible to implement the function in runtime.
     *
     *
     * To fill this gap, an aggregation query that contains duplicate group
     * sets is rewritten into a Union of Aggregate operators whose group sets are
     * distinct. The number of inputs to the Union is equal to the maximum number
     * of duplicates. In the `N`th input to the Union, calls to the
     * `GROUP_ID` aggregate function are replaced by the integer literal
     * `N`.
     *
     *
     * This method also handles the case where group sets are distinct but
     * there is a call to `GROUP_ID`. That call is replaced by the integer
     * literal `0`.
     *
     *
     * Also see the discussion in
     * [[CALCITE-1824]
 * GROUP_ID returns wrong result](https://issues.apache.org/jira/browse/CALCITE-1824) and
     * [[CALCITE-4748]
 * If there are duplicate GROUPING SETS, Calcite should return duplicate
 * rows](https://issues.apache.org/jira/browse/CALCITE-4748).
     */
    private fun rewriteAggregateWithDuplicateGroupSets(
        groupSet: ImmutableBitSet,
        groupSets: ImmutableSortedMultiset<ImmutableBitSet>,
        aggregateCalls: List<AggCall>
    ): RelBuilder {
        val fieldNamesIfNoRewrite: List<String> = Aggregate.deriveRowType(
            typeFactory, peek().getRowType(), false,
            groupSet, groupSets.asList(),
            aggregateCalls.stream().map { c -> (c as AggCallPlus).aggregateCall() }
                .collect(Util.toImmutableList())).getFieldNames()

        // If n duplicates exist for a particular grouping, the {@code GROUP_ID()}
        // function produces values in the range 0 to n-1. For each value,
        // we need to figure out the corresponding group sets.
        //
        // For example, "... GROUPING SETS (a, a, b, c, c, c, c)"
        // (i) The max value of the GROUP_ID() function returns is 3
        // (ii) GROUPING SETS (a, b, c) produces value 0,
        //      GROUPING SETS (a, c) produces value 1,
        //      GROUPING SETS (c) produces value 2
        //      GROUPING SETS (c) produces value 3
        val groupIdToGroupSets: Map<Integer, Set<ImmutableBitSet>> = HashMap()
        var maxGroupId = 0
        for (entry in groupSets.entrySet()) {
            val groupId: Int = entry.getCount() - 1
            if (groupId > maxGroupId) {
                maxGroupId = groupId
            }
            for (i in 0..groupId) {
                groupIdToGroupSets.computeIfAbsent(
                    i
                ) { k -> Sets.newTreeSet(ImmutableBitSet.COMPARATOR) }
                    .add(entry.getElement())
            }
        }

        // AggregateCall list without GROUP_ID function
        val aggregateCallsWithoutGroupId: List<AggCall> = ArrayList(aggregateCalls)
        aggregateCallsWithoutGroupId.removeIf { c: AggCall -> isGroupId(c) }

        // For each group id value, we first construct an Aggregate without
        // GROUP_ID() function call, and then create a Project node on top of it.
        // The Project adds literal value for group id in right position.
        val frame: Frame = stack.pop()
        for (groupId in 0..maxGroupId) {
            // Create the Aggregate node without GROUP_ID() call
            stack.push(frame)
            aggregate(
                groupKey(groupSet, castNonNull(groupIdToGroupSets[groupId])),
                aggregateCallsWithoutGroupId
            )
            val selectList: List<RexNode> = ArrayList()
            val groupExprLength: Int = groupSet.cardinality()
            // Project fields in group by expressions
            for (i in 0 until groupExprLength) {
                selectList.add(field(i))
            }
            // Project fields in aggregate calls
            var groupIdCount = 0
            for (i in 0 until aggregateCalls.size()) {
                if (isGroupId(aggregateCalls[i])) {
                    selectList.add(
                        rexBuilder.makeExactLiteral(
                            BigDecimal.valueOf(groupId),
                            typeFactory.createSqlType(SqlTypeName.BIGINT)
                        )
                    )
                    groupIdCount++
                } else {
                    selectList.add(field(groupExprLength + i - groupIdCount))
                }
            }
            project(selectList, fieldNamesIfNoRewrite)
        }
        return union(true, maxGroupId + 1)
    }

    private fun setOp(all: Boolean, kind: SqlKind, n: Int): RelBuilder {
        val inputs: List<RelNode> = ArrayList()
        for (i in 0 until n) {
            inputs.add(0, build())
        }
        when (kind) {
            UNION, INTERSECT, EXCEPT -> if (n < 1) {
                throw IllegalArgumentException(
                    "bad INTERSECT/UNION/EXCEPT input count"
                )
            }
            else -> throw AssertionError("bad setOp $kind")
        }
        if (n == 1) {
            return push(inputs[0])
        }
        if (config.simplifyValues()
            && kind === UNION && inputs.stream().allMatch { r -> r is Values }
        ) {
            val inputTypes: List<RelDataType> = Util.transform(inputs, RelNode::getRowType)
            val rowType: RelDataType = typeFactory
                .leastRestrictive(inputTypes)
            requireNonNull(rowType) { "leastRestrictive($inputTypes)" }
            val tuples: List<List<RexLiteral>> = ArrayList()
            for (input in inputs) {
                tuples.addAll((input as Values).tuples)
            }
            val tuples2: List<List<RexLiteral>> = if (all) tuples else Util.distinctList(tuples)
            return values(tuples2, rowType)
        }
        return push(struct.setOpFactory.createSetOp(kind, inputs, all))
    }
    /** Creates a [Union] of the `n`
     * most recent relational expressions on the stack.
     *
     * @param all Whether to create UNION ALL
     * @param n Number of inputs to the UNION operator
     */
    /** Creates a [Union] of the two most recent
     * relational expressions on the stack.
     *
     * @param all Whether to create UNION ALL
     */
    @JvmOverloads
    fun union(all: Boolean, n: Int = 2): RelBuilder {
        return setOp(all, UNION, n)
    }
    /** Creates an [Intersect] of the `n`
     * most recent relational expressions on the stack.
     *
     * @param all Whether to create INTERSECT ALL
     * @param n Number of inputs to the INTERSECT operator
     */
    /** Creates an [Intersect] of the two most
     * recent relational expressions on the stack.
     *
     * @param all Whether to create INTERSECT ALL
     */
    @JvmOverloads
    fun intersect(all: Boolean, n: Int = 2): RelBuilder {
        return setOp(all, SqlKind.INTERSECT, n)
    }
    /** Creates a [Minus] of the `n`
     * most recent relational expressions on the stack.
     *
     * @param all Whether to create EXCEPT ALL
     */
    /** Creates a [Minus] of the two most recent
     * relational expressions on the stack.
     *
     * @param all Whether to create EXCEPT ALL
     */
    @JvmOverloads
    fun minus(all: Boolean, n: Int = 2): RelBuilder {
        return setOp(all, SqlKind.EXCEPT, n)
    }

    /**
     * Creates a [TableScan] on a [TransientTable] with the given name, using as type
     * the top of the stack's type.
     *
     * @param tableName table name
     */
    @Experimental
    fun transientScan(tableName: String?): RelBuilder {
        return this.transientScan(tableName, this.peek().getRowType())
    }

    /**
     * Creates a [TableScan] on a [TransientTable] with the given name and type.
     *
     * @param tableName table name
     * @param rowType row type of the table
     */
    @Experimental
    fun transientScan(tableName: String?, rowType: RelDataType): RelBuilder {
        val transientTable: TransientTable = ListTransientTable(tableName, rowType)
        requireNonNull(relOptSchema, "relOptSchema")
        val relOptTable: RelOptTable = RelOptTableImpl.create(
            relOptSchema,
            rowType,
            transientTable,
            ImmutableList.of(tableName)
        )
        val scan: RelNode = struct.scanFactory.createScan(
            ViewExpanders.toRelContext(viewExpander, cluster),
            relOptTable
        )
        push(scan)
        rename(rowType.getFieldNames())
        return this
    }

    /**
     * Creates a [TableSpool] for the most recent relational expression.
     *
     * @param readType Spool's read type (as described in [Spool.Type])
     * @param writeType Spool's write type (as described in [Spool.Type])
     * @param table Table to write into
     */
    private fun tableSpool(
        readType: Spool.Type, writeType: Spool.Type,
        table: RelOptTable?
    ): RelBuilder {
        val spool: RelNode = struct.spoolFactory.createTableSpool(
            peek(), readType, writeType,
            table
        )
        replaceTop(spool)
        return this
    }

    /**
     * Creates a [RepeatUnion] associated to a [TransientTable] without a maximum number
     * of iterations, i.e. repeatUnion(tableName, all, -1).
     *
     * @param tableName name of the [TransientTable] associated to the [RepeatUnion]
     * @param all whether duplicates will be considered or not
     */
    @Experimental
    fun repeatUnion(tableName: String?, all: Boolean): RelBuilder {
        return repeatUnion(tableName, all, -1)
    }

    /**
     * Creates a [RepeatUnion] associated to a [TransientTable] of the
     * two most recent relational expressions on the stack.
     *
     *
     * Warning: if these relational expressions are not
     * correctly defined, this operation might lead to an infinite loop.
     *
     *
     * The generated [RepeatUnion] operates as follows:
     *
     *
     *  * Evaluate its left term once, propagating the results into the
     * [TransientTable];
     *  * Evaluate its right term (which may contain a [TableScan] on the
     * [TransientTable]) over and over until it produces no more results
     * (or until an optional maximum number of iterations is reached). On each
     * iteration, the results are propagated into the [TransientTable],
     * overwriting the results from the previous one.
     *
     *
     * @param tableName Name of the [TransientTable] associated to the
     * [RepeatUnion]
     * @param all Whether duplicates are considered
     * @param iterationLimit Maximum number of iterations; negative value means no limit
     */
    @Experimental
    fun repeatUnion(tableName: String?, all: Boolean, iterationLimit: Int): RelBuilder {
        val finder = RelOptTableFinder(tableName)
        for (i in 0 until stack.size()) { // search scan(tableName) in the stack
            peek(i).accept(finder)
            if (finder.relOptTable != null) { // found
                break
            }
        }
        if (finder.relOptTable == null) {
            throw RESOURCE.tableNotFound(tableName).ex()
        }
        val iterative: RelNode = tableSpool(Spool.Type.LAZY, Spool.Type.LAZY, finder.relOptTable).build()
        val seed: RelNode = tableSpool(Spool.Type.LAZY, Spool.Type.LAZY, finder.relOptTable).build()
        val repeatUnion: RelNode = struct.repeatUnionFactory.createRepeatUnion(
            seed, iterative, all,
            iterationLimit
        )
        return push(repeatUnion)
    }

    /**
     * Auxiliary class to find a certain RelOptTable based on its name.
     */
    private class RelOptTableFinder(private val tableName: String?) : RelHomogeneousShuttle() {
        @MonotonicNonNull
        var relOptTable: RelOptTable? = null
        @Override
        fun visit(scan: TableScan): RelNode {
            val scanTable: RelOptTable = scan.getTable()
            val qualifiedName: List<String> = scanTable.getQualifiedName()
            if (qualifiedName[qualifiedName.size() - 1].equals(tableName)) {
                relOptTable = scanTable
            }
            return super.visit(scan)
        }
    }

    /** Creates a [Join] with an array of conditions.  */
    fun join(
        joinType: JoinRelType?, condition0: RexNode?,
        vararg conditions: RexNode?
    ): RelBuilder {
        return join(joinType, Lists.asList(condition0, conditions))
    }

    /** Creates a [Join] with multiple
     * conditions.  */
    fun join(
        joinType: JoinRelType?,
        conditions: Iterable<RexNode?>?
    ): RelBuilder {
        return join(
            joinType, and(conditions),
            ImmutableSet.of()
        )
    }

    /** Creates a [Join] with one condition.  */
    fun join(joinType: JoinRelType?, condition: RexNode?): RelBuilder {
        return join(joinType, condition, ImmutableSet.of())
    }

    /** Creates a [Join] with correlating variables.  */
    fun join(
        joinType: JoinRelType, condition: RexNode,
        variablesSet: Set<CorrelationId>
    ): RelBuilder {
        var condition: RexNode = condition
        var right: Frame = stack.pop()
        val left: Frame = stack.pop()
        val join: RelNode
        val correlate = checkIfCorrelated(variablesSet, joinType, left.rel, right.rel)
        var postCondition: RexNode = literal(true)
        if (config.simplify()) {
            // Normalize expanded versions IS NOT DISTINCT FROM so that simplifier does not
            // transform the expression to something unrecognizable
            if (condition is RexCall) {
                condition = RelOptUtil.collapseExpandedIsNotDistinctFromExpr(
                    condition as RexCall,
                    rexBuilder
                )
            }
            condition = simplifier.simplifyUnknownAsFalse(condition)
        }
        if (correlate) {
            val id: CorrelationId = Iterables.getOnlyElement(variablesSet)
            when (joinType) {
                LEFT, SEMI, ANTI -> {
                    // For a LEFT/SEMI/ANTI, predicate must be evaluated first.
                    stack.push(right)
                    filter(condition.accept(Shifter(left.rel, id, right.rel)))
                    right = stack.pop()
                }
                INNER ->         // For INNER, we can defer.
                    postCondition = condition
                else -> throw IllegalArgumentException("Correlated $joinType join is not supported")
            }
            val requiredColumns: ImmutableBitSet = RelOptUtil.correlationColumns(id, right.rel)
            join = struct.correlateFactory.createCorrelate(
                left.rel, right.rel, id,
                requiredColumns, joinType
            )
        } else {
            val join0: RelNode = struct.joinFactory.createJoin(
                left.rel, right.rel,
                ImmutableList.of(), condition, variablesSet, joinType, false
            )
            join = if (join0 is Join && config.pushJoinCondition()) {
                RelOptUtil.pushDownJoinConditions(join0 as Join, this)
            } else {
                join0
            }
        }
        val fields: ImmutableList.Builder<Field> = ImmutableList.builder()
        fields.addAll(left.fields)
        fields.addAll(right.fields)
        stack.push(Frame(join, fields.build()))
        filter(postCondition)
        return this
    }

    /** Creates a [Correlate]
     * with a [CorrelationId] and an array of fields that are used by correlation.  */
    fun correlate(
        joinType: JoinRelType?,
        correlationId: CorrelationId?, vararg requiredFields: RexNode?
    ): RelBuilder {
        return correlate(joinType, correlationId, ImmutableList.copyOf(requiredFields))
    }

    /** Creates a [Correlate]
     * with a [CorrelationId] and a list of fields that are used by correlation.  */
    fun correlate(
        joinType: JoinRelType?,
        correlationId: CorrelationId?, requiredFields: Iterable<RexNode?>?
    ): RelBuilder {
        val right: Frame = stack.pop()
        val registrar = Registrar(fields(), peek().getRowType().getFieldNames())
        val requiredOrdinals: List<Integer> = registrar.registerExpressions(ImmutableList.copyOf(requiredFields))
        project(registrar.extraNodes)
        rename(registrar.names)
        val left: Frame = stack.pop()
        val correlate: RelNode = struct.correlateFactory.createCorrelate(
            left.rel, right.rel,
            correlationId, ImmutableBitSet.of(requiredOrdinals), joinType
        )
        val fields: ImmutableList.Builder<Field> = ImmutableList.builder()
        fields.addAll(left.fields)
        fields.addAll(right.fields)
        stack.push(Frame(correlate, fields.build()))
        return this
    }

    /** Creates a [Join] using USING syntax.
     *
     *
     * For each of the field names, both left and right inputs must have a
     * field of that name. Constructs a join condition that the left and right
     * fields are equal.
     *
     * @param joinType Join type
     * @param fieldNames Field names
     */
    fun join(joinType: JoinRelType?, vararg fieldNames: String): RelBuilder {
        val conditions: List<RexNode> = ArrayList()
        for (fieldName in fieldNames) {
            conditions.add(
                equals(
                    field(2, 0, fieldName),
                    field(2, 1, fieldName)
                )
            )
        }
        return join(joinType, conditions)
    }

    /** Creates a [Join] with [JoinRelType.SEMI].
     *
     *
     * A semi-join is a form of join that combines two relational expressions
     * according to some condition, and outputs only rows from the left input for
     * which at least one row from the right input matches. It only outputs
     * columns from the left input, and ignores duplicates on the right.
     *
     *
     * For example, `EMP semi-join DEPT` finds all `EMP` records
     * that do not have a corresponding `DEPT` record, similar to the
     * following SQL:
     *
     * <blockquote><pre>
     * SELECT * FROM EMP
     * WHERE EXISTS (SELECT 1 FROM DEPT
     * WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
    </blockquote> *
     */
    fun semiJoin(conditions: Iterable<RexNode?>?): RelBuilder {
        val right: Frame = stack.pop()
        val semiJoin: RelNode = struct.joinFactory.createJoin(
            peek(),
            right.rel,
            ImmutableList.of(),
            and(conditions),
            ImmutableSet.of(),
            JoinRelType.SEMI,
            false
        )
        replaceTop(semiJoin)
        return this
    }

    /** Creates a [Join] with [JoinRelType.SEMI].
     *
     * @see .semiJoin
     */
    fun semiJoin(vararg conditions: RexNode?): RelBuilder {
        return semiJoin(ImmutableList.copyOf(conditions))
    }

    /** Creates an anti-join.
     *
     *
     * An anti-join is a form of join that combines two relational expressions
     * according to some condition, but outputs only rows from the left input
     * for which no rows from the right input match.
     *
     *
     * For example, `EMP anti-join DEPT` finds all `EMP` records
     * that do not have a corresponding `DEPT` record, similar to the
     * following SQL:
     *
     * <blockquote><pre>
     * SELECT * FROM EMP
     * WHERE NOT EXISTS (SELECT 1 FROM DEPT
     * WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
    </blockquote> *
     */
    fun antiJoin(conditions: Iterable<RexNode?>?): RelBuilder {
        val right: Frame = stack.pop()
        val antiJoin: RelNode = struct.joinFactory.createJoin(
            peek(),
            right.rel,
            ImmutableList.of(),
            and(conditions),
            ImmutableSet.of(),
            JoinRelType.ANTI,
            false
        )
        replaceTop(antiJoin)
        return this
    }

    /** Creates an anti-join.
     *
     * @see .antiJoin
     */
    fun antiJoin(vararg conditions: RexNode?): RelBuilder {
        return antiJoin(ImmutableList.copyOf(conditions))
    }

    /** Assigns a table alias to the top entry on the stack.  */
    fun `as`(alias: String?): RelBuilder {
        val pair: Frame = stack.pop()
        val newFields: List<Field> = Util.transform(pair.fields) { field -> field.addAlias(alias) }
        stack.push(Frame(pair.rel, ImmutableList.copyOf(newFields)))
        return this
    }

    /** Creates a [Values].
     *
     *
     * The `values` array must have the same number of entries as
     * `fieldNames`, or an integer multiple if you wish to create multiple
     * rows.
     *
     *
     * If there are zero rows, or if all values of a any column are
     * null, this method cannot deduce the type of columns. For these cases,
     * call [.values].
     *
     * @param fieldNames Field names
     * @param values Values
     */
    fun values(@Nullable fieldNames: Array<String?>?, @Nullable vararg values: Object?): RelBuilder {
        if (fieldNames == null || fieldNames.size == 0 || values.size % fieldNames.size != 0 || values.size < fieldNames.size) {
            throw IllegalArgumentException(
                "Value count must be a positive multiple of field count"
            )
        }
        val rowCount = values.size / fieldNames.size
        for (fieldName in Ord.zip(fieldNames)) {
            if (allNull(values, fieldName.i, fieldNames.size)) {
                throw IllegalArgumentException(
                    "All values of field '" + fieldName.e
                        .toString() + "' (field index " + fieldName.i.toString() + ")" + " are null; cannot deduce type"
                )
            }
        }
        val tupleList: ImmutableList<ImmutableList<RexLiteral>> = tupleList(fieldNames.size, values)
        assert(tupleList.size() === rowCount)
        val fieldNameList: List<String> = Util.transformIndexed(Arrays.asList(fieldNames)) { name, i ->
            if (name != null) name else SqlUtil.deriveAliasFromOrdinal(i)
        }
        return values(tupleList, fieldNameList)
    }

    private fun values(
        tupleList: List<List<RexLiteral?>?>,
        fieldNames: List<String>
    ): RelBuilder {
        val typeFactory: RelDataTypeFactory = cluster.getTypeFactory()
        val builder: RelDataTypeFactory.Builder = typeFactory.builder()
        Ord.forEach(fieldNames) { fieldName, i ->
            val type: RelDataType = typeFactory.leastRestrictive(
                object : AbstractList<RelDataType?>() {
                    @Override
                    operator fun get(index: Int): RelDataType {
                        return@forEach tupleList[index]!![i].getType()
                    }

                    @Override
                    fun size(): Int {
                        return@forEach tupleList.size()
                    }
                })
            assert(type != null) { "can't infer type for field $i, $fieldName" }
            builder.add(fieldName, type)
        }
        val rowType: RelDataType = builder.build()
        return values(tupleList, rowType)
    }

    private fun tupleList(
        columnCount: Int,
        @Nullable values: Array<Object?>
    ): ImmutableList<ImmutableList<RexLiteral>> {
        val listBuilder: ImmutableList.Builder<ImmutableList<RexLiteral>> = ImmutableList.builder()
        val valueList: List<RexLiteral> = ArrayList()
        for (i in values.indices) {
            val value: Object? = values[i]
            valueList.add(literal(value))
            if ((i + 1) % columnCount == 0) {
                listBuilder.add(ImmutableList.copyOf(valueList))
                valueList.clear()
            }
        }
        return listBuilder.build()
    }

    /** Creates a relational expression that reads from an input and throws
     * all of the rows away.
     *
     *
     * Note that this method always pops one relational expression from the
     * stack. `values`, in contrast, does not pop any relational
     * expressions, and always produces a leaf.
     *
     *
     * The default implementation creates a [Values] with the same
     * specified row type and aliases as the input, and ignores the input entirely.
     * But schema-on-query systems such as Drill might override this method to
     * create a relation expression that retains the input, just to read its
     * schema.
     */
    fun empty(): RelBuilder {
        val frame: Frame = stack.pop()
        val values: RelNode = struct.valuesFactory.createValues(
            cluster, frame.rel.getRowType(),
            ImmutableList.of()
        )
        stack.push(Frame(values, frame.fields))
        return this
    }

    /** Creates a [Values] with a specified row type.
     *
     *
     * This method can handle cases that [.values]
     * cannot, such as all values of a column being null, or there being zero
     * rows.
     *
     * @param rowType Row type
     * @param columnValues Values
     */
    fun values(rowType: RelDataType, vararg columnValues: Object?): RelBuilder {
        val tupleList: ImmutableList<ImmutableList<RexLiteral>> = tupleList(rowType.getFieldCount(), columnValues)
        val values: RelNode = struct.valuesFactory.createValues(
            cluster, rowType,
            ImmutableList.copyOf(tupleList)
        )
        push(values)
        return this
    }

    /** Creates a [Values] with a specified row type.
     *
     *
     * This method can handle cases that [.values]
     * cannot, such as all values of a column being null, or there being zero
     * rows.
     *
     * @param tupleList Tuple list
     * @param rowType Row type
     */
    fun values(
        tupleList: Iterable<List<RexLiteral?>?>,
        rowType: RelDataType?
    ): RelBuilder {
        val values: RelNode = struct.valuesFactory.createValues(
            cluster, rowType,
            copy<Any?>(tupleList)
        )
        push(values)
        return this
    }

    /** Creates a [Values] with a specified row type and
     * zero rows.
     *
     * @param rowType Row type
     */
    fun values(rowType: RelDataType?): RelBuilder {
        return values(ImmutableList.< ImmutableList < RexLiteral > > of<ImmutableList<RexLiteral>>(), rowType)
    }

    /** Creates a limit without a sort.  */
    fun limit(offset: Int, fetch: Int): RelBuilder {
        return sortLimit(offset, fetch, ImmutableList.of())
    }

    /** Creates an Exchange by distribution.  */
    fun exchange(distribution: RelDistribution?): RelBuilder {
        val exchange: RelNode = struct.exchangeFactory.createExchange(peek(), distribution)
        replaceTop(exchange)
        return this
    }

    /** Creates a SortExchange by distribution and collation.  */
    fun sortExchange(
        distribution: RelDistribution?,
        collation: RelCollation?
    ): RelBuilder {
        val exchange: RelNode = struct.sortExchangeFactory.createSortExchange(
            peek(), distribution,
            collation
        )
        replaceTop(exchange)
        return this
    }

    /** Creates a [Sort] by field ordinals.
     *
     *
     * Negative fields mean descending: -1 means field(0) descending,
     * -2 means field(1) descending, etc.
     */
    fun sort(vararg fields: Int): RelBuilder {
        val builder: ImmutableList.Builder<RexNode> = ImmutableList.builder()
        for (field in fields) {
            builder.add(if (field < 0) desc(field(-field - 1)) else field(field))
        }
        return sortLimit(-1, -1, builder.build())
    }

    /** Creates a [Sort] by expressions.  */
    fun sort(vararg nodes: RexNode?): RelBuilder {
        return sortLimit(-1, -1, ImmutableList.copyOf(nodes))
    }

    /** Creates a [Sort] by expressions.  */
    fun sort(nodes: Iterable<RexNode?>): RelBuilder {
        return sortLimit(-1, -1, nodes)
    }

    /** Creates a [Sort] by expressions, with limit and offset.  */
    fun sortLimit(offset: Int, fetch: Int, vararg nodes: RexNode?): RelBuilder {
        return sortLimit(offset, fetch, ImmutableList.copyOf(nodes))
    }

    /** Creates a [Sort] by specifying collations.
     */
    fun sort(collation: RelCollation?): RelBuilder {
        val sort: RelNode = struct.sortFactory.createSort(peek(), collation, null, null)
        replaceTop(sort)
        return this
    }

    /** Creates a [Sort] by a list of expressions, with limit and offset.
     *
     * @param offset Number of rows to skip; non-positive means don't skip any
     * @param fetch Maximum number of rows to fetch; negative means no limit
     * @param nodes Sort expressions
     */
    fun sortLimit(
        offset: Int, fetch: Int,
        nodes: Iterable<RexNode?>
    ): RelBuilder {
        val registrar = Registrar(fields(), ImmutableList.of())
        val fieldCollations: List<RelFieldCollation> = registrar.registerFieldCollations(nodes)
        val offsetNode: RexNode? = if (offset <= 0) null else literal(offset)
        val fetchNode: RexNode? = if (fetch < 0) null else literal(fetch)
        if (offsetNode == null && fetch == 0 && config.simplifyLimit()) {
            return empty()
        }
        if (offsetNode == null && fetchNode == null && fieldCollations.isEmpty()) {
            return this // sort is trivial
        }
        if (fieldCollations.isEmpty()) {
            assert(registrar.addedFieldCount() == 0)
            val top: RelNode = peek()
            if (top is Sort) {
                val sort2: Sort = top as Sort
                if (sort2.offset == null && sort2.fetch == null) {
                    replaceTop(sort2.getInput())
                    val sort: RelNode = struct.sortFactory.createSort(
                        peek(), sort2.collation,
                        offsetNode, fetchNode
                    )
                    replaceTop(sort)
                    return this
                }
            }
            if (top is Project) {
                val project: Project = top as Project
                if (project.getInput() is Sort) {
                    val sort2: Sort = project.getInput() as Sort
                    if (sort2.offset == null && sort2.fetch == null) {
                        val sort: RelNode = struct.sortFactory.createSort(
                            sort2.getInput(),
                            sort2.collation, offsetNode, fetchNode
                        )
                        replaceTop(
                            struct.projectFactory.createProject(
                                sort,
                                project.getHints(),
                                project.getProjects(),
                                Pair.right(project.getNamedProjects())
                            )
                        )
                        return this
                    }
                }
            }
        }
        if (registrar.addedFieldCount() > 0) {
            project(registrar.extraNodes)
        }
        val sort: RelNode = struct.sortFactory.createSort(
            peek(),
            RelCollations.of(fieldCollations), offsetNode, fetchNode
        )
        replaceTop(sort)
        if (registrar.addedFieldCount() > 0) {
            project(registrar.originalExtraNodes)
        }
        return this
    }

    /**
     * Creates a projection that converts the current relational expression's
     * output to a desired row type.
     *
     *
     * The desired row type and the row type to be converted must have the
     * same number of fields.
     *
     * @param castRowType row type after cast
     * @param rename      if true, use field names from castRowType; if false,
     * preserve field names from rel
     */
    fun convert(castRowType: RelDataType?, rename: Boolean): RelBuilder {
        val r: RelNode = build()
        val r2: RelNode = RelOptUtil.createCastRel(
            r, castRowType, rename,
            struct.projectFactory
        )
        push(r2)
        return this
    }

    fun permute(mapping: Mapping): RelBuilder {
        assert(mapping.getMappingType().isSingleSource())
        assert(mapping.getMappingType().isMandatorySource())
        if (mapping.isIdentity()) {
            return this
        }
        val exprList: List<RexNode> = ArrayList()
        for (i in 0 until mapping.getTargetCount()) {
            exprList.add(field(mapping.getSource(i)))
        }
        return project(exprList)
    }

    /** Creates a [Match].  */
    fun match(
        pattern: RexNode?, strictStart: Boolean,
        strictEnd: Boolean, patternDefinitions: Map<String?, RexNode?>?,
        measureList: Iterable<RexNode?>, after: RexNode?,
        subsets: Map<String?, SortedSet<String?>?>?, allRows: Boolean,
        partitionKeys: Iterable<RexNode?>,
        orderKeys: Iterable<RexNode?>, interval: RexNode?
    ): RelBuilder {
        val registrar = Registrar(fields(), peek().getRowType().getFieldNames())
        val fieldCollations: List<RelFieldCollation> = registrar.registerFieldCollations(orderKeys)
        val partitionBitSet: ImmutableBitSet = ImmutableBitSet.of(registrar.registerExpressions(partitionKeys))
        val typeBuilder: RelDataTypeFactory.Builder = cluster.getTypeFactory().builder()
        for (partitionKey in partitionKeys) {
            typeBuilder.add(partitionKey.toString(), partitionKey.getType())
        }
        if (allRows) {
            for (orderKey in orderKeys) {
                if (!typeBuilder.nameExists(orderKey.toString())) {
                    typeBuilder.add(orderKey.toString(), orderKey.getType())
                }
            }
            val inputRowType: RelDataType = peek().getRowType()
            for (fs in inputRowType.getFieldList()) {
                if (!typeBuilder.nameExists(fs.getName())) {
                    typeBuilder.add(fs)
                }
            }
        }
        val measures: ImmutableMap.Builder<String, RexNode> = ImmutableMap.builder()
        for (measure in measureList) {
            val operands: List<RexNode> = (measure as RexCall).getOperands()
            val alias: String = operands[1].toString()
            typeBuilder.add(alias, operands[0].getType())
            measures.put(alias, operands[0])
        }
        val match: RelNode = struct.matchFactory.createMatch(
            peek(), pattern,
            typeBuilder.build(), strictStart, strictEnd, patternDefinitions,
            measures.build(), after, subsets, allRows,
            partitionBitSet, RelCollations.of(fieldCollations), interval
        )
        stack.push(Frame(match))
        return this
    }

    /** Creates a Pivot.
     *
     *
     * To achieve the same effect as the SQL
     *
     * <blockquote><pre>`SELECT *
     * FROM (SELECT mgr, deptno, job, sal FROM emp)
     * PIVOT (SUM(sal) AS ss, COUNT(*) AS c
     * FOR (job, deptno)
     * IN (('CLERK', 10) AS c10, ('MANAGER', 20) AS m20))
    `</pre></blockquote> *
     *
     *
     * use the builder as follows:
     *
     * <blockquote><pre>`RelBuilder b;
     * b.scan("EMP");
     * final RelBuilder.GroupKey groupKey = b.groupKey("MGR");
     * final List<RelBuilder.AggCall> aggCalls =
     * Arrays.asList(b.sum(b.field("SAL")).as("SS"),
     * b.count().as("C"));
     * final List<RexNode> axes =
     * Arrays.asList(b.field("JOB"),
     * b.field("DEPTNO"));
     * final ImmutableMap.Builder<String, List<RexNode>> valueMap =
     * ImmutableMap.builder();
     * valueMap.put("C10",
     * Arrays.asList(b.literal("CLERK"), b.literal(10)));
     * valueMap.put("M20",
     * Arrays.asList(b.literal("MANAGER"), b.literal(20)));
     * b.pivot(groupKey, aggCalls, axes, valueMap.build().entrySet());
    `</pre></blockquote> *
     *
     *
     * Note that the SQL uses a sub-query to project away columns (e.g.
     * `HIREDATE`) that it does not reference, so that they do not appear in
     * the `GROUP BY`. You do not need to do that in this API, because the
     * `groupKey` parameter specifies the keys.
     *
     *
     * Pivot is implemented by desugaring. The above example becomes the
     * following:
     *
     * <blockquote><pre>`SELECT mgr,
     * SUM(sal) FILTER (WHERE job = 'CLERK' AND deptno = 10) AS c10_ss,
     * COUNT(*) FILTER (WHERE job = 'CLERK' AND deptno = 10) AS c10_c,
     * SUM(sal) FILTER (WHERE job = 'MANAGER' AND deptno = 20) AS m20_ss,
     * COUNT(*) FILTER (WHERE job = 'MANAGER' AND deptno = 20) AS m20_c
     * FROM emp
     * GROUP BY mgr
    `</pre></blockquote> *
     *
     * @param groupKey Key columns
     * @param aggCalls Aggregate expressions to compute for each value
     * @param axes Columns to pivot
     * @param values Values to pivot, and the alias for each column group
     *
     * @return this RelBuilder
     */
    fun pivot(
        groupKey: GroupKey?,
        aggCalls: Iterable<AggCall?>,
        axes: Iterable<RexNode?>?,
        values: Iterable<Map.Entry<String?, Iterable<RexNode?>?>?>?
    ): RelBuilder {
        val axisList: List<RexNode> = ImmutableList.copyOf(axes)
        val multipliedAggCalls: List<AggCall> = ArrayList()
        Pair.forEach(values) { alias, expressions ->
            val expressionList: List<RexNode> = ImmutableList.copyOf(expressions)
            if (expressionList.size() !== axisList.size()) {
                throw IllegalArgumentException(
                    "value count must match axis count ["
                            + expressionList + "], [" + axisList + "]"
                )
            }
            aggCalls.forEach { aggCall ->
                val alias2: String = alias.toString() + "_" + (aggCall as AggCallPlus).alias()
                val filters: List<RexNode> = ArrayList()
                Pair.forEach(axisList, expressionList) { axis, expression -> filters.add(equals(axis, expression)) }
                multipliedAggCalls.add(aggCall.filter(and(filters)).`as`(alias2))
            }
        }
        return aggregate(groupKey, multipliedAggCalls)
    }

    /**
     * Creates an Unpivot.
     *
     *
     * To achieve the same effect as the SQL
     *
     * <blockquote><pre>`SELECT *
     * FROM (SELECT deptno, job, sal, comm FROM emp)
     * UNPIVOT INCLUDE NULLS (remuneration
     * FOR remuneration_type IN (comm AS 'commission',
     * sal AS 'salary'))
    `</pre></blockquote> *
     *
     *
     * use the builder as follows:
     *
     * <blockquote><pre>`RelBuilder b;
     * b.scan("EMP");
     * final List<String> measureNames = Arrays.asList("REMUNERATION");
     * final List<String> axisNames = Arrays.asList("REMUNERATION_TYPE");
     * final Map<List<RexLiteral>, List<RexNode>> axisMap =
     * ImmutableMap.<List<RexLiteral>, List<RexNode>>builder()
     * .put(Arrays.asList(b.literal("commission")),
     * Arrays.asList(b.field("COMM")))
     * .put(Arrays.asList(b.literal("salary")),
     * Arrays.asList(b.field("SAL")))
     * .build();
     * b.unpivot(false, measureNames, axisNames, axisMap);
    `</pre></blockquote> *
     *
     *
     * The query generates two columns: `remuneration_type` (an axis
     * column) and `remuneration` (a measure column). Axis columns contain
     * values to indicate the source of the row (in this case, `'salary'`
     * if the row came from the `sal` column, and `'commission'`
     * if the row came from the `comm` column).
     *
     * @param includeNulls Whether to include NULL values in the output
     * @param measureNames Names of columns to be generated to hold pivoted
     * measures
     * @param axisNames Names of columns to be generated to hold qualifying values
     * @param axisMap Mapping from the columns that hold measures to the values
     * that the axis columns will hold in the generated rows
     * @return This RelBuilder
     */
    fun unpivot(
        includeNulls: Boolean,
        measureNames: Iterable<String?>?, axisNames: Iterable<String?>?,
        axisMap: Iterable<Map.Entry<List<RexLiteral?>?, List<RexNode?>?>?>
    ): RelBuilder {
        // Make immutable copies of all arguments.
        val measureNameList: List<String> = ImmutableList.copyOf(measureNames)
        val axisNameList: List<String> = ImmutableList.copyOf(axisNames)
        val map: List<Pair<List<RexLiteral>, List<RexNode>>> = StreamSupport.stream(axisMap.spliterator(), false)
            .map { pair ->
                Pair.< List < RexLiteral >, List<RexNode>>of<List<RexLiteral?>?, List<RexNode?>?>(
                ImmutableList.< RexLiteral > copyOf < RexLiteral ? > pair.getKey(),
                ImmutableList.< RexNode > copyOf < RexNode ? > pair.getValue())
            }
            .collect(Util.toImmutableList())

        // Check that counts match.
        Pair.forEach(map) { valueList, inputMeasureList ->
            if (inputMeasureList.size() !== measureNameList.size()) {
                throw IllegalArgumentException(
                    ("Number of measures ("
                            + inputMeasureList.size()) + ") must match number of measure names ("
                            + measureNameList.size().toString() + ")"
                )
            }
            if (valueList.size() !== axisNameList.size()) {
                throw IllegalArgumentException(
                    ("Number of axis values ("
                            + valueList.size()) + ") match match number of axis names ("
                            + axisNameList.size().toString() + ")"
                )
            }
        }
        val leftRowType: RelDataType = peek().getRowType()
        val usedFields = BitSet()
        Pair.forEach(map) { aliases, nodes ->
            nodes.forEach { node ->
                if (node is RexInputRef) {
                    usedFields.set((node as RexInputRef).getIndex())
                }
            }
        }

        // Create "VALUES (('commission'), ('salary')) AS t (remuneration_type)"
        values(ImmutableList.copyOf(Pair.left(map)), axisNameList)
        join(JoinRelType.INNER)
        val unusedFields: ImmutableBitSet = ImmutableBitSet.range(leftRowType.getFieldCount())
            .except(ImmutableBitSet.fromBitSet(usedFields))
        val projects: List<RexNode> = ArrayList(fields(unusedFields))
        Ord.forEach(axisNameList) { dimensionName, d ->
            projects.add(
                alias(
                    field(leftRowType.getFieldCount() + d),
                    dimensionName
                )
            )
        }
        val conditions: List<RexNode> = ArrayList()
        Ord.forEach(measureNameList) { measureName, m ->
            val caseOperands: List<RexNode> = ArrayList()
            Pair.forEach(map) { literals, nodes ->
                Ord.forEach(literals) { literal, d ->
                    conditions.add(
                        equals(field(leftRowType.getFieldCount() + d), literal)
                    )
                }
                caseOperands.add(and(conditions))
                conditions.clear()
                caseOperands.add(nodes.get(m))
            }
            caseOperands.add(literal(null))
            projects.add(
                alias(
                    call(SqlStdOperatorTable.CASE, caseOperands),
                    measureName
                )
            )
        }
        project(projects)
        if (!includeNulls) {
            // Add 'WHERE m1 IS NOT NULL OR m2 IS NOT NULL'
            val notNullFields = BitSet()
            Ord.forEach(measureNameList) { measureName, m ->
                val f: Int = unusedFields.cardinality() + axisNameList.size() + m
                conditions.add(isNotNull(field(f)))
                notNullFields.set(f)
            }
            filter(or(conditions))
            if (measureNameList.size() === 1) {
                // If there is one field, EXCLUDE NULLS will have converted it to NOT
                // NULL.
                val builder: RelDataTypeFactory.Builder = typeFactory.builder()
                peek().getRowType().getFieldList().forEach { field ->
                    val type: RelDataType = field.getType()
                    builder.add(
                        field.getName(),
                        if (notNullFields.get(field.getIndex())) typeFactory.createTypeWithNullability(
                            type,
                            false
                        ) else type
                    )
                }
                convert(builder.build(), false)
            }
            conditions.clear()
        }
        return this
    }

    /**
     * Attaches an array of hints to the stack top relational expression.
     *
     *
     * The redundant hints would be eliminated.
     *
     * @param hints Hints
     *
     * @throws AssertionError if the top relational expression does not implement
     * [org.apache.calcite.rel.hint.Hintable]
     */
    fun hints(vararg hints: RelHint?): RelBuilder {
        return hints(ImmutableList.copyOf(hints))
    }

    /**
     * Attaches multiple hints to the stack top relational expression.
     *
     *
     * The redundant hints would be eliminated.
     *
     * @param hints Hints
     *
     * @throws AssertionError if the top relational expression does not implement
     * [org.apache.calcite.rel.hint.Hintable]
     */
    fun hints(hints: Iterable<RelHint?>?): RelBuilder {
        requireNonNull(hints, "hints")
        val relHintList: List<RelHint?> = if (hints is List) hints else Lists.newArrayList(hints)
        if (relHintList.isEmpty()) {
            return this
        }
        val frame = peek_()
        assert(frame != null) { "There is no relational expression to attach the hints" }
        assert(frame.rel is Hintable) { "The top relational expression is not a Hintable" }
        val hintable: Hintable = frame.rel as Hintable
        replaceTop(hintable.attachHints(relHintList))
        return this
    }

    /** Clears the stack.
     *
     *
     * The builder's state is now the same as when it was created.  */
    fun clear() {
        stack.clear()
    }

    /** Information necessary to create a call to an aggregate function.
     *
     * @see RelBuilder.aggregateCall
     */
    interface AggCall {
        /** Returns a copy of this AggCall that applies a filter before aggregating
         * values.  */
        fun filter(@Nullable condition: RexNode?): AggCall?

        /** Returns a copy of this AggCall that sorts its input values by
         * `orderKeys` before aggregating, as in SQL's `WITHIN GROUP`
         * clause.  */
        fun sort(orderKeys: Iterable<RexNode?>?): AggCall?

        /** Returns a copy of this AggCall that sorts its input values by
         * `orderKeys` before aggregating, as in SQL's `WITHIN GROUP`
         * clause.  */
        fun sort(vararg orderKeys: RexNode?): AggCall? {
            return sort(ImmutableList.copyOf(orderKeys))
        }

        /** Returns a copy of this AggCall that makes its input values unique by
         * `distinctKeys` before aggregating, as in SQL's
         * `WITHIN DISTINCT` clause.  */
        fun unique(@Nullable distinctKeys: Iterable<RexNode?>?): AggCall?

        /** Returns a copy of this AggCall that makes its input values unique by
         * `distinctKeys` before aggregating, as in SQL's
         * `WITHIN DISTINCT` clause.  */
        fun unique(vararg distinctKeys: RexNode?): AggCall? {
            return unique(ImmutableList.copyOf(distinctKeys))
        }

        /** Returns a copy of this AggCall that may return approximate results
         * if `approximate` is true.  */
        fun approximate(approximate: Boolean): AggCall?

        /** Returns a copy of this AggCall that ignores nulls.  */
        fun ignoreNulls(ignoreNulls: Boolean): AggCall?

        /** Returns a copy of this AggCall with a given alias.  */
        fun `as`(@Nullable alias: String?): AggCall?
        /** Returns a copy of this AggCall that is optionally distinct.  */
        /** Returns a copy of this AggCall that is distinct.  */
        @JvmOverloads
        fun distinct(distinct: Boolean = true): AggCall?

        /** Converts this aggregate call to a windowed aggregate call.  */
        fun over(): OverCall?
    }

    /** Internal methods shared by all implementations of [AggCall].  */
    private interface AggCallPlus : AggCall {
        /** Returns the aggregate function.  */
        fun op(): SqlAggFunction

        /** Returns the alias.  */
        @Nullable
        fun alias(): String

        /** Returns an [AggregateCall] that is approximately equivalent
         * to this `AggCall` and is good for certain things, such as deriving
         * field names.  */
        fun aggregateCall(): AggregateCall?

        /** Converts this `AggCall` to a good [AggregateCall].  */
        fun aggregateCall(
            registrar: Registrar?, groupSet: ImmutableBitSet?,
            r: RelNode?
        ): AggregateCall?

        /** Registers expressions in operands and filters.  */
        fun register(registrar: Registrar?)
    }

    /** Information necessary to create the GROUP BY clause of an Aggregate.
     *
     * @see RelBuilder.groupKey
     */
    interface GroupKey {
        /** Assigns an alias to this group key.
         *
         *
         * Used to assign field names in the `group` operation.  */
        fun alias(@Nullable alias: String?): GroupKey?

        /** Returns the number of columns in the group key.  */
        fun groupKeyCount(): Int
    }

    /** Implementation of [RelBuilder.GroupKey].  */
    internal class GroupKeyImpl(
        nodes: ImmutableList<RexNode?>?,
        @Nullable nodeLists: ImmutableList<ImmutableList<RexNode?>?>,
        @Nullable alias: String?
    ) : GroupKey {
        val nodes: ImmutableList<RexNode?>

        @Nullable
        val nodeLists: ImmutableList<ImmutableList<RexNode?>?>

        @Nullable
        val alias: String?

        init {
            this.nodes = requireNonNull(nodes, "nodes")
            this.nodeLists = nodeLists
            this.alias = alias
        }

        @Override
        override fun toString(): String {
            return if (alias == null) nodes.toString() else nodes.toString() + " as " + alias
        }

        @Override
        override fun groupKeyCount(): Int {
            return nodes.size()
        }

        @Override
        override fun alias(@Nullable alias: String?): GroupKey {
            return if (Objects.equals(this.alias, alias)) this else GroupKeyImpl(nodes, nodeLists, alias)
        }

        val isSimple: Boolean
            get() = nodeLists == null || nodeLists.size() === 1
    }

    /** Implementation of [AggCall].  */
    private inner class AggCallImpl internal constructor(
        aggFunction: SqlAggFunction, distinct: Boolean,
        approximate: Boolean, ignoreNulls: Boolean, @Nullable filter: RexNode?,
        @Nullable alias: String, operands: ImmutableList<RexNode?>?,
        @Nullable distinctKeys: ImmutableList<RexNode?>,
        orderKeys: ImmutableList<RexNode?>?
    ) : AggCallPlus {
        private val aggFunction: SqlAggFunction
        private val distinct: Boolean
        private val approximate: Boolean
        private val ignoreNulls: Boolean

        @Nullable
        private val filter: RexNode?

        @Nullable
        private val alias: String
        private val operands // may be empty
                : ImmutableList<RexNode?>

        @Nullable
        private val distinctKeys // may be empty or null
                : ImmutableList<RexNode?>?
        private val orderKeys // may be empty
                : ImmutableList<RexNode?>

        init {
            var filter: RexNode? = filter
            this.aggFunction = requireNonNull(aggFunction, "aggFunction")
            // If the aggregate function ignores DISTINCT,
            // make the DISTINCT flag FALSE.
            this.distinct = (distinct
                    && aggFunction.getDistinctOptionality() !== Optionality.IGNORED)
            this.approximate = approximate
            this.ignoreNulls = ignoreNulls
            this.alias = alias
            this.operands = requireNonNull(operands, "operands")
            this.distinctKeys = distinctKeys
            this.orderKeys = requireNonNull(orderKeys, "orderKeys")
            if (filter != null) {
                if (filter.getType().getSqlTypeName() !== SqlTypeName.BOOLEAN) {
                    throw RESOURCE.filterMustBeBoolean().ex()
                }
                if (filter.getType().isNullable()) {
                    filter = call(SqlStdOperatorTable.IS_TRUE, filter)
                }
            }
            this.filter = filter
        }

        @Override
        override fun toString(): String {
            val b = StringBuilder()
            b.append(aggFunction.getName())
                .append('(')
            if (distinct) {
                b.append("DISTINCT ")
            }
            if (operands.size() > 0) {
                b.append(operands.get(0))
                for (i in 1 until operands.size()) {
                    b.append(", ")
                    b.append(operands.get(i))
                }
            }
            b.append(')')
            if (filter != null) {
                b.append(" FILTER (WHERE ").append(filter).append(')')
            }
            if (distinctKeys != null) {
                b.append(" WITHIN DISTINCT (").append(distinctKeys).append(')')
            }
            return b.toString()
        }

        @Override
        override fun op(): SqlAggFunction {
            return aggFunction
        }

        @Override
        @Nullable
        override fun alias(): String {
            return alias
        }

        @Override
        override fun aggregateCall(): AggregateCall {
            // Use dummy values for collation and type. This method only promises to
            // return a call that is "approximately equivalent ... and is good for
            // deriving field names", so dummy values are good enough.
            val collation: RelCollation = RelCollations.EMPTY
            val type: RelDataType = typeFactory.createSqlType(SqlTypeName.BOOLEAN)
            return AggregateCall.create(
                aggFunction, distinct, approximate,
                ignoreNulls, ImmutableList.of(), -1, null, collation, type, alias
            )
        }

        @Override
        override fun aggregateCall(
            registrar: Registrar,
            groupSet: ImmutableBitSet, r: RelNode?
        ): AggregateCall {
            var args: List<Integer?> = registrar.registerExpressions(operands)
            val filterArg = if (filter == null) -1 else registrar.registerExpression(filter)
            if (distinct && !aggFunction.isQuantifierAllowed()) {
                throw IllegalArgumentException("DISTINCT not allowed")
            }
            if (filter != null && !aggFunction.allowsFilter()) {
                throw IllegalArgumentException("FILTER not allowed")
            }
            @Nullable val distinctKeys: ImmutableBitSet? = if (distinctKeys == null) null else ImmutableBitSet.of(
                registrar.registerExpressions(distinctKeys)
            )
            val collation: RelCollation = RelCollations.of(
                orderKeys
                    .stream()
                    .map { orderKey ->
                        collation(
                            orderKey, RelFieldCollation.Direction.ASCENDING,
                            null, Collections.emptyList()
                        )
                    }
                    .collect(Collectors.toList()))
            if (aggFunction is SqlCountAggFunction && !distinct) {
                args = args.stream()
                    .filter(r::fieldIsNullable)
                    .collect(Util.toImmutableList())
            }
            return AggregateCall.create(
                aggFunction, distinct, approximate,
                ignoreNulls, args, filterArg, distinctKeys, collation,
                groupSet.cardinality(), r, null, alias
            )
        }

        @Override
        override fun register(registrar: Registrar) {
            registrar.registerExpressions(operands)
            if (filter != null) {
                registrar.registerExpression(filter)
            }
            if (distinctKeys != null) {
                registrar.registerExpressions(distinctKeys)
            }
            registrar.registerExpressions(orderKeys)
        }

        @Override
        override fun over(): OverCall {
            return OverCallImpl(
                aggFunction, distinct, operands, ignoreNulls,
                alias
            )
        }

        @Override
        override fun sort(orderKeys: Iterable<RexNode?>?): AggCall {
            val orderKeyList: ImmutableList<RexNode> = ImmutableList.copyOf(orderKeys)
            return if (orderKeyList.equals(this.orderKeys)) this else AggCallImpl(
                aggFunction, distinct, approximate, ignoreNulls,
                filter, alias, operands, distinctKeys, orderKeyList
            )
        }

        @Override
        override fun sort(vararg orderKeys: RexNode?): AggCall {
            return sort(ImmutableList.copyOf(orderKeys))
        }

        @Override
        override fun unique(@Nullable distinctKeys: Iterable<RexNode?>?): AggCall {
            @Nullable val distinctKeyList: ImmutableList<RexNode>? =
                if (distinctKeys == null) null else ImmutableList.copyOf(distinctKeys)
            return if (Objects.equals(distinctKeyList, this.distinctKeys)) this else AggCallImpl(
                aggFunction, distinct, approximate, ignoreNulls,
                filter, alias, operands, distinctKeyList, orderKeys
            )
        }

        @Override
        override fun approximate(approximate: Boolean): AggCall {
            return if (approximate == this.approximate) this else AggCallImpl(
                aggFunction, distinct, approximate, ignoreNulls,
                filter, alias, operands, distinctKeys, orderKeys
            )
        }

        @Override
        override fun filter(@Nullable condition: RexNode?): AggCall {
            return if (Objects.equals(condition, filter)) this else AggCallImpl(
                aggFunction, distinct, approximate, ignoreNulls,
                condition, alias, operands, distinctKeys, orderKeys
            )
        }

        @Override
        override fun `as`(@Nullable alias: String?): AggCall {
            return if (Objects.equals(alias, this.alias)) this else AggCallImpl(
                aggFunction, distinct, approximate, ignoreNulls,
                filter, alias, operands, distinctKeys, orderKeys
            )
        }

        @Override
        override fun distinct(distinct: Boolean): AggCall {
            return if (distinct == this.distinct) this else AggCallImpl(
                aggFunction, distinct, approximate, ignoreNulls,
                filter, alias, operands, distinctKeys, orderKeys
            )
        }

        @Override
        override fun ignoreNulls(ignoreNulls: Boolean): AggCall {
            return if (ignoreNulls == this.ignoreNulls) this else AggCallImpl(
                aggFunction, distinct, approximate, ignoreNulls,
                filter, alias, operands, distinctKeys, orderKeys
            )
        }
    }

    /** Implementation of [AggCall] that wraps an
     * [AggregateCall].  */
    private inner class AggCallImpl2 internal constructor(
        aggregateCall: AggregateCall?,
        operands: ImmutableList<RexNode?>?
    ) : AggCallPlus {
        private val aggregateCall: AggregateCall
        private val operands: ImmutableList<RexNode>

        init {
            this.aggregateCall = requireNonNull(aggregateCall, "aggregateCall")
            this.operands = requireNonNull(operands, "operands")
        }

        @Override
        override fun over(): OverCall {
            return OverCallImpl(
                aggregateCall.getAggregation(),
                aggregateCall.isDistinct(), operands, aggregateCall.ignoreNulls(),
                aggregateCall.name
            )
        }

        @Override
        override fun toString(): String {
            return aggregateCall.toString()
        }

        @Override
        override fun op(): SqlAggFunction {
            return aggregateCall.getAggregation()
        }

        @Override
        @Nullable
        override fun alias(): String {
            return aggregateCall.name
        }

        @Override
        override fun aggregateCall(): AggregateCall {
            return aggregateCall
        }

        @Override
        override fun aggregateCall(
            registrar: Registrar?,
            groupSet: ImmutableBitSet?, r: RelNode?
        ): AggregateCall {
            return aggregateCall
        }

        @Override
        override fun register(registrar: Registrar?) {
            // nothing to do
        }

        @Override
        override fun sort(orderKeys: Iterable<RexNode?>?): AggCall {
            throw UnsupportedOperationException()
        }

        @Override
        override fun sort(vararg orderKeys: RexNode?): AggCall {
            throw UnsupportedOperationException()
        }

        @Override
        override fun unique(@Nullable distinctKeys: Iterable<RexNode?>?): AggCall {
            throw UnsupportedOperationException()
        }

        @Override
        override fun approximate(approximate: Boolean): AggCall {
            throw UnsupportedOperationException()
        }

        @Override
        override fun filter(@Nullable condition: RexNode?): AggCall {
            throw UnsupportedOperationException()
        }

        @Override
        override fun `as`(@Nullable alias: String?): AggCall {
            throw UnsupportedOperationException()
        }

        @Override
        override fun distinct(distinct: Boolean): AggCall {
            throw UnsupportedOperationException()
        }

        @Override
        override fun ignoreNulls(ignoreNulls: Boolean): AggCall {
            throw UnsupportedOperationException()
        }
    }

    /** Call to a windowed aggregate function.
     *
     *
     * To create an `OverCall`, start with an [AggCall] (created
     * by a method such as [.aggregateCall], [.sum] or [.count])
     * and call its [AggCall.over] method. For example,
     *
     * <pre>`b.scan("EMP")
     * .project(b.field("DEPTNO"),
     * b.aggregateCall(SqlStdOperatorTable.ROW_NUMBER)
     * .over()
     * .partitionBy()
     * .orderBy(b.field("EMPNO"))
     * .rowsUnbounded()
     * .allowPartial(true)
     * .nullWhenCountZero(false)
     * .as("x"))
    `</pre> *
     *
     *
     * Unlike an aggregate call, a windowed aggregate call is an expression
     * that you can use in a [Project] or [Filter]. So, to finish,
     * call [OverCall.toRex] to convert the `OverCall` to a
     * [RexNode]; the [OverCall. as] method (used in the above example)
     * does the same but also assigns an column alias.
     */
    interface OverCall {
        /** Performs an action on this OverCall.  */
        fun <R> let(consumer: Function<OverCall?, R>): R {
            return consumer.apply(this)
        }

        /** Sets the PARTITION BY clause to an array of expressions.  */
        fun partitionBy(vararg expressions: RexNode?): OverCall?

        /** Sets the PARTITION BY clause to a list of expressions.  */
        fun partitionBy(expressions: Iterable<RexNode?>?): OverCall?

        /** Sets the ORDER BY BY clause to an array of expressions.
         *
         *
         * Use [.desc], [.nullsFirst],
         * [.nullsLast] to control the sort order.  */
        fun orderBy(vararg expressions: RexNode?): OverCall?

        /** Sets the ORDER BY BY clause to a list of expressions.
         *
         *
         * Use [.desc], [.nullsFirst],
         * [.nullsLast] to control the sort order.  */
        fun orderBy(expressions: Iterable<RexNode?>?): OverCall?

        /** Sets an unbounded ROWS window,
         * equivalent to SQL `ROWS BETWEEN UNBOUNDED PRECEDING AND
         * UNBOUNDED FOLLOWING`.  */
        fun rowsUnbounded(): OverCall? {
            return rowsBetween(
                RexWindowBounds.UNBOUNDED_PRECEDING,
                RexWindowBounds.UNBOUNDED_FOLLOWING
            )
        }

        /** Sets a ROWS window with a lower bound,
         * equivalent to SQL `ROWS BETWEEN lower AND CURRENT ROW`.  */
        fun rowsFrom(lower: RexWindowBound?): OverCall? {
            return rowsBetween(lower, RexWindowBounds.UNBOUNDED_FOLLOWING)
        }

        /** Sets a ROWS window with an upper bound,
         * equivalent to SQL `ROWS BETWEEN CURRENT ROW AND upper`.  */
        fun rowsTo(upper: RexWindowBound?): OverCall? {
            return rowsBetween(RexWindowBounds.UNBOUNDED_PRECEDING, upper)
        }

        /** Sets a RANGE window with lower and upper bounds,
         * equivalent to SQL `ROWS BETWEEN lower ROW AND upper`.  */
        fun rowsBetween(lower: RexWindowBound?, upper: RexWindowBound?): OverCall?

        /** Sets an unbounded RANGE window,
         * equivalent to SQL `RANGE BETWEEN UNBOUNDED PRECEDING AND
         * UNBOUNDED FOLLOWING`.  */
        fun rangeUnbounded(): OverCall? {
            return rangeBetween(
                RexWindowBounds.UNBOUNDED_PRECEDING,
                RexWindowBounds.UNBOUNDED_FOLLOWING
            )
        }

        /** Sets a RANGE window with a lower bound,
         * equivalent to SQL `RANGE BETWEEN lower AND CURRENT ROW`.  */
        fun rangeFrom(lower: RexWindowBound?): OverCall? {
            return rangeBetween(lower, RexWindowBounds.CURRENT_ROW)
        }

        /** Sets a RANGE window with an upper bound,
         * equivalent to SQL `RANGE BETWEEN CURRENT ROW AND upper`.  */
        operator fun rangeTo(upper: RexWindowBound?): OverCall? {
            return rangeBetween(RexWindowBounds.UNBOUNDED_PRECEDING, upper)
        }

        /** Sets a RANGE window with lower and upper bounds,
         * equivalent to SQL `RANGE BETWEEN lower ROW AND upper`.  */
        fun rangeBetween(lower: RexWindowBound?, upper: RexWindowBound?): OverCall?

        /** Sets whether to allow partial width windows; default true.  */
        fun allowPartial(allowPartial: Boolean): OverCall?

        /** Sets whether the aggregate function should evaluate to null if no rows
         * are in the window; default false.  */
        fun nullWhenCountZero(nullWhenCountZero: Boolean): OverCall?

        /** Sets the alias of this expression, and converts it to a [RexNode];
         * default is the alias that was set via [AggCall. as].  */
        fun `as`(alias: String?): RexNode?

        /** Converts this expression to a [RexNode].  */
        fun toRex(): RexNode?
    }

    /** Implementation of [OverCall].  */
    private inner class OverCallImpl private constructor(
        op: SqlAggFunction, distinct: Boolean,
        operands: ImmutableList<RexNode>, ignoreNulls: Boolean,
        @Nullable alias: String, partitionKeys: ImmutableList<RexNode>,
        sortKeys: ImmutableList<RexFieldCollation>, rows: Boolean,
        lowerBound: RexWindowBound, upperBound: RexWindowBound,
        nullWhenCountZero: Boolean, allowPartial: Boolean
    ) : OverCall {
        private val operands: ImmutableList<RexNode>
        private val ignoreNulls: Boolean

        @Nullable
        private val alias: String?
        private val nullWhenCountZero: Boolean
        private val allowPartial: Boolean
        private val rows: Boolean
        private val lowerBound: RexWindowBound
        private val upperBound: RexWindowBound
        private val partitionKeys: ImmutableList<RexNode>
        private val sortKeys: ImmutableList<RexFieldCollation>
        private val op: SqlAggFunction
        private val distinct: Boolean

        init {
            this.op = op
            this.distinct = distinct
            this.operands = operands
            this.ignoreNulls = ignoreNulls
            this.alias = alias
            this.partitionKeys = partitionKeys
            this.sortKeys = sortKeys
            this.nullWhenCountZero = nullWhenCountZero
            this.allowPartial = allowPartial
            this.rows = rows
            this.lowerBound = lowerBound
            this.upperBound = upperBound
        }

        /** Creates an OverCallImpl with default settings.  */
        internal constructor(
            op: SqlAggFunction, distinct: Boolean,
            operands: ImmutableList<RexNode>, ignoreNulls: Boolean,
            @Nullable alias: String
        ) : this(
            op, distinct, operands, ignoreNulls, alias, ImmutableList.of(),
            ImmutableList.of(), true, RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING, false, true
        ) {
        }

        @Override
        override fun partitionBy(
            expressions: Iterable<RexNode?>?
        ): OverCall {
            return partitionBy_(ImmutableList.copyOf(expressions))
        }

        @Override
        override fun partitionBy(vararg expressions: RexNode?): OverCall {
            return partitionBy_(ImmutableList.copyOf(expressions))
        }

        private fun partitionBy_(partitionKeys: ImmutableList<RexNode>): OverCall {
            return OverCallImpl(
                op, distinct, operands, ignoreNulls, alias,
                partitionKeys, sortKeys, rows, lowerBound, upperBound,
                nullWhenCountZero, allowPartial
            )
        }

        private fun orderBy_(sortKeys: ImmutableList<RexFieldCollation>): OverCall {
            return OverCallImpl(
                op, distinct, operands, ignoreNulls, alias,
                partitionKeys, sortKeys, rows, lowerBound, upperBound,
                nullWhenCountZero, allowPartial
            )
        }

        @Override
        override fun orderBy(sortKeys: Iterable<RexNode?>): OverCall {
            val fieldCollations: ImmutableList.Builder<RexFieldCollation> = ImmutableList.builder()
            sortKeys.forEach { sortKey ->
                fieldCollations.add(
                    rexCollation(
                        sortKey, RelFieldCollation.Direction.ASCENDING,
                        RelFieldCollation.NullDirection.UNSPECIFIED
                    )
                )
            }
            return orderBy_(fieldCollations.build())
        }

        @Override
        override fun orderBy(vararg sortKeys: RexNode?): OverCall {
            return orderBy(Arrays.asList(sortKeys))
        }

        @Override
        override fun rowsBetween(
            lowerBound: RexWindowBound?,
            upperBound: RexWindowBound?
        ): OverCall {
            return OverCallImpl(
                op, distinct, operands, ignoreNulls, alias,
                partitionKeys, sortKeys, true, lowerBound, upperBound,
                nullWhenCountZero, allowPartial
            )
        }

        @Override
        override fun rangeBetween(
            lowerBound: RexWindowBound?,
            upperBound: RexWindowBound?
        ): OverCall {
            return OverCallImpl(
                op, distinct, operands, ignoreNulls, alias,
                partitionKeys, sortKeys, false, lowerBound, upperBound,
                nullWhenCountZero, allowPartial
            )
        }

        @Override
        override fun allowPartial(allowPartial: Boolean): OverCall {
            return OverCallImpl(
                op, distinct, operands, ignoreNulls, alias,
                partitionKeys, sortKeys, rows, lowerBound, upperBound,
                nullWhenCountZero, allowPartial
            )
        }

        @Override
        override fun nullWhenCountZero(nullWhenCountZero: Boolean): OverCall {
            return OverCallImpl(
                op, distinct, operands, ignoreNulls, alias,
                partitionKeys, sortKeys, rows, lowerBound, upperBound,
                nullWhenCountZero, allowPartial
            )
        }

        @Override
        override fun `as`(alias: String?): RexNode {
            return OverCallImpl(
                op, distinct, operands, ignoreNulls, alias,
                partitionKeys, sortKeys, rows, lowerBound, upperBound,
                nullWhenCountZero, allowPartial
            ).toRex()
        }

        @Override
        override fun toRex(): RexNode {
            val bind: RexCallBinding = object : RexCallBinding(
                typeFactory, op, operands,
                ImmutableList.of()
            ) {
                @get:Override
                val groupCount: Int
                    get() = if (SqlWindow.isAlwaysNonEmpty(lowerBound, upperBound)) 1 else 0
            }
            val type: RelDataType = op.inferReturnType(bind)
            val over: RexNode = rexBuilder
                .makeOver(
                    type, op, operands, partitionKeys, sortKeys,
                    lowerBound, upperBound, rows, allowPartial, nullWhenCountZero,
                    distinct, ignoreNulls
                )
            return alias?.let { alias(over, it) } ?: over
        }
    }

    /** Collects the extra expressions needed for [.aggregate].
     *
     *
     * The extra expressions come from the group key and as arguments to
     * aggregate calls, and later there will be a [.project] or a
     * [.rename] if necessary.  */
    private class Registrar internal constructor(fields: Iterable<RexNode?>?, fieldNames: List<String?>?) {
        val originalExtraNodes: List<RexNode>
        val extraNodes: List<RexNode?>
        val names: List<String>

        init {
            originalExtraNodes = ImmutableList.copyOf(fields)
            extraNodes = ArrayList(originalExtraNodes)
            names = ArrayList(fieldNames)
        }

        fun registerExpression(node: RexNode?): Int {
            return when (node.getKind()) {
                AS -> {
                    val operands: List<RexNode> = (node as RexCall?).operands
                    val i = registerExpression(operands[0])
                    names.set(i, RexLiteral.stringValue(operands[1]))
                    i
                }
                DESCENDING, NULLS_FIRST, NULLS_LAST -> registerExpression((node as RexCall?).operands.get(0))
                else -> {
                    val i2 = extraNodes.indexOf(node)
                    if (i2 >= 0) {
                        return i2
                    }
                    extraNodes.add(node)
                    names.add(null)
                    extraNodes.size() - 1
                }
            }
        }

        fun registerExpressions(nodes: Iterable<RexNode?>): List<Integer> {
            val builder: List<Integer> = ArrayList()
            for (node in nodes) {
                builder.add(registerExpression(node))
            }
            return builder
        }

        fun registerFieldCollations(
            orderKeys: Iterable<RexNode?>
        ): List<RelFieldCollation> {
            val fieldCollations: List<RelFieldCollation> = ArrayList()
            for (orderKey in orderKeys) {
                val collation: RelFieldCollation = collation(
                    orderKey, RelFieldCollation.Direction.ASCENDING, null,
                    extraNodes
                )
                if (!RelCollations.ordinals(fieldCollations)
                        .contains(collation.getFieldIndex())
                ) {
                    fieldCollations.add(collation)
                }
            }
            return ImmutableList.copyOf(fieldCollations)
        }

        /** Returns the number of fields added.  */
        fun addedFieldCount(): Int {
            return extraNodes.size() - originalExtraNodes.size()
        }
    }

    /** Builder stack frame.
     *
     *
     * Describes a previously created relational expression and
     * information about how table aliases map into its row type.  */
    private class Frame {
        val rel: RelNode
        val fields: ImmutableList<Field>

        constructor(rel: RelNode, fields: ImmutableList<Field>) {
            this.rel = rel
            this.fields = fields
        }

        constructor(rel: RelNode) {
            val tableAlias = deriveAlias(rel)
            val builder: ImmutableList.Builder<Field> = ImmutableList.builder()
            val aliases: ImmutableSet<String?> =
                if (tableAlias == null) ImmutableSet.of() else ImmutableSet.of(tableAlias)
            for (field in rel.getRowType().getFieldList()) {
                builder.add(Field(aliases, field))
            }
            this.rel = rel
            fields = builder.build()
        }

        @Override
        override fun toString(): String {
            return rel.toString() + ": " + fields
        }

        fun fields(): List<RelDataTypeField> {
            return Pair.right(fields)
        }

        companion object {
            @Nullable
            private fun deriveAlias(rel: RelNode): String? {
                if (rel is TableScan) {
                    val scan: TableScan = rel as TableScan
                    val names: List<String> = scan.getTable().getQualifiedName()
                    if (!names.isEmpty()) {
                        return Util.last(names)
                    }
                }
                return null
            }
        }
    }

    /** A field that belongs to a stack [Frame].  */
    private class Field internal constructor(left: ImmutableSet<String?>?, right: RelDataTypeField?) :
        Pair<ImmutableSet<String?>?, RelDataTypeField?>(left, right) {
        fun addAlias(alias: String?): Field {
            if (left.contains(alias)) {
                return this
            }
            val aliasList: ImmutableSet<String?> =
                ImmutableSet.< String > builder < String ? > ().addAll(left).add(alias).build()
            return Field(aliasList, right)
        }
    }

    /** Shuttle that shifts a predicate's inputs to the left, replacing early
     * ones with references to a
     * [RexCorrelVariable].  */
    private inner class Shifter internal constructor(left: RelNode, id: CorrelationId, right: RelNode) : RexShuttle() {
        private val left: RelNode
        private val id: CorrelationId
        private val right: RelNode

        init {
            this.left = left
            this.id = id
            this.right = right
        }

        @Override
        fun visitInputRef(inputRef: RexInputRef): RexNode {
            val leftRowType: RelDataType = left.getRowType()
            val rexBuilder: RexBuilder = rexBuilder
            val leftCount: Int = leftRowType.getFieldCount()
            return if (inputRef.getIndex() < leftCount) {
                val v: RexNode = rexBuilder.makeCorrel(leftRowType, id)
                rexBuilder.makeFieldAccess(v, inputRef.getIndex())
            } else {
                rexBuilder.makeInputRef(right, inputRef.getIndex() - leftCount)
            }
        }
    }

    /** Configuration of RelBuilder.
     *
     *
     * It is immutable, and all fields are public.
     *
     *
     * Start with the [.DEFAULT] instance,
     * and call `withXxx` methods to set its properties.  */
    @Value.Immutable
    interface Config {
        /** Controls whether to merge two [Project] operators when inlining
         * expressions causes complexity to increase.
         *
         *
         * Usually merging projects is beneficial, but occasionally the
         * result is more complex than the original projects. Consider:
         *
         * <pre>
         * P: Project(a+b+c AS x, d+e+f AS y, g+h+i AS z)  # complexity 15
         * Q: Project(x*y*z AS p, x-y-z AS q)              # complexity 10
         * R: Project((a+b+c)*(d+e+f)*(g+h+i) AS s,
         * (a+b+c)-(d+e+f)-(g+h+i) AS t)        # complexity 34
        </pre> *
         *
         * The complexity of an expression is the number of nodes (leaves and
         * operators). For example, `a+b+c` has complexity 5 (3 field
         * references and 2 calls):
         *
         * <pre>
         * +
         * /  \
         * +    c
         * / \
         * a   b
        </pre> *
         *
         *
         * A negative value never allows merges.
         *
         *
         * A zero or positive value, `bloat`, allows a merge if complexity
         * of the result is less than or equal to the sum of the complexity of the
         * originals plus `bloat`.
         *
         *
         * The default value, 100, allows a moderate increase in complexity but
         * prevents cases where complexity would run away into the millions and run
         * out of memory. Moderate complexity is OK; the implementation, say via
         * [org.apache.calcite.adapter.enumerable.EnumerableCalc], will often
         * gather common sub-expressions and compute them only once.
         */
        @Value.Default
        fun bloat(): Int {
            return 100
        }

        /** Sets [.bloat].  */
        fun withBloat(bloat: Int): Config?

        /** Whether [RelBuilder.aggregate] should eliminate duplicate
         * aggregate calls; default true.  */
        @Value.Default
        fun dedupAggregateCalls(): Boolean {
            return true
        }

        /** Sets [.dedupAggregateCalls].  */
        fun withDedupAggregateCalls(dedupAggregateCalls: Boolean): Config?

        /** Whether [RelBuilder.aggregate] should prune unused
         * input columns; default true.  */
        @Value.Default
        fun pruneInputOfAggregate(): Boolean {
            return true
        }

        /** Sets [.pruneInputOfAggregate].  */
        fun withPruneInputOfAggregate(pruneInputOfAggregate: Boolean): Config?

        /** Whether to push down join conditions; default false (but
         * [SqlToRelConverter.config] by default sets this to true).  */
        @Value.Default
        fun pushJoinCondition(): Boolean {
            return false
        }

        /** Sets [.pushJoinCondition].  */
        fun withPushJoinCondition(pushJoinCondition: Boolean): Config?

        /** Whether to simplify expressions; default true.  */
        @Value.Default
        fun simplify(): Boolean {
            return true
        }

        /** Sets [.simplify].  */
        fun withSimplify(simplify: Boolean): Config

        /** Whether to simplify LIMIT 0 to an empty relation; default true.  */
        @Value.Default
        fun simplifyLimit(): Boolean {
            return true
        }

        /** Sets [.simplifyLimit].  */
        fun withSimplifyLimit(simplifyLimit: Boolean): Config?

        /** Whether to simplify `Union(Values, Values)` or
         * `Union(Project(Values))` to `Values`; default true.  */
        @Value.Default
        fun simplifyValues(): Boolean {
            return true
        }

        /** Sets [.simplifyValues].  */
        fun withSimplifyValues(simplifyValues: Boolean): Config?

        /** Whether to create an Aggregate even if we know that the input is
         * already unique; default false.  */
        @Value.Default
        fun aggregateUnique(): Boolean {
            return false
        }

        /** Sets [.aggregateUnique].  */
        fun withAggregateUnique(aggregateUnique: Boolean): Config?

        companion object {
            /** Default configuration.  */
            val DEFAULT: Config = ImmutableRelBuilder.Config.of()
        }
    }

    companion object {
        /**
         * Derives the view expander
         * [org.apache.calcite.plan.RelOptTable.ViewExpander]
         * to be used for this RelBuilder.
         *
         *
         * The ViewExpander instance is used for expanding views in the default
         * table scan factory `RelFactories.TableScanFactoryImpl`.
         * You can also define a new table scan factory in the `struct`
         * to override the whole table scan creation.
         *
         *
         * The default view expander does not support expanding views.
         */
        private fun getViewExpander(
            cluster: RelOptCluster,
            context: Context?
        ): RelOptTable.ViewExpander {
            return context.maybeUnwrap(RelOptTable.ViewExpander::class.java)
                .orElseGet { ViewExpanders.simpleContext(cluster) }
        }

        /** Derives the Config to be used for this RelBuilder.
         *
         *
         * Overrides [RelBuilder.Config.simplify] if
         * [Hook.REL_BUILDER_SIMPLIFY] is set.
         */
        private fun getConfig(context: Context?): Config {
            val config: Config = context.maybeUnwrap(Config::class.java).orElse(Config.DEFAULT)
            val simplify: Boolean = Hook.REL_BUILDER_SIMPLIFY.get(config.simplify())
            return config.withSimplify(simplify)
        }

        /** Creates a RelBuilder.  */
        fun create(config: FrameworkConfig): RelBuilder {
            return Frameworks.withPrepare(
                config
            ) { cluster, relOptSchema, rootSchema, statement -> RelBuilder(config.getContext(), cluster, relOptSchema) }
        }

        /** Creates a [RelBuilderFactory], a partially-created RelBuilder.
         * Just add a [RelOptCluster] and a [RelOptSchema]  */
        fun proto(context: Context?): RelBuilderFactory {
            return RelBuilderFactory { cluster, schema -> RelBuilder(context, cluster, schema) }
        }

        /** Creates a [RelBuilderFactory] that uses a given set of factories.  */
        fun proto(vararg factories: Object?): RelBuilderFactory {
            return proto(Contexts.of(factories))
        }

        private fun groupKey_(
            nodes: Iterable<RexNode?>,
            nodeLists: Iterable<Iterable<RexNode?>?>
        ): GroupKey {
            val builder: ImmutableList.Builder<ImmutableList<RexNode>> = ImmutableList.builder()
            for (nodeList in nodeLists) {
                builder.add(ImmutableList.copyOf(nodeList))
            }
            return GroupKeyImpl(ImmutableList.copyOf(nodes), builder.build(), null)
        }

        /**
         * Gets column mappings of the operator.
         *
         * @param op operator instance
         * @return column mappings associated with this function
         */
        @Nullable
        private fun getColumnMappings(op: SqlOperator): Set<RelColumnMapping>? {
            val inference: SqlReturnTypeInference = op.getReturnTypeInference()
            return if (inference is TableFunctionReturnTypeInference) {
                (inference as TableFunctionReturnTypeInference).getColumnMappings()
            } else {
                null
            }
        }

        private fun isGroupId(c: AggCall): Boolean {
            return (c as AggCallPlus).op().kind === SqlKind.GROUP_ID
        }

        /** Returns whether all values for a given column are null.  */
        private fun allNull(@Nullable values: Array<Object?>, column: Int, columnCount: Int): Boolean {
            var i = column
            while (i < values.size) {
                if (values[i] != null) {
                    return false
                }
                i += columnCount
            }
            return true
        }

        /** Converts an iterable of lists into an immutable list of immutable lists
         * with the same contents. Returns the same object if possible.  */
        private fun <E> copy(
            tupleList: Iterable<List<E>?>
        ): ImmutableList<ImmutableList<E>?> {
            val builder: ImmutableList.Builder<ImmutableList<E>> = ImmutableList.builder()
            var changeCount = 0
            for (literals in tupleList) {
                val literals2: ImmutableList<E> = ImmutableList.copyOf(literals)
                builder.add(literals2)
                if (literals !== literals2) {
                    ++changeCount
                }
            }
            return if (changeCount == 0 && tupleList is ImmutableList) {
                // don't make a copy if we don't have to
                tupleList as ImmutableList<ImmutableList<E>?>
            } else builder.build()
        }

        private fun collation(
            node: RexNode,
            direction: RelFieldCollation.Direction,
            nullDirection: @Nullable RelFieldCollation.NullDirection?,
            extraNodes: List<RexNode?>
        ): RelFieldCollation {
            return when (node.getKind()) {
                INPUT_REF -> RelFieldCollation(
                    (node as RexInputRef).getIndex(), direction,
                    Util.first(nullDirection, direction.defaultNullDirection())
                )
                DESCENDING -> collation(
                    (node as RexCall).getOperands().get(0),
                    RelFieldCollation.Direction.DESCENDING,
                    nullDirection, extraNodes
                )
                NULLS_FIRST -> collation(
                    (node as RexCall).getOperands().get(0), direction,
                    RelFieldCollation.NullDirection.FIRST, extraNodes
                )
                NULLS_LAST -> collation(
                    (node as RexCall).getOperands().get(0), direction,
                    RelFieldCollation.NullDirection.LAST, extraNodes
                )
                else -> {
                    val fieldIndex: Int = extraNodes.size()
                    extraNodes.add(node)
                    RelFieldCollation(
                        fieldIndex, direction,
                        Util.first(nullDirection, direction.defaultNullDirection())
                    )
                }
            }
        }

        private fun rexCollation(
            node: RexNode,
            direction: RelFieldCollation.Direction,
            nullDirection: @Nullable RelFieldCollation.NullDirection?
        ): RexFieldCollation {
            return when (node.getKind()) {
                DESCENDING -> rexCollation(
                    (node as RexCall).operands.get(0),
                    RelFieldCollation.Direction.DESCENDING, nullDirection
                )
                NULLS_LAST -> rexCollation(
                    (node as RexCall).operands.get(0),
                    direction, RelFieldCollation.NullDirection.LAST
                )
                NULLS_FIRST -> rexCollation(
                    (node as RexCall).operands.get(0),
                    direction, RelFieldCollation.NullDirection.FIRST
                )
                else -> {
                    val flags: Set<SqlKind> = EnumSet.noneOf(SqlKind::class.java)
                    if (direction === RelFieldCollation.Direction.DESCENDING) {
                        flags.add(SqlKind.DESCENDING)
                    }
                    if (nullDirection === RelFieldCollation.NullDirection.FIRST) {
                        flags.add(SqlKind.NULLS_FIRST)
                    }
                    if (nullDirection === RelFieldCollation.NullDirection.LAST) {
                        flags.add(SqlKind.NULLS_LAST)
                    }
                    RexFieldCollation(node, flags)
                }
            }
        }

        /**
         * Checks for [CorrelationId], then validates the id is not used on left,
         * and finally checks if id is actually used on right.
         *
         * @return true if a correlate id is present and used
         *
         * @throws IllegalArgumentException if the [CorrelationId] is used by left side or if the a
         * [CorrelationId] is present and the [JoinRelType] is FULL or RIGHT.
         */
        private fun checkIfCorrelated(
            variablesSet: Set<CorrelationId>,
            joinType: JoinRelType, leftNode: RelNode, rightRel: RelNode
        ): Boolean {
            if (variablesSet.size() !== 1) {
                return false
            }
            val id: CorrelationId = Iterables.getOnlyElement(variablesSet)
            if (!RelOptUtil.notContainsCorrelation(leftNode, id, Litmus.IGNORE)) {
                throw IllegalArgumentException(
                    "variable " + id
                            + " must not be used by left input to correlation"
                )
            }
            return when (joinType) {
                RIGHT, FULL -> throw IllegalArgumentException("Correlated $joinType join is not supported")
                else -> !RelOptUtil.correlationColumns(
                    Iterables.getOnlyElement(variablesSet),
                    rightRel
                ).isEmpty()
            }
        }
    }
}
