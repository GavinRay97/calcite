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
package org.apache.calcite.adapter.enumerable

import org.apache.calcite.adapter.java.JavaTypeFactory

/** Implementation of [org.apache.calcite.rel.core.Match] in
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention].  */
class EnumerableMatch
/**
 * Creates an EnumerableMatch.
 *
 *
 * Use [.create] unless you know what you're doing.
 */
    (
    cluster: RelOptCluster?, traitSet: RelTraitSet?,
    input: RelNode?, rowType: RelDataType?, pattern: RexNode?,
    strictStart: Boolean, strictEnd: Boolean,
    patternDefinitions: Map<String?, RexNode?>?, measures: Map<String?, RexNode?>?,
    after: RexNode?, subsets: Map<String?, SortedSet<String?>?>?,
    allRows: Boolean, partitionKeys: ImmutableBitSet?, orderKeys: RelCollation?,
    @Nullable interval: RexNode?
) : Match(
    cluster, traitSet, input, rowType, pattern, strictStart, strictEnd,
    patternDefinitions, measures, after, subsets, allRows, partitionKeys,
    orderKeys, interval
), EnumerableRel {
    @Override
    fun copy(traitSet: RelTraitSet?, inputs: List<RelNode?>): RelNode {
        return EnumerableMatch(
            getCluster(), traitSet, inputs[0], getRowType(),
            pattern, strictStart, strictEnd, patternDefinitions, measures, after,
            subsets, allRows, partitionKeys, orderKeys, interval
        )
    }

    @Override
    fun implement(
        implementor: EnumerableRelImplementor,
        pref: EnumerableRel.Prefer?
    ): EnumerableRel.Result {
        val builder = BlockBuilder()
        val input: EnumerableRel = getInput() as EnumerableRel
        val result: Result = implementor.visitChild(this, 0, input, pref)
        val physType: PhysType = PhysTypeImpl.of(
            implementor.getTypeFactory(), input.getRowType(),
            result.format
        )
        val inputExp: Expression = builder.append("input", result.block)
        val inputPhysType: PhysType = result.physType
        val keyPhysType: PhysType = inputPhysType.project(partitionKeys.asList(), JavaRowFormat.LIST)
        val row_: ParameterExpression = Expressions.parameter(inputPhysType.getJavaRowType(), "row_")
        val keySelector_: Expression = builder.append(
            "keySelector",
            inputPhysType.generateSelector(
                row_,
                partitionKeys.asList(),
                keyPhysType.getFormat()
            )
        )
        val typeBuilder: RelDataTypeFactory.Builder = implementor.getTypeFactory().builder()
        measures.forEach { name, value -> typeBuilder.add(name, value.getType()).nullable(true) }
        val emitType: PhysType = PhysTypeImpl.of(
            implementor.getTypeFactory(), typeBuilder.build(),
            result.format
        )
        val matcher_: Expression = implementMatcher(implementor, physType, builder, row_)
        val emitter_: Expression = implementEmitter(implementor, emitType, physType)
        val visitor = MaxHistoryFutureVisitor()
        patternDefinitions.values().forEach { pd -> pd.accept(visitor) }

        // Fetch
        // Calculate how many steps we need to look back or forward
        val history: Int = visitor.getHistory()
        val future: Int = visitor.getFuture()
        builder.add(
            Expressions.return_(
                null,
                Expressions.call(
                    BuiltInMethod.MATCH.method, inputExp, keySelector_,
                    matcher_, emitter_, Expressions.constant(history),
                    Expressions.constant(future)
                )
            )
        )
        return implementor.result(emitType, builder.toBlock())
    }

    private fun implementEmitter(
        implementor: EnumerableRelImplementor,
        physType: PhysType, inputPhysType: PhysType
    ): Expression {
        val rows_: ParameterExpression =
            Expressions.parameter(Types.of(List::class.java, inputPhysType.getJavaRowType()), "rows")
        val rowStates_: ParameterExpression = Expressions.parameter(List::class.java, "rowStates")
        val symbols_: ParameterExpression = Expressions.parameter(List::class.java, "symbols")
        val match_: ParameterExpression = Expressions.parameter(Int::class.javaPrimitiveType, "match")
        val consumer_: ParameterExpression = Expressions.parameter(Consumer::class.java, "consumer")
        val i_: ParameterExpression = Expressions.parameter(Int::class.javaPrimitiveType, "i")
        val row_: ParameterExpression = Expressions.parameter(inputPhysType.getJavaRowType(), "row")
        val builder2 = BlockBuilder()

        // Add loop variable initialization
        builder2.add(
            Expressions.declare(
                0, row_,
                EnumUtils.convert(
                    Expressions.call(rows_, BuiltInMethod.LIST_GET.method, i_),
                    inputPhysType.getJavaRowType()
                )
            )
        )
        val rexBuilder = RexBuilder(implementor.getTypeFactory())
        val rexProgramBuilder = RexProgramBuilder(inputPhysType.getRowType(), rexBuilder)
        for (entry in measures.entrySet()) {
            rexProgramBuilder.addProject(entry.getValue(), entry.getKey())
        }
        val translator: RexToLixTranslator = RexToLixTranslator.forAggregation(
            getCluster().getTypeFactory() as JavaTypeFactory,
            builder2,
            PassedRowsInputGetter(row_, rows_, inputPhysType),
            implementor.getConformance()
        )
        val result_: ParameterExpression = Expressions.parameter(physType.getJavaRowType())
        builder2.add(
            Expressions.declare(
                Modifier.FINAL, result_,
                Expressions.new_(physType.getJavaRowType())
            )
        )
        Ord.forEach(measures.values()) { measure, i ->
            builder2.add(
                Expressions.statement(
                    Expressions.assign(
                        physType.fieldReference(result_, i),
                        implementMeasure(
                            translator, rows_, symbols_, i_, row_,
                            measure
                        )
                    )
                )
            )
        }
        builder2.add(
            Expressions.statement(
                Expressions.call(
                    consumer_, BuiltInMethod.CONSUMER_ACCEPT.method,
                    result_
                )
            )
        )
        val builder = BlockBuilder()

        // Loop Length

        // we have to use an explicit for (int i = ...) loop, as we need to know later
        // which of the matched rows are already passed (in MatchUtils), so foreach cannot be used
        builder.add(
            Expressions.for_(
                Expressions.declare(0, i_, Expressions.constant(0)),
                Expressions.lessThan(
                    i_,
                    Expressions.call(rows_, BuiltInMethod.COLLECTION_SIZE.method)
                ),
                Expressions.preIncrementAssign(i_),
                builder2.toBlock()
            )
        )
        return Expressions.new_(
            Types.of(Enumerables.Emitter::class.java), NO_EXPRS,
            Expressions.list(
                EnumUtils.overridingMethodDecl(
                    BuiltInMethod.EMITTER_EMIT.method,
                    ImmutableList.of(
                        rows_, rowStates_, symbols_, match_,
                        consumer_
                    ),
                    builder.toBlock()
                )
            )
        )
    }

    private fun implementMatcher(
        implementor: EnumerableRelImplementor,
        physType: PhysType, builder: BlockBuilder, row_: ParameterExpression
    ): Expression {
        val patternBuilder_: Expression = builder.append(
            "patternBuilder",
            Expressions.call(BuiltInMethod.PATTERN_BUILDER.method)
        )
        val automaton_: Expression = builder.append(
            "automaton",
            Expressions.call(
                implementPattern(patternBuilder_, pattern),
                BuiltInMethod.PATTERN_TO_AUTOMATON.method
            )
        )
        var matcherBuilder_: Expression = builder.append(
            "matcherBuilder",
            Expressions.call(BuiltInMethod.MATCHER_BUILDER.method, automaton_)
        )
        val builder2 = BlockBuilder()


        // Wrap a MemoryEnumerable around
        for (entry in patternDefinitions.entrySet()) {
            // Translate REX to Expressions
            val rexBuilder = RexBuilder(implementor.getTypeFactory())
            val rexProgramBuilder = RexProgramBuilder(physType.getRowType(), rexBuilder)
            rexProgramBuilder.addCondition(entry.getValue())
            val inputGetter1: RexToLixTranslator.InputGetter = PrevInputGetter(row_, physType)
            val condition: Expression = RexToLixTranslator
                .translateCondition(
                    rexProgramBuilder.getProgram(),
                    getCluster().getTypeFactory() as JavaTypeFactory,
                    builder2,
                    inputGetter1,
                    implementor.allCorrelateVariables,
                    implementor.getConformance()
                )
            builder2.add(Expressions.return_(null, condition))
            val predicate_: Expression = implementPredicate(physType, row_, builder2.toBlock())
            matcherBuilder_ = Expressions.call(
                matcherBuilder_,
                BuiltInMethod.MATCHER_BUILDER_ADD.method,
                Expressions.constant(entry.getKey()),
                predicate_
            )
        }
        return builder.append(
            "matcher",
            Expressions.call(
                matcherBuilder_,
                BuiltInMethod.MATCHER_BUILDER_BUILD.method
            )
        )
    }

    /**
     * Visitor that finds out how much "history" we need in the past and future.
     */
    private class MaxHistoryFutureVisitor : RexVisitorImpl<Void?>(true) {
        var history = 0
            private set
        var future = 0
            private set

        @Override
        fun visitCall(call: RexCall): Void? {
            call.operands.forEach { o -> o.accept(this) }
            val operand: RexLiteral
            when (call.op.kind) {
                PREV -> {
                    operand = call.getOperands().get(1) as RexLiteral
                    val prev: Int = requireNonNull(
                        operand.getValueAs(Integer::class.java)
                    ) { "operand in $call" }
                    history = Math.max(history, prev)
                }
                NEXT -> {
                    operand = call.getOperands().get(1) as RexLiteral
                    val next: Int = requireNonNull(
                        operand.getValueAs(Integer::class.java)
                    ) { "operand in $call" }
                    future = Math.max(future, next)
                }
                else -> {}
            }
            return null
        }

        @Override
        fun visitSubQuery(subQuery: RexSubQuery?): Void? {
            return null
        }
    }

    /**
     * A special Getter that is able to return a field from a list of objects.
     */
    internal class PassedRowsInputGetter(
        row: ParameterExpression, passedRows: ParameterExpression,
        physType: PhysType
    ) : RexToLixTranslator.InputGetter {
        @Nullable
        private var index: Expression? = null
        private val row: ParameterExpression
        private val passedRows: ParameterExpression
        private val generator: Function<Expression, RexToLixTranslator.InputGetter>
        private val physType: PhysType

        init {
            this.row = row
            this.passedRows = passedRows
            generator = Function<Expression, RexToLixTranslator.InputGetter> { e -> InputGetterImpl(e, physType) }
            this.physType = physType
        }

        fun setIndex(@Nullable index: Expression?) {
            this.index = index
        }

        @Override
        fun field(
            list: BlockBuilder?, index: Int,
            @Nullable storageType: Type?
        ): Expression {
            return if (this.index == null) {
                generator.apply(row).field(list, index, storageType)
            } else Expressions.condition(
                Expressions.greaterThanOrEqual(this.index, Expressions.constant(0)),
                generator.apply(
                    EnumUtils.convert(
                        Expressions.call(
                            passedRows,
                            BuiltInMethod.LIST_GET.method, this.index
                        ),
                        physType.getJavaRowType()
                    )
                )
                    .field(list, index, storageType),
                Expressions.constant(null)
            )
        }
    }

    /**
     * A special Getter that "interchanges" the PREV and the field call.
     */
    internal class PrevInputGetter(row: ParameterExpression, physType: PhysType) : RexToLixTranslator.InputGetter {
        @Nullable
        private var offset: Expression? = null
        private val row: ParameterExpression
        private val generator: Function<Expression, RexToLixTranslator.InputGetter>
        private val physType: PhysType

        init {
            this.row = row
            generator = Function<Expression, RexToLixTranslator.InputGetter> { e -> InputGetterImpl(e, physType) }
            this.physType = physType
        }

        fun setOffset(@Nullable offset: Expression?) {
            this.offset = offset
        }

        @Override
        fun field(
            list: BlockBuilder, index: Int,
            @Nullable storageType: Type?
        ): Expression {
            val row: ParameterExpression = Expressions.parameter(physType.getJavaRowType())
            val tmp: ParameterExpression = Expressions.parameter(Object::class.java)
            list.add(
                Expressions.declare(
                    0, tmp,
                    Expressions.call(
                        this.row, BuiltInMethod.MEMORY_GET1.method,
                        requireNonNull(offset, "offset")
                    )
                )
            )
            list.add(
                Expressions.declare(
                    0, row,
                    Expressions.convert_(tmp, physType.getJavaRowType())
                )
            )

            // Add return statement if here is a null!
            list.add(
                Expressions.ifThen(
                    Expressions.equal(tmp, Expressions.constant(null)),
                    Expressions.return_(null, Expressions.constant(false))
                )
            )
            return generator.apply(row).field(list, index, storageType)
        }
    }

    companion object {
        /** Creates an EnumerableMatch.  */
        fun create(
            input: RelNode, rowType: RelDataType?,
            pattern: RexNode?, strictStart: Boolean, strictEnd: Boolean,
            patternDefinitions: Map<String?, RexNode?>?, measures: Map<String?, RexNode?>?,
            after: RexNode?, subsets: Map<String?, SortedSet<String?>?>?,
            allRows: Boolean, partitionKeys: ImmutableBitSet?, orderKeys: RelCollation?,
            @Nullable interval: RexNode?
        ): EnumerableMatch {
            val cluster: RelOptCluster = input.getCluster()
            val traitSet: RelTraitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE)
            return EnumerableMatch(
                cluster, traitSet, input, rowType, pattern,
                strictStart, strictEnd, patternDefinitions, measures, after, subsets,
                allRows, partitionKeys, orderKeys, interval
            )
        }

        private fun implementMeasure(
            translator: RexToLixTranslator,
            rows_: ParameterExpression, symbols_: ParameterExpression,
            i_: ParameterExpression, row_: ParameterExpression, value: RexNode
        ): Expression {
            val matchFunction: SqlMatchFunction
            val matchImplementor: MatchImplementor
            return when (value.getKind()) {
                LAST, PREV, CLASSIFIER -> {
                    matchFunction = (value as RexCall).getOperator() as SqlMatchFunction
                    matchImplementor = RexImpTable.INSTANCE.get(matchFunction)

                    // Work with the implementor
                    matchImplementor.implement(
                        translator, value as RexCall,
                        row_, rows_, symbols_, i_
                    )
                }
                RUNNING, FINAL -> {
                    // See [CALCITE-3341], this should be changed a bit, to implement
                    // FINAL behavior
                    val operands: List<RexNode> = (value as RexCall).getOperands()
                    assert(operands.size() === 1)
                    when (operands[0].getKind()) {
                        LAST, PREV, CLASSIFIER -> {
                            val call: RexCall = operands[0] as RexCall
                            matchFunction = call.getOperator() as SqlMatchFunction
                            matchImplementor = RexImpTable.INSTANCE.get(matchFunction)
                            // Work with the implementor
                            requireNonNull(
                                translator.inputGetter as PassedRowsInputGetter,
                                "inputGetter"
                            )
                                .setIndex(null)
                            return matchImplementor.implement(
                                translator, call, row_, rows_,
                                symbols_, i_
                            )
                        }
                        else -> {}
                    }
                    translator.translate(operands[0])
                }
                else -> translator.translate(value)
            }
        }

        /** Generates code for a predicate.  */
        private fun implementPredicate(
            physType: PhysType,
            rows_: ParameterExpression, body: BlockStatement
        ): Expression {
            val memberDeclarations: List<MemberDeclaration> = ArrayList()
            val row_: ParameterExpression = Expressions.parameter(
                Types.of(
                    MemoryFactory.Memory::class.java,
                    physType.getJavaRowType()
                ), "row_"
            )
            Expressions.assign(
                row_,
                Expressions.call(rows_, BuiltInMethod.MEMORY_GET0.method)
            )

            // Implement the Predicate here based on the pattern definition

            // Add a predicate method:
            //
            //   public boolean test(E row, List<E> rows) {
            //     return ...;
            //   }
            memberDeclarations.add(
                EnumUtils.overridingMethodDecl(
                    BuiltInMethod.PREDICATE_TEST.method,
                    ImmutableList.of(row_), body
                )
            )
            if (EnumerableRules.BRIDGE_METHODS) {
                // Add a bridge method:
                //
                //   public boolean test(Object row, Object rows) {
                //     return this.test(row, (List) rows);
                //   }
                val row0_: ParameterExpression = Expressions.parameter(Object::class.java, "row")
                @SuppressWarnings("unused") val rowsO_: ParameterExpression =
                    Expressions.parameter(Object::class.java, "rows")
                val bridgeBody = BlockBuilder()
                bridgeBody.add(
                    Expressions.return_(
                        null,
                        Expressions.call(
                            Expressions.parameter(Comparable::class.java, "this"),
                            BuiltInMethod.PREDICATE_TEST.method,
                            Expressions.convert_(
                                row0_,
                                Types.of(
                                    MemoryFactory.Memory::class.java,
                                    physType.getJavaRowType()
                                )
                            )
                        )
                    )
                )
                memberDeclarations.add(
                    EnumUtils.overridingMethodDecl(
                        BuiltInMethod.PREDICATE_TEST.method,
                        ImmutableList.of(row0_), bridgeBody.toBlock()
                    )
                )
            }
            return Expressions.new_(
                Types.of(Predicate::class.java), NO_EXPRS,
                memberDeclarations
            )
        }

        /** Generates code for a pattern.
         *
         *
         * For example, for the pattern `(A B)`, generates
         * `patternBuilder.symbol("A").symbol("B").seq()`.  */
        private fun implementPattern(
            patternBuilder_: Expression,
            pattern: RexNode
        ): Expression {
            var patternBuilder_: Expression = patternBuilder_
            return when (pattern.getKind()) {
                LITERAL -> {
                    val symbol: String = (pattern as RexLiteral).getValueAs(String::class.java)
                    Expressions.call(
                        patternBuilder_,
                        BuiltInMethod.PATTERN_BUILDER_SYMBOL.method,
                        Expressions.constant(symbol)
                    )
                }
                PATTERN_CONCAT -> {
                    val concat: RexCall = pattern as RexCall
                    for (operand in Ord.zip(concat.operands)) {
                        patternBuilder_ = implementPattern(patternBuilder_, operand.e)
                        if (operand.i > 0) {
                            patternBuilder_ = Expressions.call(
                                patternBuilder_,
                                BuiltInMethod.PATTERN_BUILDER_SEQ.method
                            )
                        }
                    }
                    patternBuilder_
                }
                else -> throw AssertionError("unknown kind: $pattern")
            }
        }
    }
}
