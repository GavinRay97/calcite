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

import org.apache.calcite.DataContext

/** Implementation of [org.apache.calcite.rel.core.Calc] in
 * [enumerable calling convention][org.apache.calcite.adapter.enumerable.EnumerableConvention].  */
class EnumerableCalc(
    cluster: RelOptCluster?,
    traitSet: RelTraitSet?,
    input: RelNode?,
    program: RexProgram
) : Calc(cluster, traitSet, ImmutableList.of(), input, program), EnumerableRel {
    /**
     * Creates an EnumerableCalc.
     *
     *
     * Use [.create] unless you know what you're doing.
     */
    init {
        assert(getConvention() is EnumerableConvention)
        assert(!program.containsAggs())
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster?,
        traitSet: RelTraitSet?,
        input: RelNode?,
        program: RexProgram,
        collationList: List<RelCollation?>?
    ) : this(cluster, traitSet, input, program) {
        Util.discard(collationList)
    }

    @Override
    fun copy(
        traitSet: RelTraitSet?, child: RelNode?,
        program: RexProgram
    ): EnumerableCalc {
        // we do not need to copy program; it is immutable
        return EnumerableCalc(getCluster(), traitSet, child, program)
    }

    @Override
    fun implement(implementor: EnumerableRelImplementor, pref: Prefer): Result {
        val typeFactory: JavaTypeFactory = implementor.getTypeFactory()
        val builder = BlockBuilder()
        val child: EnumerableRel = getInput() as EnumerableRel
        val result: Result = implementor.visitChild(this, 0, child, pref)
        val physType: PhysType = PhysTypeImpl.of(
            typeFactory, getRowType(), pref.prefer(result.format)
        )

        // final Enumerable<Employee> inputEnumerable = <<child adapter>>;
        // return new Enumerable<IntString>() {
        //     Enumerator<IntString> enumerator() {
        //         return new Enumerator<IntString>() {
        //             public void reset() {
        // ...
        val outputJavaType: Type = physType.getJavaRowType()
        val enumeratorType: Type = Types.of(
            Enumerator::class.java, outputJavaType
        )
        val inputJavaType: Type = result.physType.getJavaRowType()
        val inputEnumerator: ParameterExpression = Expressions.parameter(
            Types.of(
                Enumerator::class.java, inputJavaType
            ),
            "inputEnumerator"
        )
        val input: Expression = EnumUtils.convert(
            Expressions.call(
                inputEnumerator,
                BuiltInMethod.ENUMERATOR_CURRENT.method
            ),
            inputJavaType
        )
        val rexBuilder: RexBuilder = getCluster().getRexBuilder()
        val mq: RelMetadataQuery = getCluster().getMetadataQuery()
        val predicates: RelOptPredicateList = mq.getPulledUpPredicates(child)
        val simplify = RexSimplify(rexBuilder, predicates, RexUtil.EXECUTOR)
        val program: RexProgram = program.normalize(rexBuilder, simplify)
        val moveNextBody: BlockStatement
        moveNextBody = if (program.getCondition() == null) {
            Blocks.toFunctionBlock(
                Expressions.call(
                    inputEnumerator,
                    BuiltInMethod.ENUMERATOR_MOVE_NEXT.method
                )
            )
        } else {
            val builder2 = BlockBuilder()
            val condition: Expression = RexToLixTranslator.translateCondition(
                program,
                typeFactory,
                builder2,
                InputGetterImpl(input, result.physType),
                implementor.allCorrelateVariables, implementor.getConformance()
            )
            builder2.add(
                Expressions.ifThen(
                    condition,
                    Expressions.return_(
                        null, Expressions.constant(true)
                    )
                )
            )
            Expressions.block(
                Expressions.while_(
                    Expressions.call(
                        inputEnumerator,
                        BuiltInMethod.ENUMERATOR_MOVE_NEXT.method
                    ),
                    builder2.toBlock()
                ),
                Expressions.return_(
                    null,
                    Expressions.constant(false)
                )
            )
        }
        val builder3 = BlockBuilder()
        val conformance: SqlConformance = implementor.map.getOrDefault(
            "_conformance",
            SqlConformanceEnum.DEFAULT
        ) as SqlConformance
        val expressions: List<Expression> = RexToLixTranslator.translateProjects(
            program,
            typeFactory,
            conformance,
            builder3,
            null,
            physType,
            DataContext.ROOT,
            InputGetterImpl(input, result.physType),
            implementor.allCorrelateVariables
        )
        builder3.add(
            Expressions.return_(
                null, physType.record(expressions)
            )
        )
        val currentBody: BlockStatement = builder3.toBlock()
        val inputEnumerable: Expression = builder.append(
            "inputEnumerable", result.block, false
        )
        val body: Expression = Expressions.new_(
            enumeratorType,
            NO_EXPRS,
            Expressions.list(
                Expressions.fieldDecl(
                    Modifier.PUBLIC
                            or Modifier.FINAL,
                    inputEnumerator,
                    Expressions.call(
                        inputEnumerable,
                        BuiltInMethod.ENUMERABLE_ENUMERATOR.method
                    )
                ),
                EnumUtils.overridingMethodDecl(
                    BuiltInMethod.ENUMERATOR_RESET.method,
                    NO_PARAMS,
                    Blocks.toFunctionBlock(
                        Expressions.call(
                            inputEnumerator,
                            BuiltInMethod.ENUMERATOR_RESET.method
                        )
                    )
                ),
                EnumUtils.overridingMethodDecl(
                    BuiltInMethod.ENUMERATOR_MOVE_NEXT.method,
                    NO_PARAMS,
                    moveNextBody
                ),
                EnumUtils.overridingMethodDecl(
                    BuiltInMethod.ENUMERATOR_CLOSE.method,
                    NO_PARAMS,
                    Blocks.toFunctionBlock(
                        Expressions.call(
                            inputEnumerator,
                            BuiltInMethod.ENUMERATOR_CLOSE.method
                        )
                    )
                ),
                Expressions.methodDecl(
                    Modifier.PUBLIC,
                    if (BRIDGE_METHODS) Object::class.java else outputJavaType,
                    "current",
                    NO_PARAMS,
                    currentBody
                )
            )
        )
        builder.add(
            Expressions.return_(
                null,
                Expressions.new_(
                    BuiltInMethod.ABSTRACT_ENUMERABLE_CTOR.constructor,  // TODO: generics
                    //   Collections.singletonList(inputRowType),
                    NO_EXPRS,
                    ImmutableList.< MemberDeclaration > of < MemberDeclaration ? > Expressions.methodDecl(
                        Modifier.PUBLIC,
                        enumeratorType,
                        BuiltInMethod.ENUMERABLE_ENUMERATOR.method.getName(),
                        NO_PARAMS,
                        Blocks.toFunctionBlock(body)
                    )
                )
            )
        )
        return implementor.result(physType, builder.toBlock())
    }

    @Override
    @Nullable
    override fun passThroughTraits(
        required: RelTraitSet?
    ): Pair<RelTraitSet, List<RelTraitSet>> {
        val exps: List<RexNode> = Util.transform(
            program.getProjectList(),
            program::expandLocalRef
        )
        return EnumerableTraitsUtils.passThroughTraitsForProject(
            required, exps,
            input.getRowType(), input.getCluster().getTypeFactory(), traitSet
        )
    }

    @Override
    @Nullable
    override fun deriveTraits(
        childTraits: RelTraitSet?, childId: Int
    ): Pair<RelTraitSet, List<RelTraitSet>> {
        val exps: List<RexNode> = Util.transform(
            program.getProjectList(),
            program::expandLocalRef
        )
        return EnumerableTraitsUtils.deriveTraitsForProject(
            childTraits, childId, exps,
            input.getRowType(), input.getCluster().getTypeFactory(), traitSet
        )
    }

    @get:Override
    val program: RexProgram

    companion object {
        /** Creates an EnumerableCalc.  */
        fun create(
            input: RelNode,
            program: RexProgram
        ): EnumerableCalc {
            val cluster: RelOptCluster = input.getCluster()
            val mq: RelMetadataQuery = cluster.getMetadataQuery()
            val traitSet: RelTraitSet = cluster.traitSet()
                .replace(EnumerableConvention.INSTANCE)
                .replaceIfs(
                    RelCollationTraitDef.INSTANCE
                ) { RelMdCollation.calc(mq, input, program) }
                .replaceIf(
                    RelDistributionTraitDef.INSTANCE
                ) { RelMdDistribution.calc(mq, input, program) }
            return EnumerableCalc(cluster, traitSet, input, program)
        }
    }
}
