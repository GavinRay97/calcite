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
package org.apache.calcite.rel.logical

import org.apache.calcite.plan.Convention

/**
 * A relational expression which computes project expressions and also filters.
 *
 *
 * This relational expression combines the functionality of
 * [LogicalProject] and [LogicalFilter].
 * It should be created in the later
 * stages of optimization, by merging consecutive [LogicalProject] and
 * [LogicalFilter] nodes together.
 *
 *
 * The following rules relate to `LogicalCalc`:
 *
 *
 *  * [FilterToCalcRule] creates this from a [LogicalFilter]
 *  * [ProjectToCalcRule] creates this from a [LogicalProject]
 *  * [org.apache.calcite.rel.rules.FilterCalcMergeRule]
 * merges this with a [LogicalFilter]
 *  * [org.apache.calcite.rel.rules.ProjectCalcMergeRule]
 * merges this with a [LogicalProject]
 *  * [org.apache.calcite.rel.rules.CalcMergeRule]
 * merges two `LogicalCalc`s
 *
 */
class LogicalCalc  //~ Static fields/initializers ---------------------------------------------
//~ Constructors -----------------------------------------------------------
/** Creates a LogicalCalc.  */
    (
    cluster: RelOptCluster?,
    traitSet: RelTraitSet?,
    hints: List<RelHint?>?,
    child: RelNode?,
    program: RexProgram?
) : Calc(cluster, traitSet, hints, child, program) {
    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster?,
        traitSet: RelTraitSet?,
        child: RelNode?,
        program: RexProgram?
    ) : this(cluster, traitSet, ImmutableList.of(), child, program) {
    }

    /**
     * Creates a LogicalCalc by parsing serialized output.
     */
    constructor(input: RelInput) : this(
        input.getCluster(),
        input.getTraitSet(),
        ImmutableList.of(),
        input.getInput(),
        RexProgram.create(input)
    ) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster?,
        traitSet: RelTraitSet?,
        child: RelNode?,
        program: RexProgram?,
        collationList: List<RelCollation?>?
    ) : this(cluster, traitSet, ImmutableList.of(), child, program) {
        Util.discard(collationList)
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun copy(
        traitSet: RelTraitSet?, child: RelNode?,
        program: RexProgram?
    ): LogicalCalc {
        return LogicalCalc(getCluster(), traitSet, hints, child, program)
    }

    @Override
    fun collectVariablesUsed(variableSet: Set<CorrelationId?>) {
        val vuv: RelOptUtil.VariableUsedVisitor = VariableUsedVisitor(null)
        vuv.visitEach(program.getExprList())
        variableSet.addAll(vuv.variables)
    }

    @Override
    fun withHints(hintList: List<RelHint?>?): RelNode {
        return LogicalCalc(
            getCluster(), traitSet,
            ImmutableList.copyOf(hintList), input, program
        )
    }

    companion object {
        fun create(
            input: RelNode,
            program: RexProgram?
        ): LogicalCalc {
            val cluster: RelOptCluster = input.getCluster()
            val mq: RelMetadataQuery = cluster.getMetadataQuery()
            val traitSet: RelTraitSet = cluster.traitSet()
                .replace(Convention.NONE)
                .replaceIfs(
                    RelCollationTraitDef.INSTANCE
                ) { RelMdCollation.calc(mq, input, program) }
                .replaceIf(
                    RelDistributionTraitDef.INSTANCE
                ) { RelMdDistribution.calc(mq, input, program) }
            return LogicalCalc(cluster, traitSet, ImmutableList.of(), input, program)
        }
    }
}
