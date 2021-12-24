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
 * Planner rule that pushes
 * a [semi-join][Join.isSemiJoin] down in a tree past
 * a [org.apache.calcite.rel.core.Project].
 *
 *
 * The intention is to trigger other rules that will convert
 * `SemiJoin`s.
 *
 *
 * SemiJoin(LogicalProject(X), Y)  LogicalProject(SemiJoin(X, Y))
 *
 * @see org.apache.calcite.rel.rules.SemiJoinFilterTransposeRule
 */
@Value.Enclosing
class SemiJoinProjectTransposeRule
/** Creates a SemiJoinProjectTransposeRule.  */
protected constructor(config: Config?) : RelRule<SemiJoinProjectTransposeRule.Config?>(config), TransformationRule {
    //~ Methods ----------------------------------------------------------------
    @Override
    fun onMatch(call: RelOptRuleCall) {
        val semiJoin: Join = call.rel(0)
        val project: Project = call.rel(1)

        // Convert the LHS semi-join keys to reference the child projection
        // expression; all projection expressions must be RexInputRefs,
        // otherwise, we wouldn't have created this semi-join.

        // convert the semijoin condition to reflect the LHS with the project
        // pulled up
        val newCondition: RexNode = adjustCondition(project, semiJoin)
        val newSemiJoin: LogicalJoin = LogicalJoin.create(
            project.getInput(),
            semiJoin.getRight(),  // No need to copy the hints, the framework would try to do that.
            ImmutableList.of(),
            newCondition,
            ImmutableSet.of(), JoinRelType.SEMI
        )

        // Create the new projection.  Note that the projection expressions
        // are the same as the original because they only reference the LHS
        // of the semijoin and the semijoin only projects out the LHS
        val relBuilder: RelBuilder = call.builder()
        relBuilder.push(newSemiJoin)
        relBuilder.project(project.getProjects(), project.getRowType().getFieldNames())
        call.transformTo(relBuilder.build())
    }

    /** Rule configuration.  */
    @Value.Immutable
    interface Config : RelRule.Config {
        @Override
        fun toRule(): SemiJoinProjectTransposeRule? {
            return SemiJoinProjectTransposeRule(this)
        }

        /** Defines an operand tree for the given classes.  */
        fun withOperandFor(
            joinClass: Class<out Join?>?,
            projectClass: Class<out Project?>?
        ): Config? {
            return withOperandSupplier { b ->
                b.operand(joinClass).predicate(Join::isSemiJoin).inputs { b2 -> b2.operand(projectClass).anyInputs() }
            }
                .`as`(Config::class.java)
        }

        companion object {
            val DEFAULT: Config = ImmutableSemiJoinProjectTransposeRule.Config.of()
                .withOperandFor(LogicalJoin::class.java, LogicalProject::class.java)
        }
    }

    companion object {
        /**
         * Pulls the project above the semijoin and returns the resulting semijoin
         * condition. As a result, the semijoin condition should be modified such
         * that references to the LHS of a semijoin should now reference the
         * children of the project that's on the LHS.
         *
         * @param project  LogicalProject on the LHS of the semijoin
         * @param semiJoin the semijoin
         * @return the modified semijoin condition
         */
        private fun adjustCondition(project: Project, semiJoin: Join): RexNode {
            // create two RexPrograms -- the bottom one representing a
            // concatenation of the project and the RHS of the semijoin and the
            // top one representing the semijoin condition
            val rexBuilder: RexBuilder = project.getCluster().getRexBuilder()
            val typeFactory: RelDataTypeFactory = rexBuilder.getTypeFactory()
            val rightChild: RelNode = semiJoin.getRight()

            // for the bottom RexProgram, the input is a concatenation of the
            // child of the project and the RHS of the semijoin
            val bottomInputRowType: RelDataType = SqlValidatorUtil.deriveJoinRowType(
                project.getInput().getRowType(),
                rightChild.getRowType(),
                JoinRelType.INNER,
                typeFactory,
                null,
                semiJoin.getSystemFieldList()
            )
            val bottomProgramBuilder = RexProgramBuilder(bottomInputRowType, rexBuilder)

            // add the project expressions, then add input references for the RHS
            // of the semijoin
            for (pair in project.getNamedProjects()) {
                bottomProgramBuilder.addProject(pair.left, pair.right)
            }
            val nLeftFields: Int = project.getInput().getRowType().getFieldCount()
            val rightFields: List<RelDataTypeField> = rightChild.getRowType().getFieldList()
            val nRightFields: Int = rightFields.size()
            for (i in 0 until nRightFields) {
                val field: RelDataTypeField = rightFields[i]
                val inputRef: RexNode = rexBuilder.makeInputRef(
                    field.getType(), i + nLeftFields
                )
                bottomProgramBuilder.addProject(inputRef, field.getName())
            }
            val bottomProgram: RexProgram = bottomProgramBuilder.getProgram()

            // input rowtype into the top program is the concatenation of the
            // project and the RHS of the semijoin
            val topInputRowType: RelDataType = SqlValidatorUtil.deriveJoinRowType(
                project.getRowType(),
                rightChild.getRowType(),
                JoinRelType.INNER,
                typeFactory,
                null,
                semiJoin.getSystemFieldList()
            )
            val topProgramBuilder = RexProgramBuilder(
                topInputRowType,
                rexBuilder
            )
            topProgramBuilder.addIdentity()
            topProgramBuilder.addCondition(semiJoin.getCondition())
            val topProgram: RexProgram = topProgramBuilder.getProgram()

            // merge the programs and expand out the local references to form
            // the new semijoin condition; it now references a concatenation of
            // the project's child and the RHS of the semijoin
            val mergedProgram: RexProgram = RexProgramBuilder.mergePrograms(
                topProgram,
                bottomProgram,
                rexBuilder
            )
            return mergedProgram.expandLocalRef(
                requireNonNull(
                    mergedProgram.getCondition()
                ) { "mergedProgram.getCondition() for $mergedProgram" })
        }
    }
}
