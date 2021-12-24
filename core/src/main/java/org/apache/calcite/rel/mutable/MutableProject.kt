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
package org.apache.calcite.rel.mutable

import org.apache.calcite.rel.core.Project

/** Mutable equivalent of [org.apache.calcite.rel.core.Project].  */
class MutableProject private constructor(
    rowType: RelDataType, input: MutableRel,
    projects: List<RexNode>
) : MutableSingleRel(MutableRelType.PROJECT, rowType, input) {
    val projects: List<RexNode>

    init {
        this.projects = projects
        assert(RexUtil.compatibleTypes(projects, rowType, Litmus.THROW))
    }

    @Override
    override fun equals(@Nullable obj: Object): Boolean {
        return (obj === this
                || (obj is MutableProject
                && MutableRel.PAIRWISE_STRING_EQUIVALENCE.equivalent(
            projects, (obj as MutableProject).projects
        )
                && input!!.equals((obj as MutableProject).input)))
    }

    @Override
    override fun hashCode(): Int {
        return Objects.hash(
            input,
            MutableRel.PAIRWISE_STRING_EQUIVALENCE.hash(projects)
        )
    }

    @Override
    override fun digest(buf: StringBuilder): StringBuilder {
        return buf.append("Project(projects: ").append(projects).append(")")
    }

    /** Returns a list of (expression, name) pairs.  */
    val namedProjects: List<Any>
        get() = Pair.zip(projects, rowType.getFieldNames())
    val mapping: @Nullable Mappings.TargetMapping?
        get() = Project.getMapping(input!!.rowType.getFieldCount(), projects)

    @Override
    override fun clone(): MutableRel {
        return of(rowType, input!!.clone(), projects)
    }

    companion object {
        /**
         * Creates a MutableProject.
         *
         * @param rowType   Row type
         * @param input     Input relational expression
         * @param projects  List of expressions for the input columns
         */
        fun of(
            rowType: RelDataType, input: MutableRel,
            projects: List<RexNode>
        ): MutableProject {
            return MutableProject(rowType, input, projects)
        }

        /**
         * Creates a MutableProject.
         *
         * @param input         Input relational expression
         * @param exprList      List of expressions for the input columns
         * @param fieldNameList Aliases of the expressions, or null to generate
         */
        fun of(
            input: MutableRel, exprList: List<RexNode>,
            fieldNameList: List<String?>?
        ): MutableRel {
            val rowType: RelDataType = RexUtil.createStructType(
                input.cluster.getTypeFactory(), exprList,
                fieldNameList, SqlValidatorUtil.F_SUGGESTER
            )
            return of(rowType, input, exprList)
        }
    }
}
