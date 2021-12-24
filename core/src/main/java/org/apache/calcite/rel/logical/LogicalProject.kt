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
 * Sub-class of [org.apache.calcite.rel.core.Project] not
 * targeted at any particular engine or calling convention.
 */
class LogicalProject : Project {
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a LogicalProject.
     *
     *
     * Use [.create] unless you know what you're doing.
     *
     * @param cluster  Cluster this relational expression belongs to
     * @param traitSet Traits of this relational expression
     * @param hints    Hints of this relational expression
     * @param input    Input relational expression
     * @param projects List of expressions for the input columns
     * @param rowType  Output row type
     */
    constructor(
        cluster: RelOptCluster?,
        traitSet: RelTraitSet,
        hints: List<RelHint?>?,
        input: RelNode?,
        projects: List<RexNode?>?,
        rowType: RelDataType?
    ) : super(cluster, traitSet, hints, input, projects, rowType) {
        assert(traitSet.containsIfApplicable(Convention.NONE))
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster?, traitSet: RelTraitSet?,
        input: RelNode?, projects: List<RexNode?>?, rowType: RelDataType?
    ) : this(cluster, traitSet, ImmutableList.of(), input, projects, rowType) {
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster?, traitSet: RelTraitSet?,
        input: RelNode?, projects: List<RexNode?>?, rowType: RelDataType?,
        flags: Int
    ) : this(cluster, traitSet, ImmutableList.of(), input, projects, rowType) {
        Util.discard(flags)
    }

    @Deprecated // to be removed before 2.0
    constructor(
        cluster: RelOptCluster, input: RelNode?,
        projects: List<RexNode?>?, @Nullable fieldNames: List<String?>?, flags: Int
    ) : this(
        cluster, cluster.traitSetOf(RelCollations.EMPTY),
        ImmutableList.of(), input, projects,
        RexUtil.createStructType(
            cluster.getTypeFactory(), projects,
            fieldNames, null
        )
    ) {
        Util.discard(flags)
    }

    /**
     * Creates a LogicalProject by parsing serialized output.
     */
    constructor(input: RelInput?) : super(input) {}

    @Override
    fun copy(
        traitSet: RelTraitSet?, input: RelNode?,
        projects: List<RexNode?>?, rowType: RelDataType?
    ): LogicalProject {
        return LogicalProject(getCluster(), traitSet, hints, input, projects, rowType)
    }

    @Override
    fun accept(shuttle: RelShuttle): RelNode {
        return shuttle.visit(this)
    }

    @Override
    fun withHints(hintList: List<RelHint?>?): RelNode {
        return LogicalProject(
            getCluster(), traitSet, hintList,
            input, getProjects(), getRowType()
        )
    }

    @Override
    fun deepEquals(@Nullable obj: Object?): Boolean {
        return deepEquals0(obj)
    }

    @Override
    fun deepHashCode(): Int {
        return deepHashCode0()
    }

    companion object {
        //~ Methods ----------------------------------------------------------------
        /** Creates a LogicalProject.  */
        fun create(
            input: RelNode, hints: List<RelHint?>?,
            projects: List<RexNode?>?,
            @Nullable fieldNames: List<String?>?
        ): LogicalProject {
            val cluster: RelOptCluster = input.getCluster()
            val rowType: RelDataType = RexUtil.createStructType(
                cluster.getTypeFactory(), projects,
                fieldNames, SqlValidatorUtil.F_SUGGESTER
            )
            return create(input, hints, projects, rowType)
        }

        /** Creates a LogicalProject, specifying row type rather than field names.  */
        fun create(
            input: RelNode, hints: List<RelHint?>?,
            projects: List<RexNode?>?, rowType: RelDataType?
        ): LogicalProject {
            val cluster: RelOptCluster = input.getCluster()
            val mq: RelMetadataQuery = cluster.getMetadataQuery()
            val traitSet: RelTraitSet = cluster.traitSet().replace(Convention.NONE)
                .replaceIfs(
                    RelCollationTraitDef.INSTANCE
                ) { RelMdCollation.project(mq, input, projects) }
            return LogicalProject(cluster, traitSet, hints, input, projects, rowType)
        }
    }
}
