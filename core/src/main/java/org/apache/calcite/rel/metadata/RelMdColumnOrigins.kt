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
package org.apache.calcite.rel.metadata

import org.apache.calcite.plan.RelOptTable

/**
 * RelMdColumnOrigins supplies a default implementation of
 * [RelMetadataQuery.getColumnOrigins] for the standard logical algebra.
 */
class RelMdColumnOrigins  //~ Constructors -----------------------------------------------------------
private constructor() : MetadataHandler<BuiltInMetadata.ColumnOrigin?> {
    //~ Methods ----------------------------------------------------------------
    @get:Override
    override val def: org.apache.calcite.rel.metadata.MetadataDef<M>
        get() = BuiltInMetadata.ColumnOrigin.DEF

    @Nullable
    fun getColumnOrigins(
        rel: Aggregate,
        mq: RelMetadataQuery, iOutputColumn: Int
    ): Set<RelColumnOrigin> {
        if (iOutputColumn < rel.getGroupCount()) {
            // get actual index of Group columns.
            return mq.getColumnOrigins(rel.getInput(), rel.getGroupSet().asList().get(iOutputColumn))
        }

        // Aggregate columns are derived from input columns
        val call: AggregateCall = rel.getAggCallList().get(
            iOutputColumn
                    - rel.getGroupCount()
        )
        val set: Set<RelColumnOrigin> = HashSet()
        for (iInput in call.getArgList()) {
            var inputSet: Set<RelColumnOrigin>? = mq.getColumnOrigins(rel.getInput(), iInput)
            inputSet = createDerivedColumnOrigins(inputSet)
            if (inputSet != null) {
                set.addAll(inputSet)
            }
        }
        return set
    }

    @Nullable
    fun getColumnOrigins(
        rel: Join, mq: RelMetadataQuery,
        iOutputColumn: Int
    ): Set<RelColumnOrigin>? {
        val nLeftColumns: Int = rel.getLeft().getRowType().getFieldList().size()
        var set: Set<RelColumnOrigin>?
        var derived = false
        if (iOutputColumn < nLeftColumns) {
            set = mq.getColumnOrigins(rel.getLeft(), iOutputColumn)
            if (rel.getJoinType().generatesNullsOnLeft()) {
                derived = true
            }
        } else {
            set = mq.getColumnOrigins(rel.getRight(), iOutputColumn - nLeftColumns)
            if (rel.getJoinType().generatesNullsOnRight()) {
                derived = true
            }
        }
        if (derived) {
            // nulls are generated due to outer join; that counts
            // as derivation
            set = createDerivedColumnOrigins(set)
        }
        return set
    }

    @Nullable
    fun getColumnOrigins(
        rel: SetOp,
        mq: RelMetadataQuery, iOutputColumn: Int
    ): Set<RelColumnOrigin>? {
        val set: Set<RelColumnOrigin> = HashSet()
        for (input in rel.getInputs()) {
            val inputSet: Set<RelColumnOrigin> = mq.getColumnOrigins(input, iOutputColumn) ?: return null
            set.addAll(inputSet)
        }
        return set
    }

    @Nullable
    fun getColumnOrigins(
        rel: Project,
        mq: RelMetadataQuery, iOutputColumn: Int
    ): Set<RelColumnOrigin>? {
        val input: RelNode = rel.getInput()
        val rexNode: RexNode = rel.getProjects().get(iOutputColumn)
        if (rexNode is RexInputRef) {
            // Direct reference:  no derivation added.
            val inputRef: RexInputRef = rexNode as RexInputRef
            return mq.getColumnOrigins(input, inputRef.getIndex())
        }
        // Anything else is a derivation, possibly from multiple columns.
        val set: Set<RelColumnOrigin> = getMultipleColumns(rexNode, input, mq)
        return createDerivedColumnOrigins(set)
    }

    @Nullable
    fun getColumnOrigins(
        rel: Calc,
        mq: RelMetadataQuery, iOutputColumn: Int
    ): Set<RelColumnOrigin>? {
        val input: RelNode = rel.getInput()
        val rexShuttle: RexShuttle = object : RexShuttle() {
            @Override
            fun visitLocalRef(localRef: RexLocalRef?): RexNode {
                return rel.getProgram().expandLocalRef(localRef)
            }
        }
        val projects: List<RexNode> = ArrayList()
        for (rex in rexShuttle.apply(rel.getProgram().getProjectList())) {
            projects.add(rex)
        }
        val rexNode: RexNode = projects[iOutputColumn]
        if (rexNode is RexInputRef) {
            // Direct reference:  no derivation added.
            val inputRef: RexInputRef = rexNode as RexInputRef
            return mq.getColumnOrigins(input, inputRef.getIndex())
        }
        // Anything else is a derivation, possibly from multiple columns.
        val set: Set<RelColumnOrigin> = getMultipleColumns(rexNode, input, mq)
        return createDerivedColumnOrigins(set)
    }

    @Nullable
    fun getColumnOrigins(
        rel: Filter,
        mq: RelMetadataQuery, iOutputColumn: Int
    ): Set<RelColumnOrigin> {
        return mq.getColumnOrigins(rel.getInput(), iOutputColumn)
    }

    @Nullable
    fun getColumnOrigins(
        rel: Sort, mq: RelMetadataQuery,
        iOutputColumn: Int
    ): Set<RelColumnOrigin> {
        return mq.getColumnOrigins(rel.getInput(), iOutputColumn)
    }

    @Nullable
    fun getColumnOrigins(
        rel: TableModify, mq: RelMetadataQuery,
        iOutputColumn: Int
    ): Set<RelColumnOrigin> {
        return mq.getColumnOrigins(rel.getInput(), iOutputColumn)
    }

    @Nullable
    fun getColumnOrigins(
        rel: Exchange,
        mq: RelMetadataQuery, iOutputColumn: Int
    ): Set<RelColumnOrigin> {
        return mq.getColumnOrigins(rel.getInput(), iOutputColumn)
    }

    @Nullable
    fun getColumnOrigins(
        rel: TableFunctionScan,
        mq: RelMetadataQuery, iOutputColumn: Int
    ): Set<RelColumnOrigin>? {
        val set: Set<RelColumnOrigin> = HashSet()
        val mappings: Set<RelColumnMapping> = rel.getColumnMappings()
            ?: return if (rel.getInputs().size() > 0) {
                // This is a non-leaf transformation:  say we don't
                // know about origins, because there are probably
                // columns below.
                null
            } else {
                // This is a leaf transformation: say there are fer sure no
                // column origins.
                set
            }
        for (mapping in mappings) {
            if (mapping.iOutputColumn !== iOutputColumn) {
                continue
            }
            val input: RelNode = rel.getInputs().get(mapping.iInputRel)
            val column: Int = mapping.iInputColumn
            var origins: Set<RelColumnOrigin>? = mq.getColumnOrigins(input, column) ?: return null
            if (mapping.derived) {
                origins = createDerivedColumnOrigins(origins)
            }
            set.addAll(origins)
        }
        return set
    }

    // Catch-all rule when none of the others apply.
    @Nullable
    fun getColumnOrigins(
        rel: RelNode,
        mq: RelMetadataQuery?, iOutputColumn: Int
    ): Set<RelColumnOrigin>? {
        // NOTE jvs 28-Mar-2006: We may get this wrong for a physical table
        // expression which supports projections.  In that case,
        // it's up to the plugin writer to override with the
        // correct information.
        if (rel.getInputs().size() > 0) {
            // No generic logic available for non-leaf rels.
            return null
        }
        val set: Set<RelColumnOrigin> = HashSet()
        val table: RelOptTable = rel.getTable()
            ?: // Somebody is making column values up out of thin air, like a
            // VALUES clause, so we return an empty set.
            return set

        // Detect the case where a physical table expression is performing
        // projection, and say we don't know instead of making any assumptions.
        // (Theoretically we could try to map the projection using column
        // names.)  This detection assumes the table expression doesn't handle
        // rename as well.
        if (table.getRowType() !== rel.getRowType()) {
            return null
        }
        set.add(RelColumnOrigin(table, iOutputColumn, false))
        return set
    }

    companion object {
        val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
            RelMdColumnOrigins(), BuiltInMetadata.ColumnOrigin.Handler::class.java
        )

        @PolyNull
        private fun createDerivedColumnOrigins(
            @PolyNull inputSet: Set<RelColumnOrigin>?
        ): Set<RelColumnOrigin>? {
            if (inputSet == null) {
                return null
            }
            val set: Set<RelColumnOrigin> = HashSet()
            for (rco in inputSet) {
                val derived = RelColumnOrigin(
                    rco.getOriginTable(),
                    rco.getOriginColumnOrdinal(),
                    true
                )
                set.add(derived)
            }
            return set
        }

        private fun getMultipleColumns(
            rexNode: RexNode, input: RelNode,
            mq: RelMetadataQuery
        ): Set<RelColumnOrigin> {
            val set: Set<RelColumnOrigin> = HashSet()
            val visitor: RexVisitor<Void> = object : RexVisitorImpl<Void?>(true) {
                @Override
                fun visitInputRef(inputRef: RexInputRef): Void? {
                    val inputSet: Set<RelColumnOrigin> = mq.getColumnOrigins(input, inputRef.getIndex())
                    if (inputSet != null) {
                        set.addAll(inputSet)
                    }
                    return null
                }
            }
            rexNode.accept(visitor)
            return set
        }
    }
}
