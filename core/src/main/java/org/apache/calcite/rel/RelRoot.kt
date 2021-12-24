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
package org.apache.calcite.rel

import org.apache.calcite.rel.hint.RelHint

/**
 * Root of a tree of [RelNode].
 *
 *
 * One important reason that RelRoot exists is to deal with queries like
 *
 * <blockquote>`SELECT name
 * FROM emp
 * ORDER BY empno DESC`</blockquote>
 *
 *
 * Calcite knows that the result must be sorted, but cannot represent its
 * sort order as a collation, because `empno` is not a field in the
 * result.
 *
 *
 * Instead we represent this as
 *
 * <blockquote>`RelRoot: {
 * rel: Sort($1 DESC)
 * Project(name, empno)
 * TableScan(EMP)
 * fields: [0]
 * collation: [1 DESC]
 * }`</blockquote>
 *
 *
 * Note that the `empno` field is present in the result, but the
 * `fields` mask tells the consumer to throw it away.
 *
 *
 * Another use case is queries like this:
 *
 * <blockquote>`SELECT name AS n, name AS n2, empno AS n
 * FROM emp`</blockquote>
 *
 *
 * The there are multiple uses of the `name` field. and there are
 * multiple columns aliased as `n`. You can represent this as
 *
 * <blockquote>`RelRoot: {
 * rel: Project(name, empno)
 * TableScan(EMP)
 * fields: [(0, "n"), (0, "n2"), (1, "n")]
 * collation: []
 * }`</blockquote>
 */
class RelRoot(
    rel: RelNode, validatedRowType: RelDataType, kind: SqlKind,
    fields: List<Pair<Integer?, String?>?>?, collation: RelCollation?, hints: List<RelHint?>?
) {
    val rel: RelNode
    val validatedRowType: RelDataType
    val kind: SqlKind
    val fields: ImmutableList<Pair<Integer?, String?>?>
    val collation: RelCollation
    val hints: ImmutableList<RelHint?>

    /**
     * Creates a RelRoot.
     *
     * @param validatedRowType Original row type returned by query validator
     * @param kind Type of query (SELECT, UPDATE, ...)
     */
    init {
        this.rel = rel
        this.validatedRowType = validatedRowType
        this.kind = kind
        this.fields = ImmutableList.copyOf(fields)
        this.collation = Objects.requireNonNull(collation, "collation")
        this.hints = ImmutableList.copyOf(hints)
    }

    @Override
    override fun toString(): String {
        return ("Root {kind: " + kind
                + ", rel: " + rel
                + ", rowType: " + validatedRowType
                + ", fields: " + fields
                + ", collation: " + collation + "}")
    }

    /** Creates a copy of this RelRoot, assigning a [RelNode].  */
    fun withRel(rel: RelNode): RelRoot {
        return if (rel === this.rel) {
            this
        } else RelRoot(rel, validatedRowType, kind, fields, collation, hints)
    }

    /** Creates a copy, assigning a new kind.  */
    fun withKind(kind: SqlKind): RelRoot {
        return if (kind === this.kind) {
            this
        } else RelRoot(rel, validatedRowType, kind, fields, collation, hints)
    }

    fun withCollation(collation: RelCollation?): RelRoot {
        return RelRoot(rel, validatedRowType, kind, fields, collation, hints)
    }

    /** Creates a copy, assigning the query hints.  */
    fun withHints(hints: List<RelHint?>?): RelRoot {
        return RelRoot(rel, validatedRowType, kind, fields, collation, hints)
    }

    /** Returns the root relational expression, creating a [LogicalProject]
     * if necessary to remove fields that are not needed.  */
    fun project(): RelNode {
        return project(false)
    }

    /** Returns the root relational expression as a [LogicalProject].
     *
     * @param force Create a Project even if all fields are used
     */
    fun project(force: Boolean): RelNode {
        if (isRefTrivial
            && (SqlKind.DML.contains(kind)
                    || !force
                    || rel is LogicalProject)
        ) {
            return rel
        }
        val projects: List<RexNode> = ArrayList(fields.size())
        val rexBuilder: RexBuilder = rel.getCluster().getRexBuilder()
        for (field in fields) {
            projects.add(rexBuilder.makeInputRef(rel, field.left))
        }
        return LogicalProject.create(rel, hints, projects, Pair.right(fields))
    }

    val isNameTrivial: Boolean
        get() {
            val inputRowType: RelDataType = rel.getRowType()
            return Pair.right(fields).equals(inputRowType.getFieldNames())
        }

    // DML statements return a single count column.
    // The validated type is of the SELECT.
    // Still, we regard the mapping as trivial.
    val isRefTrivial: Boolean
        get() {
            if (SqlKind.DML.contains(kind)) {
                // DML statements return a single count column.
                // The validated type is of the SELECT.
                // Still, we regard the mapping as trivial.
                return true
            }
            val inputRowType: RelDataType = rel.getRowType()
            return Mappings.isIdentity(Pair.left(fields), inputRowType.getFieldCount())
        }
    val isCollationTrivial: Boolean
        get() {
            val collations: List<RelCollation> = rel.getTraitSet()
                .getTraits(RelCollationTraitDef.INSTANCE)
            return collations != null && collations.size() === 1 && collations[0].equals(collation)
        }

    companion object {
        /** Creates a simple RelRoot.  */
        fun of(rel: RelNode, kind: SqlKind): RelRoot {
            return of(rel, rel.getRowType(), kind)
        }

        /** Creates a simple RelRoot.  */
        fun of(rel: RelNode, rowType: RelDataType, kind: SqlKind): RelRoot {
            val refs: ImmutableIntList = ImmutableIntList.identity(rowType.getFieldCount())
            val names: List<String> = rowType.getFieldNames()
            return RelRoot(
                rel, rowType, kind, Pair.zip(refs, names),
                RelCollations.EMPTY, ArrayList()
            )
        }
    }
}
