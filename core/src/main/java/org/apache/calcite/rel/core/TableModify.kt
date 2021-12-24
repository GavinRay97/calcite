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
package org.apache.calcite.rel.core

import org.apache.calcite.plan.RelOptCluster

/**
 * Relational expression that modifies a table.
 *
 *
 * It is similar to [org.apache.calcite.rel.core.TableScan],
 * but represents a request to modify a table rather than read from it.
 * It takes one child which produces the modified rows. Those rows are:
 *
 *
 *  * For `INSERT`, those rows are the new values;
 *  * for `DELETE`, the old values;
 *  * for `UPDATE`, all old values plus updated new values.
 *
 */
abstract class TableModify protected constructor(
    cluster: RelOptCluster,
    traitSet: RelTraitSet?,
    table: RelOptTable,
    catalogReader: CatalogReader,
    input: RelNode?,
    operation: Operation,
    @Nullable updateColumnList: List<String>?,
    @Nullable sourceExpressionList: List<RexNode>?,
    flattened: Boolean
) : SingleRel(cluster, traitSet, input) {
    //~ Enums ------------------------------------------------------------------
    /**
     * Enumeration of supported modification operations.
     */
    enum class Operation {
        INSERT, UPDATE, DELETE, MERGE
    }
    //~ Instance fields --------------------------------------------------------
    /**
     * The connection to the optimizing session.
     */
    protected var catalogReader: CatalogReader

    /**
     * The table definition.
     */
    protected val table: RelOptTable
    val operation: Operation

    @get:Nullable
    @Nullable
    val updateColumnList: List<String>?

    @Nullable
    private val sourceExpressionList: List<RexNode>?

    @MonotonicNonNull
    private var inputRowType: RelDataType? = null
    val isFlattened: Boolean
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a `TableModify`.
     *
     *
     * The UPDATE operation has format like this:
     * <blockquote>
     * <pre>UPDATE table SET iden1 = exp1, ident2 = exp2  WHERE condition</pre>
    </blockquote> *
     *
     * @param cluster    Cluster this relational expression belongs to
     * @param traitSet   Traits of this relational expression
     * @param table      Target table to modify
     * @param catalogReader accessor to the table metadata.
     * @param input      Sub-query or filter condition
     * @param operation  Modify operation (INSERT, UPDATE, DELETE)
     * @param updateColumnList List of column identifiers to be updated
     * (e.g. ident1, ident2); null if not UPDATE
     * @param sourceExpressionList List of value expressions to be set
     * (e.g. exp1, exp2); null if not UPDATE
     * @param flattened Whether set flattens the input row type
     */
    init {
        this.table = table
        this.catalogReader = catalogReader
        this.operation = operation
        this.updateColumnList = updateColumnList
        this.sourceExpressionList = sourceExpressionList
        if (operation == Operation.UPDATE) {
            requireNonNull(updateColumnList, "updateColumnList")
            requireNonNull(sourceExpressionList, "sourceExpressionList")
            Preconditions.checkArgument(
                sourceExpressionList!!.size()
                        === updateColumnList.size()
            )
        } else {
            if (operation == Operation.MERGE) {
                requireNonNull(updateColumnList, "updateColumnList")
            } else {
                Preconditions.checkArgument(updateColumnList == null)
            }
            Preconditions.checkArgument(sourceExpressionList == null)
        }
        val relOptSchema: RelOptSchema = table.getRelOptSchema()
        if (relOptSchema != null) {
            cluster.getPlanner().registerSchema(relOptSchema)
        }
        isFlattened = flattened
    }

    /**
     * Creates a TableModify by parsing serialized output.
     */
    protected constructor(input: RelInput) : this(
        input.getCluster(),
        input.getTraitSet(),
        input.getTable("table"),
        requireNonNull(
            input.getTable("table").getRelOptSchema(),
            "relOptSchema"
        ) as CatalogReader,
        input.getInput(),
        requireNonNull(input.getEnum("operation", Operation::class.java), "operation"),
        input.getStringList("updateColumnList"),
        input.getExpressionList("sourceExpressionList"),
        input.getBoolean("flattened", false)
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    fun getCatalogReader(): CatalogReader {
        return catalogReader
    }

    @Override
    fun getTable(): RelOptTable {
        return table
    }

    @Nullable
    fun getSourceExpressionList(): List<RexNode>? {
        return sourceExpressionList
    }

    val isInsert: Boolean
        get() = operation == Operation.INSERT
    val isUpdate: Boolean
        get() = operation == Operation.UPDATE
    val isDelete: Boolean
        get() = operation == Operation.DELETE
    val isMerge: Boolean
        get() = operation == Operation.MERGE

    @Override
    fun deriveRowType(): RelDataType {
        return RelOptUtil.createDmlRowType(
            SqlKind.INSERT, getCluster().getTypeFactory()
        )
    }

    @Override
    fun getExpectedInputRowType(ordinalInParent: Int): RelDataType? {
        assert(ordinalInParent == 0)
        if (inputRowType != null) {
            return inputRowType
        }
        val typeFactory: RelDataTypeFactory = getCluster().getTypeFactory()
        val rowType: RelDataType = table.getRowType()
        inputRowType = when (operation) {
            Operation.UPDATE -> {
                assert(updateColumnList != null) { "updateColumnList must not be null for $operation" }
                typeFactory.createJoinType(
                    rowType,
                    getCatalogReader().createTypeFromProjection(
                        rowType,
                        updateColumnList
                    )
                )
            }
            Operation.MERGE -> {
                assert(updateColumnList != null) { "updateColumnList must not be null for $operation" }
                typeFactory.createJoinType(
                    typeFactory.createJoinType(rowType, rowType),
                    getCatalogReader().createTypeFromProjection(
                        rowType,
                        updateColumnList
                    )
                )
            }
            else -> rowType
        }
        if (isFlattened) {
            inputRowType = SqlTypeUtil.flattenRecordType(
                typeFactory,
                inputRowType,
                null
            )
        }
        return inputRowType
    }

    @Override
    fun explainTerms(pw: RelWriter?): RelWriter {
        return super.explainTerms(pw)
            .item("table", table.getQualifiedName())
            .item("operation", RelEnumTypes.fromEnum(operation))
            .itemIf("updateColumnList", updateColumnList, updateColumnList != null)
            .itemIf(
                "sourceExpressionList", sourceExpressionList,
                sourceExpressionList != null
            )
            .item("flattened", isFlattened)
    }

    @Override
    @Nullable
    fun computeSelfCost(
        planner: RelOptPlanner,
        mq: RelMetadataQuery
    ): RelOptCost {
        // REVIEW jvs 21-Apr-2006:  Just for now...
        val rowCount: Double = mq.getRowCount(this)
        return planner.getCostFactory().makeCost(rowCount, 0, 0)
    }
}
