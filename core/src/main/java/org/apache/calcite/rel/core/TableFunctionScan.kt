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

import org.apache.calcite.linq4j.Ord

/**
 * Relational expression that calls a table-valued function.
 *
 *
 * The function returns a result set.
 * It can appear as a leaf in a query tree,
 * or can be applied to relational inputs.
 *
 * @see org.apache.calcite.rel.logical.LogicalTableFunctionScan
 */
abstract class TableFunctionScan protected constructor(
    cluster: RelOptCluster?,
    traitSet: RelTraitSet?,
    inputs: List<RelNode?>?,
    rexCall: RexNode,
    @Nullable elementType: Type?,
    rowType: RelDataType,
    @Nullable columnMappings: Set<RelColumnMapping?>?
) : AbstractRelNode(cluster, traitSet) {
    //~ Instance fields --------------------------------------------------------
    private val rexCall: RexNode

    @Nullable
    private val elementType: Type?
    private var inputs: ImmutableList<RelNode?>

    @Nullable
    protected val columnMappings: ImmutableSet<RelColumnMapping?>
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a `TableFunctionScan`.
     *
     * @param cluster        Cluster that this relational expression belongs to
     * @param inputs         0 or more relational inputs
     * @param traitSet       Trait set
     * @param rexCall        Function invocation expression
     * @param elementType    Element type of the collection that will implement
     * this table
     * @param rowType        Row type produced by function
     * @param columnMappings Column mappings associated with this function
     */
    init {
        this.rexCall = rexCall
        this.elementType = elementType
        rowType = rowType
        this.inputs = ImmutableList.copyOf(inputs)
        this.columnMappings = if (columnMappings == null) null else ImmutableSet.copyOf(columnMappings)
    }

    /**
     * Creates a TableFunctionScan by parsing serialized output.
     */
    protected constructor(input: RelInput) : this(
        input.getCluster(), input.getTraitSet(), input.getInputs(),
        requireNonNull(input.getExpression("invocation"), "invocation"),
        input.get("elementType") as Type,
        input.getRowType("rowType"),
        ImmutableSet.of()
    ) {
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun copy(
        traitSet: RelTraitSet?,
        inputs: List<RelNode?>?
    ): TableFunctionScan {
        return copy(
            traitSet, inputs, rexCall, elementType, getRowType(),
            columnMappings
        )
    }

    /**
     * Copies this relational expression, substituting traits and
     * inputs.
     *
     * @param traitSet       Traits
     * @param inputs         0 or more relational inputs
     * @param rexCall        Function invocation expression
     * @param elementType    Element type of the collection that will implement
     * this table
     * @param rowType        Row type produced by function
     * @param columnMappings Column mappings associated with this function
     * @return Copy of this relational expression, substituting traits and
     * inputs
     */
    abstract fun copy(
        traitSet: RelTraitSet?,
        inputs: List<RelNode?>?,
        rexCall: RexNode?,
        @Nullable elementType: Type?,
        rowType: RelDataType?,
        @Nullable columnMappings: Set<RelColumnMapping?>?
    ): TableFunctionScan

    @Override
    fun getInputs(): List<RelNode?> {
        return inputs
    }

    @Override
    fun accept(shuttle: RexShuttle): RelNode {
        val rexCall: RexNode = shuttle.apply(rexCall)
        return if (rexCall === this.rexCall) {
            this
        } else copy(
            traitSet, inputs, rexCall, elementType, getRowType(),
            columnMappings
        )
    }

    @Override
    fun replaceInput(ordinalInParent: Int, p: RelNode?) {
        val newInputs: List<RelNode> = ArrayList(inputs)
        newInputs.set(ordinalInParent, p)
        inputs = ImmutableList.copyOf(newInputs)
        recomputeDigest()
    }

    @Override
    fun estimateRowCount(mq: RelMetadataQuery): Double {
        // Calculate result as the sum of the input row count estimates,
        // assuming there are any, otherwise use the superclass default.  So
        // for a no-input UDX, behave like an AbstractRelNode; for a one-input
        // UDX, behave like a SingleRel; for a multi-input UDX, behave like
        // UNION ALL.  TODO jvs 10-Sep-2007: UDX-supplied costing metadata.
        if (inputs.size() === 0) {
            return super.estimateRowCount(mq)
        }
        var nRows = 0.0
        for (input in inputs) {
            val d: Double = mq.getRowCount(input)
            if (d != null) {
                nRows += d
            }
        }
        return nRows
    }

    /**
     * Returns function invocation expression.
     *
     *
     * Within this rexCall, instances of
     * [org.apache.calcite.rex.RexInputRef] refer to entire input
     * [org.apache.calcite.rel.RelNode]s rather than their fields.
     *
     * @return function invocation expression
     */
    val call: RexNode
        get() = rexCall

    @Override
    fun explainTerms(pw: RelWriter): RelWriter {
        super.explainTerms(pw)
        for (ord in Ord.zip(inputs)) {
            pw.input("input#" + ord.i, ord.e)
        }
        pw.item("invocation", rexCall)
            .item("rowType", rowType)
        if (elementType != null) {
            pw.item("elementType", elementType)
        }
        return pw
    }

    /**
     * Returns set of mappings known for this table function, or null if unknown
     * (not the same as empty!).
     *
     * @return set of mappings known for this table function, or null if unknown
     * (not the same as empty!)
     */
    @Nullable
    fun getColumnMappings(): Set<RelColumnMapping?> {
        return columnMappings
    }

    /**
     * Returns element type of the collection that will implement this table.
     *
     * @return element type of the collection that will implement this table
     */
    @Nullable
    fun getElementType(): Type? {
        return elementType
    }
}
