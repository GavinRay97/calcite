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
package org.apache.calcite.rex

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.util.Litmus
import java.util.List
import java.util.Objects.requireNonNull

/**
 * Visitor which checks the validity of a [RexNode] expression.
 *
 *
 * There are two modes of operation:
 *
 *
 *  * Use`fail=true` to throw an [AssertionError] as soon as
 * an invalid node is detected:
 *
 * <blockquote>`RexNode node;<br></br>
 * RelDataType rowType;<br></br>
 * assert new RexChecker(rowType, true).isValid(node);`</blockquote>
 *
 * This mode requires that assertions are enabled.
 *
 *  * Use `fail=false` to test for validity without throwing an
 * error.
 *
 * <blockquote>`RexNode node;<br></br>
 * RelDataType rowType;<br></br>
 * RexChecker checker = new RexChecker(rowType, false);<br></br>
 * node.accept(checker);<br></br>
 * if (!checker.valid) {<br></br>
 * &nbsp;&nbsp;&nbsp;...<br></br>
 * }`</blockquote>
 *
 *
 *
 * @see RexNode
 */
class RexChecker(
    inputTypeList: List<RelDataType>, context: @Nullable RelNode.Context?,
    litmus: Litmus
) : RexVisitorImpl<Boolean?>(true) {
    //~ Instance fields --------------------------------------------------------
    protected val context: @Nullable RelNode.Context?
    protected val litmus: Litmus
    protected val inputTypeList: List<RelDataType>

    /**
     * Returns the number of failures encountered.
     *
     * @return Number of failures
     */
    var failureCount = 0
        protected set
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a RexChecker with a given input row type.
     *
     *
     * If `fail` is true, the checker will throw an
     * [AssertionError] if an invalid node is found and assertions are
     * enabled.
     *
     *
     * Otherwise, each method returns whether its part of the tree is valid.
     *
     * @param inputRowType Input row type
     * @param context Context of the enclosing [RelNode], or null
     * @param litmus What to do if an invalid node is detected
     */
    constructor(
        inputRowType: RelDataType?, context: @Nullable RelNode.Context?,
        litmus: Litmus?
    ) : this(RelOptUtil.getFieldTypeList(inputRowType), context, litmus) {
    }

    /**
     * Creates a RexChecker with a given set of input fields.
     *
     *
     * If `fail` is true, the checker will throw an
     * [AssertionError] if an invalid node is found and assertions are
     * enabled.
     *
     *
     * Otherwise, each method returns whether its part of the tree is valid.
     *
     * @param inputTypeList Input row type
     * @param context Context of the enclosing [RelNode], or null
     * @param litmus What to do if an error is detected
     */
    init {
        this.inputTypeList = inputTypeList
        this.context = context
        this.litmus = litmus
    }

    //~ Methods ----------------------------------------------------------------
    @Override
    fun visitInputRef(ref: RexInputRef): Boolean {
        val index: Int = ref.getIndex()
        if (index < 0 || index >= inputTypeList.size()) {
            ++failureCount
            return litmus.fail(
                "RexInputRef index {} out of range 0..{}",
                index, inputTypeList.size() - 1
            )
        }
        if (!ref.getType().isStruct()
            && !RelOptUtil.eq(
                "ref", ref.getType(), "input",
                inputTypeList[index], litmus
            )
        ) {
            ++failureCount
            return litmus.fail(null)
        }
        return litmus.succeed()
    }

    @Override
    fun visitLocalRef(ref: RexLocalRef?): Boolean {
        ++failureCount
        return litmus.fail("RexLocalRef illegal outside program")
    }

    @Override
    fun visitCall(call: RexCall): Boolean {
        for (operand in call.getOperands()) {
            val valid: Boolean = operand.accept(this)
            if (valid != null && !valid) {
                return litmus.fail(null)
            }
        }
        return litmus.succeed()
    }

    @Override
    fun visitFieldAccess(fieldAccess: RexFieldAccess): Boolean {
        super.visitFieldAccess(fieldAccess)
        val refType: RelDataType = fieldAccess.getReferenceExpr().getType()
        assert(refType.isStruct())
        val field: RelDataTypeField = fieldAccess.getField()
        val index: Int = field.getIndex()
        if (index < 0 || index >= refType.getFieldList().size()) {
            ++failureCount
            return litmus.fail(null)
        }
        val typeField: RelDataTypeField = refType.getFieldList().get(index)
        if (!RelOptUtil.eq(
                "type1",
                typeField.getType(),
                "type2",
                fieldAccess.getType(), litmus
            )
        ) {
            ++failureCount
            return litmus.fail(null)
        }
        return litmus.succeed()
    }

    @Override
    fun visitCorrelVariable(v: RexCorrelVariable): Boolean {
        if (context != null
            && !context.correlationIds().contains(v.id)
        ) {
            ++failureCount
            return litmus.fail(
                "correlation id {} not found in correlation list {}",
                v, context.correlationIds()
            )
        }
        return litmus.succeed()
    }

    /**
     * Returns whether an expression is valid.
     */
    fun isValid(expr: RexNode): Boolean {
        return requireNonNull(
            expr.accept(this)
        ) { "expr.accept(RexChecker) for expr=$expr" }
    }
}
