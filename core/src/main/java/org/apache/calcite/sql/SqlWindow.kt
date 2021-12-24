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
package org.apache.calcite.sql

import org.apache.calcite.linq4j.Ord

/**
 * SQL window specification.
 *
 *
 * For example, the query
 *
 * <blockquote>
 * <pre>SELECT sum(a) OVER (w ROWS 3 PRECEDING)
 * FROM t
 * WINDOW w AS (PARTITION BY x, y ORDER BY z),
 * w1 AS (w ROWS 5 PRECEDING UNBOUNDED FOLLOWING)</pre>
</blockquote> *
 *
 *
 * declares windows w and w1, and uses a window in an OVER clause. It thus
 * contains 3 [SqlWindow] objects.
 */
class SqlWindow(
    pos: SqlParserPos?, @Nullable declName: SqlIdentifier?,
    @Nullable refName: SqlIdentifier?, partitionList: SqlNodeList, orderList: SqlNodeList?,
    isRows: SqlLiteral, @Nullable lowerBound: SqlNode?, @Nullable upperBound: SqlNode?,
    @Nullable allowPartial: SqlLiteral?
) : SqlCall(pos) {
    //~ Instance fields --------------------------------------------------------
    /** The name of the window being declared.  */
    @Nullable
    var declName: SqlIdentifier?

    /** The name of the window being referenced, or null.  */
    @Nullable
    var refName: SqlIdentifier?

    /** The list of partitioning columns.  */
    var partitionList: SqlNodeList

    /** The list of ordering columns.  */
    var orderList: SqlNodeList?

    /** Whether it is a physical (rows) or logical (values) range.  */
    var isRows: SqlLiteral

    /** The lower bound of the window.  */
    @Nullable
    var lowerBound: SqlNode?

    /** The upper bound of the window.  */
    @Nullable
    var upperBound: SqlNode?

    /** Whether to allow partial results. It may be null.  */
    @Nullable
    var allowPartial: SqlLiteral?

    @Nullable
    private var windowCall: SqlCall? = null
    //~ Constructors -----------------------------------------------------------
    /**
     * Creates a window.
     */
    init {
        this.declName = declName
        this.refName = refName
        this.partitionList = partitionList
        this.orderList = orderList
        this.isRows = isRows
        this.lowerBound = lowerBound
        this.upperBound = upperBound
        this.allowPartial = allowPartial
        assert(declName == null || declName.isSimple())
        assert(partitionList != null)
        assert(orderList != null)
    }

    //~ Methods ----------------------------------------------------------------
    @get:Override
    val operator: SqlOperator
        get() = SqlWindowOperator.INSTANCE

    @get:Override
    val kind: SqlKind
        get() = SqlKind.WINDOW

    @get:Override
    @get:SuppressWarnings("nullness")
    val operandList: List<Any>
        get() = ImmutableNullableList.of(
            declName, refName, partitionList, orderList,
            isRows, lowerBound, upperBound, allowPartial
        )

    @SuppressWarnings("assignment.type.incompatible")
    @Override
    fun setOperand(i: Int, @Nullable operand: SqlNode) {
        when (i) {
            0 -> declName = operand as SqlIdentifier
            1 -> refName = operand as SqlIdentifier
            2 -> partitionList = operand as SqlNodeList
            3 -> orderList = operand as SqlNodeList
            4 -> isRows = operand as SqlLiteral
            5 -> lowerBound = operand
            6 -> upperBound = operand
            7 -> allowPartial = operand as SqlLiteral
            else -> throw AssertionError(i)
        }
    }

    @Override
    fun unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int) {
        if (null != declName) {
            declName.unparse(writer, 0, 0)
            writer.keyword("AS")
        }

        // Override, so we don't print extra parentheses.
        operator.unparse(writer, this, 0, 0)
    }

    @Nullable
    fun getDeclName(): SqlIdentifier? {
        return declName
    }

    fun setDeclName(declName: SqlIdentifier) {
        assert(declName.isSimple())
        this.declName = declName
    }

    @Nullable
    fun getLowerBound(): SqlNode? {
        return lowerBound
    }

    fun setLowerBound(@Nullable lowerBound: SqlNode?) {
        this.lowerBound = lowerBound
    }

    @Nullable
    fun getUpperBound(): SqlNode? {
        return upperBound
    }

    fun setUpperBound(@Nullable upperBound: SqlNode?) {
        this.upperBound = upperBound
    }

    /**
     * Returns if the window is guaranteed to have rows.
     * This is useful to refine data type of window aggregates.
     * For instance sum(non-nullable) over (empty window) is NULL.
     *
     * @return true when the window is non-empty
     *
     * @see org.apache.calcite.rel.core.Window.Group.isAlwaysNonEmpty
     * @see SqlOperatorBinding.getGroupCount
     * @see org.apache.calcite.sql.validate.SqlValidatorImpl.resolveWindow
     */
    val isAlwaysNonEmpty: Boolean
        get() {
            val lower: RexWindowBound
            val upper: RexWindowBound
            lower = if (lowerBound == null) {
                if (upperBound == null) {
                    RexWindowBounds.UNBOUNDED_PRECEDING
                } else {
                    RexWindowBounds.CURRENT_ROW
                }
            } else if (lowerBound is SqlLiteral) {
                RexWindowBounds.create(lowerBound, null)
            } else {
                return false
            }
            upper = if (upperBound == null) {
                RexWindowBounds.CURRENT_ROW
            } else if (upperBound is SqlLiteral) {
                RexWindowBounds.create(upperBound, null)
            } else {
                return false
            }
            return isAlwaysNonEmpty(lower, upper)
        }

    fun setRows(isRows: SqlLiteral) {
        this.isRows = isRows
    }

    @Pure
    fun isRows(): Boolean {
        return isRows.booleanValue()
    }

    fun getOrderList(): SqlNodeList? {
        return orderList
    }

    fun setOrderList(orderList: SqlNodeList?) {
        this.orderList = orderList
    }

    fun getPartitionList(): SqlNodeList {
        return partitionList
    }

    fun setPartitionList(partitionList: SqlNodeList) {
        this.partitionList = partitionList
    }

    @Nullable
    fun getRefName(): SqlIdentifier? {
        return refName
    }

    fun setWindowCall(@Nullable windowCall: SqlCall?) {
        this.windowCall = windowCall
        assert(
            windowCall == null
                    || windowCall.getOperator() is SqlAggFunction
        )
    }

    @Nullable
    fun getWindowCall(): SqlCall? {
        return windowCall
    }

    /**
     * Creates a new window by combining this one with another.
     *
     *
     * For example,
     *
     * <blockquote><pre>WINDOW (w PARTITION BY x ORDER BY y)
     * overlay
     * WINDOW w AS (PARTITION BY z)</pre></blockquote>
     *
     *
     * yields
     *
     * <blockquote><pre>WINDOW (PARTITION BY z ORDER BY y)</pre></blockquote>
     *
     *
     * Does not alter this or the other window.
     *
     * @return A new window
     */
    fun overlay(that: SqlWindow, validator: SqlValidator): SqlWindow {
        // check 7.11 rule 10c
        val partitions: SqlNodeList = getPartitionList()
        if (0 != partitions.size()) {
            throw validator.newValidationError(
                partitions.get(0),
                RESOURCE.partitionNotAllowed()
            )
        }

        // 7.11 rule 10d
        val baseOrder: SqlNodeList? = getOrderList()
        val refOrder: SqlNodeList? = that.getOrderList()
        if (0 != baseOrder.size() && 0 != refOrder.size()) {
            throw validator.newValidationError(
                baseOrder.get(0),
                RESOURCE.orderByOverlap()
            )
        }

        // 711 rule 10e
        val lowerBound: SqlNode? = that.getLowerBound()
        val upperBound: SqlNode? = that.getUpperBound()
        if (null != lowerBound || null != upperBound) {
            throw validator.newValidationError(
                that.isRows,
                RESOURCE.refWindowWithFrame()
            )
        }
        val declNameNew: SqlIdentifier = declName
        var refNameNew: SqlIdentifier? = refName
        var partitionListNew: SqlNodeList = partitionList
        var orderListNew: SqlNodeList? = orderList
        val isRowsNew: SqlLiteral = isRows
        var lowerBoundNew: SqlNode? = lowerBound
        var upperBoundNew: SqlNode? = upperBound
        val allowPartialNew: SqlLiteral = allowPartial

        // Clear the reference window, because the reference is now resolved.
        // The overlaying window may have its own reference, of course.
        refNameNew = null

        // Overlay other parameters.
        if (setOperand(partitionListNew, that.partitionList, validator)) {
            partitionListNew = that.partitionList
        }
        if (setOperand(orderListNew, that.orderList, validator)) {
            orderListNew = that.orderList
        }
        if (setOperand(lowerBoundNew, that.lowerBound, validator)) {
            lowerBoundNew = that.lowerBound
        }
        if (setOperand(upperBoundNew, that.upperBound, validator)) {
            upperBoundNew = that.upperBound
        }
        return SqlWindow(
            SqlParserPos.ZERO,
            declNameNew,
            refNameNew,
            partitionListNew,
            orderListNew,
            isRowsNew,
            lowerBoundNew,
            upperBoundNew,
            allowPartialNew
        )
    }

    /**
     * Overridden method to specifically check only the right subtree of a window
     * definition.
     *
     * @param node The SqlWindow to compare to "this" window
     * @param litmus What to do if an error is detected (nodes are not equal)
     *
     * @return boolean true if all nodes in the subtree are equal
     */
    @Override
    fun equalsDeep(@Nullable node: SqlNode, litmus: Litmus?): Boolean {
        // This is the difference over super.equalsDeep.  It skips
        // operands[0] the declared name fo this window.  We only want
        // to check the window components.
        return (node === this
                || node is SqlWindow
                && SqlNode.equalDeep(
            Util.skip(operandList),
            Util.skip((node as SqlWindow).operandList), litmus
        ))
    }

    /**
     * Returns whether partial windows are allowed. If false, a partial window
     * (for example, a window of size 1 hour which has only 45 minutes of data
     * in it) will appear to windowed aggregate functions to be empty.
     */
    @EnsuresNonNullIf(expression = "allowPartial", result = false)
    fun isAllowPartial(): Boolean {
        // Default (and standard behavior) is to allow partial windows.
        return (allowPartial == null
                || allowPartial.booleanValue())
    }

    @Override
    fun validate(
        validator: SqlValidator,
        scope: SqlValidatorScope
    ) {
        val operandScope: SqlValidatorScope = scope // REVIEW
        @SuppressWarnings("unused") val declName: SqlIdentifier? = declName
        val refName: SqlIdentifier? = refName
        var partitionList: SqlNodeList = partitionList
        var orderList: SqlNodeList? = orderList
        var isRows: SqlLiteral = isRows
        var lowerBound: SqlNode? = lowerBound
        var upperBound: SqlNode? = upperBound
        var allowPartial: SqlLiteral? = allowPartial
        if (refName != null) {
            val win: SqlWindow = validator.resolveWindow(this, operandScope)
            partitionList = win.partitionList
            orderList = win.orderList
            isRows = win.isRows
            lowerBound = win.lowerBound
            upperBound = win.upperBound
            allowPartial = win.allowPartial
        }
        for (partitionItem in partitionList) {
            try {
                partitionItem.accept(Util.OverFinder.INSTANCE)
            } catch (e: ControlFlowException) {
                throw validator.newValidationError(
                    this,
                    RESOURCE.partitionbyShouldNotContainOver()
                )
            }
            partitionItem.validateExpr(validator, operandScope)
        }
        for (orderItem in orderList) {
            val savedColumnReferenceExpansion: Boolean = validator.config().columnReferenceExpansion()
            validator.transform { config -> config.withColumnReferenceExpansion(false) }
            try {
                orderItem.accept(Util.OverFinder.INSTANCE)
            } catch (e: ControlFlowException) {
                throw validator.newValidationError(
                    this,
                    RESOURCE.orderbyShouldNotContainOver()
                )
            }
            try {
                orderItem.validateExpr(validator, scope)
            } finally {
                validator.transform { config -> config.withColumnReferenceExpansion(savedColumnReferenceExpansion) }
            }
        }

        // 6.10 rule 6a Function RANK & DENSE_RANK require ORDER BY clause
        if (orderList.size() === 0 && !SqlValidatorUtil.containsMonotonic(scope)
            && windowCall != null && windowCall.getOperator().requiresOrder()
        ) {
            throw validator.newValidationError(this, RESOURCE.funcNeedsOrderBy())
        }

        // Run framing checks if there are any
        if (upperBound != null || lowerBound != null) {
            // 6.10 Rule 6a RANK & DENSE_RANK do not allow ROWS or RANGE
            if (windowCall != null && !windowCall.getOperator().allowsFraming()) {
                throw validator.newValidationError(isRows, RESOURCE.rankWithFrame())
            }
            var orderTypeFam: SqlTypeFamily? = null

            // SQL03 7.10 Rule 11a
            if (orderList.size() > 0) {
                // if order by is a compound list then range not allowed
                if (orderList.size() > 1 && !isRows()) {
                    throw validator.newValidationError(
                        isRows,
                        RESOURCE.compoundOrderByProhibitsRange()
                    )
                }

                // get the type family for the sort key for Frame Boundary Val.
                val orderType: RelDataType = validator.deriveType(
                    operandScope,
                    orderList.get(0)
                )
                orderTypeFam = orderType.getSqlTypeName().getFamily()
            } else {
                // requires an ORDER BY clause if frame is logical(RANGE)
                // We relax this requirement if the table appears to be
                // sorted already
                if (!isRows() && !SqlValidatorUtil.containsMonotonic(scope)) {
                    throw validator.newValidationError(
                        this,
                        RESOURCE.overMissingOrderBy()
                    )
                }
            }

            // Let the bounds validate themselves
            validateFrameBoundary(
                lowerBound,
                isRows(),
                orderTypeFam,
                validator,
                operandScope
            )
            validateFrameBoundary(
                upperBound,
                isRows(),
                orderTypeFam,
                validator,
                operandScope
            )

            // Validate across boundaries. 7.10 Rule 8 a-d
            checkSpecialLiterals(this, validator)
        } else if (orderList.size() === 0 && !SqlValidatorUtil.containsMonotonic(scope)
            && windowCall != null && windowCall.getOperator().requiresOrder()
        ) {
            throw validator.newValidationError(this, RESOURCE.overMissingOrderBy())
        }
        if (!isRows() && !isAllowPartial()) {
            throw validator.newValidationError(
                castNonNull(allowPartial),
                RESOURCE.cannotUseDisallowPartialWithRange()
            )
        }
    }

    /**
     * Creates a window `(RANGE *columnName* CURRENT ROW)`.
     *
     * @param columnName Order column
     */
    fun createCurrentRowWindow(columnName: String?): SqlWindow {
        return create(
            null,
            null,
            SqlNodeList(SqlParserPos.ZERO),
            SqlNodeList(
                ImmutableList.of(
                    SqlIdentifier(columnName, SqlParserPos.ZERO)
                ),
                SqlParserPos.ZERO
            ),
            SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
            createCurrentRow(SqlParserPos.ZERO),
            createCurrentRow(SqlParserPos.ZERO),
            SqlLiteral.createBoolean(true, SqlParserPos.ZERO),
            SqlParserPos.ZERO
        )
    }

    /**
     * Creates a window `(RANGE *columnName* UNBOUNDED
     * PRECEDING)`.
     *
     * @param columnName Order column
     */
    fun createUnboundedPrecedingWindow(columnName: String?): SqlWindow {
        return create(
            null,
            null,
            SqlNodeList(SqlParserPos.ZERO),
            SqlNodeList(
                ImmutableList.of(
                    SqlIdentifier(columnName, SqlParserPos.ZERO)
                ),
                SqlParserPos.ZERO
            ),
            SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
            createUnboundedPreceding(SqlParserPos.ZERO),
            createCurrentRow(SqlParserPos.ZERO),
            SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
            SqlParserPos.ZERO
        )
    }

    @Deprecated // to be removed before 2.0
    fun populateBounds() {
        if (lowerBound == null && upperBound == null) {
            setLowerBound(createUnboundedPreceding(pos))
        }
        if (lowerBound == null) {
            setLowerBound(createCurrentRow(pos))
        }
        if (upperBound == null) {
            setUpperBound(createCurrentRow(pos))
        }
    }

    /**
     * An enumeration of types of bounds in a window: `CURRENT ROW`,
     * `UNBOUNDED PRECEDING`, and `UNBOUNDED FOLLOWING`.
     */
    internal enum class Bound(private val sql: String) : Symbolizable {
        CURRENT_ROW("CURRENT ROW"), UNBOUNDED_PRECEDING("UNBOUNDED PRECEDING"), UNBOUNDED_FOLLOWING("UNBOUNDED FOLLOWING");

        @Override
        override fun toString(): String {
            return sql
        }
    }

    /** An operator describing a window specification.  */
    private class SqlWindowOperator private constructor() :
        SqlOperator("WINDOW", SqlKind.WINDOW, 2, true, null, null, null) {
        @get:Override
        val syntax: SqlSyntax
            get() = SqlSyntax.SPECIAL

        @SuppressWarnings("argument.type.incompatible")
        @Override
        fun createCall(
            @Nullable functionQualifier: SqlLiteral?,
            pos: SqlParserPos?,
            @Nullable vararg operands: SqlNode
        ): SqlCall {
            assert(functionQualifier == null)
            assert(operands.size == 8)
            return create(
                operands[0] as SqlIdentifier,
                operands[1] as SqlIdentifier,
                operands[2] as SqlNodeList,
                operands[3] as SqlNodeList,
                operands[4] as SqlLiteral,
                operands[5],
                operands[6],
                operands[7] as SqlLiteral,
                pos
            )
        }

        @Override
        fun <R> acceptCall(
            visitor: SqlVisitor<R>?,
            call: SqlCall,
            onlyExpressions: Boolean,
            argHandler: SqlBasicVisitor.ArgHandler<R>
        ) {
            if (onlyExpressions) {
                for (operand in Ord.zip(call.getOperandList())) {
                    // if the second param is an Identifier then it's supposed to
                    // be a name from a window clause and isn't part of the
                    // group by check
                    if (operand.e == null) {
                        continue
                    }
                    if (operand.i === 1 && operand.e is SqlIdentifier) {
                        // skip refName
                        continue
                    }
                    argHandler.visitChild(visitor, call, operand.i, operand.e)
                }
            } else {
                super.acceptCall(visitor, call, onlyExpressions, argHandler)
            }
        }

        @Override
        fun unparse(
            writer: SqlWriter,
            call: SqlCall,
            leftPrec: Int,
            rightPrec: Int
        ) {
            val window = call as SqlWindow
            val frame: SqlWriter.Frame = writer.startList(SqlWriter.FrameTypeEnum.WINDOW, "(", ")")
            if (window.refName != null) {
                window.refName.unparse(writer, 0, 0)
            }
            if (window.partitionList.size() > 0) {
                writer.sep("PARTITION BY")
                val partitionFrame: SqlWriter.Frame = writer.startList("", "")
                window.partitionList.unparse(writer, 0, 0)
                writer.endList(partitionFrame)
            }
            if (window.orderList.size() > 0) {
                writer.sep("ORDER BY")
                val orderFrame: SqlWriter.Frame = writer.startList("", "")
                window.orderList.unparse(writer, 0, 0)
                writer.endList(orderFrame)
            }
            val lowerBound: SqlNode? = window.lowerBound
            val upperBound: SqlNode? = window.upperBound
            if (lowerBound == null) {
                // No ROWS or RANGE clause
            } else if (upperBound == null) {
                if (window.isRows()) {
                    writer.sep("ROWS")
                } else {
                    writer.sep("RANGE")
                }
                lowerBound.unparse(writer, 0, 0)
            } else {
                if (window.isRows()) {
                    writer.sep("ROWS BETWEEN")
                } else {
                    writer.sep("RANGE BETWEEN")
                }
                lowerBound.unparse(writer, 0, 0)
                writer.keyword("AND")
                upperBound.unparse(writer, 0, 0)
            }

            // ALLOW PARTIAL/DISALLOW PARTIAL
            if (window.allowPartial == null) {
                // do nothing
            } else if (window.isAllowPartial()) {
                // We could output "ALLOW PARTIAL", but this syntax is
                // non-standard. Omitting the clause has the same effect.
            } else {
                writer.keyword("DISALLOW PARTIAL")
            }
            writer.endList(frame)
        }

        companion object {
            val INSTANCE = SqlWindowOperator()
        }
    }

    companion object {
        /**
         * The FOLLOWING operator used exclusively in a window specification.
         */
        val FOLLOWING_OPERATOR: SqlPostfixOperator = SqlPostfixOperator(
            "FOLLOWING", SqlKind.FOLLOWING, 20,
            ReturnTypes.ARG0, null,
            null
        )

        /**
         * The PRECEDING operator used exclusively in a window specification.
         */
        val PRECEDING_OPERATOR: SqlPostfixOperator = SqlPostfixOperator(
            "PRECEDING", SqlKind.PRECEDING, 20,
            ReturnTypes.ARG0, null,
            null
        )

        fun create(
            @Nullable declName: SqlIdentifier?, @Nullable refName: SqlIdentifier?,
            partitionList: SqlNodeList, orderList: SqlNodeList?, isRows: SqlLiteral,
            @Nullable lowerBound: SqlNode?, @Nullable upperBound: SqlNode?, @Nullable allowPartial: SqlLiteral?,
            pos: SqlParserPos?
        ): SqlWindow {
            // If there's only one bound and it's 'FOLLOWING', make it the upper
            // bound.
            var lowerBound: SqlNode? = lowerBound
            var upperBound: SqlNode? = upperBound
            if (upperBound == null && lowerBound != null && lowerBound.getKind() === SqlKind.FOLLOWING) {
                upperBound = lowerBound
                lowerBound = null
            }
            return SqlWindow(
                pos, declName, refName, partitionList, orderList,
                isRows, lowerBound, upperBound, allowPartial
            )
        }

        fun isAlwaysNonEmpty(
            lower: RexWindowBound,
            upper: RexWindowBound
        ): Boolean {
            val lowerKey: Int = lower.getOrderKey()
            val upperKey: Int = upper.getOrderKey()
            return lowerKey > -1 && lowerKey <= upperKey
        }
        // CHECKSTYLE: IGNORE 1
        /** @see Util.deprecated
         */
        fun checkSpecialLiterals(window: SqlWindow, validator: SqlValidator) {
            val lowerBound: SqlNode? = window.getLowerBound()
            val upperBound: SqlNode? = window.getUpperBound()
            var lowerLitType: Object? = null
            var upperLitType: Object? = null
            var lowerOp: SqlOperator? = null
            var upperOp: SqlOperator? = null
            if (null != lowerBound) {
                if (lowerBound.getKind() === SqlKind.LITERAL) {
                    lowerLitType = (lowerBound as SqlLiteral).getValue()
                    if (Bound.UNBOUNDED_FOLLOWING == lowerLitType) {
                        throw validator.newValidationError(
                            lowerBound,
                            RESOURCE.badLowerBoundary()
                        )
                    }
                } else if (lowerBound is SqlCall) {
                    lowerOp = (lowerBound as SqlCall).getOperator()
                }
            }
            if (null != upperBound) {
                if (upperBound.getKind() === SqlKind.LITERAL) {
                    upperLitType = (upperBound as SqlLiteral).getValue()
                    if (Bound.UNBOUNDED_PRECEDING == upperLitType) {
                        throw validator.newValidationError(
                            upperBound,
                            RESOURCE.badUpperBoundary()
                        )
                    }
                } else if (upperBound is SqlCall) {
                    upperOp = (upperBound as SqlCall).getOperator()
                }
            }
            if (Bound.CURRENT_ROW == lowerLitType) {
                if (null != upperOp) {
                    if (upperOp === PRECEDING_OPERATOR) {
                        throw validator.newValidationError(
                            castNonNull(upperBound),
                            RESOURCE.currentRowPrecedingError()
                        )
                    }
                }
            } else if (null != lowerOp) {
                if (lowerOp === FOLLOWING_OPERATOR) {
                    if (null != upperOp) {
                        if (upperOp === PRECEDING_OPERATOR) {
                            throw validator.newValidationError(
                                castNonNull(upperBound),
                                RESOURCE.followingBeforePrecedingError()
                            )
                        }
                    } else if (null != upperLitType) {
                        if (Bound.CURRENT_ROW == upperLitType) {
                            throw validator.newValidationError(
                                castNonNull(upperBound),
                                RESOURCE.currentRowFollowingError()
                            )
                        }
                    }
                }
            }
        }

        fun createCurrentRow(pos: SqlParserPos?): SqlNode {
            return Bound.CURRENT_ROW.symbol(pos)
        }

        fun createUnboundedFollowing(pos: SqlParserPos?): SqlNode {
            return Bound.UNBOUNDED_FOLLOWING.symbol(pos)
        }

        fun createUnboundedPreceding(pos: SqlParserPos?): SqlNode {
            return Bound.UNBOUNDED_PRECEDING.symbol(pos)
        }

        fun createFollowing(e: SqlNode?, pos: SqlParserPos?): SqlNode {
            return FOLLOWING_OPERATOR.createCall(pos, e)
        }

        fun createPreceding(e: SqlNode?, pos: SqlParserPos?): SqlNode {
            return PRECEDING_OPERATOR.createCall(pos, e)
        }

        fun createBound(range: SqlLiteral): SqlNode {
            return range
        }

        /**
         * Returns whether an expression represents the "CURRENT ROW" bound.
         */
        fun isCurrentRow(node: SqlNode): Boolean {
            return (node is SqlLiteral
                    && (node as SqlLiteral).symbolValue(Bound::class.java) === Bound.CURRENT_ROW)
        }

        /**
         * Returns whether an expression represents the "UNBOUNDED PRECEDING" bound.
         */
        fun isUnboundedPreceding(node: SqlNode): Boolean {
            return (node is SqlLiteral
                    && (node as SqlLiteral).symbolValue(Bound::class.java) === Bound.UNBOUNDED_PRECEDING)
        }

        /**
         * Returns whether an expression represents the "UNBOUNDED FOLLOWING" bound.
         */
        fun isUnboundedFollowing(node: SqlNode): Boolean {
            return (node is SqlLiteral
                    && (node as SqlLiteral).symbolValue(Bound::class.java) === Bound.UNBOUNDED_FOLLOWING)
        }

        private fun setOperand(
            @Nullable clonedOperand: SqlNode?, @Nullable thatOperand: SqlNode?,
            validator: SqlValidator
        ): Boolean {
            return if (thatOperand != null && !SqlNodeList.isEmptyList(thatOperand)) {
                if (clonedOperand == null
                    || SqlNodeList.isEmptyList(clonedOperand)
                ) {
                    true
                } else {
                    throw validator.newValidationError(
                        clonedOperand,
                        RESOURCE.cannotOverrideWindowAttribute()
                    )
                }
            } else false
        }

        private fun validateFrameBoundary(
            @Nullable bound: SqlNode?,
            isRows: Boolean,
            @Nullable orderTypeFam: SqlTypeFamily?,
            validator: SqlValidator,
            scope: SqlValidatorScope
        ) {
            if (null == bound) {
                return
            }
            bound.validate(validator, scope)
            when (bound.getKind()) {
                LITERAL -> {}
                OTHER, FOLLOWING, PRECEDING -> {
                    assert(bound is SqlCall)
                    val boundVal: SqlNode = (bound as SqlCall).operand(0)

                    // SQL03 7.10 rule 11b Physical ROWS must be a numeric constant. JR:
                    // actually it's SQL03 7.11 rule 11b "exact numeric with scale 0"
                    // means not only numeric constant but exact numeric integral
                    // constant. We also interpret the spec. to not allow negative
                    // values, but allow zero.
                    if (isRows) {
                        if (boundVal is SqlNumericLiteral) {
                            val boundLiteral: SqlNumericLiteral = boundVal as SqlNumericLiteral
                            if (!boundLiteral.isExact()
                                || boundLiteral.getScale() != null && boundLiteral.getScale() !== 0
                                || 0 > boundLiteral.longValue(true)
                            ) {
                                // true == throw if not exact (we just tested that - right?)
                                throw validator.newValidationError(
                                    boundVal,
                                    RESOURCE.rowMustBeNonNegativeIntegral()
                                )
                            }
                        } else {
                            // Allow expressions in ROWS clause
                        }
                    }

                    // if this is a range spec check and make sure the boundary type
                    // and order by type are compatible
                    if (orderTypeFam != null && !isRows) {
                        val boundType: RelDataType = validator.deriveType(scope, boundVal)
                        val boundTypeFamily: SqlTypeFamily = boundType.getSqlTypeName().getFamily()
                        val allowableBoundTypeFamilies: List<SqlTypeFamily> = orderTypeFam.allowableDifferenceTypes()
                        if (allowableBoundTypeFamilies.isEmpty()) {
                            throw validator.newValidationError(
                                boundVal,
                                RESOURCE.orderByDataTypeProhibitsRange()
                            )
                        }
                        if (!allowableBoundTypeFamilies.contains(boundTypeFamily)) {
                            throw validator.newValidationError(
                                boundVal,
                                RESOURCE.orderByRangeMismatch()
                            )
                        }
                    }
                }
                else -> throw AssertionError("Unexpected node type")
            }
        }
    }
}
