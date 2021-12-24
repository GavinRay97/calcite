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
package org.apache.calcite.sql.validate.implicit

import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlCallBinding
import org.apache.calcite.sql.SqlFunction
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlInsert
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlLiteral
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlNodeList
import org.apache.calcite.sql.SqlOperator
import org.apache.calcite.sql.SqlSelect
import org.apache.calcite.sql.SqlUpdate
import org.apache.calcite.sql.SqlUtil
import org.apache.calcite.sql.SqlWith
import org.apache.calcite.sql.`fun`.SqlCase
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlOperandMetadata
import org.apache.calcite.sql.type.SqlTypeFamily
import org.apache.calcite.sql.type.SqlTypeUtil
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql.validate.SqlValidatorScope
import org.apache.calcite.util.Util
import java.math.BigDecimal
import java.util.AbstractList
import java.util.ArrayList
import java.util.Arrays
import java.util.List
import java.util.stream.Collectors
import org.apache.calcite.linq4j.Nullness.castNonNull
import org.apache.calcite.sql.validate.SqlNonNullableAccessors.getScope
import org.apache.calcite.sql.validate.SqlNonNullableAccessors.getSelectList
import java.util.Objects.requireNonNull

/**
 * Default implementation of Calcite implicit type cast.
 */
class TypeCoercionImpl(typeFactory: RelDataTypeFactory?, validator: SqlValidator?) :
    AbstractTypeCoercion(typeFactory, validator) {
    /**
     * Widen a SqlNode's field type to common type,
     * mainly used for set operations like UNION, INTERSECT and EXCEPT.
     *
     *
     * Rules:
     * <pre>
     *
     * type1, type2  type3       select a, b, c from t1
     * \      \      \
     * type4  type5  type6              UNION
     * /      /      /
     * type7  type8  type9       select d, e, f from t2
    </pre> *
     * For struct type (type1, type2, type3) union type (type4, type5, type6),
     * infer the first result column type type7 as the wider type of type1 and type4,
     * the second column type as the wider type of type2 and type5 and so on.
     *
     * @param scope       Validator scope
     * @param query       Query node to update the field type for
     * @param columnIndex Target column index
     * @param targetType  Target type to cast to
     */
    @Override
    override fun rowTypeCoercion(
        @Nullable scope: SqlValidatorScope?,
        query: SqlNode,
        columnIndex: Int,
        targetType: RelDataType?
    ): Boolean {
        val kind: SqlKind = query.getKind()
        return when (kind) {
            SELECT -> {
                val selectNode: SqlSelect = query as SqlSelect
                val scope1: SqlValidatorScope = validator.getSelectScope(selectNode)
                if (!coerceColumnType(scope1, getSelectList(selectNode), columnIndex, targetType)) {
                    return false
                }
                updateInferredColumnType(scope1, query, columnIndex, targetType)
                true
            }
            VALUES -> {
                for (rowConstructor in (query as SqlCall).getOperandList()) {
                    if (!coerceOperandType(scope, rowConstructor as SqlCall, columnIndex, targetType)) {
                        return false
                    }
                }
                updateInferredColumnType(
                    requireNonNull(scope, "scope"), query, columnIndex, targetType
                )
                true
            }
            WITH -> {
                val body: SqlNode = (query as SqlWith).body
                rowTypeCoercion(validator.getOverScope(query), body, columnIndex, targetType)
            }
            UNION, INTERSECT, EXCEPT -> {
                // Set operations are binary for now.
                val operand0: SqlCall = (query as SqlCall).operand(0)
                val operand1: SqlCall = (query as SqlCall).operand(1)
                val coerced = (rowTypeCoercion(scope, operand0, columnIndex, targetType)
                        && rowTypeCoercion(scope, operand1, columnIndex, targetType))
                // Update the nested SET operator node type.
                if (coerced) {
                    updateInferredColumnType(
                        requireNonNull(scope, "scope"), query, columnIndex, targetType
                    )
                }
                coerced
            }
            else -> false
        }
    }

    /**
     * Coerces operands in binary arithmetic expressions to NUMERIC types.
     *
     *
     * For binary arithmetic operators like [+, -, *, /, %]:
     * If the operand is VARCHAR,
     * coerce it to data type of the other operand if its data type is NUMERIC;
     * If the other operand is DECIMAL,
     * coerce the STRING operand to max precision/scale DECIMAL.
     */
    @Override
    override fun binaryArithmeticCoercion(binding: SqlCallBinding): Boolean {
        // Assume the operator has NUMERIC family operand type checker.
        val operator: SqlOperator = binding.getOperator()
        val kind: SqlKind = operator.getKind()
        var coerced = false
        // Binary operator
        if (binding.getOperandCount() === 2) {
            val type1: RelDataType = binding.getOperandType(0)
            val type2: RelDataType = binding.getOperandType(1)
            // Special case for datetime + interval or datetime - interval
            if (kind === SqlKind.PLUS || kind === SqlKind.MINUS) {
                if (SqlTypeUtil.isInterval(type1) || SqlTypeUtil.isInterval(type2)) {
                    return false
                }
            }
            // Binary arithmetic operator like: + - * / %
            if (kind.belongsTo(SqlKind.BINARY_ARITHMETIC)) {
                coerced = binaryArithmeticWithStrings(binding, type1, type2)
            }
        }
        return coerced
    }

    /**
     * For NUMERIC and STRING operands, cast STRING to data type of the other operand.
     */
    protected fun binaryArithmeticWithStrings(
        binding: SqlCallBinding,
        left: RelDataType?,
        right: RelDataType?
    ): Boolean {
        // For expression "NUMERIC <OP> CHARACTER",
        // PostgreSQL and MS-SQL coerce the CHARACTER operand to NUMERIC,
        // i.e. for '9':VARCHAR(1) / 2: INT, '9' would be coerced to INTEGER,
        // while for '9':VARCHAR(1) / 3.3: DOUBLE, '9' would be coerced to DOUBLE.
        // They do not allow both CHARACTER operands for binary arithmetic operators.

        // MySQL and Oracle would coerce all the string operands to DOUBLE.

        // Keep sync with PostgreSQL and MS-SQL because their behaviors are more in
        // line with the SQL standard.
        var left: RelDataType? = left
        var right: RelDataType? = right
        if (SqlTypeUtil.isString(left) && SqlTypeUtil.isNumeric(right)) {
            // If the numeric operand is DECIMAL type, coerce the STRING operand to
            // max precision/scale DECIMAL.
            if (SqlTypeUtil.isDecimal(right)) {
                right = SqlTypeUtil.getMaxPrecisionScaleDecimal(factory)
            }
            return coerceOperandType(binding.getScope(), binding.getCall(), 0, right)
        } else if (SqlTypeUtil.isNumeric(left) && SqlTypeUtil.isString(right)) {
            if (SqlTypeUtil.isDecimal(left)) {
                left = SqlTypeUtil.getMaxPrecisionScaleDecimal(factory)
            }
            return coerceOperandType(binding.getScope(), binding.getCall(), 1, left)
        }
        return false
    }

    /**
     * Coerces operands in binary comparison expressions.
     *
     *
     * Rules:
     *
     *  * For EQUALS(=) operator: 1. If operands are BOOLEAN and NUMERIC, evaluate
     * `1=true` and `0=false` all to be true; 2. If operands are datetime and string,
     * do nothing because the SqlToRelConverter already makes the type coercion;
     *  * For binary comparison [=, &gt;, &gt;=, &lt;, &lt;=]: try to find the
     * common type, i.e. "1 &gt; '1'" will be converted to "1 &gt; 1";
     *  * For BETWEEN operator, find the common comparison data type of all the operands,
     * the common type is deduced from left to right, i.e. for expression "A between B and C",
     * finds common comparison type D between A and B
     * then common comparison type E between D and C as the final common type.
     *
     */
    @Override
    override fun binaryComparisonCoercion(binding: SqlCallBinding): Boolean {
        val operator: SqlOperator = binding.getOperator()
        val kind: SqlKind = operator.getKind()
        val operandCnt: Int = binding.getOperandCount()
        var coerced = false
        // Binary operator
        if (operandCnt == 2) {
            val type1: RelDataType = binding.getOperandType(0)
            val type2: RelDataType = binding.getOperandType(1)
            // EQUALS(=) NOT_EQUALS(<>)
            if (kind.belongsTo(SqlKind.BINARY_EQUALITY)) {
                // STRING and datetime
                coerced = dateTimeStringEquality(binding, type1, type2) || coerced
                // BOOLEAN and NUMERIC
                // BOOLEAN and literal
                coerced = booleanEquality(binding, type1, type2) || coerced
            }
            // Binary comparison operator like: = > >= < <=
            if (kind.belongsTo(SqlKind.BINARY_COMPARISON)) {
                val commonType: RelDataType = commonTypeForBinaryComparison(type1, type2)
                if (null != commonType) {
                    coerced = coerceOperandsType(binding.getScope(), binding.getCall(), commonType)
                }
            }
        }
        // Infix operator like: BETWEEN
        if (kind === SqlKind.BETWEEN) {
            val operandTypes: List<RelDataType> = Util.range(operandCnt).stream()
                .map(binding::getOperandType)
                .collect(Collectors.toList())
            val commonType: RelDataType? = commonTypeForComparison(operandTypes)
            if (null != commonType) {
                coerced = coerceOperandsType(binding.getScope(), binding.getCall(), commonType)
            }
        }
        return coerced
    }

    /**
     * Finds the common type for binary comparison
     * when the size of operands `dataTypes` is more than 2.
     * If there are N(more than 2) operands,
     * finds the common type between two operands from left to right:
     *
     *
     * Rules:
     * <pre>
     * type1     type2    type3
     * |         |        |
     * +- type4 -+        |
     * |             |
     * +--- type5 ---+
    </pre> *
     * For operand data types (type1, type2, type3), deduce the common type type4
     * from type1 and type2, then common type type5 from type4 and type3.
     */
    @Nullable
    protected fun commonTypeForComparison(dataTypes: List<RelDataType?>): RelDataType? {
        assert(dataTypes.size() > 2)
        val type1: RelDataType? = dataTypes[0]
        val type2: RelDataType? = dataTypes[1]
        // No need to do type coercion if all the data types have the same type name.
        var allWithSameName: Boolean = SqlTypeUtil.sameNamedType(type1, type2)
        run {
            var i = 2
            while (i < dataTypes.size() && allWithSameName) {
                allWithSameName = SqlTypeUtil.sameNamedType(dataTypes[i - 1], dataTypes[i])
                i++
            }
        }
        if (allWithSameName) {
            return null
        }
        var commonType: RelDataType?
        if (SqlTypeUtil.sameNamedType(type1, type2)) {
            commonType = factory.leastRestrictive(Arrays.asList(type1, type2))
        } else {
            commonType = commonTypeForBinaryComparison(type1, type2)
        }
        var i = 2
        while (i < dataTypes.size() && commonType != null) {
            if (SqlTypeUtil.sameNamedType(commonType, dataTypes[i])) {
                commonType = factory.leastRestrictive(Arrays.asList(commonType, dataTypes[i]))
            } else {
                commonType = commonTypeForBinaryComparison(commonType, dataTypes[i])
            }
            i++
        }
        return commonType
    }

    /**
     * Datetime and STRING equality: cast STRING type to datetime type, SqlToRelConverter already
     * makes the conversion but we still keep this interface overridable
     * so user can have their custom implementation.
     */
    protected fun dateTimeStringEquality(
        binding: SqlCallBinding,
        left: RelDataType?,
        right: RelDataType?
    ): Boolean {
        // REVIEW Danny 2018-05-23 we do not need to coerce type for EQUALS
        // because SqlToRelConverter already does this.
        // REVIEW Danny 2019-09-23, we should unify the coercion rules in TypeCoercion
        // instead of SqlToRelConverter.
        if (SqlTypeUtil.isCharacter(left)
            && SqlTypeUtil.isDatetime(right)
        ) {
            return coerceOperandType(binding.getScope(), binding.getCall(), 0, right)
        }
        return if (SqlTypeUtil.isCharacter(right)
            && SqlTypeUtil.isDatetime(left)
        ) {
            coerceOperandType(binding.getScope(), binding.getCall(), 1, left)
        } else false
    }

    /**
     * Casts "BOOLEAN = NUMERIC" to "NUMERIC = NUMERIC". Expressions like 1=`expr` and
     * 0=`expr` can be simplified to `expr` and `not expr`, but this better happens
     * in [org.apache.calcite.rex.RexSimplify].
     *
     *
     * There are 2 cases that need type coercion here:
     *
     *  1. Case1: `boolean expr1` = 1 or `boolean expr1` = 0, replace the numeric literal with
     * `true` or `false` boolean literal.
     *  1. Case2: `boolean expr1` = `numeric expr2`, replace expr1 to `1` or `0` numeric
     * literal.
     *
     * For case2, wrap the operand in a cast operator, during sql-to-rel conversion
     * we would convert expression `cast(expr1 as right)` to `case when expr1 then 1 else 0.`
     */
    protected fun booleanEquality(
        binding: SqlCallBinding,
        left: RelDataType?, right: RelDataType?
    ): Boolean {
        val lNode: SqlNode = binding.operand(0)
        val rNode: SqlNode = binding.operand(1)
        if (SqlTypeUtil.isNumeric(left)
            && !SqlUtil.isNullLiteral(lNode, false)
            && SqlTypeUtil.isBoolean(right)
        ) {
            // Case1: numeric literal and boolean
            if (lNode.getKind() === SqlKind.LITERAL) {
                val `val`: BigDecimal = (lNode as SqlLiteral).getValueAs(BigDecimal::class.java)
                return if (`val`.compareTo(BigDecimal.ONE) === 0) {
                    val lNode1: SqlNode = SqlLiteral.createBoolean(true, SqlParserPos.ZERO)
                    binding.getCall().setOperand(0, lNode1)
                    true
                } else {
                    val lNode1: SqlNode = SqlLiteral.createBoolean(false, SqlParserPos.ZERO)
                    binding.getCall().setOperand(0, lNode1)
                    true
                }
            }
            // Case2: boolean and numeric
            return coerceOperandType(binding.getScope(), binding.getCall(), 1, left)
        }
        if (SqlTypeUtil.isNumeric(right)
            && !SqlUtil.isNullLiteral(rNode, false)
            && SqlTypeUtil.isBoolean(left)
        ) {
            // Case1: literal numeric + boolean
            if (rNode.getKind() === SqlKind.LITERAL) {
                val `val`: BigDecimal = (rNode as SqlLiteral).getValueAs(BigDecimal::class.java)
                return if (`val`.compareTo(BigDecimal.ONE) === 0) {
                    val rNode1: SqlNode = SqlLiteral.createBoolean(true, SqlParserPos.ZERO)
                    binding.getCall().setOperand(1, rNode1)
                    true
                } else {
                    val rNode1: SqlNode = SqlLiteral.createBoolean(false, SqlParserPos.ZERO)
                    binding.getCall().setOperand(1, rNode1)
                    true
                }
            }
            // Case2: boolean + numeric
            return coerceOperandType(binding.getScope(), binding.getCall(), 0, right)
        }
        return false
    }

    /**
     * CASE and COALESCE type coercion, collect all the branches types including then
     * operands and else operands to find a common type, then cast the operands to the common type
     * when needed.
     */
    @Override
    override fun caseWhenCoercion(callBinding: SqlCallBinding): Boolean {
        // For sql statement like:
        // `case when ... then (a, b, c) when ... then (d, e, f) else (g, h, i)`
        // an exception throws when entering this method.
        val caseCall: SqlCase = callBinding.getCall() as SqlCase
        val thenList: SqlNodeList = caseCall.getThenOperands()
        val argTypes: List<RelDataType> = ArrayList<RelDataType>()
        val scope: SqlValidatorScope = getScope(callBinding)
        for (node in thenList) {
            argTypes.add(
                validator.deriveType(
                    scope, node
                )
            )
        }
        val elseOp: SqlNode = requireNonNull(
            caseCall.getElseOperand()
        ) { "getElseOperand() is null for $caseCall" }
        val elseOpType: RelDataType = validator.deriveType(scope, elseOp)
        argTypes.add(elseOpType)
        // Entering this method means we have already got a wider type, recompute it here
        // just to make the interface more clear.
        val widerType: RelDataType = getWiderTypeFor(argTypes, true)
        if (null != widerType) {
            var coerced = false
            for (i in 0 until thenList.size()) {
                coerced = coerceColumnType(scope, thenList, i, widerType) || coerced
            }
            if (needToCast(scope, elseOp, widerType)) {
                coerced = (coerceOperandType(scope, caseCall, 3, widerType)
                        || coerced)
            }
            return coerced
        }
        return false
    }

    /**
     * {@inheritDoc}
     *
     *
     * STRATEGIES
     *
     *
     * With(Without) sub-query:
     *
     *
     *
     *  * With sub-query: find the common type through comparing the left hand
     * side (LHS) expression types with corresponding right hand side (RHS)
     * expression derived from the sub-query expression's row type. Wrap the
     * fields of the LHS and RHS in CAST operators if it is needed.
     *
     *  * Without sub-query: convert the nodes of the RHS to the common type by
     * checking all the argument types and find out the minimum common type that
     * all the arguments can be cast to.
     *
     *
     *
     *
     * How to find the common type:
     *
     *
     *
     *  * For both struct sql types (LHS and RHS), find the common type of every
     * LHS and RHS fields pair:
     *
     * <pre>
     * (field1, field2, field3)    (field4, field5, field6)
     * |        |       |          |       |       |
     * +--------+---type1----------+       |       |
     * |       |                  |       |
     * +-------+----type2---------+       |
     * |                          |
     * +-------------type3--------+
    </pre> *
     *  * For both basic sql types(LHS and RHS),
     * find the common type of LHS and RHS nodes.
     *
     */
    @Override
    override fun inOperationCoercion(binding: SqlCallBinding): Boolean {
        val operator: SqlOperator = binding.getOperator()
        if (operator.getKind() === SqlKind.IN || operator.getKind() === SqlKind.NOT_IN) {
            assert(binding.getOperandCount() === 2)
            val type1: RelDataType = binding.getOperandType(0)
            val type2: RelDataType = binding.getOperandType(1)
            val node1: SqlNode = binding.operand(0)
            val node2: SqlNode = binding.operand(1)
            val scope: SqlValidatorScope = binding.getScope()
            if (type1.isStruct()
                && type2.isStruct()
                && type1.getFieldCount() !== type2.getFieldCount()
            ) {
                return false
            }
            val colCount = if (type1.isStruct()) type1.getFieldCount() else 1
            val argTypes: Array<RelDataType?> = arrayOfNulls<RelDataType>(2)
            argTypes[0] = type1
            argTypes[1] = type2
            var coerced = false
            val widenTypes: List<RelDataType> = ArrayList()
            for (i in 0 until colCount) {
                val i2: Int = i
                val columnIthTypes: List<RelDataType> = object : AbstractList<RelDataType?>() {
                    @Override
                    operator fun get(index: Int): RelDataType {
                        return if (argTypes[index].isStruct()) argTypes[index].getFieldList().get(i2)
                            .getType() else argTypes[index]
                    }

                    @Override
                    fun size(): Int {
                        return argTypes.size
                    }
                }
                var widenType: RelDataType? = commonTypeForBinaryComparison(
                    columnIthTypes[0],
                    columnIthTypes[1]
                )
                if (widenType == null) {
                    widenType = getTightestCommonType(columnIthTypes[0], columnIthTypes[1])
                }
                if (widenType == null) {
                    // Can not find any common type, just return early.
                    return false
                }
                widenTypes.add(widenType)
            }
            assert(widenTypes.size() === colCount)
            for (i in 0 until widenTypes.size()) {
                val desired: RelDataType = widenTypes[i]
                // LSH maybe a row values or single node.
                if (node1.getKind() === SqlKind.ROW) {
                    assert(node1 is SqlCall)
                    if (coerceOperandType(scope, node1 as SqlCall, i, desired)) {
                        updateInferredColumnType(
                            requireNonNull(scope, "scope"),
                            node1, i, widenTypes[i]
                        )
                        coerced = true
                    }
                } else {
                    coerced = (coerceOperandType(scope, binding.getCall(), 0, desired)
                            || coerced)
                }
                // RHS may be a row values expression or sub-query.
                if (node2 is SqlNodeList) {
                    val node3: SqlNodeList = node2 as SqlNodeList
                    var listCoerced = false
                    if (type2.isStruct()) {
                        for (node in node2 as SqlNodeList) {
                            assert(node is SqlCall)
                            listCoerced = coerceOperandType(scope, node as SqlCall, i, desired) || listCoerced
                        }
                        if (listCoerced) {
                            updateInferredColumnType(
                                requireNonNull(scope, "scope"),
                                node2, i, desired
                            )
                        }
                    } else {
                        for (j in 0 until (node2 as SqlNodeList).size()) {
                            listCoerced = coerceColumnType(scope, node3, j, desired) || listCoerced
                        }
                        if (listCoerced) {
                            updateInferredType(node2, desired)
                        }
                    }
                    coerced = coerced || listCoerced
                } else {
                    // Another sub-query.
                    val scope1: SqlValidatorScope =
                        if (node2 is SqlSelect) validator.getSelectScope(node2 as SqlSelect) else scope
                    coerced = rowTypeCoercion(scope1, node2, i, desired) || coerced
                }
            }
            return coerced
        }
        return false
    }

    @Override
    fun builtinFunctionCoercion(
        binding: SqlCallBinding,
        operandTypes: List<RelDataType>,
        expectedFamilies: List<SqlTypeFamily?>
    ): Boolean {
        assert(binding.getOperandCount() === operandTypes.size())
        if (!canImplicitTypeCast(operandTypes, expectedFamilies)) {
            return false
        }
        var coerced = false
        for (i in 0 until operandTypes.size()) {
            val implicitType: RelDataType = implicitCast(operandTypes[i], expectedFamilies[i])
            coerced = (null != implicitType && operandTypes[i] !== implicitType && coerceOperandType(
                binding.getScope(),
                binding.getCall(),
                i,
                implicitType
            )
                    || coerced)
        }
        return coerced
    }

    /**
     * Type coercion for user-defined functions (UDFs).
     */
    @Override
    override fun userDefinedFunctionCoercion(
        scope: SqlValidatorScope,
        call: SqlCall, function: SqlFunction
    ): Boolean {
        val operandMetadata: SqlOperandMetadata = requireNonNull(
            function.getOperandTypeChecker() as SqlOperandMetadata
        ) { "getOperandTypeChecker is not defined for $function" }
        val paramTypes: List<RelDataType> = operandMetadata.paramTypes(scope.getValidator().getTypeFactory())
        var coerced = false
        for (i in 0 until call.operandCount()) {
            val operand: SqlNode = call.operand(i)
            coerced = if (operand.getKind() === SqlKind.ARGUMENT_ASSIGNMENT) {
                val operandList: List<SqlNode> = (operand as SqlCall).getOperandList()
                val name: String = (operandList[1] as SqlIdentifier).getSimple()
                val paramNames: List<String> = operandMetadata.paramNames()
                val formalIndex = paramNames.indexOf(name)
                if (formalIndex < 0) {
                    return false
                }
                // Column list operand type is not supported now.
                coerceOperandType(
                    scope, operand as SqlCall, 0,
                    paramTypes[formalIndex]
                ) || coerced
            } else {
                coerceOperandType(scope, call, i, paramTypes[i]) || coerced
            }
        }
        return coerced
    }

    @Override
    override fun querySourceCoercion(
        @Nullable scope: SqlValidatorScope,
        sourceRowType: RelDataType, targetRowType: RelDataType, query: SqlNode
    ): Boolean {
        val sourceFields: List<RelDataTypeField> = sourceRowType.getFieldList()
        val targetFields: List<RelDataTypeField> = targetRowType.getFieldList()
        val sourceCount: Int = sourceFields.size()
        for (i in 0 until sourceCount) {
            val sourceType: RelDataType = sourceFields[i].getType()
            val targetType: RelDataType = targetFields[i].getType()
            if (!SqlTypeUtil.equalSansNullability(validator.getTypeFactory(), sourceType, targetType)
                && !SqlTypeUtil.canCastFrom(targetType, sourceType, true)
            ) {
                // Returns early if types not equals and can not do type coercion.
                return false
            }
        }
        var coerced = false
        for (i in 0 until sourceFields.size()) {
            val targetType: RelDataType = targetFields[i].getType()
            coerced = coerceSourceRowType(scope, query, i, targetType) || coerced
        }
        return coerced
    }

    /**
     * Coerces the field expression at index `columnIndex` of source
     * in an INSERT or UPDATE query to target type.
     *
     * @param sourceScope  Query source scope
     * @param query        Query
     * @param columnIndex  Source column index to coerce type
     * @param targetType   Target type
     */
    private fun coerceSourceRowType(
        @Nullable sourceScope: SqlValidatorScope,
        query: SqlNode,
        columnIndex: Int,
        targetType: RelDataType
    ): Boolean {
        return when (query.getKind()) {
            INSERT -> {
                val insert: SqlInsert = query as SqlInsert
                coerceSourceRowType(
                    sourceScope,
                    insert.getSource(),
                    columnIndex,
                    targetType
                )
            }
            UPDATE -> {
                val update: SqlUpdate = query as SqlUpdate
                val sourceExpressionList: SqlNodeList = update.getSourceExpressionList()
                if (sourceExpressionList != null) {
                    coerceColumnType(sourceScope, sourceExpressionList, columnIndex, targetType)
                } else {
                    // Note: this is dead code since sourceExpressionList is always non-null
                    coerceSourceRowType(
                        sourceScope,
                        castNonNull(update.getSourceSelect()),
                        columnIndex,
                        targetType
                    )
                }
            }
            else -> rowTypeCoercion(sourceScope, query, columnIndex, targetType)
        }
    }
}
