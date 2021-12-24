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

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.rel.type.DynamicRecordType
import org.apache.calcite.rel.type.RelDataType
import org.apache.calcite.rel.type.RelDataTypeFactory
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl
import org.apache.calcite.rel.type.RelDataTypeField
import org.apache.calcite.sql.SqlCall
import org.apache.calcite.sql.SqlCollation
import org.apache.calcite.sql.SqlDynamicParam
import org.apache.calcite.sql.SqlIdentifier
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.sql.SqlNode
import org.apache.calcite.sql.SqlNodeList
import org.apache.calcite.sql.`fun`.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.type.SqlTypeFamily
import org.apache.calcite.sql.type.SqlTypeName
import org.apache.calcite.sql.type.SqlTypeUtil
import org.apache.calcite.sql.validate.SqlValidator
import org.apache.calcite.sql.validate.SqlValidatorNamespace
import org.apache.calcite.sql.validate.SqlValidatorScope
import org.apache.calcite.util.Pair
import org.apache.calcite.util.Util
import com.google.common.collect.ImmutableList
import java.nio.charset.Charset
import java.util.ArrayList
import java.util.List
import java.util.Map
import org.apache.calcite.sql.type.NonNullableAccessors.getCollation
import java.util.Objects.requireNonNull

/**
 * Base class for all the type coercion rules. If you want to have a custom type coercion rules,
 * inheriting this class is not necessary, but would have some convenient tool methods.
 *
 *
 * We make tool methods: [.coerceOperandType], [.coerceColumnType],
 * [.needToCast], [.updateInferredType], [.updateInferredColumnType]
 * all overridable by derived classes, you can define system specific type coercion logic.
 *
 *
 * Caution that these methods may modify the [SqlNode] tree, you should know what the
 * effect is when using these methods to customize your type coercion rules.
 *
 *
 * This class also defines the default implementation of the type widening strategies, see
 * [TypeCoercion] doc and methods: [.getTightestCommonType], [.getWiderTypeFor],
 * [.getWiderTypeForTwo], [.getWiderTypeForDecimal],
 * [.commonTypeForBinaryComparison] for the detail strategies.
 */
abstract class AbstractTypeCoercion internal constructor(typeFactory: RelDataTypeFactory?, validator: SqlValidator?) :
    TypeCoercion {
    protected var validator: SqlValidator
    protected var factory: RelDataTypeFactory

    //~ Constructors -----------------------------------------------------------
    init {
        factory = requireNonNull(typeFactory, "typeFactory")
        this.validator = requireNonNull(validator, "validator")
    }
    //~ Methods ----------------------------------------------------------------
    /**
     * Cast operand at index `index` to target type.
     * we do this base on the fact that validate happens before type coercion.
     */
    protected fun coerceOperandType(
        @Nullable scope: SqlValidatorScope?,
        call: SqlCall,
        index: Int,
        targetType: RelDataType
    ): Boolean {
        // Transform the JavaType to SQL type because the SqlDataTypeSpec
        // does not support deriving JavaType yet.
        var targetType: RelDataType = targetType
        if (RelDataTypeFactoryImpl.isJavaType(targetType)) {
            targetType = (factory as JavaTypeFactory).toSql(targetType)
        }
        val operand: SqlNode = call.getOperandList().get(index)
        if (operand is SqlDynamicParam) {
            // Do not support implicit type coercion for dynamic param.
            return false
        }
        requireNonNull(scope, "scope")
        // Check it early.
        if (!needToCast(scope, operand, targetType)) {
            return false
        }
        // Fix up nullable attr.
        val targetType1: RelDataType = syncAttributes(validator.deriveType(scope, operand), targetType)
        val desired: SqlNode = castTo(operand, targetType1)
        call.setOperand(index, desired)
        updateInferredType(desired, targetType1)
        return true
    }

    /**
     * Coerce all the operands to `commonType`.
     *
     * @param scope      Validator scope
     * @param call       the call
     * @param commonType common type to coerce to
     */
    protected fun coerceOperandsType(
        @Nullable scope: SqlValidatorScope?,
        call: SqlCall,
        commonType: RelDataType
    ): Boolean {
        var coerced = false
        for (i in 0 until call.operandCount()) {
            coerced = coerceOperandType(scope, call, i, commonType) || coerced
        }
        return coerced
    }

    /**
     * Cast column at index `index` to target type.
     *
     * @param scope      Validator scope for the node list
     * @param nodeList   Column node list
     * @param index      Index of column
     * @param targetType Target type to cast to
     */
    protected fun coerceColumnType(
        @Nullable scope: SqlValidatorScope?,
        nodeList: SqlNodeList,
        index: Int,
        targetType: RelDataType
    ): Boolean {
        // Transform the JavaType to SQL type because the SqlDataTypeSpec
        // does not support deriving JavaType yet.
        var targetType: RelDataType = targetType
        if (RelDataTypeFactoryImpl.isJavaType(targetType)) {
            targetType = (factory as JavaTypeFactory).toSql(targetType)
        }

        // This will happen when there is a star/dynamic-star column in the select list,
        // and the source is values expression, i.e. `select * from (values(1, 2, 3))`.
        // There is no need to coerce the column type, only remark
        // the inferred row type has changed, we will then add in type coercion
        // when expanding star/dynamic-star.

        // See SqlToRelConverter#convertSelectList for details.
        if (index >= nodeList.size()) {
            // Can only happen when there is a star(*) in the column,
            // just return true.
            return true
        }
        val node: SqlNode = nodeList.get(index)
        if (node is SqlDynamicParam) {
            // Do not support implicit type coercion for dynamic param.
            return false
        }
        if (node is SqlIdentifier) {
            // Do not expand a star/dynamic table col.
            val node1: SqlIdentifier = node as SqlIdentifier
            if (node1.isStar()) {
                return true
            } else if (DynamicRecordType.isDynamicStarColName(Util.last(node1.names))) {
                // Should support implicit cast for dynamic table.
                return false
            }
        }
        requireNonNull(scope, "scope is needed for needToCast(scope, operand, targetType)")
        if (node is SqlCall) {
            val node2: SqlCall = node as SqlCall
            if (node2.getOperator().kind === SqlKind.AS) {
                val operand: SqlNode = node2.operand(0)
                if (!needToCast(scope, operand, targetType)) {
                    return false
                }
                val targetType2: RelDataType = syncAttributes(validator.deriveType(scope, operand), targetType)
                val casted: SqlNode = castTo(operand, targetType2)
                node2.setOperand(0, casted)
                updateInferredType(casted, targetType2)
                return true
            }
        }
        if (!needToCast(scope, node, targetType)) {
            return false
        }
        val targetType3: RelDataType = syncAttributes(validator.deriveType(scope, node), targetType)
        val node3: SqlNode = castTo(node, targetType3)
        nodeList.set(index, node3)
        updateInferredType(node3, targetType3)
        return true
    }

    /**
     * Sync the data type additional attributes before casting,
     * i.e. nullability, charset, collation.
     */
    fun syncAttributes(
        fromType: RelDataType?,
        toType: RelDataType
    ): RelDataType {
        var syncedType: RelDataType = toType
        if (fromType != null) {
            syncedType = factory.createTypeWithNullability(syncedType, fromType.isNullable())
            if (SqlTypeUtil.inCharOrBinaryFamilies(fromType)
                && SqlTypeUtil.inCharOrBinaryFamilies(toType)
            ) {
                val charset: Charset = fromType.getCharset()
                if (charset != null && SqlTypeUtil.inCharFamily(syncedType)) {
                    val collation: SqlCollation = getCollation(fromType)
                    syncedType = factory.createTypeWithCharsetAndCollation(
                        syncedType,
                        charset,
                        collation
                    )
                }
            }
        }
        return syncedType
    }

    /** Decide if a SqlNode should be casted to target type, derived class
     * can override this strategy.  */
    protected fun needToCast(scope: SqlValidatorScope?, node: SqlNode?, toType: RelDataType): Boolean {
        val fromType: RelDataType = validator.deriveType(scope, node) ?: return false
        // This depends on the fact that type validate happens before coercion.
        // We do not have inferred type for some node, i.e. LOCALTIME.

        // This prevents that we cast a JavaType to normal RelDataType.
        if (fromType is RelDataTypeFactoryImpl.JavaType
            && toType.getSqlTypeName() === fromType.getSqlTypeName()
        ) {
            return false
        }

        // Do not make a cast when we don't know specific type (ANY) of the origin node.
        if (toType.getSqlTypeName() === SqlTypeName.ANY
            || fromType.getSqlTypeName() === SqlTypeName.ANY
        ) {
            return false
        }

        // No need to cast between char and varchar.
        if (SqlTypeUtil.isCharacter(toType) && SqlTypeUtil.isCharacter(fromType)) {
            return false
        }

        // No need to cast if the source type precedence list
        // contains target type. i.e. do not cast from
        // tinyint to int or int to bigint.
        if (fromType.getPrecedenceList().containsType(toType)
            && SqlTypeUtil.isIntType(fromType)
            && SqlTypeUtil.isIntType(toType)
        ) {
            return false
        }

        // Implicit type coercion does not handle nullability.
        if (SqlTypeUtil.equalSansNullability(factory, fromType, toType)) {
            return false
        }
        assert(SqlTypeUtil.canCastFrom(toType, fromType, true))
        return true
    }

    /**
     * Update inferred type for a SqlNode.
     */
    protected fun updateInferredType(node: SqlNode?, type: RelDataType?) {
        validator.setValidatedNodeType(node, type)
        val namespace: SqlValidatorNamespace = validator.getNamespace(node)
        if (namespace != null) {
            namespace.setType(type)
        }
    }

    /**
     * Update inferred row type for a query, i.e. SqlCall that returns struct type
     * or SqlSelect.
     *
     * @param scope       Validator scope
     * @param query       Node to inferred type
     * @param columnIndex Column index to update
     * @param desiredType Desired column type
     */
    protected fun updateInferredColumnType(
        scope: SqlValidatorScope?,
        query: SqlNode?,
        columnIndex: Int,
        desiredType: RelDataType?
    ) {
        val rowType: RelDataType = validator.deriveType(scope, query)
        assert(rowType.isStruct())
        assert(columnIndex < rowType.getFieldList().size())
        val fieldList: List<Map.Entry<String, RelDataType>> = ArrayList()
        for (i in 0 until rowType.getFieldCount()) {
            val field: RelDataTypeField = rowType.getFieldList().get(i)
            val name: String = field.getName()
            val type: RelDataType = field.getType()
            val targetType: RelDataType = if (i == columnIndex) desiredType else type
            fieldList.add(Pair.of(name, targetType))
        }
        updateInferredType(query, factory.createStructType(fieldList))
    }

    /**
     * Case1: type widening with no precision loss.
     * Find the tightest common type of two types that might be used in binary expression.
     *
     * @return tightest common type, i.e. INTEGER + DECIMAL(10, 2) returns DECIMAL(10, 2)
     */
    @Override
    @Nullable
    override fun getTightestCommonType(
        @Nullable type1: RelDataType?, @Nullable type2: RelDataType?
    ): RelDataType? {
        if (type1 == null || type2 == null) {
            return null
        }
        // If only different with nullability, return type with be nullable true.
        if (type1.equals(type2)
            || (type1.isNullable() !== type2.isNullable()
                    && factory.createTypeWithNullability(type1, type2.isNullable()).equals(type2))
        ) {
            return factory.createTypeWithNullability(
                type1,
                type1.isNullable() || type2.isNullable()
            )
        }
        // If one type is with Null type name: returns the other.
        if (SqlTypeUtil.isNull(type1)) {
            return type2
        }
        if (SqlTypeUtil.isNull(type2)) {
            return type1
        }
        var resultType: RelDataType? = null
        if (SqlTypeUtil.isString(type1)
            && SqlTypeUtil.isString(type2)
        ) {
            resultType = factory.leastRestrictive(ImmutableList.of(type1, type2))
        }
        // For numeric types: promote to highest type.
        // i.e. MS-SQL/MYSQL supports numeric types cast from/to each other.
        if (SqlTypeUtil.isNumeric(type1) && SqlTypeUtil.isNumeric(type2)) {
            // For fixed precision decimals casts from(to) each other or other numeric types,
            // we let the operator decide the precision and scale of the result.
            if (!SqlTypeUtil.isDecimal(type1) && !SqlTypeUtil.isDecimal(type2)) {
                resultType = factory.leastRestrictive(ImmutableList.of(type1, type2))
            }
        }
        // Date + Timestamp -> Timestamp.
        if (SqlTypeUtil.isDate(type1) && SqlTypeUtil.isTimestamp(type2)) {
            resultType = type2
        }
        if (SqlTypeUtil.isDate(type2) && SqlTypeUtil.isTimestamp(type1)) {
            resultType = type1
        }
        if (type1.isStruct() && type2.isStruct()) {
            if (SqlTypeUtil.equalAsStructSansNullability(
                    factory, type1, type2,
                    validator.getCatalogReader().nameMatcher()
                )
            ) {
                // If two fields only differs with name case and/or nullability:
                // - different names: use f1.name
                // - different nullabilities: `nullable` is true if one of them is nullable.
                val fields: List<RelDataType> = ArrayList()
                val fieldNames: List<String> = type1.getFieldNames()
                for (pair in Pair.zip(type1.getFieldList(), type2.getFieldList())) {
                    val leftType: RelDataType = pair.left.getType()
                    val rightType: RelDataType = pair.right.getType()
                    val dataType: RelDataType = getTightestCommonTypeOrThrow(leftType, rightType)
                    val isNullable = leftType.isNullable() || rightType.isNullable()
                    fields.add(factory.createTypeWithNullability(dataType, isNullable))
                }
                return factory.createStructType(type1.getStructKind(), fields, fieldNames)
            }
        }
        if (SqlTypeUtil.isArray(type1) && SqlTypeUtil.isArray(type2)) {
            if (SqlTypeUtil.equalSansNullability(factory, type1, type2)) {
                resultType = factory.createTypeWithNullability(
                    type1,
                    type1.isNullable() || type2.isNullable()
                )
            }
        }
        if (SqlTypeUtil.isMap(type1) && SqlTypeUtil.isMap(type2)) {
            if (SqlTypeUtil.equalSansNullability(factory, type1, type2)) {
                val keyType: RelDataType = getTightestCommonTypeOrThrow(type1.getKeyType(), type2.getKeyType())
                val valType: RelDataType = getTightestCommonTypeOrThrow(type1.getValueType(), type2.getValueType())
                resultType = factory.createMapType(keyType, valType)
            }
        }
        return resultType
    }

    private fun getTightestCommonTypeOrThrow(
        @Nullable type1: RelDataType, @Nullable type2: RelDataType
    ): RelDataType {
        return requireNonNull(
            getTightestCommonType(type1, type2)
        ) { "expected non-null getTightestCommonType for $type1 and $type2" }
    }

    /**
     * Promote all the way to VARCHAR.
     */
    @Nullable
    private fun promoteToVarChar(
        @Nullable type1: RelDataType?, @Nullable type2: RelDataType?
    ): RelDataType? {
        var resultType: RelDataType? = null
        if (type1 == null || type2 == null) {
            return null
        }
        // No promotion for char and varchar.
        if (SqlTypeUtil.isCharacter(type1) && SqlTypeUtil.isCharacter(type2)) {
            return null
        }
        // 1. Do not distinguish CHAR and VARCHAR, i.e. (INTEGER + CHAR(3))
        //    and (INTEGER + VARCHAR(5)) would both deduce VARCHAR type.
        // 2. VARCHAR has 65536 as default precision.
        // 3. Following MS-SQL: BINARY or BOOLEAN can be casted to VARCHAR.
        if (SqlTypeUtil.isAtomic(type1) && SqlTypeUtil.isCharacter(type2)) {
            resultType = factory.createSqlType(SqlTypeName.VARCHAR)
        }
        if (SqlTypeUtil.isCharacter(type1) && SqlTypeUtil.isAtomic(type2)) {
            resultType = factory.createSqlType(SqlTypeName.VARCHAR)
        }
        return resultType
    }

    /**
     * Determines common type for a comparison operator when one operand is String type and the
     * other is not. For date + timestamp operands, use timestamp as common type,
     * i.e. Timestamp(2017-01-01 00:00 ...) &gt; Date(2018) evaluates to be false.
     */
    @Override
    @Nullable
    override fun commonTypeForBinaryComparison(
        @Nullable type1: RelDataType?, @Nullable type2: RelDataType?
    ): RelDataType? {
        if (type1 == null || type2 == null) {
            return null
        }
        val typeName1: SqlTypeName = type1.getSqlTypeName()
        val typeName2: SqlTypeName = type2.getSqlTypeName()
        if (typeName1 == null || typeName2 == null) {
            return null
        }

        // DATETIME + CHARACTER -> DATETIME
        // REVIEW Danny 2019-09-23: There is some legacy redundant code in SqlToRelConverter
        // that coerce Datetime and CHARACTER comparison.
        if (SqlTypeUtil.isCharacter(type1) && SqlTypeUtil.isDatetime(type2)) {
            return type2
        }
        if (SqlTypeUtil.isDatetime(type1) && SqlTypeUtil.isCharacter(type2)) {
            return type1
        }

        // DATE + TIMESTAMP -> TIMESTAMP
        if (SqlTypeUtil.isDate(type1) && SqlTypeUtil.isTimestamp(type2)) {
            return type2
        }
        if (SqlTypeUtil.isDate(type2) && SqlTypeUtil.isTimestamp(type1)) {
            return type1
        }
        if (SqlTypeUtil.isString(type1) && typeName2 === SqlTypeName.NULL) {
            return type1
        }
        if (typeName1 === SqlTypeName.NULL && SqlTypeUtil.isString(type2)) {
            return type2
        }
        if (SqlTypeUtil.isDecimal(type1) && SqlTypeUtil.isCharacter(type2)
            || SqlTypeUtil.isCharacter(type1) && SqlTypeUtil.isDecimal(type2)
        ) {
            // There is no proper DECIMAL type for VARCHAR, using max precision/scale DECIMAL
            // as the best we can do.
            return SqlTypeUtil.getMaxPrecisionScaleDecimal(factory)
        }

        // Keep sync with MS-SQL:
        // 1. BINARY/VARBINARY can not cast to FLOAT/REAL/DOUBLE
        // because of precision loss,
        // 2. CHARACTER to TIMESTAMP need explicit cast because of TimeZone.
        // Hive:
        // 1. BINARY can not cast to any other types,
        // 2. CHARACTER can only be coerced to DOUBLE/DECIMAL.
        if (SqlTypeUtil.isBinary(type2) && SqlTypeUtil.isApproximateNumeric(type1)
            || SqlTypeUtil.isBinary(type1) && SqlTypeUtil.isApproximateNumeric(type2)
        ) {
            return null
        }

        // 1 > '1' will be coerced to 1 > 1.
        if (SqlTypeUtil.isAtomic(type1) && SqlTypeUtil.isCharacter(type2)) {
            return if (SqlTypeUtil.isTimestamp(type1)) {
                null
            } else type1
        }
        return if (SqlTypeUtil.isCharacter(type1) && SqlTypeUtil.isAtomic(type2)) {
            if (SqlTypeUtil.isTimestamp(type2)) {
                null
            } else type2
        } else null
    }

    /**
     * Case2: type widening. The main difference with [.getTightestCommonType]
     * is that we allow some precision loss when widening decimal to fractional,
     * or promote fractional to string type.
     */
    @Override
    @Nullable
    override fun getWiderTypeForTwo(
        @Nullable type1: RelDataType?,
        @Nullable type2: RelDataType?,
        stringPromotion: Boolean
    ): RelDataType? {
        if (type1 == null || type2 == null) {
            return null
        }
        var resultType: RelDataType? = getTightestCommonType(type1, type2)
        if (null == resultType) {
            resultType = getWiderTypeForDecimal(type1, type2)
        }
        if (null == resultType && stringPromotion) {
            resultType = promoteToVarChar(type1, type2)
        }
        if (null == resultType) {
            if (SqlTypeUtil.isArray(type1) && SqlTypeUtil.isArray(type2)) {
                val valType: RelDataType? = getWiderTypeForTwo(
                    type1.getComponentType(),
                    type2.getComponentType(), stringPromotion
                )
                if (null != valType) {
                    resultType = factory.createArrayType(valType, -1)
                }
            }
        }
        return resultType
    }

    /**
     * Finds a wider type when one or both types are decimal type.
     * If the wider decimal type's precision/scale exceeds system limitation,
     * this rule will truncate the decimal type to the max precision/scale.
     * For decimal and fractional types, returns a decimal type
     * which has the higher precision of the two.
     *
     *
     * The default implementation depends on the max precision/scale of the type system,
     * you can override it based on the specific system requirement in
     * [org.apache.calcite.rel.type.RelDataTypeSystem].
     */
    @Override
    @Nullable
    override fun getWiderTypeForDecimal(
        @Nullable type1: RelDataType?, @Nullable type2: RelDataType?
    ): RelDataType? {
        if (type1 == null || type2 == null) {
            return null
        }
        if (!SqlTypeUtil.isDecimal(type1) && !SqlTypeUtil.isDecimal(type2)) {
            return null
        }
        // For Calcite `DECIMAL` default to have max allowed precision,
        // so just return decimal type.
        // This is based on the RelDataTypeSystem implementation,
        // subclass should override it correctly.
        return if (SqlTypeUtil.isNumeric(type1) && SqlTypeUtil.isNumeric(type2)) {
            factory.leastRestrictive(ImmutableList.of(type1, type2))
        } else null
    }

    /**
     * Similar to [.getWiderTypeForTwo], but can handle
     * sequence types. [.getWiderTypeForTwo] doesn't satisfy the associative law,
     * i.e. (a op b) op c may not equal to a op (b op c). This is only a problem for STRING type or
     * nested STRING type in collection type like ARRAY. Excluding these types,
     * [.getWiderTypeForTwo] satisfies the associative law. For instance,
     * (DATE, INTEGER, VARCHAR) should have VARCHAR as the wider common type.
     */
    @Override
    @Nullable
    fun getWiderTypeFor(
        typeList: List<RelDataType?>,
        stringPromotion: Boolean
    ): RelDataType? {
        assert(typeList.size() > 1)
        var resultType: RelDataType? = typeList[0]
        val target: List<RelDataType> = if (stringPromotion) partitionByCharacter(typeList) else typeList
        for (tp in target) {
            resultType = getWiderTypeForTwo(tp, resultType, stringPromotion)
            if (null == resultType) {
                return null
            }
        }
        return resultType
    }

    /**
     * Checks if the types and families can have implicit type coercion.
     * We will check the type one by one, that means the 1th type and 1th family,
     * 2th type and 2th family, and the like.
     *
     * @param types    Data type need to check
     * @param families Desired type families list
     */
    fun canImplicitTypeCast(types: List<RelDataType?>, families: List<SqlTypeFamily?>?): Boolean {
        var needed = false
        if (types.size() !== families.size()) {
            return false
        }
        for (pair in Pair.zip(types, families)) {
            val implicitType: RelDataType = implicitCast(pair.left, pair.right) ?: return false
            needed = pair.left !== implicitType || needed
        }
        return needed
    }

    /**
     * Type coercion based on the inferred type from passed in operand
     * and the [SqlTypeFamily] defined in the checkers,
     * e.g. the [org.apache.calcite.sql.type.FamilyOperandTypeChecker].
     *
     *
     * Caution that we do not cast from NUMERIC to NUMERIC.
     * See [CalciteImplicitCasts](https://docs.google.com/spreadsheets/d/1GhleX5h5W8-kJKh7NMJ4vtoE78pwfaZRJl88ULX_MgU/edit?usp=sharing)
     * for the details.
     *
     * @param in       Inferred operand type
     * @param expected Expected [SqlTypeFamily] of registered SqlFunction
     * @return common type of implicit cast, null if we do not find any
     */
    @Nullable
    fun implicitCast(`in`: RelDataType, expected: SqlTypeFamily): RelDataType? {
        val numericFamilies: List<SqlTypeFamily> = ImmutableList.of(
            SqlTypeFamily.NUMERIC,
            SqlTypeFamily.DECIMAL,
            SqlTypeFamily.APPROXIMATE_NUMERIC,
            SqlTypeFamily.EXACT_NUMERIC,
            SqlTypeFamily.INTEGER
        )
        val dateTimeFamilies: List<SqlTypeFamily> = ImmutableList.of(
            SqlTypeFamily.DATE,
            SqlTypeFamily.TIME, SqlTypeFamily.TIMESTAMP
        )
        // If the expected type is already a parent of the input type, no need to cast.
        if (expected.getTypeNames().contains(`in`.getSqlTypeName())) {
            return `in`
        }
        // Cast null type (usually from null literals) into target type.
        if (SqlTypeUtil.isNull(`in`)) {
            return expected.getDefaultConcreteType(factory)
        }
        if (SqlTypeUtil.isNumeric(`in`) && expected === SqlTypeFamily.DECIMAL) {
            return factory.decimalOf(`in`)
        }
        // FLOAT/DOUBLE -> DECIMAL
        if (SqlTypeUtil.isApproximateNumeric(`in`) && expected === SqlTypeFamily.EXACT_NUMERIC) {
            return factory.decimalOf(`in`)
        }
        // DATE to TIMESTAMP
        if (SqlTypeUtil.isDate(`in`) && expected === SqlTypeFamily.TIMESTAMP) {
            return factory.createSqlType(SqlTypeName.TIMESTAMP)
        }
        // TIMESTAMP to DATE.
        if (SqlTypeUtil.isTimestamp(`in`) && expected === SqlTypeFamily.DATE) {
            return factory.createSqlType(SqlTypeName.DATE)
        }
        // If the function accepts any NUMERIC type and the input is a STRING,
        // returns the expected type family's default type.
        // REVIEW Danny 2018-05-22: same with MS-SQL and MYSQL.
        if (SqlTypeUtil.isCharacter(`in`) && numericFamilies.contains(expected)) {
            return expected.getDefaultConcreteType(factory)
        }
        // STRING + DATE -> DATE;
        // STRING + TIME -> TIME;
        // STRING + TIMESTAMP -> TIMESTAMP
        if (SqlTypeUtil.isCharacter(`in`) && dateTimeFamilies.contains(expected)) {
            return expected.getDefaultConcreteType(factory)
        }
        // STRING + BINARY -> VARBINARY
        if (SqlTypeUtil.isCharacter(`in`) && expected === SqlTypeFamily.BINARY) {
            return expected.getDefaultConcreteType(factory)
        }
        // If we get here, `in` will never be STRING type.
        return if (SqlTypeUtil.isAtomic(`in`)
            && (expected === SqlTypeFamily.STRING
                    || expected === SqlTypeFamily.CHARACTER)
        ) {
            expected.getDefaultConcreteType(factory)
        } else null
    }

    companion object {
        /** It should not be used directly, because some other work should be done
         * before cast operation, see [.coerceColumnType], [.coerceOperandType].
         *
         *
         * Ignore constant reduction which should happen in RexSimplify.
         */
        private fun castTo(node: SqlNode, type: RelDataType): SqlNode {
            return SqlStdOperatorTable.CAST.createCall(
                SqlParserPos.ZERO, node,
                SqlTypeUtil.convertTypeToSpec(type).withNullable(type.isNullable())
            )
        }

        private fun partitionByCharacter(types: List<RelDataType?>): List<RelDataType> {
            val withCharacterTypes: List<RelDataType> = ArrayList()
            val nonCharacterTypes: List<RelDataType> = ArrayList()
            for (tp in types) {
                if (SqlTypeUtil.hasCharacter(tp)) {
                    withCharacterTypes.add(tp)
                } else {
                    nonCharacterTypes.add(tp)
                }
            }
            val partitioned: List<RelDataType> = ArrayList()
            partitioned.addAll(withCharacterTypes)
            partitioned.addAll(nonCharacterTypes)
            return partitioned
        }
    }
}
