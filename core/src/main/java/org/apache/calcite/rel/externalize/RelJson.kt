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
package org.apache.calcite.rel.externalize

import org.apache.calcite.avatica.AvaticaUtils

/**
 * Utilities for converting [org.apache.calcite.rel.RelNode]
 * into JSON format.
 */
class RelJson(@Nullable jsonBuilder: JsonBuilder?) {
    private val constructorMap: Map<String, Constructor> = HashMap()

    @Nullable
    private val jsonBuilder: JsonBuilder?

    init {
        this.jsonBuilder = jsonBuilder
    }

    private fun jsonBuilder(): JsonBuilder {
        return requireNonNull(jsonBuilder, "jsonBuilder")
    }

    fun create(map: Map<String, Object?>?): RelNode {
        val type: String = Companion[map, "type"]
        val constructor: Constructor? = getConstructor(type)
        return try {
            constructor.newInstance(map) as RelNode
        } catch (e: InstantiationException) {
            throw RuntimeException(
                "while invoking constructor for type '$type'", e
            )
        } catch (e: ClassCastException) {
            throw RuntimeException(
                "while invoking constructor for type '$type'", e
            )
        } catch (e: InvocationTargetException) {
            throw RuntimeException(
                "while invoking constructor for type '$type'", e
            )
        } catch (e: IllegalAccessException) {
            throw RuntimeException(
                "while invoking constructor for type '$type'", e
            )
        }
    }

    fun getConstructor(type: String): Constructor? {
        var constructor: Constructor? = constructorMap[type]
        if (constructor == null) {
            val clazz: Class = typeNameToClass(type)
            constructor = try {
                clazz.getConstructor(RelInput::class.java)
            } catch (e: NoSuchMethodException) {
                throw RuntimeException(
                    "class does not have required constructor, "
                            + clazz + "(RelInput)"
                )
            }
            constructorMap.put(type, constructor)
        }
        return constructor
    }

    /**
     * Converts a type name to a class. E.g. `getClass("LogicalProject")`
     * returns [org.apache.calcite.rel.logical.LogicalProject].class.
     */
    fun typeNameToClass(type: String): Class {
        if (!type.contains(".")) {
            for (package_ in PACKAGES) {
                try {
                    return Class.forName(package_ + type)
                } catch (e: ClassNotFoundException) {
                    // ignore
                }
            }
        }
        return try {
            Class.forName(type)
        } catch (e: ClassNotFoundException) {
            throw RuntimeException("unknown type $type")
        }
    }

    /**
     * Inverse of [.typeNameToClass].
     */
    fun classToTypeName(class_: Class<out RelNode?>): String {
        val canonicalName: String = class_.getName()
        for (package_ in PACKAGES) {
            if (canonicalName.startsWith(package_)) {
                val remaining: String = canonicalName.substring(package_.length())
                if (remaining.indexOf('.') < 0 && remaining.indexOf('$') < 0) {
                    return remaining
                }
            }
        }
        return canonicalName
    }

    fun toJson(node: RelCollationImpl): Object {
        val list: List<Object> = ArrayList()
        for (fieldCollation in node.getFieldCollations()) {
            val map: Map<String, Object> = jsonBuilder().map()
            map.put("field", fieldCollation.getFieldIndex())
            map.put("direction", fieldCollation.getDirection().name())
            map.put("nulls", fieldCollation.nullDirection.name())
            list.add(map)
        }
        return list
    }

    fun toCollation(
        jsonFieldCollations: List<Map<String?, Object?>?>
    ): RelCollation {
        val fieldCollations: List<RelFieldCollation> = ArrayList()
        for (map in jsonFieldCollations) {
            fieldCollations.add(toFieldCollation(map))
        }
        return RelCollations.of(fieldCollations)
    }

    fun toFieldCollation(map: Map<String, Object?>?): RelFieldCollation {
        val field: Integer = get(map, "field")
        val direction: RelFieldCollation.Direction = enumVal(
            RelFieldCollation.Direction::class.java,
            map, "direction"
        )
        val nullDirection: RelFieldCollation.NullDirection = enumVal(
            RelFieldCollation.NullDirection::class.java,
            map, "nulls"
        )
        return RelFieldCollation(field, direction, nullDirection)
    }

    fun toDistribution(map: Map<String, Object?>): RelDistribution {
        val type: RelDistribution.Type = enumVal(
            RelDistribution.Type::class.java,
            map, "type"
        )
        var list: ImmutableIntList = EMPTY
        val keys: List<Integer>? = map["keys"]
        if (keys != null) {
            list = ImmutableIntList.copyOf(keys)
        }
        return RelDistributions.of(type, list)
    }

    private fun toJson(relDistribution: RelDistribution): Object {
        val map: Map<String, Object> = jsonBuilder().map()
        map.put("type", relDistribution.getType().name())
        if (!relDistribution.getKeys().isEmpty()) {
            map.put("keys", relDistribution.getKeys())
        }
        return map
    }

    fun toType(typeFactory: RelDataTypeFactory, o: Object?): RelDataType {
        return if (o is List) {
            val builder: RelDataTypeFactory.Builder = typeFactory.builder()
            for (jsonMap in o) {
                builder.add(Companion[jsonMap, "name"], toType(typeFactory, jsonMap))
            }
            builder.build()
        } else if (o is Map) {
            @SuppressWarnings("unchecked") val map: Map<String, Object>? = o
            val type: RelDataType = getRelDataType(typeFactory, map)
            val nullable: Boolean = Companion[map, "nullable"]
            typeFactory.createTypeWithNullability(type, nullable)
        } else {
            val sqlTypeName: SqlTypeName = requireNonNull(
                Util.enumVal(SqlTypeName::class.java, o as String?)
            ) { "unable to find enum value " + o + " in class " + SqlTypeName::class.java }
            typeFactory.createSqlType(sqlTypeName)
        }
    }

    private fun getRelDataType(typeFactory: RelDataTypeFactory, map: Map<String, Object>?): RelDataType {
        val fields: Object? = map!!["fields"]
        if (fields != null) {
            // Nested struct
            return toType(typeFactory, fields)
        }
        val sqlTypeName: SqlTypeName = enumVal(SqlTypeName::class.java, map, "type")
        val component: Object
        val componentType: RelDataType
        return when (sqlTypeName) {
            INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND -> {
                val startUnit: TimeUnit = sqlTypeName.getStartUnit()
                val endUnit: TimeUnit = sqlTypeName.getEndUnit()
                typeFactory.createSqlIntervalType(
                    SqlIntervalQualifier(startUnit, endUnit, SqlParserPos.ZERO)
                )
            }
            ARRAY -> {
                component = requireNonNull(map["component"], "component")
                componentType = toType(typeFactory, component)
                typeFactory.createArrayType(componentType, -1)
            }
            MAP -> {
                val key: Object = get<Object>(map, "key")
                val value: Object = get<Object>(map, "value")
                val keyType: RelDataType = toType(typeFactory, key)
                val valueType: RelDataType = toType(typeFactory, value)
                typeFactory.createMapType(keyType, valueType)
            }
            MULTISET -> {
                component = requireNonNull(map["component"], "component")
                componentType = toType(typeFactory, component)
                typeFactory.createMultisetType(componentType, -1)
            }
            else -> {
                val precision: Integer? = map["precision"] as Integer?
                val scale: Integer? = map["scale"] as Integer?
                if (precision == null) {
                    typeFactory.createSqlType(sqlTypeName)
                } else if (scale == null) {
                    typeFactory.createSqlType(sqlTypeName, precision)
                } else {
                    typeFactory.createSqlType(sqlTypeName, precision, scale)
                }
            }
        }
    }

    fun toJson(node: AggregateCall): Object {
        val map: Map<String, Object> = jsonBuilder().map()
        val aggMap: Map<String, Object> = toJson(node.getAggregation())
        if (node.getAggregation().getFunctionType().isUserDefined()) {
            aggMap.put("class", node.getAggregation().getClass().getName())
        }
        map.put("agg", aggMap)
        map.put("type", toJson(node.getType()))
        map.put("distinct", node.isDistinct())
        map.put("operands", node.getArgList())
        map.put("name", node.getName())
        return map
    }

    @Nullable
    fun toJson(@Nullable value: Object?): Object? {
        return if (value == null || value is Number
            || value is String
            || value is Boolean
        ) {
            value
        } else if (value is RexNode) {
            toJson(value as RexNode?)
        } else if (value is RexWindow) {
            toJson(value as RexWindow?)
        } else if (value is RexFieldCollation) {
            toJson(value as RexFieldCollation?)
        } else if (value is RexWindowBound) {
            toJson(value as RexWindowBound?)
        } else if (value is CorrelationId) {
            toJson(value as CorrelationId)
        } else if (value is List) {
            val list: List<Object> = jsonBuilder().list()
            for (o in value) {
                list.add(toJson(o))
            }
            list
        } else if (value is ImmutableBitSet) {
            val list: List<Object> = jsonBuilder().list()
            for (integer in value as ImmutableBitSet) {
                list.add(toJson(integer))
            }
            list
        } else if (value is AggregateCall) {
            toJson(value as AggregateCall?)
        } else if (value is RelCollationImpl) {
            toJson(value as RelCollationImpl?)
        } else if (value is RelDataType) {
            toJson(value as RelDataType?)
        } else if (value is RelDataTypeField) {
            toJson(value as RelDataTypeField?)
        } else if (value is RelDistribution) {
            toJson(value as RelDistribution?)
        } else {
            throw UnsupportedOperationException(
                "type not serializable: "
                        + value + " (type " + value.getClass().getCanonicalName() + ")"
            )
        }
    }

    private fun toJson(node: RelDataType): Object {
        val map: Map<String, Object> = jsonBuilder().map()
        if (node.isStruct()) {
            val list: List<Object> = jsonBuilder().list()
            for (field in node.getFieldList()) {
                list.add(toJson(field))
            }
            map.put("fields", list)
            map.put("nullable", node.isNullable())
        } else {
            map.put("type", node.getSqlTypeName().name())
            map.put("nullable", node.isNullable())
            if (node.getComponentType() != null) {
                map.put("component", toJson(node.getComponentType()))
            }
            val keyType: RelDataType = node.getKeyType()
            if (keyType != null) {
                map.put("key", toJson(keyType))
            }
            val valueType: RelDataType = node.getValueType()
            if (valueType != null) {
                map.put("value", toJson(valueType))
            }
            if (node.getSqlTypeName().allowsPrec()) {
                map.put("precision", node.getPrecision())
            }
            if (node.getSqlTypeName().allowsScale()) {
                map.put("scale", node.getScale())
            }
        }
        return map
    }

    private fun toJson(node: RelDataTypeField): Object {
        val map: Map<String, Object>
        if (node.getType().isStruct()) {
            map = jsonBuilder().map()
            map.put("fields", toJson(node.getType()))
            map.put("nullable", node.getType().isNullable())
        } else {
            map = toJson(node.getType()) as Map<String, Object>
        }
        map.put("name", node.getName())
        return map
    }

    private fun toJson(node: RexNode): Object {
        val map: Map<String, Object>
        return when (node.getKind()) {
            FIELD_ACCESS -> {
                map = jsonBuilder().map()
                val fieldAccess: RexFieldAccess = node as RexFieldAccess
                map.put("field", fieldAccess.getField().getName())
                map.put("expr", toJson(fieldAccess.getReferenceExpr()))
                map
            }
            LITERAL -> {
                val literal: RexLiteral = node as RexLiteral
                val value: Object = literal.getValue3()
                map = jsonBuilder().map()
                map.put("literal", RelEnumTypes.fromEnum(value))
                map.put("type", toJson(node.getType()))
                map
            }
            INPUT_REF -> {
                map = jsonBuilder().map()
                map.put("input", (node as RexSlot).getIndex())
                map.put("name", (node as RexSlot).getName())
                map
            }
            LOCAL_REF -> {
                map = jsonBuilder().map()
                map.put("input", (node as RexSlot).getIndex())
                map.put("name", (node as RexSlot).getName())
                map.put("type", toJson(node.getType()))
                map
            }
            CORREL_VARIABLE -> {
                map = jsonBuilder().map()
                map.put("correl", (node as RexCorrelVariable).getName())
                map.put("type", toJson(node.getType()))
                map
            }
            else -> {
                if (node is RexCall) {
                    val call: RexCall = node as RexCall
                    map = jsonBuilder().map()
                    map.put("op", toJson(call.getOperator()))
                    val list: List<Object> = jsonBuilder().list()
                    for (operand in call.getOperands()) {
                        list.add(toJson(operand))
                    }
                    map.put("operands", list)
                    when (node.getKind()) {
                        CAST -> map.put("type", toJson(node.getType()))
                        else -> {}
                    }
                    if (call.getOperator() is SqlFunction) {
                        if ((call.getOperator() as SqlFunction).getFunctionType().isUserDefined()) {
                            val op: SqlOperator = call.getOperator()
                            map.put("class", op.getClass().getName())
                            map.put("type", toJson(node.getType()))
                            map.put("deterministic", op.isDeterministic())
                            map.put("dynamic", op.isDynamicFunction())
                        }
                    }
                    if (call is RexOver) {
                        val over: RexOver = call as RexOver
                        map.put("distinct", over.isDistinct())
                        map.put("type", toJson(node.getType()))
                        map.put("window", toJson(over.getWindow()))
                    }
                    return map
                }
                throw UnsupportedOperationException("unknown rex $node")
            }
        }
    }

    private fun toJson(window: RexWindow): Object {
        val map: Map<String, Object> = jsonBuilder().map()
        if (window.partitionKeys.size() > 0) {
            map.put("partition", toJson(window.partitionKeys))
        }
        if (window.orderKeys.size() > 0) {
            map.put("order", toJson(window.orderKeys))
        }
        if (window.getLowerBound() == null) {
            // No ROWS or RANGE clause
        } else if (window.getUpperBound() == null) {
            if (window.isRows()) {
                map.put("rows-lower", toJson(window.getLowerBound()))
            } else {
                map.put("range-lower", toJson(window.getLowerBound()))
            }
        } else {
            if (window.isRows()) {
                map.put("rows-lower", toJson(window.getLowerBound()))
                map.put("rows-upper", toJson(window.getUpperBound()))
            } else {
                map.put("range-lower", toJson(window.getLowerBound()))
                map.put("range-upper", toJson(window.getUpperBound()))
            }
        }
        return map
    }

    private fun toJson(collation: RexFieldCollation): Object {
        val map: Map<String, Object> = jsonBuilder().map()
        map.put("expr", toJson(collation.left))
        map.put("direction", collation.getDirection().name())
        map.put("null-direction", collation.getNullDirection().name())
        return map
    }

    private fun toJson(windowBound: RexWindowBound): Object {
        val map: Map<String, Object> = jsonBuilder().map()
        if (windowBound.isCurrentRow()) {
            map.put("type", "CURRENT_ROW")
        } else if (windowBound.isUnbounded()) {
            map.put("type", if (windowBound.isPreceding()) "UNBOUNDED_PRECEDING" else "UNBOUNDED_FOLLOWING")
        } else {
            map.put("type", if (windowBound.isPreceding()) "PRECEDING" else "FOLLOWING")
            val offset: RexNode = requireNonNull(
                windowBound.getOffset()
            ) { "getOffset for window bound $windowBound" }
            map.put("offset", toJson(offset))
        }
        return map
    }

    @PolyNull
    fun toRex(relInput: RelInput, @PolyNull o: Object?): RexNode? {
        val cluster: RelOptCluster = relInput.getCluster()
        val rexBuilder: RexBuilder = cluster.getRexBuilder()
        return if (o == null) {
            null
        } else if (o is Map) {
            val map: Map = o
            val opMap: Map<String, Object> = map.get("op")
            val typeFactory: RelDataTypeFactory = cluster.getTypeFactory()
            if (opMap != null) {
                if (map.containsKey("class")) {
                    opMap.put("class", map.get("class"))
                }
                @SuppressWarnings("unchecked") val operands: List = get(map as Map<String, Object?>, "operands")
                val rexOperands: List<RexNode> = toRexList(relInput, operands)
                val jsonType: Object = map.get("type")
                val window: Map = map.get("window")
                return if (window != null) {
                    val operator: SqlAggFunction = requireNonNull(toAggregation(opMap), "operator")
                    val type: RelDataType = toType(typeFactory, requireNonNull(jsonType, "jsonType"))
                    var partitionKeys: List<RexNode?> = ArrayList()
                    val partition: Object = window.get("partition")
                    if (partition != null) {
                        partitionKeys = toRexList(relInput, partition as List)
                    }
                    val orderKeys: List<RexFieldCollation> = ArrayList()
                    if (window.containsKey("order")) {
                        addRexFieldCollationList(orderKeys, relInput, window.get("order") as List)
                    }
                    val lowerBound: RexWindowBound?
                    val upperBound: RexWindowBound?
                    val physical: Boolean
                    if (window.get("rows-lower") != null) {
                        lowerBound = toRexWindowBound(relInput, window.get("rows-lower") as Map)
                        upperBound = toRexWindowBound(relInput, window.get("rows-upper") as Map)
                        physical = true
                    } else if (window.get("range-lower") != null) {
                        lowerBound = toRexWindowBound(relInput, window.get("range-lower") as Map)
                        upperBound = toRexWindowBound(relInput, window.get("range-upper") as Map)
                        physical = false
                    } else {
                        // No ROWS or RANGE clause
                        // Note: lower and upper bounds are non-nullable, so this branch is not reachable
                        lowerBound = null
                        upperBound = null
                        physical = false
                    }
                    val distinct: Boolean = Companion[map, "distinct"]
                    rexBuilder.makeOver(
                        type, operator, rexOperands, partitionKeys,
                        ImmutableList.copyOf(orderKeys),
                        requireNonNull(lowerBound, "lowerBound"),
                        requireNonNull(upperBound, "upperBound"),
                        physical,
                        true, false, distinct, false
                    )
                } else {
                    val operator: SqlOperator = requireNonNull(toOp(opMap), "operator")
                    val type: RelDataType
                    type = if (jsonType != null) {
                        toType(typeFactory, jsonType)
                    } else {
                        rexBuilder.deriveReturnType(operator, rexOperands)
                    }
                    rexBuilder.makeCall(type, operator, rexOperands)
                }
            }
            val input: Integer = map.get("input") as Integer
            if (input != null) {
                // Check if it is a local ref.
                if (map.containsKey("type")) {
                    val type: RelDataType = toType(typeFactory, map.get("type"))
                    return rexBuilder.makeLocalRef(type, input)
                }
                val inputNodes: List<RelNode> = relInput.getInputs()
                var i: Int = input
                for (inputNode in inputNodes) {
                    val rowType: RelDataType = inputNode.getRowType()
                    if (i < rowType.getFieldCount()) {
                        val field: RelDataTypeField = rowType.getFieldList().get(i)
                        return rexBuilder.makeInputRef(field.getType(), input)
                    }
                    i -= rowType.getFieldCount()
                }
                throw RuntimeException("input field $input is out of range")
            }
            val field = map.get("field") as String
            if (field != null) {
                val jsonExpr: Object = get(map, "expr")
                val expr: RexNode? = toRex(relInput, jsonExpr)
                return rexBuilder.makeFieldAccess(expr, field, true)
            }
            val correl = map.get("correl") as String
            if (correl != null) {
                val jsonType: Object = get(map, "type")
                val type: RelDataType = toType(typeFactory, jsonType)
                return rexBuilder.makeCorrel(type, CorrelationId(correl))
            }
            if (map.containsKey("literal")) {
                var literal: Object = map.get("literal")
                val type: RelDataType = toType(typeFactory, map.get("type"))
                if (literal == null) {
                    return rexBuilder.makeNullLiteral(type)
                }
                if (type == null) {
                    // In previous versions, type was not specified for all literals.
                    // To keep backwards compatibility, if type is not specified
                    // we just interpret the literal
                    return toRex(relInput, literal)
                }
                if (type.getSqlTypeName() === SqlTypeName.SYMBOL) {
                    literal = RelEnumTypes.toEnum(literal as String)
                }
                return rexBuilder.makeLiteral(literal, type)
            }
            throw UnsupportedOperationException("cannot convert to rex $o")
        } else if (o is Boolean) {
            rexBuilder.makeLiteral(o as Boolean?)
        } else if (o is String) {
            rexBuilder.makeLiteral(o as String?)
        } else if (o is Number) {
            val number = o as Number
            if (number is Double || number is Float) {
                rexBuilder.makeApproxLiteral(
                    BigDecimal.valueOf(number.doubleValue())
                )
            } else {
                rexBuilder.makeExactLiteral(
                    BigDecimal.valueOf(number.longValue())
                )
            }
        } else {
            throw UnsupportedOperationException("cannot convert to rex $o")
        }
    }

    private fun addRexFieldCollationList(
        list: List<RexFieldCollation>,
        relInput: RelInput, @Nullable order: List<Map<String, Object>>?
    ) {
        if (order == null) {
            return
        }
        for (o in order) {
            val expr: RexNode = requireNonNull(toRex(relInput, o["expr"]), "expr")
            val directions: Set<SqlKind> = HashSet()
            if (Direction.valueOf(Companion[o, "direction"]) === Direction.DESCENDING) {
                directions.add(SqlKind.DESCENDING)
            }
            if (NullDirection.valueOf(Companion[o, "null-direction"]) === NullDirection.FIRST) {
                directions.add(SqlKind.NULLS_FIRST)
            } else {
                directions.add(SqlKind.NULLS_LAST)
            }
            list.add(RexFieldCollation(expr, directions))
        }
    }

    @Nullable
    private fun toRexWindowBound(
        input: RelInput,
        @Nullable map: Map<String, Object>?
    ): RexWindowBound? {
        if (map == null) {
            return null
        }
        val type: String = Companion[map, "type"]
        return when (type) {
            "CURRENT_ROW" -> RexWindowBounds.CURRENT_ROW
            "UNBOUNDED_PRECEDING" -> RexWindowBounds.UNBOUNDED_PRECEDING
            "UNBOUNDED_FOLLOWING" -> RexWindowBounds.UNBOUNDED_FOLLOWING
            "PRECEDING" -> RexWindowBounds.preceding(
                toRex(
                    input,
                    get<Object>(map, "offset")
                )
            )
            "FOLLOWING" -> RexWindowBounds.following(
                toRex(
                    input,
                    get<Object>(map, "offset")
                )
            )
            else -> throw UnsupportedOperationException("cannot convert type to rex window bound $type")
        }
    }

    private fun toRexList(relInput: RelInput, operands: List): List<RexNode> {
        val list: List<RexNode> = ArrayList()
        for (operand in operands) {
            list.add(toRex(relInput, operand))
        }
        return list
    }

    @Nullable
    fun toOp(map: Map<String, Object?>): SqlOperator {
        // in case different operator has the same kind, check with both name and kind.
        val name: String = Companion[map, "name"]
        val kind: String = Companion[map, "kind"]
        val syntax: String = Companion[map, "syntax"]
        val sqlKind: SqlKind = SqlKind.valueOf(kind)
        val sqlSyntax: SqlSyntax = SqlSyntax.valueOf(syntax)
        val operators: List<SqlOperator> = ArrayList()
        SqlStdOperatorTable.instance().lookupOperatorOverloads(
            SqlIdentifier(name, SqlParserPos(0, 0)),
            null,
            sqlSyntax,
            operators,
            SqlNameMatchers.liberal()
        )
        for (operator in operators) {
            if (operator.kind === sqlKind) {
                return operator
            }
        }
        val class_ = map["class"] as String?
        if (class_ != null) {
            return AvaticaUtils.instantiatePlugin(SqlOperator::class.java, class_)
        }
        throw RESOURCE.noOperator(name, kind, syntax).ex()
    }

    @Nullable
    fun toAggregation(map: Map<String, Object?>): SqlAggFunction {
        return toOp(map) as SqlAggFunction
    }

    private fun toJson(operator: SqlOperator): Map<String, Object> {
        // User-defined operators are not yet handled.
        val map: Map<String, Object> = jsonBuilder().map()
        map.put("name", operator.getName())
        map.put("kind", operator.kind.toString())
        map.put("syntax", operator.getSyntax().toString())
        return map
    }

    companion object {
        val PACKAGES: List<String> = ImmutableList.of(
            "org.apache.calcite.rel.",
            "org.apache.calcite.rel.core.",
            "org.apache.calcite.rel.logical.",
            "org.apache.calcite.adapter.jdbc.",
            "org.apache.calcite.adapter.jdbc.JdbcRules$"
        )

        @SuppressWarnings("unchecked")
        private operator fun <T : Object?> get(
            map: Map<String, Object?>?,
            key: String
        ): T {
            return requireNonNull(map!![key]) { "entry for key $key" } as T
        }

        private fun <T : Enum<T>?> enumVal(
            clazz: Class<T>, map: Map<String, Object?>?,
            key: String
        ): T {
            val textValue: String = Companion[map, key]
            return requireNonNull(
                Util.enumVal(clazz, textValue)
            ) { "unable to find enum value $textValue in class $clazz" }
        }

        private fun toJson(node: CorrelationId): Object {
            return node.getId()
        }
    }
}
